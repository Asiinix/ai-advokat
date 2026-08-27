from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import psycopg


@dataclass(frozen=True)
class IndexJob:
    doc_id: str
    source_sha256: str


@dataclass(frozen=True)
class SotIndexJob:
    decision_key: str
    source_sha256: str


@dataclass(frozen=True)
class DocumentPayload:
    doc_id: str
    title: str
    source_url: str
    pages: int | None
    updated_at: str
    text: str
    metadata: dict[str, Any]
    source_sha256: str


@dataclass(frozen=True)
class SotDocumentPayload:
    decision_key: str
    decision_id: str
    title: str
    source_url: str
    updated_at: str
    text: str
    metadata: dict[str, Any]
    source_sha256: str
    case_number: str
    court: str
    judge: str
    region: str
    instance: str
    proceeding_type: str
    decision_date: str
    parties: Any


# The two corpora share one database connection string and nothing else: the
# legal queue keeps its historical table name so no existing row is rewritten,
# and the judicial queue lives in its own table referencing sot_decisions.
LEGAL_INDEX_JOBS_TABLE = "search_index_jobs"
SOT_INDEX_JOBS_TABLE = "sot_search_index_jobs"


class KnowledgeDatabase:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url

    def connect(self):
        return psycopg.connect(self.database_url)

    def ensure_schema(self) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS search_index_jobs (
                    doc_id TEXT PRIMARY KEY REFERENCES documents(doc_id) ON DELETE CASCADE,
                    source_sha256 TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    chunks_indexed INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    locked_by TEXT,
                    locked_at TIMESTAMPTZ,
                    indexed_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS search_index_jobs_claim_idx
                ON search_index_jobs(status, updated_at, doc_id)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS search_index_jobs_locked_idx
                ON search_index_jobs(status, locked_at)
                """
            )
            # The judicial job table is created next to the legal one. It
            # references sot_decisions when that table already exists (the SOT
            # parser deployed before this service); in a database where the SOT
            # pipeline has never run yet, the table is created without the
            # foreign key so the legal corpus and the MCP service still work.
            cur.execute("SELECT to_regclass('sot_decisions')")
            row = cur.fetchone()
            reference = ""
            if row and row[0]:
                reference = "REFERENCES sot_decisions(decision_key) ON DELETE CASCADE"
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SOT_INDEX_JOBS_TABLE} (
                    decision_key TEXT PRIMARY KEY {reference},
                    source_sha256 TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    chunks_indexed INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    locked_by TEXT,
                    locked_at TIMESTAMPTZ,
                    indexed_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS sot_search_index_jobs_claim_idx
                ON {SOT_INDEX_JOBS_TABLE}(status, updated_at, decision_key)
                """
            )
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS sot_search_index_jobs_locked_idx
                ON {SOT_INDEX_JOBS_TABLE}(status, locked_at)
                """
            )

    # --- legal corpus queue (PRG.ZANGER documents) -------------------------

    def seed_jobs(self, limit: int) -> int:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH candidates AS (
                    SELECT d.doc_id, output.sha256
                    FROM documents AS d
                    JOIN document_outputs AS output
                      ON output.doc_id = d.doc_id
                     AND output.format = 'txt'
                    LEFT JOIN search_index_jobs AS job ON job.doc_id = d.doc_id
                    WHERE d.status = 'exported'
                      AND (
                          job.doc_id IS NULL
                          OR job.source_sha256 IS DISTINCT FROM output.sha256
                      )
                    ORDER BY d.doc_id
                    LIMIT %s
                )
                INSERT INTO search_index_jobs(doc_id, source_sha256, status)
                SELECT doc_id, sha256, 'queued'
                FROM candidates
                ON CONFLICT(doc_id) DO UPDATE SET
                    source_sha256 = excluded.source_sha256,
                    status = 'queued',
                    attempts = 0,
                    chunks_indexed = 0,
                    last_error = NULL,
                    locked_by = NULL,
                    locked_at = NULL,
                    indexed_at = NULL,
                    updated_at = now()
                WHERE search_index_jobs.source_sha256 IS DISTINCT FROM excluded.source_sha256
                """,
                (limit,),
            )
            return int(cur.rowcount or 0)

    def requeue_stale(self, lease_seconds: int) -> int:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE search_index_jobs
                SET status = 'queued',
                    locked_by = NULL,
                    locked_at = NULL,
                    updated_at = now()
                WHERE status = 'processing'
                  AND (
                      locked_at IS NULL
                      OR locked_at < now() - (%s * INTERVAL '1 second')
                  )
                """,
                (lease_seconds,),
            )
            return int(cur.rowcount or 0)

    def requeue_failed(self) -> int:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE search_index_jobs
                SET status = 'queued',
                    locked_by = NULL,
                    locked_at = NULL,
                    updated_at = now()
                WHERE status = 'failed'
                """
            )
            return int(cur.rowcount or 0)

    def claim_jobs(self, worker_id: str, limit: int) -> list[IndexJob]:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH next_jobs AS (
                    SELECT doc_id
                    FROM search_index_jobs
                    WHERE status = 'queued'
                    ORDER BY updated_at, doc_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                )
                UPDATE search_index_jobs AS job
                SET status = 'processing',
                    attempts = attempts + 1,
                    locked_by = %s,
                    locked_at = now(),
                    updated_at = now()
                FROM next_jobs
                WHERE job.doc_id = next_jobs.doc_id
                RETURNING job.doc_id, job.source_sha256
                """,
                (limit, worker_id),
            )
            return [IndexJob(doc_id=str(row[0]), source_sha256=str(row[1])) for row in cur.fetchall()]

    def load_document(self, doc_id: str) -> DocumentPayload | None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.doc_id,
                       COALESCE(d.title, ''),
                       COALESCE(d.source_url, ''),
                       d.pages,
                       d.updated_at,
                       txt.content,
                       txt.encoding,
                       txt.sha256,
                       meta.content,
                       meta.encoding
                FROM documents AS d
                JOIN document_outputs AS txt
                  ON txt.doc_id = d.doc_id
                 AND txt.format = 'txt'
                LEFT JOIN document_outputs AS meta
                  ON meta.doc_id = d.doc_id
                 AND meta.format = 'meta'
                WHERE d.doc_id = %s
                """,
                (doc_id,),
            )
            row = cur.fetchone()
        if not row:
            return None
        text = bytes(row[5]).decode(str(row[6] or "utf-8"), errors="replace")
        metadata: dict[str, Any] = {}
        if row[8] is not None:
            try:
                metadata = json.loads(bytes(row[8]).decode(str(row[9] or "utf-8"), errors="replace"))
            except (TypeError, ValueError, UnicodeError):
                metadata = {}
        updated_at = row[4]
        if hasattr(updated_at, "isoformat"):
            updated_at = updated_at.isoformat()
        else:
            updated_at = str(updated_at)
        return DocumentPayload(
            doc_id=str(row[0]),
            title=str(row[1]),
            source_url=str(row[2]),
            pages=int(row[3]) if row[3] is not None else None,
            updated_at=updated_at,
            text=text,
            metadata=metadata,
            source_sha256=str(row[7]),
        )

    def document_output_formats(self, doc_id: str) -> list[str]:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT format FROM document_outputs WHERE doc_id = %s ORDER BY format",
                (doc_id,),
            )
            return [str(row[0]) for row in cur.fetchall()]

    def mark_indexed(self, doc_id: str, chunks_indexed: int) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE search_index_jobs
                SET status = 'indexed',
                    chunks_indexed = %s,
                    last_error = NULL,
                    locked_by = NULL,
                    locked_at = NULL,
                    indexed_at = now(),
                    updated_at = now()
                WHERE doc_id = %s
                """,
                (chunks_indexed, doc_id),
            )

    def mark_failed(self, doc_id: str, error: str) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE search_index_jobs
                SET status = 'failed',
                    last_error = %s,
                    locked_by = NULL,
                    locked_at = NULL,
                    updated_at = now()
                WHERE doc_id = %s
                """,
                (error[:4000], doc_id),
            )

    def job_stats(self) -> dict[str, int]:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM search_index_jobs GROUP BY status")
            return {str(status): int(count) for status, count in cur.fetchall()}

    def collection_stats(self) -> dict[str, int]:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM documents GROUP BY status")
            return {str(status): int(count) for status, count in cur.fetchall()}

    def related_documents(self, doc_id: str, limit: int) -> list[dict[str, Any]]:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT link.linked_doc_id,
                       COALESCE(target.title, ''),
                       COALESCE(target.status, 'unknown'),
                       link.position
                FROM document_links AS link
                LEFT JOIN documents AS target ON target.doc_id = link.linked_doc_id
                WHERE link.doc_id = %s
                ORDER BY link.position
                LIMIT %s
                """,
                (doc_id, limit),
            )
            rows = cur.fetchall()
        return [
            {
                "doc_id": str(row[0]),
                "title": str(row[1]),
                "status": str(row[2]),
                "position": int(row[3]),
            }
            for row in rows
        ]

    # --- judicial corpus queue (PRG.SOT decisions) -------------------------

    def seed_sot_jobs(self, limit: int) -> int:
        try:
            with self.connect() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    WITH candidates AS (
                        SELECT d.decision_key, output.sha256
                        FROM sot_decisions AS d
                        JOIN sot_decision_outputs AS output
                          ON output.decision_key = d.decision_key
                         AND output.format = 'txt'
                        LEFT JOIN sot_search_index_jobs AS job ON job.decision_key = d.decision_key
                        WHERE d.status = 'exported'
                          AND (
                              job.decision_key IS NULL
                              OR job.source_sha256 IS DISTINCT FROM output.sha256
                          )
                        ORDER BY d.decision_key
                        LIMIT %s
                    )
                    INSERT INTO sot_search_index_jobs(decision_key, source_sha256, status)
                    SELECT decision_key, sha256, 'queued'
                    FROM candidates
                    ON CONFLICT(decision_key) DO UPDATE SET
                        source_sha256 = excluded.source_sha256,
                        status = 'queued',
                        attempts = 0,
                        chunks_indexed = 0,
                        last_error = NULL,
                        locked_by = NULL,
                        locked_at = NULL,
                        indexed_at = NULL,
                        updated_at = now()
                    WHERE sot_search_index_jobs.source_sha256 IS DISTINCT FROM excluded.source_sha256
                    """,
                    (limit,),
                )
                return int(cur.rowcount or 0)
        except psycopg.errors.UndefinedTable:
            # The SOT parser has not created its tables in this database yet:
            # there is nothing to seed, and the legal corpus must keep working.
            return 0

    def requeue_sot_stale(self, lease_seconds: int) -> int:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sot_search_index_jobs
                SET status = 'queued',
                    locked_by = NULL,
                    locked_at = NULL,
                    updated_at = now()
                WHERE status = 'processing'
                  AND (
                      locked_at IS NULL
                      OR locked_at < now() - (%s * INTERVAL '1 second')
                  )
                """,
                (lease_seconds,),
            )
            return int(cur.rowcount or 0)

    def requeue_sot_failed(self) -> int:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sot_search_index_jobs
                SET status = 'queued',
                    locked_by = NULL,
                    locked_at = NULL,
                    updated_at = now()
                WHERE status = 'failed'
                """
            )
            return int(cur.rowcount or 0)

    def claim_sot_jobs(self, worker_id: str, limit: int) -> list[SotIndexJob]:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH next_jobs AS (
                    SELECT decision_key
                    FROM sot_search_index_jobs
                    WHERE status = 'queued'
                    ORDER BY updated_at, decision_key
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                )
                UPDATE sot_search_index_jobs AS job
                SET status = 'processing',
                    attempts = attempts + 1,
                    locked_by = %s,
                    locked_at = now(),
                    updated_at = now()
                FROM next_jobs
                WHERE job.decision_key = next_jobs.decision_key
                RETURNING job.decision_key, job.source_sha256
                """,
                (limit, worker_id),
            )
            return [
                SotIndexJob(decision_key=str(row[0]), source_sha256=str(row[1]))
                for row in cur.fetchall()
            ]

    def load_sot_decision(self, decision_key: str) -> SotDocumentPayload | None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.decision_key,
                       COALESCE(d.decision_id, ''),
                       COALESCE(d.title, ''),
                       COALESCE(d.source_url, ''),
                       COALESCE(d.case_number, ''),
                       COALESCE(d.court, ''),
                       COALESCE(d.judge, ''),
                       COALESCE(d.region, ''),
                       COALESCE(d.instance, ''),
                       COALESCE(d.proceeding_type, ''),
                       COALESCE(d.decision_date, ''),
                       d.parties,
                       d.metadata,
                       d.updated_at,
                       txt.content,
                       txt.encoding,
                       txt.sha256
                FROM sot_decisions AS d
                JOIN sot_decision_outputs AS txt
                  ON txt.decision_key = d.decision_key
                 AND txt.format = 'txt'
                WHERE d.decision_key = %s
                """,
                (decision_key,),
            )
            row = cur.fetchone()
        if not row:
            return None

        parties = _json_value(row[11])
        metadata = _json_value(row[12])
        if not isinstance(metadata, dict):
            metadata = {}
        metadata = dict(metadata)
        for index, name in (
            (4, "case_number"),
            (5, "court"),
            (6, "judge"),
            (7, "region"),
            (8, "instance"),
            (9, "proceeding_type"),
            (10, "decision_date"),
        ):
            if row[index]:
                metadata.setdefault(name, str(row[index]).strip())
        if parties:
            metadata.setdefault("parties", parties)

        updated_at = row[13]
        if hasattr(updated_at, "isoformat"):
            updated_at = updated_at.isoformat()
        else:
            updated_at = str(updated_at)
        text = bytes(row[14]).decode(str(row[15] or "utf-8"), errors="replace")
        return SotDocumentPayload(
            decision_key=str(row[0]),
            decision_id=str(row[1]),
            title=str(row[2]),
            source_url=str(row[3]),
            updated_at=updated_at,
            text=text,
            metadata=metadata,
            source_sha256=str(row[16]),
            case_number=str(row[4] or ""),
            court=str(row[5] or ""),
            judge=str(row[6] or ""),
            region=str(row[7] or ""),
            instance=str(row[8] or ""),
            proceeding_type=str(row[9] or ""),
            decision_date=str(row[10] or ""),
            parties=parties,
        )

    def sot_output_formats(self, decision_key: str) -> list[str]:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT format FROM sot_decision_outputs WHERE decision_key = %s ORDER BY format",
                (decision_key,),
            )
            return [str(row[0]) for row in cur.fetchall()]

    def mark_sot_indexed(self, decision_key: str, chunks_indexed: int) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sot_search_index_jobs
                SET status = 'indexed',
                    chunks_indexed = %s,
                    last_error = NULL,
                    locked_by = NULL,
                    locked_at = NULL,
                    indexed_at = now(),
                    updated_at = now()
                WHERE decision_key = %s
                """,
                (chunks_indexed, decision_key),
            )

    def mark_sot_failed(self, decision_key: str, error: str) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sot_search_index_jobs
                SET status = 'failed',
                    last_error = %s,
                    locked_by = NULL,
                    locked_at = NULL,
                    updated_at = now()
                WHERE decision_key = %s
                """,
                (error[:4000], decision_key),
            )

    def sot_job_stats(self) -> dict[str, int]:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM sot_search_index_jobs GROUP BY status")
            return {str(status): int(count) for status, count in cur.fetchall()}

    def sot_collection_stats(self) -> dict[str, int]:
        try:
            with self.connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT status, COUNT(*) FROM sot_decisions GROUP BY status")
                return {str(status): int(count) for status, count in cur.fetchall()}
        except psycopg.errors.UndefinedTable:
            return {}


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return None
