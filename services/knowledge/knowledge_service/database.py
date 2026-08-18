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
class DocumentPayload:
    doc_id: str
    title: str
    source_url: str
    pages: int | None
    updated_at: str
    text: str
    metadata: dict[str, Any]
    source_sha256: str


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
