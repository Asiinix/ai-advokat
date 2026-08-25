from __future__ import annotations

import hashlib
import json
import threading
from pathlib import Path
from typing import Iterable

from .catalog import (
    OUTCOME_DONE,
    OUTCOME_PENDING,
    PHASE_COMPLETED,
    PHASE_PENDING,
    CatalogScanState,
    build_stub,
    format_list,
    parse_format_list,
)
from .document import DocumentData
from .listing import DocumentRef
from .utils import now_iso


class PostgresCrawlStore:
    storage_label = "Postgres"
    stores_document_outputs = True

    def __init__(self, database_url: str) -> None:
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError(
                "Postgres storage needs psycopg. Install project dependencies first."
            ) from exc

        self._psycopg = psycopg
        self._lock = threading.Lock()
        self._conn = psycopg.connect(database_url)
        self._init_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _init_schema(self) -> None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS listing_pages (
                    page INTEGER PRIMARY KEY,
                    status TEXT NOT NULL,
                    doc_count INTEGER NOT NULL DEFAULT 0,
                    total INTEGER,
                    error TEXT,
                    updated_at TIMESTAMPTZ NOT NULL
                )
                """
            )
            cur.execute("ALTER TABLE listing_pages ADD COLUMN IF NOT EXISTS docs_status TEXT")
            cur.execute("ALTER TABLE listing_pages ADD COLUMN IF NOT EXISTS docs_error TEXT")
            cur.execute("ALTER TABLE listing_pages ADD COLUMN IF NOT EXISTS docs_started_at TIMESTAMPTZ")
            cur.execute("ALTER TABLE listing_pages ADD COLUMN IF NOT EXISTS docs_finished_at TIMESTAMPTZ")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    doc_id TEXT PRIMARY KEY,
                    source_system TEXT NOT NULL DEFAULT 'prg_zanger'
                        CHECK (source_system = 'prg_zanger'),
                    corpus_type TEXT NOT NULL DEFAULT 'legal_act'
                        CHECK (corpus_type = 'legal_act'),
                    title TEXT,
                    source_url TEXT,
                    status TEXT NOT NULL,
                    is_free BOOLEAN,
                    pages INTEGER,
                    formats TEXT,
                    error TEXT,
                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL
                )
                """
            )
            cur.execute(
                "ALTER TABLE documents ADD COLUMN IF NOT EXISTS source_system "
                "TEXT NOT NULL DEFAULT 'prg_zanger' CHECK (source_system = 'prg_zanger')"
            )
            cur.execute(
                "ALTER TABLE documents ADD COLUMN IF NOT EXISTS corpus_type "
                "TEXT NOT NULL DEFAULT 'legal_act' CHECK (corpus_type = 'legal_act')"
            )
            cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS queue_depth INTEGER NOT NULL DEFAULT 0")
            cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS locked_by TEXT")
            cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ")
            cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS listing_documents (
                    page INTEGER NOT NULL,
                    doc_id TEXT NOT NULL,
                    title TEXT NOT NULL DEFAULT '',
                    source_url TEXT NOT NULL DEFAULT '',
                    search_id TEXT NOT NULL DEFAULT '',
                    position INTEGER NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (page, doc_id)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS document_outputs (
                    doc_id TEXT NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
                    format TEXT NOT NULL,
                    path TEXT,
                    content_type TEXT NOT NULL,
                    encoding TEXT,
                    content BYTEA NOT NULL,
                    size_bytes BIGINT NOT NULL,
                    sha256 TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (doc_id, format)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS document_links (
                    doc_id TEXT NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
                    linked_doc_id TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (doc_id, linked_doc_id)
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS listing_documents_doc_id_idx ON listing_documents(doc_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS documents_status_depth_idx ON documents(status, queue_depth)")
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS documents_updated_doc_idx
                ON documents(updated_at DESC, doc_id DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS documents_status_updated_doc_idx
                ON documents(status, updated_at DESC, doc_id DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS documents_title_sort_idx
                ON documents((COALESCE(title, '')), doc_id DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS documents_doc_id_length_idx
                ON documents((length(doc_id)), doc_id)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS documents_doc_id_pattern_idx
                ON documents(doc_id text_pattern_ops)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS documents_title_search_idx
                ON documents USING GIN (
                    to_tsvector('simple', COALESCE(title, ''))
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS documents_formats_gin_idx
                ON documents USING GIN (
                    (COALESCE(
                        string_to_array(NULLIF(formats, ''), ','),
                        ARRAY[]::text[]
                    ))
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS document_outputs_format_idx ON document_outputs(format)")
            cur.execute("CREATE INDEX IF NOT EXISTS document_links_linked_idx ON document_links(linked_doc_id)")
            self._init_catalog_schema(cur)
            self._conn.commit()

    def _init_catalog_schema(self, cur) -> None:
        """Catalog scan state, kept apart from the legacy listing_* tables."""
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS catalog_scans (
                scan_id TEXT PRIMARY KEY,
                list_url TEXT NOT NULL,
                product TEXT NOT NULL,
                formats TEXT NOT NULL,
                phase TEXT NOT NULL,
                total_documents INTEGER,
                page_size INTEGER,
                total_pages INTEGER,
                next_page INTEGER NOT NULL DEFAULT 1,
                pages_done INTEGER NOT NULL DEFAULT 0,
                docs_seen BIGINT NOT NULL DEFAULT 0,
                docs_enqueued BIGINT NOT NULL DEFAULT 0,
                error TEXT,
                started_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS catalog_scan_documents (
                scan_id TEXT NOT NULL,
                doc_id TEXT NOT NULL,
                page INTEGER NOT NULL,
                position INTEGER NOT NULL DEFAULT 0,
                title TEXT NOT NULL DEFAULT '',
                source_url TEXT NOT NULL DEFAULT '',
                outcome TEXT NOT NULL DEFAULT 'pending',
                failure_kind TEXT,
                http_status INTEGER,
                stub JSONB,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (scan_id, doc_id)
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS catalog_scan_documents_outcome_idx
            ON catalog_scan_documents(scan_id, outcome)
            """
        )

    def mark_listing_page(
        self,
        page: int,
        status: str,
        doc_count: int = 0,
        total: int | None = None,
        error: str | None = None,
    ) -> None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO listing_pages(page, status, doc_count, total, error, updated_at)
                VALUES(%s, %s, %s, %s, %s, %s)
                ON CONFLICT(page) DO UPDATE SET
                    status=excluded.status,
                    doc_count=excluded.doc_count,
                    total=excluded.total,
                    error=excluded.error,
                    updated_at=excluded.updated_at
                """,
                (page, status, doc_count, total, error, now_iso()),
            )
            self._conn.commit()

    def get_listing_page_status(self, page: int) -> str | None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute("SELECT status FROM listing_pages WHERE page = %s", (page,))
            row = cur.fetchone()
        return str(row[0]) if row else None

    def mark_listing_documents_status(
        self,
        page: int,
        status: str,
        error: str | None = None,
    ) -> None:
        now = now_iso()
        started_at = now if status == "processing" else None
        finished_at = now if status in {"done", "partial", "failed"} else None
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO listing_pages(
                    page, status, doc_count, total, error, updated_at,
                    docs_status, docs_error, docs_started_at, docs_finished_at
                )
                VALUES(%s, 'listed', 0, NULL, NULL, %s, %s, %s, %s, %s)
                ON CONFLICT(page) DO UPDATE SET
                    docs_status=excluded.docs_status,
                    docs_error=excluded.docs_error,
                    docs_started_at=COALESCE(excluded.docs_started_at, listing_pages.docs_started_at),
                    docs_finished_at=CASE
                        WHEN excluded.docs_status = 'processing' THEN NULL
                        ELSE COALESCE(excluded.docs_finished_at, listing_pages.docs_finished_at)
                    END,
                    updated_at=excluded.updated_at
                """,
                (page, now, status, error, started_at, finished_at),
            )
            if status == "done":
                cur.execute(
                    """
                    UPDATE listing_pages
                    SET docs_status = 'done',
                        docs_finished_at = COALESCE(docs_finished_at, %s),
                        updated_at = %s
                    WHERE page <= %s
                      AND COALESCE(docs_status, '') = ''
                      AND EXISTS (
                          SELECT 1
                          FROM listing_documents
                          WHERE listing_documents.page = listing_pages.page
                      )
                    """,
                    (finished_at, now, page),
                )
            self._conn.commit()

    def is_listing_documents_done(self, page: int) -> bool:
        with self._lock, self._conn.cursor() as cur:
            cur.execute("SELECT docs_status FROM listing_pages WHERE page = %s", (page,))
            row = cur.fetchone()
        return bool(row and row[0] == "done")

    def is_listing_documents_queued(self, page: int) -> bool:
        with self._lock, self._conn.cursor() as cur:
            cur.execute("SELECT docs_status FROM listing_pages WHERE page = %s", (page,))
            row = cur.fetchone()
        return bool(row and row[0] in {"queued", "done"})

    def recommended_range_start(self, from_page: int, to_page: int) -> int:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT MIN(page)
                FROM listing_pages
                WHERE page BETWEEN %s AND %s
                  AND docs_status IN ('processing', 'partial', 'failed')
                """,
                (from_page, to_page),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                return int(row[0])

            cur.execute(
                """
                SELECT MAX(ld.page)
                FROM listing_documents AS ld
                LEFT JOIN listing_pages AS lp ON lp.page = ld.page
                WHERE ld.page BETWEEN %s AND %s
                  AND COALESCE(lp.docs_status, '') <> 'done'
                """,
                (from_page, to_page),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                return int(row[0])

            cur.execute(
                """
                SELECT MAX(page)
                FROM listing_pages
                WHERE page BETWEEN %s AND %s
                  AND docs_status = 'done'
                """,
                (from_page, to_page),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                next_page = int(row[0]) + 1
                if next_page <= to_page:
                    return max(from_page, next_page)
        return from_page

    def recommended_enqueue_start(self, from_page: int, to_page: int) -> int:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT page, docs_status
                FROM listing_pages
                WHERE page BETWEEN %s AND %s
                """,
                (from_page, to_page),
            )
            rows = cur.fetchall()
        statuses = {int(page): status for page, status in rows}
        for page in range(from_page, to_page + 1):
            if statuses.get(page) not in {"queued", "done"}:
                return page
        return to_page + 1

    def save_listing_documents(self, page: int, refs: Iterable[DocumentRef]) -> None:
        now = now_iso()
        refs = list(refs)
        with self._lock, self._conn.cursor() as cur:
            cur.execute("DELETE FROM listing_documents WHERE page = %s", (page,))
            cur.executemany(
                """
                INSERT INTO listing_documents(page, doc_id, title, source_url, search_id, position, updated_at)
                VALUES(%s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (page, ref.doc_id, ref.title, ref.source_url, ref.search_id, index, now)
                    for index, ref in enumerate(refs)
                ],
            )
            self._conn.commit()

    def get_listing_documents(self, page: int) -> list[DocumentRef]:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT doc_id, title, source_url, search_id
                FROM listing_documents
                WHERE page = %s
                ORDER BY position
                """,
                (page,),
            )
            rows = cur.fetchall()
        return [
            DocumentRef(
                doc_id=str(doc_id),
                title=str(title or ""),
                source_url=str(source_url or ""),
                search_id=str(search_id or ""),
            )
            for doc_id, title, source_url, search_id in rows
        ]

    def upsert_document(
        self,
        doc_id: str,
        status: str,
        title: str = "",
        source_url: str = "",
        is_free: bool | None = None,
        pages: int | None = None,
        formats: Iterable[str] | None = None,
        error: str | None = None,
    ) -> None:
        now = now_iso()
        formats_text = ",".join(formats or [])
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO documents(
                    doc_id, title, source_url, status, is_free, pages, formats, error, created_at, updated_at
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(doc_id) DO UPDATE SET
                    title=COALESCE(NULLIF(excluded.title, ''), documents.title),
                    source_url=COALESCE(NULLIF(excluded.source_url, ''), documents.source_url),
                    status=excluded.status,
                    is_free=COALESCE(excluded.is_free, documents.is_free),
                    pages=COALESCE(excluded.pages, documents.pages),
                    formats=COALESCE(NULLIF(excluded.formats, ''), documents.formats),
                    error=excluded.error,
                    locked_by=CASE WHEN excluded.status = 'processing' THEN documents.locked_by ELSE NULL END,
                    locked_at=CASE WHEN excluded.status = 'processing' THEN documents.locked_at ELSE NULL END,
                    updated_at=excluded.updated_at
                """,
                (doc_id, title, source_url, status, is_free, pages, formats_text, error, now, now),
            )
            self._conn.commit()

    def enqueue_document_refs(
        self,
        refs: Iterable[DocumentRef],
        depth: int = 0,
        force: bool = False,
        formats: Iterable[str] = (),
        retry_failed: bool = False,
    ) -> int:
        now = now_iso()
        added = 0
        required_formats = tuple(formats)
        with self._lock, self._conn.cursor() as cur:
            for ref in refs:
                cur.execute(
                    """
                    SELECT status, is_free, error, queue_depth
                    FROM documents
                    WHERE doc_id = %s
                    """,
                    (ref.doc_id,),
                )
                row = cur.fetchone()
                if row:
                    status = str(row[0])
                    error = str(row[2] or "").lower()
                    terminal_failed = row[1] is False or "not marked as free" in error
                    outputs_ready = False
                    if status == "exported" and required_formats:
                        placeholders = ", ".join(["%s"] * len(required_formats))
                        cur.execute(
                            f"""
                            SELECT format
                            FROM document_outputs
                            WHERE doc_id = %s AND format IN ({placeholders})
                            """,
                            (ref.doc_id, *required_formats),
                        )
                        existing = {str(item[0]) for item in cur.fetchall()}
                        outputs_ready = all(fmt in existing for fmt in required_formats)
                    elif status == "exported":
                        outputs_ready = True

                    skip_failed = terminal_failed and not retry_failed
                    if not force and (outputs_ready or status == "processing" or (status == "failed" and skip_failed)):
                        cur.execute(
                            """
                            UPDATE documents
                            SET title=COALESCE(NULLIF(%s, ''), title),
                                source_url=COALESCE(NULLIF(%s, ''), source_url),
                                queue_depth=LEAST(queue_depth, %s),
                                updated_at=%s
                            WHERE doc_id = %s
                            """,
                            (ref.title, ref.source_url, depth, now, ref.doc_id),
                        )
                        continue

                    was_queued = status == "queued"
                    cur.execute(
                        """
                        UPDATE documents
                        SET title=COALESCE(NULLIF(%s, ''), title),
                            source_url=COALESCE(NULLIF(%s, ''), source_url),
                            status='queued',
                            queue_depth=LEAST(queue_depth, %s),
                            error=NULL,
                            locked_by=NULL,
                            locked_at=NULL,
                            updated_at=%s
                        WHERE doc_id = %s
                        """,
                        (ref.title, ref.source_url, depth, now, ref.doc_id),
                    )
                    if not was_queued:
                        added += 1
                    continue

                cur.execute(
                    """
                    INSERT INTO documents(
                        doc_id, title, source_url, status, is_free, pages, formats, error,
                        created_at, updated_at, queue_depth, locked_by, locked_at, attempts
                    )
                    VALUES(%s, %s, %s, 'queued', NULL, NULL, '', NULL, %s, %s, %s, NULL, NULL, 0)
                    """,
                    (ref.doc_id, ref.title, ref.source_url, now, now, depth),
                )
                added += 1
            self._conn.commit()
        return added

    def claim_queued_document(self, worker_id: str) -> tuple[DocumentRef, int] | None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                WITH next_doc AS (
                    SELECT doc_id
                    FROM documents
                    WHERE status = 'queued'
                    ORDER BY queue_depth, updated_at, doc_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE documents AS d
                SET status='processing',
                    locked_by=%s,
                    locked_at=now(),
                    attempts=attempts + 1,
                    updated_at=now()
                FROM next_doc
                WHERE d.doc_id = next_doc.doc_id
                RETURNING d.doc_id, d.title, d.source_url, d.queue_depth
                """,
                (worker_id,),
            )
            row = cur.fetchone()
            self._conn.commit()
        if not row:
            return None
        return (
            DocumentRef(
                doc_id=str(row[0]),
                title=str(row[1] or ""),
                source_url=str(row[2] or ""),
            ),
            int(row[3] or 0),
        )

    def requeue_stale_documents(self, lease_seconds: int) -> int:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE documents
                SET status='queued',
                    locked_by=NULL,
                    locked_at=NULL,
                    updated_at=now()
                WHERE status = 'processing'
                  AND (
                      locked_at IS NULL
                      OR locked_at < now() - (%s * INTERVAL '1 second')
                  )
                """,
                (lease_seconds,),
            )
            count = cur.rowcount
            self._conn.commit()
        return int(count or 0)

    def queue_stats(self) -> dict[str, int]:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, COUNT(*)
                FROM documents
                WHERE status IN ('queued', 'processing', 'exported', 'failed')
                GROUP BY status
                """
            )
            rows = cur.fetchall()
        return {str(status): int(count) for status, count in rows}

    def get_document_status(self, doc_id: str) -> str | None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute("SELECT status FROM documents WHERE doc_id = %s", (doc_id,))
            row = cur.fetchone()
        return str(row[0]) if row else None

    def is_terminal_document_failure(self, doc_id: str) -> bool:
        with self._lock, self._conn.cursor() as cur:
            cur.execute("SELECT status, is_free, error FROM documents WHERE doc_id = %s", (doc_id,))
            row = cur.fetchone()
        if not row or row[0] != "failed":
            return False
        error = str(row[2] or "").lower()
        return row[1] is False or "not marked as free" in error

    def failed_documents(self) -> list[str]:
        with self._lock, self._conn.cursor() as cur:
            cur.execute("SELECT doc_id FROM documents WHERE status = 'failed' ORDER BY updated_at")
            rows = cur.fetchall()
        return [str(row[0]) for row in rows]

    def claim_failed_document_without_title(
        self,
        worker_id: str,
        lease_seconds: int,
    ) -> DocumentRef | None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                WITH next_doc AS (
                    SELECT doc_id
                    FROM documents
                    WHERE status = 'failed'
                      AND COALESCE(title, '') = ''
                      AND (
                          locked_at IS NULL
                          OR locked_by IS NULL
                          OR locked_by NOT LIKE 'failed-title:%%'
                          OR locked_at < now() - (%s * INTERVAL '1 second')
                      )
                    ORDER BY updated_at, doc_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE documents AS d
                SET locked_by=%s,
                    locked_at=now(),
                    updated_at=now()
                FROM next_doc
                WHERE d.doc_id = next_doc.doc_id
                  AND d.status = 'failed'
                  AND COALESCE(d.title, '') = ''
                RETURNING d.doc_id, d.source_url
                """,
                (lease_seconds, worker_id),
            )
            row = cur.fetchone()
            self._conn.commit()
        if not row:
            return None
        return DocumentRef(doc_id=str(row[0]), source_url=str(row[1] or ""))

    def update_failed_document_title(
        self,
        doc_id: str,
        title: str,
        is_free: bool | None = None,
        pages: int | None = None,
    ) -> None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE documents
                SET title=COALESCE(NULLIF(%s, ''), title),
                    is_free=COALESCE(%s, is_free),
                    pages=COALESCE(%s, pages),
                    locked_by=NULL,
                    locked_at=NULL,
                    updated_at=now()
                WHERE doc_id = %s
                  AND status = 'failed'
                """,
                (title, is_free, pages, doc_id),
            )
            self._conn.commit()

    def defer_failed_title_enrichment(self, doc_id: str) -> None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE documents
                SET locked_at=now(),
                    updated_at=now()
                WHERE doc_id = %s
                  AND status = 'failed'
                """,
                (doc_id,),
            )
            self._conn.commit()

    def stats(self) -> dict[str, int]:
        with self._lock, self._conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM documents GROUP BY status")
            rows = cur.fetchall()
        return {str(status): int(count) for status, count in rows}

    def listing_stats(self) -> dict[str, int]:
        with self._lock, self._conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM listing_pages GROUP BY status")
            rows = cur.fetchall()
        return {str(status): int(count) for status, count in rows}

    def has_document_outputs(self, doc_id: str, formats: Iterable[str]) -> bool:
        required = tuple(formats)
        if not required:
            return True
        placeholders = ", ".join(["%s"] * len(required))
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                f"SELECT format FROM document_outputs WHERE doc_id = %s AND format IN ({placeholders})",
                (doc_id, *required),
            )
            existing = {str(row[0]) for row in cur.fetchall()}
        return all(fmt in existing for fmt in required)

    def get_document_links(self, doc_id: str) -> list[str]:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT linked_doc_id
                FROM document_links
                WHERE doc_id = %s
                ORDER BY position
                """,
                (doc_id,),
            )
            rows = cur.fetchall()
        return [str(row[0]) for row in rows]

    def save_document_outputs(self, document: DocumentData, paths: dict[str, Path]) -> None:
        now = now_iso()
        content_types = {
            "html": "text/html",
            "txt": "text/plain",
            "json": "application/json",
            "pdf": "application/pdf",
            "meta": "application/json",
        }
        with self._lock, self._conn.cursor() as cur:
            for fmt, path in paths.items():
                content = Path(path).read_bytes()
                cur.execute(
                    """
                    INSERT INTO document_outputs(
                        doc_id, format, path, content_type, encoding, content, size_bytes, sha256, updated_at
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(doc_id, format) DO UPDATE SET
                        path=excluded.path,
                        content_type=excluded.content_type,
                        encoding=excluded.encoding,
                        content=excluded.content,
                        size_bytes=excluded.size_bytes,
                        sha256=excluded.sha256,
                        updated_at=excluded.updated_at
                    """,
                    (
                        document.doc_id,
                        fmt,
                        str(path),
                        content_types.get(fmt, "application/octet-stream"),
                        None if fmt == "pdf" else "utf-8",
                        content,
                        len(content),
                        hashlib.sha256(content).hexdigest(),
                        now,
                    ),
                )
            cur.execute("DELETE FROM document_links WHERE doc_id = %s", (document.doc_id,))
            for position, linked_doc_id in enumerate(document.linked_doc_ids):
                cur.execute(
                    """
                    INSERT INTO document_links(doc_id, linked_doc_id, position, updated_at)
                    VALUES(%s, %s, %s, %s)
                    ON CONFLICT(doc_id, linked_doc_id) DO UPDATE SET
                        position=excluded.position,
                        updated_at=excluded.updated_at
                    """,
                    (document.doc_id, linked_doc_id, position, now),
                )
            self._conn.commit()

    # --- catalog scan -----------------------------------------------------

    CATALOG_SCAN_COLUMNS = (
        "scan_id, list_url, product, formats, phase, total_documents, page_size, total_pages, "
        "next_page, pages_done, docs_seen, docs_enqueued, error, started_at, updated_at, completed_at"
    )

    @staticmethod
    def _timestamp_text(value: object) -> str:
        if value is None:
            return ""
        isoformat = getattr(value, "isoformat", None)
        return isoformat() if callable(isoformat) else str(value)

    def _catalog_scan_from_row(self, row: tuple) -> CatalogScanState:
        return CatalogScanState(
            scan_id=str(row[0]),
            list_url=str(row[1]),
            product=str(row[2]),
            formats=parse_format_list(row[3]),
            phase=str(row[4]),
            total_documents=None if row[5] is None else int(row[5]),
            page_size=None if row[6] is None else int(row[6]),
            total_pages=None if row[7] is None else int(row[7]),
            next_page=int(row[8] or 1),
            pages_done=int(row[9] or 0),
            docs_seen=int(row[10] or 0),
            docs_enqueued=int(row[11] or 0),
            error=None if row[12] is None else str(row[12]),
            started_at=self._timestamp_text(row[13]),
            updated_at=self._timestamp_text(row[14]),
            completed_at=self._timestamp_text(row[15]) or None,
        )

    def get_catalog_scan(self, scan_id: str) -> CatalogScanState | None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                f"SELECT {self.CATALOG_SCAN_COLUMNS} FROM catalog_scans WHERE scan_id = %s",
                (scan_id,),
            )
            row = cur.fetchone()
        return self._catalog_scan_from_row(row) if row else None

    def ensure_catalog_scan(
        self,
        scan_id: str,
        list_url: str,
        product: str,
        formats: Iterable[str],
    ) -> CatalogScanState:
        """Create the scan row once and refuse to reuse an id with other settings."""
        formats_text = format_list(tuple(formats))
        now = now_iso()
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO catalog_scans(
                    scan_id, list_url, product, formats, phase, next_page,
                    pages_done, docs_seen, docs_enqueued, started_at, updated_at
                )
                VALUES(%s, %s, %s, %s, %s, 1, 0, 0, 0, %s, %s)
                ON CONFLICT(scan_id) DO NOTHING
                """,
                (scan_id, list_url, product, formats_text, PHASE_PENDING, now, now),
            )
            cur.execute(
                f"SELECT {self.CATALOG_SCAN_COLUMNS} FROM catalog_scans WHERE scan_id = %s",
                (scan_id,),
            )
            row = cur.fetchone()
            self._conn.commit()
        state = self._catalog_scan_from_row(row)
        mismatch = [
            f"{name}: scan has {stored!r}, run asked for {given!r}"
            for name, stored, given in (
                ("list-url", state.list_url, list_url),
                ("product", state.product, product),
                ("formats", format_list(state.formats), formats_text),
            )
            if stored != given
        ]
        if mismatch:
            raise ValueError(
                f"Catalog scan '{scan_id}' was started with a different configuration ("
                + "; ".join(mismatch)
                + "). Use a new --scan-id or repeat the original settings."
            )
        return state

    def set_catalog_scan_discovery(
        self,
        scan_id: str,
        total_documents: int,
        page_size: int,
        total_pages: int,
    ) -> None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE catalog_scans
                SET total_documents=%s, page_size=%s, total_pages=%s, updated_at=%s
                WHERE scan_id = %s
                """,
                (total_documents, page_size, total_pages, now_iso(), scan_id),
            )
            self._conn.commit()

    def set_catalog_scan_phase(self, scan_id: str, phase: str, error: str | None = None) -> None:
        now = now_iso()
        completed_at = now if phase == PHASE_COMPLETED else None
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE catalog_scans
                SET phase=%s,
                    error=%s,
                    completed_at=COALESCE(%s::timestamptz, completed_at),
                    updated_at=%s
                WHERE scan_id = %s
                """,
                (phase, error, completed_at, now, scan_id),
            )
            self._conn.commit()

    def record_catalog_page(self, scan_id: str, page: int, refs: Iterable[DocumentRef]) -> int:
        """Store the membership of one listing page without touching outcomes."""
        now = now_iso()
        rows = [
            (scan_id, ref.doc_id, page, index, ref.title, ref.source_url, OUTCOME_PENDING, now)
            for index, ref in enumerate(refs)
        ]
        if not rows:
            return 0
        with self._lock, self._conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO catalog_scan_documents(
                    scan_id, doc_id, page, position, title, source_url, outcome, updated_at
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(scan_id, doc_id) DO UPDATE SET
                    page=excluded.page,
                    position=excluded.position,
                    title=COALESCE(NULLIF(excluded.title, ''), catalog_scan_documents.title),
                    source_url=COALESCE(NULLIF(excluded.source_url, ''), catalog_scan_documents.source_url),
                    updated_at=excluded.updated_at
                """,
                rows,
            )
            self._conn.commit()
        return len(rows)

    def advance_catalog_scan(self, scan_id: str, next_page: int, docs_enqueued: int = 0) -> None:
        """Move the resume cursor forward; replaying a page never moves it back."""
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE catalog_scans
                SET docs_enqueued=docs_enqueued + CASE WHEN %s > next_page THEN %s ELSE 0 END,
                    next_page=GREATEST(next_page, %s),
                    pages_done=GREATEST(pages_done, %s - 1),
                    docs_seen=(SELECT COUNT(*) FROM catalog_scan_documents WHERE scan_id = %s),
                    updated_at=%s
                WHERE scan_id = %s
                """,
                (next_page, docs_enqueued, next_page, next_page, scan_id, now_iso(), scan_id),
            )
            self._conn.commit()

    def is_catalog_scan_member(self, scan_id: str, doc_id: str) -> bool:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM catalog_scan_documents WHERE scan_id = %s AND doc_id = %s",
                (scan_id, doc_id),
            )
            row = cur.fetchone()
        return row is not None

    def record_catalog_document_outcome(
        self,
        scan_id: str,
        doc_id: str,
        outcome: str,
        failure_kind: str | None = None,
        http_status: int | None = None,
        detail: str = "",
    ) -> dict[str, object] | None:
        """Record a terminal outcome; failures keep a credential-free JSON stub."""
        now = now_iso()
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT page, position, title, source_url
                FROM catalog_scan_documents
                WHERE scan_id = %s AND doc_id = %s
                """,
                (scan_id, doc_id),
            )
            row = cur.fetchone()
            if row is None:
                self._conn.commit()
                return None
            stub = None
            if outcome != OUTCOME_DONE:
                stub = build_stub(
                    scan_id=scan_id,
                    doc_id=doc_id,
                    outcome=outcome,
                    page=int(row[0]),
                    position=int(row[1] or 0),
                    title=str(row[2] or ""),
                    source_url=str(row[3] or ""),
                    failure_kind=failure_kind,
                    http_status=http_status,
                    detail=detail,
                    recorded_at=now,
                )
            cur.execute(
                """
                UPDATE catalog_scan_documents
                SET outcome=%s, failure_kind=%s, http_status=%s, stub=%s::jsonb, updated_at=%s
                WHERE scan_id = %s AND doc_id = %s
                """,
                (
                    outcome,
                    failure_kind if stub else None,
                    http_status if stub else None,
                    json.dumps(stub, ensure_ascii=False) if stub else None,
                    now,
                    scan_id,
                    doc_id,
                ),
            )
            self._conn.commit()
        return stub

    def resolve_catalog_scan_outcomes(self, scan_id: str) -> int:
        """Close out members that another run already exported."""
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE catalog_scan_documents AS members
                SET outcome=%s, failure_kind=NULL, http_status=NULL, stub=NULL, updated_at=%s
                FROM documents
                WHERE members.scan_id = %s
                  AND members.outcome = %s
                  AND documents.doc_id = members.doc_id
                  AND documents.status = 'exported'
                """,
                (OUTCOME_DONE, now_iso(), scan_id, OUTCOME_PENDING),
            )
            count = cur.rowcount
            self._conn.commit()
        return int(count or 0)

    def pending_catalog_document_count(self, scan_id: str) -> int:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM catalog_scan_documents WHERE scan_id = %s AND outcome = %s",
                (scan_id, OUTCOME_PENDING),
            )
            row = cur.fetchone()
        return int(row[0] or 0) if row else 0

    def reclaim_catalog_scan_documents(self, scan_id: str) -> int:
        """Return this scan's documents left in processing by a dead container."""
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE documents
                SET status='queued', locked_by=NULL, locked_at=NULL, updated_at=now()
                WHERE status = 'processing'
                  AND doc_id IN (
                      SELECT doc_id FROM catalog_scan_documents WHERE scan_id = %s AND outcome = %s
                  )
                """,
                (scan_id, OUTCOME_PENDING),
            )
            count = cur.rowcount
            self._conn.commit()
        return int(count or 0)

    def catalog_scan_stats(self, scan_id: str) -> dict[str, int]:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT outcome, COUNT(*)
                FROM catalog_scan_documents
                WHERE scan_id = %s
                GROUP BY outcome
                """,
                (scan_id,),
            )
            rows = cur.fetchall()
        return {str(outcome): int(count) for outcome, count in rows}

    def catalog_scan_stubs(self, scan_id: str) -> list[dict[str, object]]:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT stub
                FROM catalog_scan_documents
                WHERE scan_id = %s AND stub IS NOT NULL
                ORDER BY page, position, doc_id
                """,
                (scan_id,),
            )
            rows = cur.fetchall()
        stubs: list[dict[str, object]] = []
        for (stub,) in rows:
            if isinstance(stub, dict):
                stubs.append(stub)
            elif stub:
                try:
                    stubs.append(json.loads(str(stub)))
                except json.JSONDecodeError:
                    continue
        return stubs
