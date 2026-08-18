from __future__ import annotations

import hashlib
import threading
from pathlib import Path
from typing import Iterable

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
            self._conn.commit()

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

                    if not force and (outputs_ready or status == "processing" or (status == "failed" and terminal_failed)):
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
