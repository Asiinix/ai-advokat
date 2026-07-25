from __future__ import annotations

import hashlib
import threading
from pathlib import Path
from typing import Iterable

from .document import DocumentData
from .utils import now_iso


class PostgresCrawlStore:
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
                    updated_at=excluded.updated_at
                """,
                (doc_id, title, source_url, status, is_free, pages, formats_text, error, now, now),
            )
            self._conn.commit()

    def get_document_status(self, doc_id: str) -> str | None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute("SELECT status FROM documents WHERE doc_id = %s", (doc_id,))
            row = cur.fetchone()
        return str(row[0]) if row else None

    def failed_documents(self) -> list[str]:
        with self._lock, self._conn.cursor() as cur:
            cur.execute("SELECT doc_id FROM documents WHERE status = 'failed' ORDER BY updated_at")
            rows = cur.fetchall()
        return [str(row[0]) for row in rows]

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
