from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Iterable

from .utils import ensure_dir, now_iso


class CrawlStore:
    def __init__(self, out_dir: str | Path) -> None:
        self.out_dir = ensure_dir(out_dir)
        self.path = self.out_dir / "state.sqlite3"
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS listing_pages (
                    page INTEGER PRIMARY KEY,
                    status TEXT NOT NULL,
                    doc_count INTEGER NOT NULL DEFAULT 0,
                    total INTEGER,
                    error TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    doc_id TEXT PRIMARY KEY,
                    title TEXT,
                    source_url TEXT,
                    status TEXT NOT NULL,
                    is_free INTEGER,
                    pages INTEGER,
                    formats TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
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
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO listing_pages(page, status, doc_count, total, error, updated_at)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(page) DO UPDATE SET
                    status=excluded.status,
                    doc_count=excluded.doc_count,
                    total=excluded.total,
                    error=excluded.error,
                    updated_at=excluded.updated_at
                """,
                (page, status, doc_count, total, error, now_iso()),
            )

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
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO documents(
                    doc_id, title, source_url, status, is_free, pages, formats, error, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                (
                    doc_id,
                    title,
                    source_url,
                    status,
                    None if is_free is None else int(is_free),
                    pages,
                    formats_text,
                    error,
                    now,
                    now,
                ),
            )

    def get_document_status(self, doc_id: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM documents WHERE doc_id = ?",
                (doc_id,),
            ).fetchone()
        return str(row["status"]) if row else None

    def failed_documents(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT doc_id FROM documents WHERE status = 'failed' ORDER BY updated_at"
            ).fetchall()
        return [str(row["doc_id"]) for row in rows]

    def stats(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS count FROM documents GROUP BY status"
            ).fetchall()
        return {str(row["status"]): int(row["count"]) for row in rows}

    def listing_stats(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS count FROM listing_pages GROUP BY status"
            ).fetchall()
        return {str(row["status"]): int(row["count"]) for row in rows}
