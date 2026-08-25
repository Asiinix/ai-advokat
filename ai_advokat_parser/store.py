from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
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
from .listing import DocumentRef
from .utils import ensure_dir, now_iso


class CrawlStore:
    storage_label = "SQLite"
    stores_document_outputs = False

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
            self._ensure_listing_progress_columns()
            self._conn.execute(
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
                    is_free INTEGER,
                    pages INTEGER,
                    formats TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._ensure_document_queue_columns()
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS listing_documents (
                    page INTEGER NOT NULL,
                    doc_id TEXT NOT NULL,
                    title TEXT NOT NULL DEFAULT '',
                    source_url TEXT NOT NULL DEFAULT '',
                    search_id TEXT NOT NULL DEFAULT '',
                    position INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (page, doc_id)
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS listing_documents_doc_id_idx ON listing_documents(doc_id)"
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS documents_status_depth_idx ON documents(status, queue_depth)")
            self._init_catalog_schema()

    def _init_catalog_schema(self) -> None:
        """Catalog scan state, kept apart from the legacy listing_* tables."""
        self._conn.execute(
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
                docs_seen INTEGER NOT NULL DEFAULT 0,
                docs_enqueued INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                started_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT
            )
            """
        )
        self._conn.execute(
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
                stub TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (scan_id, doc_id)
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS catalog_scan_documents_outcome_idx "
            "ON catalog_scan_documents(scan_id, outcome)"
        )

    def _ensure_listing_progress_columns(self) -> None:
        rows = self._conn.execute("PRAGMA table_info(listing_pages)").fetchall()
        existing = {str(row["name"]) for row in rows}
        columns = {
            "docs_status": "TEXT",
            "docs_error": "TEXT",
            "docs_started_at": "TEXT",
            "docs_finished_at": "TEXT",
        }
        for name, column_type in columns.items():
            if name not in existing:
                self._conn.execute(f"ALTER TABLE listing_pages ADD COLUMN {name} {column_type}")

    def _ensure_document_queue_columns(self) -> None:
        rows = self._conn.execute("PRAGMA table_info(documents)").fetchall()
        existing = {str(row["name"]) for row in rows}
        columns = {
            "source_system": "TEXT NOT NULL DEFAULT 'prg_zanger' CHECK (source_system = 'prg_zanger')",
            "corpus_type": "TEXT NOT NULL DEFAULT 'legal_act' CHECK (corpus_type = 'legal_act')",
            "queue_depth": "INTEGER NOT NULL DEFAULT 0",
            "locked_by": "TEXT",
            "locked_at": "TEXT",
            "attempts": "INTEGER NOT NULL DEFAULT 0",
        }
        for name, column_type in columns.items():
            if name not in existing:
                self._conn.execute(f"ALTER TABLE documents ADD COLUMN {name} {column_type}")

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

    def get_listing_page_status(self, page: int) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM listing_pages WHERE page = ?",
                (page,),
            ).fetchone()
        return str(row["status"]) if row else None

    def mark_listing_documents_status(
        self,
        page: int,
        status: str,
        error: str | None = None,
    ) -> None:
        now = now_iso()
        started_at = now if status == "processing" else None
        finished_at = now if status in {"done", "partial", "failed"} else None
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO listing_pages(
                    page, status, doc_count, total, error, updated_at,
                    docs_status, docs_error, docs_started_at, docs_finished_at
                )
                VALUES(?, 'listed', 0, NULL, NULL, ?, ?, ?, ?, ?)
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
                self._conn.execute(
                    """
                    UPDATE listing_pages
                    SET docs_status = 'done',
                        docs_finished_at = COALESCE(docs_finished_at, ?),
                        updated_at = ?
                    WHERE page <= ?
                      AND COALESCE(docs_status, '') = ''
                      AND EXISTS (
                          SELECT 1
                          FROM listing_documents
                          WHERE listing_documents.page = listing_pages.page
                      )
                    """,
                    (finished_at, now, page),
                )

    def is_listing_documents_done(self, page: int) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT docs_status FROM listing_pages WHERE page = ?",
                (page,),
            ).fetchone()
        return bool(row and row["docs_status"] == "done")

    def is_listing_documents_queued(self, page: int) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT docs_status FROM listing_pages WHERE page = ?",
                (page,),
            ).fetchone()
        return bool(row and row["docs_status"] in {"queued", "done"})

    def recommended_range_start(self, from_page: int, to_page: int) -> int:
        with self._lock:
            active = self._conn.execute(
                """
                SELECT MIN(page) AS page
                FROM listing_pages
                WHERE page BETWEEN ? AND ?
                  AND docs_status IN ('processing', 'partial', 'failed')
                """,
                (from_page, to_page),
            ).fetchone()
            if active and active["page"] is not None:
                return int(active["page"])

            legacy = self._conn.execute(
                """
                SELECT MAX(ld.page) AS page
                FROM listing_documents AS ld
                LEFT JOIN listing_pages AS lp ON lp.page = ld.page
                WHERE ld.page BETWEEN ? AND ?
                  AND COALESCE(lp.docs_status, '') != 'done'
                """,
                (from_page, to_page),
            ).fetchone()
            if legacy and legacy["page"] is not None:
                return int(legacy["page"])

            completed = self._conn.execute(
                """
                SELECT MAX(page) AS page
                FROM listing_pages
                WHERE page BETWEEN ? AND ?
                  AND docs_status = 'done'
                """,
                (from_page, to_page),
            ).fetchone()
            if completed and completed["page"] is not None:
                next_page = int(completed["page"]) + 1
                if next_page <= to_page:
                    return max(from_page, next_page)
        return from_page

    def recommended_enqueue_start(self, from_page: int, to_page: int) -> int:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT page, docs_status
                FROM listing_pages
                WHERE page BETWEEN ? AND ?
                """,
                (from_page, to_page),
            ).fetchall()
        statuses = {int(row["page"]): row["docs_status"] for row in rows}
        for page in range(from_page, to_page + 1):
            if statuses.get(page) not in {"queued", "done"}:
                return page
        return to_page + 1

    def save_listing_documents(self, page: int, refs: Iterable[DocumentRef]) -> None:
        now = now_iso()
        refs = list(refs)
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM listing_documents WHERE page = ?", (page,))
            self._conn.executemany(
                """
                INSERT INTO listing_documents(page, doc_id, title, source_url, search_id, position, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (page, ref.doc_id, ref.title, ref.source_url, ref.search_id, index, now)
                    for index, ref in enumerate(refs)
                ],
            )

    def get_listing_documents(self, page: int) -> list[DocumentRef]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT doc_id, title, source_url, search_id
                FROM listing_documents
                WHERE page = ?
                ORDER BY position
                """,
                (page,),
            ).fetchall()
        return [
            DocumentRef(
                doc_id=str(row["doc_id"]),
                title=str(row["title"] or ""),
                source_url=str(row["source_url"] or ""),
                search_id=str(row["search_id"] or ""),
            )
            for row in rows
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
                    locked_by=CASE WHEN excluded.status = 'processing' THEN documents.locked_by ELSE NULL END,
                    locked_at=CASE WHEN excluded.status = 'processing' THEN documents.locked_at ELSE NULL END,
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
        with self._lock, self._conn:
            for ref in refs:
                row = self._conn.execute(
                    """
                    SELECT status, is_free, error, queue_depth
                    FROM documents
                    WHERE doc_id = ?
                    """,
                    (ref.doc_id,),
                ).fetchone()
                if row:
                    status = str(row["status"])
                    error = str(row["error"] or "").lower()
                    terminal_failed = row["is_free"] == 0 or "not marked as free" in error
                    outputs_ready = False
                    if status == "exported":
                        doc_dir = self.out_dir / "documents" / ref.doc_id
                        mapping = {
                            "html": doc_dir / "document.html",
                            "txt": doc_dir / "document.txt",
                            "json": doc_dir / "document.json",
                            "pdf": doc_dir / "document.pdf",
                        }
                        outputs_ready = all(mapping[fmt].exists() for fmt in required_formats)
                    skip_failed = terminal_failed and not retry_failed
                    if not force and (outputs_ready or status == "processing" or (status == "failed" and skip_failed)):
                        self._conn.execute(
                            """
                            UPDATE documents
                            SET title=COALESCE(NULLIF(?, ''), title),
                                source_url=COALESCE(NULLIF(?, ''), source_url),
                                queue_depth=MIN(queue_depth, ?),
                                updated_at=?
                            WHERE doc_id = ?
                            """,
                            (ref.title, ref.source_url, depth, now, ref.doc_id),
                        )
                        continue

                    was_queued = status == "queued"
                    self._conn.execute(
                        """
                        UPDATE documents
                        SET title=COALESCE(NULLIF(?, ''), title),
                            source_url=COALESCE(NULLIF(?, ''), source_url),
                            status='queued',
                            queue_depth=MIN(queue_depth, ?),
                            error=NULL,
                            locked_by=NULL,
                            locked_at=NULL,
                            updated_at=?
                        WHERE doc_id = ?
                        """,
                        (ref.title, ref.source_url, depth, now, ref.doc_id),
                    )
                    if not was_queued:
                        added += 1
                    continue

                self._conn.execute(
                    """
                    INSERT INTO documents(
                        doc_id, title, source_url, status, is_free, pages, formats, error,
                        created_at, updated_at, queue_depth, locked_by, locked_at, attempts
                    )
                    VALUES(?, ?, ?, 'queued', NULL, NULL, '', NULL, ?, ?, ?, NULL, NULL, 0)
                    """,
                    (ref.doc_id, ref.title, ref.source_url, now, now, depth),
                )
                added += 1
        return added

    def claim_queued_document(self, worker_id: str) -> tuple[DocumentRef, int] | None:
        now = now_iso()
        with self._lock, self._conn:
            row = self._conn.execute(
                """
                SELECT doc_id, title, source_url, queue_depth
                FROM documents
                WHERE status = 'queued'
                ORDER BY queue_depth, updated_at, doc_id
                LIMIT 1
                """
            ).fetchone()
            if not row:
                return None
            self._conn.execute(
                """
                UPDATE documents
                SET status='processing',
                    locked_by=?,
                    locked_at=?,
                    attempts=attempts + 1,
                    updated_at=?
                WHERE doc_id = ?
                """,
                (worker_id, now, now, row["doc_id"]),
            )
        return (
            DocumentRef(
                doc_id=str(row["doc_id"]),
                title=str(row["title"] or ""),
                source_url=str(row["source_url"] or ""),
            ),
            int(row["queue_depth"] or 0),
        )

    def requeue_stale_documents(self, lease_seconds: int) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=lease_seconds)).replace(microsecond=0).isoformat()
        now = now_iso()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE documents
                SET status='queued',
                    locked_by=NULL,
                    locked_at=NULL,
                    updated_at=?
                WHERE status = 'processing'
                  AND (locked_at IS NULL OR locked_at < ?)
                """,
                (now, cutoff),
            )
            return int(cursor.rowcount or 0)

    def queue_stats(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM documents
                WHERE status IN ('queued', 'processing', 'exported', 'failed')
                GROUP BY status
                """
            ).fetchall()
        return {str(row["status"]): int(row["count"]) for row in rows}

    def get_document_status(self, doc_id: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM documents WHERE doc_id = ?",
                (doc_id,),
            ).fetchone()
        return str(row["status"]) if row else None

    def is_terminal_document_failure(self, doc_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT status, is_free, error FROM documents WHERE doc_id = ?",
                (doc_id,),
            ).fetchone()
        if not row or row["status"] != "failed":
            return False
        error = str(row["error"] or "").lower()
        return row["is_free"] == 0 or "not marked as free" in error

    def failed_documents(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT doc_id FROM documents WHERE status = 'failed' ORDER BY updated_at"
            ).fetchall()
        return [str(row["doc_id"]) for row in rows]

    def claim_failed_document_without_title(
        self,
        worker_id: str,
        lease_seconds: int,
    ) -> DocumentRef | None:
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=lease_seconds)).replace(microsecond=0).isoformat()
        now = now_iso()
        with self._lock, self._conn:
            row = self._conn.execute(
                """
                SELECT doc_id, source_url
                FROM documents
                WHERE status = 'failed'
                  AND COALESCE(title, '') = ''
                  AND (
                      locked_at IS NULL
                      OR locked_by IS NULL
                      OR locked_by NOT LIKE 'failed-title:%'
                      OR locked_at < ?
                  )
                ORDER BY updated_at, doc_id
                LIMIT 1
                """,
                (cutoff,),
            ).fetchone()
            if not row:
                return None
            self._conn.execute(
                """
                UPDATE documents
                SET locked_by=?,
                    locked_at=?,
                    updated_at=?
                WHERE doc_id = ?
                  AND status = 'failed'
                  AND COALESCE(title, '') = ''
                """,
                (worker_id, now, now, row["doc_id"]),
            )
        return DocumentRef(doc_id=str(row["doc_id"]), source_url=str(row["source_url"] or ""))

    def update_failed_document_title(
        self,
        doc_id: str,
        title: str,
        is_free: bool | None = None,
        pages: int | None = None,
    ) -> None:
        now = now_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE documents
                SET title=COALESCE(NULLIF(?, ''), title),
                    is_free=COALESCE(?, is_free),
                    pages=COALESCE(?, pages),
                    locked_by=NULL,
                    locked_at=NULL,
                    updated_at=?
                WHERE doc_id = ?
                  AND status = 'failed'
                """,
                (title, None if is_free is None else int(is_free), pages, now, doc_id),
            )

    def defer_failed_title_enrichment(self, doc_id: str) -> None:
        now = now_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE documents
                SET locked_at=?,
                    updated_at=?
                WHERE doc_id = ?
                  AND status = 'failed'
                """,
                (now, now, doc_id),
            )

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

    def has_document_outputs(self, doc_id: str, formats: Iterable[str]) -> bool:
        doc_dir = self.out_dir / "documents" / doc_id
        mapping = {
            "html": doc_dir / "document.html",
            "txt": doc_dir / "document.txt",
            "json": doc_dir / "document.json",
            "pdf": doc_dir / "document.pdf",
        }
        return all(mapping[fmt].exists() for fmt in formats)

    def get_document_links(self, doc_id: str) -> list[str]:
        meta_path = self.out_dir / "documents" / doc_id / "meta.json"
        if not meta_path.exists():
            return []
        try:
            data = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        links = data.get("linked_doc_ids") or []
        return [str(item) for item in links if str(item).isdigit()]

    def save_document_outputs(self, document: object, paths: dict[str, Path]) -> None:
        return None

    # --- catalog scan -----------------------------------------------------

    def _catalog_scan_from_row(self, row: sqlite3.Row) -> CatalogScanState:
        return CatalogScanState(
            scan_id=str(row["scan_id"]),
            list_url=str(row["list_url"]),
            product=str(row["product"]),
            formats=parse_format_list(row["formats"]),
            phase=str(row["phase"]),
            total_documents=None if row["total_documents"] is None else int(row["total_documents"]),
            page_size=None if row["page_size"] is None else int(row["page_size"]),
            total_pages=None if row["total_pages"] is None else int(row["total_pages"]),
            next_page=int(row["next_page"] or 1),
            pages_done=int(row["pages_done"] or 0),
            docs_seen=int(row["docs_seen"] or 0),
            docs_enqueued=int(row["docs_enqueued"] or 0),
            error=None if row["error"] is None else str(row["error"]),
            started_at=str(row["started_at"]),
            updated_at=str(row["updated_at"]),
            completed_at=None if row["completed_at"] is None else str(row["completed_at"]),
        )

    def get_catalog_scan(self, scan_id: str) -> CatalogScanState | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM catalog_scans WHERE scan_id = ?",
                (scan_id,),
            ).fetchone()
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
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO catalog_scans(
                    scan_id, list_url, product, formats, phase, next_page,
                    pages_done, docs_seen, docs_enqueued, started_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, 1, 0, 0, 0, ?, ?)
                ON CONFLICT(scan_id) DO NOTHING
                """,
                (scan_id, list_url, product, formats_text, PHASE_PENDING, now, now),
            )
            row = self._conn.execute(
                "SELECT * FROM catalog_scans WHERE scan_id = ?",
                (scan_id,),
            ).fetchone()
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
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE catalog_scans
                SET total_documents=?, page_size=?, total_pages=?, updated_at=?
                WHERE scan_id = ?
                """,
                (total_documents, page_size, total_pages, now_iso(), scan_id),
            )

    def set_catalog_scan_phase(self, scan_id: str, phase: str, error: str | None = None) -> None:
        now = now_iso()
        completed_at = now if phase == PHASE_COMPLETED else None
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE catalog_scans
                SET phase=?,
                    error=?,
                    completed_at=CASE WHEN ? IS NULL THEN completed_at ELSE ? END,
                    updated_at=?
                WHERE scan_id = ?
                """,
                (phase, error, completed_at, completed_at, now, scan_id),
            )

    def record_catalog_page(self, scan_id: str, page: int, refs: Iterable[DocumentRef]) -> int:
        """Store the membership of one listing page without touching outcomes."""
        now = now_iso()
        rows = [
            (scan_id, ref.doc_id, page, index, ref.title, ref.source_url, OUTCOME_PENDING, now)
            for index, ref in enumerate(refs)
        ]
        if not rows:
            return 0
        with self._lock, self._conn:
            self._conn.executemany(
                """
                INSERT INTO catalog_scan_documents(
                    scan_id, doc_id, page, position, title, source_url, outcome, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(scan_id, doc_id) DO UPDATE SET
                    page=excluded.page,
                    position=excluded.position,
                    title=COALESCE(NULLIF(excluded.title, ''), catalog_scan_documents.title),
                    source_url=COALESCE(NULLIF(excluded.source_url, ''), catalog_scan_documents.source_url),
                    updated_at=excluded.updated_at
                """,
                rows,
            )
        return len(rows)

    def advance_catalog_scan(self, scan_id: str, next_page: int, docs_enqueued: int = 0) -> None:
        """Move the resume cursor forward; replaying a page never moves it back."""
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE catalog_scans
                SET docs_enqueued=docs_enqueued + CASE WHEN ? > next_page THEN ? ELSE 0 END,
                    next_page=MAX(next_page, ?),
                    pages_done=MAX(pages_done, ? - 1),
                    docs_seen=(SELECT COUNT(*) FROM catalog_scan_documents WHERE scan_id = ?),
                    updated_at=?
                WHERE scan_id = ?
                """,
                (next_page, docs_enqueued, next_page, next_page, scan_id, now_iso(), scan_id),
            )

    def is_catalog_scan_member(self, scan_id: str, doc_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM catalog_scan_documents WHERE scan_id = ? AND doc_id = ?",
                (scan_id, doc_id),
            ).fetchone()
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
        with self._lock, self._conn:
            row = self._conn.execute(
                """
                SELECT page, position, title, source_url
                FROM catalog_scan_documents
                WHERE scan_id = ? AND doc_id = ?
                """,
                (scan_id, doc_id),
            ).fetchone()
            if row is None:
                return None
            stub = None
            if outcome != OUTCOME_DONE:
                stub = build_stub(
                    scan_id=scan_id,
                    doc_id=doc_id,
                    outcome=outcome,
                    page=int(row["page"]),
                    position=int(row["position"] or 0),
                    title=str(row["title"] or ""),
                    source_url=str(row["source_url"] or ""),
                    failure_kind=failure_kind,
                    http_status=http_status,
                    detail=detail,
                    recorded_at=now,
                )
            self._conn.execute(
                """
                UPDATE catalog_scan_documents
                SET outcome=?, failure_kind=?, http_status=?, stub=?, updated_at=?
                WHERE scan_id = ? AND doc_id = ?
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
        return stub

    def resolve_catalog_scan_outcomes(self, scan_id: str) -> int:
        """Close out members that another run already exported."""
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE catalog_scan_documents
                SET outcome=?, failure_kind=NULL, http_status=NULL, stub=NULL, updated_at=?
                WHERE scan_id = ?
                  AND outcome = ?
                  AND doc_id IN (SELECT doc_id FROM documents WHERE status = 'exported')
                """,
                (OUTCOME_DONE, now_iso(), scan_id, OUTCOME_PENDING),
            )
            return int(cursor.rowcount or 0)

    def pending_catalog_document_count(self, scan_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS count FROM catalog_scan_documents WHERE scan_id = ? AND outcome = ?",
                (scan_id, OUTCOME_PENDING),
            ).fetchone()
        return int(row["count"] or 0)

    def reclaim_catalog_scan_documents(self, scan_id: str) -> int:
        """Return this scan's documents left in processing by a dead container."""
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE documents
                SET status='queued', locked_by=NULL, locked_at=NULL, updated_at=?
                WHERE status = 'processing'
                  AND doc_id IN (
                      SELECT doc_id FROM catalog_scan_documents WHERE scan_id = ? AND outcome = ?
                  )
                """,
                (now_iso(), scan_id, OUTCOME_PENDING),
            )
            return int(cursor.rowcount or 0)

    def catalog_scan_stats(self, scan_id: str) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT outcome, COUNT(*) AS count
                FROM catalog_scan_documents
                WHERE scan_id = ?
                GROUP BY outcome
                """,
                (scan_id,),
            ).fetchall()
        return {str(row["outcome"]): int(row["count"]) for row in rows}

    def catalog_scan_stubs(self, scan_id: str) -> list[dict[str, object]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT stub
                FROM catalog_scan_documents
                WHERE scan_id = ? AND stub IS NOT NULL
                ORDER BY page, position, doc_id
                """,
                (scan_id,),
            ).fetchall()
        stubs: list[dict[str, object]] = []
        for row in rows:
            try:
                stubs.append(json.loads(str(row["stub"])))
            except json.JSONDecodeError:
                continue
        return stubs
