"""SQLite state for the PRG.SOT corpus.

Physically separate from the PRG.ZANGER state: a different file
(``sot_state.sqlite3``) and a different table family (``sot_*``). Nothing here
reads or writes ``documents``/``document_outputs``, and every decision key is
namespaced with ``prg_sot:`` so it can never be mistaken for a ``doc_id``.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from ..utils import ensure_dir, now_iso
from . import CORPUS_TYPE, SOURCE_SYSTEM
from .model import (
    OUTCOME_DONE,
    OUTCOME_PENDING,
    PHASE_COMPLETED,
    PHASE_PENDING,
    RETRYABLE_OUTCOMES,
    STATUS_EXPORTED,
    STATUS_FAILED,
    STATUS_PROCESSING,
    STATUS_QUEUED,
    SotDecisionPayload,
    SotDecisionRef,
    SotScanState,
    build_stub,
)

DECISION_FORMATS = ("txt", "json")
METADATA_COLUMNS = (
    "case_number",
    "court",
    "judge",
    "region",
    "instance",
    "proceeding_type",
    "decision_date",
    "title",
)


def decision_columns(metadata) -> dict[str, str | None]:
    """Project the adapter metadata onto the fixed court columns."""
    values = dict(metadata or {})
    return {name: (str(values[name]).strip() or None) if values.get(name) not in (None, "") else None for name in METADATA_COLUMNS}


def parties_json(metadata) -> str | None:
    parties = dict(metadata or {}).get("parties")
    if parties in (None, ""):
        return None
    return json.dumps(parties, ensure_ascii=False)


def metadata_json(metadata) -> str:
    return json.dumps(dict(metadata or {}), ensure_ascii=False, sort_keys=True)


class SotStore:
    storage_label = "SQLite"

    def __init__(self, out_dir: str | Path) -> None:
        self.out_dir = ensure_dir(out_dir)
        self.path = self.out_dir / "sot_state.sqlite3"
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # --- schema -----------------------------------------------------------

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sot_scans (
                    scan_id TEXT PRIMARY KEY,
                    source_system TEXT NOT NULL DEFAULT 'prg_sot'
                        CHECK (source_system = 'prg_sot'),
                    corpus_type TEXT NOT NULL DEFAULT 'judicial_decision'
                        CHECK (corpus_type = 'judicial_decision'),
                    config_fingerprint TEXT NOT NULL,
                    query TEXT NOT NULL DEFAULT '',
                    phase TEXT NOT NULL,
                    total_decisions INTEGER,
                    page_size INTEGER,
                    total_pages INTEGER,
                    next_page INTEGER NOT NULL DEFAULT 1,
                    next_cursor TEXT,
                    pages_done INTEGER NOT NULL DEFAULT 0,
                    decisions_seen INTEGER NOT NULL DEFAULT 0,
                    decisions_enqueued INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    rate_limit_note TEXT,
                    started_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sot_decisions (
                    decision_key TEXT PRIMARY KEY,
                    decision_id TEXT NOT NULL,
                    source_system TEXT NOT NULL DEFAULT 'prg_sot'
                        CHECK (source_system = 'prg_sot'),
                    corpus_type TEXT NOT NULL DEFAULT 'judicial_decision'
                        CHECK (corpus_type = 'judicial_decision'),
                    case_number TEXT,
                    court TEXT,
                    judge TEXT,
                    region TEXT,
                    instance TEXT,
                    proceeding_type TEXT,
                    decision_date TEXT,
                    title TEXT,
                    parties TEXT,
                    metadata TEXT,
                    source_url TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    locked_by TEXT,
                    locked_at TEXT,
                    text_sha256 TEXT,
                    raw_sha256 TEXT,
                    text_chars INTEGER,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sot_decision_outputs (
                    decision_key TEXT NOT NULL
                        REFERENCES sot_decisions(decision_key) ON DELETE CASCADE,
                    format TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    encoding TEXT,
                    content BLOB NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (decision_key, format)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sot_scan_decisions (
                    scan_id TEXT NOT NULL,
                    decision_key TEXT NOT NULL,
                    decision_id TEXT NOT NULL,
                    page INTEGER NOT NULL,
                    position INTEGER NOT NULL DEFAULT 0,
                    outcome TEXT NOT NULL DEFAULT 'pending',
                    failure_kind TEXT,
                    http_status INTEGER,
                    stub TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (scan_id, decision_key)
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS sot_scan_decisions_outcome_idx "
                "ON sot_scan_decisions(scan_id, outcome)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS sot_decisions_status_idx ON sot_decisions(status, locked_at)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS sot_decisions_court_date_idx ON sot_decisions(court, decision_date)"
            )
            self._ensure_columns()

    def _ensure_columns(self) -> None:
        """Migration-safe additions for databases created by an older build."""
        wanted = {
            "sot_decisions": {
                "judge": "TEXT",
                "region": "TEXT",
                "instance": "TEXT",
                "proceeding_type": "TEXT",
                "decision_date": "TEXT",
                "parties": "TEXT",
                "metadata": "TEXT",
                "text_sha256": "TEXT",
                "raw_sha256": "TEXT",
                "text_chars": "INTEGER",
            },
            "sot_scans": {"next_cursor": "TEXT", "rate_limit_note": "TEXT"},
        }
        for table, columns in wanted.items():
            rows = self._conn.execute(f"PRAGMA table_info({table})").fetchall()
            existing = {str(row["name"]) for row in rows}
            for name, column_type in columns.items():
                if name not in existing:
                    self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {column_type}")

    # --- scan state -------------------------------------------------------

    def _scan_from_row(self, row: sqlite3.Row) -> SotScanState:
        return SotScanState(
            scan_id=str(row["scan_id"]),
            source_system=str(row["source_system"]),
            corpus_type=str(row["corpus_type"]),
            config_fingerprint=str(row["config_fingerprint"]),
            query=str(row["query"] or ""),
            phase=str(row["phase"]),
            total_decisions=None if row["total_decisions"] is None else int(row["total_decisions"]),
            page_size=None if row["page_size"] is None else int(row["page_size"]),
            total_pages=None if row["total_pages"] is None else int(row["total_pages"]),
            next_page=int(row["next_page"] or 1),
            next_cursor=None if row["next_cursor"] is None else str(row["next_cursor"]),
            pages_done=int(row["pages_done"] or 0),
            decisions_seen=int(row["decisions_seen"] or 0),
            decisions_enqueued=int(row["decisions_enqueued"] or 0),
            error=None if row["error"] is None else str(row["error"]),
            rate_limit_note=None if row["rate_limit_note"] is None else str(row["rate_limit_note"]),
            started_at=str(row["started_at"]),
            updated_at=str(row["updated_at"]),
            completed_at=None if row["completed_at"] is None else str(row["completed_at"]),
        )

    def get_scan(self, scan_id: str) -> SotScanState | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM sot_scans WHERE scan_id = ?", (scan_id,)).fetchone()
        return self._scan_from_row(row) if row else None

    def ensure_scan(
        self,
        scan_id: str,
        config_fingerprint: str,
        query: str,
        first_page: int = 1,
    ) -> SotScanState:
        """Create the scan row once and refuse to reuse an id with another contract."""
        now = now_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO sot_scans(
                    scan_id, source_system, corpus_type, config_fingerprint, query, phase,
                    next_page, pages_done, decisions_seen, decisions_enqueued, started_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?)
                ON CONFLICT(scan_id) DO NOTHING
                """,
                (scan_id, SOURCE_SYSTEM, CORPUS_TYPE, config_fingerprint, query, PHASE_PENDING, first_page, now, now),
            )
            row = self._conn.execute("SELECT * FROM sot_scans WHERE scan_id = ?", (scan_id,)).fetchone()
        state = self._scan_from_row(row)
        if state.config_fingerprint != config_fingerprint:
            raise ValueError(
                f"SOT scan '{scan_id}' was started against a different source contract. "
                "A changed search/decision template means different data: use a new --scan-id."
            )
        return state

    def set_scan_discovery(
        self,
        scan_id: str,
        total_decisions: int | None,
        page_size: int,
        total_pages: int | None,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE sot_scans
                SET total_decisions=?, page_size=?, total_pages=?, updated_at=?
                WHERE scan_id = ?
                """,
                (total_decisions, page_size, total_pages, now_iso(), scan_id),
            )

    def set_scan_phase(
        self,
        scan_id: str,
        phase: str,
        error: str | None = None,
        rate_limit_note: str | None = None,
    ) -> None:
        now = now_iso()
        completed_at = now if phase == PHASE_COMPLETED else None
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE sot_scans
                SET phase=?,
                    error=?,
                    rate_limit_note=?,
                    completed_at=CASE WHEN ? IS NULL THEN completed_at ELSE ? END,
                    updated_at=?
                WHERE scan_id = ?
                """,
                (phase, error, rate_limit_note, completed_at, completed_at, now, scan_id),
            )

    def advance_scan(
        self,
        scan_id: str,
        next_page: int,
        next_cursor: str | None = None,
        decisions_enqueued: int = 0,
    ) -> None:
        """Move the resume cursor forward; replaying a page never moves it back."""
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE sot_scans
                SET decisions_enqueued=decisions_enqueued + CASE WHEN ? > next_page THEN ? ELSE 0 END,
                    next_page=MAX(next_page, ?),
                    next_cursor=?,
                    pages_done=MAX(pages_done, ? - 1),
                    decisions_seen=(SELECT COUNT(*) FROM sot_scan_decisions WHERE scan_id = ?),
                    updated_at=?
                WHERE scan_id = ?
                """,
                (
                    next_page,
                    decisions_enqueued,
                    next_page,
                    next_cursor,
                    next_page,
                    scan_id,
                    now_iso(),
                    scan_id,
                ),
            )

    # --- membership and queue --------------------------------------------

    def record_search_page(self, scan_id: str, page: int, refs: Iterable[SotDecisionRef]) -> int:
        """Store one search page: membership plus a queued decision per ref."""
        items = list(refs)
        if not items:
            return 0
        now = now_iso()
        with self._lock, self._conn:
            for ref in items:
                columns = decision_columns(ref.metadata)
                self._conn.execute(
                    """
                    INSERT INTO sot_decisions(
                        decision_key, decision_id, source_system, corpus_type,
                        case_number, court, judge, region, instance, proceeding_type,
                        decision_date, title, parties, metadata, source_url,
                        status, created_at, updated_at
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(decision_key) DO UPDATE SET
                        case_number=COALESCE(excluded.case_number, sot_decisions.case_number),
                        court=COALESCE(excluded.court, sot_decisions.court),
                        judge=COALESCE(excluded.judge, sot_decisions.judge),
                        region=COALESCE(excluded.region, sot_decisions.region),
                        instance=COALESCE(excluded.instance, sot_decisions.instance),
                        proceeding_type=COALESCE(excluded.proceeding_type, sot_decisions.proceeding_type),
                        decision_date=COALESCE(excluded.decision_date, sot_decisions.decision_date),
                        title=COALESCE(excluded.title, sot_decisions.title),
                        parties=COALESCE(excluded.parties, sot_decisions.parties),
                        source_url=COALESCE(NULLIF(excluded.source_url, ''), sot_decisions.source_url),
                        updated_at=excluded.updated_at
                    """,
                    (
                        ref.decision_key,
                        ref.decision_id,
                        SOURCE_SYSTEM,
                        CORPUS_TYPE,
                        columns["case_number"],
                        columns["court"],
                        columns["judge"],
                        columns["region"],
                        columns["instance"],
                        columns["proceeding_type"],
                        columns["decision_date"],
                        columns["title"],
                        parties_json(ref.metadata),
                        metadata_json(ref.metadata),
                        ref.source_url,
                        STATUS_QUEUED,
                        now,
                        now,
                    ),
                )
                self._conn.execute(
                    """
                    INSERT INTO sot_scan_decisions(
                        scan_id, decision_key, decision_id, page, position, outcome, updated_at
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(scan_id, decision_key) DO UPDATE SET
                        page=excluded.page,
                        position=excluded.position,
                        updated_at=excluded.updated_at
                    """,
                    (scan_id, ref.decision_key, ref.decision_id, page, ref.position, OUTCOME_PENDING, now),
                )
        return len(items)

    def _ref_from_row(self, row: sqlite3.Row) -> SotDecisionRef:
        try:
            metadata = json.loads(str(row["metadata"] or "{}"))
        except json.JSONDecodeError:
            metadata = {}
        return SotDecisionRef(
            decision_id=str(row["decision_id"]),
            decision_key=str(row["decision_key"]),
            source_url=str(row["source_url"] or ""),
            page=int(row["page"] or 0),
            position=int(row["position"] or 0),
            metadata=metadata,
        )

    def claim_decision(self, scan_id: str, worker_id: str) -> SotDecisionRef | None:
        """Take one queued member of this scan under a lease."""
        now = now_iso()
        with self._lock, self._conn:
            row = self._conn.execute(
                """
                SELECT d.decision_key, d.decision_id, d.source_url, d.metadata, m.page, m.position
                FROM sot_scan_decisions AS m
                JOIN sot_decisions AS d ON d.decision_key = m.decision_key
                WHERE m.scan_id = ? AND m.outcome = ? AND d.status = ?
                ORDER BY m.page, m.position, m.decision_key
                LIMIT 1
                """,
                (scan_id, OUTCOME_PENDING, STATUS_QUEUED),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                """
                UPDATE sot_decisions
                SET status=?, attempts=attempts + 1, locked_by=?, locked_at=?, updated_at=?
                WHERE decision_key = ?
                """,
                (STATUS_PROCESSING, worker_id, now, now, str(row["decision_key"])),
            )
        return self._ref_from_row(row)

    def release_decision(self, decision_key: str) -> None:
        """Hand a claimed decision back untouched (fatal auth or quota stop)."""
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE sot_decisions
                SET status=?, locked_by=NULL, locked_at=NULL, attempts=MAX(0, attempts - 1), updated_at=?
                WHERE decision_key = ? AND status = ?
                """,
                (STATUS_QUEUED, now_iso(), decision_key, STATUS_PROCESSING),
            )

    def requeue_stale_decisions(self, scan_id: str, lease_seconds: int) -> int:
        """Return decisions a crashed worker left in processing."""
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=lease_seconds)).replace(microsecond=0).isoformat()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE sot_decisions
                SET status=?, locked_by=NULL, locked_at=NULL, updated_at=?
                WHERE status = ?
                  AND (locked_at IS NULL OR locked_at < ?)
                  AND decision_key IN (
                      SELECT decision_key FROM sot_scan_decisions WHERE scan_id = ? AND outcome = ?
                  )
                """,
                (STATUS_QUEUED, now_iso(), STATUS_PROCESSING, cutoff, scan_id, OUTCOME_PENDING),
            )
            return int(cursor.rowcount or 0)

    # --- outputs ----------------------------------------------------------

    def has_decision_outputs(self, decision_key: str, formats: Iterable[str]) -> bool:
        wanted = tuple(formats)
        if not wanted:
            return False
        placeholders = ",".join("?" for _ in wanted)
        with self._lock:
            row = self._conn.execute(
                f"""
                SELECT COUNT(*) AS count FROM sot_decision_outputs
                WHERE decision_key = ? AND format IN ({placeholders})
                """,
                (decision_key, *wanted),
            ).fetchone()
        return int(row["count"] or 0) >= len(set(wanted))

    def is_decision_complete(self, decision_key: str, formats: Iterable[str]) -> bool:
        return self.decision_status(decision_key) == STATUS_EXPORTED and self.has_decision_outputs(
            decision_key, formats
        )

    def save_decision(self, payload: SotDecisionPayload, formats: Iterable[str] = DECISION_FORMATS) -> None:
        """Persist text/raw outputs and mark the decision exported."""
        from .adapter import sha256_text

        wanted = tuple(formats)
        raw_json = json.dumps(payload.raw, ensure_ascii=False)
        bodies = {
            "txt": (payload.text, "text/plain"),
            "json": (raw_json, "application/json"),
        }
        now = now_iso()
        columns = decision_columns(payload.metadata)
        with self._lock, self._conn:
            for name in wanted:
                if name not in bodies:
                    raise ValueError(f"Unsupported SOT output format '{name}'.")
                body, content_type = bodies[name]
                blob = body.encode("utf-8")
                self._conn.execute(
                    """
                    INSERT INTO sot_decision_outputs(
                        decision_key, format, content_type, encoding, content,
                        size_bytes, sha256, created_at, updated_at
                    )
                    VALUES(?, ?, ?, 'utf-8', ?, ?, ?, ?, ?)
                    ON CONFLICT(decision_key, format) DO UPDATE SET
                        content_type=excluded.content_type,
                        encoding=excluded.encoding,
                        content=excluded.content,
                        size_bytes=excluded.size_bytes,
                        sha256=excluded.sha256,
                        updated_at=excluded.updated_at
                    """,
                    (
                        payload.decision_key,
                        name,
                        content_type,
                        blob,
                        len(blob),
                        sha256_text(body),
                        now,
                        now,
                    ),
                )
            self._conn.execute(
                """
                UPDATE sot_decisions
                SET status=?,
                    case_number=COALESCE(?, case_number),
                    court=COALESCE(?, court),
                    judge=COALESCE(?, judge),
                    region=COALESCE(?, region),
                    instance=COALESCE(?, instance),
                    proceeding_type=COALESCE(?, proceeding_type),
                    decision_date=COALESCE(?, decision_date),
                    title=COALESCE(?, title),
                    parties=COALESCE(?, parties),
                    metadata=?,
                    source_url=COALESCE(NULLIF(?, ''), source_url),
                    text_sha256=?,
                    raw_sha256=?,
                    text_chars=?,
                    error=NULL,
                    locked_by=NULL,
                    locked_at=NULL,
                    updated_at=?
                WHERE decision_key = ?
                """,
                (
                    STATUS_EXPORTED,
                    columns["case_number"],
                    columns["court"],
                    columns["judge"],
                    columns["region"],
                    columns["instance"],
                    columns["proceeding_type"],
                    columns["decision_date"],
                    columns["title"],
                    parties_json(payload.metadata),
                    metadata_json(payload.metadata),
                    payload.source_url,
                    sha256_text(payload.text),
                    sha256_text(raw_json),
                    len(payload.text),
                    now,
                    payload.decision_key,
                ),
            )

    def mark_decision_failed(self, decision_key: str, error: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE sot_decisions
                SET status=?, error=?, locked_by=NULL, locked_at=NULL, updated_at=?
                WHERE decision_key = ?
                """,
                (STATUS_FAILED, error[:2000], now_iso(), decision_key),
            )

    def decision_status(self, decision_key: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM sot_decisions WHERE decision_key = ?",
                (decision_key,),
            ).fetchone()
        return str(row["status"]) if row else None

    def get_decision(self, decision_key: str) -> dict[str, object] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM sot_decisions WHERE decision_key = ?",
                (decision_key,),
            ).fetchone()
        return {key: row[key] for key in row.keys()} if row else None

    def decision_stats(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS count FROM sot_decisions GROUP BY status"
            ).fetchall()
        return {str(row["status"]): int(row["count"]) for row in rows}

    # --- outcomes ---------------------------------------------------------

    def record_decision_outcome(
        self,
        scan_id: str,
        decision_key: str,
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
                SELECT m.decision_id, m.page, m.position, d.case_number, d.court, d.source_url
                FROM sot_scan_decisions AS m
                LEFT JOIN sot_decisions AS d ON d.decision_key = m.decision_key
                WHERE m.scan_id = ? AND m.decision_key = ?
                """,
                (scan_id, decision_key),
            ).fetchone()
            if row is None:
                return None
            stub = None
            if outcome != OUTCOME_DONE:
                stub = build_stub(
                    scan_id=scan_id,
                    decision_key=decision_key,
                    decision_id=str(row["decision_id"]),
                    outcome=outcome,
                    page=int(row["page"] or 0),
                    position=int(row["position"] or 0),
                    case_number=str(row["case_number"] or ""),
                    court=str(row["court"] or ""),
                    source_url=str(row["source_url"] or ""),
                    failure_kind=failure_kind,
                    http_status=http_status,
                    detail=detail,
                    recorded_at=now,
                )
            self._conn.execute(
                """
                UPDATE sot_scan_decisions
                SET outcome=?, failure_kind=?, http_status=?, stub=?, updated_at=?
                WHERE scan_id = ? AND decision_key = ?
                """,
                (
                    outcome,
                    failure_kind if stub else None,
                    http_status if stub else None,
                    json.dumps(stub, ensure_ascii=False) if stub else None,
                    now,
                    scan_id,
                    decision_key,
                ),
            )
        return stub

    def resolve_scan_outcomes(self, scan_id: str) -> int:
        """Close out members another run already exported."""
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE sot_scan_decisions
                SET outcome=?, failure_kind=NULL, http_status=NULL, stub=NULL, updated_at=?
                WHERE scan_id = ?
                  AND outcome = ?
                  AND decision_key IN (SELECT decision_key FROM sot_decisions WHERE status = ?)
                """,
                (OUTCOME_DONE, now_iso(), scan_id, OUTCOME_PENDING, STATUS_EXPORTED),
            )
            return int(cursor.rowcount or 0)

    def retry_scan_outcomes(self, scan_id: str) -> int:
        """Put failed/inaccessible/not_found members back in the queue.

        Only ever called for an explicit resume: an ordinary rerun must not keep
        re-requesting decisions the source already refused.
        """
        placeholders = ",".join("?" for _ in RETRYABLE_OUTCOMES)
        retryable = tuple(sorted(RETRYABLE_OUTCOMES))
        now = now_iso()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                f"""
                UPDATE sot_scan_decisions
                SET outcome=?, failure_kind=NULL, http_status=NULL, stub=NULL, updated_at=?
                WHERE scan_id = ? AND outcome IN ({placeholders})
                """,
                (OUTCOME_PENDING, now, scan_id, *retryable),
            )
            count = int(cursor.rowcount or 0)
            self._conn.execute(
                """
                UPDATE sot_decisions
                SET status=?, locked_by=NULL, locked_at=NULL, error=NULL, updated_at=?
                WHERE status != ?
                  AND decision_key IN (
                      SELECT decision_key FROM sot_scan_decisions WHERE scan_id = ? AND outcome = ?
                  )
                """,
                (STATUS_QUEUED, now, STATUS_EXPORTED, scan_id, OUTCOME_PENDING),
            )
        return count

    def pending_decision_count(self, scan_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS count FROM sot_scan_decisions WHERE scan_id = ? AND outcome = ?",
                (scan_id, OUTCOME_PENDING),
            ).fetchone()
        return int(row["count"] or 0)

    def scan_stats(self, scan_id: str) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT outcome, COUNT(*) AS count
                FROM sot_scan_decisions
                WHERE scan_id = ?
                GROUP BY outcome
                """,
                (scan_id,),
            ).fetchall()
        return {str(row["outcome"]): int(row["count"]) for row in rows}

    def scan_stubs(self, scan_id: str) -> list[dict[str, object]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT stub FROM sot_scan_decisions
                WHERE scan_id = ? AND stub IS NOT NULL
                ORDER BY page, position, decision_key
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
