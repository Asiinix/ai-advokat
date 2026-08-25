"""Postgres state for the PRG.SOT corpus.

Same database as the PRG.ZANGER tables, deliberately different tables. The 392k
rows in ``documents``/``document_outputs`` are never read, rewritten or migrated
by this module; the only thing the two families share is the connection string.
"""

from __future__ import annotations

import json
import threading
from typing import Iterable

from ..utils import now_iso
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
from .store import DECISION_FORMATS, decision_columns, metadata_json, parties_json


class SotPostgresStore:
    storage_label = "Postgres"

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

    # --- schema -----------------------------------------------------------

    def _init_schema(self) -> None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
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
                    total_decisions BIGINT,
                    page_size INTEGER,
                    total_pages INTEGER,
                    next_page INTEGER NOT NULL DEFAULT 1,
                    next_cursor TEXT,
                    pages_done INTEGER NOT NULL DEFAULT 0,
                    decisions_seen BIGINT NOT NULL DEFAULT 0,
                    decisions_enqueued BIGINT NOT NULL DEFAULT 0,
                    error TEXT,
                    rate_limit_note TEXT,
                    started_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL,
                    completed_at TIMESTAMPTZ
                )
                """
            )
            cur.execute(
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
                    parties JSONB,
                    metadata JSONB,
                    source_url TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    locked_by TEXT,
                    locked_at TIMESTAMPTZ,
                    text_sha256 TEXT,
                    raw_sha256 TEXT,
                    text_chars INTEGER,
                    error TEXT,
                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL
                )
                """
            )
            # Migration-safe additions for a database created by an older build.
            for name, column_type in (
                ("judge", "TEXT"),
                ("region", "TEXT"),
                ("instance", "TEXT"),
                ("proceeding_type", "TEXT"),
                ("decision_date", "TEXT"),
                ("parties", "JSONB"),
                ("metadata", "JSONB"),
                ("text_sha256", "TEXT"),
                ("raw_sha256", "TEXT"),
                ("text_chars", "INTEGER"),
            ):
                cur.execute(f"ALTER TABLE sot_decisions ADD COLUMN IF NOT EXISTS {name} {column_type}")
            cur.execute("ALTER TABLE sot_scans ADD COLUMN IF NOT EXISTS next_cursor TEXT")
            cur.execute("ALTER TABLE sot_scans ADD COLUMN IF NOT EXISTS rate_limit_note TEXT")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sot_decision_outputs (
                    decision_key TEXT NOT NULL
                        REFERENCES sot_decisions(decision_key) ON DELETE CASCADE,
                    format TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    encoding TEXT,
                    content BYTEA NOT NULL,
                    size_bytes BIGINT NOT NULL,
                    sha256 TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (decision_key, format)
                )
                """
            )
            cur.execute(
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
                    stub JSONB,
                    updated_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (scan_id, decision_key)
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS sot_scan_decisions_outcome_idx
                ON sot_scan_decisions(scan_id, outcome)
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS sot_decisions_status_idx ON sot_decisions(status, locked_at)"
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS sot_decisions_court_date_idx
                ON sot_decisions(court, decision_date)
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS sot_decisions_case_number_idx ON sot_decisions(case_number)"
            )
            self._conn.commit()

    # --- scan state -------------------------------------------------------

    @staticmethod
    def _timestamp_text(value: object) -> str:
        if value is None:
            return ""
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    def _scan_from_row(self, row: tuple) -> SotScanState:
        return SotScanState(
            scan_id=str(row[0]),
            source_system=str(row[1]),
            corpus_type=str(row[2]),
            config_fingerprint=str(row[3]),
            query=str(row[4] or ""),
            phase=str(row[5]),
            total_decisions=None if row[6] is None else int(row[6]),
            page_size=None if row[7] is None else int(row[7]),
            total_pages=None if row[8] is None else int(row[8]),
            next_page=int(row[9] or 1),
            next_cursor=None if row[10] is None else str(row[10]),
            pages_done=int(row[11] or 0),
            decisions_seen=int(row[12] or 0),
            decisions_enqueued=int(row[13] or 0),
            error=None if row[14] is None else str(row[14]),
            rate_limit_note=None if row[15] is None else str(row[15]),
            started_at=self._timestamp_text(row[16]),
            updated_at=self._timestamp_text(row[17]),
            completed_at=None if row[18] is None else self._timestamp_text(row[18]),
        )

    _SCAN_COLUMNS = """
        scan_id, source_system, corpus_type, config_fingerprint, query, phase,
        total_decisions, page_size, total_pages, next_page, next_cursor,
        pages_done, decisions_seen, decisions_enqueued, error, rate_limit_note,
        started_at, updated_at, completed_at
    """

    def get_scan(self, scan_id: str) -> SotScanState | None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(f"SELECT {self._SCAN_COLUMNS} FROM sot_scans WHERE scan_id = %s", (scan_id,))
            row = cur.fetchone()
            self._conn.commit()
        return self._scan_from_row(row) if row else None

    def ensure_scan(
        self,
        scan_id: str,
        config_fingerprint: str,
        query: str,
        first_page: int = 1,
    ) -> SotScanState:
        now = now_iso()
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sot_scans(
                    scan_id, source_system, corpus_type, config_fingerprint, query, phase,
                    next_page, pages_done, decisions_seen, decisions_enqueued, started_at, updated_at
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, 0, 0, 0, %s, %s)
                ON CONFLICT(scan_id) DO NOTHING
                """,
                (scan_id, SOURCE_SYSTEM, CORPUS_TYPE, config_fingerprint, query, PHASE_PENDING, first_page, now, now),
            )
            cur.execute(f"SELECT {self._SCAN_COLUMNS} FROM sot_scans WHERE scan_id = %s", (scan_id,))
            row = cur.fetchone()
            self._conn.commit()
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
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sot_scans
                SET total_decisions=%s, page_size=%s, total_pages=%s, updated_at=%s
                WHERE scan_id = %s
                """,
                (total_decisions, page_size, total_pages, now_iso(), scan_id),
            )
            self._conn.commit()

    def set_scan_phase(
        self,
        scan_id: str,
        phase: str,
        error: str | None = None,
        rate_limit_note: str | None = None,
    ) -> None:
        now = now_iso()
        completed_at = now if phase == PHASE_COMPLETED else None
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sot_scans
                SET phase=%s,
                    error=%s,
                    rate_limit_note=%s,
                    completed_at=COALESCE(%s::timestamptz, completed_at),
                    updated_at=%s
                WHERE scan_id = %s
                """,
                (phase, error, rate_limit_note, completed_at, now, scan_id),
            )
            self._conn.commit()

    def advance_scan(
        self,
        scan_id: str,
        next_page: int,
        next_cursor: str | None = None,
        decisions_enqueued: int = 0,
    ) -> None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sot_scans
                SET decisions_enqueued=decisions_enqueued + CASE WHEN %s > next_page THEN %s ELSE 0 END,
                    next_page=GREATEST(next_page, %s),
                    next_cursor=%s,
                    pages_done=GREATEST(pages_done, %s - 1),
                    decisions_seen=(SELECT COUNT(*) FROM sot_scan_decisions WHERE scan_id = %s),
                    updated_at=%s
                WHERE scan_id = %s
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
            self._conn.commit()

    # --- membership and queue --------------------------------------------

    def record_search_page(self, scan_id: str, page: int, refs: Iterable[SotDecisionRef]) -> int:
        items = list(refs)
        if not items:
            return 0
        now = now_iso()
        with self._lock, self._conn.cursor() as cur:
            for ref in items:
                columns = decision_columns(ref.metadata)
                cur.execute(
                    """
                    INSERT INTO sot_decisions(
                        decision_key, decision_id, source_system, corpus_type,
                        case_number, court, judge, region, instance, proceeding_type,
                        decision_date, title, parties, metadata, source_url,
                        status, created_at, updated_at
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s)
                    ON CONFLICT(decision_key) DO UPDATE SET
                        case_number=COALESCE(EXCLUDED.case_number, sot_decisions.case_number),
                        court=COALESCE(EXCLUDED.court, sot_decisions.court),
                        judge=COALESCE(EXCLUDED.judge, sot_decisions.judge),
                        region=COALESCE(EXCLUDED.region, sot_decisions.region),
                        instance=COALESCE(EXCLUDED.instance, sot_decisions.instance),
                        proceeding_type=COALESCE(EXCLUDED.proceeding_type, sot_decisions.proceeding_type),
                        decision_date=COALESCE(EXCLUDED.decision_date, sot_decisions.decision_date),
                        title=COALESCE(EXCLUDED.title, sot_decisions.title),
                        parties=COALESCE(EXCLUDED.parties, sot_decisions.parties),
                        source_url=COALESCE(NULLIF(EXCLUDED.source_url, ''), sot_decisions.source_url),
                        updated_at=EXCLUDED.updated_at
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
                cur.execute(
                    """
                    INSERT INTO sot_scan_decisions(
                        scan_id, decision_key, decision_id, page, position, outcome, updated_at
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(scan_id, decision_key) DO UPDATE SET
                        page=EXCLUDED.page,
                        position=EXCLUDED.position,
                        updated_at=EXCLUDED.updated_at
                    """,
                    (scan_id, ref.decision_key, ref.decision_id, page, ref.position, OUTCOME_PENDING, now),
                )
            self._conn.commit()
        return len(items)

    def claim_decision(self, scan_id: str, worker_id: str) -> SotDecisionRef | None:
        now = now_iso()
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                WITH next_decision AS (
                    SELECT d.decision_key
                    FROM sot_scan_decisions AS m
                    JOIN sot_decisions AS d ON d.decision_key = m.decision_key
                    WHERE m.scan_id = %s AND m.outcome = %s AND d.status = %s
                    ORDER BY m.page, m.position, m.decision_key
                    FOR UPDATE OF d SKIP LOCKED
                    LIMIT 1
                )
                UPDATE sot_decisions AS d
                SET status=%s, attempts=d.attempts + 1, locked_by=%s, locked_at=%s, updated_at=%s
                FROM next_decision
                WHERE d.decision_key = next_decision.decision_key
                RETURNING d.decision_key, d.decision_id, d.source_url, d.metadata
                """,
                (scan_id, OUTCOME_PENDING, STATUS_QUEUED, STATUS_PROCESSING, worker_id, now, now),
            )
            row = cur.fetchone()
            if row is None:
                self._conn.commit()
                return None
            cur.execute(
                "SELECT page, position FROM sot_scan_decisions WHERE scan_id = %s AND decision_key = %s",
                (scan_id, str(row[0])),
            )
            member = cur.fetchone()
            self._conn.commit()
        metadata = row[3] if isinstance(row[3], dict) else json.loads(str(row[3] or "{}"))
        return SotDecisionRef(
            decision_id=str(row[1]),
            decision_key=str(row[0]),
            source_url=str(row[2] or ""),
            page=int(member[0]) if member else 0,
            position=int(member[1]) if member else 0,
            metadata=metadata,
        )

    def release_decision(self, decision_key: str) -> None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sot_decisions
                SET status=%s, locked_by=NULL, locked_at=NULL, attempts=GREATEST(0, attempts - 1), updated_at=%s
                WHERE decision_key = %s AND status = %s
                """,
                (STATUS_QUEUED, now_iso(), decision_key, STATUS_PROCESSING),
            )
            self._conn.commit()

    def requeue_stale_decisions(self, scan_id: str, lease_seconds: int) -> int:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sot_decisions
                SET status=%s, locked_by=NULL, locked_at=NULL, updated_at=%s
                WHERE status = %s
                  AND (locked_at IS NULL OR locked_at < now() - (%s * INTERVAL '1 second'))
                  AND decision_key IN (
                      SELECT decision_key FROM sot_scan_decisions WHERE scan_id = %s AND outcome = %s
                  )
                """,
                (STATUS_QUEUED, now_iso(), STATUS_PROCESSING, lease_seconds, scan_id, OUTCOME_PENDING),
            )
            count = int(cur.rowcount or 0)
            self._conn.commit()
        return count

    # --- outputs ----------------------------------------------------------

    def has_decision_outputs(self, decision_key: str, formats: Iterable[str]) -> bool:
        wanted = tuple(sorted(set(formats)))
        if not wanted:
            return False
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM sot_decision_outputs
                WHERE decision_key = %s AND format = ANY(%s)
                """,
                (decision_key, list(wanted)),
            )
            row = cur.fetchone()
            self._conn.commit()
        return int(row[0] or 0) >= len(wanted)

    def is_decision_complete(self, decision_key: str, formats: Iterable[str]) -> bool:
        return self.decision_status(decision_key) == STATUS_EXPORTED and self.has_decision_outputs(
            decision_key, formats
        )

    def save_decision(self, payload: SotDecisionPayload, formats: Iterable[str] = DECISION_FORMATS) -> None:
        from .adapter import sha256_text

        wanted = tuple(formats)
        raw_json = json.dumps(payload.raw, ensure_ascii=False)
        bodies = {
            "txt": (payload.text, "text/plain"),
            "json": (raw_json, "application/json"),
        }
        now = now_iso()
        columns = decision_columns(payload.metadata)
        with self._lock, self._conn.cursor() as cur:
            for name in wanted:
                if name not in bodies:
                    raise ValueError(f"Unsupported SOT output format '{name}'.")
                body, content_type = bodies[name]
                blob = body.encode("utf-8")
                cur.execute(
                    """
                    INSERT INTO sot_decision_outputs(
                        decision_key, format, content_type, encoding, content,
                        size_bytes, sha256, created_at, updated_at
                    )
                    VALUES(%s, %s, %s, 'utf-8', %s, %s, %s, %s, %s)
                    ON CONFLICT(decision_key, format) DO UPDATE SET
                        content_type=EXCLUDED.content_type,
                        encoding=EXCLUDED.encoding,
                        content=EXCLUDED.content,
                        size_bytes=EXCLUDED.size_bytes,
                        sha256=EXCLUDED.sha256,
                        updated_at=EXCLUDED.updated_at
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
            cur.execute(
                """
                UPDATE sot_decisions
                SET status=%s,
                    case_number=COALESCE(%s, case_number),
                    court=COALESCE(%s, court),
                    judge=COALESCE(%s, judge),
                    region=COALESCE(%s, region),
                    instance=COALESCE(%s, instance),
                    proceeding_type=COALESCE(%s, proceeding_type),
                    decision_date=COALESCE(%s, decision_date),
                    title=COALESCE(%s, title),
                    parties=COALESCE(%s::jsonb, parties),
                    metadata=%s::jsonb,
                    source_url=COALESCE(NULLIF(%s, ''), source_url),
                    text_sha256=%s,
                    raw_sha256=%s,
                    text_chars=%s,
                    error=NULL,
                    locked_by=NULL,
                    locked_at=NULL,
                    updated_at=%s
                WHERE decision_key = %s
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
            self._conn.commit()

    def mark_decision_failed(self, decision_key: str, error: str) -> None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sot_decisions
                SET status=%s, error=%s, locked_by=NULL, locked_at=NULL, updated_at=%s
                WHERE decision_key = %s
                """,
                (STATUS_FAILED, error[:2000], now_iso(), decision_key),
            )
            self._conn.commit()

    def decision_status(self, decision_key: str) -> str | None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute("SELECT status FROM sot_decisions WHERE decision_key = %s", (decision_key,))
            row = cur.fetchone()
            self._conn.commit()
        return str(row[0]) if row else None

    def get_decision(self, decision_key: str) -> dict[str, object] | None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT decision_key, decision_id, source_system, corpus_type, case_number, court,
                       judge, region, instance, proceeding_type, decision_date, title,
                       source_url, status, text_chars, text_sha256, updated_at
                FROM sot_decisions WHERE decision_key = %s
                """,
                (decision_key,),
            )
            row = cur.fetchone()
            names = [item.name for item in cur.description] if cur.description else []
            self._conn.commit()
        if not row:
            return None
        return {name: value for name, value in zip(names, row)}

    def decision_stats(self) -> dict[str, int]:
        with self._lock, self._conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM sot_decisions GROUP BY status")
            rows = cur.fetchall()
            self._conn.commit()
        return {str(status): int(count) for status, count in rows}

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
        now = now_iso()
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT m.decision_id, m.page, m.position, d.case_number, d.court, d.source_url
                FROM sot_scan_decisions AS m
                LEFT JOIN sot_decisions AS d ON d.decision_key = m.decision_key
                WHERE m.scan_id = %s AND m.decision_key = %s
                """,
                (scan_id, decision_key),
            )
            row = cur.fetchone()
            if row is None:
                self._conn.commit()
                return None
            stub = None
            if outcome != OUTCOME_DONE:
                stub = build_stub(
                    scan_id=scan_id,
                    decision_key=decision_key,
                    decision_id=str(row[0]),
                    outcome=outcome,
                    page=int(row[1] or 0),
                    position=int(row[2] or 0),
                    case_number=str(row[3] or ""),
                    court=str(row[4] or ""),
                    source_url=str(row[5] or ""),
                    failure_kind=failure_kind,
                    http_status=http_status,
                    detail=detail,
                    recorded_at=now,
                )
            cur.execute(
                """
                UPDATE sot_scan_decisions
                SET outcome=%s, failure_kind=%s, http_status=%s, stub=%s::jsonb, updated_at=%s
                WHERE scan_id = %s AND decision_key = %s
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
            self._conn.commit()
        return stub

    def resolve_scan_outcomes(self, scan_id: str) -> int:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sot_scan_decisions
                SET outcome=%s, failure_kind=NULL, http_status=NULL, stub=NULL, updated_at=%s
                WHERE scan_id = %s
                  AND outcome = %s
                  AND decision_key IN (SELECT decision_key FROM sot_decisions WHERE status = %s)
                """,
                (OUTCOME_DONE, now_iso(), scan_id, OUTCOME_PENDING, STATUS_EXPORTED),
            )
            count = int(cur.rowcount or 0)
            self._conn.commit()
        return count

    def retry_scan_outcomes(self, scan_id: str) -> int:
        retryable = list(sorted(RETRYABLE_OUTCOMES))
        now = now_iso()
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sot_scan_decisions
                SET outcome=%s, failure_kind=NULL, http_status=NULL, stub=NULL, updated_at=%s
                WHERE scan_id = %s AND outcome = ANY(%s)
                """,
                (OUTCOME_PENDING, now, scan_id, retryable),
            )
            count = int(cur.rowcount or 0)
            cur.execute(
                """
                UPDATE sot_decisions
                SET status=%s, locked_by=NULL, locked_at=NULL, error=NULL, updated_at=%s
                WHERE status <> %s
                  AND decision_key IN (
                      SELECT decision_key FROM sot_scan_decisions WHERE scan_id = %s AND outcome = %s
                  )
                """,
                (STATUS_QUEUED, now, STATUS_EXPORTED, scan_id, OUTCOME_PENDING),
            )
            self._conn.commit()
        return count

    def pending_decision_count(self, scan_id: str) -> int:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM sot_scan_decisions WHERE scan_id = %s AND outcome = %s",
                (scan_id, OUTCOME_PENDING),
            )
            row = cur.fetchone()
            self._conn.commit()
        return int(row[0] or 0)

    def scan_stats(self, scan_id: str) -> dict[str, int]:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                "SELECT outcome, COUNT(*) FROM sot_scan_decisions WHERE scan_id = %s GROUP BY outcome",
                (scan_id,),
            )
            rows = cur.fetchall()
            self._conn.commit()
        return {str(outcome): int(count) for outcome, count in rows}

    def scan_stubs(self, scan_id: str) -> list[dict[str, object]]:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT stub FROM sot_scan_decisions
                WHERE scan_id = %s AND stub IS NOT NULL
                ORDER BY page, position, decision_key
                """,
                (scan_id,),
            )
            rows = cur.fetchall()
            self._conn.commit()
        stubs: list[dict[str, object]] = []
        for (stub,) in rows:
            if isinstance(stub, dict):
                stubs.append(stub)
                continue
            try:
                stubs.append(json.loads(str(stub)))
            except (TypeError, ValueError):
                continue
        return stubs
