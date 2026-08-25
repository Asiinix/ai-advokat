"""Shared state, outcomes and failure classification for the PRG.SOT scan.

Mirrors :mod:`ai_advokat_parser.catalog` in spirit but stays a separate module:
the two corpora must never share a state row, an outcome vocabulary or a key.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from ..catalog import sanitize_detail
from ..http_client import (
    SourceAccessDeniedError,
    SourceAuthError,
    SourceRateLimitError,
    SourceRequestError,
)
from . import CORPUS_TYPE, SOURCE_SYSTEM

PHASE_PENDING = "pending"
PHASE_ENUMERATING = "enumerating"
PHASE_DRAINING = "draining"
PHASE_PAUSED = "paused"
PHASE_RATE_LIMITED = "rate_limited"
PHASE_COMPLETED = "completed"
PHASE_ABORTED = "aborted"

OUTCOME_PENDING = "pending"
OUTCOME_DONE = "done"
OUTCOME_INACCESSIBLE = "inaccessible"
OUTCOME_NOT_FOUND = "not_found"
OUTCOME_FAILED = "failed"

TERMINAL_OUTCOMES = frozenset({OUTCOME_DONE, OUTCOME_INACCESSIBLE, OUTCOME_NOT_FOUND, OUTCOME_FAILED})
# Outcomes a plain resume leaves alone; --retry-failed puts them back in the queue.
RETRYABLE_OUTCOMES = frozenset({OUTCOME_INACCESSIBLE, OUTCOME_NOT_FOUND, OUTCOME_FAILED})

STATUS_QUEUED = "queued"
STATUS_PROCESSING = "processing"
STATUS_EXPORTED = "exported"
STATUS_FAILED = "failed"

INACCESSIBLE_STATUSES = frozenset({401, 402, 403})
NOT_FOUND_STATUSES = frozenset({404})

STUB_FIELDS = (
    "scan_id",
    "source_system",
    "corpus_type",
    "decision_key",
    "decision_id",
    "page",
    "position",
    "case_number",
    "court",
    "source_url",
    "outcome",
    "failure_kind",
    "http_status",
    "detail",
    "recorded_at",
)


class SotDiscoveryError(RuntimeError):
    """The search endpoint did not return a usable result list.

    The scan refuses to invent a corpus size or to treat an unparseable payload
    as "no more results": both would quietly truncate 16.5M decisions.
    """


class SotDecisionUnavailableError(RuntimeError):
    """The source answered, but the decision carries no readable text."""

    def __init__(self, decision_id: str, reason: str) -> None:
        super().__init__(f"Decision {decision_id} is unavailable: {reason}")
        self.decision_id = decision_id
        self.reason = reason


@dataclass(frozen=True)
class SotDecisionRef:
    """One decision as it appeared in a search result page."""

    decision_id: str
    decision_key: str
    source_url: str = ""
    page: int = 0
    position: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def case_number(self) -> str:
        return str(self.metadata.get("case_number") or "")

    @property
    def court(self) -> str:
        return str(self.metadata.get("court") or "")


@dataclass(frozen=True)
class SotDecisionPayload:
    """One fully fetched decision, ready to be persisted."""

    decision_id: str
    decision_key: str
    source_url: str
    text: str
    raw: Mapping[str, Any]
    metadata: Mapping[str, Any]

    @property
    def source_system(self) -> str:
        return SOURCE_SYSTEM

    @property
    def corpus_type(self) -> str:
        return CORPUS_TYPE


@dataclass(frozen=True)
class SotScanState:
    """One resumable enumeration of the PRG.SOT corpus."""

    scan_id: str
    source_system: str
    corpus_type: str
    config_fingerprint: str
    query: str
    phase: str
    total_decisions: int | None
    page_size: int | None
    total_pages: int | None
    next_page: int
    next_cursor: str | None
    pages_done: int
    decisions_seen: int
    decisions_enqueued: int
    error: str | None
    rate_limit_note: str | None
    started_at: str
    updated_at: str
    completed_at: str | None

    @property
    def is_completed(self) -> bool:
        return self.phase == PHASE_COMPLETED


def classify_decision_failure(exc: BaseException) -> tuple[str, str, int | None]:
    """Map a fetch failure to (outcome, failure_kind, http_status).

    Never called for :class:`SourceAuthError` or :class:`SourceRateLimitError`:
    both are verdicts about the session or the quota, not about one decision,
    and both must stop the run instead of burning through the corpus.
    """
    if isinstance(exc, (SourceAuthError, SourceRateLimitError)):
        raise AssertionError("session-level errors must not be recorded per decision")
    if isinstance(exc, SourceAccessDeniedError):
        return OUTCOME_INACCESSIBLE, "access_denied", exc.status
    if isinstance(exc, SotDecisionUnavailableError):
        return OUTCOME_INACCESSIBLE, "no_text", None
    if isinstance(exc, SourceRequestError):
        if exc.status in INACCESSIBLE_STATUSES:
            return OUTCOME_INACCESSIBLE, "forbidden", exc.status
        if exc.status in NOT_FOUND_STATUSES:
            return OUTCOME_NOT_FOUND, "not_found", exc.status
        return OUTCOME_FAILED, "request_error", exc.status
    return OUTCOME_FAILED, "error", None


def build_stub(
    scan_id: str,
    decision_key: str,
    decision_id: str,
    outcome: str,
    page: int | None = None,
    position: int | None = None,
    case_number: str = "",
    court: str = "",
    source_url: str = "",
    failure_kind: str | None = None,
    http_status: int | None = None,
    detail: str = "",
    recorded_at: str = "",
) -> dict[str, Any]:
    """Build the durable, allow-listed manifest entry for one decision."""
    stub = {
        "scan_id": scan_id,
        "source_system": SOURCE_SYSTEM,
        "corpus_type": CORPUS_TYPE,
        "decision_key": decision_key,
        "decision_id": decision_id,
        "page": page,
        "position": position,
        "case_number": case_number or "",
        "court": court or "",
        "source_url": source_url or "",
        "outcome": outcome,
        "failure_kind": failure_kind,
        "http_status": http_status,
        "detail": sanitize_detail(detail),
        "recorded_at": recorded_at,
    }
    return {key: stub[key] for key in STUB_FIELDS}
