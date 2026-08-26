"""Shared state and failure classification for the PRG full catalog scan.

The catalog scan enumerates every document of the source listing, so it has to
survive restarts and it has to keep a durable, credential-free trace of every
document it could not export. Both storage backends share the small state
container and the classification helpers defined here.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any

from .config import CREDENTIAL_ENV_NAMES
from .document import DocumentNotFreeError, DocumentUnavailableError
from .http_client import SourceAccessDeniedError, SourceRequestError

PHASE_PENDING = "pending"
PHASE_ENUMERATING = "enumerating"
PHASE_DRAINING = "draining"
PHASE_PAUSED = "paused"
PHASE_COMPLETED = "completed"
PHASE_ABORTED = "aborted"

OUTCOME_PENDING = "pending"
OUTCOME_DONE = "done"
OUTCOME_INACCESSIBLE = "inaccessible"
OUTCOME_NOT_FOUND = "not_found"
OUTCOME_FAILED = "failed"

TERMINAL_OUTCOMES = frozenset({OUTCOME_DONE, OUTCOME_INACCESSIBLE, OUTCOME_NOT_FOUND, OUTCOME_FAILED})

# 401 only reaches the classifier as SourceAccessDeniedError: a plain 401 that a
# fresh login could fix is raised as SourceAuthError and stays fatal.
INACCESSIBLE_STATUSES = frozenset({401, 402, 403})
NOT_FOUND_STATUSES = frozenset({404})

STUB_FIELDS = (
    "scan_id",
    "doc_id",
    "page",
    "position",
    "title",
    "source_url",
    "outcome",
    "failure_kind",
    "http_status",
    "detail",
    "recorded_at",
)

DETAIL_MAX_LEN = 300
SECRET_MARKERS = (
    "cookie",
    "set-cookie",
    "__requestverificationtoken",
    "authorization",
    "proxy-authorization",
    "password",
)
# Proxy URLs are resolved only from dedicated environment variables and may
# embed credentials, so any variable whose name carries the PROXY token is
# treated as secret-bearing. Token matching (not a suffix) is what covers the
# documented AI_ADVOCAT_SOT_PROXY_PX1 style alongside HTTP_PROXY/http_proxy.
PROXY_ENV_TOKEN = "PROXY"


def _names_proxy_value(name: str) -> bool:
    return PROXY_ENV_TOKEN in name.upper().split("_")


class CatalogDiscoveryError(RuntimeError):
    """The listing did not report a usable document total.

    The scan refuses to guess how big the catalog is: an invented page count
    would either stop early or hammer the source with empty pages.
    """


@dataclass(frozen=True)
class CatalogScanState:
    """One resumable enumeration of the whole source catalog."""

    scan_id: str
    list_url: str
    product: str
    formats: tuple[str, ...]
    phase: str
    total_documents: int | None
    page_size: int | None
    total_pages: int | None
    next_page: int
    pages_done: int
    docs_seen: int
    docs_enqueued: int
    error: str | None
    started_at: str
    updated_at: str
    completed_at: str | None

    @property
    def is_completed(self) -> bool:
        return self.phase == PHASE_COMPLETED


def format_list(formats: Any) -> str:
    return ",".join(str(item) for item in (formats or ()))


def parse_format_list(raw: str | None) -> tuple[str, ...]:
    return tuple(item for item in str(raw or "").split(",") if item)


def sanitize_detail(message: str, env: dict[str, str] | None = None) -> str:
    """Reduce an exception message to something safe to persist and to ship.

    Stub rows end up in Postgres and in ``catalog-stubs`` dumps, so they must
    never carry a response body, a cookie, an anti-forgery token or credentials.
    """
    source = os.environ if env is None else env
    text = re.sub(r"\s+", " ", str(message or "")).strip()
    if not text:
        return ""
    lowered = text.lower()
    if "<html" in lowered or "<!doctype" in lowered or "<form" in lowered:
        return "source returned an HTML page (body omitted)"
    if any(marker in lowered for marker in SECRET_MARKERS):
        return "detail omitted: message referenced sensitive material"
    for name, raw_value in source.items():
        value = (raw_value or "").strip()
        if not value or value not in text:
            continue
        if name in CREDENTIAL_ENV_NAMES:
            text = text.replace(value, "***")
        elif _names_proxy_value(name) and len(value) >= 8:
            # NO_PROXY-style values can be trivial ("*", "localhost"); only a
            # value long enough to be a URL is worth scrubbing as a secret.
            text = text.replace(value, "***")
    if len(text) > DETAIL_MAX_LEN:
        text = f"{text[:DETAIL_MAX_LEN]}…"
    return text


def classify_document_failure(exc: BaseException) -> tuple[str, str, int | None]:
    """Map a download failure to (outcome, failure_kind, http_status).

    Never called for :class:`SourceAuthError`: a broken PRG session is fatal for
    the whole scan and must not be written down as a per-document failure.
    """
    if isinstance(exc, DocumentNotFreeError):
        return OUTCOME_INACCESSIBLE, "paid", None
    if isinstance(exc, SourceAccessDeniedError):
        return OUTCOME_INACCESSIBLE, "access_denied", exc.status
    if isinstance(exc, DocumentUnavailableError):
        return OUTCOME_INACCESSIBLE, "no_pages", None
    if isinstance(exc, SourceRequestError):
        if exc.status in INACCESSIBLE_STATUSES:
            return OUTCOME_INACCESSIBLE, "forbidden", exc.status
        if exc.status in NOT_FOUND_STATUSES:
            return OUTCOME_NOT_FOUND, "not_found", exc.status
        return OUTCOME_FAILED, "request_error", exc.status
    return OUTCOME_FAILED, "error", None


def build_stub(
    scan_id: str,
    doc_id: str,
    outcome: str,
    page: int | None = None,
    position: int | None = None,
    title: str = "",
    source_url: str = "",
    failure_kind: str | None = None,
    http_status: int | None = None,
    detail: str = "",
    recorded_at: str = "",
) -> dict[str, Any]:
    """Build the durable, allow-listed manifest entry for one document."""
    stub = {
        "scan_id": scan_id,
        "doc_id": doc_id,
        "page": page,
        "position": position,
        "title": title or "",
        "source_url": source_url or "",
        "outcome": outcome,
        "failure_kind": failure_kind,
        "http_status": http_status,
        "detail": sanitize_detail(detail),
        "recorded_at": recorded_at,
    }
    return {key: stub[key] for key in STUB_FIELDS}
