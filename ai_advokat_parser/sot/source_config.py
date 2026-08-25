"""The verified-source contract for PRG.SOT.

Nothing in this repository knows the real PRG.SOT search or decision endpoints:
they sit behind a paid login and were never observed. Inventing plausible routes
would produce a parser that looks finished and silently scrapes nothing, so the
templates are configuration, not code.

An operator captures the two requests from a live, subscribed session (browser
DevTools -> Network -> Copy as cURL), fills the variables below and only then can
``sot-scan`` write anything. Until every required template is present and valid,
every command fails before the first database write.
"""

from __future__ import annotations

import json
import os
import re
import urllib.parse
from dataclasses import dataclass
from typing import Any, Mapping

from ..config import SOT_BASE_URL

SEARCH_URL_ENV = "AI_ADVOCAT_SOT_SEARCH_URL_TEMPLATE"
SEARCH_METHOD_ENV = "AI_ADVOCAT_SOT_SEARCH_METHOD"
SEARCH_BODY_ENV = "AI_ADVOCAT_SOT_SEARCH_BODY_TEMPLATE"
DECISION_URL_ENV = "AI_ADVOCAT_SOT_DECISION_URL_TEMPLATE"
DECISION_METHOD_ENV = "AI_ADVOCAT_SOT_DECISION_METHOD"
DECISION_BODY_ENV = "AI_ADVOCAT_SOT_DECISION_BODY_TEMPLATE"
RESULTS_PATH_ENV = "AI_ADVOCAT_SOT_RESULTS_PATH"
TOTAL_PATH_ENV = "AI_ADVOCAT_SOT_TOTAL_PATH"
NEXT_CURSOR_PATH_ENV = "AI_ADVOCAT_SOT_NEXT_CURSOR_PATH"
ID_PATH_ENV = "AI_ADVOCAT_SOT_ID_PATH"
TEXT_PATH_ENV = "AI_ADVOCAT_SOT_TEXT_PATH"
FIELD_MAP_ENV = "AI_ADVOCAT_SOT_FIELD_MAP"
PAGE_SIZE_ENV = "AI_ADVOCAT_SOT_PAGE_SIZE"
FIRST_PAGE_ENV = "AI_ADVOCAT_SOT_FIRST_PAGE"
QUERY_ENV = "AI_ADVOCAT_SOT_QUERY"
BASE_URL_ENV = "AI_ADVOCAT_SOT_BASE_URL"
DECISION_URL_TEMPLATE_ENV = "AI_ADVOCAT_SOT_DECISION_PAGE_URL_TEMPLATE"

SUPPORTED_METHODS = ("GET", "POST")
PLACEHOLDER_RE = re.compile(r"\{([a-z_]+)\}")

SEARCH_PLACEHOLDERS = frozenset({"page", "page_size", "cursor", "query", "offset"})
DECISION_PLACEHOLDERS = frozenset({"decision_id"})

# Court metadata we persist for every decision. The operator maps each one to a
# dotted path inside the real search/decision payload; unmapped fields stay NULL
# rather than being guessed from a field name that may not exist.
METADATA_FIELDS = (
    "case_number",
    "court",
    "judge",
    "region",
    "instance",
    "proceeding_type",
    "decision_date",
    "title",
    "parties",
)

DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 200


class SotConfigError(RuntimeError):
    """The source contract is missing or malformed.

    Raised before any state is written, so a half-configured deploy cannot
    create a scan row, a lease or a partial decision.
    """


def dotted_get(payload: Any, path: str, default: Any = None) -> Any:
    """Read ``a.b.0.c`` out of decoded JSON without raising on a missing step."""
    if not path:
        return default
    current = payload
    for part in path.split("."):
        if isinstance(current, Mapping):
            if part not in current:
                return default
            current = current[part]
            continue
        if isinstance(current, (list, tuple)):
            try:
                index = int(part)
            except ValueError:
                return default
            if index < 0 or index >= len(current):
                return default
            current = current[index]
            continue
        return default
    return default if current is None else current


def dotted_values(payload: Any, path: str) -> list[Any]:
    """Read every value from a dotted path with ``*``/``[]`` list expansion."""
    if not path:
        return []
    normalized = path.replace("[]", ".*")
    current = [payload]
    for part in normalized.split("."):
        if not part:
            continue
        following: list[Any] = []
        for value in current:
            if part == "*":
                if isinstance(value, (list, tuple)):
                    following.extend(value)
                continue
            if isinstance(value, Mapping):
                if part in value:
                    following.append(value[part])
                continue
            if isinstance(value, (list, tuple)):
                try:
                    index = int(part)
                except ValueError:
                    continue
                if 0 <= index < len(value):
                    following.append(value[index])
        current = following
        if not current:
            break
    return [value for value in current if value is not None]


def _template_placeholders(template: str) -> set[str]:
    return set(PLACEHOLDER_RE.findall(template or ""))


@dataclass(frozen=True)
class SotSourceConfig:
    """One captured, verified PRG.SOT request pair plus its response shape."""

    base_url: str
    search_url_template: str
    search_method: str
    search_body_template: str
    decision_url_template: str
    decision_method: str
    decision_body_template: str
    results_path: str
    id_path: str
    text_path: str
    total_path: str
    next_cursor_path: str
    field_map: Mapping[str, str]
    page_size: int
    first_page: int
    query: str
    decision_page_url_template: str

    @property
    def is_configured(self) -> bool:
        return not self.missing_requirements()

    def missing_requirements(self) -> list[str]:
        missing = []
        if not self.search_url_template:
            missing.append(SEARCH_URL_ENV)
        if not self.decision_url_template:
            missing.append(DECISION_URL_ENV)
        if not self.results_path:
            missing.append(RESULTS_PATH_ENV)
        if not self.id_path:
            missing.append(ID_PATH_ENV)
        if not self.text_path:
            missing.append(TEXT_PATH_ENV)
        return missing

    def fingerprint(self) -> str:
        """Stable identity of the contract, used to pin a scan to one shape."""
        payload = {
            "search": [self.search_method, self.search_url_template, self.search_body_template],
            "decision": [self.decision_method, self.decision_url_template, self.decision_body_template],
            "paths": [self.results_path, self.id_path, self.text_path, self.total_path, self.next_cursor_path],
            "field_map": dict(sorted(self.field_map.items())),
            "page_size": self.page_size,
            "query": self.query,
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    def validate(self) -> "SotSourceConfig":
        """Fail loudly on anything that would send an authenticated cookie astray."""
        missing = self.missing_requirements()
        if missing:
            raise SotConfigError(
                "PRG.SOT source endpoints are not configured. Capture them from a live "
                "subscribed session and set: " + ", ".join(missing) + ". "
                "See README > PRG.SOT for the exact capture procedure."
            )
        problems: list[str] = []
        for name, method in ((SEARCH_METHOD_ENV, self.search_method), (DECISION_METHOD_ENV, self.decision_method)):
            if method not in SUPPORTED_METHODS:
                problems.append(f"{name} must be one of {', '.join(SUPPORTED_METHODS)}, got '{method}'")

        search_placeholders = _template_placeholders(
            f"{self.search_url_template} {self.search_body_template}"
        )
        unknown = sorted(search_placeholders - SEARCH_PLACEHOLDERS)
        if unknown:
            problems.append(
                f"{SEARCH_URL_ENV}/{SEARCH_BODY_ENV} use unknown placeholders: {', '.join(unknown)}. "
                f"Supported: {', '.join(sorted(SEARCH_PLACEHOLDERS))}"
            )
        if not search_placeholders & {"page", "offset", "cursor"}:
            problems.append(
                f"{SEARCH_URL_ENV} or {SEARCH_BODY_ENV} must paginate with one of "
                "{page}, {offset} or {cursor}; otherwise the scan would read page 1 forever"
            )

        decision_placeholders = _template_placeholders(
            f"{self.decision_url_template} {self.decision_body_template}"
        )
        unknown = sorted(decision_placeholders - DECISION_PLACEHOLDERS)
        if unknown:
            problems.append(
                f"{DECISION_URL_ENV}/{DECISION_BODY_ENV} use unknown placeholders: {', '.join(unknown)}. "
                f"Supported: {', '.join(sorted(DECISION_PLACEHOLDERS))}"
            )
        if "decision_id" not in decision_placeholders:
            problems.append(f"{DECISION_URL_ENV} or {DECISION_BODY_ENV} must contain {{decision_id}}")

        for name, template in (
            (SEARCH_URL_ENV, self.search_url_template),
            (DECISION_URL_ENV, self.decision_url_template),
        ):
            problems.extend(self._url_problems(name, template))

        for name, template in ((SEARCH_BODY_ENV, self.search_body_template), (DECISION_BODY_ENV, self.decision_body_template)):
            if not template:
                continue
            probe = PLACEHOLDER_RE.sub("1", template)
            try:
                json.loads(probe)
            except ValueError:
                problems.append(f"{name} must be a JSON object template")

        if not 1 <= self.page_size <= MAX_PAGE_SIZE:
            problems.append(f"{PAGE_SIZE_ENV} must be between 1 and {MAX_PAGE_SIZE}")
        if self.first_page < 0:
            problems.append(f"{FIRST_PAGE_ENV} must be >= 0")

        unknown_fields = sorted(set(self.field_map) - set(METADATA_FIELDS))
        if unknown_fields:
            problems.append(
                f"{FIELD_MAP_ENV} maps unknown fields: {', '.join(unknown_fields)}. "
                f"Known fields: {', '.join(METADATA_FIELDS)}"
            )

        if problems:
            raise SotConfigError("Invalid PRG.SOT source configuration: " + "; ".join(problems))
        return self

    def _url_problems(self, name: str, template: str) -> list[str]:
        probe = PLACEHOLDER_RE.sub("1", template)
        parsed = urllib.parse.urlparse(probe)
        if parsed.scheme not in {"http", "https"}:
            return [f"{name} must be an absolute http(s) URL"]
        allowed = urllib.parse.urlparse(self.base_url)
        if (parsed.scheme.lower(), parsed.netloc.lower()) != (allowed.scheme.lower(), allowed.netloc.lower()):
            # The client carries a live PRG session cookie; a template pointing
            # anywhere else would hand that session to a third party.
            return [
                f"{name} points at {parsed.scheme}://{parsed.netloc}, which is not the configured "
                f"PRG.SOT origin {self.base_url}. Set {BASE_URL_ENV} deliberately if this is intended."
            ]
        return []

    def search_request(self, page: int, cursor: str | None, offset: int) -> tuple[str, str, Any]:
        """Render (url, method, json body) for one search page."""
        values = {
            "page": str(page),
            "page_size": str(self.page_size),
            "cursor": cursor or "",
            "offset": str(offset),
            "query": self.query,
        }
        url = self._render(self.search_url_template, values, quote=True)
        body = None
        if self.search_body_template:
            body = json.loads(self._render(self.search_body_template, values, quote=False, json_safe=True))
        return url, self.search_method, body

    def decision_request(self, decision_id: str) -> tuple[str, str, Any]:
        values = {"decision_id": str(decision_id)}
        url = self._render(self.decision_url_template, values, quote=True)
        body = None
        if self.decision_body_template:
            body = json.loads(self._render(self.decision_body_template, values, quote=False, json_safe=True))
        return url, self.decision_method, body

    def decision_page_url(self, decision_id: str) -> str:
        if not self.decision_page_url_template:
            return ""
        return self._render(self.decision_page_url_template, {"decision_id": str(decision_id)}, quote=True)

    @staticmethod
    def _render(template: str, values: Mapping[str, str], quote: bool, json_safe: bool = False) -> str:
        def replace(match: re.Match) -> str:
            raw = values.get(match.group(1), "")
            if quote:
                return urllib.parse.quote(raw, safe="")
            if json_safe:
                return json.dumps(raw, ensure_ascii=False)[1:-1]
            return raw

        return PLACEHOLDER_RE.sub(replace, template)

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        overrides: Mapping[str, Any] | None = None,
    ) -> "SotSourceConfig":
        source = os.environ if env is None else env
        picked = {key: value for key, value in dict(overrides or {}).items() if value not in (None, "")}

        def text(name: str, key: str, default: str = "") -> str:
            if key in picked:
                return str(picked[key]).strip()
            return (source.get(name) or default).strip()

        def number(name: str, key: str, default: int) -> int:
            raw = picked.get(key, source.get(name) or default)
            try:
                return int(raw)
            except (TypeError, ValueError) as exc:
                raise SotConfigError(f"{name} must be an integer, got {raw!r}") from exc

        raw_map = text(FIELD_MAP_ENV, "field_map")
        field_map: dict[str, str] = {}
        if raw_map:
            try:
                decoded = json.loads(raw_map)
            except ValueError as exc:
                raise SotConfigError(f"{FIELD_MAP_ENV} must be a JSON object of field -> dotted path") from exc
            if not isinstance(decoded, Mapping):
                raise SotConfigError(f"{FIELD_MAP_ENV} must be a JSON object of field -> dotted path")
            field_map = {str(key): str(value) for key, value in decoded.items() if str(value).strip()}

        return cls(
            base_url=text(BASE_URL_ENV, "base_url", SOT_BASE_URL).rstrip("/"),
            search_url_template=text(SEARCH_URL_ENV, "search_url_template"),
            search_method=text(SEARCH_METHOD_ENV, "search_method", "GET").upper(),
            search_body_template=text(SEARCH_BODY_ENV, "search_body_template"),
            decision_url_template=text(DECISION_URL_ENV, "decision_url_template"),
            decision_method=text(DECISION_METHOD_ENV, "decision_method", "GET").upper(),
            decision_body_template=text(DECISION_BODY_ENV, "decision_body_template"),
            results_path=text(RESULTS_PATH_ENV, "results_path"),
            id_path=text(ID_PATH_ENV, "id_path"),
            text_path=text(TEXT_PATH_ENV, "text_path"),
            total_path=text(TOTAL_PATH_ENV, "total_path"),
            next_cursor_path=text(NEXT_CURSOR_PATH_ENV, "next_cursor_path"),
            field_map=field_map,
            page_size=number(PAGE_SIZE_ENV, "page_size", DEFAULT_PAGE_SIZE),
            first_page=number(FIRST_PAGE_ENV, "first_page", 1),
            query=text(QUERY_ENV, "query"),
            decision_page_url_template=text(DECISION_URL_TEMPLATE_ENV, "decision_page_url_template"),
        )
