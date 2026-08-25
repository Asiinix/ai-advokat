"""The thin PRG.SOT source adapter.

It performs exactly the two requests the operator captured (search page and one
decision) and reshapes the responses using the configured dotted paths. It never
falls back to a guessed route, a guessed field name or a guessed corpus size.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping

from ..config import AuthProfile, sot_auth_profile
from ..exporters import html_to_text
from ..http_client import SourceClient
from . import decision_key as make_decision_key
from .model import SotDecisionPayload, SotDecisionRef, SotDecisionUnavailableError, SotDiscoveryError
from .source_config import METADATA_FIELDS, SotSourceConfig, dotted_get, dotted_values

# The scan is a background bulk reader on a shared subscription, so it stays far
# below anything a human session would produce.
DEFAULT_WORKERS = 1
MAX_WORKERS = 4


@dataclass(frozen=True)
class SotSearchPage:
    page: int
    refs: list[SotDecisionRef]
    total: int | None
    next_cursor: str | None
    raw_count: int
    payload_keys: tuple[str, ...]
    item_keys: tuple[str, ...]


@dataclass(frozen=True)
class SotPayloadSchema:
    root_keys: tuple[str, ...]
    nested_keys: tuple[tuple[str, tuple[str, ...]], ...]
    text_candidates: tuple[tuple[str, int], ...]


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def build_sot_client(
    config: SotSourceConfig,
    timeout: float = 30.0,
    retries: int = 3,
    retry_delay: float = 1.5,
    login_url: str | None = None,
) -> SourceClient:
    """A SourceClient bound to the PRG.SOT auth profile and its own cookie jar."""
    profile: AuthProfile = sot_auth_profile(login_url=login_url, return_url=f"{config.base_url}/")
    return SourceClient(
        timeout=timeout,
        retries=retries,
        retry_delay=retry_delay,
        auth=profile,
        # A shared subscription must never be pushed through its own quota, so a
        # 429 stops the worker with the source's own wait instead of retrying.
        raise_on_rate_limit=True,
    )


class SotSource:
    def __init__(self, client: SourceClient, config: SotSourceConfig) -> None:
        self.client = client
        self.config = config.validate()

    def fetch_search_page(self, page: int, cursor: str | None = None, offset: int | None = None) -> SotSearchPage:
        resolved_offset = offset if offset is not None else max(0, page - self.config.first_page) * self.config.page_size
        url, method, body = self.config.search_request(page, cursor, resolved_offset)
        payload, _response = self.client.request_json(url, method=method, json_body=body)
        items = dotted_get(payload, self.config.results_path)
        if items is None or not isinstance(items, (list, tuple)):
            raise SotDiscoveryError(
                f"PRG.SOT search page {page} has no list at '{self.config.results_path}'. "
                "Re-capture the response shape instead of treating this as the end of the corpus."
            )

        refs: list[SotDecisionRef] = []
        for position, item in enumerate(items):
            raw_id = dotted_get(item, self.config.id_path)
            if raw_id is None or str(raw_id).strip() == "":
                raise SotDiscoveryError(
                    f"PRG.SOT search page {page} item {position} has no id at '{self.config.id_path}'."
                )
            decision_id = str(raw_id).strip()
            refs.append(
                SotDecisionRef(
                    decision_id=decision_id,
                    decision_key=make_decision_key(decision_id),
                    source_url=self.config.decision_page_url(decision_id),
                    page=page,
                    position=position,
                    metadata=self.extract_metadata(item),
                )
            )

        total = None
        if self.config.total_path:
            raw_total = dotted_get(payload, self.config.total_path)
            if raw_total is not None:
                try:
                    total = int(raw_total)
                except (TypeError, ValueError):
                    total = None
        next_cursor = None
        if self.config.next_cursor_path:
            raw_cursor = dotted_get(payload, self.config.next_cursor_path)
            next_cursor = str(raw_cursor) if raw_cursor not in (None, "") else None
        payload_keys = tuple(sorted(str(key) for key in payload)) if isinstance(payload, Mapping) else ()
        first_item = items[0] if items else None
        item_keys = tuple(sorted(str(key) for key in first_item)) if isinstance(first_item, Mapping) else ()
        return SotSearchPage(
            page=page,
            refs=refs,
            total=total,
            next_cursor=next_cursor,
            raw_count=len(items),
            payload_keys=payload_keys,
            item_keys=item_keys,
        )

    def fetch_decision(self, ref: SotDecisionRef) -> SotDecisionPayload:
        url, method, body = self.config.decision_request(ref.decision_id)
        payload, _response = self.client.request_json(url, method=method, json_body=body)
        raw_values = dotted_values(payload, self.config.text_path)
        text_parts = []
        for raw_text in raw_values:
            if isinstance(raw_text, (Mapping, list, tuple)):
                continue
            cleaned = html_to_text(str(raw_text)).strip()
            if cleaned:
                text_parts.append(cleaned)
        text = "\n\n---\n\n".join(text_parts)
        if not text:
            raise SotDecisionUnavailableError(
                ref.decision_id,
                f"the response carries no text at '{self.config.text_path}'",
            )
        metadata = dict(ref.metadata)
        metadata.update({key: value for key, value in self.extract_metadata(payload).items() if value})
        metadata["document_count"] = len(text_parts)
        return SotDecisionPayload(
            decision_id=ref.decision_id,
            decision_key=ref.decision_key,
            source_url=ref.source_url or self.config.decision_page_url(ref.decision_id),
            text=text,
            raw=payload if isinstance(payload, Mapping) else {"payload": payload},
            metadata=metadata,
        )

    def inspect_decision_schema(self, ref: SotDecisionRef) -> SotPayloadSchema:
        """Read one decision and return only structural hints, never values."""
        url, method, body = self.config.decision_request(ref.decision_id)
        payload, _response = self.client.request_json(url, method=method, json_body=body)
        root_keys = tuple(sorted(str(key) for key in payload)) if isinstance(payload, Mapping) else ()
        nested: list[tuple[str, tuple[str, ...]]] = []
        text_candidates: list[tuple[str, int]] = []

        def walk(value: Any, path: str, depth: int) -> None:
            if isinstance(value, str):
                if len(value.strip()) >= 80:
                    text_candidates.append((path, len(value)))
                return
            if depth >= 3:
                return
            if isinstance(value, Mapping):
                if path:
                    nested.append((path, tuple(sorted(str(key) for key in value))))
                for key, child in value.items():
                    walk(child, f"{path}.{key}" if path else str(key), depth + 1)
                return
            if isinstance(value, (list, tuple)) and value:
                first = value[0]
                list_path = f"{path}[]"
                if isinstance(first, Mapping):
                    nested.append((list_path, tuple(sorted(str(key) for key in first))))
                walk(first, f"{path}.0", depth + 1)

        walk(payload, "", 0)
        return SotPayloadSchema(
            root_keys=root_keys,
            nested_keys=tuple(nested[:30]),
            text_candidates=tuple(text_candidates[:30]),
        )

    def extract_metadata(self, item: Any) -> dict[str, Any]:
        """Pull the configured court fields out of one source object."""
        found: dict[str, Any] = {}
        for name in METADATA_FIELDS:
            path = self.config.field_map.get(name)
            if not path:
                continue
            if name == "parties" and "|" in path:
                values = [dotted_get(item, candidate.strip()) for candidate in path.split("|")]
                parties = [value for value in values if value not in (None, "")]
                if parties:
                    found[name] = parties
                continue
            value = dotted_get(item, path)
            if value in (None, ""):
                continue
            found[name] = value if name == "parties" else _as_text(value)
        return found


def _as_text(value: Any) -> str:
    if isinstance(value, (str, int, float)):
        return str(value).strip()
    return json.dumps(value, ensure_ascii=False)
