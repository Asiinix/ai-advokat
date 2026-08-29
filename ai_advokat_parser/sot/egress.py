"""Configurable egress partitions for the PRG.SOT reader.

PRG explicitly authorized rotating the egress IP and running parallel sessions
once a quota is spent, so the pool below spreads requests over independent
partitions: each one owns its own SourceClient, cookie jar and login session,
optionally behind its own HTTP(S) proxy.

Two rules keep this safe:

- the descriptors carry only a safe partition id and the *name* of the
  environment variable holding the proxy URL; the URL itself (which may embed
  proxy credentials) is resolved from that separate variable and never appears
  in descriptors, errors, logs or diagnostics;
- every per-partition quota answer is honoured exactly: a limited partition
  rests until its own reset. Ordinary callers receive one aggregate
  SourceRateLimitError when all routes rest; the supervised corpus scan keeps
  the same pool alive, sleeps until the earliest route recovers, and therefore
  never forgets the longer cooldowns.

The default is a single explicit direct partition. Source semantics stay the
same, while ambient proxy variables are deliberately ignored so routing is
controlled only by these descriptors.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
import urllib.parse
from dataclasses import dataclass
from typing import Callable, Mapping

from ..http_client import (
    RETRYABLE_STATUSES,
    RateLimitInfo,
    ResponseText,
    SourceAccessDeniedError,
    SourceAuthError,
    SourceAuthNetworkError,
    SourceClient,
    SourceRateLimitError,
    SourceRequestError,
)
from .adapter import build_sot_client
from .source_config import SotConfigError, SotSourceConfig

PARTITIONS_ENV = "AI_ADVOCAT_SOT_EGRESS_PARTITIONS"

# A partition whose quota is spent but whose response named no reset still has
# to rest for a while, otherwise the pool would bounce straight back to it.
DEFAULT_COOLDOWN_SECONDS = 60.0

# How long a partition rests after a failure of its own egress path (a network
# error or its proxy demanding credentials). Long enough to stop hammering a
# dead proxy, short enough that a transient outage heals within one run.
QUARANTINE_SECONDS = 300.0
# Long source resets are observed in bounded heartbeat slices. The pool keeps
# the real cooldown timestamp and never contacts a resting partition early;
# slicing only keeps the worker observable and interruptible.
MAX_WAIT_SLICE_SECONDS = 300.0
PROXY_AUTH_STATUS = 407

DEFAULT_PARTITION_ID = "direct"
PARTITION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
DESCRIPTOR_KEYS = frozenset({"id", "proxy_env", "enabled"})


@dataclass(frozen=True)
class SotEgressPartition:
    """One egress descriptor: a safe id plus the *name* of the proxy variable."""

    partition_id: str
    proxy_env: str | None = None
    enabled: bool = True


def partitions_from_env(env: Mapping[str, str] | None = None) -> tuple[SotEgressPartition, ...]:
    """Parse the JSON partition descriptors; a single direct partition when unset."""
    source = os.environ if env is None else env
    raw = (source.get(PARTITIONS_ENV) or "").strip()
    if not raw:
        return (SotEgressPartition(DEFAULT_PARTITION_ID),)

    try:
        decoded = json.loads(raw)
    except ValueError as exc:
        raise SotConfigError(
            f"{PARTITIONS_ENV} must be a JSON array of partition descriptors."
        ) from exc
    if not isinstance(decoded, list) or not decoded:
        raise SotConfigError(f"{PARTITIONS_ENV} must be a non-empty JSON array of partition descriptors.")

    problems: list[str] = []
    partitions: list[SotEgressPartition] = []
    seen: set[str] = set()
    for position, item in enumerate(decoded):
        if not isinstance(item, Mapping):
            problems.append(f"descriptor {position} is not a JSON object")
            continue
        unknown = sorted(set(str(key) for key in item) - DESCRIPTOR_KEYS)
        if unknown:
            # Refusing unknown keys is what stops an operator from pasting the
            # proxy URL (and its credentials) straight into the descriptor.
            problems.append(
                f"descriptor {position} has unsupported keys: {', '.join(unknown)}. "
                f"Supported: {', '.join(sorted(DESCRIPTOR_KEYS))}; proxy URLs belong in the "
                "environment variable named by proxy_env, never in the descriptor"
            )
            continue
        partition_id = str(item.get("id") or "").strip()
        if not PARTITION_ID_RE.match(partition_id):
            problems.append(
                f"descriptor {position} needs an 'id' of 1-64 letters, digits, '.', '_' or '-'"
            )
            continue
        if partition_id in seen:
            problems.append(f"partition id '{partition_id}' is declared more than once")
            continue
        seen.add(partition_id)
        proxy_env = item.get("proxy_env")
        if proxy_env is not None:
            proxy_env = str(proxy_env).strip()
            if not ENV_NAME_RE.match(proxy_env):
                problems.append(
                    f"partition '{partition_id}': proxy_env must be an environment variable "
                    "name (UPPER_SNAKE_CASE)"
                )
                continue
        enabled = item.get("enabled", True)
        if not isinstance(enabled, bool):
            problems.append(f"partition '{partition_id}': enabled must be true or false")
            continue
        partitions.append(SotEgressPartition(partition_id, proxy_env or None, enabled))

    if problems:
        raise SotConfigError(f"Invalid {PARTITIONS_ENV}: " + "; ".join(problems))
    if not any(partition.enabled for partition in partitions):
        raise SotConfigError(f"{PARTITIONS_ENV} disables every partition; enable at least one.")
    return tuple(partitions)


def resolve_proxy_url(partition: SotEgressPartition, env: Mapping[str, str] | None = None) -> str | None:
    """Read one partition's proxy URL from its own environment variable.

    The returned value is handed straight to the SourceClient and must never be
    logged or embedded in an error message: it may carry proxy credentials.
    """
    if partition.proxy_env is None:
        return None
    source = os.environ if env is None else env
    raw = (source.get(partition.proxy_env) or "").strip()
    if not raw:
        raise SotConfigError(
            f"Egress partition '{partition.partition_id}' names {partition.proxy_env}, "
            "but that environment variable is not set."
        )
    parsed = urllib.parse.urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise SotConfigError(
            f"Egress partition '{partition.partition_id}': {partition.proxy_env} must be an "
            "absolute http(s) proxy URL. The configured value is not echoed here on purpose."
        )
    return raw


def _is_partition_local(exc: SourceRequestError) -> bool:
    """True for failures of this egress path, not of the requested content.

    A network-level failure (no HTTP status) or a proxy demanding credentials
    (HTTP 407) says nothing about the URL: another partition may well succeed.
    Auth verdicts and ordinary content/status errors (404, 500, bad JSON) are
    shared truths and must surface unchanged instead of being retried through
    every partition.
    """
    if isinstance(exc, (SourceAccessDeniedError, SourceRateLimitError)):
        return False
    if isinstance(exc, SourceAuthError):
        # A rejected login has an HTTP verdict (normally 200 with the login
        # form, or 401) and is shared across partitions. Contract/security
        # failures may also lack a status, so only the explicit network subtype,
        # a proxy-auth challenge, or a retryable upstream status may fail over.
        return (
            isinstance(exc, SourceAuthNetworkError)
            or exc.status == PROXY_AUTH_STATUS
            or exc.status in RETRYABLE_STATUSES
        )
    return exc.status is None or exc.status == PROXY_AUTH_STATUS


class _PartitionState:
    __slots__ = ("descriptor", "client", "cooldown_until", "last_info")

    def __init__(self, descriptor: SotEgressPartition, client: SourceClient) -> None:
        self.descriptor = descriptor
        self.client = client
        self.cooldown_until: float | None = None
        self.last_info: RateLimitInfo | None = None


class SotEgressPool:
    """Thread-safe failover over independent PRG.SOT sessions.

    Requests stick to one partition until it reports its quota is spent (an
    HTTP 429, or a successful response with remaining=0); the pool then rests
    that partition until its own reset and continues on another enabled one.
    """

    def __init__(
        self,
        partitions: tuple[SotEgressPartition, ...],
        client_factory: Callable[[SotEgressPartition], SourceClient],
        time_source: Callable[[], float] = time.time,
        sleeper: Callable[[float], object] = time.sleep,
        wait_when_exhausted: bool = False,
    ) -> None:
        enabled = [partition for partition in partitions if partition.enabled]
        if not enabled:
            raise SotConfigError("SotEgressPool needs at least one enabled partition.")
        self._states = [_PartitionState(partition, client_factory(partition)) for partition in enabled]
        self._time = time_source
        self._sleep = sleeper
        self._wait_when_exhausted = wait_when_exhausted
        self._lock = threading.Lock()
        self._current = 0

    # --- the facade the adapter and the probe already rely on --------------

    @property
    def partition_ids(self) -> tuple[str, ...]:
        return tuple(state.descriptor.partition_id for state in self._states)

    @property
    def auth(self):
        return self._states[0].client.auth

    @property
    def uses_authentication(self) -> bool:
        return self._states[0].client.uses_authentication

    @property
    def authenticated_landing(self) -> ResponseText | None:
        return self._states[0].client.authenticated_landing

    @property
    def last_rate_limit(self) -> RateLimitInfo:
        with self._lock:
            state = self._states[self._current]
        return state.client.last_rate_limit

    def authenticate(self) -> bool:
        """Log every partition in, proving each egress path works end to end.

        A partition whose login is throttled rests until its own reset instead
        of failing the pool; only when no partition logged in at all does the
        rate-limit error surface to an ordinary caller. The supervised scan
        instead waits inside this pool, preserving every partition cooldown.
        """
        while True:
            available = self._available_states()
            if not available:
                if self._wait_when_exhausted:
                    self._wait_for_next_partition("авторизация")
                    continue
                # A repeated explicit authenticate() while the pool is still
                # cooling gets the same aggregate signal as a data request.
                self._select(self._states[0].client.auth.login_url)
                available = self._available_states()

            result = False
            unauthenticated = False
            limited: SourceRateLimitError | None = None
            limited_states: list[_PartitionState] = []
            unavailable: SourceAuthError | None = None
            for state in available:
                try:
                    authenticated = state.client.authenticate()
                    result = authenticated or result
                    unauthenticated = not authenticated or unauthenticated
                except SourceRateLimitError as exc:
                    self._mark_limited(state, exc.rate_limit)
                    limited = exc
                    limited_states.append(state)
                except SourceAuthError as exc:
                    if not _is_partition_local(exc):
                        raise
                    self._quarantine(state)
                    unavailable = exc
            if result:
                return True
            if unauthenticated:
                # Missing credentials are configuration, not an egress outage.
                return False
            if self._wait_when_exhausted:
                self._wait_for_next_partition("авторизация")
                continue
            if limited is not None:
                raise self._aggregate_login_limit(limited, limited_states) from limited
            if unavailable is not None:
                raise unavailable
            return False

    def request_json(
        self,
        url: str,
        method: str = "GET",
        json_body=None,
        headers: dict[str, str] | None = None,
    ):
        while True:
            try:
                state = self._select(url)
            except SourceRateLimitError:
                if not self._wait_when_exhausted:
                    raise
                self._wait_for_next_partition("запрос")
                continue
            try:
                payload, response = state.client.request_json(
                    url, method=method, json_body=json_body, headers=headers
                )
            except SourceRateLimitError as exc:
                self._mark_limited(state, exc.rate_limit)
                continue
            except SourceRequestError as exc:
                if not _is_partition_local(exc):
                    raise
                self._quarantine(state)
                if not self._any_available():
                    if not self._wait_when_exhausted:
                        # Nothing left to fail over to: surface the real
                        # failure instead of dressing it up as a rate limit.
                        raise
                    self._wait_for_next_partition("запрос")
                continue
            info = response.rate_limit
            if info.remaining is not None and info.remaining <= 0:
                # This answer still counts, but the partition is spent: rest it
                # now so the next request already goes out through another one.
                self._mark_limited(state, info)
            return payload, response

    def get_json(self, url: str, query: dict | None = None):
        if query:
            separator = "&" if urllib.parse.urlparse(url).query else "?"
            url = f"{url}{separator}{urllib.parse.urlencode(query, doseq=True)}"
        payload, _response = self.request_json(url)
        return payload

    # --- safe diagnostics ---------------------------------------------------

    def diagnostics(self) -> list[str]:
        """One safe line per partition: id, quota state, never a URL or cookie."""
        now = self._time()
        lines = []
        with self._lock:
            for state in self._states:
                descriptor = state.descriptor
                if state.cooldown_until is not None and state.cooldown_until > now:
                    availability = f"cooling {state.cooldown_until - now:.0f}s"
                else:
                    availability = "available"
                quota = (state.last_info or state.client.last_rate_limit).describe()
                proxy = descriptor.proxy_env or "-"
                lines.append(f"{descriptor.partition_id}: proxy_env={proxy}, {availability}, {quota}")
        return lines

    # --- selection and cooldown ---------------------------------------------

    def _select(self, url: str) -> _PartitionState:
        now = self._time()
        switched: str | None = None
        with self._lock:
            count = len(self._states)
            for offset in range(count):
                index = (self._current + offset) % count
                state = self._states[index]
                if state.cooldown_until is not None and state.cooldown_until <= now:
                    state.cooldown_until = None
                if state.cooldown_until is None:
                    if index != self._current:
                        self._current = index
                        switched = state.descriptor.partition_id
                    selected = state
                    break
            else:
                earliest = min(state.cooldown_until for state in self._states)
                described = ", ".join(
                    f"{state.descriptor.partition_id} (reset-in {max(0.0, state.cooldown_until - now):.0f}s)"
                    for state in self._states
                )
                selected = None
        if selected is None:
            raise SourceRateLimitError(
                url,
                f"All PRG.SOT egress partitions are rate limited: {described}.",
                RateLimitInfo(reset_at=earliest, remaining=0),
            )
        if switched is not None:
            print(f"[sot] egress: переключение на партицию {switched}")
        return selected

    def _available_states(self) -> list[_PartitionState]:
        now = self._time()
        with self._lock:
            available = []
            for state in self._states:
                if state.cooldown_until is not None and state.cooldown_until <= now:
                    state.cooldown_until = None
                if state.cooldown_until is None:
                    available.append(state)
            return available

    def _wait_for_next_partition(self, stage: str) -> None:
        now = self._time()
        with self._lock:
            waits = [
                max(0.0, state.cooldown_until - now)
                for state in self._states
                if state.cooldown_until is not None
            ]
        # Every temporary failure records a positive cooldown. Keep a defensive
        # one-second floor so a clock edge can never turn this into a busy loop.
        remaining = min(waits) if waits else DEFAULT_COOLDOWN_SECONDS
        delay = max(1.0, min(remaining, MAX_WAIT_SLICE_SECONDS))
        print(
            f"[sot] egress: все партиции временно недоступны ({stage}); "
            f"следующая проверка через {delay:.0f}s"
        )
        self._sleep(delay)

    def _aggregate_login_limit(
        self,
        limited: SourceRateLimitError,
        limited_states: list[_PartitionState],
    ) -> SourceRateLimitError:
        """One quota signal with the earliest reset among throttled logins."""
        now = self._time()
        with self._lock:
            resets = [
                (state.descriptor.partition_id, state.cooldown_until)
                for state in limited_states
                if state.cooldown_until is not None
            ]
        earliest = min(reset for _partition_id, reset in resets)
        described = ", ".join(
            f"{partition_id} (reset-in {max(0.0, reset - now):.0f}s)"
            for partition_id, reset in resets
        )
        return SourceRateLimitError(
            limited.url,
            f"No PRG.SOT egress partition could authenticate; quota-limited: {described}.",
            RateLimitInfo(reset_at=earliest, remaining=0),
        )

    def _mark_limited(self, state: _PartitionState, info: RateLimitInfo) -> None:
        now = self._time()
        delay = info.delay(now)
        if delay <= 0:
            delay = DEFAULT_COOLDOWN_SECONDS
        self._rest(state, now + delay, info)
        print(
            f"[sot] egress {state.descriptor.partition_id}: квота исчерпана "
            f"({info.describe()}), партиция отдыхает {delay:.0f}s"
        )

    def _quarantine(self, state: _PartitionState) -> None:
        self._rest(state, self._time() + QUARANTINE_SECONDS)
        print(
            f"[sot] egress {state.descriptor.partition_id}: сбой egress-пути "
            f"(сеть или прокси), карантин {QUARANTINE_SECONDS:.0f}s"
        )

    def _rest(self, state: _PartitionState, until: float, info: RateLimitInfo | None = None) -> None:
        with self._lock:
            # Marks can race: a response that was already in flight when the
            # partition got its long reset may carry a shorter hint, and a
            # shorter mark must never cut an already recorded rest short.
            if state.cooldown_until is None or until > state.cooldown_until:
                state.cooldown_until = until
            if info is not None:
                state.last_info = info

    def _any_available(self) -> bool:
        now = self._time()
        with self._lock:
            return any(
                state.cooldown_until is None or state.cooldown_until <= now
                for state in self._states
            )


def build_sot_egress_client(
    config: SotSourceConfig,
    timeout: float = 30.0,
    retries: int = 3,
    retry_delay: float = 1.5,
    login_url: str | None = None,
    env: Mapping[str, str] | None = None,
    wait_when_exhausted: bool = False,
) -> SourceClient | SotEgressPool:
    """One SourceClient for ordinary direct use, a pool for routing or waiting.

    Proxy URLs are resolved here, before anything is opened or written, so a
    partition that names a missing variable fails the run loudly and early.
    """
    partitions = partitions_from_env(env)
    enabled = [partition for partition in partitions if partition.enabled]

    def factory(partition: SotEgressPartition) -> SourceClient:
        resolved = resolve_proxy_url(partition, env)
        return build_sot_client(
            config,
            timeout=timeout,
            retries=retries,
            retry_delay=retry_delay,
            login_url=login_url,
            # An explicit "" marks the direct partition as proxy-less: its
            # traffic never inherits ambient http_proxy/https_proxy, because
            # proxy URLs come only from the variable named by proxy_env.
            proxy_url=resolved if resolved is not None else "",
        )

    if len(enabled) == 1 and not wait_when_exhausted:
        return factory(enabled[0])
    return SotEgressPool(
        tuple(enabled),
        factory,
        wait_when_exhausted=wait_when_exhausted,
    )


__all__ = [
    "DEFAULT_COOLDOWN_SECONDS",
    "MAX_WAIT_SLICE_SECONDS",
    "PARTITIONS_ENV",
    "QUARANTINE_SECONDS",
    "SotEgressPartition",
    "SotEgressPool",
    "build_sot_egress_client",
    "partitions_from_env",
    "resolve_proxy_url",
]
