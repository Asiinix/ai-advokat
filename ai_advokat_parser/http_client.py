from __future__ import annotations

import email.utils
import datetime as dt
import html
import json
import os
import re
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from http.cookiejar import CookieJar, DefaultCookiePolicy
from typing import Any, Mapping

from .config import (
    AUTH_HOST,
    AUTH_LOGIN_URL,
    AUTH_PASSWORD_ENV,
    AUTH_RETURN_APP,
    AUTH_RETURN_URL,
    AUTH_USERNAME_ENV,
    DEFAULT_HEADERS,
    AuthProfile,
)

RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
RATE_LIMIT_STATUSES = frozenset({429})
AUTH_STATUSES = frozenset({401})
LOGIN_PATH_MARKER = "/account/login"
LOGIN_FAILURE_COOLDOWN = 60.0
LOGIN_BODY_MARKERS = ("__RequestVerificationToken", "PersonalDataAgreement")
SUPPORTED_METHODS = ("GET", "POST")

# Vendors disagree on the spelling, so every known variant is read and the
# largest wait wins. A value above this cut-off is an absolute epoch second.
RESET_HEADERS = ("retry-after", "x-ratelimit-reset", "ratelimit-reset", "x-rate-limit-reset")
REMAINING_HEADERS = ("x-ratelimit-remaining", "ratelimit-remaining", "x-rate-limit-remaining")
LIMIT_HEADERS = ("x-ratelimit-limit", "ratelimit-limit", "x-rate-limit-limit")
EPOCH_CUTOFF = 1_000_000_000

TOKEN_INPUT_RE = re.compile(
    r"<input\b[^>]*name=[\"']__RequestVerificationToken[\"'][^>]*>",
    re.I,
)
VALUE_ATTR_RE = re.compile(r"(?<![-\w])value\s*=\s*[\"'](?P<value>[^\"']*)[\"']", re.I)
FORM_TAG_RE = re.compile(r"<form\b[^>]*>", re.I)
ACTION_ATTR_RE = re.compile(r"(?<![-\w])action\s*=\s*[\"'](?P<action>[^\"']*)[\"']", re.I)
PASSWORD_INPUT_RE = re.compile(r"<input\b[^>]*name=[\"']Password[\"'][^>]*>", re.I)


def default_auth_profile() -> AuthProfile:
    """The PRG.ZANGER profile every legacy call site keeps using.

    It is rebuilt from this module's constants on every call on purpose: those
    constants are the documented patch point for tests and for staging, and a
    profile captured once at import time would silently ignore them.
    """
    return AuthProfile(
        name="prg_zanger",
        login_url=AUTH_LOGIN_URL,
        return_url=AUTH_RETURN_URL,
        return_app=AUTH_RETURN_APP,
        username_env=AUTH_USERNAME_ENV,
        password_env=AUTH_PASSWORD_ENV,
        auth_host=AUTH_HOST,
    )


class SourceRequestError(RuntimeError):
    def __init__(self, url: str, message: str, status: int | None = None):
        super().__init__(message)
        self.url = url
        self.status = status


class SourceAuthError(SourceRequestError):
    """Raised when the source requires a PRG login that is missing or rejected.

    Messages never carry credentials, cookies, anti-forgery tokens or response
    bodies, because they end up in crawler logs and in the document store.
    """


class SourceAccessDeniedError(SourceRequestError):
    """Raised when a document stays login-walled for an authenticated session.

    Deliberately not a :class:`SourceAuthError`: the PRG session itself is
    healthy (the client just logged in again and replayed the request), so the
    verdict is about this one URL and must not abort a whole run.
    """


class SourceRateLimitError(SourceRequestError):
    """Raised when the source answers HTTP 429 and the caller opted in.

    Carries the wait the source itself asked for, so a long run can pause for
    exactly that long instead of guessing or hammering through the limit.
    """

    def __init__(self, url: str, message: str, rate_limit: "RateLimitInfo", status: int = 429):
        super().__init__(url, message, status=status)
        self.rate_limit = rate_limit


@dataclass(frozen=True)
class RateLimitInfo:
    """What the source told us about its own quota, in a comparable shape."""

    retry_after: float | None = None
    reset_at: float | None = None
    remaining: int | None = None
    limit: int | None = None

    def delay(self, now: float | None = None) -> float:
        """Seconds to wait before the next request, never negative."""
        moment = time.time() if now is None else now
        waits = [value for value in (self.retry_after,) if value is not None]
        if self.reset_at is not None:
            waits.append(self.reset_at - moment)
        return max([0.0] + waits)

    def describe(self) -> str:
        parts = []
        if self.retry_after is not None:
            parts.append(f"retry-after={self.retry_after:g}s")
        if self.reset_at is not None:
            parts.append(f"reset-in={max(0.0, self.reset_at - time.time()):.0f}s")
        if self.remaining is not None:
            parts.append(f"remaining={self.remaining}")
        if self.limit is not None:
            parts.append(f"limit={self.limit}")
        return ", ".join(parts) or "no quota headers"


def _header_value(headers: Mapping[str, str], names: tuple[str, ...]) -> str | None:
    lowered = {str(key).lower(): str(value) for key, value in dict(headers).items()}
    for name in names:
        value = lowered.get(name)
        if value is not None and value.strip():
            return value.strip()
    return None


def _parse_seconds(raw: str, now: float) -> tuple[float | None, float | None]:
    """Return (retry_after, reset_at) for one Retry-After/reset header value."""
    try:
        number = float(raw)
    except ValueError:
        try:
            moment = email.utils.parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            moment = None
        if moment is None:
            # PRG.SOT currently sends x-rate-limit-reset as ISO-8601, including
            # a seven-digit fractional second (for example
            # 2026-09-01T18:03:02.3333513Z), rather than an HTTP date.
            normalized = raw.strip()
            normalized = re.sub(
                r"(\.\d{6})\d+(?=(?:[Zz]|[+-]\d{2}:\d{2})$)",
                r"\1",
                normalized,
            )
            if normalized.endswith(("Z", "z")):
                normalized = f"{normalized[:-1]}+00:00"
            try:
                moment = dt.datetime.fromisoformat(normalized)
            except ValueError:
                return None, None
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=dt.timezone.utc)
        return None, moment.timestamp()
    if number > EPOCH_CUTOFF:
        return None, number
    return max(0.0, number), None


def parse_rate_limit(headers: Mapping[str, str], now: float | None = None) -> RateLimitInfo:
    """Read the quota headers of one response without trusting their spelling."""
    moment = time.time() if now is None else now
    retry_after: float | None = None
    reset_at: float | None = None
    for name in RESET_HEADERS:
        raw = _header_value(headers, (name,))
        if raw is None:
            continue
        seconds, epoch = _parse_seconds(raw, moment)
        if seconds is not None:
            retry_after = seconds if retry_after is None else max(retry_after, seconds)
        if epoch is not None:
            reset_at = epoch if reset_at is None else max(reset_at, epoch)

    def _int(names: tuple[str, ...]) -> int | None:
        raw = _header_value(headers, names)
        if raw is None:
            return None
        try:
            return int(float(raw))
        except ValueError:
            return None

    return RateLimitInfo(
        retry_after=retry_after,
        reset_at=reset_at,
        remaining=_int(REMAINING_HEADERS),
        limit=_int(LIMIT_HEADERS),
    )


@dataclass(frozen=True)
class ResponseText:
    url: str
    status: int
    text: str
    headers: Mapping[str, str] = field(default_factory=dict)

    @property
    def rate_limit(self) -> RateLimitInfo:
        return parse_rate_limit(self.headers)


@dataclass(frozen=True)
class Credentials:
    username: str
    password: str

    def __repr__(self) -> str:
        return "Credentials(username='***', password='***')"


def credentials_from_env(
    env: Mapping[str, str] | None = None,
    profile: AuthProfile | None = None,
) -> Credentials | None:
    """Read the credentials of one auth profile from the environment.

    Returns None when neither variable is set (anonymous mode) and raises when
    only one of them is set, so a half-configured deploy fails loudly.
    """
    source = os.environ if env is None else env
    resolved = profile or default_auth_profile()
    username = (source.get(resolved.username_env) or "").strip()
    password = source.get(resolved.password_env) or ""
    if not username and not password:
        return None
    if not username or not password:
        missing = resolved.username_env if not username else resolved.password_env
        raise SourceAuthError(
            resolved.login_url,
            f"Incomplete PRG credentials: {missing} is empty. "
            f"Set both {resolved.username_env} and {resolved.password_env}, or neither.",
        )
    return Credentials(username=username, password=password)


def looks_like_login_response(url: str, text: str, auth_host: str = AUTH_HOST) -> bool:
    parsed = urllib.parse.urlparse(url)
    if (parsed.hostname or "").lower() == auth_host:
        return True
    if LOGIN_PATH_MARKER in parsed.path.lower():
        return True
    return all(marker in text for marker in LOGIN_BODY_MARKERS) and PASSWORD_INPUT_RE.search(text) is not None


def looks_like_html(text: str) -> bool:
    head = text.lstrip()[:200].lower()
    return head.startswith("<!doctype html") or head.startswith("<html") or "<body" in head


def same_origin(left: str, right: str) -> bool:
    left_url = urllib.parse.urlparse(left)
    right_url = urllib.parse.urlparse(right)
    return (left_url.scheme.lower(), left_url.netloc.lower()) == (
        right_url.scheme.lower(),
        right_url.netloc.lower(),
    )


def parse_login_form(text: str, base_url: str) -> tuple[str, str]:
    """Return (form action URL, anti-forgery token) of the PRG login page."""
    token_input = TOKEN_INPUT_RE.search(text)
    if token_input is None:
        raise SourceAuthError(base_url, "PRG login page does not contain an anti-forgery token field.")
    value = VALUE_ATTR_RE.search(token_input.group(0))
    token = html.unescape(value.group("value")) if value else ""
    if not token:
        raise SourceAuthError(base_url, "PRG login page returned an empty anti-forgery token.")

    action = ""
    for form_tag in FORM_TAG_RE.finditer(text):
        if form_tag.start() > token_input.start():
            break
        action_attr = ACTION_ATTR_RE.search(form_tag.group(0))
        action = html.unescape(action_attr.group("action")) if action_attr else ""
    action_url = urllib.parse.urljoin(base_url, action) if action else base_url
    if not same_origin(action_url, base_url):
        raise SourceAuthError(base_url, "PRG login form returned an unexpected action origin.")
    return action_url, token


class SourceClient:
    def __init__(
        self,
        timeout: float = 30.0,
        retries: int = 3,
        retry_delay: float = 1.5,
        headers: dict[str, str] | None = None,
        credentials: Credentials | None = None,
        cookie_jar: CookieJar | None = None,
        auth: AuthProfile | None = None,
        raise_on_rate_limit: bool = False,
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self.headers = {**DEFAULT_HEADERS, **(headers or {})}
        # None means "the PRG.ZANGER defaults as they are right now", which is
        # what the legacy call sites and their module-level patches expect.
        self._auth = auth
        self.raise_on_rate_limit = raise_on_rate_limit
        self.credentials = credentials_from_env(profile=self.auth) if credentials is None else credentials
        # CookieJar guards its own storage with a lock, and the opener handlers
        # are stateless, so one jar/opener can be shared by all worker threads.
        cookie_policy = DefaultCookiePolicy(
            strict_ns_unverifiable=False,
            strict_rfc2965_unverifiable=False,
        )
        self.cookie_jar = CookieJar(cookie_policy) if cookie_jar is None else cookie_jar
        self._opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookie_jar))
        self._auth_lock = threading.Lock()
        self._auth_generation = 0
        self._login_failure: tuple[str, int | None] | None = None
        self._login_failed_at = 0.0
        self._last_rate_limit = RateLimitInfo()

    @property
    def auth(self) -> AuthProfile:
        return self._auth if self._auth is not None else default_auth_profile()

    @property
    def uses_authentication(self) -> bool:
        return self.credentials is not None

    @property
    def authenticated(self) -> bool:
        return self._auth_generation > 0

    @property
    def last_rate_limit(self) -> RateLimitInfo:
        """Quota headers of the most recent response, for pacing decisions."""
        return self._last_rate_limit

    def authenticate(self) -> bool:
        """Validate configured credentials before starting a batch of work."""
        if self.credentials is None:
            return False
        self._ensure_authenticated()
        return True

    def get_text(self, url: str, query: dict[str, Any] | None = None) -> ResponseText:
        if query:
            separator = "&" if urllib.parse.urlparse(url).query else "?"
            url = f"{url}{separator}{urllib.parse.urlencode(query, doseq=True)}"
        return self._request_text(url, allow_reauth=True)

    def get_json(self, url: str, query: dict[str, Any] | None = None) -> Any:
        response = self.get_text(url, query=query)
        return self._decode_json(response)

    def request_json(
        self,
        url: str,
        method: str = "GET",
        json_body: Any = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[Any, ResponseText]:
        """Run one GET/POST and decode JSON, returning the raw response too.

        Search APIs are frequently POST-only, and the caller needs the response
        headers to honour the source rate limit, so this returns both halves.
        """
        upper = method.upper()
        if upper not in SUPPORTED_METHODS:
            raise ValueError(f"Unsupported HTTP method '{method}'. Supported: {', '.join(SUPPORTED_METHODS)}.")
        data = None
        extra = dict(headers or {})
        if json_body is not None:
            data = json.dumps(json_body, ensure_ascii=False).encode("utf-8")
            extra.setdefault("Content-Type", "application/json")
        response = self._request_text(url, allow_reauth=True, method=upper, data=data, extra_headers=extra)
        return self._decode_json(response), response

    def _decode_json(self, response: ResponseText) -> Any:
        try:
            return json.loads(response.text)
        except json.JSONDecodeError as exc:
            if looks_like_login_response(response.url, response.text, self.auth.auth_host):
                raise self._auth_error(response.url, status=response.status) from exc
            if looks_like_html(response.text):
                raise SourceRequestError(
                    response.url,
                    f"Expected JSON but the source returned an HTML page (HTTP {response.status}).",
                    status=response.status,
                ) from exc
            raise SourceRequestError(
                response.url,
                f"Invalid JSON response from the source (HTTP {response.status}).",
                status=response.status,
            ) from exc

    def _request_text(
        self,
        url: str,
        allow_reauth: bool,
        method: str = "GET",
        data: bytes | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> ResponseText:
        generation = self._ensure_authenticated()
        request_headers = {**self.headers, **(extra_headers or {})}

        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            request = urllib.request.Request(url, data=data, headers=request_headers, method=method)
            try:
                response = self._open(request)
            except urllib.error.HTTPError as exc:
                body_headers = dict(exc.headers.items()) if exc.headers else {}
                exc.read()
                if exc.code in AUTH_STATUSES:
                    retried = self._retry_after_reauth(
                        url, allow_reauth, generation, method=method, data=data, extra_headers=extra_headers
                    )
                    if retried is not None:
                        return retried
                    raise self._auth_error(url, status=exc.code, after_reauth=not allow_reauth) from exc
                if exc.code in RATE_LIMIT_STATUSES and self.raise_on_rate_limit:
                    rate_limit = parse_rate_limit(body_headers)
                    self._last_rate_limit = rate_limit
                    raise SourceRateLimitError(
                        url,
                        f"Source rate limit reached (HTTP {exc.code}): {rate_limit.describe()}.",
                        rate_limit,
                        status=exc.code,
                    ) from exc
                if exc.code in RETRYABLE_STATUSES and attempt < self.retries:
                    time.sleep(self.retry_delay * attempt)
                    continue
                raise SourceRequestError(
                    url,
                    f"Source request failed with HTTP {exc.code}.",
                    status=exc.code,
                ) from exc
            except (urllib.error.URLError, socket.timeout, TimeoutError) as exc:
                last_error = exc
                if attempt < self.retries:
                    time.sleep(self.retry_delay * attempt)
                    continue
                raise SourceRequestError(url, str(exc)) from exc

            if looks_like_login_response(response.url, response.text, self.auth.auth_host):
                retried = self._retry_after_reauth(
                    url, allow_reauth, generation, method=method, data=data, extra_headers=extra_headers
                )
                if retried is not None:
                    return retried
                raise self._auth_error(url, status=response.status, after_reauth=not allow_reauth)
            return response

        raise SourceRequestError(url, str(last_error or "Unknown request error"))

    def _retry_after_reauth(
        self,
        url: str,
        allow_reauth: bool,
        generation: int,
        method: str = "GET",
        data: bytes | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> ResponseText | None:
        """Log in again once and replay the request; None when that is not allowed."""
        if not allow_reauth or self.credentials is None:
            return None
        self._ensure_authenticated(after=generation)
        return self._request_text(
            url, allow_reauth=False, method=method, data=data, extra_headers=extra_headers
        )

    def _ensure_authenticated(self, after: int | None = None) -> int:
        """Authenticate if needed and return the session generation used.

        Only one thread logs in at a time; the others reuse the session that was
        just established instead of starting their own login.
        """
        if self.credentials is None:
            return 0
        with self._auth_lock:
            if self._auth_generation > 0 and (after is None or after != self._auth_generation):
                return self._auth_generation
            self._raise_cached_login_failure()
            try:
                self._perform_login(self.credentials)
            except SourceAuthError as exc:
                # Remember the rejection for a while: without it every queued
                # document would fire its own login attempt at the source.
                self._login_failure = (str(exc), exc.status)
                self._login_failed_at = time.monotonic()
                raise
            self._login_failure = None
            self._auth_generation += 1
            return self._auth_generation

    def _raise_cached_login_failure(self) -> None:
        if self._login_failure is None:
            return
        if time.monotonic() - self._login_failed_at >= LOGIN_FAILURE_COOLDOWN:
            self._login_failure = None
            return
        message, status = self._login_failure
        raise SourceAuthError(self.auth.login_url, message, status=status)

    def _perform_login(self, credentials: Credentials) -> None:
        for attempt in range(1, self.retries + 1):
            try:
                self._perform_login_once(credentials)
                return
            except SourceAuthError as exc:
                transient = exc.status is None or exc.status in RETRYABLE_STATUSES
                if not transient or attempt >= self.retries:
                    raise
                time.sleep(self.retry_delay * attempt)
        raise SourceAuthError(self.auth.login_url, f"PRG login failed after {self.retries} attempts.")

    def _perform_login_once(self, credentials: Credentials) -> None:
        profile = self.auth
        query = urllib.parse.urlencode({"returnUrl": profile.return_url, "returnApp": profile.return_app})
        page = self._login_request(
            urllib.request.Request(f"{profile.login_url}?{query}", headers=self.headers, method="GET"),
            stage="login page request",
        )
        if not same_origin(page.url, profile.login_url):
            raise SourceAuthError(profile.login_url, "PRG login page redirected to an unexpected origin.")
        action_url, token = parse_login_form(page.text, page.url)
        page_url = urllib.parse.urlparse(page.url)

        payload = urllib.parse.urlencode(
            {
                "__RequestVerificationToken": token,
                "Login": credentials.username,
                "Password": credentials.password,
                "ReturnApp": profile.return_app,
                "ReturnUrl": profile.return_url,
                "PersonalDataAgreement": "true",
                "Remember": "false",
            }
        ).encode("utf-8")
        headers = {
            **self.headers,
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": f"{page_url.scheme}://{page_url.netloc}",
            "Referer": page.url,
        }
        response = self._login_request(
            urllib.request.Request(action_url, data=payload, headers=headers, method="POST"),
            stage="login",
        )
        if looks_like_login_response(response.url, response.text, profile.auth_host):
            raise SourceAuthError(
                profile.login_url,
                "PRG login was rejected: the login form was returned instead of a redirect to "
                f"{profile.return_url}. Check {profile.username_env}/{profile.password_env}.",
                status=response.status,
            )
        if not same_origin(response.url, profile.return_url) or not self._has_cookie_for(profile.return_url):
            raise SourceAuthError(
                profile.login_url,
                f"PRG login did not establish a session cookie for {profile.return_url}.",
                status=response.status,
            )

    def _login_request(self, request: urllib.request.Request, stage: str) -> ResponseText:
        """Run a login step, never surfacing the request body or the response body."""
        try:
            return self._open(request)
        except urllib.error.HTTPError as exc:
            exc.read()
            raise SourceAuthError(
                self.auth.login_url,
                f"PRG {stage} failed with HTTP {exc.code}.",
                status=exc.code,
            ) from exc
        except (urllib.error.URLError, socket.timeout, TimeoutError) as exc:
            raise SourceAuthError(self.auth.login_url, f"PRG {stage} failed with a network error.") from exc

    def _has_cookie_for(self, url: str) -> bool:
        host = (urllib.parse.urlparse(url).hostname or "").lower()
        self.cookie_jar.clear_expired_cookies()
        for cookie in self.cookie_jar:
            domain = cookie.domain.lstrip(".").lower()
            if host == domain or host.endswith(f".{domain}"):
                return True
        return False

    def _open(self, request: urllib.request.Request) -> ResponseText:
        with self._opener.open(request, timeout=self.timeout) as response:
            raw = response.read()
            charset = response.headers.get_content_charset() or "utf-8"
            headers = dict(response.headers.items())
            self._last_rate_limit = parse_rate_limit(headers)
            return ResponseText(
                url=response.geturl(),
                status=int(response.status),
                text=raw.decode(charset, errors="replace"),
                headers=headers,
            )

    def _auth_error(
        self,
        url: str,
        status: int | None = None,
        after_reauth: bool = False,
    ) -> SourceRequestError:
        profile = self.auth
        if self.credentials is None:
            return SourceAuthError(
                url,
                "The source returned a login page instead of content. "
                f"Set {profile.username_env} and {profile.password_env} to read protected PRG documents.",
                status=status,
            )
        if after_reauth:
            # The login itself succeeded, so this URL is simply out of reach for
            # the configured account: a per-document verdict, not a dead session.
            return SourceAccessDeniedError(
                url,
                "The source returned a login page even after re-authentication; "
                "this document is not available to the configured PRG account.",
                status=status,
            )
        return SourceAuthError(
            url,
            "The source returned a login page and the PRG session could not be established.",
            status=status,
        )
