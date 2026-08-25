from __future__ import annotations

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
from dataclasses import dataclass
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
)

RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
AUTH_STATUSES = frozenset({401})
LOGIN_PATH_MARKER = "/account/login"
LOGIN_FAILURE_COOLDOWN = 60.0
LOGIN_BODY_MARKERS = ("__RequestVerificationToken", "PersonalDataAgreement")

TOKEN_INPUT_RE = re.compile(
    r"<input\b[^>]*name=[\"']__RequestVerificationToken[\"'][^>]*>",
    re.I,
)
VALUE_ATTR_RE = re.compile(r"(?<![-\w])value\s*=\s*[\"'](?P<value>[^\"']*)[\"']", re.I)
FORM_TAG_RE = re.compile(r"<form\b[^>]*>", re.I)
ACTION_ATTR_RE = re.compile(r"(?<![-\w])action\s*=\s*[\"'](?P<action>[^\"']*)[\"']", re.I)
PASSWORD_INPUT_RE = re.compile(r"<input\b[^>]*name=[\"']Password[\"'][^>]*>", re.I)


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


@dataclass(frozen=True)
class ResponseText:
    url: str
    status: int
    text: str


@dataclass(frozen=True)
class Credentials:
    username: str
    password: str

    def __repr__(self) -> str:
        return "Credentials(username='***', password='***')"


def credentials_from_env(env: Mapping[str, str] | None = None) -> Credentials | None:
    """Read PRG credentials from the environment.

    Returns None when neither variable is set (anonymous mode) and raises when
    only one of them is set, so a half-configured deploy fails loudly.
    """
    source = os.environ if env is None else env
    username = (source.get(AUTH_USERNAME_ENV) or "").strip()
    password = source.get(AUTH_PASSWORD_ENV) or ""
    if not username and not password:
        return None
    if not username or not password:
        missing = AUTH_USERNAME_ENV if not username else AUTH_PASSWORD_ENV
        raise SourceAuthError(
            AUTH_LOGIN_URL,
            f"Incomplete PRG credentials: {missing} is empty. "
            f"Set both {AUTH_USERNAME_ENV} and {AUTH_PASSWORD_ENV}, or neither.",
        )
    return Credentials(username=username, password=password)


def looks_like_login_response(url: str, text: str) -> bool:
    parsed = urllib.parse.urlparse(url)
    if (parsed.hostname or "").lower() == AUTH_HOST:
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
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self.headers = {**DEFAULT_HEADERS, **(headers or {})}
        self.credentials = credentials_from_env() if credentials is None else credentials
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

    @property
    def uses_authentication(self) -> bool:
        return self.credentials is not None

    @property
    def authenticated(self) -> bool:
        return self._auth_generation > 0

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
        return self._get_text(url, allow_reauth=True)

    def get_json(self, url: str, query: dict[str, Any] | None = None) -> Any:
        response = self.get_text(url, query=query)
        try:
            return json.loads(response.text)
        except json.JSONDecodeError as exc:
            if looks_like_login_response(response.url, response.text):
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

    def _get_text(self, url: str, allow_reauth: bool) -> ResponseText:
        generation = self._ensure_authenticated()

        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            request = urllib.request.Request(url, headers=self.headers, method="GET")
            try:
                response = self._open(request)
            except urllib.error.HTTPError as exc:
                exc.read()
                if exc.code in AUTH_STATUSES:
                    retried = self._retry_after_reauth(url, allow_reauth, generation)
                    if retried is not None:
                        return retried
                    raise self._auth_error(url, status=exc.code) from exc
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

            if looks_like_login_response(response.url, response.text):
                retried = self._retry_after_reauth(url, allow_reauth, generation)
                if retried is not None:
                    return retried
                raise self._auth_error(url, status=response.status)
            return response

        raise SourceRequestError(url, str(last_error or "Unknown request error"))

    def _retry_after_reauth(self, url: str, allow_reauth: bool, generation: int) -> ResponseText | None:
        """Log in again once and replay the request; None when that is not allowed."""
        if not allow_reauth or self.credentials is None:
            return None
        self._ensure_authenticated(after=generation)
        return self._get_text(url, allow_reauth=False)

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
        raise SourceAuthError(AUTH_LOGIN_URL, message, status=status)

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
        raise SourceAuthError(AUTH_LOGIN_URL, f"PRG login failed after {self.retries} attempts.")

    def _perform_login_once(self, credentials: Credentials) -> None:
        query = urllib.parse.urlencode({"returnUrl": AUTH_RETURN_URL, "returnApp": AUTH_RETURN_APP})
        page = self._login_request(
            urllib.request.Request(f"{AUTH_LOGIN_URL}?{query}", headers=self.headers, method="GET"),
            stage="login page request",
        )
        if not same_origin(page.url, AUTH_LOGIN_URL):
            raise SourceAuthError(AUTH_LOGIN_URL, "PRG login page redirected to an unexpected origin.")
        action_url, token = parse_login_form(page.text, page.url)
        page_url = urllib.parse.urlparse(page.url)

        payload = urllib.parse.urlencode(
            {
                "__RequestVerificationToken": token,
                "Login": credentials.username,
                "Password": credentials.password,
                "ReturnApp": AUTH_RETURN_APP,
                "ReturnUrl": AUTH_RETURN_URL,
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
        if looks_like_login_response(response.url, response.text):
            raise SourceAuthError(
                AUTH_LOGIN_URL,
                "PRG login was rejected: the login form was returned instead of a redirect to prg.kz. "
                f"Check {AUTH_USERNAME_ENV}/{AUTH_PASSWORD_ENV}.",
                status=response.status,
            )
        if not same_origin(response.url, AUTH_RETURN_URL) or not self._has_cookie_for(AUTH_RETURN_URL):
            raise SourceAuthError(
                AUTH_LOGIN_URL,
                "PRG login did not establish a session cookie for prg.kz.",
                status=response.status,
            )

    def _login_request(self, request: urllib.request.Request, stage: str) -> ResponseText:
        """Run a login step, never surfacing the request body or the response body."""
        try:
            return self._open(request)
        except urllib.error.HTTPError as exc:
            exc.read()
            raise SourceAuthError(
                AUTH_LOGIN_URL,
                f"PRG {stage} failed with HTTP {exc.code}.",
                status=exc.code,
            ) from exc
        except (urllib.error.URLError, socket.timeout, TimeoutError) as exc:
            raise SourceAuthError(AUTH_LOGIN_URL, f"PRG {stage} failed with a network error.") from exc

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
            return ResponseText(
                url=response.geturl(),
                status=int(response.status),
                text=raw.decode(charset, errors="replace"),
            )

    def _auth_error(self, url: str, status: int | None = None) -> SourceAuthError:
        if self.credentials is None:
            message = (
                "The source returned a login page instead of content. "
                f"Set {AUTH_USERNAME_ENV} and {AUTH_PASSWORD_ENV} to read protected PRG documents."
            )
        else:
            message = (
                "The source returned a login page even after re-authentication; "
                "the PRG session could not be established."
            )
        return SourceAuthError(url, message, status=status)
