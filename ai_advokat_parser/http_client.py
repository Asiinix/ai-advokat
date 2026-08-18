from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from .config import DEFAULT_HEADERS


class SourceRequestError(RuntimeError):
    def __init__(self, url: str, message: str, status: int | None = None):
        super().__init__(message)
        self.url = url
        self.status = status


@dataclass(frozen=True)
class ResponseText:
    url: str
    status: int
    text: str


class SourceClient:
    def __init__(
        self,
        timeout: float = 30.0,
        retries: int = 3,
        retry_delay: float = 1.5,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self.headers = {**DEFAULT_HEADERS, **(headers or {})}

    def get_text(self, url: str, query: dict[str, Any] | None = None) -> ResponseText:
        if query:
            separator = "&" if urllib.parse.urlparse(url).query else "?"
            url = f"{url}{separator}{urllib.parse.urlencode(query, doseq=True)}"

        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            request = urllib.request.Request(url, headers=self.headers, method="GET")
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    raw = response.read()
                    charset = response.headers.get_content_charset() or "utf-8"
                    text = raw.decode(charset, errors="replace")
                    return ResponseText(
                        url=response.geturl(),
                        status=int(response.status),
                        text=text,
                    )
            except urllib.error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                if exc.code in {429, 500, 502, 503, 504} and attempt < self.retries:
                    time.sleep(self.retry_delay * attempt)
                    continue
                raise SourceRequestError(url, body or str(exc), status=exc.code) from exc
            except (urllib.error.URLError, TimeoutError) as exc:
                last_error = exc
                if attempt < self.retries:
                    time.sleep(self.retry_delay * attempt)
                    continue
                raise SourceRequestError(url, str(exc)) from exc

        raise SourceRequestError(url, str(last_error or "Unknown request error"))

    def get_json(self, url: str, query: dict[str, Any] | None = None) -> Any:
        response = self.get_text(url, query=query)
        try:
            return json.loads(response.text)
        except json.JSONDecodeError as exc:
            preview = response.text[:500].replace("\n", " ")
            raise SourceRequestError(response.url, f"Invalid JSON response: {preview}") from exc
