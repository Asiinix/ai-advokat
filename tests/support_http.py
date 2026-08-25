"""A tiny local stand-in for the PRG/auth.zakon.kz pair used by the auth tests."""

from __future__ import annotations

import http.server
import json
import threading
import urllib.parse

LOGIN_PAGE = """<!DOCTYPE html>
<html><body>
  <form method="post" action="{action}">
    <input name="__RequestVerificationToken" type="hidden" value="{token}" />
    <input name="Login" type="text" />
    <input name="Password" type="password" />
    <input name="PersonalDataAgreement" type="checkbox" value="true" />
  </form>
</body></html>
"""

WELCOME_PAGE = "<!DOCTYPE html><html><body><p>prg.kz</p></body></html>"

CATALOG_PAGE = """<!DOCTYPE html>
<html><body>
  <div class="summary">{total_line}</div>
  {links}
</body></html>
"""


def make_document_payload(doc_id: str, title: str = "", is_free: bool = True, pages: int = 1) -> dict:
    """A minimal GetDocument response shaped like the PRG API."""
    return {
        "name": title or f"Документ {doc_id}",
        "isDocumentFree": is_free,
        "style": "",
        "pages": [
            {
                "paragpraphs": [
                    {"paragraphId": index + 1, "html": f"<p>{doc_id} часть {index + 1}</p>"}
                ]
            }
            for index in range(pages)
        ],
    }


def make_empty_document_payload(doc_id: str, title: str = "", is_free: bool = True) -> dict:
    """A response the source accepts but that carries no readable page."""
    payload = make_document_payload(doc_id, title=title, is_free=is_free)
    payload["pages"] = []
    return payload


class FakeSourceState:
    """Recorded traffic and behaviour switches for :class:`FakeSourceServer`."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.username = "user@example.kz"
        self.password = "s3cret-pass"
        self.form_action = "/account/login"
        self.reject_login = False
        self.omit_session_cookie = False
        self.session_max_uses: int | None = None
        self.protected_status = 302
        self.login_gets = 0
        self.login_posts = 0
        self.protected_hits = 0
        self.login_payloads: list[dict[str, list[str]]] = []
        self.issued_tokens: list[str] = []
        self.sessions: dict[str, int] = {}
        self._session_counter = 0
        # Catalog listing (/catalog) and document API (/mapi/...) fixtures.
        self.catalog_doc_ids: list[str] = []
        self.catalog_page_size = 2
        self.catalog_total: int | None = None
        self.catalog_page_hits: list[int] = []
        self.documents: dict[str, dict] = {}
        self.document_status: dict[str, int] = {}
        self.document_hits: list[str] = []

    def load_catalog(self, doc_ids: list[str], page_size: int = 2, total: int | None = None) -> None:
        self.catalog_doc_ids = list(doc_ids)
        self.catalog_page_size = page_size
        self.catalog_total = len(doc_ids) if total is None else total
        for doc_id in doc_ids:
            self.documents.setdefault(doc_id, make_document_payload(doc_id))

    def catalog_page(self, page: int) -> list[str]:
        with self.lock:
            self.catalog_page_hits.append(page)
        start = (page - 1) * self.catalog_page_size
        return self.catalog_doc_ids[start : start + self.catalog_page_size]

    def record_document_hit(self, doc_id: str) -> None:
        with self.lock:
            self.document_hits.append(doc_id)

    def issue_token(self) -> str:
        with self.lock:
            token = f"token-{len(self.issued_tokens) + 1}"
            self.issued_tokens.append(token)
            self.login_gets += 1
            return token

    def register_login(self, payload: dict[str, list[str]]) -> str | None:
        with self.lock:
            self.login_posts += 1
            self.login_payloads.append(payload)
            token = (payload.get("__RequestVerificationToken") or [""])[0]
            login = (payload.get("Login") or [""])[0]
            password = (payload.get("Password") or [""])[0]
            if self.reject_login or token not in self.issued_tokens:
                return None
            if login != self.username or password != self.password:
                return None
            self._session_counter += 1
            session = f"session-{self._session_counter}"
            self.sessions[session] = 0
            return session

    def use_session(self, session: str | None) -> bool:
        with self.lock:
            self.protected_hits += 1
            if session is None or session not in self.sessions:
                return False
            self.sessions[session] += 1
            if self.session_max_uses is not None and self.sessions[session] > self.session_max_uses:
                return False
            return True


class FakeSourceServer:
    def __init__(self, state: FakeSourceState | None = None) -> None:
        self.state = state or FakeSourceState()
        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), self._build_handler())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    @property
    def login_url(self) -> str:
        return f"{self.base_url}/account/login"

    def start(self) -> "FakeSourceServer":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def _build_handler(self):
        state = self.state

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def log_message(self, *args, **kwargs) -> None:  # keep the test output clean
                pass

            def _send(self, status: int, body: str, content_type: str, extra_headers=()) -> None:
                payload = body.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", f"{content_type}; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                for name, value in extra_headers:
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(payload)

            def _send_login_page(self) -> None:
                token = state.issue_token()
                self._send(200, LOGIN_PAGE.format(action=state.form_action, token=token), "text/html")

            def _session(self) -> str | None:
                cookie = self.headers.get("Cookie") or ""
                for chunk in cookie.split(";"):
                    name, _, value = chunk.strip().partition("=")
                    if name == "PRGSESSION":
                        return value
                return None

            def do_GET(self) -> None:
                path = urllib.parse.urlparse(self.path).path
                if path == "/account/login":
                    self._send_login_page()
                elif path == "/prg/":
                    self._send(200, WELCOME_PAGE, "text/html")
                elif path == "/public":
                    self._send(200, json.dumps({"public": True}), "application/json")
                elif path.startswith("/mapi/"):
                    if state.use_session(self._session()):
                        doc_id = path.rstrip("/").split("/")[-2] if path.count("/") >= 6 else ""
                        forced = state.document_status.get(doc_id)
                        if forced:
                            state.record_document_hit(doc_id)
                            self._send(
                                forced,
                                json.dumps({"error": "denied", "secret": "must-not-leak"}),
                                "application/json",
                            )
                            return
                        payload = state.documents.get(doc_id)
                        if payload is not None:
                            state.record_document_hit(doc_id)
                            self._send(200, json.dumps(payload), "application/json")
                            return
                        self._send(200, json.dumps({"ok": True, "path": path}), "application/json")
                    elif state.protected_status in {401, 403}:
                        self._send(
                            state.protected_status,
                            json.dumps({"error": "unauthorized", "secret": "must-not-leak"}),
                            "application/json",
                        )
                    else:
                        self._send(302, "", "text/html", [("Location", "/account/login")])
                elif path == "/catalog":
                    if not state.use_session(self._session()):
                        self._send(302, "", "text/html", [("Location", "/account/login")])
                        return
                    query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                    page = int((query.get("currentPage") or ["1"])[0])
                    links = "\n".join(
                        f"<a href='/lawyer/document/?doc_id={doc_id}'>Документ {doc_id}</a>"
                        for doc_id in state.catalog_page(page)
                    )
                    total_line = "" if state.catalog_total is None else f"Документов: {state.catalog_total}"
                    self._send(200, CATALOG_PAGE.format(total_line=total_line, links=links), "text/html")
                elif path == "/listing":
                    if state.use_session(self._session()):
                        self._send(200, "<a href='/lawyer/document/?doc_id=42'>Doc</a>", "text/html")
                    else:
                        self._send(302, "", "text/html", [("Location", "/account/login")])
                else:
                    self._send(404, "not found", "text/plain")

            def do_POST(self) -> None:
                path = urllib.parse.urlparse(self.path).path
                if path != "/account/login":
                    self._send(404, "not found", "text/plain")
                    return
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length).decode("utf-8")
                payload = urllib.parse.parse_qs(raw, keep_blank_values=True)
                session = state.register_login(payload)
                if session is None:
                    self._send_login_page()
                    return
                headers = [("Location", "/prg/")]
                if not state.omit_session_cookie:
                    headers.append(("Set-Cookie", f"PRGSESSION={session}; Path=/"))
                self._send(302, "", "text/html", headers)

        return Handler
