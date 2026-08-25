"""A local stand-in for the PRG.SOT application used by the judicial corpus tests.

It answers the shared zakon.kz login (with a SUDBASEV2 returnApp), a paginated
search endpoint and a per-decision endpoint. Nothing here mirrors a real PRG.SOT
route: the tests configure the adapter with these fake templates exactly the way
an operator will configure it with the real, captured ones.
"""

from __future__ import annotations

import http.server
import json
import threading
import urllib.parse

from .support_http import LOGIN_PAGE

SOT_WELCOME_PAGE = "<!DOCTYPE html><html><body><p>sb.prg.kz</p></body></html>"


def make_decision_payload(decision_id: str, text: str | None = None) -> dict:
    """A decision document shaped the way the fake source returns it."""
    return {
        "decision": {
            "id": decision_id,
            "caseNumber": f"2-{decision_id}/2026",
            "court": "Специализированный межрайонный экономический суд",
            "judgeName": "Судья Иванова",
            "region": "Алматы",
            "instanceName": "первая инстанция",
            "proceedingType": "гражданское",
            "decisionDate": "2026-03-14",
            "heading": f"Решение по делу 2-{decision_id}/2026",
            "sides": [{"role": "истец", "name": "ТОО Альфа"}, {"role": "ответчик", "name": "ТОО Бета"}],
            "body": text if text is not None else f"Текст судебного акта {decision_id}. " * 20,
        }
    }


class FakeSotState:
    """Recorded traffic and behaviour switches for :class:`FakeSotServer`."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.username = "sot-user@example.kz"
        self.password = "sot-s3cret"
        self.form_action = "/account/login"
        self.reject_login = False
        self.return_app_seen: list[str] = []

        self.decision_ids: list[str] = []
        self.page_size = 2
        self.report_total = True
        self.use_cursor = False

        self.search_hits: list[int] = []
        self.decision_hits: list[str] = []
        self.login_gets = 0
        self.login_posts = 0
        self.sessions: dict[str, int] = {}
        self._session_counter = 0

        # decision_id -> HTTP status the endpoint should answer with instead.
        self.decision_status: dict[str, int] = {}
        # decision_id -> replacement payload (e.g. one with an empty body).
        self.decisions: dict[str, dict] = {}
        # Number of remaining search requests before the source answers 429.
        self.search_rate_limit_after: int | None = None
        self.rate_limit_headers = {"Retry-After": "2", "X-RateLimit-Remaining": "0"}
        self.quota_headers = {"X-RateLimit-Remaining": "24999", "X-RateLimit-Limit": "25000"}

    def load(self, count: int = 5, page_size: int = 2, prefix: str = "9") -> list[str]:
        self.decision_ids = [f"{prefix}{index:04d}" for index in range(count)]
        self.page_size = page_size
        for decision_id in self.decision_ids:
            self.decisions.setdefault(decision_id, make_decision_payload(decision_id))
        return list(self.decision_ids)

    def page_items(self, page: int) -> list[str]:
        with self.lock:
            self.search_hits.append(page)
        start = (page - 1) * self.page_size
        return self.decision_ids[start : start + self.page_size]

    def register_login(self, payload: dict[str, list[str]]) -> str | None:
        with self.lock:
            self.login_posts += 1
            self.return_app_seen.append((payload.get("ReturnApp") or [""])[0])
            login = (payload.get("Login") or [""])[0]
            password = (payload.get("Password") or [""])[0]
            if self.reject_login or login != self.username or password != self.password:
                return None
            self._session_counter += 1
            session = f"sot-session-{self._session_counter}"
            self.sessions[session] = 0
            return session

    def use_session(self, session: str | None) -> bool:
        with self.lock:
            if session is None or session not in self.sessions:
                return False
            self.sessions[session] += 1
            return True


class FakeSotServer:
    def __init__(self, state: FakeSotState | None = None) -> None:
        self.state = state or FakeSotState()
        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), self._build_handler())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    @property
    def login_url(self) -> str:
        return f"{self.base_url}/account/login"

    def config_overrides(self, **extra) -> dict:
        """The contract an operator would capture from this fake source."""
        overrides = {
            "base_url": self.base_url,
            "search_url_template": f"{self.base_url}/api/search?page={{page}}&size={{page_size}}",
            "search_method": "GET",
            "decision_url_template": f"{self.base_url}/api/decision/{{decision_id}}",
            "decision_method": "GET",
            "results_path": "data.items",
            "total_path": "data.total",
            "id_path": "id",
            "text_path": "decision.body",
            "page_size": self.state.page_size,
            "decision_page_url_template": f"{self.base_url}/decision/{{decision_id}}",
            "field_map": json.dumps(
                {
                    "case_number": "caseNumber",
                    "court": "court",
                    "judge": "judgeName",
                    "region": "region",
                    "instance": "instanceName",
                    "proceeding_type": "proceedingType",
                    "decision_date": "decisionDate",
                    "title": "heading",
                    "parties": "sides",
                }
            ),
        }
        overrides.update(extra)
        return overrides

    def start(self) -> "FakeSotServer":
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
                with state.lock:
                    state.login_gets += 1
                self._send(200, LOGIN_PAGE.format(action=state.form_action, token="sot-token"), "text/html")

            def _session(self) -> str | None:
                cookie = self.headers.get("Cookie") or ""
                for chunk in cookie.split(";"):
                    name, _, value = chunk.strip().partition("=")
                    if name == "SOTSESSION":
                        return value
                return None

            def _require_session(self) -> bool:
                if state.use_session(self._session()):
                    return True
                self._send(302, "", "text/html", [("Location", "/account/login")])
                return False

            def do_GET(self) -> None:
                parsed = urllib.parse.urlparse(self.path)
                path = parsed.path
                if path == "/account/login":
                    self._send_login_page()
                elif path == "/sot/" or path == "/":
                    self._send(200, SOT_WELCOME_PAGE, "text/html")
                elif path == "/api/search":
                    self._search(parsed)
                elif path.startswith("/api/decision/"):
                    self._decision(path.rsplit("/", 1)[-1])
                else:
                    self._send(404, "not found", "text/plain")

            def _search(self, parsed) -> None:
                if not self._require_session():
                    return
                with state.lock:
                    if state.search_rate_limit_after is not None:
                        if state.search_rate_limit_after <= 0:
                            headers = list(state.rate_limit_headers.items())
                            self._send(429, json.dumps({"error": "rate limit"}), "application/json", headers)
                            return
                        state.search_rate_limit_after -= 1
                query = urllib.parse.parse_qs(parsed.query)
                page = int((query.get("page") or ["1"])[0])
                items = [
                    {
                        "id": decision_id,
                        **{
                            key: value
                            for key, value in make_decision_payload(decision_id)["decision"].items()
                            if key != "body"
                        },
                    }
                    for decision_id in state.page_items(page)
                ]
                body: dict = {"data": {"items": items}}
                if state.report_total:
                    body["data"]["total"] = len(state.decision_ids)
                if state.use_cursor:
                    has_more = page * state.page_size < len(state.decision_ids)
                    body["data"]["nextCursor"] = f"cursor-{page + 1}" if has_more else None
                self._send(200, json.dumps(body), "application/json", list(state.quota_headers.items()))

            def _decision(self, decision_id: str) -> None:
                if not self._require_session():
                    return
                with state.lock:
                    state.decision_hits.append(decision_id)
                forced = state.decision_status.get(decision_id)
                if forced:
                    self._send(
                        forced,
                        json.dumps({"error": "denied", "secret": "must-not-leak"}),
                        "application/json",
                    )
                    return
                payload = state.decisions.get(decision_id)
                if payload is None:
                    self._send(404, json.dumps({"error": "not found"}), "application/json")
                    return
                self._send(200, json.dumps(payload), "application/json", list(state.quota_headers.items()))

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
                self._send(
                    302,
                    "",
                    "text/html",
                    [("Location", "/"), ("Set-Cookie", f"SOTSESSION={session}; Path=/")],
                )

        return Handler
