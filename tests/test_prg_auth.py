from __future__ import annotations

import concurrent.futures
import contextlib
import io
import os
import tempfile
import unittest
from unittest import mock

from ai_advokat_parser import cli, crawler as crawler_module, http_client, railway_worker
from ai_advokat_parser.crawler import Crawler
from ai_advokat_parser.config import AUTH_PASSWORD_ENV, AUTH_USERNAME_ENV
from ai_advokat_parser.http_client import (
    Credentials,
    ResponseText,
    SourceAuthError,
    SourceClient,
    credentials_from_env,
    parse_login_form,
)
from ai_advokat_parser.listing import fetch_listing_page

from .support_http import FakeSourceServer


def clean_env(**overrides: str) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if key not in {AUTH_USERNAME_ENV, AUTH_PASSWORD_ENV}}
    env.update(overrides)
    return env


class LoginFormParsingTest(unittest.TestCase):
    def test_reads_token_and_resolves_relative_action(self) -> None:
        page = (
            "<html><body><form action='/prefix/account/login' method='post'>"
            "<input type='hidden' name='__RequestVerificationToken' value='abc&amp;123' />"
            "</form></body></html>"
        )
        action, token = parse_login_form(page, "https://auth.zakon.kz/account/login?returnApp=prgWeb")
        self.assertEqual(action, "https://auth.zakon.kz/prefix/account/login")
        self.assertEqual(token, "abc&123")

    def test_falls_back_to_page_url_without_action(self) -> None:
        page = "<form><input name=\"__RequestVerificationToken\" value=\"tok\"></form>"
        action, token = parse_login_form(page, "https://auth.zakon.kz/account/login")
        self.assertEqual(action, "https://auth.zakon.kz/account/login")
        self.assertEqual(token, "tok")

    def test_missing_token_raises_auth_error(self) -> None:
        with self.assertRaises(SourceAuthError):
            parse_login_form("<form></form>", "https://auth.zakon.kz/account/login")

    def test_rejects_form_action_on_another_origin(self) -> None:
        page = (
            "<form action='https://attacker.invalid/collect'>"
            "<input name='__RequestVerificationToken' value='tok'>"
            "</form>"
        )
        with self.assertRaises(SourceAuthError) as ctx:
            parse_login_form(page, "https://auth.zakon.kz/account/login")
        self.assertIn("unexpected action origin", str(ctx.exception))


class CredentialsFromEnvTest(unittest.TestCase):
    def test_returns_none_when_nothing_configured(self) -> None:
        self.assertIsNone(credentials_from_env(clean_env()))

    def test_returns_credentials_when_both_set(self) -> None:
        credentials = credentials_from_env(
            clean_env(**{AUTH_USERNAME_ENV: "user@example.kz", AUTH_PASSWORD_ENV: "pass"})
        )
        self.assertEqual(credentials, Credentials("user@example.kz", "pass"))

    def test_username_without_password_fails_clearly(self) -> None:
        env = clean_env(**{AUTH_USERNAME_ENV: "user@example.kz"})
        with self.assertRaises(SourceAuthError) as ctx:
            credentials_from_env(env)
        message = str(ctx.exception)
        self.assertIn(AUTH_PASSWORD_ENV, message)
        self.assertNotIn("user@example.kz", message)

    def test_password_without_username_fails_clearly(self) -> None:
        env = clean_env(**{AUTH_PASSWORD_ENV: "s3cret-pass"})
        with self.assertRaises(SourceAuthError) as ctx:
            credentials_from_env(env)
        message = str(ctx.exception)
        self.assertIn(AUTH_USERNAME_ENV, message)
        self.assertNotIn("s3cret-pass", message)

    def test_client_construction_uses_env(self) -> None:
        with mock.patch.dict(os.environ, clean_env(), clear=True):
            self.assertFalse(SourceClient().uses_authentication)
        env = clean_env(**{AUTH_USERNAME_ENV: "user@example.kz", AUTH_PASSWORD_ENV: "s3cret-pass"})
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertTrue(SourceClient().uses_authentication)
        with mock.patch.dict(os.environ, clean_env(**{AUTH_USERNAME_ENV: "user"}), clear=True):
            with self.assertRaises(SourceAuthError):
                SourceClient()

    def test_credentials_repr_is_redacted(self) -> None:
        credentials = Credentials("user@example.kz", "s3cret-pass")
        for rendered in (repr(credentials), str(credentials), f"{credentials}"):
            self.assertNotIn("user@example.kz", rendered)
            self.assertNotIn("s3cret-pass", rendered)


class AuthenticatedClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = FakeSourceServer().start()
        self.addCleanup(self.server.stop)
        patcher = mock.patch.object(http_client, "AUTH_LOGIN_URL", self.server.login_url)
        patcher.start()
        self.addCleanup(patcher.stop)
        return_patcher = mock.patch.object(http_client, "AUTH_RETURN_URL", f"{self.server.base_url}/prg/")
        return_patcher.start()
        self.addCleanup(return_patcher.stop)
        env_patcher = mock.patch.dict(os.environ, clean_env(), clear=True)
        env_patcher.start()
        self.addCleanup(env_patcher.stop)

    def make_client(self, authenticated: bool = True, **kwargs) -> SourceClient:
        credentials = (
            Credentials(self.server.state.username, self.server.state.password) if authenticated else None
        )
        return SourceClient(timeout=5, retries=2, retry_delay=0, credentials=credentials, **kwargs)

    def test_login_posts_form_token_and_credentials(self) -> None:
        client = self.make_client()
        data = client.get_json(f"{self.server.base_url}/mapi/api/Document/GetDocument/1/0")

        self.assertEqual(data["ok"], True)
        state = self.server.state
        self.assertEqual(state.login_gets, 1)
        self.assertEqual(state.login_posts, 1)
        payload = {key: values[0] for key, values in state.login_payloads[0].items()}
        self.assertEqual(payload["__RequestVerificationToken"], state.issued_tokens[0])
        self.assertEqual(payload["Login"], state.username)
        self.assertEqual(payload["Password"], state.password)
        self.assertEqual(payload["ReturnApp"], "prgWeb")
        self.assertEqual(payload["ReturnUrl"], http_client.AUTH_RETURN_URL)
        self.assertEqual(payload["PersonalDataAgreement"], "true")
        self.assertEqual(payload["Remember"], "false")

    def test_login_page_redirect_to_another_origin_is_rejected(self) -> None:
        client = self.make_client()
        hostile_page = ResponseText(
            url="https://attacker.invalid/login",
            status=200,
            text=(
                "<form action='/collect'>"
                "<input name='__RequestVerificationToken' value='tok'>"
                "</form>"
            ),
        )

        with mock.patch.object(client, "_login_request", return_value=hostile_page):
            with self.assertRaises(SourceAuthError) as ctx:
                client.authenticate()

        rendered = str(ctx.exception)
        self.assertIn("unexpected origin", rendered)
        self.assertNotIn(self.server.state.username, rendered)
        self.assertNotIn(self.server.state.password, rendered)

    def test_login_without_application_cookie_is_rejected(self) -> None:
        self.server.state.omit_session_cookie = True
        client = self.make_client()

        with self.assertRaises(SourceAuthError) as ctx:
            client.authenticate()

        self.assertIn("session cookie", str(ctx.exception))
        self.assertFalse(client.authenticated)

    def test_session_cookie_is_reused_for_follow_up_requests(self) -> None:
        client = self.make_client()
        for index in range(3):
            client.get_json(f"{self.server.base_url}/mapi/api/Document/GetDocument/{index}/0")

        self.assertEqual(self.server.state.login_posts, 1)
        self.assertEqual([cookie.name for cookie in client.cookie_jar], ["PRGSESSION"])
        self.assertEqual(self.server.state.protected_hits, 3)

    def test_explicit_auth_preflight_is_reused_by_follow_up(self) -> None:
        client = self.make_client()

        self.assertTrue(client.authenticate())
        self.assertTrue(client.get_json(f"{self.server.base_url}/mapi/api/Document/GetDocument/1/0")["ok"])
        self.assertEqual(self.server.state.login_posts, 1)

    def test_concurrent_requests_authenticate_once(self) -> None:
        client = self.make_client()
        urls = [f"{self.server.base_url}/mapi/api/Document/GetDocument/{index}/0" for index in range(12)]

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            results = list(executor.map(client.get_json, urls))

        self.assertTrue(all(item["ok"] for item in results))
        self.assertEqual(self.server.state.login_posts, 1)
        self.assertEqual(self.server.state.login_gets, 1)

    def test_expired_session_triggers_one_reauth_and_one_retry(self) -> None:
        self.server.state.session_max_uses = 1
        client = self.make_client()

        first = client.get_json(f"{self.server.base_url}/mapi/api/Document/GetDocument/1/0")
        second = client.get_json(f"{self.server.base_url}/mapi/api/Document/GetDocument/2/0")

        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertEqual(self.server.state.login_posts, 2)

    def test_expired_session_with_401_triggers_reauth(self) -> None:
        self.server.state.session_max_uses = 1
        self.server.state.protected_status = 401
        client = self.make_client()

        client.get_json(f"{self.server.base_url}/mapi/api/Document/GetDocument/1/0")
        second = client.get_json(f"{self.server.base_url}/mapi/api/Document/GetDocument/2/0")

        self.assertTrue(second["ok"])
        self.assertEqual(self.server.state.login_posts, 2)

    def test_403_does_not_relogin_or_leak_response_body(self) -> None:
        self.server.state.session_max_uses = 1
        self.server.state.protected_status = 403
        client = self.make_client()

        client.get_json(f"{self.server.base_url}/mapi/api/Document/GetDocument/1/0")
        with self.assertRaises(http_client.SourceRequestError) as ctx:
            client.get_json(f"{self.server.base_url}/mapi/api/Document/GetDocument/2/0")

        self.assertNotIsInstance(ctx.exception, SourceAuthError)
        self.assertNotIn("must-not-leak", str(ctx.exception))
        self.assertEqual(self.server.state.login_posts, 1)

    def test_permanent_login_wall_does_not_loop(self) -> None:
        self.server.state.session_max_uses = 0
        client = self.make_client()

        # The login itself keeps working, so this is a verdict about the URL and
        # not about the session: it must not be a fatal SourceAuthError.
        with self.assertRaises(http_client.SourceAccessDeniedError) as ctx:
            client.get_json(f"{self.server.base_url}/mapi/api/Document/GetDocument/1/0")

        self.assertNotIsInstance(ctx.exception, SourceAuthError)
        self.assertEqual(self.server.state.login_posts, 2)
        self.assertIn("re-authentication", str(ctx.exception))

    def test_concurrent_expiry_reauthenticates_once(self) -> None:
        client = self.make_client()
        client.get_json(f"{self.server.base_url}/mapi/api/Document/GetDocument/0/0")
        # Every session issued so far is now dead, so all workers hit the login wall at once.
        with self.server.state.lock:
            self.server.state.sessions.clear()
        urls = [f"{self.server.base_url}/mapi/api/Document/GetDocument/{index}/0" for index in range(1, 9)]

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(client.get_json, urls))

        self.assertTrue(all(item["ok"] for item in results))
        self.assertEqual(self.server.state.login_posts, 2)

    def test_rejected_credentials_error_hides_secrets(self) -> None:
        self.server.state.reject_login = True
        client = self.make_client()

        with self.assertRaises(SourceAuthError) as ctx:
            client.get_json(f"{self.server.base_url}/mapi/api/Document/GetDocument/1/0")

        rendered = f"{ctx.exception} {ctx.exception.args!r}"
        self.assertNotIn(self.server.state.password, rendered)
        self.assertNotIn(self.server.state.username, rendered)
        self.assertNotIn("PRGSESSION", rendered)
        for token in self.server.state.issued_tokens:
            self.assertNotIn(token, rendered)
        self.assertNotIn("<form", rendered)
        self.assertIn(AUTH_USERNAME_ENV, rendered)
        self.assertEqual(self.server.state.login_posts, 1)

    def test_rejected_credentials_are_not_retried_on_every_request(self) -> None:
        self.server.state.reject_login = True
        client = self.make_client()
        url = f"{self.server.base_url}/mapi/api/Document/GetDocument/1/0"

        for _ in range(3):
            with self.assertRaises(SourceAuthError):
                client.get_json(url)
        self.assertEqual(self.server.state.login_posts, 1)

        # Once the cooldown lapses the client is free to try again.
        self.server.state.reject_login = False
        client._login_failed_at -= http_client.LOGIN_FAILURE_COOLDOWN
        self.assertTrue(client.get_json(url)["ok"])
        self.assertEqual(self.server.state.login_posts, 2)

    def test_login_page_instead_of_json_is_reported_as_auth_error(self) -> None:
        client = self.make_client(authenticated=False)

        with self.assertRaises(SourceAuthError) as ctx:
            client.get_json(f"{self.server.base_url}/mapi/api/Document/GetDocument/1/0")

        message = str(ctx.exception)
        self.assertIn(AUTH_USERNAME_ENV, message)
        self.assertNotIn("__RequestVerificationToken", message)
        self.assertLess(len(message), 400)
        self.assertEqual(self.server.state.login_posts, 0)

    def test_listing_login_page_is_not_reported_as_empty_page(self) -> None:
        client = self.make_client(authenticated=False)

        with self.assertRaises(SourceAuthError):
            fetch_listing_page(client, page=1, list_url=f"{self.server.base_url}/listing")

    def test_authenticated_listing_returns_documents(self) -> None:
        client = self.make_client()
        listing = fetch_listing_page(client, page=1, list_url=f"{self.server.base_url}/listing")

        self.assertEqual([ref.doc_id for ref in listing.documents], ["42"])

    def test_anonymous_client_never_logs_in(self) -> None:
        client = self.make_client(authenticated=False)

        data = client.get_json(f"{self.server.base_url}/public")

        self.assertEqual(data, {"public": True})
        self.assertFalse(client.uses_authentication)
        self.assertFalse(client.authenticated)
        self.assertEqual(self.server.state.login_gets, 0)
        self.assertEqual(self.server.state.login_posts, 0)

    def test_anonymous_client_from_empty_env(self) -> None:
        client = SourceClient(timeout=5, retries=2, retry_delay=0)

        self.assertFalse(client.uses_authentication)
        self.assertEqual(client.get_json(f"{self.server.base_url}/public"), {"public": True})

    def test_non_json_html_error_is_summarised(self) -> None:
        client = self.make_client(authenticated=False)

        with self.assertRaises(http_client.SourceRequestError) as ctx:
            client.get_json(f"{self.server.base_url}/prg/")

        self.assertNotIsInstance(ctx.exception, SourceAuthError)
        self.assertIn("HTML", str(ctx.exception))


class ConstructionPathsTest(unittest.TestCase):
    """Every place that builds a SourceClient must pick up the credentials env."""

    def setUp(self) -> None:
        self.server = FakeSourceServer().start()
        self.addCleanup(self.server.stop)
        patcher = mock.patch.object(http_client, "AUTH_LOGIN_URL", self.server.login_url)
        patcher.start()
        self.addCleanup(patcher.stop)
        return_patcher = mock.patch.object(http_client, "AUTH_RETURN_URL", f"{self.server.base_url}/prg/")
        return_patcher.start()
        self.addCleanup(return_patcher.stop)
        self.env = clean_env(
            **{
                AUTH_USERNAME_ENV: self.server.state.username,
                AUTH_PASSWORD_ENV: self.server.state.password,
            }
        )
        self.env.pop("AI_ADVOCAT_DATABASE_URL", None)
        self.env.pop("DATABASE_URL", None)

    def test_cli_list_command_authenticates(self) -> None:
        with mock.patch.dict(os.environ, self.env, clear=True):
            with contextlib.redirect_stdout(io.StringIO()) as out:
                cli.main(["--timeout", "5", "list", "--page", "1", "--list-url", f"{self.server.base_url}/listing"])

        self.assertEqual(self.server.state.login_posts, 1)
        self.assertIn("42", out.getvalue())

    def test_large_command_fails_before_requesting_documents_when_login_is_rejected(self) -> None:
        self.server.state.reject_login = True
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(os.environ, self.env, clear=True):
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as ctx:
                        cli.main(["--out", tmp, "--include-paid", "--force", "doc", "1"])

        self.assertEqual(ctx.exception.code, 2)
        self.assertEqual(self.server.state.login_posts, 1)
        self.assertEqual(self.server.state.protected_hits, 0)

    def test_crawler_and_menu_crawler_are_env_aware(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(os.environ, self.env, clear=True):
                with contextlib.redirect_stdout(io.StringIO()):
                    crawler = cli.make_menu_crawler(
                        tmp,
                        formats=("html",),
                        delay=0,
                        workers=1,
                        force=False,
                        follow_links_depth=0,
                        max_linked_docs=None,
                    )
                try:
                    self.assertTrue(crawler.client.uses_authentication)
                finally:
                    crawler.close()

    def test_crawler_reports_incomplete_credentials(self) -> None:
        env = dict(self.env)
        env.pop(AUTH_PASSWORD_ENV)
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(os.environ, env, clear=True):
                with contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaises(SourceAuthError):
                        Crawler(out_dir=tmp)


class FatalBatchAuthTest(unittest.TestCase):
    def test_direct_batch_stops_on_first_auth_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(os.environ, clean_env(), clear=True):
                crawler = Crawler(out_dir=tmp, delay=0, force=True)
            try:
                error = SourceAuthError("https://prg.kz/mapi", "authentication unavailable")
                with mock.patch.object(crawler_module.DocumentDownloader, "fetch_document", side_effect=error):
                    with self.assertRaises(SourceAuthError):
                        crawler.crawl_doc_ids(["1001", "1002"])

                self.assertEqual(crawler.store.get_document_status("1001"), "failed")
                self.assertIsNone(crawler.store.get_document_status("1002"))
            finally:
                crawler.close()

    def test_claimed_queue_item_is_requeued_on_auth_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(os.environ, clean_env(), clear=True):
                crawler = Crawler(out_dir=tmp, delay=0, force=True)
            try:
                ref = crawler_module.DocumentRef(doc_id="1001")
                crawler.enqueue_refs([ref])
                claimed = crawler.store.claim_queued_document("test-worker")
                self.assertIsNotNone(claimed)

                error = SourceAuthError("https://prg.kz/mapi", "authentication unavailable")
                with mock.patch.object(crawler_module.DocumentDownloader, "fetch_document", side_effect=error):
                    with self.assertRaises(SourceAuthError):
                        crawler._process_ref(ref, index=1, total=1, depth=0, claimed=True)

                self.assertEqual(crawler.store.get_document_status("1001"), "queued")
            finally:
                crawler.close()


class RailwayWorkerRedactionTest(unittest.TestCase):
    def test_command_log_masks_credential_values(self) -> None:
        env = {AUTH_USERNAME_ENV: "user@example.kz", AUTH_PASSWORD_ENV: "s3cret-pass"}
        command = "--out /tmp/data doc 1 --note user@example.kz:s3cret-pass"

        redacted = railway_worker.redact_secrets(command, env)

        self.assertNotIn("user@example.kz", redacted)
        self.assertNotIn("s3cret-pass", redacted)
        self.assertIn("--out /tmp/data doc 1", redacted)

    def test_auth_mode_labels(self) -> None:
        self.assertEqual(railway_worker.auth_mode({}), "anonymous")
        self.assertEqual(
            railway_worker.auth_mode({AUTH_USERNAME_ENV: "user", AUTH_PASSWORD_ENV: "pass"}),
            "PRG login configured",
        )
        self.assertIn("incomplete", railway_worker.auth_mode({AUTH_USERNAME_ENV: "user"}))


if __name__ == "__main__":
    unittest.main()
