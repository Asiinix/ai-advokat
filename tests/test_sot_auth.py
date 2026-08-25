"""Two PRG applications, one HTTP client, no shared session state.

PRG.ZANGER must keep behaving exactly as before (returnApp=prgWeb, prg.kz), and
PRG.SOT must log into the same zakon.kz SSO with returnApp=SUDBASEV2 and the
sb.prg.kz origin, using its own credentials and its own cookie jar.
"""

from __future__ import annotations

import os
import unittest
from unittest import mock

from ai_advokat_parser import http_client
from ai_advokat_parser.config import (
    AUTH_PASSWORD_ENV,
    AUTH_RETURN_APP,
    AUTH_USERNAME_ENV,
    SOT_AUTH_RETURN_APP,
    SOT_AUTH_RETURN_URL,
    SOT_PASSWORD_ENV,
    SOT_USERNAME_ENV,
    sot_auth_profile,
)
from ai_advokat_parser.http_client import (
    Credentials,
    SourceAuthError,
    SourceClient,
    SourceRateLimitError,
    credentials_from_env,
    default_auth_profile,
    parse_rate_limit,
)
from ai_advokat_parser.sot.adapter import build_sot_client
from ai_advokat_parser.sot.source_config import SotSourceConfig

from .support_http import FakeSourceServer
from .support_sot import FakeSotServer

CREDENTIAL_ENV = (AUTH_USERNAME_ENV, AUTH_PASSWORD_ENV, SOT_USERNAME_ENV, SOT_PASSWORD_ENV)


def clean_env(**overrides: str) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if key not in CREDENTIAL_ENV}
    env.update(overrides)
    return env


class AuthProfileShapeTest(unittest.TestCase):
    def test_default_profile_is_unchanged_zanger(self) -> None:
        profile = default_auth_profile()
        self.assertEqual(profile.name, "prg_zanger")
        self.assertEqual(profile.return_app, AUTH_RETURN_APP)
        self.assertEqual(profile.return_app, "prgWeb")
        self.assertEqual(profile.username_env, AUTH_USERNAME_ENV)
        self.assertTrue(profile.return_url.startswith("https://prg.kz"))

    def test_sot_profile_uses_sudbasev2_and_sb_origin(self) -> None:
        profile = sot_auth_profile()
        self.assertEqual(profile.name, "prg_sot")
        self.assertEqual(profile.return_app, "SUDBASEV2")
        self.assertEqual(profile.return_app, SOT_AUTH_RETURN_APP)
        self.assertEqual(profile.return_url, "https://sb.prg.kz/")
        self.assertEqual(profile.return_url, SOT_AUTH_RETURN_URL)
        self.assertEqual(profile.username_env, SOT_USERNAME_ENV)
        # Both applications sit behind the same central login.
        self.assertEqual(profile.auth_host, default_auth_profile().auth_host)

    def test_credentials_are_read_per_profile(self) -> None:
        env = clean_env(
            **{
                AUTH_USERNAME_ENV: "zanger@example.kz",
                AUTH_PASSWORD_ENV: "zanger-pass",
                SOT_USERNAME_ENV: "sot@example.kz",
                SOT_PASSWORD_ENV: "sot-pass",
            }
        )
        self.assertEqual(credentials_from_env(env), Credentials("zanger@example.kz", "zanger-pass"))
        self.assertEqual(
            credentials_from_env(env, sot_auth_profile()),
            Credentials("sot@example.kz", "sot-pass"),
        )

    def test_zanger_credentials_do_not_leak_into_sot(self) -> None:
        env = clean_env(**{AUTH_USERNAME_ENV: "zanger@example.kz", AUTH_PASSWORD_ENV: "zanger-pass"})
        self.assertIsNone(credentials_from_env(env, sot_auth_profile()))

    def test_incomplete_sot_credentials_name_the_sot_variables(self) -> None:
        env = clean_env(**{SOT_USERNAME_ENV: "sot@example.kz"})
        with self.assertRaises(SourceAuthError) as ctx:
            credentials_from_env(env, sot_auth_profile())
        message = str(ctx.exception)
        self.assertIn(SOT_PASSWORD_ENV, message)
        self.assertNotIn(AUTH_PASSWORD_ENV, message)
        self.assertNotIn("sot@example.kz", message)


class ZangerProfileLoginTest(unittest.TestCase):
    """The legacy profile still speaks prgWeb against the legacy fake server."""

    def setUp(self) -> None:
        self.server = FakeSourceServer().start()
        self.addCleanup(self.server.stop)
        for name, value in (
            ("AUTH_LOGIN_URL", self.server.login_url),
            ("AUTH_RETURN_URL", f"{self.server.base_url}/prg/"),
        ):
            patcher = mock.patch.object(http_client, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        env_patcher = mock.patch.dict(os.environ, clean_env(), clear=True)
        env_patcher.start()
        self.addCleanup(env_patcher.stop)

    def test_login_posts_prgweb_return_app(self) -> None:
        client = SourceClient(
            timeout=5,
            retries=2,
            retry_delay=0,
            credentials=Credentials(self.server.state.username, self.server.state.password),
        )
        client.authenticate()
        payload = {key: values[0] for key, values in self.server.state.login_payloads[0].items()}
        self.assertEqual(payload["ReturnApp"], "prgWeb")
        self.assertEqual(payload["ReturnUrl"], f"{self.server.base_url}/prg/")

    def test_rate_limit_stays_a_plain_request_error_by_default(self) -> None:
        client = SourceClient(timeout=5, retries=1, retry_delay=0)
        self.assertFalse(client.raise_on_rate_limit)


class SotProfileLoginTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = FakeSotServer().start()
        self.addCleanup(self.server.stop)
        self.env = clean_env(
            **{
                SOT_USERNAME_ENV: self.server.state.username,
                SOT_PASSWORD_ENV: self.server.state.password,
            }
        )
        env_patcher = mock.patch.dict(os.environ, self.env, clear=True)
        env_patcher.start()
        self.addCleanup(env_patcher.stop)
        self.config = SotSourceConfig.from_env(overrides=self.server.config_overrides())

    def make_client(self) -> SourceClient:
        return build_sot_client(self.config, timeout=5, retries=2, retry_delay=0, login_url=self.server.login_url)

    def test_login_posts_sudbasev2_return_app_and_sb_origin(self) -> None:
        client = self.make_client()

        self.assertTrue(client.authenticate())

        self.assertEqual(self.server.state.return_app_seen, ["SUDBASEV2"])
        self.assertEqual(client.auth.return_url, f"{self.server.base_url}/")
        self.assertEqual([cookie.name for cookie in client.cookie_jar], ["SOTSESSION"])

    def test_sot_client_opts_into_rate_limit_errors(self) -> None:
        self.assertTrue(self.make_client().raise_on_rate_limit)

    def test_rejected_sot_login_hides_secrets_and_names_sot_variables(self) -> None:
        self.server.state.reject_login = True
        client = self.make_client()

        with self.assertRaises(SourceAuthError) as ctx:
            client.authenticate()

        rendered = f"{ctx.exception} {ctx.exception.args!r}"
        self.assertNotIn(self.server.state.password, rendered)
        self.assertNotIn(self.server.state.username, rendered)
        self.assertNotIn("SOTSESSION", rendered)
        self.assertNotIn("<form", rendered)
        self.assertIn(SOT_USERNAME_ENV, rendered)
        self.assertNotIn(AUTH_USERNAME_ENV, rendered)

    def test_login_form_on_another_origin_is_rejected(self) -> None:
        client = self.make_client()
        hostile = http_client.ResponseText(
            url="https://attacker.invalid/login",
            status=200,
            text="<form action='/collect'><input name='__RequestVerificationToken' value='tok'></form>",
        )
        with mock.patch.object(client, "_login_request", return_value=hostile):
            with self.assertRaises(SourceAuthError) as ctx:
                client.authenticate()
        self.assertIn("unexpected origin", str(ctx.exception))

    def test_two_profiles_in_one_process_keep_separate_sessions(self) -> None:
        zanger_server = FakeSourceServer().start()
        self.addCleanup(zanger_server.stop)
        env = dict(self.env)
        env[AUTH_USERNAME_ENV] = zanger_server.state.username
        env[AUTH_PASSWORD_ENV] = zanger_server.state.password
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch.object(http_client, "AUTH_LOGIN_URL", zanger_server.login_url):
                with mock.patch.object(http_client, "AUTH_RETURN_URL", f"{zanger_server.base_url}/prg/"):
                    zanger = SourceClient(timeout=5, retries=2, retry_delay=0)
                    sot = self.make_client()
                    zanger.authenticate()
                    sot.authenticate()

        zanger_payload = {key: values[0] for key, values in zanger_server.state.login_payloads[0].items()}
        self.assertEqual(zanger_payload["ReturnApp"], "prgWeb")
        self.assertEqual(self.server.state.return_app_seen, ["SUDBASEV2"])
        self.assertEqual([cookie.name for cookie in zanger.cookie_jar], ["PRGSESSION"])
        self.assertEqual([cookie.name for cookie in sot.cookie_jar], ["SOTSESSION"])
        self.assertNotEqual(zanger.credentials, sot.credentials)


class RateLimitParsingTest(unittest.TestCase):
    def test_retry_after_seconds_and_remaining(self) -> None:
        info = parse_rate_limit({"Retry-After": "30", "X-RateLimit-Remaining": "24999"})
        self.assertEqual(info.retry_after, 30.0)
        self.assertEqual(info.remaining, 24999)
        self.assertEqual(info.delay(now=1000.0), 30.0)

    def test_reset_epoch_header_becomes_a_wait(self) -> None:
        info = parse_rate_limit({"X-RateLimit-Reset": "2000000000"})
        self.assertEqual(info.reset_at, 2000000000.0)
        self.assertEqual(info.delay(now=1999999940.0), 60.0)
        self.assertEqual(info.delay(now=2000000100.0), 0.0)

    def test_http_date_retry_after(self) -> None:
        info = parse_rate_limit({"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})
        self.assertIsNotNone(info.reset_at)

    def test_prg_iso_reset_header_becomes_a_wait(self) -> None:
        info = parse_rate_limit({"X-Rate-Limit-Reset": "2026-09-01T18:03:02.3333513Z"})
        self.assertIsNotNone(info.reset_at)
        self.assertGreater(info.delay(now=info.reset_at - 60.0), 59.0)
        self.assertEqual(info.delay(now=info.reset_at + 1.0), 0.0)

    def test_missing_headers_are_not_invented(self) -> None:
        info = parse_rate_limit({})
        self.assertIsNone(info.retry_after)
        self.assertIsNone(info.remaining)
        self.assertEqual(info.delay(), 0.0)
        self.assertIn("no quota headers", info.describe())


class SotRateLimitResponseTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = FakeSotServer().start()
        self.addCleanup(self.server.stop)
        env_patcher = mock.patch.dict(
            os.environ,
            clean_env(
                **{
                    SOT_USERNAME_ENV: self.server.state.username,
                    SOT_PASSWORD_ENV: self.server.state.password,
                }
            ),
            clear=True,
        )
        env_patcher.start()
        self.addCleanup(env_patcher.stop)
        self.config = SotSourceConfig.from_env(overrides=self.server.config_overrides())

    def test_429_raises_with_the_sources_own_wait(self) -> None:
        self.server.state.load(count=2, page_size=2)
        self.server.state.search_rate_limit_after = 0
        client = build_sot_client(self.config, timeout=5, retries=2, retry_delay=0, login_url=self.server.login_url)

        with self.assertRaises(SourceRateLimitError) as ctx:
            client.request_json(f"{self.server.base_url}/api/search?page=1&size=2")

        self.assertEqual(ctx.exception.rate_limit.retry_after, 2.0)
        self.assertEqual(ctx.exception.rate_limit.remaining, 0)
        # Only one attempt: the client must not push through the limit.
        self.assertEqual(len(self.server.state.search_hits), 0)

    def test_successful_response_exposes_the_quota_headers(self) -> None:
        self.server.state.load(count=2, page_size=2)
        client = build_sot_client(self.config, timeout=5, retries=2, retry_delay=0, login_url=self.server.login_url)

        client.request_json(f"{self.server.base_url}/api/search?page=1&size=2")

        self.assertEqual(client.last_rate_limit.remaining, 24999)
        self.assertEqual(client.last_rate_limit.limit, 25000)


if __name__ == "__main__":
    unittest.main()
