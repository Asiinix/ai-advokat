"""Extra hardening coverage for the PRG.SOT egress partitions.

These tests pin the defence-in-depth guarantees around the pool: a direct
partition never inherits ambient proxy variables, an invalid proxy URL is
rejected without echoing it (a proxy URL can embed credentials), object reprs
never leak proxy URLs or credentials, and the durable-detail sanitiser strips
proxy variable values.
"""

from __future__ import annotations

import os
import unittest
import urllib.request
from unittest import mock

from ai_advokat_parser.catalog import sanitize_detail
from ai_advokat_parser.config import SOT_PASSWORD_ENV, SOT_USERNAME_ENV
from ai_advokat_parser.http_client import Credentials, SourceAuthError, SourceClient
from ai_advokat_parser.sot.adapter import build_sot_client
from ai_advokat_parser.sot.egress import build_sot_egress_client
from ai_advokat_parser.sot.source_config import SotSourceConfig

from .support_sot import FakeSotServer

PROXY_ENV = "TEST_SOT_PROXY_URL"
SECRET_PROXY_URL = "http://proxyuser:proxy-s3cret@127.0.0.1:9"


def proxy_handlers(client: SourceClient) -> list[urllib.request.ProxyHandler]:
    return [handler for handler in client._opener.handlers if isinstance(handler, urllib.request.ProxyHandler)]


class SourceClientProxyHardeningTest(unittest.TestCase):
    def test_direct_partition_ignores_ambient_proxy_variables(self) -> None:
        ambient = {
            "http_proxy": SECRET_PROXY_URL,
            "https_proxy": SECRET_PROXY_URL,
            "HTTP_PROXY": SECRET_PROXY_URL,
        }
        with mock.patch.dict(os.environ, ambient, clear=True):
            client = SourceClient(proxy_url="", credentials=Credentials("u", "p"))
        # The empty explicit mapping suppresses the default ambient handler and
        # registers no proxy routing at all: the opener is provably direct.
        self.assertEqual(proxy_handlers(client), [])
        self.assertFalse(client.uses_proxy)

    def test_proxied_partition_maps_http_and_https_targets(self) -> None:
        client = SourceClient(proxy_url=SECRET_PROXY_URL, credentials=Credentials("u", "p"))
        handlers = proxy_handlers(client)
        self.assertEqual(len(handlers), 1)
        self.assertEqual(handlers[0].proxies, {"http": SECRET_PROXY_URL, "https": SECRET_PROXY_URL})
        self.assertTrue(client.uses_proxy)

    def test_explicit_proxy_ignores_ambient_no_proxy_wildcard(self) -> None:
        client = SourceClient(proxy_url=SECRET_PROXY_URL, credentials=Credentials("u", "p"))
        handler = proxy_handlers(client)[0]
        routed = urllib.request.Request("http://sb.prg.kz/api")
        stock = urllib.request.Request("http://sb.prg.kz/api")
        with mock.patch.dict(os.environ, {"no_proxy": "*"}, clear=False):
            handler.proxy_open(routed, SECRET_PROXY_URL, "http")
            urllib.request.ProxyHandler({"http": SECRET_PROXY_URL}).proxy_open(
                stock, SECRET_PROXY_URL, "http"
            )
        # The stock handler honours no_proxy='*' and leaves the request direct;
        # the explicit handler still routes it (and its credentials) to the proxy.
        self.assertEqual(stock.host, "sb.prg.kz")
        self.assertEqual(routed.host, "127.0.0.1:9")
        self.assertTrue(routed.has_header("Proxy-authorization"))

    def test_invalid_proxy_url_is_rejected_without_echoing_it(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SourceClient(proxy_url="socks5://user:sup3r-secret@host:1080", credentials=Credentials("u", "p"))
        message = str(ctx.exception)
        self.assertNotIn("sup3r-secret", message)
        self.assertNotIn("socks5", message)
        self.assertNotIn("host:1080", message)

    def test_repr_hides_proxy_url_credentials_and_cookies(self) -> None:
        client = SourceClient(
            proxy_url=SECRET_PROXY_URL,
            credentials=Credentials("login@example.kz", "pw-s3cret"),
        )
        rendered = f"{client!r}"
        self.assertNotIn("proxy-s3cret", rendered)
        self.assertNotIn("pw-s3cret", rendered)
        self.assertNotIn("login@example.kz", rendered)
        self.assertNotIn("proxyuser", rendered)
        self.assertNotIn("127.0.0.1", rendered)
        self.assertIn("proxy='proxy'", rendered)


class EgressDirectPartitionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = FakeSotServer().start()
        self.addCleanup(self.server.stop)
        self.config = SotSourceConfig.from_env(env={}, overrides=self.server.config_overrides())

    def test_default_direct_client_does_not_inherit_ambient_proxies(self) -> None:
        ambient = {"http_proxy": SECRET_PROXY_URL, "https_proxy": SECRET_PROXY_URL}
        with mock.patch.dict(os.environ, ambient, clear=True):
            client = build_sot_egress_client(
                self.config,
                timeout=5,
                retries=1,
                retry_delay=0,
                login_url=self.server.login_url,
                env={},
            )
        self.assertEqual(proxy_handlers(client), [])
        self.assertFalse(client.uses_proxy)

    def test_no_proxy_wildcard_never_bypasses_the_partition_proxy_end_to_end(self) -> None:
        # The fake source is reachable directly, the proxy is a dead port. If
        # no_proxy='*' could bypass the explicit proxy, the login would succeed
        # by connecting straight to the source - which must never happen.
        ambient = {
            "no_proxy": "*",
            "NO_PROXY": "*",
            SOT_USERNAME_ENV: self.server.state.username,
            SOT_PASSWORD_ENV: self.server.state.password,
        }
        with mock.patch.dict(os.environ, ambient, clear=False):
            client = build_sot_client(
                self.config,
                timeout=5,
                retries=1,
                retry_delay=0,
                login_url=self.server.login_url,
                proxy_url=SECRET_PROXY_URL,
            )
            with self.assertRaises(SourceAuthError):
                client.authenticate()
        self.assertEqual(self.server.state.login_gets, 0)


class SanitizeProxyValuesTest(unittest.TestCase):
    def test_proxy_env_values_are_stripped_from_durable_details(self) -> None:
        env = {
            PROXY_ENV: SECRET_PROXY_URL,
            SOT_USERNAME_ENV: "user@example.kz",
        }
        message = f"connect failed via {env[PROXY_ENV]} for {env[SOT_USERNAME_ENV]}"
        sanitized = sanitize_detail(message, env)
        self.assertNotIn("proxy-s3cret", sanitized)
        self.assertNotIn("127.0.0.1", sanitized)
        self.assertNotIn("user@example.kz", sanitized)
        self.assertIn("***", sanitized)

    def test_proxy_authorization_marker_is_treated_as_sensitive(self) -> None:
        sanitized = sanitize_detail("server asked for Proxy-Authorization", {})
        self.assertEqual(sanitized, "detail omitted: message referenced sensitive material")

    def test_documented_partition_variable_name_is_redacted(self) -> None:
        # The README documents AI_ADVOCAT_SOT_PROXY_PX1-style names, where
        # PROXY is a token in the middle, not a suffix.
        env = {"AI_ADVOCAT_SOT_PROXY_PX1": SECRET_PROXY_URL}
        sanitized = sanitize_detail(f"connect failed via {SECRET_PROXY_URL}", env)
        self.assertNotIn("proxy-s3cret", sanitized)
        self.assertNotIn("127.0.0.1", sanitized)
        self.assertIn("***", sanitized)

    def test_trivial_no_proxy_values_do_not_mangle_messages(self) -> None:
        sanitized = sanitize_detail("glob pattern * did not match", {"no_proxy": "*"})
        self.assertEqual(sanitized, "glob pattern * did not match")


if __name__ == "__main__":
    unittest.main()
