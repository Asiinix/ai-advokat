"""Coverage for the PRG.SOT egress-partition pool.

PRG explicitly authorized IP rotation and parallel sessions after a spent
quota, so these tests prove the pool rotates exactly on the two authorized
signals (HTTP 429 and a successful remaining=0), honours every per-partition
reset, keeps one independent session per partition, and never lets a proxy URL
or credential into an error message or a diagnostic line.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
import urllib.request
from unittest import mock

from ai_advokat_parser.config import (
    AUTH_PASSWORD_ENV,
    AUTH_USERNAME_ENV,
    SOT_PASSWORD_ENV,
    SOT_USERNAME_ENV,
)
from ai_advokat_parser.http_client import (
    RateLimitInfo,
    ResponseText,
    SourceAuthError,
    SourceClient,
    SourceRateLimitError,
    SourceRequestError,
)
from ai_advokat_parser.sot import runtime as sot_runtime
from ai_advokat_parser.sot.adapter import SotSource
from ai_advokat_parser.sot.egress import (
    DEFAULT_COOLDOWN_SECONDS,
    PARTITIONS_ENV,
    QUARANTINE_SECONDS,
    SotEgressPartition,
    SotEgressPool,
    build_sot_egress_client,
    partitions_from_env,
    resolve_proxy_url,
)
from ai_advokat_parser.sot.model import PHASE_COMPLETED
from ai_advokat_parser.sot.scan import SotScanner
from ai_advokat_parser.sot.source_config import SotConfigError, SotSourceConfig
from ai_advokat_parser.sot.store import SotStore

from .support_sot import FakeSotServer

PROXY_ENV = "TEST_SOT_PROXY_URL"
# 127.0.0.1:9 (discard) refuses connections immediately, so proxy failures are fast.
SECRET_PROXY_URL = "http://proxyuser:proxy-s3cret@127.0.0.1:9"


def clean_env(**overrides: str) -> dict[str, str]:
    dropped = {AUTH_USERNAME_ENV, AUTH_PASSWORD_ENV, SOT_USERNAME_ENV, SOT_PASSWORD_ENV, PROXY_ENV}
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in dropped and not key.startswith("AI_ADVOCAT_SOT_")
    }
    env.update(overrides)
    return env


def partitions_json(*descriptors: dict) -> str:
    return json.dumps(list(descriptors))


class PartitionDescriptorTest(unittest.TestCase):
    def test_default_is_one_direct_partition(self) -> None:
        partitions = partitions_from_env(clean_env())
        self.assertEqual(partitions, (SotEgressPartition("direct"),))
        self.assertIsNone(resolve_proxy_url(partitions[0], clean_env()))

    def test_invalid_json_is_rejected(self) -> None:
        env = clean_env(**{PARTITIONS_ENV: "not json"})
        with self.assertRaises(SotConfigError) as ctx:
            partitions_from_env(env)
        self.assertIn(PARTITIONS_ENV, str(ctx.exception))

    def test_non_list_and_empty_list_are_rejected(self) -> None:
        for raw in ('{"id": "a"}', "[]"):
            with self.assertRaises(SotConfigError):
                partitions_from_env(clean_env(**{PARTITIONS_ENV: raw}))

    def test_bad_ids_and_duplicates_are_rejected(self) -> None:
        for descriptors in (
            [{"proxy_env": PROXY_ENV}],
            [{"id": "плохой id"}],
            [{"id": "a"}, {"id": "a"}],
        ):
            with self.assertRaises(SotConfigError):
                partitions_from_env(clean_env(**{PARTITIONS_ENV: json.dumps(descriptors)}))

    def test_proxy_url_in_descriptor_is_rejected_and_not_echoed(self) -> None:
        env = clean_env(
            **{PARTITIONS_ENV: partitions_json({"id": "a", "proxy_url": SECRET_PROXY_URL})}
        )
        with self.assertRaises(SotConfigError) as ctx:
            partitions_from_env(env)
        message = str(ctx.exception)
        self.assertIn("proxy_env", message)
        self.assertNotIn("proxy-s3cret", message)
        self.assertNotIn(SECRET_PROXY_URL, message)

    def test_proxy_env_must_be_an_env_var_name(self) -> None:
        env = clean_env(
            **{PARTITIONS_ENV: partitions_json({"id": "a", "proxy_env": "http://not-a-name"})}
        )
        with self.assertRaises(SotConfigError):
            partitions_from_env(env)

    def test_all_partitions_disabled_is_rejected(self) -> None:
        env = clean_env(**{PARTITIONS_ENV: partitions_json({"id": "a", "enabled": False})})
        with self.assertRaises(SotConfigError):
            partitions_from_env(env)

    def test_missing_proxy_env_variable_names_partition_and_variable(self) -> None:
        partition = SotEgressPartition("a", proxy_env=PROXY_ENV)
        with self.assertRaises(SotConfigError) as ctx:
            resolve_proxy_url(partition, clean_env())
        message = str(ctx.exception)
        self.assertIn("a", message)
        self.assertIn(PROXY_ENV, message)

    def test_non_http_proxy_url_is_rejected_without_echoing_it(self) -> None:
        env = clean_env(**{PROXY_ENV: "socks5://user:secret-value@host:1080"})
        with self.assertRaises(SotConfigError) as ctx:
            resolve_proxy_url(SotEgressPartition("a", proxy_env=PROXY_ENV), env)
        message = str(ctx.exception)
        self.assertIn(PROXY_ENV, message)
        self.assertNotIn("secret-value", message)


class FakeClock:
    def __init__(self, now: float = 1_000_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class ScriptedClient:
    """A stand-in SourceClient: each call pops the next scripted answer."""

    def __init__(self, partition_id: str) -> None:
        self.partition_id = partition_id
        self.script: list = []
        self.default: Exception | dict | None = None
        self.calls = 0
        self.lock = threading.Lock()
        self.last_rate_limit = RateLimitInfo()
        self.auth_step: bool | Exception = True

    def answer(self, url: str):
        with self.lock:
            self.calls += 1
            if self.script:
                step = self.script.pop(0)
            else:
                step = self.default if self.default is not None else {}
        if isinstance(step, Exception):
            raise step
        headers = dict(step.get("headers", {}))
        response = ResponseText(url=url, status=200, text="{}", headers=headers)
        return {"served_by": self.partition_id}, response

    def request_json(self, url, method="GET", json_body=None, headers=None):
        return self.answer(url)

    def authenticate(self) -> bool:
        if isinstance(self.auth_step, Exception):
            raise self.auth_step
        return self.auth_step


def make_pool(partition_ids: tuple[str, ...], clock: FakeClock) -> tuple[SotEgressPool, dict[str, ScriptedClient]]:
    clients = {partition_id: ScriptedClient(partition_id) for partition_id in partition_ids}
    partitions = tuple(SotEgressPartition(partition_id) for partition_id in partition_ids)
    pool = SotEgressPool(partitions, lambda p: clients[p.partition_id], time_source=clock)
    return pool, clients


def limited(seconds: float) -> SourceRateLimitError:
    info = RateLimitInfo(retry_after=seconds, remaining=0)
    return SourceRateLimitError("https://sot.invalid/api", "limited", info)


class PoolSelectionTest(unittest.TestCase):
    def test_429_fails_over_to_the_next_partition(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a", "b"), clock)
        clients["a"].script = [limited(120.0)]

        payload, _response = pool.request_json("https://sot.invalid/api")

        self.assertEqual(payload["served_by"], "b")
        self.assertEqual(clients["a"].calls, 1)
        self.assertEqual(clients["b"].calls, 1)

    def test_successful_remaining_zero_rotates_before_the_next_request(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a", "b"), clock)
        clients["a"].script = [{"headers": {"X-RateLimit-Remaining": "0", "Retry-After": "300"}}]

        first, _ = pool.request_json("https://sot.invalid/api")
        second, _ = pool.request_json("https://sot.invalid/api")

        # The remaining=0 answer itself still counts; only the next one moves.
        self.assertEqual(first["served_by"], "a")
        self.assertEqual(second["served_by"], "b")

    def test_remaining_zero_without_reset_uses_the_default_cooldown(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a", "b"), clock)
        clients["a"].script = [{"headers": {"X-RateLimit-Remaining": "0"}}]

        pool.request_json("https://sot.invalid/api")
        self.assertEqual(pool.request_json("https://sot.invalid/api")[0]["served_by"], "b")
        clock.now += DEFAULT_COOLDOWN_SECONDS + 1
        # Selection is sticky, so 'b' keeps serving; rest 'b' to prove 'a' healed.
        clients["b"].script = [limited(30.0)]
        self.assertEqual(pool.request_json("https://sot.invalid/api")[0]["served_by"], "a")

    def test_all_partitions_exhausted_raises_aggregate_with_earliest_reset(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a", "b"), clock)
        clients["a"].script = [limited(120.0)]
        clients["b"].script = [limited(30.0)]

        with self.assertRaises(SourceRateLimitError) as ctx:
            pool.request_json("https://sot.invalid/api")

        exc = ctx.exception
        self.assertIn("a", str(exc))
        self.assertIn("b", str(exc))
        # The earliest reset wins: partition b frees up after 30s.
        self.assertEqual(exc.rate_limit.reset_at, clock.now + 30.0)
        self.assertEqual(exc.rate_limit.remaining, 0)
        self.assertAlmostEqual(exc.rate_limit.delay(now=clock.now), 30.0)

    def test_cooldown_expiry_makes_a_partition_selectable_again(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a",), clock)
        clients["a"].script = [limited(60.0)]
        with self.assertRaises(SourceRateLimitError):
            pool.request_json("https://sot.invalid/api")

        clock.now += 61.0
        self.assertEqual(pool.request_json("https://sot.invalid/api")[0]["served_by"], "a")

    def test_concurrent_requests_are_thread_safe(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a", "b"), clock)
        # Partition a is permanently limited: whichever thread reaches it first
        # rotates the pool, and any thread racing it just marks it again.
        clients["a"].default = limited(600.0)
        errors: list[BaseException] = []

        def hammer() -> None:
            try:
                for _ in range(25):
                    payload, _ = pool.request_json("https://sot.invalid/api")
                    assert payload["served_by"] == "b"
            except BaseException as exc:  # noqa: BLE001 - collected for the assertion
                errors.append(exc)

        threads = [threading.Thread(target=hammer) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        self.assertEqual(errors, [])
        self.assertEqual(clients["b"].calls, 8 * 25)
        self.assertGreaterEqual(clients["a"].calls, 1)

    def test_diagnostics_show_partition_state_without_secrets(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a", "b"), clock)
        clients["a"].script = [limited(90.0)]
        pool.request_json("https://sot.invalid/api")

        lines = pool.diagnostics()

        self.assertEqual(len(lines), 2)
        self.assertTrue(lines[0].startswith("a:"))
        self.assertIn("cooling", lines[0])
        self.assertIn("available", lines[1])


class PoolQuarantineTest(unittest.TestCase):
    """Partition-local failures rotate; shared content errors surface as-is."""

    def test_network_error_quarantines_and_fails_over(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a", "b"), clock)
        clients["a"].script = [SourceRequestError("https://sot.invalid/api", "connection refused")]

        payload, _ = pool.request_json("https://sot.invalid/api")

        self.assertEqual(payload["served_by"], "b")
        state_a = pool._states[0]
        self.assertEqual(state_a.cooldown_until, clock.now + QUARANTINE_SECONDS)

    def test_proxy_407_quarantines_and_fails_over(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a", "b"), clock)
        clients["a"].script = [
            SourceRequestError("https://sot.invalid/api", "Source request failed with HTTP 407.", status=407)
        ]

        payload, _ = pool.request_json("https://sot.invalid/api")

        self.assertEqual(payload["served_by"], "b")
        self.assertEqual(pool._states[0].cooldown_until, clock.now + QUARANTINE_SECONDS)

    def test_ordinary_status_errors_do_not_rotate(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a", "b"), clock)
        clients["a"].script = [
            SourceRequestError("https://sot.invalid/api", "Source request failed with HTTP 500.", status=500)
        ]

        with self.assertRaises(SourceRequestError):
            pool.request_json("https://sot.invalid/api")

        # The verdict is about the content, not the egress path: partition a is
        # neither quarantined nor abandoned, and b was never bothered.
        self.assertIsNone(pool._states[0].cooldown_until)
        self.assertEqual(clients["b"].calls, 0)
        self.assertEqual(pool.request_json("https://sot.invalid/api")[0]["served_by"], "a")

    def test_auth_errors_do_not_rotate(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a", "b"), clock)
        clients["a"].script = [
            SourceAuthError("https://sot.invalid/api", "login rejected", status=200)
        ]

        with self.assertRaises(SourceAuthError):
            pool.request_json("https://sot.invalid/api")

        self.assertIsNone(pool._states[0].cooldown_until)
        self.assertEqual(clients["b"].calls, 0)

    def test_network_auth_error_quarantines_and_fails_over(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a", "b"), clock)
        clients["a"].script = [SourceAuthError("https://sot.invalid/login", "network error")]

        payload, _ = pool.request_json("https://sot.invalid/api")

        self.assertEqual(payload["served_by"], "b")
        self.assertEqual(pool._states[0].cooldown_until, clock.now + QUARANTINE_SECONDS)

    def test_explicit_authentication_skips_a_dead_egress_partition(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a", "b"), clock)
        clients["a"].auth_step = SourceAuthError("https://sot.invalid/login", "network error")

        self.assertTrue(pool.authenticate())
        self.assertEqual(pool._states[0].cooldown_until, clock.now + QUARANTINE_SECONDS)

    def test_explicit_authentication_does_not_hide_rejected_credentials(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a", "b"), clock)
        clients["a"].auth_step = SourceAuthError(
            "https://sot.invalid/login", "login rejected", status=200
        )

        with self.assertRaises(SourceAuthError):
            pool.authenticate()
        self.assertIsNone(pool._states[0].cooldown_until)

    def test_network_error_with_no_partition_left_surfaces_the_real_error(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a", "b"), clock)
        clients["a"].default = SourceRequestError("https://sot.invalid/api", "connection refused")
        clients["b"].default = SourceRequestError("https://sot.invalid/api", "connection refused")

        with self.assertRaises(SourceRequestError) as ctx:
            pool.request_json("https://sot.invalid/api")

        # Every path is down: that is a network problem, not a rate limit.
        self.assertNotIsInstance(ctx.exception, SourceRateLimitError)


class PoolCooldownRaceTest(unittest.TestCase):
    def test_a_shorter_mark_never_cuts_an_existing_rest_short(self) -> None:
        clock = FakeClock()
        pool, clients = make_pool(("a", "b"), clock)
        state_a = pool._states[0]

        # The deterministic replay of the race: a 429 with a 300s reset lands
        # first, then a success with a 60s hint that was already in flight.
        pool._mark_limited(state_a, RateLimitInfo(retry_after=300.0, remaining=0))
        pool._mark_limited(state_a, RateLimitInfo(retry_after=60.0, remaining=0))

        self.assertEqual(state_a.cooldown_until, clock.now + 300.0)
        clock.now += 61.0
        self.assertEqual(pool.request_json("https://sot.invalid/api")[0]["served_by"], "b")
        clock.now += 240.0
        clients["b"].script = [limited(30.0)]
        self.assertEqual(pool.request_json("https://sot.invalid/api")[0]["served_by"], "a")

    def test_a_longer_mark_still_extends_the_rest(self) -> None:
        clock = FakeClock()
        pool, _clients = make_pool(("a", "b"), clock)
        state_a = pool._states[0]

        pool._mark_limited(state_a, RateLimitInfo(retry_after=60.0, remaining=0))
        pool._mark_limited(state_a, RateLimitInfo(retry_after=300.0, remaining=0))

        self.assertEqual(state_a.cooldown_until, clock.now + 300.0)


class LoginRateLimitTest(unittest.TestCase):
    """A throttled login is a quota verdict, never a credential failure."""

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
        self.server.state.login_rate_limited = True
        self.server.state.rate_limit_headers = {"Retry-After": "300", "X-RateLimit-Remaining": "0"}

    def test_sot_login_429_raises_rate_limit_with_parsed_headers(self) -> None:
        client = build_sot_egress_client(
            self.config, timeout=5, retries=2, retry_delay=0, login_url=self.server.login_url
        )
        with self.assertRaises(SourceRateLimitError) as ctx:
            client.authenticate()
        exc = ctx.exception
        self.assertEqual(exc.status, 429)
        self.assertEqual(exc.rate_limit.retry_after, 300.0)
        rendered = f"{exc} {exc.args!r}"
        self.assertNotIn(self.server.state.username, rendered)
        self.assertNotIn(self.server.state.password, rendered)

    def test_legacy_client_without_opt_in_keeps_the_auth_error(self) -> None:
        from ai_advokat_parser.config import sot_auth_profile

        client = SourceClient(
            timeout=5,
            retries=2,
            retry_delay=0,
            auth=sot_auth_profile(login_url=self.server.login_url, return_url=f"{self.server.base_url}/"),
            raise_on_rate_limit=False,
        )
        with self.assertRaises(SourceAuthError) as ctx:
            client.authenticate()
        self.assertEqual(ctx.exception.status, 429)

    def test_pool_marks_throttled_logins_and_raises_the_quota_verdict(self) -> None:
        with mock.patch.dict(
            os.environ, {PARTITIONS_ENV: partitions_json({"id": "a"}, {"id": "b"})}
        ):
            pool = build_sot_egress_client(
                self.config, timeout=5, retries=2, retry_delay=0, login_url=self.server.login_url
            )
        self.assertIsInstance(pool, SotEgressPool)

        with self.assertRaises(SourceRateLimitError):
            pool.authenticate()

        # Both partitions are resting until the login throttle resets, so a
        # request now reports the aggregate quota verdict instead of crashing.
        with self.assertRaises(SourceRateLimitError) as ctx:
            pool.request_json(f"{self.server.base_url}/api/search?page=1&size=2")
        self.assertIn("egress partitions are rate limited", str(ctx.exception))

    def test_probe_auth_reports_a_throttled_login_as_a_rate_limit(self) -> None:
        code = sot_runtime.probe_auth(timeout=5, retries=2, login_url=self.server.login_url)
        self.assertEqual(code, 3)


class EgressBuilderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = FakeSotServer().start()
        self.addCleanup(self.server.stop)
        self.config = SotSourceConfig.from_env(
            env=clean_env(), overrides=self.server.config_overrides()
        )

    def build(self, env: dict[str, str]):
        return build_sot_egress_client(
            self.config, timeout=5, retries=1, retry_delay=0, login_url=self.server.login_url, env=env
        )

    def test_default_env_builds_the_plain_direct_client(self) -> None:
        client = self.build(clean_env())
        self.assertIsInstance(client, SourceClient)
        self.assertFalse(client.uses_proxy)

    def test_single_proxy_partition_builds_one_proxied_client(self) -> None:
        env = clean_env(
            **{
                PARTITIONS_ENV: partitions_json({"id": "px", "proxy_env": PROXY_ENV}),
                PROXY_ENV: SECRET_PROXY_URL,
            }
        )
        client = self.build(env)
        self.assertIsInstance(client, SourceClient)
        self.assertTrue(client.uses_proxy)

    def test_pool_partitions_get_independent_clients_sessions_and_proxies(self) -> None:
        env = clean_env(
            **{
                PARTITIONS_ENV: partitions_json(
                    {"id": "direct"}, {"id": "px", "proxy_env": PROXY_ENV}
                ),
                PROXY_ENV: SECRET_PROXY_URL,
            }
        )
        pool = self.build(env)
        self.assertIsInstance(pool, SotEgressPool)
        self.assertEqual(pool.partition_ids, ("direct", "px"))

        direct, proxied = (state.client for state in pool._states)
        self.assertIsNot(direct, proxied)
        self.assertIsNot(direct.cookie_jar, proxied.cookie_jar)
        self.assertFalse(direct.uses_proxy)
        self.assertTrue(proxied.uses_proxy)
        proxy_handlers = [
            handler
            for handler in proxied._opener.handlers
            if isinstance(handler, urllib.request.ProxyHandler)
        ]
        self.assertEqual(len(proxy_handlers), 1)
        self.assertEqual(proxy_handlers[0].proxies, {"http": SECRET_PROXY_URL, "https": SECRET_PROXY_URL})

    def test_missing_proxy_variable_fails_the_build_loudly(self) -> None:
        env = clean_env(
            **{PARTITIONS_ENV: partitions_json({"id": "direct"}, {"id": "px", "proxy_env": PROXY_ENV})}
        )
        with self.assertRaises(SotConfigError) as ctx:
            self.build(env)
        self.assertIn(PROXY_ENV, str(ctx.exception))

    def test_disabled_partitions_are_left_out_of_the_pool(self) -> None:
        env = clean_env(
            **{
                PARTITIONS_ENV: partitions_json(
                    {"id": "a"}, {"id": "px", "proxy_env": PROXY_ENV, "enabled": False}
                )
            }
        )
        client = self.build(env)
        # The disabled proxy partition is skipped entirely, so its missing
        # variable does not matter and the single survivor is a plain client.
        self.assertIsInstance(client, SourceClient)

    def test_unreachable_proxy_errors_never_leak_the_proxy_url(self) -> None:
        env = clean_env(
            **{
                PARTITIONS_ENV: partitions_json({"id": "px", "proxy_env": PROXY_ENV}),
                PROXY_ENV: SECRET_PROXY_URL,
                SOT_USERNAME_ENV: self.server.state.username,
                SOT_PASSWORD_ENV: self.server.state.password,
            }
        )
        with mock.patch.dict(os.environ, env, clear=True):
            client = self.build(env)
            with self.assertRaises(SourceAuthError) as ctx:
                client.authenticate()
        rendered = f"{ctx.exception} {ctx.exception.args!r}"
        self.assertNotIn("proxy-s3cret", rendered)
        self.assertNotIn("proxyuser", rendered)
        self.assertNotIn(SECRET_PROXY_URL, rendered)


class LivePoolTest(unittest.TestCase):
    """Two direct partitions against the fake source: real logins, real 429s."""

    def setUp(self) -> None:
        self.server = FakeSotServer().start()
        self.addCleanup(self.server.stop)
        env_patcher = mock.patch.dict(
            os.environ,
            clean_env(
                **{
                    SOT_USERNAME_ENV: self.server.state.username,
                    SOT_PASSWORD_ENV: self.server.state.password,
                    PARTITIONS_ENV: partitions_json({"id": "a"}, {"id": "b"}),
                }
            ),
            clear=True,
        )
        env_patcher.start()
        self.addCleanup(env_patcher.stop)
        self.config = SotSourceConfig.from_env(overrides=self.server.config_overrides())
        # A long reset keeps the limited partition resting for the whole test.
        self.server.state.rate_limit_headers = {"Retry-After": "300", "X-RateLimit-Remaining": "0"}

    def make_pool(self) -> SotEgressPool:
        pool = build_sot_egress_client(
            self.config, timeout=5, retries=2, retry_delay=0, login_url=self.server.login_url
        )
        self.assertIsInstance(pool, SotEgressPool)
        return pool

    def test_each_partition_logs_in_with_its_own_session(self) -> None:
        pool = self.make_pool()
        self.assertTrue(pool.authenticate())
        self.assertEqual(self.server.state.login_posts, 2)
        self.assertEqual(sorted(self.server.state.sessions), ["sot-session-1", "sot-session-2"])
        jars = [state.client.cookie_jar for state in pool._states]
        self.assertIsNot(jars[0], jars[1])

    def test_429_on_one_session_fails_over_transparently(self) -> None:
        self.server.state.load(count=2, page_size=2)
        # The first partition to log in becomes sot-session-1; spend its quota.
        self.server.state.rate_limited_sessions.add("sot-session-1")
        pool = self.make_pool()

        payload, _response = pool.request_json(f"{self.server.base_url}/api/search?page=1&size=2")

        self.assertEqual(len(payload["data"]["items"]), 2)
        self.assertEqual(self.server.state.search_sessions, ["sot-session-1", "sot-session-2"])

    def test_scan_finishes_on_the_healthy_partition_without_duplicates(self) -> None:
        ids = self.server.state.load(count=5, page_size=2)
        self.server.state.rate_limited_sessions.add("sot-session-1")
        source = SotSource(self.make_pool(), self.config)

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = SotStore(tmp.name)
        self.addCleanup(store.close)

        scanner = SotScanner(store, source, delay=0, sleep=lambda seconds: None)
        state = scanner.run(scan_id="egress-scan")

        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertEqual(state.decisions_seen, len(ids))
        self.assertEqual(store.pending_decision_count("egress-scan"), 0)
        # Every decision was fetched exactly once, and only by the healthy session.
        self.assertEqual(sorted(self.server.state.decision_hits), sorted(ids))
        self.assertEqual(set(self.server.state.decision_sessions), {"sot-session-2"})


if __name__ == "__main__":
    unittest.main()
