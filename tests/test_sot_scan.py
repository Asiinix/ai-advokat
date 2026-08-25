"""Coverage for the PRG.SOT judicial corpus scan.

Everything runs against a local fake source and a SQLite state file: no test
needs network access, a subscription or real credentials. The fake source stands
in for the endpoints an operator will capture from a live session, which is the
only way to exercise pagination, resume, leases and rate limits without them.
"""

from __future__ import annotations

import contextlib
import inspect
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ai_advokat_parser import cli
from ai_advokat_parser.config import (
    AUTH_PASSWORD_ENV,
    AUTH_USERNAME_ENV,
    SOT_PASSWORD_ENV,
    SOT_USERNAME_ENV,
)
from ai_advokat_parser.http_client import SourceAuthError, SourceRateLimitError, RateLimitInfo
from ai_advokat_parser.sot import CORPUS_TYPE, SOURCE_SYSTEM, decision_key
from ai_advokat_parser.sot.adapter import SotSource, build_sot_client
from ai_advokat_parser.sot.model import (
    OUTCOME_DONE,
    OUTCOME_INACCESSIBLE,
    OUTCOME_NOT_FOUND,
    OUTCOME_PENDING,
    PHASE_ABORTED,
    PHASE_COMPLETED,
    PHASE_PAUSED,
    PHASE_RATE_LIMITED,
    STATUS_EXPORTED,
    STATUS_PROCESSING,
    STATUS_QUEUED,
    SotDecisionRef,
    SotDiscoveryError,
    STUB_FIELDS,
)
from ai_advokat_parser.sot.postgres_store import SotPostgresStore
from ai_advokat_parser.sot.scan import SotScanner
from ai_advokat_parser.sot.source_config import SotConfigError, SotSourceConfig
from ai_advokat_parser.sot.store import SotStore

from .support_sot import FakeSotServer, make_decision_payload

STORE_METHODS = (
    "ensure_scan",
    "get_scan",
    "set_scan_discovery",
    "set_scan_phase",
    "advance_scan",
    "record_search_page",
    "claim_decision",
    "release_decision",
    "requeue_stale_decisions",
    "has_decision_outputs",
    "is_decision_complete",
    "save_decision",
    "mark_decision_failed",
    "decision_status",
    "get_decision",
    "decision_stats",
    "record_decision_outcome",
    "resolve_scan_outcomes",
    "retry_scan_outcomes",
    "pending_decision_count",
    "scan_stats",
    "scan_stubs",
    "close",
)


def clean_env(**overrides: str) -> dict[str, str]:
    dropped = {
        AUTH_USERNAME_ENV,
        AUTH_PASSWORD_ENV,
        SOT_USERNAME_ENV,
        SOT_PASSWORD_ENV,
        "AI_ADVOCAT_DATABASE_URL",
        "DATABASE_URL",
    }
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in dropped and not key.startswith("AI_ADVOCAT_SOT_")
    }
    env.update(overrides)
    return env


class RecordingSleep:
    """Stands in for time.sleep so a honoured pause is observable, not endured."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(float(seconds))


class SotScanBase(unittest.TestCase):
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

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.out_dir = tmp.name
        self.store = SotStore(self.out_dir)
        self.addCleanup(self.store.close)
        self.sleep = RecordingSleep()

    def load(self, count: int = 5, page_size: int = 2) -> list[str]:
        return self.server.state.load(count=count, page_size=page_size)

    def make_source(self, **overrides) -> SotSource:
        config = SotSourceConfig.from_env(overrides=self.server.config_overrides(**overrides))
        client = build_sot_client(config, timeout=5, retries=2, retry_delay=0, login_url=self.server.login_url)
        return SotSource(client, config)

    def make_scanner(self, source: SotSource | None = None, **kwargs) -> SotScanner:
        return SotScanner(
            self.store,
            source or self.make_source(),
            delay=0,
            sleep=self.sleep,
            **kwargs,
        )

    def run_scan(self, scanner: SotScanner, scan_id: str = "sot-1", **kwargs):
        with contextlib.redirect_stdout(io.StringIO()):
            return scanner.run(scan_id=scan_id, poll_interval=0, **kwargs)


class SotSourceConfigTest(unittest.TestCase):
    def base(self, **overrides) -> SotSourceConfig:
        values = {
            "base_url": "https://sb.prg.kz",
            "search_url_template": "https://sb.prg.kz/api/search?page={page}&size={page_size}",
            "decision_url_template": "https://sb.prg.kz/api/decision/{decision_id}",
            "results_path": "data.items",
            "id_path": "id",
            "text_path": "body",
        }
        values.update(overrides)
        return SotSourceConfig.from_env(env={}, overrides=values)

    def test_unconfigured_source_lists_every_missing_variable(self) -> None:
        config = SotSourceConfig.from_env(env={})
        self.assertFalse(config.is_configured)
        with self.assertRaises(SotConfigError) as ctx:
            config.validate()
        message = str(ctx.exception)
        for name in config.missing_requirements():
            self.assertIn(name, message)
        self.assertIn("live", message.lower() + " subscribed session")

    def test_valid_contract_passes(self) -> None:
        config = self.base()
        self.assertIs(config.validate(), config)
        self.assertTrue(config.is_configured)

    def test_search_template_without_pagination_is_rejected(self) -> None:
        with self.assertRaises(SotConfigError) as ctx:
            self.base(search_url_template="https://sb.prg.kz/api/search").validate()
        self.assertIn("paginate", str(ctx.exception))

    def test_decision_template_without_placeholder_is_rejected(self) -> None:
        with self.assertRaises(SotConfigError) as ctx:
            self.base(decision_url_template="https://sb.prg.kz/api/decision").validate()
        self.assertIn("decision_id", str(ctx.exception))

    def test_unsupported_method_is_rejected(self) -> None:
        with self.assertRaises(SotConfigError) as ctx:
            self.base(search_method="DELETE").validate()
        self.assertIn("GET, POST", str(ctx.exception))

    def test_template_on_a_foreign_origin_is_rejected(self) -> None:
        with self.assertRaises(SotConfigError) as ctx:
            self.base(search_url_template="https://attacker.invalid/api?page={page}").validate()
        message = str(ctx.exception)
        self.assertIn("attacker.invalid", message)
        self.assertIn("sb.prg.kz", message)

    def test_unknown_placeholder_is_rejected(self) -> None:
        with self.assertRaises(SotConfigError) as ctx:
            self.base(search_url_template="https://sb.prg.kz/api?page={page}&secret={token}").validate()
        self.assertIn("token", str(ctx.exception))

    def test_unknown_metadata_field_is_rejected(self) -> None:
        with self.assertRaises(SotConfigError) as ctx:
            self.base(field_map=json.dumps({"verdict_colour": "colour"})).validate()
        self.assertIn("verdict_colour", str(ctx.exception))

    def test_post_body_template_must_be_json(self) -> None:
        with self.assertRaises(SotConfigError) as ctx:
            self.base(search_method="POST", search_body_template="page={page}").validate()
        self.assertIn("JSON object template", str(ctx.exception))

    def test_rendered_requests_use_the_captured_templates(self) -> None:
        config = self.base(
            search_method="POST",
            search_body_template='{"page": "{page}", "size": "{page_size}", "q": "{query}"}',
            query="иск",
            page_size=25,
        ).validate()
        url, method, body = config.search_request(3, cursor=None, offset=50)
        self.assertEqual(method, "POST")
        self.assertEqual(url, "https://sb.prg.kz/api/search?page=3&size=25")
        self.assertEqual(body, {"page": "3", "size": "25", "q": "иск"})
        self.assertEqual(
            config.decision_request("A/1"),
            ("https://sb.prg.kz/api/decision/A%2F1", "GET", None),
        )

    def test_fingerprint_changes_with_the_contract(self) -> None:
        self.assertNotEqual(self.base().fingerprint(), self.base(page_size=50).fingerprint())
        self.assertEqual(self.base().fingerprint(), self.base().fingerprint())


class SotPaginationTest(SotScanBase):
    def test_pages_are_walked_in_order_and_totals_are_recorded(self) -> None:
        decision_ids = self.load(count=5, page_size=2)
        scanner = self.make_scanner()

        state = self.run_scan(scanner)

        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertEqual(state.source_system, SOURCE_SYSTEM)
        self.assertEqual(state.corpus_type, CORPUS_TYPE)
        self.assertEqual(state.total_decisions, 5)
        self.assertEqual(state.page_size, 2)
        self.assertEqual(state.total_pages, 3)
        self.assertEqual(state.decisions_seen, 5)
        self.assertEqual(self.server.state.search_hits, [1, 2, 3])
        self.assertEqual(sorted(self.server.state.decision_hits), sorted(decision_ids))
        self.assertEqual(self.store.scan_stats("sot-1"), {OUTCOME_DONE: 5})

    def test_each_page_is_exported_before_the_next_page_is_enumerated(self) -> None:
        decision_ids = self.load(count=4, page_size=2)
        source = self.make_source()
        original = source.fetch_search_page

        def observed(page, cursor=None, offset=None):
            if page == 2:
                self.assertEqual(
                    self.store.decision_status(decision_key(decision_ids[0])),
                    STATUS_EXPORTED,
                )
            return original(page, cursor=cursor, offset=offset)

        with mock.patch.object(source, "fetch_search_page", side_effect=observed):
            state = self.run_scan(self.make_scanner(source=source))

        self.assertEqual(state.phase, PHASE_COMPLETED)

    def test_cursor_pagination_stops_when_the_cursor_runs_out(self) -> None:
        self.server.state.use_cursor = True
        self.load(count=4, page_size=2)
        scanner = self.make_scanner(source=self.make_source(next_cursor_path="data.nextCursor"))

        state = self.run_scan(scanner)

        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertEqual(self.server.state.search_hits, [1, 2])
        self.assertIsNone(state.next_cursor)

    def test_missing_total_still_scans_but_never_invents_a_size(self) -> None:
        self.server.state.report_total = False
        self.load(count=3, page_size=2)
        scanner = self.make_scanner()

        state = self.run_scan(scanner)

        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertIsNone(state.total_decisions)
        self.assertIsNone(state.total_pages)
        self.assertEqual(state.decisions_seen, 3)

    def test_unparseable_search_payload_aborts_instead_of_truncating(self) -> None:
        self.load(count=4, page_size=2)
        scanner = self.make_scanner(source=self.make_source(results_path="data.rows"))

        with self.assertRaises(SotDiscoveryError):
            self.run_scan(scanner)

        state = self.store.get_scan("sot-1")
        self.assertEqual(state.phase, PHASE_ABORTED)
        self.assertEqual(self.server.state.decision_hits, [])

    def test_metadata_is_stored_for_every_decision(self) -> None:
        decision_ids = self.load(count=2, page_size=2)
        self.run_scan(self.make_scanner())

        row = self.store.get_decision(decision_key(decision_ids[0]))
        self.assertEqual(row["source_system"], SOURCE_SYSTEM)
        self.assertEqual(row["corpus_type"], CORPUS_TYPE)
        self.assertEqual(row["case_number"], f"2-{decision_ids[0]}/2026")
        self.assertEqual(row["region"], "Алматы")
        self.assertEqual(row["instance"], "первая инстанция")
        self.assertEqual(row["proceeding_type"], "гражданское")
        self.assertEqual(row["decision_date"], "2026-03-14")
        self.assertIn("Иванова", row["judge"])
        self.assertIn("ТОО Альфа", str(row["parties"]))
        self.assertTrue(row["text_sha256"])
        self.assertTrue(row["raw_sha256"])
        self.assertGreater(int(row["text_chars"]), 0)


class SotResumeTest(SotScanBase):
    def test_max_pages_pauses_and_the_next_run_finishes(self) -> None:
        decision_ids = self.load(count=6, page_size=2)
        scanner = self.make_scanner()

        first = self.run_scan(scanner, max_pages=1)
        self.assertEqual(first.phase, PHASE_PAUSED)
        self.assertEqual(first.next_page, 2)
        self.assertEqual(self.server.state.search_hits, [1])
        self.assertEqual(sorted(self.server.state.decision_hits), decision_ids[:2])

        second = self.run_scan(scanner)
        self.assertEqual(second.phase, PHASE_COMPLETED)
        self.assertEqual(self.server.state.search_hits, [1, 2, 3])
        self.assertEqual(sorted(self.server.state.decision_hits), sorted(decision_ids))

    def test_max_decisions_pauses_inside_a_page(self) -> None:
        self.load(count=6, page_size=3)
        scanner = self.make_scanner()

        state = self.run_scan(scanner, max_decisions=2)
        self.assertEqual(state.phase, PHASE_PAUSED)
        self.assertEqual(state.next_page, 1)
        self.assertEqual(state.decisions_seen, 2)

        state = self.run_scan(scanner)
        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertEqual(state.decisions_seen, 6)

    def test_completed_rerun_is_a_noop(self) -> None:
        self.load(count=2, page_size=2)
        scanner = self.make_scanner()
        self.run_scan(scanner)
        pages_before = list(self.server.state.search_hits)
        decisions_before = list(self.server.state.decision_hits)

        state = self.run_scan(scanner)

        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertEqual(self.server.state.search_hits, pages_before)
        self.assertEqual(self.server.state.decision_hits, decisions_before)

    def test_scan_id_is_pinned_to_one_source_contract(self) -> None:
        self.load(count=2, page_size=2)
        self.run_scan(self.make_scanner())

        other = self.make_scanner(source=self.make_source(page_size=50))
        with self.assertRaises(ValueError) as ctx:
            self.run_scan(other)
        self.assertIn("different source contract", str(ctx.exception))

    def test_decisions_left_processing_by_a_crash_are_reclaimed(self) -> None:
        decision_ids = self.load(count=2, page_size=2)
        scanner = self.make_scanner()
        self.store.ensure_scan("sot-1", scanner.source.config.fingerprint(), "", first_page=1)
        ref = SotDecisionRef(
            decision_id=decision_ids[0],
            decision_key=decision_key(decision_ids[0]),
            page=1,
            position=0,
        )
        self.store.record_search_page("sot-1", 1, [ref])
        self.assertIsNotNone(self.store.claim_decision("sot-1", "dead-container"))
        self.assertEqual(self.store.decision_status(ref.decision_key), STATUS_PROCESSING)
        # The container died an hour ago, so its lease has long expired.
        with self.store._conn as conn:
            conn.execute(
                "UPDATE sot_decisions SET locked_at = ? WHERE decision_key = ?",
                ("2000-01-01T00:00:00+00:00", ref.decision_key),
            )

        state = self.run_scan(scanner, lease_seconds=1800)

        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertEqual(self.store.decision_status(ref.decision_key), STATUS_EXPORTED)

    def test_a_live_lease_is_not_stolen_from_a_running_worker(self) -> None:
        decision_ids = self.load(count=2, page_size=2)
        scanner = self.make_scanner()
        self.store.ensure_scan("sot-1", scanner.source.config.fingerprint(), "", first_page=1)
        ref = SotDecisionRef(
            decision_id=decision_ids[0],
            decision_key=decision_key(decision_ids[0]),
            page=1,
            position=0,
        )
        self.store.record_search_page("sot-1", 1, [ref])
        self.store.claim_decision("sot-1", "live-worker")

        self.assertEqual(self.store.requeue_stale_decisions("sot-1", 3600), 0)
        self.assertEqual(self.store.decision_status(ref.decision_key), STATUS_PROCESSING)


class SotSkipAndRetryTest(SotScanBase):
    def test_already_exported_decision_is_not_fetched_again(self) -> None:
        decision_ids = self.load(count=4, page_size=2)
        scanner = self.make_scanner()
        self.run_scan(scanner)
        self.assertEqual(sorted(set(self.server.state.decision_hits)), sorted(decision_ids))

        # A second scan over the same corpus must reuse what is already stored.
        second = self.make_scanner()
        self.server.state.decision_hits.clear()
        state = self.run_scan(second, scan_id="sot-2")

        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertEqual(self.server.state.decision_hits, [])
        self.assertEqual(self.store.scan_stats("sot-2"), {OUTCOME_DONE: 4})

    def test_failures_are_not_retried_unless_resuming_explicitly(self) -> None:
        decision_ids = self.load(count=2, page_size=2)
        broken = decision_ids[0]
        self.server.state.decision_status[broken] = 500
        scanner = self.make_scanner()
        self.run_scan(scanner)
        self.assertEqual(self.store.scan_stats("sot-1"), {OUTCOME_DONE: 1, "failed": 1})

        del self.server.state.decision_status[broken]
        self.server.state.decision_hits.clear()
        self.run_scan(scanner)
        self.assertEqual(self.server.state.decision_hits, [])

        state = self.run_scan(scanner, retry_failed=True)
        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertIn(broken, self.server.state.decision_hits)
        self.assertEqual(self.store.scan_stats("sot-1"), {OUTCOME_DONE: 2})
        self.assertEqual(self.store.scan_stubs("sot-1"), [])

    def test_retry_keeps_successful_decisions_untouched(self) -> None:
        decision_ids = self.load(count=2, page_size=2)
        self.server.state.decision_status[decision_ids[0]] = 500
        scanner = self.make_scanner()
        self.run_scan(scanner)
        self.server.state.decision_hits.clear()

        self.run_scan(scanner, retry_failed=True)

        self.assertNotIn(decision_ids[1], self.server.state.decision_hits)


class SotStubTest(SotScanBase):
    def test_inaccessible_and_missing_decisions_get_credential_free_stubs(self) -> None:
        decision_ids = self.load(count=4, page_size=2)
        forbidden, missing, empty, good = decision_ids
        self.server.state.decision_status[forbidden] = 403
        self.server.state.decision_status[missing] = 404
        self.server.state.decisions[empty] = make_decision_payload(empty, text="   ")
        scanner = self.make_scanner()

        state = self.run_scan(scanner)

        self.assertEqual(state.phase, PHASE_COMPLETED)
        stats = self.store.scan_stats("sot-1")
        self.assertEqual(stats.get(OUTCOME_DONE), 1)
        self.assertEqual(stats.get(OUTCOME_INACCESSIBLE), 2)
        self.assertEqual(stats.get(OUTCOME_NOT_FOUND), 1)

        stubs = {stub["decision_id"]: stub for stub in self.store.scan_stubs("sot-1")}
        self.assertEqual(set(stubs), {forbidden, missing, empty})
        self.assertEqual(stubs[forbidden]["http_status"], 403)
        self.assertEqual(stubs[missing]["outcome"], OUTCOME_NOT_FOUND)
        self.assertEqual(stubs[empty]["failure_kind"], "no_text")
        for stub in stubs.values():
            self.assertEqual(set(stub), set(STUB_FIELDS))
            self.assertEqual(stub["source_system"], SOURCE_SYSTEM)
            self.assertEqual(stub["corpus_type"], CORPUS_TYPE)
            self.assertTrue(stub["decision_key"].startswith("prg_sot:"))
            rendered = json.dumps(stub, ensure_ascii=False)
            self.assertNotIn("must-not-leak", rendered)
            self.assertNotIn(self.server.state.password, rendered)
            self.assertNotIn(self.server.state.username, rendered)
            self.assertNotIn("SOTSESSION", rendered)

    def test_stubs_survive_a_restart_and_are_dumpable(self) -> None:
        decision_ids = self.load(count=2, page_size=2)
        self.server.state.decision_status[decision_ids[0]] = 403
        self.run_scan(self.make_scanner())

        reopened = SotStore(self.out_dir)
        self.addCleanup(reopened.close)
        stubs = reopened.scan_stubs("sot-1")
        self.assertEqual(len(stubs), 1)
        self.assertEqual(stubs[0]["decision_id"], decision_ids[0])


class SotFatalErrorTest(SotScanBase):
    def test_auth_error_aborts_and_hands_the_decision_back(self) -> None:
        decision_ids = self.load(count=2, page_size=2)
        scanner = self.make_scanner()
        error = SourceAuthError("https://sb.prg.kz/api", "PRG.SOT session could not be established")

        with mock.patch.object(SotSource, "fetch_decision", side_effect=error):
            with self.assertRaises(SourceAuthError):
                self.run_scan(scanner)

        state = self.store.get_scan("sot-1")
        self.assertEqual(state.phase, PHASE_ABORTED)
        self.assertIn("auth", state.error or "")
        statuses = {decision_key(item): self.store.decision_status(decision_key(item)) for item in decision_ids}
        self.assertNotIn("failed", statuses.values())
        self.assertIn(STATUS_QUEUED, statuses.values())
        self.assertEqual(self.store.scan_stubs("sot-1"), [])

    def test_aborted_scan_resumes_on_the_next_run(self) -> None:
        decision_ids = self.load(count=2, page_size=2)
        scanner = self.make_scanner()
        error = SourceAuthError("https://sb.prg.kz/api", "PRG.SOT session could not be established")
        with mock.patch.object(SotSource, "fetch_decision", side_effect=error):
            with self.assertRaises(SourceAuthError):
                self.run_scan(scanner)

        state = self.run_scan(scanner)

        self.assertEqual(state.phase, PHASE_COMPLETED)
        for item in decision_ids:
            self.assertEqual(self.store.decision_status(decision_key(item)), STATUS_EXPORTED)


class SotRateLimitTest(SotScanBase):
    def make_rate_limit_error(self, retry_after: float) -> SourceRateLimitError:
        return SourceRateLimitError(
            "https://sb.prg.kz/api/search",
            "Source rate limit reached (HTTP 429).",
            RateLimitInfo(retry_after=retry_after, remaining=0, limit=25000),
        )

    def test_short_limit_is_waited_out_and_the_scan_continues(self) -> None:
        decision_ids = self.load(count=4, page_size=2)
        self.server.state.search_rate_limit_after = 1
        scanner = self.make_scanner(max_pause_seconds=60)

        def clear_limit(seconds: float) -> None:
            self.sleep.calls.append(seconds)
            self.server.state.search_rate_limit_after = None

        scanner._sleep = clear_limit

        state = self.run_scan(scanner)

        self.assertEqual(state.phase, PHASE_COMPLETED)
        # Exactly the wait the source asked for, no shortcut and no extra push.
        self.assertIn(2.0, self.sleep.calls)
        self.assertEqual(sorted(self.server.state.decision_hits), sorted(decision_ids))

    def test_long_limit_stops_the_run_instead_of_pushing_through(self) -> None:
        self.load(count=4, page_size=2)
        scanner = self.make_scanner(max_pause_seconds=1)
        error = self.make_rate_limit_error(3600)

        with mock.patch.object(SotSource, "fetch_search_page", side_effect=error):
            state = self.run_scan(scanner)

        self.assertEqual(state.phase, PHASE_RATE_LIMITED)
        self.assertIn("rate limit", state.rate_limit_note or "")
        self.assertIn("remaining=0", state.rate_limit_note or "")
        self.assertEqual(self.sleep.calls, [])
        self.assertEqual(self.server.state.decision_hits, [])

    def test_pause_budget_stops_a_run_that_keeps_being_throttled(self) -> None:
        self.load(count=8, page_size=2)
        scanner = self.make_scanner(max_pause_seconds=60, pause_budget=2)
        error = self.make_rate_limit_error(1)

        with mock.patch.object(SotSource, "fetch_search_page", side_effect=error):
            state = self.run_scan(scanner)

        self.assertEqual(state.phase, PHASE_RATE_LIMITED)
        self.assertEqual(self.sleep.calls, [1.0, 1.0])

    def test_rate_limit_while_fetching_returns_the_decision_to_the_queue(self) -> None:
        decision_ids = self.load(count=2, page_size=2)
        scanner = self.make_scanner()
        error = self.make_rate_limit_error(3600)

        with mock.patch.object(SotSource, "fetch_decision", side_effect=error):
            state = self.run_scan(scanner)

        self.assertEqual(state.phase, PHASE_RATE_LIMITED)
        statuses = {self.store.decision_status(decision_key(item)) for item in decision_ids}
        self.assertEqual(statuses, {STATUS_QUEUED})
        self.assertEqual(self.store.scan_stubs("sot-1"), [])

        # The next run picks the same decisions up without losing any.
        state = self.run_scan(scanner)
        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertEqual(self.store.scan_stats("sot-1"), {OUTCOME_DONE: 2})


class SotSeparationTest(SotScanBase):
    def test_keys_are_namespaced_and_never_bare_ids(self) -> None:
        self.assertEqual(decision_key("35502996"), "prg_sot:35502996")
        self.assertEqual(decision_key("prg_sot:1"), "prg_sot:1")
        with self.assertRaises(ValueError):
            decision_key("  ")

    def test_sot_tables_are_separate_from_the_zanger_ones(self) -> None:
        self.load(count=2, page_size=2)
        self.run_scan(self.make_scanner())

        with self.store._conn as conn:
            tables = {
                str(row[0])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
        self.assertEqual(
            tables & {"sot_scans", "sot_decisions", "sot_decision_outputs", "sot_scan_decisions"},
            {"sot_scans", "sot_decisions", "sot_decision_outputs", "sot_scan_decisions"},
        )
        self.assertEqual(tables & {"documents", "document_outputs", "listing_pages", "catalog_scans"}, set())
        self.assertTrue((Path(self.out_dir) / "sot_state.sqlite3").exists())
        self.assertFalse((Path(self.out_dir) / "state.sqlite3").exists())

    def test_provenance_columns_are_immutable(self) -> None:
        decision_ids = self.load(count=1, page_size=2)
        self.run_scan(self.make_scanner())
        key = decision_key(decision_ids[0])

        import sqlite3

        for column, value in (("source_system", "prg_zanger"), ("corpus_type", "legal_act")):
            with self.assertRaises(sqlite3.IntegrityError):
                with self.store._conn as conn:
                    conn.execute(f"UPDATE sot_decisions SET {column} = ? WHERE decision_key = ?", (value, key))

        row = self.store.get_decision(key)
        self.assertEqual(row["source_system"], SOURCE_SYSTEM)
        self.assertEqual(row["corpus_type"], CORPUS_TYPE)

    def test_outputs_store_text_and_raw_json_with_hashes(self) -> None:
        decision_ids = self.load(count=1, page_size=2)
        self.run_scan(self.make_scanner())
        key = decision_key(decision_ids[0])

        self.assertTrue(self.store.has_decision_outputs(key, ("txt", "json")))
        with self.store._conn as conn:
            rows = conn.execute(
                "SELECT format, sha256, size_bytes, content FROM sot_decision_outputs WHERE decision_key = ?",
                (key,),
            ).fetchall()
        formats = {str(row["format"]): row for row in rows}
        self.assertEqual(set(formats), {"txt", "json"})
        for row in rows:
            self.assertEqual(len(str(row["sha256"])), 64)
            self.assertGreater(int(row["size_bytes"]), 0)
        self.assertIn("Текст судебного акта", bytes(formats["txt"]["content"]).decode("utf-8"))
        json.loads(bytes(formats["json"]["content"]).decode("utf-8"))

    def test_txt_output_hash_matches_the_knowledge_indexer_join_contract(self) -> None:
        """The knowledge service seeds sot_search_index_jobs from the txt
        output's sha256 and compares it with the claimed job. That hash must
        equal sot_decisions.text_sha256 for every exported decision, or a
        changed decision could be indexed as if nothing changed."""
        decision_ids = self.load(count=1, page_size=2)
        self.run_scan(self.make_scanner())
        key = decision_key(decision_ids[0])

        row = self.store.get_decision(key)
        self.assertEqual(row["status"], STATUS_EXPORTED)
        self.assertTrue(row["text_sha256"])
        with self.store._conn as conn:
            txt_row = conn.execute(
                "SELECT sha256, content FROM sot_decision_outputs WHERE decision_key = ? AND format = 'txt'",
                (key,),
            ).fetchone()
        self.assertIsNotNone(txt_row)
        self.assertEqual(str(txt_row["sha256"]), row["text_sha256"])
        self.assertNotEqual(str(txt_row["sha256"]), row["raw_sha256"])


class SotStoreApiTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.store = SotStore(tmp.name)
        self.addCleanup(self.store.close)

    def test_both_stores_expose_the_same_api(self) -> None:
        for name in STORE_METHODS:
            sqlite_method = getattr(SotStore, name, None)
            postgres_method = getattr(SotPostgresStore, name, None)
            self.assertIsNotNone(sqlite_method, f"SotStore is missing {name}")
            self.assertIsNotNone(postgres_method, f"SotPostgresStore is missing {name}")
            self.assertEqual(
                inspect.signature(sqlite_method),
                inspect.signature(postgres_method),
                f"{name} signatures differ between the stores",
            )

    def test_state_operations_are_idempotent(self) -> None:
        state = self.store.ensure_scan("scan-x", "fingerprint-1", "иск")
        self.assertEqual(state.next_page, 1)
        again = self.store.ensure_scan("scan-x", "fingerprint-1", "иск")
        self.assertEqual(again.started_at, state.started_at)

        refs = [
            SotDecisionRef(decision_id="1", decision_key=decision_key("1"), page=1, position=0),
            SotDecisionRef(decision_id="2", decision_key=decision_key("2"), page=1, position=1),
        ]
        self.assertEqual(self.store.record_search_page("scan-x", 1, refs), 2)
        self.assertEqual(self.store.record_search_page("scan-x", 1, refs), 2)
        self.store.advance_scan("scan-x", next_page=2, decisions_enqueued=2)
        self.store.advance_scan("scan-x", next_page=2, decisions_enqueued=2)

        state = self.store.get_scan("scan-x")
        self.assertEqual(state.next_page, 2)
        self.assertEqual(state.pages_done, 1)
        self.assertEqual(state.decisions_seen, 2)
        self.assertEqual(state.decisions_enqueued, 2)
        self.assertEqual(self.store.pending_decision_count("scan-x"), 2)

        stub = self.store.record_decision_outcome(
            "scan-x",
            decision_key("1"),
            OUTCOME_INACCESSIBLE,
            failure_kind="forbidden",
            http_status=403,
            detail="Source request failed with HTTP 403.",
        )
        self.assertEqual(stub["decision_id"], "1")
        self.assertEqual(self.store.scan_stats("scan-x").get(OUTCOME_INACCESSIBLE), 1)
        self.assertIsNone(self.store.record_decision_outcome("scan-x", decision_key("404"), OUTCOME_DONE))

        self.assertEqual(self.store.retry_scan_outcomes("scan-x"), 1)
        self.assertEqual(self.store.scan_stats("scan-x"), {OUTCOME_PENDING: 2})
        self.assertEqual(self.store.scan_stubs("scan-x"), [])

    def test_mismatched_contract_is_refused(self) -> None:
        self.store.ensure_scan("scan-y", "fingerprint-1", "")
        with self.assertRaises(ValueError):
            self.store.ensure_scan("scan-y", "fingerprint-2", "")


class SotCliTest(SotScanBase):
    def test_scan_without_a_contract_fails_before_touching_storage(self) -> None:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as err:
            with self.assertRaises(SystemExit) as ctx:
                cli.main(["--out", f"{self.out_dir}/fresh", "sot-scan", "--scan-id", "cli-scan"])

        self.assertEqual(ctx.exception.code, 2)
        message = err.getvalue()
        self.assertIn("AI_ADVOCAT_SOT_SEARCH_URL_TEMPLATE", message)
        self.assertFalse(Path(self.out_dir, "fresh").exists())
        self.assertEqual(self.server.state.search_hits, [])

    def test_status_reports_the_missing_contract_without_calling_the_source(self) -> None:
        with contextlib.redirect_stdout(io.StringIO()) as out:
            cli.main(["--out", self.out_dir, "sot-status"])

        rendered = out.getvalue()
        self.assertIn("PRG.SOT", rendered)
        self.assertIn("не настроен", rendered)
        self.assertIn("AI_ADVOCAT_SOT_SEARCH_URL_TEMPLATE", rendered)
        self.assertEqual(self.server.state.login_posts, 0)
        self.assertEqual(self.server.state.search_hits, [])

    def test_status_reports_a_finished_scan(self) -> None:
        self.load(count=2, page_size=2)
        self.run_scan(self.make_scanner(), scan_id="cli-scan")

        with contextlib.redirect_stdout(io.StringIO()) as out:
            cli.main(["--out", self.out_dir, "sot-status", "--scan-id", "cli-scan"])

        rendered = out.getvalue()
        self.assertIn("cli-scan", rendered)
        self.assertIn(PHASE_COMPLETED, rendered)
        self.assertIn(f"{OUTCOME_DONE}: 2", rendered)
        self.assertIn("prg_sot/judicial_decision", rendered)

    def test_stubs_command_dumps_json(self) -> None:
        decision_ids = self.load(count=2, page_size=2)
        self.server.state.decision_status[decision_ids[0]] = 403
        self.run_scan(self.make_scanner(), scan_id="cli-scan")

        with contextlib.redirect_stdout(io.StringIO()) as out, contextlib.redirect_stderr(io.StringIO()):
            cli.main(["--out", self.out_dir, "sot-stubs", "--scan-id", "cli-scan"])
        payload = json.loads(out.getvalue())

        self.assertEqual(payload["scan_id"], "cli-scan")
        self.assertEqual(payload["source_system"], SOURCE_SYSTEM)
        self.assertEqual(len(payload["decisions"]), 1)

        target = Path(self.out_dir) / "stubs" / "cli-scan.json"
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            cli.main(["--out", self.out_dir, "sot-stubs", "--scan-id", "cli-scan", "--output", str(target)])
        self.assertEqual(json.loads(target.read_text(encoding="utf-8"))["scan_id"], "cli-scan")

    def test_probe_auth_logs_in_and_reads_one_page_without_writing(self) -> None:
        self.load(count=4, page_size=2)
        overrides = self.server.config_overrides()
        env = clean_env(
            **{
                SOT_USERNAME_ENV: self.server.state.username,
                SOT_PASSWORD_ENV: self.server.state.password,
                "AI_ADVOCAT_SOT_BASE_URL": overrides["base_url"],
                "AI_ADVOCAT_SOT_SEARCH_URL_TEMPLATE": overrides["search_url_template"],
                "AI_ADVOCAT_SOT_DECISION_URL_TEMPLATE": overrides["decision_url_template"],
                "AI_ADVOCAT_SOT_RESULTS_PATH": overrides["results_path"],
                "AI_ADVOCAT_SOT_TOTAL_PATH": overrides["total_path"],
                "AI_ADVOCAT_SOT_ID_PATH": overrides["id_path"],
                "AI_ADVOCAT_SOT_TEXT_PATH": overrides["text_path"],
                "AI_ADVOCAT_SOT_FIELD_MAP": overrides["field_map"],
            }
        )
        with mock.patch.dict(os.environ, env, clear=True):
            with contextlib.redirect_stdout(io.StringIO()) as out:
                code = cli.sot_runtime.probe_auth(timeout=5, page=1, login_url=self.server.login_url)

        rendered = out.getvalue()
        self.assertEqual(code, 0)
        self.assertIn("SUDBASEV2", rendered)
        self.assertIn("/assets/sot-app.js?v=1", rendered)
        self.assertNotIn("cdn.invalid", rendered)
        self.assertIn("элементов 2", rendered)
        self.assertIn("поля ответа: data", rendered)
        self.assertIn("поля карточки: caseNumber", rendered)
        self.assertIn("prg_sot:", rendered)
        self.assertNotIn(self.server.state.password, rendered)
        self.assertNotIn("SOTSESSION", rendered)
        self.assertEqual(self.server.state.search_hits, [1])
        self.assertEqual(self.server.state.decision_hits, [])

    def test_probe_auth_without_credentials_is_a_clear_failure(self) -> None:
        with mock.patch.dict(os.environ, clean_env(), clear=True):
            with contextlib.redirect_stdout(io.StringIO()) as out:
                code = cli.sot_runtime.probe_auth(timeout=5, login_url=self.server.login_url)
        self.assertEqual(code, 2)
        self.assertIn(SOT_USERNAME_ENV, out.getvalue())
        self.assertEqual(self.server.state.login_posts, 0)

    def test_probe_auth_only_succeeds_before_source_contract_is_captured(self) -> None:
        env = clean_env(
            **{
                SOT_USERNAME_ENV: self.server.state.username,
                SOT_PASSWORD_ENV: self.server.state.password,
                "AI_ADVOCAT_SOT_BASE_URL": self.server.base_url,
            }
        )
        with mock.patch.dict(os.environ, env, clear=True):
            with contextlib.redirect_stdout(io.StringIO()) as out:
                code = cli.sot_runtime.probe_auth(timeout=5, login_url=self.server.login_url)

        rendered = out.getvalue()
        self.assertEqual(code, 0)
        self.assertIn("вход выполнен", rendered)
        self.assertIn("контракт источника не настроен", rendered)
        self.assertEqual(self.server.state.login_posts, 1)
        self.assertEqual(self.server.state.search_hits, [])


if __name__ == "__main__":
    unittest.main()
