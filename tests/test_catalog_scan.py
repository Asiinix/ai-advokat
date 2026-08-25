"""Coverage for the resumable full catalog scan.

Everything runs against the local fake PRG server and a SQLite state file, so
no test needs network access or real credentials.
"""

from __future__ import annotations

import contextlib
import inspect
import io
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ai_advokat_parser import catalog, cli, crawler as crawler_module, document as document_module, http_client
from ai_advokat_parser.catalog import (
    OUTCOME_DONE,
    OUTCOME_INACCESSIBLE,
    OUTCOME_NOT_FOUND,
    PHASE_ABORTED,
    PHASE_COMPLETED,
    PHASE_PAUSED,
    CatalogDiscoveryError,
)
from ai_advokat_parser.config import (
    AUTH_PASSWORD_ENV,
    AUTH_USERNAME_ENV,
    DEFAULT_ALL_DOCUMENTS_LIST_URL,
    DEFAULT_LIST_URL,
)
from ai_advokat_parser.crawler import Crawler
from ai_advokat_parser.http_client import SourceAccessDeniedError, SourceAuthError
from ai_advokat_parser.listing import DocumentRef
from ai_advokat_parser.postgres_store import PostgresCrawlStore
from ai_advokat_parser.store import CrawlStore

from .support_http import FakeSourceServer, make_empty_document_payload

CATALOG_METHODS = (
    "ensure_catalog_scan",
    "get_catalog_scan",
    "set_catalog_scan_discovery",
    "set_catalog_scan_phase",
    "record_catalog_page",
    "advance_catalog_scan",
    "is_catalog_scan_member",
    "record_catalog_document_outcome",
    "resolve_catalog_scan_outcomes",
    "pending_catalog_document_count",
    "reclaim_catalog_scan_documents",
    "catalog_scan_stats",
    "catalog_scan_stubs",
    "enqueue_document_refs",
)


def clean_env(**overrides: str) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {
            AUTH_USERNAME_ENV,
            AUTH_PASSWORD_ENV,
            "AI_ADVOCAT_DATABASE_URL",
            "DATABASE_URL",
        }
    }
    env.update(overrides)
    return env


class CatalogListUrlTest(unittest.TestCase):
    def test_all_documents_url_asks_for_paid_documents_too(self) -> None:
        self.assertIn("onlyFreeDocuments=false", DEFAULT_ALL_DOCUMENTS_LIST_URL)
        self.assertEqual(catalog_url_of_config(), DEFAULT_ALL_DOCUMENTS_LIST_URL)

    def test_legacy_default_url_stays_free_only(self) -> None:
        self.assertIn("onlyFreeDocuments=true", DEFAULT_LIST_URL)
        self.assertNotIn("onlyFreeDocuments=false", DEFAULT_LIST_URL)


def catalog_url_of_config() -> str:
    from ai_advokat_parser.config import ALL_DOCUMENTS_LIST_URL

    return ALL_DOCUMENTS_LIST_URL


class CatalogScanBase(unittest.TestCase):
    def setUp(self) -> None:
        self.server = FakeSourceServer().start()
        self.addCleanup(self.server.stop)
        for module, name, value in (
            (http_client, "AUTH_LOGIN_URL", self.server.login_url),
            (http_client, "AUTH_RETURN_URL", f"{self.server.base_url}/prg/"),
            (document_module, "API_BASE_URL", f"{self.server.base_url}/mapi"),
        ):
            patcher = mock.patch.object(module, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

        env_patcher = mock.patch.dict(
            os.environ,
            clean_env(
                **{
                    AUTH_USERNAME_ENV: self.server.state.username,
                    AUTH_PASSWORD_ENV: self.server.state.password,
                }
            ),
            clear=True,
        )
        env_patcher.start()
        self.addCleanup(env_patcher.stop)

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.out_dir = tmp.name
        self.list_url = f"{self.server.base_url}/catalog"
        self.formats = ("html", "txt")

    def make_crawler(self, **kwargs) -> Crawler:
        with contextlib.redirect_stdout(io.StringIO()):
            crawler = Crawler(
                out_dir=self.out_dir,
                formats=self.formats,
                only_free=False,
                delay=0,
                timeout=5,
                retries=2,
                **kwargs,
            )
        self.addCleanup(crawler.close)
        return crawler

    def run_scan(self, crawler: Crawler, scan_id: str = "scan-1", **kwargs):
        with contextlib.redirect_stdout(io.StringIO()):
            return crawler.run_catalog_scan(
                scan_id=scan_id,
                list_url=self.list_url,
                poll_interval=0,
                **kwargs,
            )

    def load_catalog(self, count: int = 5, page_size: int = 2, total: int | None = None) -> list[str]:
        doc_ids = [str(1000 + index) for index in range(count)]
        self.server.state.load_catalog(doc_ids, page_size=page_size, total=total)
        return doc_ids


class CatalogDiscoveryTest(CatalogScanBase):
    def test_discovery_stores_total_page_size_and_page_count(self) -> None:
        doc_ids = self.load_catalog(count=5, page_size=2)
        crawler = self.make_crawler()

        state = self.run_scan(crawler)

        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertEqual(state.total_documents, 5)
        self.assertEqual(state.page_size, 2)
        self.assertEqual(state.total_pages, 3)
        self.assertEqual(state.next_page, 4)
        self.assertEqual(state.docs_seen, 5)
        self.assertEqual(crawler.store.catalog_scan_stats("scan-1"), {OUTCOME_DONE: 5})
        self.assertEqual(sorted(set(self.server.state.document_hits)), sorted(doc_ids))
        for doc_id in doc_ids:
            self.assertTrue((Path(self.out_dir) / "documents" / doc_id / "document.html").exists())

    def test_missing_total_is_fatal_and_downloads_nothing(self) -> None:
        self.load_catalog(count=4, page_size=2)
        self.server.state.catalog_total = None
        crawler = self.make_crawler()

        with self.assertRaises(CatalogDiscoveryError):
            self.run_scan(crawler)

        state = crawler.store.get_catalog_scan("scan-1")
        self.assertEqual(state.phase, PHASE_ABORTED)
        self.assertIsNone(state.total_pages)
        self.assertEqual(self.server.state.document_hits, [])

    def test_configuration_change_on_same_scan_id_is_rejected(self) -> None:
        self.load_catalog(count=2, page_size=2)
        crawler = self.make_crawler()
        self.run_scan(crawler)

        with self.assertRaises(ValueError) as ctx:
            crawler.store.ensure_catalog_scan("scan-1", "https://other.invalid/list", "lawyer", self.formats)
        self.assertIn("different configuration", str(ctx.exception))


class CatalogResumeTest(CatalogScanBase):
    def test_legacy_listing_progress_does_not_block_the_scan(self) -> None:
        doc_ids = self.load_catalog(count=4, page_size=2)
        crawler = self.make_crawler()
        # Pretend an older `range` run already finished the same listing pages.
        crawler.store.save_listing_documents(1, [DocumentRef(doc_id=doc_ids[0])])
        crawler.store.mark_listing_page(1, "done", doc_count=1)
        crawler.store.mark_listing_documents_status(1, "done")

        state = self.run_scan(crawler)

        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertEqual(sorted(self.server.state.catalog_page_hits), [1, 2])
        self.assertEqual(sorted(set(self.server.state.document_hits)), sorted(doc_ids))

    def test_restart_resumes_at_next_page_and_completed_rerun_is_a_noop(self) -> None:
        doc_ids = self.load_catalog(count=6, page_size=2)
        crawler = self.make_crawler()

        first = self.run_scan(crawler, max_pages=1)
        self.assertEqual(first.phase, PHASE_PAUSED)
        self.assertEqual(first.next_page, 2)
        self.assertEqual(self.server.state.catalog_page_hits, [1])
        self.assertEqual(sorted(set(self.server.state.document_hits)), doc_ids[:2])

        second = self.run_scan(crawler)
        self.assertEqual(second.phase, PHASE_COMPLETED)
        self.assertEqual(self.server.state.catalog_page_hits, [1, 2, 3])
        self.assertEqual(sorted(set(self.server.state.document_hits)), sorted(doc_ids))

        pages_before = list(self.server.state.catalog_page_hits)
        docs_before = list(self.server.state.document_hits)
        third = self.run_scan(crawler)
        self.assertEqual(third.phase, PHASE_COMPLETED)
        self.assertEqual(self.server.state.catalog_page_hits, pages_before)
        self.assertEqual(self.server.state.document_hits, docs_before)

    def test_max_docs_smoke_run_pauses_inside_a_page(self) -> None:
        self.load_catalog(count=6, page_size=3)
        crawler = self.make_crawler()

        state = self.run_scan(crawler, max_docs=2)

        self.assertEqual(state.phase, PHASE_PAUSED)
        self.assertEqual(state.next_page, 1)
        self.assertEqual(state.docs_seen, 2)
        self.assertEqual(len(self.server.state.document_hits), 2)

        state = self.run_scan(crawler)
        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertEqual(state.docs_seen, 6)

    def test_documents_left_processing_by_a_restart_are_reclaimed(self) -> None:
        doc_ids = self.load_catalog(count=2, page_size=2)
        crawler = self.make_crawler()
        with contextlib.redirect_stdout(io.StringIO()):
            crawler.store.ensure_catalog_scan("scan-1", self.list_url, crawler.product, self.formats)
        crawler.store.record_catalog_page("scan-1", 1, [DocumentRef(doc_id=doc_ids[0])])
        crawler.store.enqueue_document_refs([DocumentRef(doc_id=doc_ids[0])], formats=self.formats)
        crawler.store.claim_queued_document("dead-container")
        self.assertEqual(crawler.store.get_document_status(doc_ids[0]), "processing")

        state = self.run_scan(crawler)

        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertEqual(crawler.store.get_document_status(doc_ids[0]), "exported")


class CatalogSkipAndRetryTest(CatalogScanBase):
    def test_exported_document_with_all_outputs_is_not_downloaded_again(self) -> None:
        doc_ids = self.load_catalog(count=4, page_size=2)
        ready = doc_ids[1]
        doc_dir = Path(self.out_dir) / "documents" / ready
        doc_dir.mkdir(parents=True)
        (doc_dir / "document.html").write_text("<html></html>", encoding="utf-8")
        (doc_dir / "document.txt").write_text("text", encoding="utf-8")
        crawler = self.make_crawler()
        crawler.store.upsert_document(ready, "exported", title="готово", formats=self.formats)

        state = self.run_scan(crawler)

        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertNotIn(ready, self.server.state.document_hits)
        self.assertEqual(crawler.store.catalog_scan_stats("scan-1"), {OUTCOME_DONE: 4})

    def test_previous_paid_failure_is_retried_by_the_scan(self) -> None:
        doc_ids = self.load_catalog(count=2, page_size=2)
        paid = doc_ids[0]
        crawler = self.make_crawler()
        crawler.store.upsert_document(
            paid,
            "failed",
            title="платный",
            is_free=False,
            error=f"Document {paid} is not marked as free.",
        )
        self.assertTrue(crawler.store.is_terminal_document_failure(paid))

        state = self.run_scan(crawler)

        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertIn(paid, self.server.state.document_hits)
        self.assertEqual(crawler.store.get_document_status(paid), "exported")
        self.assertEqual(crawler.store.catalog_scan_stats("scan-1"), {OUTCOME_DONE: 2})


class CatalogFailureStubTest(CatalogScanBase):
    def test_inaccessible_and_missing_documents_get_credential_free_stubs(self) -> None:
        doc_ids = self.load_catalog(count=4, page_size=2)
        forbidden, missing, empty, good = doc_ids
        self.server.state.document_status[forbidden] = 403
        self.server.state.document_status[missing] = 404
        self.server.state.documents[empty] = make_empty_document_payload(empty)
        crawler = self.make_crawler()

        state = self.run_scan(crawler)

        self.assertEqual(state.phase, PHASE_COMPLETED)
        stats = crawler.store.catalog_scan_stats("scan-1")
        self.assertEqual(stats.get(OUTCOME_DONE), 1)
        self.assertEqual(stats.get(OUTCOME_INACCESSIBLE), 2)
        self.assertEqual(stats.get(OUTCOME_NOT_FOUND), 1)

        stubs = {stub["doc_id"]: stub for stub in crawler.store.catalog_scan_stubs("scan-1")}
        self.assertEqual(set(stubs), {forbidden, missing, empty})
        self.assertEqual(stubs[forbidden]["outcome"], OUTCOME_INACCESSIBLE)
        self.assertEqual(stubs[forbidden]["http_status"], 403)
        self.assertEqual(stubs[missing]["outcome"], OUTCOME_NOT_FOUND)
        self.assertEqual(stubs[empty]["failure_kind"], "no_pages")
        self.assertEqual(stubs[good if good in stubs else forbidden]["scan_id"], "scan-1")
        for stub in stubs.values():
            self.assertEqual(set(stub), set(catalog.STUB_FIELDS))
            rendered = json.dumps(stub, ensure_ascii=False)
            self.assertNotIn("must-not-leak", rendered)
            self.assertNotIn(self.server.state.password, rendered)
            self.assertNotIn(self.server.state.username, rendered)
            self.assertNotIn("PRGSESSION", rendered)

    def test_success_after_a_previous_failure_clears_the_stub(self) -> None:
        doc_ids = self.load_catalog(count=2, page_size=2)
        broken = doc_ids[0]
        self.server.state.document_status[broken] = 404
        crawler = self.make_crawler()
        self.run_scan(crawler)
        self.assertEqual(len(crawler.store.catalog_scan_stubs("scan-1")), 1)

        del self.server.state.document_status[broken]
        crawler.store.set_catalog_scan_phase("scan-1", PHASE_PAUSED)
        crawler.store.record_catalog_document_outcome("scan-1", broken, catalog.OUTCOME_PENDING)
        crawler.store.enqueue_document_refs(
            [DocumentRef(doc_id=broken)],
            formats=self.formats,
            retry_failed=True,
        )
        self.run_scan(crawler)

        self.assertEqual(crawler.store.catalog_scan_stubs("scan-1"), [])
        self.assertEqual(crawler.store.catalog_scan_stats("scan-1"), {OUTCOME_DONE: 2})

    def test_access_denied_after_reauth_is_not_fatal(self) -> None:
        self.assertFalse(issubclass(SourceAccessDeniedError, SourceAuthError))
        outcome, failure_kind, status = catalog.classify_document_failure(
            SourceAccessDeniedError("https://prg.kz/mapi", "still walled", status=401)
        )
        self.assertEqual((outcome, failure_kind, status), (OUTCOME_INACCESSIBLE, "access_denied", 401))

    def test_stub_detail_drops_html_and_secrets(self) -> None:
        self.assertIn("HTML page", catalog.sanitize_detail("<html><body>secret</body></html>"))
        self.assertIn("sensitive", catalog.sanitize_detail("Set-Cookie: PRGSESSION=abc"))
        self.assertEqual(
            catalog.sanitize_detail("login failed for s3cret", {AUTH_PASSWORD_ENV: "s3cret"}),
            "login failed for ***",
        )


class CatalogAuthFailureTest(CatalogScanBase):
    def test_auth_error_aborts_the_scan_without_mass_failures(self) -> None:
        doc_ids = self.load_catalog(count=4, page_size=2)
        crawler = self.make_crawler()
        error = SourceAuthError("https://prg.kz/mapi", "PRG session could not be established")

        with mock.patch.object(crawler_module.DocumentDownloader, "fetch_document", side_effect=error):
            with self.assertRaises(SourceAuthError):
                self.run_scan(crawler)

        state = crawler.store.get_catalog_scan("scan-1")
        self.assertEqual(state.phase, PHASE_ABORTED)
        self.assertIn("auth", (state.error or ""))
        statuses = {doc_id: crawler.store.get_document_status(doc_id) for doc_id in doc_ids}
        self.assertNotIn("failed", statuses.values())
        # The document the worker had already claimed goes back to the queue.
        self.assertIn("queued", statuses.values())
        self.assertEqual(crawler.store.catalog_scan_stubs("scan-1"), [])

    def test_aborted_scan_resumes_on_the_next_run(self) -> None:
        doc_ids = self.load_catalog(count=2, page_size=2)
        crawler = self.make_crawler()
        error = SourceAuthError("https://prg.kz/mapi", "PRG session could not be established")
        with mock.patch.object(crawler_module.DocumentDownloader, "fetch_document", side_effect=error):
            with self.assertRaises(SourceAuthError):
                self.run_scan(crawler)

        state = self.run_scan(crawler)

        self.assertEqual(state.phase, PHASE_COMPLETED)
        self.assertEqual(crawler.store.catalog_scan_stats("scan-1"), {OUTCOME_DONE: 2})
        for doc_id in doc_ids:
            self.assertEqual(crawler.store.get_document_status(doc_id), "exported")


class CatalogStoreApiTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.out_dir = Path(tmp.name)
        self.store = CrawlStore(self.out_dir)
        self.addCleanup(self.store.close)

    def test_both_stores_expose_the_same_catalog_api(self) -> None:
        for name in CATALOG_METHODS:
            sqlite_method = getattr(CrawlStore, name, None)
            postgres_method = getattr(PostgresCrawlStore, name, None)
            self.assertIsNotNone(sqlite_method, f"CrawlStore is missing {name}")
            self.assertIsNotNone(postgres_method, f"PostgresCrawlStore is missing {name}")
            self.assertEqual(
                inspect.signature(sqlite_method),
                inspect.signature(postgres_method),
                f"{name} signatures differ between the stores",
            )

    def test_zanger_rows_carry_immutable_corpus_provenance(self) -> None:
        self.store.upsert_document("7000", "queued")
        with self.store._conn as conn:
            row = conn.execute(
                "SELECT source_system, corpus_type FROM documents WHERE doc_id = ?",
                ("7000",),
            ).fetchone()
        self.assertEqual((row["source_system"], row["corpus_type"]), ("prg_zanger", "legal_act"))

        for column, value in (("source_system", "prg_sot"), ("corpus_type", "judicial_decision")):
            with self.assertRaises(sqlite3.IntegrityError):
                with self.store._conn as conn:
                    conn.execute(f"UPDATE documents SET {column} = ? WHERE doc_id = ?", (value, "7000"))

    def test_enqueue_keeps_terminal_failures_unless_retry_is_asked(self) -> None:
        ref = DocumentRef(doc_id="7001")
        self.store.upsert_document("7001", "failed", is_free=False, error="Document 7001 is not marked as free.")

        self.assertEqual(self.store.enqueue_document_refs([ref], formats=("html",)), 0)
        self.assertEqual(self.store.get_document_status("7001"), "failed")

        self.assertEqual(self.store.enqueue_document_refs([ref], formats=("html",), retry_failed=True), 1)
        self.assertEqual(self.store.get_document_status("7001"), "queued")

    def test_retry_failed_still_skips_exported_documents_with_outputs(self) -> None:
        doc_dir = self.out_dir / "documents" / "7002"
        doc_dir.mkdir(parents=True)
        (doc_dir / "document.html").write_text("<html></html>", encoding="utf-8")
        self.store.upsert_document("7002", "exported", formats=("html",))

        added = self.store.enqueue_document_refs(
            [DocumentRef(doc_id="7002")],
            formats=("html",),
            retry_failed=True,
        )

        self.assertEqual(added, 0)
        self.assertEqual(self.store.get_document_status("7002"), "exported")

    def test_catalog_state_operations_are_idempotent(self) -> None:
        state = self.store.ensure_catalog_scan("scan-x", "https://prg.kz/list", "lawyer", ("html", "txt"))
        self.assertEqual(state.next_page, 1)
        self.assertEqual(state.formats, ("html", "txt"))
        again = self.store.ensure_catalog_scan("scan-x", "https://prg.kz/list", "lawyer", ("html", "txt"))
        self.assertEqual(again.started_at, state.started_at)

        refs = [DocumentRef(doc_id="8001", title="A"), DocumentRef(doc_id="8002", title="B")]
        self.store.record_catalog_page("scan-x", 1, refs)
        self.store.record_catalog_page("scan-x", 1, refs)
        self.store.advance_catalog_scan("scan-x", next_page=2, docs_enqueued=2)
        self.store.advance_catalog_scan("scan-x", next_page=2, docs_enqueued=2)

        state = self.store.get_catalog_scan("scan-x")
        self.assertEqual(state.next_page, 2)
        self.assertEqual(state.pages_done, 1)
        self.assertEqual(state.docs_seen, 2)
        self.assertEqual(state.docs_enqueued, 2)
        self.assertTrue(self.store.is_catalog_scan_member("scan-x", "8001"))
        self.assertFalse(self.store.is_catalog_scan_member("scan-x", "9999"))
        self.assertEqual(self.store.pending_catalog_document_count("scan-x"), 2)

        stub = self.store.record_catalog_document_outcome(
            "scan-x",
            "8001",
            OUTCOME_INACCESSIBLE,
            failure_kind="forbidden",
            http_status=403,
            detail="Source request failed with HTTP 403.",
        )
        self.assertEqual(stub["doc_id"], "8001")
        self.assertEqual(self.store.catalog_scan_stats("scan-x").get(OUTCOME_INACCESSIBLE), 1)
        self.assertIsNone(self.store.record_catalog_document_outcome("scan-x", "9999", OUTCOME_DONE))

        self.store.record_catalog_document_outcome("scan-x", "8001", OUTCOME_DONE)
        self.assertEqual(self.store.catalog_scan_stubs("scan-x"), [])


class CatalogCliTest(CatalogScanBase):
    def test_catalog_scan_refuses_to_follow_links(self) -> None:
        self.load_catalog(count=2, page_size=2)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as err:
            with self.assertRaises(SystemExit) as ctx:
                cli.main(
                    [
                        "--out",
                        self.out_dir,
                        "--follow-links-depth",
                        "1",
                        "catalog-scan",
                        "--scan-id",
                        "cli-scan",
                        "--list-url",
                        self.list_url,
                    ]
                )
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("follow", err.getvalue())
        self.assertEqual(self.server.state.catalog_page_hits, [])

    def test_catalog_scan_command_always_includes_paid_documents(self) -> None:
        args = cli.build_parser().parse_args(["catalog-scan", "--scan-id", "cli-scan"])
        self.assertEqual(args.list_url, DEFAULT_ALL_DOCUMENTS_LIST_URL)
        self.assertFalse(args.include_paid)
        args.out = self.out_dir
        with contextlib.redirect_stdout(io.StringIO()):
            crawler = cli.make_crawler(args)
        self.addCleanup(crawler.close)
        self.assertFalse(crawler.only_free)

    def test_status_and_stubs_commands_report_the_scan(self) -> None:
        doc_ids = self.load_catalog(count=2, page_size=2)
        self.server.state.document_status[doc_ids[0]] = 403
        argv = [
            "--out",
            self.out_dir,
            "--delay",
            "0",
            "--timeout",
            "5",
            "catalog-scan",
            "--scan-id",
            "cli-scan",
            "--list-url",
            self.list_url,
            "--poll-interval",
            "0",
        ]
        with contextlib.redirect_stdout(io.StringIO()):
            cli.main(argv)

        with contextlib.redirect_stdout(io.StringIO()) as status_out:
            cli.main(["--out", self.out_dir, "catalog-status", "--scan-id", "cli-scan"])
        rendered = status_out.getvalue()
        self.assertIn("cli-scan", rendered)
        self.assertIn(PHASE_COMPLETED, rendered)
        self.assertIn(f"{OUTCOME_INACCESSIBLE}: 1", rendered)

        with contextlib.redirect_stdout(io.StringIO()) as stubs_out, contextlib.redirect_stderr(io.StringIO()):
            cli.main(["--out", self.out_dir, "catalog-stubs", "--scan-id", "cli-scan"])
        payload = json.loads(stubs_out.getvalue())
        self.assertEqual(payload["scan_id"], "cli-scan")
        self.assertEqual(payload["phase"], PHASE_COMPLETED)
        self.assertEqual(len(payload["documents"]), 1)
        self.assertEqual(payload["documents"][0]["doc_id"], doc_ids[0])

        target = Path(self.out_dir) / "stubs" / "cli-scan.json"
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            cli.main(["--out", self.out_dir, "catalog-stubs", "--scan-id", "cli-scan", "--output", str(target)])
        self.assertEqual(json.loads(target.read_text(encoding="utf-8"))["scan_id"], "cli-scan")

    def test_status_of_unknown_scan_is_reported(self) -> None:
        with contextlib.redirect_stdout(io.StringIO()) as out:
            cli.main(["--out", self.out_dir, "catalog-status", "--scan-id", "missing"])
        self.assertIn("не найден", out.getvalue())


if __name__ == "__main__":
    unittest.main()
