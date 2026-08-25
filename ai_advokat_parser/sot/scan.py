"""Resumable, idempotent orchestration of one PRG.SOT corpus scan.

The corpus is ~16.5M decisions behind a metered subscription, so the scan is
built around three rules: never repeat work that is already stored, never keep
pushing once the source says "slow down", and never lose a decision because a
container died mid-flight.
"""

from __future__ import annotations

import concurrent.futures
import os
import socket
import threading
import time

from ..catalog import sanitize_detail
from ..http_client import SourceAuthError, SourceRateLimitError
from .adapter import MAX_WORKERS, SotSource
from .model import (
    OUTCOME_DONE,
    PHASE_ABORTED,
    PHASE_COMPLETED,
    PHASE_DRAINING,
    PHASE_ENUMERATING,
    PHASE_PAUSED,
    PHASE_RATE_LIMITED,
    SotDecisionRef,
    SotDiscoveryError,
    SotScanState,
    classify_decision_failure,
)
from .store import DECISION_FORMATS

# How long a single pause is allowed to last, and how many pauses one run
# tolerates before it stops and lets the next deploy pick the scan up.
DEFAULT_MAX_PAUSE_SECONDS = 300.0
DEFAULT_PAUSE_BUDGET = 3


class SotScanner:
    def __init__(
        self,
        store,
        source: SotSource,
        formats: tuple[str, ...] = DECISION_FORMATS,
        delay: float = 1.0,
        workers: int = 1,
        max_pause_seconds: float = DEFAULT_MAX_PAUSE_SECONDS,
        pause_budget: int = DEFAULT_PAUSE_BUDGET,
        sleep=time.sleep,
    ) -> None:
        self.store = store
        self.source = source
        self.formats = tuple(formats)
        self.delay = max(0.0, delay)
        # Conservative on purpose: this is a bulk reader on a shared account.
        self.workers = max(1, min(int(workers), MAX_WORKERS))
        self.max_pause_seconds = max(0.0, max_pause_seconds)
        self.pause_budget = max(0, pause_budget)
        self._sleep = sleep
        self._pauses_used = 0
        self._rate_limit_note: str | None = None
        self._processed_this_run = 0
        self._lock = threading.Lock()

    # --- public entry point ----------------------------------------------

    def run(
        self,
        scan_id: str,
        max_pages: int | None = None,
        max_decisions: int | None = None,
        retry_failed: bool = False,
        lease_seconds: int = 1800,
        poll_interval: float = 5.0,
    ) -> SotScanState:
        config = self.source.config
        state = self.store.ensure_scan(
            scan_id,
            config.fingerprint(),
            config.query,
            first_page=config.first_page,
        )
        if state.phase == PHASE_COMPLETED and not retry_failed:
            print(f"[sot] {scan_id}: скан уже завершен {state.completed_at}, ничего не делаю")
            return state

        self._pauses_used = 0
        self._rate_limit_note = None
        self._processed_this_run = 0

        reclaimed = self.store.requeue_stale_decisions(scan_id, lease_seconds)
        if reclaimed:
            print(f"[sot] {scan_id}: возвращено в очередь после перезапуска: {reclaimed}")
        if retry_failed:
            retried = self.store.retry_scan_outcomes(scan_id)
            print(f"[sot] {scan_id}: повторно поставлено в очередь неуспешных: {retried}")

        print(
            f"[sot] {scan_id}: старт, фаза {state.phase}, страница {state.next_page}"
            + (f"/{state.total_pages}" if state.total_pages else "")
            + f", форматы {','.join(self.formats)}, workers {self.workers}"
        )

        self.store.set_scan_phase(scan_id, PHASE_ENUMERATING)
        enumeration_error: str | None = None
        enumeration_complete = False
        try:
            enumeration_complete = self._enumerate(
                scan_id,
                state,
                max_pages,
                max_decisions,
                lease_seconds=lease_seconds,
                poll_interval=poll_interval,
            )
        except SourceAuthError as exc:
            self._abort(scan_id, exc)
            raise
        except SotDiscoveryError as exc:
            self.store.set_scan_phase(scan_id, PHASE_ABORTED, error=sanitize_detail(str(exc)))
            raise
        except SourceRateLimitError as exc:
            enumeration_error = self._note_rate_limit(exc)
            print(f"[sot] {scan_id}: перечисление остановлено лимитом: {enumeration_error}")
        except Exception as exc:
            # A broken search page must not silently skip its decisions: stop
            # enumerating, still fetch what is already queued, resume later.
            enumeration_error = sanitize_detail(f"{type(exc).__name__}: {exc}")
            print(f"[sot] {scan_id}: перечисление остановлено: {enumeration_error}")

        self.store.set_scan_phase(
            scan_id, PHASE_DRAINING, error=enumeration_error, rate_limit_note=self._rate_limit_note
        )
        if self._rate_limit_note is None:
            try:
                self._processed_this_run += self._drain(
                    scan_id, lease_seconds=lease_seconds, poll_interval=poll_interval
                )
            except SourceAuthError as exc:
                self._abort(scan_id, exc)
                raise
        processed = self._processed_this_run

        resolved = self.store.resolve_scan_outcomes(scan_id)
        pending = self.store.pending_decision_count(scan_id)
        if enumeration_complete and pending == 0 and enumeration_error is None:
            self.store.set_scan_phase(scan_id, PHASE_COMPLETED, rate_limit_note=self._rate_limit_note)
        elif self._rate_limit_note is not None:
            self.store.set_scan_phase(
                scan_id,
                PHASE_RATE_LIMITED,
                error=enumeration_error,
                rate_limit_note=self._rate_limit_note,
            )
        else:
            reason = enumeration_error or (
                "enumeration stopped before the end of the corpus"
                if not enumeration_complete
                else f"{pending} decisions still pending"
            )
            self.store.set_scan_phase(scan_id, PHASE_PAUSED, error=reason)

        state = self.store.get_scan(scan_id)
        stats = self.store.scan_stats(scan_id)
        print(
            f"[sot] {scan_id}: фаза {state.phase}, обработано за запуск {processed}, "
            f"страниц {state.pages_done}"
            + (f"/{state.total_pages}" if state.total_pages else "")
            + f", решений {state.decisions_seen}, итоги {stats}"
            + (f", закрыто по exported {resolved}" if resolved else "")
        )
        return state

    # --- enumeration ------------------------------------------------------

    def _enumerate(
        self,
        scan_id: str,
        state: SotScanState,
        max_pages: int | None,
        max_decisions: int | None,
        lease_seconds: int,
        poll_interval: float,
    ) -> bool:
        """Walk the search pages; True when the corpus was read to the end."""
        config = self.source.config
        page = max(config.first_page, state.next_page)
        cursor = state.next_cursor
        total_pages = state.total_pages
        pages_fetched = 0
        seen_this_run = 0
        uses_cursor = bool(config.next_cursor_path)

        while True:
            if max_pages is not None and pages_fetched >= max_pages:
                print(f"[sot] {scan_id}: достигнут --max-pages, остановка на странице {page}")
                return False
            if max_decisions is not None and seen_this_run >= max_decisions:
                print(f"[sot] {scan_id}: достигнут --max-decisions, остановка на странице {page}")
                return False
            if total_pages is not None and page > total_pages:
                return True

            result = self._search_page(page, cursor)
            if result is None:
                return False
            pages_fetched += 1

            if not result.refs:
                # An empty page is the natural end of both pagination styles; a
                # malformed payload raised SotDiscoveryError long before this.
                self.store.advance_scan(scan_id, next_page=page, next_cursor=None)
                print(f"[sot] {scan_id}: страница {page} пуста, перечисление завершено")
                return True

            if state.page_size is None and pages_fetched == 1:
                total_pages = None
                if result.total is not None:
                    total_pages = (int(result.total) + len(result.refs) - 1) // len(result.refs)
                self.store.set_scan_discovery(
                    scan_id,
                    total_decisions=result.total,
                    page_size=len(result.refs),
                    total_pages=total_pages,
                )
                print(
                    f"[sot] {scan_id}: всего решений {result.total if result.total is not None else '-'}, "
                    f"по {len(result.refs)} на странице, страниц {total_pages if total_pages else '-'}"
                )

            refs = result.refs
            truncated = False
            if max_decisions is not None and seen_this_run + len(refs) > max_decisions:
                refs = refs[: max(0, max_decisions - seen_this_run)]
                truncated = True

            added = self.store.record_search_page(scan_id, page, refs)
            seen_this_run += len(refs)
            print(f"[sot] {scan_id}: страница {page}: решений {len(refs)}, в очередь {added}")

            if truncated:
                # The page is half consumed, so the cursor stays put and a later
                # run reads it again from the start.
                self.store.advance_scan(scan_id, next_page=page, next_cursor=cursor)
                if not self._drain_page(scan_id, lease_seconds, poll_interval):
                    return False
                print(f"[sot] {scan_id}: достигнут --max-decisions внутри страницы {page}")
                return False

            cursor = result.next_cursor
            self.store.advance_scan(scan_id, next_page=page + 1, next_cursor=cursor, decisions_enqueued=added)
            # Persist searchable text continuously instead of first enumerating
            # millions of cards and only then starting downloads. The separate
            # knowledge indexer can seed this page as soon as each txt output
            # commits, so the corpus becomes searchable during the scan.
            if not self._drain_page(scan_id, lease_seconds, poll_interval):
                return False
            if uses_cursor and cursor is None:
                print(f"[sot] {scan_id}: источник больше не отдает курсор, перечисление завершено")
                return True
            page += 1
            if self.delay:
                self._sleep(self.delay)

    def _drain_page(self, scan_id: str, lease_seconds: int, poll_interval: float) -> bool:
        self._processed_this_run += self._drain(
            scan_id,
            lease_seconds=lease_seconds,
            poll_interval=poll_interval,
        )
        return self._rate_limit_note is None

    def _search_page(self, page: int, cursor: str | None):
        """One search request, retried once after an honoured rate-limit pause."""
        while True:
            try:
                return self.source.fetch_search_page(page, cursor=cursor)
            except SourceRateLimitError as exc:
                if not self._pause_for(exc):
                    return None

    # --- draining ---------------------------------------------------------

    def _drain(self, scan_id: str, lease_seconds: int, poll_interval: float) -> int:
        worker_prefix = f"sot:{scan_id}:{socket.gethostname()}:{os.getpid()}"
        shared = {"claimed": 0, "processed": 0, "active": 0, "stop": False}
        shared_lock = threading.Lock()

        def worker_loop(worker_number: int) -> None:
            worker_id = f"{worker_prefix}:{worker_number}"
            while True:
                with shared_lock:
                    if shared["stop"]:
                        return
                ref = self.store.claim_decision(scan_id, worker_id)
                if ref is None:
                    if self.store.requeue_stale_decisions(scan_id, lease_seconds):
                        continue
                    with shared_lock:
                        if int(shared["active"]) == 0:
                            shared["stop"] = True
                            return
                    self._sleep(max(0.1, poll_interval))
                    continue

                with shared_lock:
                    shared["claimed"] += 1
                    shared["active"] += 1
                    index = int(shared["claimed"])
                try:
                    self._process(scan_id, ref, index=index)
                except SourceAuthError:
                    self.store.release_decision(ref.decision_key)
                    with shared_lock:
                        shared["stop"] = True
                    raise
                except SourceRateLimitError as exc:
                    # The quota is a property of the account, not of this
                    # decision: give it back and stop the whole drain.
                    self.store.release_decision(ref.decision_key)
                    self._note_rate_limit(exc)
                    with shared_lock:
                        shared["stop"] = True
                    print(f"[sot] {scan_id}: пауза по лимиту источника: {self._rate_limit_note}")
                    return
                finally:
                    with shared_lock:
                        shared["processed"] += 1
                        shared["active"] -= 1

        if self.workers == 1:
            worker_loop(1)
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = [executor.submit(worker_loop, number) for number in range(1, self.workers + 1)]
                for future in concurrent.futures.as_completed(futures):
                    future.result()
        return int(shared["processed"])

    def _process(self, scan_id: str, ref: SotDecisionRef, index: int) -> None:
        """Fetch and store one claimed decision, then record its outcome."""
        decision_key = ref.decision_key
        if self.store.is_decision_complete(decision_key, self.formats):
            self.store.record_decision_outcome(scan_id, decision_key, OUTCOME_DONE)
            print(f"[sot] {index} {decision_key}: уже выгружено, пропускаю")
            return

        try:
            payload = self.source.fetch_decision(ref)
            self.store.save_decision(payload, self.formats)
            self.store.record_decision_outcome(scan_id, decision_key, OUTCOME_DONE)
            print(f"[sot] {index} {decision_key}: готово ({len(payload.text)} символов)")
        except (SourceAuthError, SourceRateLimitError):
            raise
        except Exception as exc:
            outcome, failure_kind, http_status = classify_decision_failure(exc)
            self.store.mark_decision_failed(decision_key, sanitize_detail(str(exc)))
            self.store.record_decision_outcome(
                scan_id,
                decision_key,
                outcome,
                failure_kind=failure_kind,
                http_status=http_status,
                detail=str(exc),
            )
            print(f"[sot] {index} {decision_key}: {outcome}/{failure_kind}")
        finally:
            if self.delay:
                self._sleep(self.delay)

    # --- rate limiting ----------------------------------------------------

    def _pause_for(self, exc: SourceRateLimitError) -> bool:
        """Honour the wait the source asked for; False when the run must stop.

        The limit is never worked around: the only options are to wait exactly
        as long as the source said, or to stop and let a later run continue.
        """
        wait = exc.rate_limit.delay()
        with self._lock:
            budget_left = self._pauses_used < self.pause_budget
            if budget_left:
                self._pauses_used += 1
        if not budget_left or wait > self.max_pause_seconds:
            self._note_rate_limit(exc)
            return False
        print(f"[sot] лимит источника, жду {wait:.0f}s ({exc.rate_limit.describe()})")
        self._sleep(wait)
        return True

    def _note_rate_limit(self, exc: SourceRateLimitError) -> str:
        note = sanitize_detail(f"rate limit: {exc.rate_limit.describe()}")
        with self._lock:
            self._rate_limit_note = note
        return note

    def _abort(self, scan_id: str, exc: BaseException) -> None:
        error = sanitize_detail(f"auth: {exc}")
        self.store.set_scan_phase(scan_id, PHASE_ABORTED, error=error)
        print(f"[sot] {scan_id}: скан прерван из-за авторизации PRG.SOT")
