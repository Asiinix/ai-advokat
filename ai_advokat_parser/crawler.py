from __future__ import annotations

import concurrent.futures
import gc
import json
import os
import shutil
import socket
import threading
import time
from pathlib import Path
from typing import Iterable

from .catalog import (
    OUTCOME_DONE,
    PHASE_ABORTED,
    PHASE_COMPLETED,
    PHASE_DRAINING,
    PHASE_ENUMERATING,
    PHASE_PAUSED,
    CatalogDiscoveryError,
    CatalogScanState,
    classify_document_failure,
    sanitize_detail,
)
from .config import DEFAULT_ALL_DOCUMENTS_LIST_URL, DEFAULT_LIST_URL
from .document import DocumentDownloader, DocumentNotFreeError, DocumentUnavailableError
from .exporters import export_document
from .http_client import SourceAuthError, SourceClient
from .listing import DocumentRef, fetch_listing_page, load_document_refs_from_file
from .postgres_store import PostgresCrawlStore
from .store import CrawlStore


def outputs_exist(out_dir: str | Path, doc_id: str, formats: tuple[str, ...]) -> bool:
    doc_dir = Path(out_dir) / "documents" / doc_id
    mapping = {
        "html": doc_dir / "document.html",
        "txt": doc_dir / "document.txt",
        "json": doc_dir / "document.json",
        "pdf": doc_dir / "document.pdf",
    }
    return all(mapping[fmt].exists() for fmt in formats)


def read_exported_links(out_dir: str | Path, doc_id: str) -> list[str]:
    meta_path = Path(out_dir) / "documents" / doc_id / "meta.json"
    if not meta_path.exists():
        return []
    try:
        data = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    links = data.get("linked_doc_ids") or []
    return [str(item) for item in links if str(item).isdigit()]


def cleanup_document_exports(out_dir: str | Path, doc_id: str) -> None:
    doc_dir = Path(out_dir) / "documents" / doc_id
    if doc_dir.exists():
        shutil.rmtree(doc_dir)


def positive_env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, str(default))))
    except ValueError:
        return default


class Crawler:
    def __init__(
        self,
        out_dir: str | Path = "data",
        formats: tuple[str, ...] = ("html", "txt"),
        product: str = "lawyer",
        only_free: bool = True,
        delay: float = 0.8,
        timeout: float = 30.0,
        retries: int = 3,
        workers: int = 1,
        force: bool = False,
        follow_links_depth: int = 0,
        max_linked_docs: int | None = None,
    ) -> None:
        self.out_dir = Path(out_dir)
        self.formats = formats
        self.product = product
        self.only_free = only_free
        self.delay = delay
        self.workers = max(1, workers)
        self.force = force
        self.follow_links_depth = max(0, follow_links_depth)
        self.max_linked_docs = max_linked_docs
        self._docs_since_gc = 0
        self._gc_interval = positive_env_int("AI_ADVOCAT_GC_INTERVAL", 25)
        database_url = os.environ.get("AI_ADVOCAT_DATABASE_URL") or os.environ.get("DATABASE_URL")
        if database_url and os.environ.get("AI_ADVOCAT_DISABLE_POSTGRES") not in {"1", "true", "yes"}:
            self.store = PostgresCrawlStore(database_url)
        else:
            self.store = CrawlStore(self.out_dir)
        self.client = SourceClient(timeout=timeout, retries=retries)
        if self.client.uses_authentication:
            print("[auth] PRG: учетные данные настроены через переменные окружения")

    def close(self) -> None:
        self.store.close()

    def crawl_range(
        self,
        from_page: int,
        to_page: int,
        list_url: str = DEFAULT_LIST_URL,
        max_docs: int | None = None,
        enqueue_only: bool = False,
    ) -> None:
        if from_page < 1 or to_page < from_page:
            raise ValueError("Page range must be valid: from_page >= 1 and to_page >= from_page.")

        if not self.force and max_docs is None:
            if enqueue_only:
                resume_page = self.store.recommended_enqueue_start(from_page, to_page)
                if resume_page > to_page:
                    print(f"[resume] все страницы {from_page}-{to_page} уже в очереди или обработаны")
                    return
                if resume_page > from_page:
                    print(f"[resume] продолжаю постановку в очередь со страницы {resume_page}/{to_page}")
                    from_page = resume_page
            else:
                resume_page = self.store.recommended_range_start(from_page, to_page)
                if resume_page > from_page:
                    print(f"[resume] продолжаю диапазон со страницы {resume_page}/{to_page}")
                    from_page = resume_page

        seen: set[str] = set()
        queued_count = 0
        for page in range(from_page, to_page + 1):
            if enqueue_only and not self.force and self.store.is_listing_documents_queued(page):
                print(f"[list] страница {page}/{to_page}: документы уже в очереди, пропускаю")
                continue
            if not self.force and self.store.is_listing_documents_done(page):
                print(f"[list] страница {page}/{to_page}: документы уже обработаны, пропускаю")
                continue

            page_refs: list[DocumentRef] = []
            fetched_page = False
            page_limited = False
            if not self.force and self.store.get_listing_page_status(page) == "done":
                page_refs = self.store.get_listing_documents(page)
                if page_refs:
                    print(f"[list] страница {page}/{to_page}: уже есть, документов {len(page_refs)}")
                else:
                    print(f"[list] страница {page}/{to_page}: done без сохраненного списка, перечитываю")

            try:
                if not page_refs:
                    print(f"[list] страница {page}/{to_page}")
                    listing = fetch_listing_page(self.client, page=page, list_url=list_url)
                    page_refs = listing.documents
                    fetched_page = True
                    self.store.save_listing_documents(page, page_refs)
                    self.store.mark_listing_page(
                        page,
                        "done",
                        doc_count=len(page_refs),
                        total=listing.total,
                    )
                    print(
                        f"       найдено {len(page_refs)} документов"
                        + (f", всего {listing.total}" if listing.total else "")
                    )
                new_refs: list[DocumentRef] = []
                for ref in page_refs:
                    if ref.doc_id not in seen:
                        seen.add(ref.doc_id)
                        new_refs.append(ref)
                        queued_count += 1
                        if max_docs and queued_count >= max_docs:
                            page_limited = True
                            break
                if new_refs and enqueue_only:
                    added = self.enqueue_refs(new_refs, depth=0)
                    print(f"[queue] страница {page}: поставлено в очередь {added}/{len(new_refs)}")
                elif new_refs:
                    self.store.mark_listing_documents_status(page, "processing")
                    self.crawl_refs(new_refs)
                else:
                    print(f"[docs] страница {page}: новых документов нет")
                if enqueue_only:
                    self.store.mark_listing_documents_status(page, "partial" if page_limited else "queued")
                else:
                    self.store.mark_listing_documents_status(page, "partial" if page_limited else "done")
                if max_docs and queued_count >= max_docs:
                    break
            except SourceAuthError:
                raise
            except Exception as exc:
                self.store.mark_listing_documents_status(page, "failed", error=str(exc))
                self.store.mark_listing_page(page, "failed", error=str(exc))
                print(f"[list] ошибка на странице {page}: {exc}")
            if fetched_page:
                time.sleep(self.delay)

    def crawl_file(self, path: str | Path) -> None:
        refs = load_document_refs_from_file(path)
        print(f"[file] загружено doc_id: {len(refs)}")
        self.crawl_refs(refs)

    def crawl_doc_ids(self, doc_ids: Iterable[str]) -> None:
        self.crawl_refs([DocumentRef(doc_id=str(doc_id)) for doc_id in doc_ids])

    def retry_failed(self) -> None:
        doc_ids = self.store.failed_documents()
        print(f"[retry] документов с ошибкой: {len(doc_ids)}")
        self.crawl_doc_ids(doc_ids)

    def enrich_failed_titles(
        self,
        limit: int | None = None,
        lease_seconds: int = 86400,
    ) -> int:
        if limit is not None and limit < 1:
            limit = None

        worker_prefix = f"failed-title:{socket.gethostname()}:{os.getpid()}"
        state = {
            "claimed": 0,
            "enriched": 0,
            "failed": 0,
            "stop": False,
        }
        state_lock = threading.Lock()

        def worker_loop(worker_number: int) -> None:
            worker_id = f"{worker_prefix}:{worker_number}"
            downloader = DocumentDownloader(self.client, product=self.product, only_free=False)
            while True:
                with state_lock:
                    if state["stop"] or (limit is not None and state["claimed"] >= limit):
                        return
                    ref = self.store.claim_failed_document_without_title(worker_id, lease_seconds)
                    if ref is None:
                        return
                    state["claimed"] += 1
                    index = int(state["claimed"])

                print(f"[failed-title] {index}/{limit or 'failed'} {ref.doc_id}: читаю metadata")
                try:
                    metadata = downloader.fetch_document_metadata(ref.doc_id)
                    title = metadata.title.strip()
                    if not title:
                        raise ValueError("The source did not return a document title.")
                    self.store.update_failed_document_title(
                        ref.doc_id,
                        title=title,
                        is_free=metadata.is_free,
                        pages=metadata.pages,
                    )
                    with state_lock:
                        state["enriched"] += 1
                    print(f"[failed-title] {index}/{limit or 'failed'} {ref.doc_id}: title готов")
                except SourceAuthError:
                    self.store.defer_failed_title_enrichment(ref.doc_id)
                    with state_lock:
                        state["stop"] = True
                    raise
                except Exception as exc:
                    self.store.defer_failed_title_enrichment(ref.doc_id)
                    with state_lock:
                        state["failed"] += 1
                    print(f"[failed-title] {index}/{limit or 'failed'} {ref.doc_id}: ошибка: {exc}")
                finally:
                    time.sleep(self.delay)

        print(
            f"[failed-title] старт: workers={self.workers}, "
            f"limit={limit if limit is not None else 'нет'}, lease={lease_seconds}s"
        )
        if self.workers == 1:
            worker_loop(1)
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = [executor.submit(worker_loop, number) for number in range(1, self.workers + 1)]
                for future in concurrent.futures.as_completed(futures):
                    future.result()

        print(
            f"[failed-title] остановка: проверено {state['claimed']}, "
            f"обогащено {state['enriched']}, ошибок {state['failed']}"
        )
        return int(state["enriched"])

    def enqueue_refs(self, refs: Iterable[DocumentRef], depth: int = 0) -> int:
        refs = list(refs)
        if not refs:
            return 0
        return self.store.enqueue_document_refs(
            refs,
            depth=depth,
            force=self.force,
            formats=self.formats,
        )

    def process_queue(
        self,
        limit: int | None = None,
        idle_seconds: float = 0,
        lease_seconds: int = 1800,
        poll_interval: float = 5.0,
        producer_done: threading.Event | None = None,
    ) -> int:
        if limit is not None and limit < 1:
            limit = None
        stale = self.store.requeue_stale_documents(lease_seconds)
        if stale:
            print(f"[queue] возвращено зависших processing -> queued: {stale}")

        worker_prefix = f"{socket.gethostname()}:{os.getpid()}"
        state = {
            "claimed": 0,
            "processed": 0,
            "active": 0,
            "last_work": time.monotonic(),
            "stop": False,
        }
        state_lock = threading.Lock()

        def should_stop() -> bool:
            with state_lock:
                if state["stop"]:
                    return True
                return bool(limit is not None and state["claimed"] >= limit)

        def worker_loop(worker_number: int) -> None:
            worker_id = f"{worker_prefix}:{worker_number}"
            while not should_stop():
                item = self.store.claim_queued_document(worker_id)
                if item is None:
                    stale_count = self.store.requeue_stale_documents(lease_seconds)
                    if stale_count:
                        print(f"[queue] {worker_number}: возвращено зависших задач: {stale_count}")
                        continue
                    with state_lock:
                        idle_for = time.monotonic() - float(state["last_work"])
                        active = int(state["active"])
                        producer_finished = producer_done is None or producer_done.is_set()
                        if active == 0 and producer_finished and (idle_seconds <= 0 or idle_for >= idle_seconds):
                            state["stop"] = True
                            return
                    time.sleep(max(0.1, poll_interval))
                    continue

                ref, depth = item
                with state_lock:
                    if limit is not None and state["claimed"] >= limit:
                        self.store.enqueue_document_refs([ref], depth=depth, force=True, formats=self.formats)
                        state["stop"] = True
                        return
                    state["claimed"] += 1
                    state["active"] += 1
                    index = int(state["claimed"])
                    state["last_work"] = time.monotonic()

                try:
                    linked_doc_ids = self._process_ref(
                        ref,
                        index=index,
                        total=limit or "queue",
                        depth=depth,
                        claimed=True,
                    )
                    if self.follow_links_depth and depth < self.follow_links_depth and linked_doc_ids:
                        refs_to_enqueue = [DocumentRef(doc_id=doc_id) for doc_id in linked_doc_ids]
                        added = self.enqueue_refs(refs_to_enqueue, depth=depth + 1)
                        if added:
                            print(f"[links] {ref.doc_id}: поставлено в очередь связанных документов: {added}")
                except SourceAuthError:
                    with state_lock:
                        state["stop"] = True
                    raise
                finally:
                    with state_lock:
                        state["processed"] += 1
                        state["active"] -= 1
                        state["last_work"] = time.monotonic()

        print(
            f"[queue] старт worker-режима: workers={self.workers}, "
            f"limit={limit if limit is not None else 'нет'}, depth={self.follow_links_depth}"
        )
        if self.workers == 1:
            worker_loop(1)
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = [executor.submit(worker_loop, number) for number in range(1, self.workers + 1)]
                for future in concurrent.futures.as_completed(futures):
                    future.result()

        stats = self.store.queue_stats()
        print(
            f"[queue] остановка: обработано {state['processed']}, "
            f"queued={stats.get('queued', 0)}, processing={stats.get('processing', 0)}, "
            f"exported={stats.get('exported', 0)}, failed={stats.get('failed', 0)}"
        )
        return int(state["processed"])

    def crawl_range_pipeline(
        self,
        from_page: int,
        to_page: int,
        list_url: str = DEFAULT_LIST_URL,
        max_docs: int | None = None,
        idle_seconds: float = 60,
        lease_seconds: int = 1800,
        poll_interval: float = 5.0,
    ) -> int:
        producer_done = threading.Event()
        producer_error: list[BaseException] = []

        def producer() -> None:
            try:
                self.crawl_range(
                    from_page=from_page,
                    to_page=to_page,
                    list_url=list_url,
                    max_docs=max_docs,
                    enqueue_only=True,
                )
            except BaseException as exc:
                producer_error.append(exc)
                raise
            finally:
                producer_done.set()

        print(
            f"[pipeline] старт: pages={from_page}-{to_page}, "
            f"workers={self.workers}, depth={self.follow_links_depth}"
        )
        producer_thread = threading.Thread(target=producer, name="ai-advokat-listing-producer", daemon=True)
        producer_thread.start()
        processed = self.process_queue(
            idle_seconds=idle_seconds,
            lease_seconds=lease_seconds,
            poll_interval=poll_interval,
            producer_done=producer_done,
        )
        producer_thread.join()
        if producer_error:
            raise producer_error[0]
        print(f"[pipeline] готово: обработано документов {processed}")
        return processed

    def crawl_refs(self, refs: list[DocumentRef]) -> None:
        if not refs:
            print("[docs] нечего загружать")
            return
        if self.follow_links_depth > 0:
            self._crawl_refs_with_links(refs)
            return
        if self.workers == 1:
            for index, ref in enumerate(refs, start=1):
                self._process_ref(ref, index=index, total=len(refs))
            return

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(self._process_ref, ref, index, len(refs)): ref
                for index, ref in enumerate(refs, start=1)
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except SourceAuthError:
                    for pending in futures:
                        pending.cancel()
                    raise

    def _crawl_refs_with_links(self, refs: list[DocumentRef]) -> None:
        queue: list[tuple[DocumentRef, int]] = [(ref, 0) for ref in refs]
        queued_doc_ids = {ref.doc_id for ref in refs}
        seen: set[str] = set()
        position = 0
        linked_added_total = 0

        if self.workers > 1:
            print("[links] follow-links-depth включен, обхожу граф ссылок последовательно")

        while position < len(queue):
            ref, depth = queue[position]
            position += 1
            if ref.doc_id in seen:
                continue
            seen.add(ref.doc_id)

            linked_doc_ids = self._process_ref(ref, index=position, total=len(queue), depth=depth)
            if depth >= self.follow_links_depth:
                continue

            added = 0
            for linked_doc_id in linked_doc_ids:
                if self.max_linked_docs is not None and linked_added_total >= self.max_linked_docs:
                    break
                if linked_doc_id in seen or linked_doc_id in queued_doc_ids:
                    continue
                queue.append((DocumentRef(doc_id=linked_doc_id), depth + 1))
                queued_doc_ids.add(linked_doc_id)
                added += 1
                linked_added_total += 1
            if added:
                print(f"[links] {ref.doc_id}: добавлено связанных документов: {added}")

    def _process_ref(
        self,
        ref: DocumentRef,
        index: int,
        total: int | str,
        depth: int = 0,
        claimed: bool = False,
    ) -> list[str]:
        doc_id = ref.doc_id
        status = None if self.force else self.store.get_document_status(doc_id)
        if not self.force and self.store.has_document_outputs(doc_id, self.formats):
            if status != "exported":
                self.store.upsert_document(
                    doc_id,
                    "exported",
                    title=ref.title,
                    source_url=ref.source_url,
                    formats=self.formats,
                )
            print(f"[docs] {index}/{total} {doc_id}: уже готово, пропускаю")
            return self.store.get_document_links(doc_id)

        if not claimed:
            if not self.force and status == "failed" and self.store.is_terminal_document_failure(doc_id):
                print(f"[docs] {index}/{total} {doc_id}: платный/недоступный, пропускаю")
                return []

            self.store.upsert_document(
                doc_id,
                "queued",
                title=ref.title,
                source_url=ref.source_url,
            )
            self.store.upsert_document(
                doc_id,
                "processing",
                title=ref.title,
                source_url=ref.source_url,
            )
        depth_label = f", depth={depth}" if self.follow_links_depth else ""
        print(f"[docs] {index}/{total} {doc_id}: загрузка{depth_label}")
        try:
            downloader = DocumentDownloader(self.client, product=self.product, only_free=self.only_free)
            document = downloader.fetch_document(doc_id)
            self.store.upsert_document(
                doc_id,
                "processing",
                title=document.title,
                source_url=ref.source_url,
                is_free=document.is_free,
                pages=len(document.raw.get("pages") or []),
            )
            paths = export_document(document, self.out_dir, self.formats)
            self.store.save_document_outputs(document, paths)
            linked_doc_ids = list(document.linked_doc_ids)
            link_count = len(linked_doc_ids)
            self.store.upsert_document(
                doc_id,
                "exported",
                title=document.title,
                source_url=ref.source_url,
                is_free=document.is_free,
                pages=len(document.raw.get("pages") or []),
                formats=self.formats,
            )
            if getattr(self.store, "stores_document_outputs", False):
                cleanup_document_exports(self.out_dir, doc_id)
            outputs = ", ".join(f"{key}:{path.name}" for key, path in paths.items() if key != "meta")
            print(
                f"[docs] {index}/{total} {doc_id}: готово "
                f"({outputs}, links:{link_count})"
            )
            return linked_doc_ids
        except DocumentNotFreeError as exc:
            metadata = exc.metadata
            self.store.upsert_document(
                doc_id,
                "failed",
                title=metadata.title or ref.title,
                source_url=ref.source_url,
                is_free=metadata.is_free,
                pages=metadata.pages,
                error=str(exc),
            )
            print(f"[docs] {index}/{total} {doc_id}: платный/недоступный, title сохранен")
            return []
        except SourceAuthError:
            if claimed:
                self.store.enqueue_document_refs(
                    [ref],
                    depth=depth,
                    force=True,
                    formats=self.formats,
                )
            else:
                self.store.upsert_document(
                    doc_id,
                    "failed",
                    title=ref.title,
                    source_url=ref.source_url,
                    error="PRG authentication interrupted the batch; retry after fixing credentials.",
                )
            raise
        except Exception as exc:
            self.store.upsert_document(doc_id, "failed", title=ref.title, source_url=ref.source_url, error=str(exc))
            print(f"[docs] {index}/{total} {doc_id}: ошибка: {exc}")
            return []
        finally:
            if getattr(self.store, "stores_document_outputs", False):
                try:
                    cleanup_document_exports(self.out_dir, doc_id)
                except OSError as cleanup_error:
                    print(f"[docs] {index}/{total} {doc_id}: не удалось очистить временные файлы: {cleanup_error}")
            self._maybe_collect_garbage()
            time.sleep(self.delay)

    def _maybe_collect_garbage(self) -> None:
        self._docs_since_gc += 1
        if self._docs_since_gc >= self._gc_interval:
            self._docs_since_gc = 0
            gc.collect()

    # --- full catalog scan ------------------------------------------------

    def run_catalog_scan(
        self,
        scan_id: str,
        list_url: str = DEFAULT_ALL_DOCUMENTS_LIST_URL,
        max_pages: int | None = None,
        max_docs: int | None = None,
        lease_seconds: int = 1800,
        poll_interval: float = 5.0,
    ) -> CatalogScanState:
        """Enumerate the whole catalog once, then export everything it listed.

        The scan is identified by ``scan_id`` and is resumable: a restarted
        container repeats the same command, picks up the persisted page cursor
        and never downloads a document that is already exported in full. Once
        the scan is completed the same command becomes a no-op.
        """
        state = self.store.ensure_catalog_scan(scan_id, list_url, self.product, self.formats)
        if state.phase == PHASE_COMPLETED:
            print(f"[catalog] {scan_id}: скан уже завершен {state.completed_at}, ничего не делаю")
            return state

        reclaimed = self.store.reclaim_catalog_scan_documents(scan_id)
        if reclaimed:
            print(f"[catalog] {scan_id}: возвращено в очередь после перезапуска: {reclaimed}")

        print(
            f"[catalog] {scan_id}: старт, фаза {state.phase}, страница {state.next_page}"
            + (f"/{state.total_pages}" if state.total_pages else "")
            + f", форматы {','.join(self.formats)}"
        )
        self.store.set_catalog_scan_phase(scan_id, PHASE_ENUMERATING)
        enumeration_error: str | None = None
        try:
            self._enumerate_catalog(scan_id, state, list_url, max_pages, max_docs)
        except SourceAuthError as exc:
            self._abort_catalog_scan(scan_id, exc)
            raise
        except CatalogDiscoveryError as exc:
            self.store.set_catalog_scan_phase(scan_id, PHASE_ABORTED, error=sanitize_detail(str(exc)))
            raise
        except Exception as exc:
            # A broken listing page must not silently skip its documents: stop
            # enumerating, still export what is already queued, and resume later.
            enumeration_error = sanitize_detail(f"{type(exc).__name__}: {exc}")
            print(f"[catalog] {scan_id}: обход списка остановлен: {enumeration_error}")

        self.store.set_catalog_scan_phase(scan_id, PHASE_DRAINING, error=enumeration_error)
        try:
            processed = self._drain_catalog_queue(scan_id, lease_seconds=lease_seconds, poll_interval=poll_interval)
        except SourceAuthError as exc:
            self._abort_catalog_scan(scan_id, exc)
            raise

        resolved = self.store.resolve_catalog_scan_outcomes(scan_id)
        pending = self.store.pending_catalog_document_count(scan_id)
        state = self.store.get_catalog_scan(scan_id)
        enumeration_done = state.total_pages is not None and state.next_page > state.total_pages
        if enumeration_done and pending == 0 and enumeration_error is None:
            self.store.set_catalog_scan_phase(scan_id, PHASE_COMPLETED)
        else:
            reason = enumeration_error or (
                f"enumeration stopped at page {state.next_page}"
                if not enumeration_done
                else f"{pending} documents still pending"
            )
            self.store.set_catalog_scan_phase(scan_id, PHASE_PAUSED, error=reason)

        state = self.store.get_catalog_scan(scan_id)
        stats = self.store.catalog_scan_stats(scan_id)
        print(
            f"[catalog] {scan_id}: фаза {state.phase}, обработано за запуск {processed}, "
            f"страниц {state.pages_done}/{state.total_pages}, документов {state.docs_seen}, "
            f"итоги {stats}" + (f", закрыто по exported {resolved}" if resolved else "")
        )
        return state

    def _abort_catalog_scan(self, scan_id: str, exc: BaseException) -> None:
        error = sanitize_detail(f"auth: {exc}")
        self.store.set_catalog_scan_phase(scan_id, PHASE_ABORTED, error=error)
        print(f"[catalog] {scan_id}: скан прерван из-за авторизации PRG")

    def _enumerate_catalog(
        self,
        scan_id: str,
        state: CatalogScanState,
        list_url: str,
        max_pages: int | None,
        max_docs: int | None,
    ) -> None:
        page = max(1, state.next_page)
        total_pages = state.total_pages
        pages_fetched = 0
        docs_this_run = 0

        while total_pages is None or page <= total_pages:
            if max_pages is not None and pages_fetched >= max_pages:
                print(f"[catalog] {scan_id}: достигнут --max-pages, остановка на странице {page}")
                return
            if max_docs is not None and docs_this_run >= max_docs:
                print(f"[catalog] {scan_id}: достигнут --max-docs, остановка на странице {page}")
                return

            listing = fetch_listing_page(self.client, page=page, list_url=list_url)
            pages_fetched += 1
            refs = listing.documents

            if total_pages is None:
                if listing.total is None or not refs:
                    raise CatalogDiscoveryError(
                        f"Catalog listing page {page} returned no document total or no documents; "
                        "refusing to guess the catalog size."
                    )
                page_size = len(refs)
                total_pages = (int(listing.total) + page_size - 1) // page_size
                self.store.set_catalog_scan_discovery(
                    scan_id,
                    total_documents=int(listing.total),
                    page_size=page_size,
                    total_pages=total_pages,
                )
                print(
                    f"[catalog] {scan_id}: всего документов {listing.total}, "
                    f"по {page_size} на странице, страниц {total_pages}"
                )

            truncated = False
            if max_docs is not None and docs_this_run + len(refs) > max_docs:
                refs = refs[: max(0, max_docs - docs_this_run)]
                truncated = True

            self.store.record_catalog_page(scan_id, page, refs)
            added = self.store.enqueue_document_refs(
                refs,
                depth=0,
                force=self.force,
                formats=self.formats,
                retry_failed=True,
            )
            docs_this_run += len(refs)
            print(
                f"[catalog] {scan_id}: страница {page}/{total_pages}: "
                f"документов {len(refs)}, в очередь {added}"
            )
            if truncated:
                # The page is only half consumed, so the cursor stays put and a
                # later run reads it again from the start. Re-stating the current
                # page still refreshes the counters.
                self.store.advance_catalog_scan(scan_id, next_page=page)
                print(f"[catalog] {scan_id}: достигнут --max-docs внутри страницы {page}")
                return
            self.store.advance_catalog_scan(scan_id, next_page=page + 1, docs_enqueued=added)
            page += 1
            time.sleep(self.delay)

    def _drain_catalog_queue(self, scan_id: str, lease_seconds: int, poll_interval: float) -> int:
        stale = self.store.requeue_stale_documents(lease_seconds)
        if stale:
            print(f"[catalog] {scan_id}: возвращено зависших processing -> queued: {stale}")

        worker_prefix = f"catalog:{scan_id}:{socket.gethostname()}:{os.getpid()}"
        state = {"claimed": 0, "processed": 0, "active": 0, "stop": False}
        state_lock = threading.Lock()

        def worker_loop(worker_number: int) -> None:
            worker_id = f"{worker_prefix}:{worker_number}"
            while True:
                with state_lock:
                    if state["stop"]:
                        return
                item = self.store.claim_queued_document(worker_id)
                if item is None:
                    stale_count = self.store.requeue_stale_documents(lease_seconds)
                    if stale_count:
                        continue
                    with state_lock:
                        if int(state["active"]) == 0:
                            state["stop"] = True
                            return
                    time.sleep(max(0.1, poll_interval))
                    continue

                ref, depth = item
                with state_lock:
                    state["claimed"] += 1
                    state["active"] += 1
                    index = int(state["claimed"])
                try:
                    self._process_catalog_ref(scan_id, ref, index=index, depth=depth)
                except SourceAuthError:
                    with state_lock:
                        state["stop"] = True
                    raise
                finally:
                    with state_lock:
                        state["processed"] += 1
                        state["active"] -= 1

        if self.workers == 1:
            worker_loop(1)
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = [executor.submit(worker_loop, number) for number in range(1, self.workers + 1)]
                for future in concurrent.futures.as_completed(futures):
                    future.result()
        return int(state["processed"])

    def _process_catalog_ref(self, scan_id: str, ref: DocumentRef, index: int, depth: int = 0) -> None:
        """Export one claimed document and record its outcome for the scan.

        Documents that the queue holds for other reasons are still processed,
        they just do not get a scan outcome row.
        """
        doc_id = ref.doc_id
        member = self.store.is_catalog_scan_member(scan_id, doc_id)

        if not self.force and self.store.has_document_outputs(doc_id, self.formats):
            self.store.upsert_document(
                doc_id,
                "exported",
                title=ref.title,
                source_url=ref.source_url,
                formats=self.formats,
            )
            if member:
                self.store.record_catalog_document_outcome(scan_id, doc_id, OUTCOME_DONE)
            print(f"[catalog] {index} {doc_id}: уже готово, пропускаю")
            return

        print(f"[catalog] {index} {doc_id}: загрузка")
        try:
            downloader = DocumentDownloader(self.client, product=self.product, only_free=self.only_free)
            document = downloader.fetch_document(doc_id)
            pages = len(document.raw.get("pages") or [])
            self.store.upsert_document(
                doc_id,
                "processing",
                title=document.title,
                source_url=ref.source_url,
                is_free=document.is_free,
                pages=pages,
            )
            paths = export_document(document, self.out_dir, self.formats)
            self.store.save_document_outputs(document, paths)
            self.store.upsert_document(
                doc_id,
                "exported",
                title=document.title,
                source_url=ref.source_url,
                is_free=document.is_free,
                pages=pages,
                formats=self.formats,
            )
            if member:
                self.store.record_catalog_document_outcome(scan_id, doc_id, OUTCOME_DONE)
            outputs = ", ".join(f"{key}:{path.name}" for key, path in paths.items() if key != "meta")
            print(f"[catalog] {index} {doc_id}: готово ({outputs})")
        except SourceAuthError:
            # Fatal for the whole scan: hand the claimed document back untouched
            # instead of blaming it for a broken PRG session.
            self.store.enqueue_document_refs([ref], depth=depth, force=True, formats=self.formats)
            raise
        except Exception as exc:
            outcome, failure_kind, http_status = classify_document_failure(exc)
            metadata = getattr(exc, "metadata", None) if isinstance(
                exc, (DocumentNotFreeError, DocumentUnavailableError)
            ) else None
            self.store.upsert_document(
                doc_id,
                "failed",
                title=(metadata.title if metadata else "") or ref.title,
                source_url=ref.source_url,
                is_free=metadata.is_free if metadata else None,
                pages=metadata.pages if metadata else None,
                error=str(exc),
            )
            if member:
                self.store.record_catalog_document_outcome(
                    scan_id,
                    doc_id,
                    outcome,
                    failure_kind=failure_kind,
                    http_status=http_status,
                    detail=str(exc),
                )
            print(f"[catalog] {index} {doc_id}: {outcome}/{failure_kind}")
        finally:
            if getattr(self.store, "stores_document_outputs", False):
                try:
                    cleanup_document_exports(self.out_dir, doc_id)
                except OSError as cleanup_error:
                    print(f"[catalog] {index} {doc_id}: не удалось очистить временные файлы: {cleanup_error}")
            self._maybe_collect_garbage()
            time.sleep(self.delay)
