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

from .config import DEFAULT_LIST_URL
from .document import DocumentDownloader
from .exporters import export_document
from .http_client import PRGClient
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
        self._gc_interval = positive_env_int("PRG_GC_INTERVAL", 25)
        database_url = os.environ.get("PRG_DATABASE_URL") or os.environ.get("DATABASE_URL")
        if database_url and os.environ.get("PRG_DISABLE_POSTGRES") not in {"1", "true", "yes"}:
            self.store = PostgresCrawlStore(database_url)
        else:
            self.store = CrawlStore(self.out_dir)
        self.client = PRGClient(timeout=timeout, retries=retries)

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
        producer_thread = threading.Thread(target=producer, name="prg-listing-producer", daemon=True)
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
                future.result()

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
