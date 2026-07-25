from __future__ import annotations

import concurrent.futures
import json
import os
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
    ) -> None:
        if from_page < 1 or to_page < from_page:
            raise ValueError("Page range must be valid: from_page >= 1 and to_page >= from_page.")

        refs: list[DocumentRef] = []
        seen: set[str] = set()
        for page in range(from_page, to_page + 1):
            print(f"[list] страница {page}/{to_page}")
            try:
                listing = fetch_listing_page(self.client, page=page, list_url=list_url)
                self.store.mark_listing_page(
                    page,
                    "done",
                    doc_count=len(listing.documents),
                    total=listing.total,
                )
                print(
                    f"       найдено {len(listing.documents)} документов"
                    + (f", всего {listing.total}" if listing.total else "")
                )
                for ref in listing.documents:
                    if ref.doc_id not in seen:
                        seen.add(ref.doc_id)
                        refs.append(ref)
                        if max_docs and len(refs) >= max_docs:
                            break
                if max_docs and len(refs) >= max_docs:
                    break
            except Exception as exc:
                self.store.mark_listing_page(page, "failed", error=str(exc))
                print(f"[list] ошибка на странице {page}: {exc}")
            time.sleep(self.delay)

        self.crawl_refs(refs)

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
                if linked_doc_id in seen:
                    continue
                if any(item.doc_id == linked_doc_id for item, _ in queue[position:]):
                    continue
                queue.append((DocumentRef(doc_id=linked_doc_id), depth + 1))
                added += 1
                linked_added_total += 1
            if added:
                print(f"[links] {ref.doc_id}: добавлено связанных документов: {added}")

    def _process_ref(
        self,
        ref: DocumentRef,
        index: int,
        total: int,
        depth: int = 0,
    ) -> list[str]:
        doc_id = ref.doc_id
        if (
            not self.force
            and self.store.get_document_status(doc_id) == "exported"
            and self.store.has_document_outputs(doc_id, self.formats)
        ):
            print(f"[docs] {index}/{total} {doc_id}: уже готово, пропускаю")
            return read_exported_links(self.out_dir, doc_id)

        self.store.upsert_document(
            doc_id,
            "queued",
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
                "downloaded",
                title=document.title,
                source_url=ref.source_url,
                is_free=document.is_free,
                pages=len(document.raw.get("pages") or []),
            )
            paths = export_document(document, self.out_dir, self.formats)
            self.store.save_document_outputs(document, paths)
            self.store.upsert_document(
                doc_id,
                "exported",
                title=document.title,
                source_url=ref.source_url,
                is_free=document.is_free,
                pages=len(document.raw.get("pages") or []),
                formats=self.formats,
            )
            outputs = ", ".join(f"{key}:{path.name}" for key, path in paths.items() if key != "meta")
            print(
                f"[docs] {index}/{total} {doc_id}: готово "
                f"({outputs}, links:{len(document.linked_doc_ids)})"
            )
            return document.linked_doc_ids
        except Exception as exc:
            self.store.upsert_document(doc_id, "failed", title=ref.title, source_url=ref.source_url, error=str(exc))
            print(f"[docs] {index}/{total} {doc_id}: ошибка: {exc}")
            return []
        finally:
            time.sleep(self.delay)
