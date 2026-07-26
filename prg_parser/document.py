from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass
from typing import Any, Callable

from .config import API_BASE_URL, BASE_URL
from .http_client import PRGClient

ProgressCallback = Callable[[str], None]


@dataclass(frozen=True)
class DocumentData:
    doc_id: str
    title: str
    is_free: bool
    raw: dict[str, Any]
    paragraphs: list[dict[str, Any]]
    linked_doc_ids: list[str]
    html: str


@dataclass(frozen=True)
class DocumentMetadata:
    doc_id: str
    title: str
    is_free: bool | None
    pages: int | None


class DocumentNotFreeError(PermissionError):
    def __init__(self, metadata: DocumentMetadata) -> None:
        super().__init__(f"Document {metadata.doc_id} is not marked as free.")
        self.metadata = metadata


def document_metadata_from_response(doc_id: str, data: dict[str, Any]) -> DocumentMetadata:
    raw_is_free = data.get("isDocumentFree")
    pages = data.get("pages")
    return DocumentMetadata(
        doc_id=doc_id,
        title=str(data.get("name") or ""),
        is_free=None if raw_is_free is None else bool(raw_is_free),
        pages=len(pages) if isinstance(pages, list) else None,
    )


def document_page_url(doc_id: str, page_index: int, product: str = "lawyer") -> str:
    query = urllib.parse.urlencode({"withHtmlTags": "true", "product": product})
    return f"{API_BASE_URL}/api/Document/GetDocument/{doc_id}/{page_index}?{query}"


def render_paragraph(paragraph: dict[str, Any]) -> str:
    raw_html = paragraph.get("html") or ""
    children = paragraph.get("paragpraphs") or []
    if not children:
        return raw_html

    rendered_children = "".join(render_paragraph(child) for child in children)
    tag = None
    html_tags = paragraph.get("htmlTags") or []
    if html_tags and isinstance(html_tags[0], dict):
        tag = (html_tags[0].get("tag") or "").lower()

    if tag and f"</{tag}>" not in raw_html.lower():
        return f"{raw_html}{rendered_children}</{tag}>"
    return f"{raw_html}{rendered_children}"


def extract_doc_id_from_href(raw_href: str) -> tuple[str | None, str | None]:
    href = raw_href.replace("&amp;", "&")
    parsed = urllib.parse.urlparse(href)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    doc_id = (query.get("doc_id") or [""])[0] or None
    sub_id = (query.get("sub_id") or [""])[0] or None

    if not sub_id and parsed.fragment.startswith("sub_id="):
        sub_id = parsed.fragment.split("=", 1)[1] or None
    if not doc_id:
        match = re.search(r"(?:^|[?&])doc_id=(\d+)", href)
        doc_id = match.group(1) if match else None
    return doc_id, sub_id


def local_document_href(current_doc_id: str, target_doc_id: str, sub_id: str | None = None) -> str:
    href = "document.html" if target_doc_id == current_doc_id else f"../{target_doc_id}/document.html"
    if sub_id:
        href = f"{href}#SUB{sub_id}"
    return href


def remote_document_href(raw_href: str) -> str:
    if raw_href.startswith("?doc_id="):
        return f"{BASE_URL}/lawyer/document/{raw_href}"
    if raw_href.startswith("/"):
        return urllib.parse.urljoin(BASE_URL, raw_href)
    return raw_href


def rewrite_links(fragment: str, current_doc_id: str) -> str:
    def replace_href(match: re.Match[str]) -> str:
        prefix = match.group("prefix")
        quote = match.group("quote")
        raw_href = match.group("href")
        target_doc_id, sub_id = extract_doc_id_from_href(raw_href)
        if target_doc_id:
            href = local_document_href(current_doc_id, target_doc_id, sub_id)
        else:
            href = remote_document_href(raw_href)
        return f"{prefix}{href}{quote}"

    return re.sub(
        r"(?P<prefix>href=(?P<quote>[\"']))(?P<href>[^\"']+)(?P=quote)",
        replace_href,
        fragment,
    )


def extract_linked_doc_ids(fragment: str, current_doc_id: str) -> list[str]:
    found: list[str] = []
    seen: set[str] = {current_doc_id}

    for match in re.finditer(r"href=[\"'](?P<href>[^\"']+)[\"']", fragment):
        doc_id, _ = extract_doc_id_from_href(match.group("href"))
        if doc_id and doc_id not in seen:
            seen.add(doc_id)
            found.append(doc_id)

    for match in re.finditer(r"doc-id=[\"'](?P<doc_id>\d+)[\"']", fragment):
        doc_id = match.group("doc_id")
        if doc_id not in seen:
            seen.add(doc_id)
            found.append(doc_id)

    return found


class DocumentDownloader:
    def __init__(
        self,
        client: PRGClient,
        product: str = "lawyer",
        only_free: bool = True,
    ) -> None:
        self.client = client
        self.product = product
        self.only_free = only_free

    def fetch_document(
        self,
        doc_id: str,
        progress: ProgressCallback | None = None,
    ) -> DocumentData:
        first = self._fetch_page(doc_id, 0)
        metadata = document_metadata_from_response(doc_id, first)
        is_free = bool(metadata.is_free)
        title = metadata.title or f"document-{doc_id}"
        if self.only_free and not is_free:
            raise DocumentNotFreeError(metadata)

        pages = first.get("pages") or []
        if not pages:
            raise ValueError(f"Document {doc_id} has no pages in API response.")

        collected_pages: list[dict[str, Any]] = [dict(page or {}) for page in pages]
        total_pages = len(collected_pages)
        if progress:
            progress(f"{doc_id}: найдено чанков {total_pages}")

        for index in range(total_pages):
            page = collected_pages[index]
            if page.get("paragpraphs") is None:
                if progress:
                    progress(f"{doc_id}: загружаю чанк {index + 1}/{total_pages}")
                data = self._fetch_page(doc_id, index)
                loaded_pages = data.get("pages") or []
                if index >= len(loaded_pages):
                    raise ValueError(f"Document {doc_id}: API did not return page {index}.")
                page = loaded_pages[index] or {}
                collected_pages[index] = page

        paragraphs: list[dict[str, Any]] = []
        for page in collected_pages:
            for paragraph in page.get("paragpraphs") or []:
                paragraphs.append(paragraph)
        paragraphs.sort(key=lambda item: int(item.get("paragraphId") or 0))

        raw_body_html = "\n".join(render_paragraph(item) for item in paragraphs)
        linked_doc_ids = extract_linked_doc_ids(raw_body_html, doc_id)
        body_html = rewrite_links(raw_body_html, doc_id)
        raw = dict(first)
        raw["pages"] = collected_pages
        return DocumentData(
            doc_id=doc_id,
            title=title,
            is_free=is_free,
            raw=raw,
            paragraphs=paragraphs,
            linked_doc_ids=linked_doc_ids,
            html=body_html,
        )

    def fetch_document_metadata(self, doc_id: str) -> DocumentMetadata:
        first = self._fetch_page(doc_id, 0)
        return document_metadata_from_response(doc_id, first)

    def _fetch_page(self, doc_id: str, page_index: int) -> dict[str, Any]:
        url = document_page_url(doc_id, page_index, self.product)
        data = self.client.get_json(url)
        if not isinstance(data, dict):
            raise ValueError(f"Document {doc_id}: expected JSON object, got {type(data).__name__}.")
        return data
