from __future__ import annotations

import html
import re
import urllib.parse
from dataclasses import dataclass
from pathlib import Path

from .config import DEFAULT_LIST_URL
from .http_client import PRGClient


@dataclass(frozen=True)
class DocumentRef:
    doc_id: str
    title: str = ""
    source_url: str = ""
    search_id: str = ""


@dataclass(frozen=True)
class ListingPage:
    page: int
    total: int | None
    total_pages: int | None
    documents: list[DocumentRef]
    url: str


def set_current_page(list_url: str, page: int) -> str:
    parsed = urllib.parse.urlparse(list_url)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    query["currentPage"] = [str(page)]
    rebuilt_query = urllib.parse.urlencode(query, doseq=True)
    return urllib.parse.urlunparse(parsed._replace(query=rebuilt_query))


def strip_tags(value: str) -> str:
    value = re.sub(r"<br\s*/?>", "\n", value, flags=re.I)
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def parse_listing_html(raw_html: str, page: int, url: str) -> ListingPage:
    total = None
    total_match = re.search(r"Документов:\s*([\d\s]+)", raw_html)
    if total_match:
        total = int(re.sub(r"\s+", "", total_match.group(1)))

    documents: list[DocumentRef] = []
    seen: set[str] = set()
    pattern = re.compile(
        r"<a\b[^>]*href=(?P<quote>[\"'])"
        r"(?P<href>/lawyer/document/\?doc_id=\d+[^\"']*)"
        r"(?P=quote)[^>]*>(?P<body>.*?)</a>",
        re.I | re.S,
    )
    for match in pattern.finditer(raw_html):
        href = html.unescape(match.group("href"))
        query = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
        doc_id = (query.get("doc_id") or [""])[0]
        if not doc_id or doc_id in seen:
            continue
        seen.add(doc_id)
        documents.append(
            DocumentRef(
                doc_id=doc_id,
                title=strip_tags(match.group("body")),
                source_url=urllib.parse.urljoin("https://prg.kz", href),
                search_id=(query.get("searchId") or [""])[0],
            )
        )

    total_pages = (total + 24) // 25 if total is not None else None
    return ListingPage(page=page, total=total, total_pages=total_pages, documents=documents, url=url)


def fetch_listing_page(
    client: PRGClient,
    page: int,
    list_url: str = DEFAULT_LIST_URL,
) -> ListingPage:
    url = set_current_page(list_url, page)
    response = client.get_text(url)
    return parse_listing_html(response.text, page=page, url=response.url)


def load_document_refs_from_file(path: str | Path) -> list[DocumentRef]:
    refs: list[DocumentRef] = []
    seen: set[str] = set()
    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.search(r"(?:doc_id=)?(\d{4,})", line)
        if not match:
            continue
        doc_id = match.group(1)
        if doc_id in seen:
            continue
        seen.add(doc_id)
        refs.append(DocumentRef(doc_id=doc_id, source_url=line if line.startswith("http") else ""))
    return refs
