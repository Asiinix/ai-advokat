from __future__ import annotations

import json
from typing import Any, Iterable

import httpx

from .chunking import TextChunk
from .corpus import (
    CORPUS_JUDICIAL_DECISION,
    CORPUS_LEGAL_ACT,
    LEGAL_SOURCE_FIELDS,
    SOT_SOURCE_FIELDS,
    build_result,
    legal_chunk_id,
    sot_chunk_id,
)
from .database import DocumentPayload, SotDocumentPayload

# The legacy mapping is preserved byte for byte so the already-indexed 392k
# legal chunks never need a reindex; the SOT mapping adds the court metadata
# fields without touching the legal index.
LEGAL_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "15s",
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "doc_id": {"type": "keyword"},
            "chunk_id": {"type": "keyword"},
            "chunk_number": {"type": "integer"},
            "title": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 1024}},
            },
            "heading": {"type": "text"},
            "content": {"type": "text"},
            "source_url": {"type": "keyword", "ignore_above": 2048},
            "pages": {"type": "integer"},
            "char_start": {"type": "integer"},
            "char_end": {"type": "integer"},
            "source_sha256": {"type": "keyword"},
            "document_updated_at": {"type": "date"},
        },
    },
}

_TEXT_WITH_KEYWORD = {
    "type": "text",
    "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
}

SOT_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "15s",
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "doc_id": {"type": "keyword"},
            "chunk_id": {"type": "keyword"},
            "chunk_number": {"type": "integer"},
            "decision_id": {"type": "keyword"},
            "title": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 1024}},
            },
            "heading": {"type": "text"},
            "content": {"type": "text"},
            "source_url": {"type": "keyword", "ignore_above": 2048},
            "case_number": _TEXT_WITH_KEYWORD,
            "court": _TEXT_WITH_KEYWORD,
            "judge": _TEXT_WITH_KEYWORD,
            "region": _TEXT_WITH_KEYWORD,
            "instance": _TEXT_WITH_KEYWORD,
            "proceeding_type": _TEXT_WITH_KEYWORD,
            "decision_date": {"type": "date", "ignore_malformed": True},
            # PRG.SOT may expose parties as a string, a list or an object. It
            # is serialized to searchable text below; the exact structured
            # value remains available from Postgres through get_document.
            "parties": {"type": "text"},
            # Keep captured metadata in _source without letting arbitrary
            # source keys cause mapping explosion or type clashes.
            "metadata": {"type": "object", "enabled": False},
            "char_start": {"type": "integer"},
            "char_end": {"type": "integer"},
            "source_sha256": {"type": "keyword"},
            "document_updated_at": {"type": "date"},
        },
    },
}

LEGAL_DOCUMENT_URL_TEMPLATE = "https://prg.kz/lawyer/document/?doc_id={doc_id}"


class ElasticsearchStore:
    """One concrete corpus inside the shared Elasticsearch cluster.

    A store is bound to exactly one index and one corpus; spreading a query
    over both corpora is the MCP layer's job, so a store never mixes mappings
    or id namespaces.
    """

    def __init__(
        self,
        base_url: str,
        index_name: str,
        corpus: str = CORPUS_LEGAL_ACT,
        timeout: float = 60.0,
        transport: Any = None,
    ) -> None:
        if corpus not in (CORPUS_LEGAL_ACT, CORPUS_JUDICIAL_DECISION):
            raise ValueError(
                f"ElasticsearchStore serves one concrete corpus, got '{corpus}'. "
                "Spread a search over both corpora at the MCP layer, not here."
            )
        self.base_url = base_url.rstrip("/")
        self.index_name = index_name
        self.corpus = corpus
        self.client = httpx.Client(base_url=self.base_url, timeout=timeout, transport=transport)

    @property
    def is_judicial(self) -> bool:
        return self.corpus == CORPUS_JUDICIAL_DECISION

    def close(self) -> None:
        self.client.close()

    def health(self, timeout: float = 5.0) -> dict[str, Any]:
        response = self.client.get("/_cluster/health", timeout=timeout)
        response.raise_for_status()
        return response.json()

    def ensure_index(self) -> None:
        response = self.client.head(f"/{self.index_name}")
        if response.status_code == 200:
            return
        if response.status_code != 404:
            response.raise_for_status()
        mapping = SOT_MAPPING if self.is_judicial else LEGAL_MAPPING
        created = self.client.put(f"/{self.index_name}", json=mapping)
        created.raise_for_status()

    def replace_document(self, document: DocumentPayload | SotDocumentPayload, chunks: Iterable[TextChunk]) -> int:
        doc_id = document.decision_key if self.is_judicial else document.doc_id
        deleted = self.client.post(
            f"/{self.index_name}/_delete_by_query",
            params={"conflicts": "proceed", "refresh": "false"},
            json={"query": {"term": {"doc_id": doc_id}}},
        )
        if deleted.status_code not in {200, 404}:
            deleted.raise_for_status()

        chunk_list = list(chunks)
        for offset in range(0, len(chunk_list), 500):
            lines: list[str] = []
            for chunk in chunk_list[offset : offset + 500]:
                chunk_id = (
                    sot_chunk_id(document.decision_key, chunk.number)
                    if self.is_judicial
                    else legal_chunk_id(document.doc_id, chunk.number)
                )
                lines.append(json.dumps({"index": {"_index": self.index_name, "_id": chunk_id}}))
                lines.append(json.dumps(self._chunk_body(document, chunk_id, chunk), ensure_ascii=False))
            payload = "\n".join(lines) + "\n"
            response = self.client.post(
                "/_bulk",
                content=payload.encode("utf-8"),
                headers={"Content-Type": "application/x-ndjson"},
            )
            response.raise_for_status()
            result = response.json()
            if result.get("errors"):
                failures = [
                    item
                    for item in result.get("items", [])
                    if int(item.get("index", {}).get("status", 500)) >= 300
                ]
                raise RuntimeError(f"Elasticsearch bulk indexing failed: {failures[:3]}")
        return len(chunk_list)

    def _chunk_body(self, document, chunk_id: str, chunk: TextChunk) -> dict[str, Any]:
        body: dict[str, Any] = {
            "doc_id": document.decision_key if self.is_judicial else document.doc_id,
            "chunk_id": chunk_id,
            "chunk_number": chunk.number,
            "title": document.title,
            "heading": chunk.heading,
            "content": chunk.content,
            "source_url": document.source_url
            or (
                ""
                if self.is_judicial
                else LEGAL_DOCUMENT_URL_TEMPLATE.format(doc_id=document.doc_id)
            ),
            "char_start": chunk.char_start,
            "char_end": chunk.char_end,
            "source_sha256": document.source_sha256,
            "document_updated_at": document.updated_at,
        }
        if self.is_judicial:
            body["decision_id"] = document.decision_id
            body["case_number"] = document.case_number or ""
            body["court"] = document.court or ""
            body["judge"] = document.judge or ""
            body["region"] = document.region or ""
            body["instance"] = document.instance or ""
            body["proceeding_type"] = document.proceeding_type or ""
            if document.decision_date:
                body["decision_date"] = document.decision_date
            if document.parties:
                body["parties"] = _searchable_text(document.parties)
            if document.metadata:
                body["metadata"] = document.metadata
        else:
            body["pages"] = document.pages
        return body

    def search(self, query: str, limit: int) -> dict[str, Any]:
        fields = ["title^5", "heading^3", "content"]
        if self.is_judicial:
            fields.extend(["case_number^4", "court^2", "judge^2", "parties"])
        body = {
            "size": limit,
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": fields,
                    "type": "best_fields",
                }
            },
            "collapse": {"field": "doc_id"},
            "highlight": {
                "fields": {"content": {"fragment_size": 450, "number_of_fragments": 2}},
                "pre_tags": [""],
                "post_tags": [""],
            },
            "_source": list(SOT_SOURCE_FIELDS if self.is_judicial else LEGAL_SOURCE_FIELDS),
        }
        response = self.client.post(f"/{self.index_name}/_search", json=body)
        if response.status_code == 404:
            # The index for this corpus has not been created yet: nothing has
            # been indexed there, which is an empty result set, not an outage.
            return {
                "query": query,
                "index": self.index_name,
                "corpus": self.corpus,
                "results": [],
                "count": 0,
            }
        response.raise_for_status()
        raw = response.json()
        results = [
            build_result(hit, self.corpus) for hit in raw.get("hits", {}).get("hits", [])
        ]
        return {
            "query": query,
            "index": self.index_name,
            "corpus": self.corpus,
            "results": results,
            "count": len(results),
        }

    def count(self) -> int:
        response = self.client.get(f"/{self.index_name}/_count")
        if response.status_code == 404:
            return 0
        response.raise_for_status()
        return int(response.json().get("count", 0))


def _searchable_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
