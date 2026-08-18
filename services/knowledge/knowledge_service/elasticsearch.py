from __future__ import annotations

import json
from typing import Any, Iterable

import httpx

from .chunking import TextChunk
from .database import DocumentPayload


class ElasticsearchStore:
    def __init__(self, base_url: str, index_name: str, timeout: float = 60.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.index_name = index_name
        self.client = httpx.Client(base_url=self.base_url, timeout=timeout)

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
        mapping = {
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
        created = self.client.put(f"/{self.index_name}", json=mapping)
        created.raise_for_status()

    def replace_document(self, document: DocumentPayload, chunks: Iterable[TextChunk]) -> int:
        deleted = self.client.post(
            f"/{self.index_name}/_delete_by_query",
            params={"conflicts": "proceed", "refresh": "false"},
            json={"query": {"term": {"doc_id": document.doc_id}}},
        )
        if deleted.status_code not in {200, 404}:
            deleted.raise_for_status()

        chunk_list = list(chunks)
        for offset in range(0, len(chunk_list), 500):
            lines: list[str] = []
            for chunk in chunk_list[offset : offset + 500]:
                chunk_id = f"{document.doc_id}:{chunk.number}"
                lines.append(json.dumps({"index": {"_index": self.index_name, "_id": chunk_id}}))
                body = {
                    "doc_id": document.doc_id,
                    "chunk_id": chunk_id,
                    "chunk_number": chunk.number,
                    "title": document.title,
                    "heading": chunk.heading,
                    "content": chunk.content,
                    "source_url": document.source_url
                    or f"https://prg.kz/lawyer/document/?doc_id={document.doc_id}",
                    "pages": document.pages,
                    "char_start": chunk.char_start,
                    "char_end": chunk.char_end,
                    "source_sha256": document.source_sha256,
                    "document_updated_at": document.updated_at,
                }
                lines.append(json.dumps(body, ensure_ascii=False))
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

    def search(self, query: str, limit: int) -> dict[str, Any]:
        body = {
            "size": limit,
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["title^5", "heading^3", "content"],
                    "type": "best_fields",
                }
            },
            "collapse": {"field": "doc_id"},
            "highlight": {
                "fields": {"content": {"fragment_size": 450, "number_of_fragments": 2}},
                "pre_tags": [""],
                "post_tags": [""],
            },
            "_source": [
                "doc_id",
                "title",
                "heading",
                "content",
                "chunk_number",
                "char_start",
                "char_end",
                "source_url",
                "pages",
            ],
        }
        response = self.client.post(f"/{self.index_name}/_search", json=body)
        response.raise_for_status()
        raw = response.json()
        results = []
        for hit in raw.get("hits", {}).get("hits", []):
            source = hit.get("_source", {})
            fragments = hit.get("highlight", {}).get("content", [])
            excerpt = " … ".join(fragments) if fragments else str(source.get("content", ""))[:900]
            results.append(
                {
                    "doc_id": source.get("doc_id", ""),
                    "title": source.get("title", ""),
                    "heading": source.get("heading", ""),
                    "excerpt": excerpt,
                    "score": hit.get("_score"),
                    "chunk_number": source.get("chunk_number"),
                    "char_start": source.get("char_start"),
                    "char_end": source.get("char_end"),
                    "source_url": source.get("source_url", ""),
                    "pages": source.get("pages"),
                }
            )
        return {"query": query, "results": results, "count": len(results)}

    def count(self) -> int:
        response = self.client.get(f"/{self.index_name}/_count")
        if response.status_code == 404:
            return 0
        response.raise_for_status()
        return int(response.json().get("count", 0))
