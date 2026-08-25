"""Focused coverage for the per-corpus Elasticsearch store.

An httpx MockTransport stands in for a real cluster: the tests pin the exact
mappings, bulk ids, delete scopes and result provenance the dual-corpus search
depends on.
"""

import json

import httpx
import pytest

from knowledge_service.chunking import TextChunk
from knowledge_service.corpus import (
    CORPUS_JUDICIAL_DECISION,
    CORPUS_LEGAL_ACT,
    LEGAL_SOURCE_FIELDS,
    SOURCE_SYSTEM_JUDICIAL,
    SOURCE_SYSTEM_LEGAL,
    SOT_SOURCE_FIELDS,
)
from knowledge_service.database import DocumentPayload, SotDocumentPayload
from knowledge_service.elasticsearch import LEGAL_MAPPING, SOT_MAPPING, ElasticsearchStore


def make_store(corpus, index, handler):
    return ElasticsearchStore(
        "http://es.test:9200", index, corpus=corpus, transport=httpx.MockTransport(handler)
    )


def legal_payload() -> DocumentPayload:
    return DocumentPayload(
        doc_id="123",
        title="Закон",
        source_url="https://prg.kz/lawyer/document/?doc_id=123",
        pages=2,
        updated_at="2026-01-01T00:00:00+00:00",
        text="Статья 1. Текст закона. " * 30,
        metadata={},
        source_sha256="sha-legal",
    )


def sot_payload() -> SotDocumentPayload:
    return SotDocumentPayload(
        decision_key="prg_sot:9",
        decision_id="9",
        title="Решение по делу",
        source_url="https://sb.prg.kz/decision/9",
        updated_at="2026-01-01T00:00:00+00:00",
        text="Текст судебного акта. " * 30,
        metadata={"case_number": "2-9/2026", "extra": 1},
        source_sha256="sha-sot",
        case_number="2-9/2026",
        court="Межрайонный суд",
        judge="Иванова",
        region="Алматы",
        instance="первая инстанция",
        proceeding_type="гражданское",
        decision_date="2026-03-14",
        parties=[{"role": "истец", "name": "ТОО Альфа"}],
    )


def test_store_rejects_the_virtual_all_corpus():
    with pytest.raises(ValueError) as ctx:
        ElasticsearchStore("http://es.test:9200", "idx", corpus="all")
    assert "concrete corpus" in str(ctx.value)


def test_ensure_index_writes_the_corpus_specific_mapping():
    calls = []

    def handler(request):
        calls.append(request)
        if request.method == "HEAD":
            return httpx.Response(404)
        return httpx.Response(200, json={"acknowledged": True})

    store = make_store(CORPUS_JUDICIAL_DECISION, "sot_idx", handler)
    store.ensure_index()

    put = next(request for request in calls if request.method == "PUT")
    assert str(put.url).endswith("/sot_idx")
    body = json.loads(put.content)
    assert body == SOT_MAPPING
    assert body["mappings"]["dynamic"] == "strict"
    for field in (
        "case_number",
        "court",
        "judge",
        "region",
        "instance",
        "proceeding_type",
        "decision_date",
        "parties",
        "metadata",
    ):
        assert field in body["mappings"]["properties"]
    assert "pages" not in body["mappings"]["properties"]


def test_legal_mapping_is_preserved_without_judicial_fields():
    assert LEGAL_MAPPING["mappings"]["properties"]["pages"] == {"type": "integer"}
    assert "case_number" not in LEGAL_MAPPING["mappings"]["properties"]


def test_legal_replace_keeps_legacy_chunk_ids_and_fields():
    seen = {}

    def handler(request):
        seen[(request.method, request.url.path)] = request
        if request.url.path.endswith("/_delete_by_query"):
            return httpx.Response(200, json={"deleted": 0})
        if request.url.path.endswith("/_bulk"):
            return httpx.Response(200, json={"errors": False, "items": []})
        return httpx.Response(404)

    store = make_store(CORPUS_LEGAL_ACT, "legal_idx", handler)
    chunks = [TextChunk(number=0, content="Статья 1.", char_start=0, char_end=9, heading="Статья 1")]
    assert store.replace_document(legal_payload(), chunks) == 1

    lines = seen[("POST", "/_bulk")].content.decode("utf-8").strip().split("\n")
    action = json.loads(lines[0])
    body = json.loads(lines[1])
    assert action == {"index": {"_index": "legal_idx", "_id": "123:0"}}
    assert body["doc_id"] == "123"
    assert body["pages"] == 2
    assert "case_number" not in body

    delete = seen[("POST", "/legal_idx/_delete_by_query")]
    assert json.loads(delete.content)["query"]["term"]["doc_id"] == "123"


def test_judicial_replace_uses_namespaced_ids_and_court_metadata():
    seen = {}

    def handler(request):
        seen[(request.method, request.url.path)] = request
        if request.url.path.endswith("/_delete_by_query"):
            return httpx.Response(200, json={"deleted": 0})
        if request.url.path.endswith("/_bulk"):
            return httpx.Response(200, json={"errors": False, "items": []})
        return httpx.Response(404)

    store = make_store(CORPUS_JUDICIAL_DECISION, "sot_idx", handler)
    chunks = [TextChunk(number=0, content="Текст решения.", char_start=0, char_end=13, heading="")]
    assert store.replace_document(sot_payload(), chunks) == 1

    lines = seen[("POST", "/_bulk")].content.decode("utf-8").strip().split("\n")
    action = json.loads(lines[0])
    body = json.loads(lines[1])
    assert action == {"index": {"_index": "sot_idx", "_id": "prg_sot:9:0"}}
    assert body["doc_id"] == "prg_sot:9"
    assert body["decision_id"] == "9"
    assert body["case_number"] == "2-9/2026"
    assert body["court"] == "Межрайонный суд"
    assert body["judge"] == "Иванова"
    assert body["region"] == "Алматы"
    assert body["instance"] == "первая инстанция"
    assert body["proceeding_type"] == "гражданское"
    assert body["decision_date"] == "2026-03-14"
    assert json.loads(body["parties"]) == [{"role": "истец", "name": "ТОО Альфа"}]
    assert body["metadata"]["extra"] == 1
    assert "pages" not in body

    delete = seen[("POST", "/sot_idx/_delete_by_query")]
    assert json.loads(delete.content)["query"]["term"]["doc_id"] == "prg_sot:9"


def test_judicial_parties_accept_a_scalar_source_shape():
    seen = {}

    def handler(request):
        seen[request.url.path] = request
        if request.url.path.endswith("/_delete_by_query"):
            return httpx.Response(200, json={"deleted": 0})
        return httpx.Response(200, json={"errors": False, "items": []})

    payload = sot_payload()
    payload = SotDocumentPayload(**{**payload.__dict__, "parties": "Истец: ТОО Альфа"})
    store = make_store(CORPUS_JUDICIAL_DECISION, "sot_idx", handler)
    store.replace_document(
        payload,
        [TextChunk(number=0, content="Текст.", char_start=0, char_end=6, heading="")],
    )

    lines = seen["/_bulk"].content.decode("utf-8").strip().split("\n")
    assert json.loads(lines[1])["parties"] == "Истец: ТОО Альфа"


def test_judicial_search_reads_the_sot_index_and_tags_results():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "hits": {
                    "hits": [
                        {
                            "_score": 2.5,
                            "_source": {
                                "doc_id": "prg_sot:9",
                                "decision_id": "9",
                                "title": "Решение",
                                "heading": "",
                                "content": "текст решения",
                                "source_url": "https://sb.prg.kz/decision/9",
                                "case_number": "2-9/2026",
                                "court": "Межрайонный суд",
                            },
                            "highlight": {"content": ["текст решения"]},
                        }
                    ]
                }
            },
        )

    store = make_store(CORPUS_JUDICIAL_DECISION, "sot_idx", handler)
    result = store.search("решение", 3)

    assert str(requests[0].url).endswith("/sot_idx/_search")
    sent = json.loads(requests[0].content)
    assert sent["_source"] == list(SOT_SOURCE_FIELDS)
    assert result["count"] == 1
    assert result["index"] == "sot_idx"
    hit = result["results"][0]
    assert hit["source_system"] == SOURCE_SYSTEM_JUDICIAL
    assert hit["corpus_type"] == CORPUS_JUDICIAL_DECISION
    assert hit["case_number"] == "2-9/2026"
    assert hit["excerpt"] == "текст решения"


def test_legal_search_keeps_legacy_source_fields_and_provenance():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "hits": {
                    "hits": [
                        {
                            "_score": 1.0,
                            "_source": {"doc_id": "123", "title": "Закон", "content": "текст", "pages": 4},
                        }
                    ]
                }
            },
        )

    store = make_store(CORPUS_LEGAL_ACT, "legal_idx", handler)
    result = store.search("закон", 3)

    sent = json.loads(requests[0].content)
    assert sent["_source"] == list(LEGAL_SOURCE_FIELDS)
    hit = result["results"][0]
    assert hit["source_system"] == SOURCE_SYSTEM_LEGAL
    assert hit["corpus_type"] == CORPUS_LEGAL_ACT
    assert hit["pages"] == 4


def test_search_on_a_missing_index_returns_empty_results():
    def handler(request):
        return httpx.Response(404, json={"error": {"type": "index_not_found_exception"}})

    store = make_store(CORPUS_JUDICIAL_DECISION, "sot_idx", handler)
    result = store.search("решение", 3)
    assert result["results"] == []
    assert result["count"] == 0


def test_count_reads_only_the_own_index():
    def handler(request):
        assert str(request.url).endswith("/legal_idx/_count")
        return httpx.Response(200, json={"count": 7})

    store = make_store(CORPUS_LEGAL_ACT, "legal_idx", handler)
    assert store.count() == 7


def test_count_returns_zero_for_a_missing_index():
    def handler(request):
        return httpx.Response(404)

    store = make_store(CORPUS_JUDICIAL_DECISION, "sot_idx", handler)
    assert store.count() == 0
