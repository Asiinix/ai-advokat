"""Focused coverage for the dual-corpus indexer loop.

Fake stores and a fake database let the tests observe one full
claim -> verify -> chunk -> index -> mark cycle without Postgres,
Elasticsearch or a never-ending loop.
"""

from unittest import mock

import pytest

from knowledge_service.database import DocumentPayload, IndexJob
from knowledge_service.indexer import index_payload, run_indexer
from knowledge_service.settings import Settings


def make_settings(**overrides) -> Settings:
    values = dict(
        database_url="postgresql://x",
        elasticsearch_url="http://es.test:9200",
        elasticsearch_index="legal_idx",
        sot_elasticsearch_index="sot_idx",
        indexer_corpus="legal_act",
        mcp_default_corpus="all",
        mode="indexer",
        port=8000,
        mcp_api_key="",
        mcp_allowed_hosts=("localhost:*",),
        mcp_allowed_origins=(),
        indexer_seed_batch_size=10,
        indexer_claim_batch_size=5,
        indexer_poll_seconds=0.1,
        indexer_lease_seconds=1800,
        chunk_max_chars=700,
        chunk_overlap_chars=100,
        max_document_chars=10000,
    )
    values.update(overrides)
    return Settings(**values)


class FakeSearchStore:
    def __init__(self):
        self.ensured = False
        self.replaced: list = []

    def health(self, timeout=5.0):
        return {"status": "green"}

    def ensure_index(self):
        self.ensured = True

    def replace_document(self, payload, chunks):
        chunks = list(chunks)
        self.replaced.append((payload, chunks))
        return len(chunks)

    def close(self):
        pass


class FakeDatabase:
    def __init__(self, payload=None):
        self.payload = payload
        self.ensured = False
        self.indexed = []
        self.failed = []
        self._claim_calls = 0

    def ensure_schema(self):
        self.ensured = True

    def requeue_stale(self, lease_seconds):
        return 0

    def requeue_failed(self):
        return 0

    def seed_jobs(self, limit):
        return 0

    def claim_jobs(self, worker_id, limit):
        self._claim_calls += 1
        if self._claim_calls == 1:
            return [IndexJob(doc_id="123", source_sha256="sha-123")]
        return []

    def load_document(self, doc_id):
        return self.payload

    def mark_indexed(self, doc_id, chunks_indexed):
        self.indexed.append((doc_id, chunks_indexed))

    def mark_failed(self, doc_id, error):
        self.failed.append((doc_id, error))

    def job_stats(self):
        return {"queued": 0}


def legal_payload(sha256="sha-123") -> DocumentPayload:
    return DocumentPayload(
        doc_id="123",
        title="Закон",
        source_url="",
        pages=None,
        updated_at="2026-01-01T00:00:00+00:00",
        text="Статья 1. Текст закона. " * 20,
        metadata={},
        source_sha256=sha256,
    )


def test_run_indexer_indexes_a_claimed_legal_job():
    settings = make_settings()
    database = FakeDatabase(payload=legal_payload())
    store = FakeSearchStore()

    # The loop would run forever; after the batch is processed the next idle
    # poll raises, which also exercises the finally/close path.
    with mock.patch("knowledge_service.indexer.time.sleep", side_effect=RuntimeError("idle stop")):
        with pytest.raises(RuntimeError):
            run_indexer(settings, database=database, legal_search=store, sot_search=FakeSearchStore())

    assert database.ensured is True
    assert store.ensured is True
    assert database.indexed == [("123", 1)]
    assert database.failed == []
    assert store.replaced and store.replaced[0][0].doc_id == "123"


def test_run_indexer_marks_failures_and_keeps_going():
    settings = make_settings()

    class BrokenDatabase(FakeDatabase):
        def load_document(self, doc_id):
            raise RuntimeError("txt output missing")

    database = BrokenDatabase()
    store = FakeSearchStore()
    with mock.patch("knowledge_service.indexer.time.sleep", side_effect=RuntimeError("idle stop")):
        with pytest.raises(RuntimeError):
            run_indexer(settings, database=database, legal_search=store, sot_search=FakeSearchStore())

    assert database.failed and database.failed[0][0] == "123"
    assert store.replaced == []


def test_index_payload_rejects_a_document_changed_after_the_claim():
    payload = legal_payload(sha256="sha-new")
    with pytest.raises(RuntimeError) as ctx:
        index_payload(FakeSearchStore(), payload, make_settings(), "sha-old")
    assert "changed after" in str(ctx.value)


def test_index_payload_chunks_and_indexes_an_unchanged_document():
    store = FakeSearchStore()
    indexed = index_payload(store, legal_payload(), make_settings(), "sha-123")
    assert indexed == 1
    assert store.replaced[0][0].source_sha256 == "sha-123"
