from __future__ import annotations

import os
import socket
import time

from .chunking import chunk_text
from .corpus import CORPUS_JUDICIAL_DECISION, CORPUS_LEGAL_ACT
from .database import KnowledgeDatabase
from .elasticsearch import ElasticsearchStore
from .settings import Settings


def index_payload(store, payload, settings: Settings, job_source_sha256: str) -> int:
    """Chunk and index one payload, refusing anything changed mid-flight."""
    if payload.source_sha256 != job_source_sha256:
        raise RuntimeError("document changed after the indexing job was claimed")
    chunks = chunk_text(
        payload.text,
        max_chars=settings.chunk_max_chars,
        overlap_chars=settings.chunk_overlap_chars,
    )
    return store.replace_document(payload, chunks)


def run_indexer(
    settings: Settings,
    database: KnowledgeDatabase | None = None,
    legal_search: ElasticsearchStore | None = None,
    sot_search: ElasticsearchStore | None = None,
) -> None:
    database = database or KnowledgeDatabase(settings.database_url)
    legal_search = legal_search or ElasticsearchStore(
        settings.elasticsearch_url, settings.elasticsearch_index, corpus=CORPUS_LEGAL_ACT
    )
    sot_search = sot_search or ElasticsearchStore(
        settings.elasticsearch_url, settings.sot_elasticsearch_index, corpus=CORPUS_JUDICIAL_DECISION
    )
    worker_id = f"{socket.gethostname()}:{os.getpid()}"
    database.ensure_schema()
    while True:
        try:
            primary = legal_search if settings.indexes_legal else sot_search
            health = primary.health(timeout=5.0)
            if settings.indexes_legal:
                legal_search.ensure_index()
            if settings.indexes_judicial:
                sot_search.ensure_index()
            print(f"[indexer] Elasticsearch ready: {health.get('status', 'unknown')}")
            break
        except Exception as exc:
            print(f"[indexer] waiting for Elasticsearch: {exc}")
            time.sleep(settings.indexer_poll_seconds)

    if settings.indexes_legal:
        stale = database.requeue_stale(settings.indexer_lease_seconds)
        if stale:
            print(f"[indexer] returned stale legal jobs to queue: {stale}")
        failed = database.requeue_failed()
        if failed:
            print(f"[indexer] returned failed legal jobs to queue for this deployment: {failed}")
    if settings.indexes_judicial:
        stale = database.requeue_sot_stale(settings.indexer_lease_seconds)
        if stale:
            print(f"[indexer] returned stale judicial jobs to queue: {stale}")
        failed = database.requeue_sot_failed()
        if failed:
            print(f"[indexer] returned failed judicial jobs to queue for this deployment: {failed}")

    processed_legal = 0
    processed_judicial = 0
    print(
        f"[indexer] started: legal={settings.elasticsearch_index if settings.indexes_legal else 'off'}, "
        f"judicial={settings.sot_elasticsearch_index if settings.indexes_judicial else 'off'}, "
        f"claim_batch={settings.indexer_claim_batch_size}"
    )
    try:
        while True:
            if settings.indexes_legal:
                seeded = database.seed_jobs(settings.indexer_seed_batch_size)
                if seeded:
                    print(f"[indexer] queued legal documents: {seeded}")
            if settings.indexes_judicial:
                seeded = database.seed_sot_jobs(settings.indexer_seed_batch_size)
                if seeded:
                    print(f"[indexer] queued judicial decisions: {seeded}")

            legal_jobs = (
                database.claim_jobs(worker_id, settings.indexer_claim_batch_size)
                if settings.indexes_legal
                else []
            )
            judicial_jobs = (
                database.claim_sot_jobs(worker_id, settings.indexer_claim_batch_size)
                if settings.indexes_judicial
                else []
            )

            if not legal_jobs and not judicial_jobs:
                stats: dict[str, object] = {}
                if settings.indexes_legal:
                    stats["legal"] = database.job_stats()
                if settings.indexes_judicial:
                    stats["judicial"] = database.sot_job_stats()
                print(f"[indexer] idle: {stats}")
                time.sleep(settings.indexer_poll_seconds)
                continue

            for job in legal_jobs:
                try:
                    document = database.load_document(job.doc_id)
                    if document is None:
                        raise RuntimeError("document txt output is missing")
                    indexed = index_payload(legal_search, document, settings, job.source_sha256)
                    database.mark_indexed(job.doc_id, indexed)
                    processed_legal += 1
                    print(
                        f"[indexer] {job.doc_id}: indexed chunks={indexed}, "
                        f"legal processed={processed_legal}"
                    )
                except Exception as exc:
                    database.mark_failed(job.doc_id, str(exc))
                    print(f"[indexer] {job.doc_id}: failed: {exc}")

            for job in judicial_jobs:
                try:
                    decision = database.load_sot_decision(job.decision_key)
                    if decision is None:
                        raise RuntimeError("decision txt output is missing")
                    indexed = index_payload(sot_search, decision, settings, job.source_sha256)
                    database.mark_sot_indexed(job.decision_key, indexed)
                    processed_judicial += 1
                    print(
                        f"[indexer] {job.decision_key}: indexed chunks={indexed}, "
                        f"judicial processed={processed_judicial}"
                    )
                except Exception as exc:
                    database.mark_sot_failed(job.decision_key, str(exc))
                    print(f"[indexer] {job.decision_key}: failed: {exc}")
    finally:
        legal_search.close()
        sot_search.close()
