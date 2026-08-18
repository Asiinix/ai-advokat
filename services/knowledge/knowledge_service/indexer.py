from __future__ import annotations

import os
import socket
import time

from .chunking import chunk_text
from .database import KnowledgeDatabase
from .elasticsearch import ElasticsearchStore
from .settings import Settings


def run_indexer(settings: Settings) -> None:
    database = KnowledgeDatabase(settings.database_url)
    search = ElasticsearchStore(settings.elasticsearch_url, settings.elasticsearch_index)
    worker_id = f"{socket.gethostname()}:{os.getpid()}"
    database.ensure_schema()
    while True:
        try:
            health = search.health(timeout=5.0)
            search.ensure_index()
            print(f"[indexer] Elasticsearch ready: {health.get('status', 'unknown')}")
            break
        except Exception as exc:
            print(f"[indexer] waiting for Elasticsearch: {exc}")
            time.sleep(settings.indexer_poll_seconds)
    stale = database.requeue_stale(settings.indexer_lease_seconds)
    if stale:
        print(f"[indexer] returned stale jobs to queue: {stale}")
    failed = database.requeue_failed()
    if failed:
        print(f"[indexer] returned failed jobs to queue for this deployment: {failed}")

    processed = 0
    print(
        f"[indexer] started: index={settings.elasticsearch_index}, "
        f"claim_batch={settings.indexer_claim_batch_size}"
    )
    try:
        while True:
            seeded = database.seed_jobs(settings.indexer_seed_batch_size)
            if seeded:
                print(f"[indexer] queued documents: {seeded}")
            jobs = database.claim_jobs(worker_id, settings.indexer_claim_batch_size)
            if not jobs:
                stats = database.job_stats()
                print(f"[indexer] idle: {stats}")
                time.sleep(settings.indexer_poll_seconds)
                continue

            for job in jobs:
                try:
                    document = database.load_document(job.doc_id)
                    if document is None:
                        raise RuntimeError("document txt output is missing")
                    if document.source_sha256 != job.source_sha256:
                        raise RuntimeError("document changed after the indexing job was claimed")
                    chunks = chunk_text(
                        document.text,
                        max_chars=settings.chunk_max_chars,
                        overlap_chars=settings.chunk_overlap_chars,
                    )
                    indexed = search.replace_document(document, chunks)
                    database.mark_indexed(job.doc_id, indexed)
                    processed += 1
                    print(
                        f"[indexer] {job.doc_id}: indexed chunks={indexed}, "
                        f"processed={processed}"
                    )
                except Exception as exc:
                    database.mark_failed(job.doc_id, str(exc))
                    print(f"[indexer] {job.doc_id}: failed: {exc}")
    finally:
        search.close()
