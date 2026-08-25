from __future__ import annotations

import hmac
from typing import Any

import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.responses import JSONResponse
from starlette.routing import Route

from .corpus import (
    CORPUS_JUDICIAL_DECISION,
    CORPUS_LEGAL_ACT,
    SOURCE_SYSTEM_JUDICIAL,
    SOURCE_SYSTEM_LEGAL,
    SOT_ID_PREFIX,
    indices_for_corpus,
    merge_ranked_results,
    normalize_corpus,
)
from .database import KnowledgeDatabase
from .elasticsearch import ElasticsearchStore
from .settings import Settings


settings = Settings.from_env()
database = KnowledgeDatabase(settings.database_url)
legal_search = ElasticsearchStore(
    settings.elasticsearch_url, settings.elasticsearch_index, corpus=CORPUS_LEGAL_ACT
)
sot_search = ElasticsearchStore(
    settings.elasticsearch_url, settings.sot_elasticsearch_index, corpus=CORPUS_JUDICIAL_DECISION
)

mcp = FastMCP(
    "AI Advokat Legal Knowledge",
    instructions=(
        "Search and read two collections that share one API: legal acts from "
        "PRG.ZANGER and judicial decisions from PRG.SOT. Use search_documents "
        "first (corpus=all searches both by default), then get_document for "
        "exact source text. Legal documents use their bare doc_id; judicial "
        "decisions use the namespaced prg_sot: key returned in results. Always "
        "cite doc_id, title and source_system in answers."
    ),
    stateless_http=True,
    json_response=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=list(settings.mcp_allowed_hosts),
        allowed_origins=list(settings.mcp_allowed_origins),
    ),
)


@mcp.tool()
def search_documents(query: str, corpus: str | None = None, limit: int = 5) -> dict[str, Any]:
    """Full-text search across legal acts (PRG.ZANGER) and judicial decisions (PRG.SOT).

    corpus: 'legal_act', 'judicial_decision' or 'all'. The default is 'all':
    both indices are queried and their results merged into one ranking. Every
    result carries source_system and corpus_type so a passage can always be
    cited.
    """
    cleaned = query.strip()
    if not cleaned:
        raise ValueError("query must not be empty")
    resolved = normalize_corpus(corpus, default=settings.mcp_default_corpus)
    safe_limit = max(1, min(limit, 20))
    wanted = indices_for_corpus(
        resolved, settings.elasticsearch_index, settings.sot_elasticsearch_index
    )
    collected = []
    if settings.elasticsearch_index in wanted:
        collected.append((CORPUS_LEGAL_ACT, legal_search.search(cleaned, safe_limit)["results"]))
    if settings.sot_elasticsearch_index in wanted:
        collected.append(
            (CORPUS_JUDICIAL_DECISION, sot_search.search(cleaned, safe_limit)["results"])
        )
    results = merge_ranked_results(collected, safe_limit)
    sources: dict[str, int] = {}
    for result in results:
        system = str(result.get("source_system") or "unknown")
        sources[system] = sources.get(system, 0) + 1
    return {
        "query": cleaned,
        "corpus": resolved,
        "results": results,
        "count": len(results),
        "sources": sources,
    }


@mcp.tool()
def get_document(doc_id: str, offset: int = 0, max_chars: int = 20000) -> dict[str, Any]:
    """Read an exact slice of a document, with pagination for long documents.

    Legal acts (PRG.ZANGER) use their bare doc_id. Judicial decisions (PRG.SOT)
    use the namespaced key returned by search, e.g. 'prg_sot:35502996'. The
    response always carries source_system and corpus_type.
    """
    key = doc_id.strip()
    safe_offset = max(0, offset)
    safe_limit = max(1000, min(max_chars, settings.max_document_chars))

    if key.startswith(SOT_ID_PREFIX):
        decision = database.load_sot_decision(key)
        if decision is None:
            return {
                "found": False,
                "doc_id": key,
                "source_system": SOURCE_SYSTEM_JUDICIAL,
                "corpus_type": CORPUS_JUDICIAL_DECISION,
            }
        end = min(len(decision.text), safe_offset + safe_limit)
        return {
            "found": True,
            "doc_id": decision.decision_key,
            "decision_id": decision.decision_id,
            "title": decision.title,
            "source_url": decision.source_url,
            "source_system": SOURCE_SYSTEM_JUDICIAL,
            "corpus_type": CORPUS_JUDICIAL_DECISION,
            "case_number": decision.case_number,
            "court": decision.court,
            "judge": decision.judge,
            "region": decision.region,
            "instance": decision.instance,
            "proceeding_type": decision.proceeding_type,
            "decision_date": decision.decision_date,
            "parties": decision.parties,
            "offset": safe_offset,
            "next_offset": end if end < len(decision.text) else None,
            "total_chars": len(decision.text),
            "content": decision.text[safe_offset:end],
            "metadata": decision.metadata,
        }

    document = database.load_document(key)
    if document is None:
        return {
            "found": False,
            "doc_id": key,
            "source_system": SOURCE_SYSTEM_LEGAL,
            "corpus_type": CORPUS_LEGAL_ACT,
        }
    end = min(len(document.text), safe_offset + safe_limit)
    return {
        "found": True,
        "doc_id": document.doc_id,
        "title": document.title,
        "source_url": document.source_url
        or f"https://prg.kz/lawyer/document/?doc_id={document.doc_id}",
        "source_system": SOURCE_SYSTEM_LEGAL,
        "corpus_type": CORPUS_LEGAL_ACT,
        "pages": document.pages,
        "offset": safe_offset,
        "next_offset": end if end < len(document.text) else None,
        "total_chars": len(document.text),
        "content": document.text[safe_offset:end],
        "metadata": document.metadata,
    }


@mcp.tool()
def get_related_documents(doc_id: str, limit: int = 20) -> dict[str, Any]:
    """Return documents referenced by a selected legal document.

    Legal acts carry the document_links graph of PRG.ZANGER. Judicial decisions
    have no stored link graph; a prg_sot: key returns an empty related list with
    its provenance instead of failing, so callers stay source-aware.
    """
    key = doc_id.strip()
    safe_limit = max(1, min(limit, 100))
    if key.startswith(SOT_ID_PREFIX):
        return {
            "doc_id": key,
            "related": [],
            "count": 0,
            "source_system": SOURCE_SYSTEM_JUDICIAL,
            "corpus_type": CORPUS_JUDICIAL_DECISION,
            "supported": False,
        }
    related = database.related_documents(key, safe_limit)
    return {
        "doc_id": key,
        "related": related,
        "count": len(related),
        "source_system": SOURCE_SYSTEM_LEGAL,
        "corpus_type": CORPUS_LEGAL_ACT,
        "supported": True,
    }


@mcp.tool()
def get_collection_status() -> dict[str, Any]:
    """Return both corpora: collection rows, indexing queues, Elasticsearch health and indexed chunk counts.

    The legal keys (documents, index_jobs, indexed_chunks) keep their original
    meaning; the judicial corpus reports under sot_documents, sot_index_jobs and
    sot_indexed_chunks.
    """
    database.ensure_schema()
    legal_chunks = legal_search.count()
    sot_chunks = sot_search.count()
    return {
        "documents": database.collection_stats(),
        "index_jobs": database.job_stats(),
        "indexed_chunks": legal_chunks,
        "sot_documents": database.sot_collection_stats(),
        "sot_index_jobs": database.sot_job_stats(),
        "sot_indexed_chunks": sot_chunks,
        "indices": {
            settings.elasticsearch_index: legal_chunks,
            settings.sot_elasticsearch_index: sot_chunks,
        },
        "elasticsearch": legal_search.health(),
    }


async def health(_request) -> JSONResponse:
    try:
        cluster = legal_search.health(timeout=3.0)
        return JSONResponse({"ok": True, "elasticsearch": cluster.get("status", "unknown")})
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=503)


class ApiKeyMiddleware:
    def __init__(self, wrapped_app, api_key: str) -> None:
        self.wrapped_app = wrapped_app
        self.api_key = api_key

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") == "http" and scope.get("path") != "/health" and self.api_key:
            headers = {key.lower(): value for key, value in scope.get("headers", [])}
            supplied = headers.get(b"authorization", b"").decode("latin-1")
            expected = f"Bearer {self.api_key}"
            if not hmac.compare_digest(supplied, expected):
                response = JSONResponse({"error": "unauthorized"}, status_code=401)
                await response(scope, receive, send)
                return
        await self.wrapped_app(scope, receive, send)


database.ensure_schema()
mcp_app = mcp.streamable_http_app()
mcp_app.routes.append(Route("/health", health))
app = ApiKeyMiddleware(mcp_app, settings.mcp_api_key)


def run_mcp() -> None:
    uvicorn.run(app, host="0.0.0.0", port=settings.port, proxy_headers=True)
