from __future__ import annotations

import hmac
from typing import Any

import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.responses import JSONResponse
from starlette.routing import Route

from .database import KnowledgeDatabase
from .elasticsearch import ElasticsearchStore
from .settings import Settings


settings = Settings.from_env()
database = KnowledgeDatabase(settings.database_url)
search = ElasticsearchStore(settings.elasticsearch_url, settings.elasticsearch_index)

mcp = FastMCP(
    "PRG Legal Knowledge",
    instructions=(
        "Search and read the PRG.ZANGER legal document collection. "
        "Use search_documents first, then get_document for exact source text. "
        "Always cite doc_id and title in answers."
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
def search_documents(query: str, limit: int = 5) -> dict[str, Any]:
    """Search legal documents and return the best matching passage from each document."""
    cleaned = query.strip()
    if not cleaned:
        raise ValueError("query must not be empty")
    return search.search(cleaned, max(1, min(limit, 20)))


@mcp.tool()
def get_document(doc_id: str, offset: int = 0, max_chars: int = 20000) -> dict[str, Any]:
    """Read an exact slice of a PRG document by doc_id, with pagination for long documents."""
    document = database.load_document(doc_id.strip())
    if document is None:
        return {"found": False, "doc_id": doc_id}
    safe_offset = max(0, offset)
    safe_limit = max(1000, min(max_chars, settings.max_document_chars))
    end = min(len(document.text), safe_offset + safe_limit)
    return {
        "found": True,
        "doc_id": document.doc_id,
        "title": document.title,
        "source_url": document.source_url
        or f"https://prg.kz/lawyer/document/?doc_id={document.doc_id}",
        "pages": document.pages,
        "offset": safe_offset,
        "next_offset": end if end < len(document.text) else None,
        "total_chars": len(document.text),
        "content": document.text[safe_offset:end],
        "metadata": document.metadata,
    }


@mcp.tool()
def get_related_documents(doc_id: str, limit: int = 20) -> dict[str, Any]:
    """Return documents referenced by the selected PRG document."""
    safe_limit = max(1, min(limit, 100))
    related = database.related_documents(doc_id.strip(), safe_limit)
    return {"doc_id": doc_id, "related": related, "count": len(related)}


@mcp.tool()
def get_collection_status() -> dict[str, Any]:
    """Return collection, indexing queue, Elasticsearch health, and indexed chunk counts."""
    database.ensure_schema()
    return {
        "documents": database.collection_stats(),
        "index_jobs": database.job_stats(),
        "indexed_chunks": search.count(),
        "elasticsearch": search.health(),
    }


async def health(_request) -> JSONResponse:
    try:
        cluster = search.health(timeout=3.0)
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
