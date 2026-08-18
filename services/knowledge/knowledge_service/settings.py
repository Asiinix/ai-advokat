from __future__ import annotations

import os
from dataclasses import dataclass


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _int_env(name: str, default: int, minimum: int = 0) -> int:
    raw = os.environ.get(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    if value < minimum:
        raise RuntimeError(f"{name} must be >= {minimum}")
    return value


def _float_env(name: str, default: float, minimum: float = 0) -> float:
    raw = os.environ.get(name, str(default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a number") from exc
    if value < minimum:
        raise RuntimeError(f"{name} must be >= {minimum}")
    return value


@dataclass(frozen=True)
class Settings:
    database_url: str
    elasticsearch_url: str
    elasticsearch_index: str
    mode: str
    port: int
    mcp_api_key: str
    mcp_allowed_hosts: tuple[str, ...]
    mcp_allowed_origins: tuple[str, ...]
    indexer_seed_batch_size: int
    indexer_claim_batch_size: int
    indexer_poll_seconds: float
    indexer_lease_seconds: int
    chunk_max_chars: int
    chunk_overlap_chars: int
    max_document_chars: int

    @classmethod
    def from_env(cls) -> "Settings":
        mode = os.environ.get("KNOWLEDGE_MODE", "mcp").strip().lower()
        if mode not in {"indexer", "mcp"}:
            raise RuntimeError("KNOWLEDGE_MODE must be 'indexer' or 'mcp'")
        max_chars = _int_env("CHUNK_MAX_CHARS", 3500, minimum=500)
        overlap = _int_env("CHUNK_OVERLAP_CHARS", 350, minimum=0)
        if overlap >= max_chars:
            raise RuntimeError("CHUNK_OVERLAP_CHARS must be smaller than CHUNK_MAX_CHARS")
        return cls(
            database_url=_required("DATABASE_URL"),
            elasticsearch_url=_required("ELASTICSEARCH_URL").rstrip("/"),
            elasticsearch_index=os.environ.get("ELASTICSEARCH_INDEX", "prg_chunks_v1").strip(),
            mode=mode,
            port=_int_env("PORT", 8000, minimum=1),
            mcp_api_key=os.environ.get("MCP_API_KEY", "").strip(),
            mcp_allowed_hosts=tuple(
                item.strip()
                for item in os.environ.get(
                    "MCP_ALLOWED_HOSTS",
                    "localhost:*,127.0.0.1:*,[::1]:*",
                ).split(",")
                if item.strip()
            ),
            mcp_allowed_origins=tuple(
                item.strip()
                for item in os.environ.get("MCP_ALLOWED_ORIGINS", "").split(",")
                if item.strip()
            ),
            indexer_seed_batch_size=_int_env("INDEXER_SEED_BATCH_SIZE", 5000, minimum=1),
            indexer_claim_batch_size=_int_env("INDEXER_CLAIM_BATCH_SIZE", 5, minimum=1),
            indexer_poll_seconds=_float_env("INDEXER_POLL_SECONDS", 5.0, minimum=0.1),
            indexer_lease_seconds=_int_env("INDEXER_LEASE_SECONDS", 1800, minimum=60),
            chunk_max_chars=max_chars,
            chunk_overlap_chars=overlap,
            max_document_chars=_int_env("MCP_MAX_DOCUMENT_CHARS", 50000, minimum=1000),
        )
