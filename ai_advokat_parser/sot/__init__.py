"""PRG.SOT judicial corpus.

A second, physically separate ingestion pipeline that lives next to the
PRG.ZANGER legal-act parser. It shares only the HTTP client and the credential
redaction helpers; state, tables, keys and search indices are its own.
"""

from __future__ import annotations

SOURCE_SYSTEM = "prg_sot"
CORPUS_TYPE = "judicial_decision"
DECISION_KEY_PREFIX = f"{SOURCE_SYSTEM}:"


def decision_key(decision_id: str) -> str:
    """Namespace a source decision id so it can never collide with a doc_id."""
    cleaned = str(decision_id).strip()
    if not cleaned:
        raise ValueError("decision_id must not be empty")
    if cleaned.startswith(DECISION_KEY_PREFIX):
        return cleaned
    return f"{DECISION_KEY_PREFIX}{cleaned}"
