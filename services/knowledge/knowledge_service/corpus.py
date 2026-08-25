"""Corpus identity for the search layer.

Two collections share one Elasticsearch cluster and one Postgres database but
nothing else: legal acts from PRG.ZANGER and judicial decisions from PRG.SOT.
They live in separate indices with separate id spaces, so a query can target one
or both without either corpus polluting the other's ranking or ids.

This module deliberately has no third-party imports: the id, result shaping and
merging rules are the part most likely to regress, and they must stay
unit-testable.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

CORPUS_LEGAL_ACT = "legal_act"
CORPUS_JUDICIAL_DECISION = "judicial_decision"
CORPUS_ALL = "all"
CORPUS_CHOICES = (CORPUS_LEGAL_ACT, CORPUS_JUDICIAL_DECISION, CORPUS_ALL)

SOURCE_SYSTEM_LEGAL = "prg_zanger"
SOURCE_SYSTEM_JUDICIAL = "prg_sot"

# Legacy chunk ids stay `<doc_id>:<n>` so the 392k already-indexed documents are
# never rewritten. Judicial chunk ids keep the `prg_sot:` namespace the parser
# already puts on every decision key, so the two id spaces can share a cluster
# (and a log line) without ambiguity.
SOT_ID_PREFIX = "prg_sot:"

LEGAL_SOURCE_URL_TEMPLATE = "https://prg.kz/lawyer/document/?doc_id={doc_id}"

DEFAULT_LEGAL_INDEX = "ai_advokat_chunks_v1"
DEFAULT_SOT_INDEX = "ai_advokat_sot_chunks_v1"


class UnknownCorpusError(ValueError):
    """Raised for a corpus filter the search layer does not serve."""


def normalize_corpus(value: str | None, default: str = CORPUS_ALL) -> str:
    """Accept the documented filter values only; never silently widen a query."""
    if value is None or str(value).strip() == "":
        return default
    cleaned = str(value).strip().lower()
    if cleaned not in CORPUS_CHOICES:
        raise UnknownCorpusError(
            f"Unknown corpus '{value}'. Use one of: {', '.join(CORPUS_CHOICES)}."
        )
    return cleaned


def legal_chunk_id(doc_id: str, chunk_number: int) -> str:
    return f"{doc_id}:{chunk_number}"


def sot_chunk_id(decision_key: str, chunk_number: int) -> str:
    key = str(decision_key)
    if not key.startswith(SOT_ID_PREFIX):
        key = f"{SOT_ID_PREFIX}{key}"
    return f"{key}:{chunk_number}"


def provenance_for_corpus(corpus: str) -> tuple[str, str]:
    """(source_system, corpus_type) for one corpus name."""
    if corpus == CORPUS_JUDICIAL_DECISION:
        return SOURCE_SYSTEM_JUDICIAL, CORPUS_JUDICIAL_DECISION
    return SOURCE_SYSTEM_LEGAL, CORPUS_LEGAL_ACT


def indices_for_corpus(corpus: str, legal_index: str, sot_index: str) -> list[str]:
    if corpus == CORPUS_LEGAL_ACT:
        return [legal_index]
    if corpus == CORPUS_JUDICIAL_DECISION:
        return [sot_index]
    return [legal_index, sot_index]


def corpus_for_index(index_name: str, legal_index: str, sot_index: str) -> str:
    """Which corpus a hit came from.

    Resolved from the index instead of a stored field: the legacy index has a
    strict mapping and must not be rewritten just to carry a constant.
    """
    if index_name == sot_index:
        return CORPUS_JUDICIAL_DECISION
    return CORPUS_LEGAL_ACT


LEGAL_SOURCE_FIELDS = (
    "doc_id",
    "title",
    "heading",
    "content",
    "chunk_number",
    "char_start",
    "char_end",
    "source_url",
    "pages",
)

SOT_SOURCE_FIELDS = (
    "doc_id",
    "decision_id",
    "title",
    "heading",
    "content",
    "chunk_number",
    "char_start",
    "char_end",
    "source_url",
    "case_number",
    "court",
    "judge",
    "region",
    "instance",
    "proceeding_type",
    "decision_date",
    "parties",
    "metadata",
)

JUDICIAL_RESULT_FIELDS = (
    "decision_id",
    "case_number",
    "court",
    "judge",
    "region",
    "instance",
    "proceeding_type",
    "decision_date",
    "parties",
)


def build_result(hit: Mapping[str, Any], corpus: str, excerpt_chars: int = 900) -> dict[str, Any]:
    """Shape one Elasticsearch hit into a corpus-tagged search result.

    ``source_system`` and ``corpus_type`` are present on every result, for both
    corpora, so a caller can always cite where a passage came from.
    """
    source = dict(hit.get("_source") or {})
    fragments = (hit.get("highlight") or {}).get("content") or []
    excerpt = " … ".join(fragments) if fragments else str(source.get("content", ""))[:excerpt_chars]
    source_system, corpus_type = provenance_for_corpus(corpus)
    doc_id = str(source.get("doc_id", ""))
    result: dict[str, Any] = {
        "doc_id": doc_id,
        "title": source.get("title", ""),
        "heading": source.get("heading", ""),
        "excerpt": excerpt,
        "score": hit.get("_score"),
        "chunk_number": source.get("chunk_number"),
        "char_start": source.get("char_start"),
        "char_end": source.get("char_end"),
        "source_url": source.get("source_url", "") or default_source_url(doc_id, corpus),
        "source_system": source_system,
        "corpus_type": corpus_type,
    }
    if corpus == CORPUS_JUDICIAL_DECISION:
        for name in JUDICIAL_RESULT_FIELDS:
            result[name] = source.get(name, "")
    else:
        result["pages"] = source.get("pages")
    return result


def default_source_url(doc_id: str, corpus: str) -> str:
    if corpus == CORPUS_JUDICIAL_DECISION:
        return ""
    if not doc_id:
        return ""
    return LEGAL_SOURCE_URL_TEMPLATE.format(doc_id=doc_id)


def merge_ranked_results(
    results_by_corpus: Iterable[tuple[str, Iterable[Mapping[str, Any]]]],
    limit: int,
) -> list[dict[str, Any]]:
    """Merge per-index result lists into one deterministic ranking.

    BM25 scores are not comparable across indices with different document sets,
    so each hit is first normalised against the best score its own index
    returned for this query and only then interleaved with the other corpus.
    Ties fall back to the raw score and then to the doc_id, which keeps the
    order stable for tests and for repeated queries.
    """
    safe_limit = max(0, int(limit))
    ranked: list[tuple[tuple[float, float, str], dict[str, Any]]] = []
    for corpus_name, results in results_by_corpus:
        items = [dict(item) for item in results]
        top_score = max(
            (
                float(score)
                for score in (item.get("score") for item in items)
                if isinstance(score, (int, float))
            ),
            default=None,
        )
        for item in items:
            raw = item.get("score")
            raw_number = float(raw) if isinstance(raw, (int, float)) else float("-inf")
            normalized = raw_number / float(top_score) if top_score and raw_number > float("-inf") else 0.0
            merged = dict(item)
            merged["corpus"] = corpus_name
            merged["rank_score"] = round(normalized, 6)
            key = (round(normalized, 6), raw_number, str(item.get("doc_id") or ""))
            ranked.append((key, merged))
    ranked.sort(key=lambda pair: pair[0], reverse=True)
    return [item for _, item in ranked[:safe_limit]]
