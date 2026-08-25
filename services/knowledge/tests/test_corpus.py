"""Focused coverage for corpus identity, id namespaces and result merging.

Everything here is pure: no database and no Elasticsearch are involved, which
is exactly why the dual-corpus rules live in corpus.py.
"""

import pytest

from knowledge_service.corpus import (
    CORPUS_ALL,
    CORPUS_CHOICES,
    CORPUS_JUDICIAL_DECISION,
    CORPUS_LEGAL_ACT,
    DEFAULT_LEGAL_INDEX,
    DEFAULT_SOT_INDEX,
    SOURCE_SYSTEM_JUDICIAL,
    SOURCE_SYSTEM_LEGAL,
    UnknownCorpusError,
    build_result,
    corpus_for_index,
    indices_for_corpus,
    legal_chunk_id,
    merge_ranked_results,
    normalize_corpus,
    provenance_for_corpus,
    sot_chunk_id,
)


def test_normalize_corpus_defaults_to_all():
    assert normalize_corpus(None) == CORPUS_ALL
    assert normalize_corpus("") == CORPUS_ALL
    assert normalize_corpus("   ") == CORPUS_ALL
    assert normalize_corpus(None, default=CORPUS_LEGAL_ACT) == CORPUS_LEGAL_ACT


def test_normalize_corpus_accepts_documented_filters_only():
    assert normalize_corpus("Legal_Act") == CORPUS_LEGAL_ACT
    assert normalize_corpus(" JUDICIAL_DECISION ") == CORPUS_JUDICIAL_DECISION
    assert normalize_corpus("ALL") == CORPUS_ALL
    assert set(CORPUS_CHOICES) == {CORPUS_LEGAL_ACT, CORPUS_JUDICIAL_DECISION, CORPUS_ALL}
    with pytest.raises(UnknownCorpusError) as ctx:
        normalize_corpus("cases")
    assert CORPUS_LEGAL_ACT in str(ctx.value)


def test_corpus_filters_map_to_exactly_the_right_indices():
    legal = DEFAULT_LEGAL_INDEX
    sot = DEFAULT_SOT_INDEX
    assert indices_for_corpus(CORPUS_LEGAL_ACT, legal, sot) == [legal]
    assert indices_for_corpus(CORPUS_JUDICIAL_DECISION, legal, sot) == [sot]
    assert indices_for_corpus(CORPUS_ALL, legal, sot) == [legal, sot]
    assert corpus_for_index(legal, legal, sot) == CORPUS_LEGAL_ACT
    assert corpus_for_index(sot, legal, sot) == CORPUS_JUDICIAL_DECISION


def test_default_index_names_are_distinct():
    assert DEFAULT_LEGAL_INDEX != DEFAULT_SOT_INDEX


def test_chunk_ids_cannot_collide_across_corpora():
    # Legacy legal ids stay <doc_id>:<n> so the already-indexed chunks are
    # never rewritten; judicial ids carry the prg_sot: namespace.
    assert legal_chunk_id("35502996", 0) == "35502996:0"
    assert legal_chunk_id("1", 0) == "1:0"
    assert sot_chunk_id("35502996", 0) == "prg_sot:35502996:0"
    assert legal_chunk_id("35502996", 0) != sot_chunk_id("35502996", 0)
    # A decision key already carries the prefix; it must not be doubled.
    assert sot_chunk_id("prg_sot:7", 2) == "prg_sot:7:2"
    # A bare legal doc_id can never equal a judicial chunk id for the same
    # input: legal ids stay bare, judicial ids are always namespaced.
    assert legal_chunk_id("9", 0) != sot_chunk_id("9", 0)


def test_provenance_for_corpus_is_constant():
    assert provenance_for_corpus(CORPUS_LEGAL_ACT) == (SOURCE_SYSTEM_LEGAL, CORPUS_LEGAL_ACT)
    assert provenance_for_corpus(CORPUS_JUDICIAL_DECISION) == (
        SOURCE_SYSTEM_JUDICIAL,
        CORPUS_JUDICIAL_DECISION,
    )


def test_every_result_carries_source_system_and_corpus_type():
    legal = build_result(
        {
            "_score": 1.5,
            "_source": {
                "doc_id": "35502996",
                "title": "Закон",
                "content": "текст закона",
                "pages": 3,
            },
        },
        CORPUS_LEGAL_ACT,
    )
    assert legal["source_system"] == SOURCE_SYSTEM_LEGAL
    assert legal["corpus_type"] == CORPUS_LEGAL_ACT
    assert legal["pages"] == 3
    assert legal["source_url"].startswith("https://prg.kz/lawyer/document/")

    judicial = build_result(
        {
            "_score": 2.0,
            "_source": {
                "doc_id": "prg_sot:9",
                "decision_id": "9",
                "title": "Решение по делу",
                "content": "текст решения",
                "case_number": "2-9/2026",
                "court": "Межрайонный суд",
                "judge": "Иванова",
                "region": "Алматы",
                "instance": "первая инстанция",
                "proceeding_type": "гражданское",
                "decision_date": "2026-03-14",
                "parties": [{"role": "истец", "name": "ТОО Альфа"}],
            },
        },
        CORPUS_JUDICIAL_DECISION,
    )
    assert judicial["source_system"] == SOURCE_SYSTEM_JUDICIAL
    assert judicial["corpus_type"] == CORPUS_JUDICIAL_DECISION
    assert judicial["decision_id"] == "9"
    assert judicial["case_number"] == "2-9/2026"
    assert judicial["court"] == "Межрайонный суд"
    assert judicial["parties"][0]["name"] == "ТОО Альфа"


def _result(corpus: str, doc_id: str, score: float) -> dict:
    return {
        "doc_id": doc_id,
        "score": score,
        "source_system": provenance_for_corpus(corpus)[0],
        "corpus_type": corpus,
    }


def test_default_all_merge_normalises_per_index_and_interleaves():
    legal = [_result(CORPUS_LEGAL_ACT, "l1", 10.0), _result(CORPUS_LEGAL_ACT, "l2", 5.0)]
    judicial = [
        _result(CORPUS_JUDICIAL_DECISION, "j1", 1.0),
        _result(CORPUS_JUDICIAL_DECISION, "j2", 0.4),
    ]

    merged = merge_ranked_results(
        [(CORPUS_LEGAL_ACT, legal), (CORPUS_JUDICIAL_DECISION, judicial)], limit=10
    )

    # Raw scores across indices are incomparable; after normalisation each
    # index's best hit ranks above the other index's second best.
    assert [item["doc_id"] for item in merged] == ["l1", "j1", "l2", "j2"]
    assert all(item["source_system"] for item in merged)
    assert all(item["corpus_type"] for item in merged)
    assert merged[0]["rank_score"] == 1.0
    assert merged[2]["rank_score"] == 0.5
    assert merged[3]["rank_score"] == 0.4


def test_merge_respects_the_limit_and_orders_ties_deterministically():
    legal = [_result(CORPUS_LEGAL_ACT, "a", 5.0), _result(CORPUS_LEGAL_ACT, "b", 5.0)]
    judicial = [_result(CORPUS_JUDICIAL_DECISION, "c", 3.0)]

    merged = merge_ranked_results(
        [(CORPUS_LEGAL_ACT, legal), (CORPUS_JUDICIAL_DECISION, judicial)], limit=2
    )

    assert len(merged) == 2
    assert [item["doc_id"] for item in merged] == ["b", "a"]


def test_merge_handles_missing_scores_and_empty_lists():
    empty_judicial = []
    legal = [_result(CORPUS_LEGAL_ACT, "l1", None)]
    merged = merge_ranked_results(
        [(CORPUS_LEGAL_ACT, legal), (CORPUS_JUDICIAL_DECISION, empty_judicial)], limit=5
    )
    assert len(merged) == 1
    assert merged[0]["doc_id"] == "l1"
    assert merged[0]["corpus"] == CORPUS_LEGAL_ACT
    assert merge_ranked_results([], limit=5) == []
