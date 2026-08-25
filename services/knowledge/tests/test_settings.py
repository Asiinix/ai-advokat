"""Focused coverage for the knowledge-service environment contract."""

import os
from unittest import mock

import pytest

from knowledge_service.corpus import CORPUS_ALL, CORPUS_JUDICIAL_DECISION, CORPUS_LEGAL_ACT
from knowledge_service.settings import Settings

BASE_ENV = {
    "DATABASE_URL": "postgresql://user:pass@db/app",
    "ELASTICSEARCH_URL": "http://es.test:9200",
}


def settings_for(**overrides):
    env = dict(BASE_ENV)
    env.update(overrides)
    with mock.patch.dict(os.environ, env, clear=True):
        return Settings.from_env()


def test_defaults_index_legal_only_and_search_all():
    settings = settings_for()
    assert settings.indexer_corpus == CORPUS_LEGAL_ACT
    assert settings.mcp_default_corpus == CORPUS_ALL
    assert settings.indexes_legal is True
    assert settings.indexes_judicial is False


def test_mcp_default_can_be_pinned_to_one_corpus_for_compatibility():
    assert settings_for(MCP_DEFAULT_CORPUS="legal_act").mcp_default_corpus == CORPUS_LEGAL_ACT
    assert (
        settings_for(MCP_DEFAULT_CORPUS="judicial_decision").mcp_default_corpus
        == CORPUS_JUDICIAL_DECISION
    )


def test_knowledge_corpus_switches_which_job_tables_the_indexer_uses():
    both = settings_for(KNOWLEDGE_CORPUS="all")
    assert both.indexes_legal is True
    assert both.indexes_judicial is True
    judicial = settings_for(KNOWLEDGE_CORPUS="judicial_decision")
    assert judicial.indexes_legal is False
    assert judicial.indexes_judicial is True


def test_index_names_must_be_distinct_and_nonempty():
    with pytest.raises(RuntimeError) as ctx:
        settings_for(ELASTICSEARCH_INDEX="same", SOT_ELASTICSEARCH_INDEX="same")
    assert "separate" in str(ctx.value)
    with pytest.raises(RuntimeError):
        settings_for(ELASTICSEARCH_INDEX="   ")
    with pytest.raises(RuntimeError):
        settings_for(SOT_ELASTICSEARCH_INDEX="")


def test_unknown_corpus_configuration_is_rejected():
    with pytest.raises(RuntimeError):
        settings_for(MCP_DEFAULT_CORPUS="cases")
    with pytest.raises(RuntimeError):
        settings_for(KNOWLEDGE_CORPUS="everything")
