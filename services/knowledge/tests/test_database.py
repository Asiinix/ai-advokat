"""Focused coverage for the two physically separate index-job tables.

No live Postgres is needed: a fake connection records the SQL each method
sends, and the tests pin that the legal queue never mentions sot_* relations
and the judicial queue never mentions documents/document_outputs.
"""

from unittest import mock

from knowledge_service import database as database_module
from knowledge_service.database import KnowledgeDatabase, SotIndexJob


class FakeCursor:
    def __init__(self):
        self.statements: list[str] = []
        self.params: list = []
        self.fetchone_results: list = []
        self.fetchall_results: list = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.statements.append(sql)
        self.params.append(params)

    def fetchone(self):
        return self.fetchone_results.pop(0) if self.fetchone_results else None

    def fetchall(self):
        return self.fetchall_results.pop(0) if self.fetchall_results else []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def patch_database(cursor):
    connection = FakeConnection(cursor)
    patcher = mock.patch.object(database_module.psycopg, "connect", return_value=connection)
    patcher.start()
    return patcher


def statements_for(method_name, *args):
    cursor = FakeCursor()
    patcher = patch_database(cursor)
    try:
        getattr(KnowledgeDatabase("postgresql://x"), method_name)(*args)
    finally:
        patcher.stop()
    return cursor


def test_ensure_schema_creates_two_job_tables_and_never_rewrites_legal_rows():
    cursor = FakeCursor()
    cursor.fetchone_results = ["sot_decisions"]
    patcher = patch_database(cursor)
    try:
        KnowledgeDatabase("postgresql://x").ensure_schema()
    finally:
        patcher.stop()

    create_tables = [
        sql for sql in cursor.statements if sql.lstrip().upper().startswith("CREATE TABLE")
    ]
    joined = " ".join(create_tables)
    assert "search_index_jobs" in joined
    assert "sot_search_index_jobs" in joined
    # The legal table keeps its historical definition, including its FK, so
    # the existing rows are untouched by any migration.
    assert "REFERENCES documents(doc_id)" in joined
    assert "REFERENCES sot_decisions(decision_key)" in joined


def test_ensure_schema_tolerates_a_database_without_sot_tables_yet():
    cursor = FakeCursor()
    cursor.fetchone_results = [None]
    patcher = patch_database(cursor)
    try:
        KnowledgeDatabase("postgresql://x").ensure_schema()
    finally:
        patcher.stop()

    sot_create = next(
        sql
        for sql in cursor.statements
        if "CREATE TABLE" in sql and "sot_search_index_jobs" in sql
    )
    assert "REFERENCES sot_decisions" not in sot_create


LEGAL_METHODS = [
    ("seed_jobs", (5,)),
    ("requeue_stale", (1800,)),
    ("requeue_failed", ()),
    ("claim_jobs", ("worker", 5)),
    ("load_document", ("123",)),
    ("document_output_formats", ("123",)),
    ("mark_indexed", ("123", 3)),
    ("mark_failed", ("123", "boom")),
    ("job_stats", ()),
    ("collection_stats", ()),
]

JUDICIAL_METHODS = [
    ("seed_sot_jobs", (5,)),
    ("requeue_sot_stale", (1800,)),
    ("requeue_sot_failed", ()),
    ("claim_sot_jobs", ("worker", 5)),
    ("load_sot_decision", ("prg_sot:9",)),
    ("sot_output_formats", ("prg_sot:9",)),
    ("mark_sot_indexed", ("prg_sot:9", 3)),
    ("mark_sot_failed", ("prg_sot:9", "boom")),
    ("sot_job_stats", ()),
    ("sot_collection_stats", ()),
]


def test_legal_job_sql_never_touches_the_judicial_tables():
    for method_name, args in LEGAL_METHODS:
        cursor = statements_for(method_name, *args)
        joined = "\n".join(cursor.statements)
        assert "sot_decisions" not in joined, method_name
        assert "sot_search_index_jobs" not in joined, method_name
        assert "sot_decision_outputs" not in joined, method_name


def test_judicial_job_sql_never_touches_the_legal_tables():
    for method_name, args in JUDICIAL_METHODS:
        cursor = statements_for(method_name, *args)
        joined = "\n".join(cursor.statements)
        # "sot_search_index_jobs" contains "search_index_jobs" as a substring,
        # so compare against the SQL with the judicial table name removed.
        without_sot_jobs = joined.replace("sot_search_index_jobs", "")
        assert "search_index_jobs" not in without_sot_jobs, method_name
        assert "documents" not in joined, method_name
        assert "document_outputs" not in joined, method_name


def test_claim_sot_jobs_returns_namespaced_keys():
    cursor = FakeCursor()
    cursor.fetchall_results = [[("prg_sot:9", "hash-9")]]
    patcher = patch_database(cursor)
    try:
        jobs = KnowledgeDatabase("postgresql://x").claim_sot_jobs("worker", 1)
    finally:
        patcher.stop()

    assert jobs == [SotIndexJob(decision_key="prg_sot:9", source_sha256="hash-9")]
    joined = "\n".join(cursor.statements)
    assert "sot_search_index_jobs" in joined
    assert "FOR UPDATE SKIP LOCKED" in joined


def test_load_sot_decision_exposes_court_metadata_and_parties():
    row = (
        "prg_sot:9",
        "9",
        "Решение по делу",
        "https://sb.prg.kz/decision/9",
        "2-9/2026",
        "Межрайонный суд",
        "Иванова",
        "Алматы",
        "первая инстанция",
        "гражданское",
        "2026-03-14",
        [{"role": "истец", "name": "ТОО Альфа"}],
        {"case_number": "2-9/2026", "extra": 1},
        "2026-01-01T00:00:00+00:00",
        "Текст судебного акта.".encode("utf-8"),
        "utf-8",
        "hash-9",
    )
    cursor = FakeCursor()
    cursor.fetchone_results = [row]
    patcher = patch_database(cursor)
    try:
        payload = KnowledgeDatabase("postgresql://x").load_sot_decision("prg_sot:9")
    finally:
        patcher.stop()

    assert payload is not None
    assert payload.decision_key == "prg_sot:9"
    assert payload.decision_id == "9"
    assert payload.case_number == "2-9/2026"
    assert payload.court == "Межрайонный суд"
    assert payload.decision_date == "2026-03-14"
    assert payload.parties[0]["name"] == "ТОО Альфа"
    assert payload.metadata["extra"] == 1
    assert payload.metadata["case_number"] == "2-9/2026"
    assert payload.text == "Текст судебного акта."
    assert payload.source_sha256 == "hash-9"


def test_load_sot_decision_merges_columns_into_empty_metadata():
    row = (
        "prg_sot:1",
        "1",
        "",
        "",
        "2-1/2026",
        "Суд",
        "",
        "",
        "",
        "",
        "2026-03-14",
        None,
        None,
        "2026-01-01T00:00:00+00:00",
        "текст".encode("utf-8"),
        "utf-8",
        "hash-1",
    )
    cursor = FakeCursor()
    cursor.fetchone_results = [row]
    patcher = patch_database(cursor)
    try:
        payload = KnowledgeDatabase("postgresql://x").load_sot_decision("prg_sot:1")
    finally:
        patcher.stop()

    assert payload.metadata == {"case_number": "2-1/2026", "court": "Суд", "decision_date": "2026-03-14"}
    assert payload.parties is None


def test_output_format_queries_stay_inside_their_corpus():
    legal_cursor = FakeCursor()
    legal_cursor.fetchall_results = [[("html",), ("txt",)]]
    legal_patcher = patch_database(legal_cursor)
    try:
        legal = KnowledgeDatabase("postgresql://x").document_output_formats("123")
    finally:
        legal_patcher.stop()

    sot_cursor = FakeCursor()
    sot_cursor.fetchall_results = [[("json",), ("txt",)]]
    sot_patcher = patch_database(sot_cursor)
    try:
        judicial = KnowledgeDatabase("postgresql://x").sot_output_formats("prg_sot:9")
    finally:
        sot_patcher.stop()

    assert legal == ["html", "txt"]
    assert judicial == ["json", "txt"]
