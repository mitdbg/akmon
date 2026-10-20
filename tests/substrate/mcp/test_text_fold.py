"""The fold behind `like` and `like_exact`, without a database (PACT
loose-match-3666). The SQL form is compared with this one on every code
point in test_loose_match_db.py.
"""
from __future__ import annotations

import unicodedata

import pytest

from okg.substrate.mcp import text_fold
from okg.substrate.mcp.operator_sql import declared_bound_mismatch, effective_attr_lowerer


def test_the_committed_tables_are_the_generators_output():
    """The tables both folds read are committed; a Python with other Unicode
    data fails here, naming its version, before any read answers with them."""
    ranges, replacements = text_fold.generate_tables()
    assert unicodedata.unidata_version == text_fold.TABLES_UNICODE
    assert ranges == text_fold.MARK_RANGES
    assert replacements == text_fold.CASEFOLD_REPLACEMENTS
    assert len(text_fold.CASEFOLD_REPLACEMENTS) == 11
    assert len(text_fold.FIXED_TABLE) == 6


@pytest.mark.parametrize("text,folded", [
    ("Müller", "muller"),
    ("MULLER", "muller"),
    ("Müller", "muller"),
    ("Straße", "strasse"),
    ("STRAẞE", "strasse"),
    ("ΟΔΟΣ", "οδοσ"),
    ("οδος", "οδοσ"),
    ("ﬁle", "file"),
    ("x²", "x2"),
    ("ＡＢ", "ab"),
    ("Øster Łódź Đoàn", "oster lodz doan"),
    ("Æsir Œuvre Þór", "aesir oeuvre thor"),
    ("İstanbul", "istanbul"),
    ("ıI", "ıi"),
    ("Ꭰꭰ", "ꭰꭰ"),
    ("ᲀ", "в"),
])
def test_the_fold(text, folded):
    assert text_fold.fold(text) == folded


def test_dotless_i_stays_apart_from_i():
    assert text_fold.fold("ı") != text_fold.fold("i")
    assert not text_fold.matches("like", "Kırşehir", "kir")


def test_like_and_like_exact_read_strings_and_lists():
    assert text_fold.matches("like", "Mueller-Muller", "MÜLLER")
    assert not text_fold.matches("like", "Miller", "muller")
    assert text_fold.matches("like", ["x", 3, "Müllerei"], "mullerei")
    assert not text_fold.matches("like", 42, "4")
    assert not text_fold.matches("like", None, "a")
    assert text_fold.matches("like_exact", "Müller", "Müller")
    assert not text_fold.matches("like_exact", "MULLER", "Müller")
    assert not text_fold.matches("like_exact", "Muller", "Müller")


@pytest.mark.parametrize("operand,problem", [
    (3, "takes one string"), ("a\x00b", "NUL"), ("a%b", "wildcard"),
    ("", "1 to 200"), ("x" * 201, "1 to 200"), ("́", "accent"),
])
def test_an_evaluator_refuses_an_operand_it_cannot_compare(operand, problem):
    assert problem in text_fold.operand_problem("like", operand)


def _row(version: int, encoding: str = "UTF8", collation: bool = True,
         unicode: str | None = "15.1") -> dict:
    return {"version": version, "encoding": encoding, "collation": collation,
            "unicode": unicode}


def test_a_database_that_cannot_fold_is_named():
    assert text_fold.database_problem(_row(170002)) is None
    assert text_fold.database_problem(_row(170002, unicode="15.0")) is None
    old = text_fold.database_problem(_row(160004, collation=False, unicode=None))
    assert old is not None and "PostgreSQL 17" in old[0]
    assert old[1]["reason"] == "loose_match_needs_postgresql_17"
    latin = text_fold.database_problem(_row(170002, encoding="LATIN1"))
    assert latin is not None and latin[1]["reason"] == "loose_match_needs_utf8"
    gap = text_fold.database_problem(_row(180000, unicode="16.0"))
    assert gap is not None and gap[1]["reason"] == "loose_match_unicode_gap"
    assert "16.0" in gap[0]


def test_the_check_asks_unicode_version_only_where_it_exists():
    asked: list[str] = []

    def execute(sql: str) -> dict:
        asked.append(sql)
        if sql == text_fold.UNICODE_VERSION_SQL:
            raise AssertionError("unicode_version() does not exist below 17")
        return {"version": 160004, "encoding": "UTF8", "collation": False}

    problem = text_fold.check_database(execute)
    assert problem is not None and asked == [text_fold.DATABASE_CHECK_SQL]


def test_the_sql_fold_binds_the_mark_pattern_once():
    """Every like leaf of one statement reads one bound pattern."""
    lower = effective_attr_lowerer("nodes")
    params: dict = {}
    first = lower({"op": "like", "field": "attrs.a", "value": "x"}, params, "w")
    second = lower({"op": "like", "field": "node_id", "value": "y"}, params, "w")
    assert first is not None and second is not None
    patterns = [name for name, value in params.items() if value == text_fold.mark_pattern()]
    assert len(patterns) == 1
    assert f"%({patterns[0]})s" in first.sql and f"%({patterns[0]})s" in second.sql
    # A column folds once: 11 generated and 6 fixed replacements.
    folds = second.sql.count("replace(") - second.sql.count("regexp_replace(")
    assert "pg_c_utf8" in second.sql and folds == 17
    exact = lower({"op": "like_exact", "field": "attrs.a", "value": "x"}, {}, "w")
    assert exact is not None and "NFC" in exact.sql and "pg_c_utf8" not in exact.sql


@pytest.mark.parametrize("node", [
    {"op": "like", "field": "attrs", "value": "x"},
    {"op": "like", "field": "attrs.a", "value": "a%b"},
    {"op": "like", "field": "attrs.a", "value": 3},
    {"op": "like_exact", "field": "attrs.a", "value": ""},
])
def test_the_lowerer_gives_no_sql_for_a_test_it_cannot_compare(node):
    """No lowering, so the read is refused, never served as no rows."""
    assert effective_attr_lowerer("nodes")(node, {}, "w") is None


def test_like_on_a_number_or_boolean_attribute_is_refused_by_name():
    declared = {"n": "num", "b": "bool", "t": "text"}
    lower = effective_attr_lowerer("nodes", declared=declared)
    assert lower({"op": "like", "field": "attrs.n", "value": "4"}, {}, "w") is None
    assert lower({"op": "like", "field": "attrs.t", "value": "4"}, {}, "w") is not None
    message, hint, fixed = declared_bound_mismatch(
        {"op": "like", "field": "attrs.b", "value": "t"}, declared,
    )
    assert "declared bool, which holds no text" in message
    assert fixed is None and "eq" in hint


def test_like_leaves_name_what_each_test_compared():
    where = {"op": "and", "clauses": [
        {"op": "like", "field": "attrs.a", "value": "Müller"},
        {"op": "not", "clause": {"op": "like_exact", "field": "node_id", "value": "Mü"}},
        {"op": "eq", "field": "attrs.like", "value": "like"},
    ]}
    assert text_fold.like_leaves(where) == [
        {"field": "attrs.a", "op": "like", "operand": "Müller", "folded": "muller"},
        {"field": "node_id", "op": "like_exact", "operand": "Mü", "folded": "Mü"},
    ]
    assert text_fold.holds_like_clause(None, {"kind": "nodes", "filter": where})
    assert not text_fold.holds_like_clause({"op": "eq", "field": "attrs.like", "value": "like"})
