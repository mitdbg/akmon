"""Issue #3666: `contains` with text matches text inside one attribute.

`contains` with a string on `attrs.<key>` (and, on search's `where`, on the
catalog `path` and `title`) means "the stored text holds this text",
ignoring the case of the ASCII letters A-Z. `contains` with an object on
field `attrs` keeps its JSON-containment meaning. These pin, with no
database: the grammar each tool reads, the one SQL lowering filter, map,
aggregate, expand and search's lookup share (its text and parameters), and
the Python row checks that must agree with it. The rows the SQL selects on a
real publish are checked in test_operator_parity_db.py and
test_read_grammar_db.py.
"""
from __future__ import annotations

from typing import Any

import anyio
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp import search_where as sw
from okg.substrate.mcp.operator_models import refuse_substring_contains
from okg.substrate.mcp import operator_sql
from okg.substrate.mcp.operator_sql import (
    compile_read_predicate,
    effective_attr_lowerer,
    effective_field_map_identity,
)
from okg.substrate.mcp.read_grammar import rewrite_call

pytest.importorskip("mcp")

DEAD_DSN = "postgresql://postgres:okg@localhost:1/okg_probe"
#: The sentence every refusal of a substring used to carry.
OLD_REFUSAL = "a substring match is not supported"


def _text(field: str, value: Any) -> dict[str, Any]:
    return {"op": "contains", "field": field, "value": value}


def _compile(predicate: Any, *, readable=None):
    return compile_read_predicate(
        predicate,
        leaf_lowerer=effective_attr_lowerer("nodes", relation="rel", readable=readable),
        field_map_identity=effective_field_map_identity("nodes", None),
        prefix="pv",
    )


# -- the three measured call shapes, as each tool's grammar reads them -------


def test_the_shorthand_reaches_each_tool_as_one_long_form_clause() -> None:
    search = rewrite_call("search", {
        "query": "words", "limit": 20,
        "where": {"type": "ticket", "attrs.message": {"op": "contains", "value": "timeout"}},
    })
    assert search.arguments["where"]["clauses"][1] == _text("attrs.message", "timeout")
    listing = rewrite_call("filter", {"limit": 20, "where": {
        "type": "doc", "attrs.repo": "repo-a",
        "attrs.path": {"op": "contains", "value": "Widget"},
    }})
    assert listing.arguments["predicate"]["clauses"][1] == _text("attrs.path", "Widget")
    count = rewrite_call("aggregate", {
        "where": {"type": "doc", "attrs.path": {"op": "contains", "value": "cleanup"}},
        "by": ["attrs.path"], "metrics": ["count"],
    })
    assert count.arguments["input"]["filter"] == _text("attrs.path", "cleanup")
    for rewritten in (search, listing, count):
        assert not rewritten.adjusted


def test_search_where_takes_text_on_an_attribute_path_and_title() -> None:
    for field in ("attrs.message", "path", "title"):
        assert sw.normalize_where(_text(field, "Timeout")) == _text(field, "Timeout")
    # The object form is unchanged.
    whole = _text("attrs", {"status": "open"})
    assert sw.normalize_where(whole) == whole


@pytest.mark.parametrize("clause", [
    _text("attrs.message", ""),
    _text("attrs.message", "a\x00b"),
    _text("path", "a\ud800b"),
    _text("type", "comm"),
    _text("id", "doc"),
    _text("source", ""),
    _text("source", {"k": "v"}),
    _text("attrs", "text"),
    _text("attrs.message", {"k": "v"}),
    _text("attrs.message", 5),
    _text("date.created", "2026"),
])
def test_search_where_refuses_every_other_contains_by_name(clause) -> None:
    with pytest.raises(sw.WhereUnsupported) as refused:
        sw.normalize_where(clause)
    assert OLD_REFUSAL not in str(refused.value)


# -- the row check: case of A-Z ignored, nothing else -----------------------


@pytest.mark.parametrize(("stored", "needle", "holds"), [
    ("Raise the TIMEOUT limit", "timeout", True),
    ("src/Widget.java", "widget", True),
    ("src/widget.java", "WIDGET", True),
    ("100% sure_", "0% S", True),
    ("100 percent", "%", False),
    ("a_b", "_", True),
    ("ab", "_", False),
    ("back\\slash", "K\\S", True),
    ("über", "ÜBER", False),  # only A-Z fold; Ü stays Ü
    ("über", "über", True),
    (["timeout"], "timeout", True),  # a list matches by a text element
    (["a", 5, "Raise TIMEOUT"], "timeout", True),
    ([5, True, None, {"k": "timeout"}], "timeout", False),
    ([["timeout"]], "timeout", False),  # one level only
    ([], "timeout", False),
    ({"k": "timeout"}, "timeout", False),
    (True, "true", False),
    (5, "5", False),
    (None, "x", False),
])
def test_text_contains_folds_ascii_letters_only(stored, needle, holds) -> None:
    assert operator_sql.text_contains(stored, needle) is holds


def test_search_evaluates_text_three_valued() -> None:
    clause = sw.normalize_where(_text("attrs.message", "TIMEOUT"))
    assert sw.evaluate({"attrs": {"message": "raise timeout"}}, clause) is True
    assert sw.evaluate({"attrs": {"message": "raise the limit"}}, clause) is False
    assert sw.evaluate({"attrs": {}}, clause) is False
    negated = sw.normalize_where({"op": "not", "clause": _text("attrs.message", "timeout")})
    assert sw.evaluate({"attrs": {}}, negated) is True
    # A masked value is unknown, and stays unknown under `not`.
    masked = {"attrs": {"message": sw.MASK_TOKEN}}
    assert sw.evaluate(masked, clause) is None
    assert sw.evaluate(masked, negated) is None


def test_search_reads_path_and_title_through_the_catalog() -> None:
    context = sw.WhereContext(
        {"doc": {"title_from": "summary", "path_from": "file"}}, {"title", "path"},
    )
    row = {"node_id": "doc:1", "subtype": "doc",
           "attrs": {"summary": "  Quarterly Review\nsecond line", "file": "docs/Plan.md"}}
    assert sw.evaluate(row, sw.normalize_where(_text("title", "quarterly")), context) is True
    assert sw.evaluate(row, sw.normalize_where(_text("title", "second")), context) is False
    assert sw.evaluate(row, sw.normalize_where(_text("path", "PLAN.MD")), context) is True


def test_the_lookup_pushes_text_on_an_attribute_to_sql_exactly() -> None:
    clause = sw.normalize_where(_text("attrs.message", "timeout"))
    assert sw.lookup_sql_clauses(clause, None, masking=False, explicit_subtypes=False) == [clause]
    assert sw.lookup_sql_exact(clause, masking=False) is True
    # Under access rules nothing on an attribute is pushed.
    assert sw.lookup_sql_clauses(clause, None, masking=True, explicit_subtypes=False) == []


def test_the_lookup_narrows_path_and_title_by_the_stored_attribute() -> None:
    context = sw.WhereContext({"doc": {"title_from": "summary", "path_from": "file"}},
                              {"title", "path"})
    where = sw.normalize_where({"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "doc"},
        _text("path", "Plan"), _text("title", "review"),
    ]})
    pushed = sw.lookup_sql_clauses(where, context, masking=False, explicit_subtypes=True)
    assert _text("attrs.file", "Plan") in pushed
    assert _text("attrs.summary", "review") in pushed
    # A superset only: the read check still decides title and path.
    assert sw.lookup_sql_exact(where, masking=False) is False


# -- the one SQL lowering ----------------------------------------------------


def test_text_on_an_attribute_compiles_exactly_with_the_folded_text_bound() -> None:
    compiled = _compile(_text("attrs.path", "Dir/100%_a\\B"))
    assert compiled.qualification == "exact" and compiled.truth_exact
    assert "strpos(translate(" in compiled.sql
    assert "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'" in compiled.sql
    assert "jsonb_typeof" in compiled.sql
    # No pattern operator: `%`, `_` and `\` are plain text in a bound value.
    assert " LIKE " not in compiled.sql.upper()
    assert "dir/100%_a\\b" in compiled.params.values()
    assert "Dir/100%_a\\B" not in compiled.sql
    assert "path" in compiled.params.values()  # the key is bound too


def test_text_under_not_and_or_stays_exact() -> None:
    tree = {"op": "and", "clauses": [
        {"op": "eq", "field": "subtype", "value": "doc"},
        {"op": "not", "clause": _text("attrs.path", "test")},
        {"op": "or", "clauses": [_text("attrs.repo", "a"), _text("attrs.repo", "b")]},
    ]}
    compiled = _compile(tree)
    assert compiled.qualification == "exact" and compiled.truth_exact


def test_a_nul_operand_or_key_is_decided_false_and_never_bound() -> None:
    compiled = _compile(_text("attrs.path", "a\x00b"))
    assert compiled.qualification == "exact"
    assert "FALSE" in compiled.sql
    assert not any("\x00" in str(v) for v in compiled.params.values())
    keyed = _compile(_text("attrs.pa\x00th", "a"))
    assert "FALSE" in keyed.sql


def test_under_a_reader_view_text_on_a_withheld_value_is_unknown() -> None:
    compiled = _compile(_text("attrs.path", "x"), readable=lambda key: f"readable_{key}")
    assert compiled.sql.count("CASE WHEN readable_path THEN") == 1


@pytest.mark.parametrize("shape", [
    _text("node_id", "doc"),
    _text("attrs", "doc"),
    _text("attrs.path", {"k": "v"}),
    _text("attrs.path", 5),
])
def test_every_other_contains_still_has_no_lowering(shape) -> None:
    assert _compile(shape).qualification == "superset"


# -- the guard every set read runs first -------------------------------------


def test_the_guard_passes_text_on_an_attribute_and_refuses_the_rest() -> None:
    refuse_substring_contains("filter", _text("attrs.path", "Widget"))
    refuse_substring_contains(
        "aggregate", {"kind": "nodes", "filter": _text("attrs.path", "cleanup")},
    )
    for shape in (_text("node_id", "doc"), _text("attrs", "doc"), _text("attrs.k", 1)):
        with pytest.raises(SubstrateError) as refused:
            refuse_substring_contains("filter", shape)
        assert refused.value.code == "unsupported_operator_form"
        assert OLD_REFUSAL not in str(refused.value)
        assert "text on one attribute" in str(refused.value)
    for bad in ("", "a\x00", "a\udc00"):
        with pytest.raises(SubstrateError) as refused:
            refuse_substring_contains("filter", _text("attrs.path", bad))
        assert refused.value.data["reason"] == "invalid_contains_text"


# -- through the server, with no database ------------------------------------


@pytest.fixture
def mcp_server(monkeypatch: pytest.MonkeyPatch):
    from okg.substrate.mcp.server import build_server

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    server = build_server(dsn=DEAD_DSN)
    try:
        yield server
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()


def _call_text(server: Any, name: str, arguments: dict[str, Any]) -> str:
    from tests.substrate.mcp._result_payload import result_text

    async def run() -> Any:
        return await server.call_tool(name, arguments)

    return result_text(anyio.run(run))


@pytest.mark.parametrize(("tool", "arguments"), [
    ("search", {"query": "words", "limit": 20, "where": {
        "type": "ticket", "attrs.message": {"op": "contains", "value": "timeout"}}}),
    ("filter", {"limit": 20, "where": {
        "type": "doc", "attrs.repo": "repo-a",
        "attrs.path": {"op": "contains", "value": "Widget"}}}),
    ("aggregate", {"where": {"type": "doc",
                             "attrs.path": {"op": "contains", "value": "cleanup"}},
                   "by": ["attrs.path"], "metrics": ["count"]}),
])
def test_the_measured_calls_are_no_longer_refused_before_the_read(
    mcp_server: Any, tool: str, arguments: dict[str, Any],
) -> None:
    """With no database the read itself fails; the condition is not refused."""
    text = _call_text(mcp_server, tool, arguments)
    assert OLD_REFUSAL not in text
    assert "JSON containment on attrs: field 'attrs'" not in text
    assert "where_unsupported" not in text
    assert "unsupported_operator_form" not in text


def test_the_python_row_check_agrees_with_text_contains(mcp_server: Any) -> None:
    matches = mcp_server._okg_operator_predicate_matches
    clause = _text("attrs.path", "WIDGET")
    assert matches({"attrs": {"path": "src/Widget.java"}}, clause) is True
    assert matches({"attrs": {"path": "src/other.java"}}, clause) is False
    assert matches({"attrs": {}}, clause) is False
    assert matches({"attrs": {}}, {"op": "not", "clause": clause}) is True
    assert matches({"attrs": {"path": ["Widget"]}}, clause) is True
    assert matches({"attrs": {"path": [5, "x"]}}, clause) is False
    assert matches({"attrs": {"path": 5}}, clause) is False


# -- `source`: the record's source id (issue #3666 follow-up) -----------------


def test_search_where_takes_text_on_source() -> None:
    clause = _text("source", "Repo-B")
    assert sw.normalize_where(clause) == clause
    assert sw.evaluate({"source": "lab.repo-b.docs"}, clause) is True
    assert sw.evaluate({"source": "lab.repo-c.docs"}, clause) is False
    assert sw.evaluate({"source": None}, clause) is False


def test_text_on_source_runs_exactly_in_the_ranked_sql() -> None:
    compiled = sw.compile_where(
        sw.normalize_where({"op": "and", "clauses": [
            {"op": "eq", "field": "type", "value": "doc"}, _text("source", "Repo-B"),
        ]}),
        columns={"id": "node_id", "type": "subtype", "source": "source"},
    )
    assert compiled.qualification == "exact" and compiled.truth_exact
    assert "strpos(translate(source, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'" in compiled.sql
    assert "repo-b" in compiled.params.values()
    nul = sw.compile_where(_text("source", "a\x00"), columns={"source": "source"})
    assert nul.qualification == "exact" and "FALSE" in nul.sql
    assert not nul.params


def test_text_on_source_never_claims_an_exact_lookup_count() -> None:
    # The query-less lookup's relation has no source column: the clause is
    # checked on read rows, so the lookup cannot count matches in SQL.
    clause = sw.normalize_where(_text("source", "repo"))
    assert sw.lookup_sql_exact(clause, masking=False) is False


def test_source_conditions_reads_top_level_eq_and_contains_only() -> None:
    from okg.substrate.mcp.source_hint import source_conditions

    where = sw.normalize_where({"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "doc"},
        {"op": "eq", "field": "source", "value": "repo-a"},
        _text("source", "Repo"),
        {"op": "not", "clause": {"op": "eq", "field": "source", "value": "x"}},
    ]})
    assert source_conditions(where) == [("eq", "repo-a"), ("contains", "Repo")]
    assert source_conditions(None) == []
    assert source_conditions(sw.normalize_where({"op": "eq", "field": "type", "value": "d"})) == []
    # The shorthand an agent writes, as search's grammar reads it.
    short = rewrite_call("search", {"query": "q", "where": {"source": "repo-a"}, "limit": 5})
    assert source_conditions(sw.normalize_where(short.arguments["where"])) == [("eq", "repo-a")]


NAMES = frozenset({"repo-a.docs", "repo-a.git", "lab.other", "repo-b"})


def test_source_hint_names_ids_holding_the_text() -> None:
    from okg.substrate.mcp.source_hint import source_hint

    (eq,) = source_hint([("eq", "epo-a")], NAMES)
    assert eq["source_ids"] == ["repo-a.docs", "repo-a.git"]
    assert eq["note"] == "no source id is exactly this text; write one of these, or use contains"
    (folded,) = source_hint([("contains", "REPO-A.D")], NAMES)
    assert folded == {"op": "contains", "sent": "REPO-A.D", "source_ids": ["repo-a.docs"]}
    (none,) = source_hint([("contains", "zzz")], NAMES)
    assert none["source_ids"] == []
    assert none["note"] == "no source id this server lists holds this text"
    (exact,) = source_hint([("eq", "lab.other")], NAMES)
    assert exact == {"op": "eq", "sent": "lab.other", "source_ids": ["lab.other"]}


def test_source_hint_is_bounded_and_says_how_many_more() -> None:
    from okg.substrate.mcp.source_hint import SOURCE_HINT_LIMIT, source_hint

    names = {f"lab.part-{i:02d}" for i in range(SOURCE_HINT_LIMIT + 3)}
    (entry,) = source_hint([("contains", "part")], names)
    assert len(entry["source_ids"]) == SOURCE_HINT_LIMIT and entry["more"] == 3


def test_source_hint_says_when_no_source_list_is_available() -> None:
    from okg.substrate.mcp.source_hint import source_hint

    (entry,) = source_hint([("eq", "x")], ())
    assert entry["source_ids"] is None
    assert entry["unavailable"] == "this server lists no source ids"
    assert source_hint([], NAMES) is None


def test_a_repository_name_is_read_as_its_source_ids_with_a_note() -> None:
    from okg.substrate.mcp.source_hint import RULE_SOURCE_REPO_AS_SOURCES, read_repo_names

    where = sw.normalize_where({"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "doc"},
        {"op": "eq", "field": "source", "value": "repo-a"},
    ]})
    read, notes = read_repo_names(where, NAMES)
    assert read == {"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "doc"},
        {"op": "in", "field": "source", "values": ["repo-a.docs", "repo-a.git"]},
    ]}
    assert notes == [{
        "rule": RULE_SOURCE_REPO_AS_SOURCES, "sent": "where source = `repo-a`",
        "served": "where source in [`repo-a.docs`, `repo-a.git`]",
    }]
    # The rewritten condition is one search serves.
    assert sw.normalize_where(read) == read
    alone, _ = read_repo_names(sw.normalize_where({"op": "eq", "field": "source",
                                                   "value": "repo-a"}), NAMES)
    assert alone == {"op": "in", "field": "source", "values": ["repo-a.docs", "repo-a.git"]}


@pytest.mark.parametrize("where", [
    {"op": "eq", "field": "source", "value": "repo-b"},  # an exact id stays exact
    {"op": "eq", "field": "source", "value": "lab.other"},
    {"op": "eq", "field": "source", "value": "repo"},  # not the part before a `.`
    {"op": "eq", "field": "source", "value": "repo-c"},  # no such repository
    {"op": "contains", "field": "source", "value": "repo-a"},
    {"op": "not", "clause": {"op": "eq", "field": "source", "value": "repo-a"}},
    {"op": "eq", "field": "type", "value": "repo-a"},
])
def test_anything_else_is_left_as_sent(where) -> None:
    from okg.substrate.mcp.source_hint import read_repo_names

    assert read_repo_names(where, NAMES) == (where, [])
    assert read_repo_names(None, NAMES) == (None, [])


def test_the_text_reply_prints_the_source_hint_and_the_contains_note() -> None:
    from okg.substrate.mcp.result_text import render
    from okg.substrate.mcp.source_hint import source_hint

    payload = {
        "status": "ok", "operator_name": "search", "method": "lexical", "query": "q",
        "results": [], "where_applied": "source = epo-a", "truncated": False,
        "source_hint": source_hint([("eq", "epo-a")], NAMES),
        "contains_text": operator_sql.text_contains_notes(
            _text("attrs.size", "5"), None, empty=True),
    }
    text = render(payload, tool="search")
    assert ('! source "epo-a": no source id is exactly this text; write one of these, '
            'or use contains; source ids holding it: "repo-a.docs", "repo-a.git"') in text
    assert ("! contains on attrs.size matches text or a list of text; a number, "
            "true/false or an object stored there never matches") in text


def test_text_on_a_list_compiles_to_one_element_walk() -> None:
    compiled = _compile(_text("attrs.tags", "Ab"))
    assert compiled.qualification == "exact" and compiled.truth_exact
    assert "WHEN 'array' THEN EXISTS (SELECT 1 FROM jsonb_array_elements(" in compiled.sql
    assert "jsonb_typeof(text_item.v) = 'string'" in compiled.sql


def test_a_declared_number_or_bool_is_named_and_an_empty_page_says_what_never_matches() -> None:
    declared = {"size": "num", "flag": "bool", "name": "text"}
    tree = {"op": "and", "clauses": [
        _text("attrs.size", "5"), _text("attrs.flag", "t"), _text("attrs.name", "x"),
    ]}
    full = operator_sql.text_contains_notes(tree, declared, empty=False)
    assert full == [
        "attrs.size is declared num; contains matches text or a list of text, never "
        "a number; compare it with eq, lt or gt",
        "attrs.flag is declared bool; contains matches text or a list of text, never "
        "true or false; compare it with eq, lt or gt",
    ]
    empty = operator_sql.text_contains_notes(tree, declared, empty=True)
    assert empty[2] == (
        "contains on attrs.name matches text or a list of text; a number, "
        "true/false or an object stored there never matches"
    )
    assert operator_sql.text_contains_notes({"op": "eq", "field": "attrs.a", "value": 1},
                                            declared, empty=True) == []


@pytest.mark.parametrize("arguments", [
    {"query": "words", "where": {"source": {"op": "contains", "value": "repo-a"}}},
    {"query": "words", "method": "grep",
     "where": {"source": {"op": "contains", "value": "repo-a"}}},
    {"where": {"type": "ticket", "source": {"op": "contains", "value": "Repo-B"}}},
])
def test_text_on_source_is_not_refused_before_the_read(mcp_server: Any, arguments) -> None:
    text = _call_text(mcp_server, "search", arguments)
    assert "where_unsupported" not in text
    assert "JSON containment" not in text
