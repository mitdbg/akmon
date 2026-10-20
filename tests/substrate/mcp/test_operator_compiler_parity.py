"""filter, map, aggregate and expand compile through one compiler (#3201).

No database. These pin the compile itself:

- every leaf the old effective-view renderer accepted, the operator compiler
  accepts, and every shape it refused stays refused (the SQL truth of both is
  compared on published rows in test_operator_parity_db.py);
- the fingerprint ignores key order and changes with any operand;
- no caller value reaches SQL text, hostile ones included, and a NUL operand
  is never even a parameter;
- set_operators no longer has a literal renderer to call.
"""
from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.mcp import operator_sql
from okg.substrate.mcp.operator_sql import (
    compile_read_predicate,
    effective_attr_lowerer,
    effective_field_map_identity,
    encode_nonfinite_operands,
)
from okg.substrate.mcp.search_where import RAW_TEXT_CONTAINS
from tests.substrate.mcp.operator_compiler_legacy import legacy_effective_predicate_sql

REPO = Path(__file__).resolve().parents[3]
DECLARED = {"due": "ts", "size": "num", "title": "text"}


def compile_new(kind: str, predicate: Any, *, declared=DECLARED, relation="rel"):
    """The server's `_compile_operator_predicate`, minus its closure.

    Returns the compiled predicate, or None where the server refuses: a
    non-exact or non-two-valued compile, or a malformed tree.
    """
    if predicate in (None, True, {}):
        predicate = None
    try:
        compiled = compile_read_predicate(
            encode_nonfinite_operands(predicate),
            leaf_lowerer=effective_attr_lowerer(kind, declared=declared, relation=relation),
            field_map_identity=effective_field_map_identity(kind, declared),
            prefix="pv",
        )
    except ValueError:
        return None
    if compiled.qualification != "exact" or not compiled.truth_exact:
        return None
    return compiled


def compile_old(kind: str, predicate: Any, *, declared=DECLARED, relation="rel"):
    params: dict[str, Any] = {}
    return legacy_effective_predicate_sql(
        kind, predicate, params, declared=declared, relation=relation
    )


def eq(field: str, value: Any) -> dict[str, Any]:
    return {"op": "eq", "field": field, "value": value}


# Every leaf and logical shape, nodes and edges. Each is accepted by both the
# old renderer and the compiler, or refused by both.
CASES: list[tuple[str, dict[str, Any]]] = [
    ("nodes", eq("subtype", "page")),
    ("nodes", eq("node_id", "n:1")),
    ("nodes", eq("subtype", None)),
    ("nodes", eq("text_hash", "abc")),
    ("nodes", eq("attrs.status", "open")),
    ("nodes", eq("attrs.status", None)),
    ("nodes", eq("attrs.n", 3)),
    ("nodes", eq("attrs.flag", True)),
    ("nodes", eq("attrs.obj", {"a": [1, 2]})),
    ("nodes", eq("attrs", {"status": "open"})),
    ("nodes", eq("attrs.due", "2026-01-01T00:00:00Z")),
    ("nodes", eq("attrs.due", "not a date")),
    ("nodes", {"op": "in", "field": "subtype", "values": ["page", "doc"]}),
    ("nodes", {"op": "in", "field": "attrs.status", "values": ["a", None, 2]}),
    ("nodes", {"op": "exists", "field": "subtype"}),
    ("nodes", {"op": "exists", "field": "attrs.status"}),
    ("nodes", {"op": "exists", "field": "attrs"}),
    ("nodes", {"op": "contains", "field": "attrs", "value": {"a": 1, "b": "x"}}),
    ("nodes", {"op": "contains", "field": "attrs", "value": {}}),
    ("nodes", {"op": "contains", "field": "attrs", "value": {"a": None}}),
    ("nodes", {"op": "prefix", "field": "attrs.path", "value": "lib/"}),
    ("nodes", {"op": "suffix", "field": "attrs.path", "value": "/x.py"}),
    ("nodes", {"op": "prefix", "field": "subtype", "value": "pa"}),
    ("nodes", {"op": "suffix", "field": "node_id", "value": ":1"}),
    # search's query-less lookup narrows its SQL page with this internal leaf
    # (search_where.lookup_sql_clauses); the chain's renderer took it, so the
    # compiler does too. On a column or the whole attrs object both refuse.
    ("nodes", {"op": RAW_TEXT_CONTAINS, "field": "attrs.path", "value": "lib/"}),
    ("nodes", {"op": "not", "clause": {"op": RAW_TEXT_CONTAINS, "field": "attrs.title", "value": "a"}}),
    ("nodes", {"op": RAW_TEXT_CONTAINS, "field": "subtype", "value": "pa"}),
    ("nodes", {"op": RAW_TEXT_CONTAINS, "field": "attrs", "value": "x"}),
    ("nodes", {"op": "lt", "field": "attrs.size", "value": 10}),
    ("nodes", {"op": "gt", "field": "attrs.size", "value": 2.5}),
    ("nodes", {"op": "between", "field": "attrs.size", "values": [1, 9.5]}),
    ("nodes", {"op": "lt", "field": "attrs.size", "value": float("inf")}),
    ("nodes", {"op": "between", "field": "attrs.size", "values": [float("-inf"), float("inf")]}),
    ("nodes", {"op": "lt", "field": "attrs.due", "value": "2026-01-01"}),
    ("nodes", {"op": "between", "field": "attrs.due", "values": ["2025-01-01", "2026-01-01T00:00:00+02:00"]}),
    ("nodes", {"op": "gt", "field": "attrs.title", "value": "m"}),
    ("nodes", {"op": "lt", "field": "attrs.undeclared", "value": "m"}),
    ("nodes", {"op": "lt", "field": "attrs.undeclared", "value": 4}),
    ("nodes", {"op": "between", "field": "subtype", "values": ["a", "q"]}),
    ("nodes", {"op": "and", "clauses": [eq("subtype", "page"), eq("attrs.status", "open")]}),
    ("nodes", {"op": "or", "clauses": [eq("attrs.status", "a"), {"op": "exists", "field": "attrs.x"}]}),
    ("nodes", {"op": "not", "clause": eq("attrs.status", "open")}),
    ("nodes", {"op": "not", "clause": {"op": "or", "clauses": [eq("subtype", "a"), eq("subtype", "b")]}}),
    ("edges", eq("edge_type", "cites")),
    ("edges", eq("src", "n:1")),
    ("edges", {"op": "in", "field": "dst", "values": ["n:1", "n:2"]}),
    ("edges", {"op": "and", "clauses": [eq("edge_type", "cites"), eq("attrs.weight", 2)]}),
    # Refused by both: unknown op, unknown field, prefix on the whole attrs
    # object, a malformed between, an empty logical node, a node field on edges.
    ("nodes", {"op": "regex", "field": "attrs.path", "value": "x"}),
    ("nodes", eq("bogus_field", "x")),
    ("nodes", {"op": "prefix", "field": "attrs", "value": "x"}),
    ("nodes", {"op": "between", "field": "attrs.size", "values": [1]}),
    ("nodes", {"op": "and", "clauses": []}),
    ("nodes", {"op": "not", "clause": eq("bogus_field", "x")}),
    ("edges", eq("subtype", "page")),
    ("nodes", eq("attrs.", "x")),
]


@pytest.mark.parametrize("kind,predicate", CASES, ids=[json.dumps(c[1])[:60] for c in CASES])
def test_compiler_accepts_exactly_what_the_old_renderer_accepted(kind, predicate) -> None:
    old = compile_old(kind, predicate)
    new = compile_new(kind, predicate)
    assert (old is None) == (new is None), (old, new)


@pytest.mark.parametrize("kind,predicate", [
    ("nodes", {"op": "gt", "field": "subtype", "value": 3}),
    ("edges", {"op": "between", "field": "src", "values": [1, 2]}),
    ("edges", {"op": "gt", "field": "edge_id", "value": "9"}),
    ("edges", {"op": "lt", "field": "edge_id", "value": 5.5}),
    # Second re-review (#3666, 2c): every other operand that compiled to a
    # constant FALSE because of its type.
    ("nodes", {"op": "gt", "field": "attrs.due", "value": "garbage"}),
    ("nodes", {"op": "lt", "field": "attrs.due", "value": 5}),
    ("nodes", {"op": "lt", "field": "attrs.size", "value": True}),
    ("nodes", {"op": "gt", "field": "attrs.size", "value": "5"}),
    ("nodes", {"op": "gt", "field": "attrs.title", "value": 5}),
    ("nodes", {"op": "between", "field": "attrs.undeclared", "values": [1, "z"]}),
    ("nodes", {"op": "lt", "field": "attrs.undeclared", "value": None}),
    ("nodes", {"op": "eq", "field": "subtype", "value": 5}),
    ("nodes", {"op": "in", "field": "node_id", "values": ["a", True]}),
    ("edges", {"op": "eq", "field": "src", "value": {"a": 1}}),
    ("edges", {"op": "eq", "field": "edge_id", "value": [1, 2]}),
    ("nodes", {"op": "eq", "field": "attrs", "value": "x"}),
])
def test_an_operand_whose_type_does_not_fit_the_field_is_not_lowered(kind, predicate) -> None:
    """Issue #3666 (2c): the old renderer compiled these to FALSE, an empty
    page marked complete. The compiler now gives no lowering, so the read is
    refused (the grammar names the fix before a set read gets here)."""
    assert compile_old(kind, predicate) is not None
    assert compile_new(kind, predicate) is None


def test_every_lowerer_gives_no_lowering_for_an_operand_of_the_wrong_type() -> None:
    """Fourth review (#3666, 2c): search's lowerer and a declared range give
    no lowering (the read is refused) rather than a constant FALSE; every
    range bound that does not compile is named with its declared type, and
    offered in that type only where it has one reading."""
    lower = operator_sql.string_field_lowerer({"type": "subtype"})
    for node in ({"op": "eq", "field": "type", "value": 5},
                 {"op": "in", "field": "type", "values": ["a", 5]},
                 {"op": "lt", "field": "type", "value": True},
                 {"op": "between", "field": "type", "values": ["a", 5]}):
        assert lower(node, {}, "w") is None, node
    mismatch = operator_sql.declared_bound_mismatch
    for predicate in ({"op": "gt", "field": "attrs.due", "value": "garbage"},
                      {"op": "gt", "field": "attrs.due", "value": 5},
                      {"op": "gt", "field": "attrs.size", "value": "5"},
                      {"op": "gt", "field": "attrs.title", "value": 5}):
        assert compile_new("nodes", predicate) is None
        assert mismatch(predicate, DECLARED) is not None, predicate
    found = mismatch({"op": "not", "clause": {"op": "and", "clauses": [
        {"op": "gt", "field": "attrs.size", "value": "-2.5"},
        {"op": "lt", "field": "attrs.title", "value": 7}]}}, DECLARED)
    assert found is not None and "declared num" in found[0]
    assert found[2] == {"op": "not", "clause": {"op": "and", "clauses": [
        {"op": "gt", "field": "attrs.size", "value": -2.5},
        {"op": "lt", "field": "attrs.title", "value": "7"}]}}
    # No one reading: words only.
    for predicate in ({"op": "gt", "field": "attrs.due", "value": 5},
                      {"op": "gt", "field": "attrs.size", "value": "five"},
                      {"op": "gt", "field": "attrs.size", "value": "1e999"},
                      {"op": "gt", "field": "attrs.flag", "value": 1}):
        found = mismatch(predicate, {**DECLARED, "flag": "bool"})
        assert found is not None and found[2] is None, (predicate, found)
    # What compiles is not a mismatch: a number, a readable instant, text, a
    # non-finite marker, an undeclared attribute, an equality.
    for predicate in ({"op": "gt", "field": "attrs.size", "value": 5},
                      encode_nonfinite_operands(
                          {"op": "lt", "field": "attrs.size", "value": float("inf")}),
                      {"op": "gt", "field": "attrs.due", "value": "2026-01-01"},
                      {"op": "gt", "field": "attrs.title", "value": "m"},
                      {"op": "gt", "field": "attrs.undeclared", "value": "5"},
                      {"op": "eq", "field": "attrs.size", "value": "5"}):
        assert mismatch(predicate, DECLARED) is None, predicate
        assert compile_new("nodes", predicate) is not None, predicate
    notes = operator_sql.declared_operand_notes
    tail = "matches only a stored"
    assert notes({"op": "eq", "field": "attrs.size", "value": "5"}, DECLARED) == [
        f'attrs.size is declared num; eq with "5" {tail} text value; for the number write 5']
    assert notes({"op": "in", "field": "attrs.due", "values": [None, "2026-01-01", 3]},
                 DECLARED) == [
        f"attrs.due is declared ts; in with 3 {tail} number value; a stored value is "
        + "a date or timestamp as text"]
    assert notes({"op": "eq", "field": "attrs.title", "value": 7}, DECLARED) == [
        f'attrs.title is declared text; eq with 7 {tail} number value; for the text write "7"']
    assert notes({"op": "contains", "field": "attrs", "value": {"size": "2.5", "x": 1}},
                 DECLARED) == [
        f'attrs.size is declared num; contains with "2.5" {tail} text value; '
        + "for the number write 2.5"]
    assert notes({"op": "eq", "field": "attrs.flag", "value": "true"},
                 {"flag": "bool"}) == [
        f'attrs.flag is declared bool; eq with "true" {tail} text value; '
        + "for the bool write true"]
    # A text guess on a range says how text compares.
    found = mismatch({"op": "gt", "field": "attrs.title", "value": 10}, DECLARED)
    assert found is not None and found[2]["value"] == "10"
    assert '"10" sorts before "5"' in found[1]
    # A long decimal written as text: a float would round it, so the note
    # writes its own digits and no range bound is guessed (words only); a
    # long whole number is exact.
    for digits in ("0.1000000000000000055", "12345678901234567890.5"):
        assert notes({"op": "eq", "field": "attrs.size", "value": digits}, DECLARED) == [
            f'attrs.size is declared num; eq with "{digits}" {tail} text value; '
            + f"for the number write {digits}"]
        found = mismatch({"op": "gt", "field": "attrs.size", "value": digits}, DECLARED)
        assert found is not None and found[2] is None, (digits, found)
    found = mismatch({"op": "gt", "field": "attrs.size", "value": "12345678901234567890"},
                     DECLARED)
    assert found is not None and found[2]["value"] == 12345678901234567890
    for predicate in ({"op": "eq", "field": "attrs.size", "value": 5},
                      {"op": "eq", "field": "attrs.size", "value": None},
                      {"op": "eq", "field": "attrs.undeclared", "value": "5"},
                      {"op": "gt", "field": "attrs.size", "value": "5"}):
        assert notes(predicate, DECLARED) == [], predicate


def test_an_integer_range_on_a_link_id_is_lowered_as_numbers() -> None:
    compiled = compile_new("edges", {"op": "between", "field": "edge_id", "values": [3, 40]})
    assert compiled is not None and "::numeric" in compiled.sql


def test_no_op_predicates_compile_to_true() -> None:
    for predicate in (None, True, {}):
        compiled = compile_new("nodes", predicate)
        assert compiled is not None
        assert compiled.sql == "TRUE"
        assert compiled.params == {}


def test_fingerprint_ignores_key_order_and_tracks_every_operand() -> None:
    a = compile_new("nodes", {"op": "and", "clauses": [eq("subtype", "page"), eq("attrs.n", 3)]})
    b = compile_new("nodes", {"clauses": [{"value": "page", "field": "subtype", "op": "eq"},
                                          {"value": 3, "op": "eq", "field": "attrs.n"}], "op": "and"})
    assert a is not None and b is not None
    assert a.fingerprint == b.fingerprint
    changed = [
        compile_new("nodes", {"op": "and", "clauses": [eq("subtype", "pages"), eq("attrs.n", 3)]}),
        compile_new("nodes", {"op": "and", "clauses": [eq("subtype", "page"), eq("attrs.n", 4)]}),
        compile_new("nodes", {"op": "and", "clauses": [eq("subtype", "page"), eq("attrs.m", 3)]}),
        compile_new("nodes", {"op": "or", "clauses": [eq("subtype", "page"), eq("attrs.n", 3)]}),
        compile_new("edges", {"op": "and", "clauses": [eq("edge_type", "page"), eq("attrs.n", 3)]}),
        compile_new("nodes", {"op": "and", "clauses": [eq("subtype", "page"), eq("attrs.n", 3)]},
                    declared={"n": "num"}),
    ]
    fingerprints = {a.fingerprint} | {c.fingerprint for c in changed if c is not None}
    assert len(fingerprints) == 1 + len(changed)


def test_unknown_field_is_refused_not_widened() -> None:
    """The failure case: an unknown field compiles only to a TRUE superset,
    which the operator path refuses (unsupported_operator_form) rather than
    serving every row."""
    raw = compile_read_predicate(
        eq("attrs_status", "open"),
        leaf_lowerer=effective_attr_lowerer("nodes", declared=DECLARED),
        field_map_identity="t",
    )
    assert raw.qualification == "superset"
    assert raw.sql == "TRUE"
    assert compile_new("nodes", eq("attrs_status", "open")) is None
    negated = compile_read_predicate(
        {"op": "not", "clause": eq("attrs_status", "open")},
        leaf_lowerer=effective_attr_lowerer("nodes", declared=DECLARED),
        field_map_identity="t",
    )
    assert negated.truth_exact is False


HOSTILE = [
    "O'Brien",
    "it''s",
    "back\\slash\\",
    "100% sure_",
    "%(pv0)s",
    "%s %%",
    "日本語 ünïcødé ✓",
    "'); DROP TABLE okg.attribute_versions; --",
]


#: Every quoted literal the effective lowerer writes itself.
SQL_CONSTANTS = {"'null'", "'string'", "'number'", "'{}'"}


def _all_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [s for v in value for s in _all_strings(v)]
    if isinstance(value, dict):
        return [s for k, v in value.items() for s in [k, *_all_strings(v)]]
    return []


@pytest.mark.parametrize("value", HOSTILE)
def test_hostile_operands_are_parameters_never_sql_text(value: str) -> None:
    shapes = [
        ("nodes", eq("attrs.status", value)),
        ("nodes", eq("subtype", value)),
        ("nodes", eq(f"attrs.{value}", "x")),
        ("nodes", {"op": "in", "field": "attrs.status", "values": [value, "b"]}),
        ("nodes", {"op": "contains", "field": "attrs", "value": {value: value}}),
        ("nodes", {"op": "prefix", "field": "attrs.path", "value": value}),
        ("nodes", {"op": "suffix", "field": "node_id", "value": value}),
        ("nodes", {"op": "lt", "field": "attrs.title", "value": value}),
        ("nodes", {"op": "between", "field": "subtype", "values": [value, value + "z"]}),
        ("nodes", {"op": "not", "clause": {"op": "exists", "field": f"attrs.{value}"}}),
        ("edges", eq("edge_type", value)),
    ]
    for kind, predicate in shapes:
        compiled = compile_new(kind, predicate)
        assert compiled is not None, predicate
        # Placeholder-shaped text equals a real placeholder, so compare the
        # SQL with the compiler's own placeholders taken out.
        bare_sql = re.sub(r"%\(pv\d+\)s", "", compiled.sql)
        assert value not in bare_sql, (predicate, compiled.sql)
        # The only quoted literals are the lowerer's own constants.
        assert set(re.findall(r"'[^']*'", compiled.sql)) <= SQL_CONSTANTS, compiled.sql
        # Scalars travel bare; JSON operands inside a JSON text parameter.
        carried: list[str] = []
        for param in compiled.params.values():
            carried.extend(_all_strings(param))
            if isinstance(param, str):
                try:
                    carried.extend(_all_strings(json.loads(param)))
                except ValueError:
                    pass
        assert any(value in text for text in carried), (predicate, compiled.params)
        # Placeholders in SQL are exactly the allocated parameter names.
        assert set(_placeholders(compiled.sql)) == set(compiled.params)


def _placeholders(sql: str) -> list[str]:
    return re.findall(r"%\(([A-Za-z_][A-Za-z0-9_]*)\)s", sql)


NUL_SHAPES = [
    (eq("attrs.status", "a\x00b"), "FALSE"),
    (eq("subtype", "pa\x00ge"), "FALSE"),
    ({"op": "in", "field": "attrs.status", "values": ["\x00"]}, "(FALSE)"),
    ({"op": "contains", "field": "attrs", "value": {"k": "v\x00"}}, "(FALSE)"),
    ({"op": "prefix", "field": "attrs.path", "value": "src\x00"}, "FALSE"),
    ({"op": "suffix", "field": "node_id", "value": "\x00"}, "FALSE"),
    ({"op": "exists", "field": "attrs.k\x00"}, "FALSE"),
    (eq("attrs.k\x00", None), "TRUE"),
    (eq("attrs.obj", {"nested": ["x\x00"]}), "FALSE"),
]


@pytest.mark.parametrize("predicate,sql", NUL_SHAPES, ids=[json.dumps(s[0]) for s in NUL_SHAPES])
def test_nul_operands_are_decided_without_the_database(predicate, sql) -> None:
    """The failure case for NUL: PostgreSQL text and jsonb cannot hold it, so
    sending it as a parameter fails the query. The compiler decides the leaf
    itself and binds nothing holding NUL."""
    compiled = compile_new("nodes", predicate)
    assert compiled is not None
    assert compiled.sql == sql
    assert "\x00" not in json.dumps(compiled.params)
    negated = compile_new("nodes", {"op": "not", "clause": predicate})
    assert negated is not None and negated.sql == f"(NOT {sql})"


def test_nul_range_bounds_are_cut_before_the_nul() -> None:
    """No stored text holds NUL, so `v < p+NUL+r` is `v <= p` and `v > p+NUL+r`
    is `v > p`, in the evaluator's own order. The NUL bound is never sent."""
    lt = compile_new("nodes", {"op": "lt", "field": "attrs.title", "value": "ab\x00c"})
    gt = compile_new("nodes", {"op": "gt", "field": "subtype", "value": "ab\x00c"})
    assert lt is not None and gt is not None
    assert " <= %(pv1)s" in lt.sql and lt.params == {"pv0": "title", "pv1": "ab"}
    assert " > %(pv0)s" in gt.sql and gt.params == {"pv0": "ab"}
    for text in ("", "a", "ab", "ab\x01", "abc", "b"):
        assert (text <= "ab") == (text < "ab\x00c")
        assert (text > "ab") == (text > "ab\x00c")


def test_set_operators_has_no_literal_renderer() -> None:
    """The quoted-literal renderer is gone: set_operators imports none of it
    and operator_sql no longer defines it."""
    source = (REPO / "src/okg/substrate/mcp/set_operators.py").read_text(encoding="utf-8")
    imported: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            imported.update(alias.name for alias in node.names)
        if isinstance(node, ast.Name):
            imported.add(node.id)
    assert not imported & {"sql_literal", "json_literal", "predicate_sql", "run_sql"}
    for name in ("sql_literal", "json_literal", "predicate_sql", "field_expr", "projection_sql"):
        assert not hasattr(operator_sql, name), name


def test_server_has_one_predicate_renderer() -> None:
    """server.py compiles operator predicates only through the compiler: the
    old effective-view renderer is deleted, and the set operators are no
    longer imported from set_operators."""
    source = (REPO / "src/okg/substrate/mcp/server.py").read_text(encoding="utf-8")
    assert "_effective_view_predicate_sql" not in source
    for name in ("filter_ordered", "aggregate_set", "expand_filtered"):
        assert name not in source, name
    assert source.count("_compile_read_predicate(") == 1


def test_non_finite_bounds_compile_as_float8_text() -> None:
    """Canonical JSON has no Infinity; a range bound may be one and has
    always compared as that float8. It is carried as a marker and bound as
    text that ``::float8`` reads back, never refused."""
    compiled = compile_new("nodes", {"op": "gt", "field": "attrs.size", "value": float("-inf")})
    assert compiled is not None
    assert compiled.params == {"pv0": "size", "pv1": "-Infinity"}
    assert "%(pv1)s::float8" in compiled.sql
    finite = compile_new("nodes", {"op": "gt", "field": "attrs.size", "value": 1.5})
    assert finite is not None and finite.fingerprint != compiled.fingerprint


def test_lookup_text_leaf_is_one_op_and_lowers_exactly() -> None:
    """The query-less lookup's internal leaf: operator_sql (which cannot
    import search_where) names the same op, and the lowerer renders it
    exactly as a bound-parameter substring test on a stored JSON string. A
    NUL operand, which no stored text holds, is decided without the
    database."""
    assert operator_sql.RAW_TEXT_CONTAINS == RAW_TEXT_CONTAINS
    leaf = {"op": RAW_TEXT_CONTAINS, "field": "attrs.path", "value": "it's 50%"}
    compiled = compile_new("nodes", leaf)
    assert compiled is not None and compiled.qualification == "exact"
    assert "strpos(" in compiled.sql and "it's" not in compiled.sql
    assert sorted(compiled.params.values()) == ["it's 50%", "path"]
    nul = compile_new("nodes", {**leaf, "value": "a\x00b"})
    assert nul is not None and nul.sql == "FALSE"
    assert all("\x00" not in str(v) for v in nul.params.values())
