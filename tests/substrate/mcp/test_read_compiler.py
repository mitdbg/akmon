"""The shared reader predicate compiler: the parts that need no database.

PACT shared-read-scanners-3199, requirements compiler and reader-semantics.
The PostgreSQL half (collation, superset truth, published lexical parity) is
the shared-scanner section of ``test_search_where_db.py``. Each claim carries its failure case.
"""
from __future__ import annotations

import datetime as dt

import pytest

from okg.substrate.mcp import search_where as sw
from okg.substrate.mcp.operator_sql import (
    CompiledPredicate,
    SqlLeaf,
    compile_read_predicate,
    string_field_lowerer,
)
from okg.substrate.nomos.read_masking import MASK_TOKEN
from tests.substrate.mcp.read_compiler_legacy import legacy_invariant_sql

COLUMNS = {"id": "node_id", "type": "subtype", "source": "source"}

EQ_TICKET = {"op": "eq", "field": "type", "value": "ticket"}
ATTR_OPEN = {"op": "eq", "field": "attrs.status", "value": "open"}

INVARIANT_PREDICATES = [
    EQ_TICKET,
    {"op": "eq", "field": "type", "value": None},
    {"op": "in", "field": "id", "values": [None]},
    {"op": "exists", "field": "source"},
    {"op": "prefix", "field": "id", "value": "ticket:"},
    {"op": "suffix", "field": "id", "value": ":0"},
    {"op": "lt", "field": "type", "value": "m"},
    {"op": "gt", "field": "type", "value": "m"},
    {"op": "between", "field": "id", "values": ["a", "n"]},
    {"op": "not", "clause": EQ_TICKET},
    {"op": "and", "clauses": [EQ_TICKET, {"op": "not", "clause": {"op": "exists", "field": "source"}}]},
    {"op": "or", "clauses": [EQ_TICKET, {"op": "prefix", "field": "id", "value": "x"}]},
]


def _compile(predicate, **kwargs) -> CompiledPredicate:
    return sw.compile_where(predicate, columns=COLUMNS, **kwargs)


# ── compiler: legacy helper parity ────────────────────────────────────


@pytest.mark.parametrize("predicate", INVARIANT_PREDICATES)
@pytest.mark.parametrize("seed", [{}, {"q": "query", "bp": 12, "w0": "taken"}])
def test_invariant_sql_matches_the_pre_change_helper(predicate, seed) -> None:
    """Old versus new on the same input: same SQL text and bindings, except
    that range comparisons now name the code-point collation explicitly."""
    old_params, new_params = dict(seed), dict(seed)
    old = legacy_invariant_sql([predicate], old_params, columns=COLUMNS)
    new = sw.invariant_sql([predicate], new_params, columns=COLUMNS)
    assert new_params == old_params
    assert [s.replace(' COLLATE "C"', "") for s in new] == old
    op = predicate["op"]
    ranged = (op in {"lt", "gt"} and isinstance(predicate.get("value"), str)) or (
        op == "between" and all(isinstance(v, str) for v in predicate["values"]))
    assert (' COLLATE "C"' in new[0]) == ranged


@pytest.mark.parametrize("predicate", INVARIANT_PREDICATES)
def test_invariant_predicates_compile_exact_and_truth_exact(predicate) -> None:
    compiled = _compile(predicate)
    assert compiled.qualification == "exact" and compiled.truth_exact
    assert compiled.residual is None
    assert compiled.predicate == predicate


#: Issue #3666 (2c): a value that is not text on a text column. The helper
#: compiled these to a constant FALSE (an empty page marked complete); the
#: compiler now gives them no lowering, so the helper refuses them and
#: search's grammar refuses the call, naming the fix, before it gets here.
#: The helper-parity rows above pinned that FALSE until this change.
WRONG_TYPE_PREDICATES = [
    {"op": "eq", "field": "type", "value": 7},
    {"op": "eq", "field": "source", "value": True},
    {"op": "in", "field": "id", "values": ["a", None, 3]},
    {"op": "in", "field": "id", "values": [3, False]},
    {"op": "lt", "field": "type", "value": 3},
    {"op": "between", "field": "id", "values": ["a", None]},
]


@pytest.mark.parametrize("predicate", WRONG_TYPE_PREDICATES)
def test_a_value_that_is_not_text_on_a_text_column_is_never_compiled_to_false(predicate) -> None:
    assert legacy_invariant_sql([predicate], {}, columns=COLUMNS) != [], predicate
    compiled = _compile(predicate)
    assert compiled.qualification != "exact" and "FALSE" not in compiled.sql, compiled
    with pytest.raises(sw.WhereUnsupported):
        sw.invariant_sql([predicate], {}, columns=COLUMNS)


def test_a_non_invariant_clause_still_refuses_on_the_invariant_helper() -> None:
    with pytest.raises(sw.WhereUnsupported):
        sw.invariant_sql([ATTR_OPEN], {}, columns=COLUMNS)


def test_seed_parameters_are_reserved_never_copied_or_retyped() -> None:
    """Caller parameters may hold non-JSON values; the compile returns only
    the names it bound, so merging cannot overwrite them. The draft
    serialized every seed and crashed on a datetime."""
    seed = {"q": "query", "bp": dt.datetime(2026, 10, 3), "pair": (1, 2), "w0": "taken"}
    before = dict(seed)
    compiled = _compile({"op": "and", "clauses": [EQ_TICKET, {"op": "eq", "field": "id", "value": "t:1"}]},
                        params=seed)
    assert seed == before
    assert set(compiled.params) == {"w4", "w5"}
    assert compiled.params == {"w4": "ticket", "w5": "t:1"}
    assert "%(w4)s" in compiled.sql and "%(w5)s" in compiled.sql


def test_compiled_structures_are_copies() -> None:
    predicate = {"op": "and", "clauses": [EQ_TICKET, ATTR_OPEN]}
    compiled = _compile(predicate)
    predicate["clauses"].append({"op": "exists", "field": "id"})
    assert len(compiled.predicate["clauses"]) == 2
    view = compiled.predicate
    view["clauses"].clear()
    assert len(compiled.predicate["clauses"]) == 2
    params = compiled.params
    params["w0"] = "changed"
    assert compiled.params["w0"] == "ticket"


def test_fingerprint_binds_predicate_compiler_version_and_field_map() -> None:
    base = _compile(EQ_TICKET)
    assert base.fingerprint == _compile(dict(EQ_TICKET)).fingerprint
    assert base.fingerprint != _compile({**EQ_TICKET, "value": "comment"}).fingerprint
    other_map = sw.compile_where(EQ_TICKET, columns={**COLUMNS, "type": "v.subtype"})
    assert other_map.field_map_identity != base.field_map_identity
    assert other_map.fingerprint != base.fingerprint
    versioned = compile_read_predicate(
        EQ_TICKET, leaf_lowerer=string_field_lowerer({"type": "subtype"}),
        field_map_identity=base.field_map_identity, compiler_version="reader-predicate-v2")
    assert versioned.fingerprint != base.fingerprint


# ── compiler: caller text never becomes SQL ───────────────────────────


@pytest.mark.parametrize("field", ["subtype) OR TRUE --", "node_id", "attrs.x'; DROP TABLE okg.nodes; --"])
def test_caller_field_names_never_reach_sql(field) -> None:
    compiled = _compile({"op": "eq", "field": field, "value": "x"})
    assert compiled.sql == "TRUE" and compiled.qualification == "superset"
    assert field not in compiled.sql and compiled.params == {}


def test_caller_values_are_bound_parameters_only() -> None:
    hostile = "x' OR '1'='1"
    compiled = _compile({"op": "or", "clauses": [
        {"op": "eq", "field": "type", "value": hostile},
        {"op": "prefix", "field": "id", "value": "%(q)s"},
        {"op": "in", "field": "source", "values": [hostile, "); DELETE"]},
    ]})
    assert hostile not in compiled.sql and "DELETE" not in compiled.sql and "%(q)s" not in compiled.sql
    bound = list(compiled.params.values())
    assert hostile in bound and "%(q)s" in bound and [hostile, "); DELETE"] in bound


@pytest.mark.parametrize("prefix", ["w); --", "", "1w", "w-x"])
def test_an_unsafe_parameter_prefix_is_refused(prefix) -> None:
    with pytest.raises(ValueError):
        _compile(EQ_TICKET, prefix=prefix)


def test_nul_operands_are_never_sent_and_keep_reader_truth() -> None:
    """PostgreSQL text cannot hold NUL: equality and prefix are exactly
    FALSE (the reader agrees), and a NUL range bound stays residual."""
    eq = _compile({"op": "eq", "field": "type", "value": "tic\x00ket"})
    assert eq.sql == "FALSE" and eq.qualification == "exact" and eq.params == {}
    prefix = _compile({"op": "prefix", "field": "id", "value": "\x00"})
    assert prefix.sql == "FALSE" and prefix.params == {}
    ranged = _compile({"op": "not", "clause": {"op": "lt", "field": "id", "value": "a\x00"}})
    assert ranged.sql == "TRUE" and ranged.qualification == "superset" and ranged.params == {}
    stored = {"node_id": "a", "subtype": "ticket"}  # stored text never holds NUL
    assert sw.evaluate(stored, {"op": "eq", "field": "type", "value": "tic\x00ket"}) is False


@pytest.mark.parametrize("predicate", [
    {"op": "and", "clauses": []},
    {"op": "or"},
    {"op": "not"},
    ["eq"],
    {"op": "in", "field": "id", "values": []},
    {"op": "in", "field": "id"},
    {"op": "between", "field": "id", "values": ["a"]},
    {"op": "prefix", "field": "id", "value": ""},
    {"op": "eq", "field": "id", "value": float("nan")},
])
def test_malformed_normalized_predicates_refuse_before_sql(predicate) -> None:
    with pytest.raises(ValueError):
        _compile(predicate)


def test_truth_exact_lowering_must_be_acceptance_exact() -> None:
    with pytest.raises(ValueError):
        SqlLeaf("x", exact=False, truth_exact=True)


# ── reader semantics: superset, residual, unknown under NOT ───────────


@pytest.mark.parametrize("field", [
    "title", "path", "date.created", "date.effective", "date.attrs.until", "attrs.status",
])
def test_reader_value_fields_stay_true_with_full_residual(field) -> None:
    leaf = {"op": "exists", "field": field}
    for predicate in (leaf, {"op": "not", "clause": leaf},
                      {"op": "or", "clauses": [EQ_TICKET, leaf]}):
        compiled = _compile(predicate)
        assert compiled.qualification == "superset" and not compiled.truth_exact
        assert compiled.residual == predicate
        assert "attrs" not in compiled.sql and "date" not in compiled.sql


def test_and_lifts_the_invariant_conjunct_beside_a_residual() -> None:
    compiled = _compile({"op": "and", "clauses": [EQ_TICKET, ATTR_OPEN]})
    assert compiled.sql == "(COALESCE(subtype = %(w0)s, FALSE) AND TRUE)"
    assert compiled.qualification == "superset" and compiled.params == {"w0": "ticket"}


def test_or_with_a_residual_branch_is_never_narrower_than_true() -> None:
    compiled = _compile({"op": "or", "clauses": [EQ_TICKET, ATTR_OPEN]})
    assert compiled.sql == "(COALESCE(subtype = %(w0)s, FALSE) OR TRUE)"
    assert compiled.qualification == "superset"


def test_not_over_a_lossy_child_is_true_and_rolls_back_its_bindings() -> None:
    compiled = _compile({"op": "not", "clause": {"op": "and", "clauses": [EQ_TICKET, ATTR_OPEN]}})
    assert compiled.sql == "TRUE" and compiled.params == {}
    assert compiled.qualification == "superset"


def test_not_requires_full_truth_exactness_not_only_acceptance() -> None:
    """A leaf whose TRUE rows are exact but whose unknown rows are not
    (a masked value) must not be negated."""
    def lowerer(node, params, prefix):
        return SqlLeaf("(col = 1)", exact=True, truth_exact=False)

    plain = compile_read_predicate({"op": "eq", "field": "f", "value": 1},
                                   leaf_lowerer=lowerer, field_map_identity="t")
    assert plain.sql == "(col = 1)" and plain.qualification == "exact" and not plain.truth_exact
    negated = compile_read_predicate({"op": "not", "clause": {"op": "eq", "field": "f", "value": 1}},
                                     leaf_lowerer=lowerer, field_map_identity="t")
    assert negated.sql == "TRUE" and negated.qualification == "superset"


def test_not_over_a_truth_exact_child_negates_in_sql() -> None:
    compiled = _compile({"op": "not", "clause": EQ_TICKET})
    assert compiled.sql == "(NOT COALESCE(subtype = %(w0)s, FALSE))"
    assert compiled.qualification == "exact" and compiled.truth_exact


def test_unknown_under_not_stays_unknown_and_sql_keeps_the_row() -> None:
    masked = {"node_id": "t:1", "subtype": "ticket", "attrs": {"status": MASK_TOKEN}}
    predicate = {"op": "not", "clause": ATTR_OPEN}
    assert sw.evaluate(masked, ATTR_OPEN) is None
    assert sw.evaluate(masked, predicate) is None
    assert not sw.matches(masked, predicate)
    assert _compile(predicate).sql == "TRUE"


def test_mask_token_value_is_never_lowered_even_on_an_attr_eq() -> None:
    compiled = _compile({"op": "eq", "field": "attrs.status", "value": MASK_TOKEN})
    assert compiled.sql == "TRUE" and MASK_TOKEN not in compiled.params.values()


# ── two compiles from one seed; non-canonical operands ────────────────


def test_two_compiles_from_one_seed_cannot_overwrite_each_other() -> None:
    """Both compiles see the same seed, so both allocate ``w0``. Merging them
    with ``dict.update`` would let the second silently replace the first;
    ``bind_into`` refuses and leaves the target unchanged."""
    seed = {"q": "query"}
    first = _compile(EQ_TICKET, params=seed)
    second = _compile({"op": "eq", "field": "source", "value": "s1"}, params=seed)
    assert set(first.params) == set(second.params) == {"w1"}  # the collision
    params = dict(seed)
    assert first.bind_into(params) == first.sql and params == {"q": "query", "w1": "ticket"}
    with pytest.raises(ValueError, match="already bound: w1"):
        second.bind_into(params)
    assert params == {"q": "query", "w1": "ticket"}
    # Seeding the second compile with what is bound so far is the fix.
    third = _compile({"op": "eq", "field": "source", "value": "s1"}, params=params)
    assert third.bind_into(params) == "COALESCE(source = %(w2)s, FALSE)"
    assert params == {"q": "query", "w1": "ticket", "w2": "s1"}


def test_invariant_sql_seeds_each_clause_with_the_parameters_bound_so_far() -> None:
    params: dict = {"bp": 3}
    parts = sw.invariant_sql([EQ_TICKET, {"op": "eq", "field": "source", "value": "s1"}],
                             params, columns=COLUMNS)
    assert parts == ["COALESCE(subtype = %(w1)s, FALSE)", "COALESCE(source = %(w2)s, FALSE)"]
    assert params == {"bp": 3, "w1": "ticket", "w2": "s1"}


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), "lone\ud800",
                                   {"k\udc00": 1}])
def test_noncanonical_operands_refuse_cleanly(value) -> None:
    """NaN, Infinity and lone surrogates have no canonical JSON: the user
    surface refuses by name, and a direct compile raises a plain ValueError
    (never an uncaught UnicodeEncodeError from fingerprinting)."""
    where = ({"op": "contains", "field": "attrs", "value": value} if isinstance(value, dict)
             else {"op": "eq", "field": "attrs.x", "value": value})
    with pytest.raises(sw.WhereUnsupported) as refused:
        sw.normalize_where(where)
    assert refused.value.data == {"reason": "invalid_where_value"}
    with pytest.raises(ValueError) as raw:
        _compile(where)
    assert type(raw.value) is ValueError and "finite numbers and valid Unicode" in str(raw.value)
