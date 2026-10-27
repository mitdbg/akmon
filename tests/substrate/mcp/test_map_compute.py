"""`map(compute=...)` without a database (PACT read-map-compute, PR 3c).

The grammar's refusals and rewrites for `map(compute)`, the rows a map keeps
(`map_compute.computed_rows`), the `calc.` names of later calls and the rows a
computed column sets aside. The tools themselves are exercised in
`test_map_compute_db.py`.

MF marks a test that fails on the base (`4bee16134486`, PR #4156): there
`map_compute` does not exist and the grammar knows no `compute`.
"""
from __future__ import annotations

import functools
import json
from typing import Any

import pytest

from okg.substrate.mcp.read_grammar import GrammarRefusal, rewrite_call

LIVE = "l1." + "e" * 8 + "." + "2" * 10 + "." + "a" * 16 + "." + "b" * 22


def _typed(i: int, used: Any, total: Any, *, states: dict[str, str] | None = None) -> dict:
    return {
        "node_id": f"site:S{i}", "subtype": "site",
        "attrs": {"static_used": used, "static_total": total, "kind": "HOT"},
        "live": {"system": "example", "fields": dict(states or {})},
    }


# ============================================================ the grammar


def test_compute_keeps_select_and_routes_where_on_a_handle_to_predicate() -> None:
    """MF (M5): with `compute`, a `where` beside a live handle is the map's
    condition (not refused), and `select` reaches the body as the list it
    prints (lowered to a list `projection`, never refused as a rename)."""
    call = rewrite_call("map", {
        "input": LIVE, "where": {"attrs.kind": "COLD"},
        "compute": {"fill": "attrs.a / attrs.b"}, "select": ["node_id", "calc.fill"],
    })
    assert call.arguments["input"] == {"ref": LIVE}
    assert call.arguments["predicate"] == {"op": "eq", "field": "attrs.kind", "value": "COLD"}
    assert call.arguments["projection"] == ["node_id", "calc.fill"]
    assert "where" not in call.arguments
    assert not [n for n in call.adjusted if n.get("rule") == "map_without_compute"]


def test_without_compute_a_where_beside_a_handle_is_still_refused() -> None:
    """Regression: today's column-only map keeps its refusal."""
    with pytest.raises(GrammarRefusal):
        rewrite_call("map", {"input": LIVE, "where": {"attrs.kind": "COLD"}})


@pytest.mark.parametrize("extra,needle", [
    ({"where": {"calc.fill": {"op": "gt", "value": 0.5}}}, "filter(input="),
    ({"where": {"op": "not", "clause": {"op": "gt", "field": "calc.fill", "value": 1}}},
     "filter(input="),
    ({"order_by": "-calc.fill"}, 'order_by="-calc.fill"'),
    ({"projection": {"f": "attrs.kind"}}, "select=[...]"),
])
def test_compute_refusals_name_the_call_to_send(extra: dict, needle: str) -> None:
    """MF (failure case): `calc.` in `where`, `order_by` and `projection`
    beside `compute` are refused by the grammar, before any read."""
    with pytest.raises(GrammarRefusal) as raised:
        rewrite_call("map", {
            "where": {"type": "site"}, "compute": {"fill": "attrs.a / attrs.b"}, **extra,
        })
    assert needle in raised.value.fix_hint


def test_a_map_without_compute_is_rewritten_as_before() -> None:
    """6.9 (regression): the grammar's rewrite of an old map is unchanged and
    carries no note, so every retry it offers is still served as written."""
    call = rewrite_call("map", {"where": {"type": "page"}, "select": ["node_id"]})
    assert call.adjusted == ()
    assert call.arguments["projection"] == ["node_id"]


# ============================================================ the rows a map keeps


def test_computed_rows_keep_value_state_and_upstream_text_behind_nul_keys() -> None:
    """MF: values under `\\x00calc.<name>`, states that are not ok and the
    columns built from upstream text in the `\\x00calc` mark; the row's own
    fields are unchanged."""
    from okg.substrate.mcp import live_reader, map_compute

    rows = [
        _typed(0, 3, 1000), _typed(1, 5, 0),
        _typed(2, None, None, states={"static_used": "unavailable", "static_total": "unavailable"}),
    ]
    program = map_compute.first_read_program({
        "fill": "attrs.static_used / attrs.static_total", "kind": "lower(attrs.kind)",
        "n": "string(attrs.static_total)",
    })
    computed = map_compute.computed_rows(program, None, rows)
    first, second, third = computed.rows
    assert first["\x00calc.fill"] == 0.003 and first["\x00calc"] == {"s": {}, "u": ["kind"]}
    assert second["\x00calc.fill"] is None
    assert second["\x00calc"]["s"] == {"fill": "error:division_by_zero"}
    assert third["\x00calc"]["s"] == {"fill": "not read", "n": "not read"}
    assert first["attrs"] == rows[0]["attrs"] and live_reader.attrs_of(first) == rows[0]["attrs"]
    assert computed.columns["calc.fill"]["states"] == {
        "ok": 1, "error:division_by_zero": 1, "not read": 1,
    }
    why = computed.columns["calc.fill"]["not_computed"][1]["why"]
    assert why == "attrs.static_used unavailable"
    # The stored rows are JSON: no Decimal, NUL keys round-trip.
    assert json.loads(json.dumps(computed.rows)) == computed.rows


def test_a_raw_row_hides_its_computed_keys_from_its_upstream_fields() -> None:
    """MF: a raw row's `attrs` view and the condition evaluator's row never
    show the NUL keys; the evaluator reads the column as `calc.<name>`."""
    from okg.substrate.mcp import live_reader, map_compute

    program = map_compute.first_read_program({"double": "attrs.size * 2"})
    computed = map_compute.computed_rows(program, None, [{"name": "a", "size": 2}])
    row = computed.rows[0]
    assert live_reader.attrs_of(row) == {"name": "a", "size": 2}
    assert live_reader.evaluation_row(row) == {"attrs": {"name": "a", "size": 2}, "calc.double": 4}


def test_an_upstream_row_with_a_reserved_key_is_refused_and_not_kept() -> None:
    """MF (failure case): a raw upstream row carrying a NUL `calc` key
    would read as computed: map refuses it and the store does not keep it."""
    from okg.substrate.mcp import live_result_sets, map_compute

    program = map_compute.first_read_program({"double": "attrs.size * 2"})
    with pytest.raises(map_compute.MapComputeRefused) as raised:
        map_compute.computed_rows(program, None, [{"size": 2, "\x00calc.x": 1}])
    assert raised.value.data["reason"] == "reserved_key"
    with pytest.raises(live_result_sets.NotRetained):
        live_result_sets.capture([{"size": 2, "\x00calc.x": 1}])


# ============================================================ calc. names later


def test_calc_names_resolve_and_a_bare_name_equal_to_a_column_reads_it() -> None:
    """MF (6.5): `calc.x` is checked against the set's columns; a bare name
    equal to one reads it with a note; any other bare name is left alone."""
    from okg.substrate.mcp import map_compute

    names = map_compute.resolve_names(
        "filter", ("fill", "band"), predicate={"op": "gt", "field": "calc.fill", "value": 1},
        projection=["node_id", "fill", "attrs.kind", "other"], order_by="-fill",
    )
    assert names.projection == ["node_id", "calc.fill", "attrs.kind", "other"]
    assert names.order_by == "-calc.fill"
    assert names.read == {"predicate": ["fill"], "select": ["fill"], "order_by": ["fill"]}
    assert {n["rule"] for n in names.notes} == {"bare_name_is_calc_column"}
    grouped = map_compute.resolve_names(
        "aggregate", ("band", "x"), by=["band"], metrics=["sum(calc.x)", "count"],
    )
    assert grouped.by == ["calc.band"] and grouped.metrics == ["sum(calc.x)", "count"]
    with pytest.raises(map_compute.MapComputeRefused) as raised:
        map_compute.resolve_names("filter", ("fill",), order_by="calc.fil")
    assert raised.value.data["reason"] == "calc_column_unknown"


def test_set_aside_counts_rows_without_a_value() -> None:
    """MF: a row whose named computed column is not ok is set aside and
    counted by column and state."""
    from okg.substrate.mcp import map_compute

    program = map_compute.first_read_program({"fill": "attrs.static_used / attrs.static_total"})
    rows = map_compute.computed_rows(
        program, None, [_typed(0, 1, 2), _typed(1, 1, 0), _typed(2, 1, 4)],
    ).rows
    kept, aside = map_compute.set_aside(rows, ["fill"])
    assert [r["node_id"] for r in kept] == ["site:S0", "site:S2"]
    assert aside.report()["columns"] == {"calc.fill": {"error:division_by_zero": 1}}


def test_upstream_keys_name_the_fields_a_first_read_fetches() -> None:
    """MF (6.2): the resolver fetches the fields the expressions read (bare
    names too), never a `calc.` column."""
    from okg.substrate.mcp import live_resolver

    pairs = live_resolver.named_keys("map", {"compute": {
        "fill": "attrs.static_used / static_total", "again": "calc.fill * 2",
        "since": "days_between(date.attrs.created_at, '2026-10-21')",
    }})
    assert pairs == [
        ("compute", "static_used"), ("compute", "static_total"), ("compute", "created_at"),
    ]


def test_a_bare_name_equal_to_a_sibling_or_input_column_reads_it() -> None:
    """MF (6.5, in expressions): a bare name equal to another computed column
    reads it; a column's own name stays an upstream field."""
    from okg.substrate.mcp import map_expressions as expr

    program = expr.compile(
        {"fill": "attrs.a / attrs.b", "pct": "fill * 100", "a": "a + 1"},
        expr.ColumnSpec(),
    )
    by_name = {c.name: c for c in program.columns}
    assert by_name["pct"].depends == ("fill",) and by_name["pct"].operands == ()
    assert by_name["a"].operands == ("attrs.a",)
    over = expr.compile({"pct": "fill * 100"}, expr.ColumnSpec(calc=("fill",)))
    assert over.columns[0].operands == ("calc.fill",)


def test_date_meanings_and_raw_record_columns_are_refused_on_live_rows() -> None:
    """MF (failure case): `date.created` (a stored record's date) and
    `node_id` on raw rows would read null on every row: refused by name."""
    from okg.substrate.mcp import map_compute

    with pytest.raises(map_compute.MapComputeRefused) as raised:
        map_compute.refuse_operands(
            map_compute.first_read_program({"age": "days_between(date.created, '2026-01-01')"}),
            groups=False, typed=True,
        )
    assert raised.value.data["reason"] == "date_on_live_rows"
    with pytest.raises(map_compute.MapComputeRefused) as raised:
        map_compute.refuse_operands(
            map_compute.first_read_program({"n": "len(node_id)"}), groups=False, typed=False,
        )
    assert raised.value.data["reason"] == "record_column_on_raw_rows"


def test_the_computed_line_names_rows_without_a_value() -> None:
    """MF: the text line per column, `calc.fill: 2 ok · 1 not read (...)`."""
    from okg.substrate.mcp import map_compute

    line = map_compute.computed_line({"calc.fill": {
        "states": {"ok": 2, "not read": 1},
        "not_computed": [{"node_id": "site:A", "state": "not read",
                          "why": "attrs.static_used unavailable"}],
    }})
    assert line == ["calc.fill: 2 ok · 1 not read (site:A: attrs.static_used unavailable)"]


def test_metric_and_order_keys_read_a_computed_column() -> None:
    """MF: `sum(calc.x)`, `by=["calc.x"]` and `order_by` on `calc.x` read
    the kept value (`\\x00calc.x`), on typed and raw rows."""
    from okg.substrate.mcp import live_reader, live_result_sets

    assert live_result_sets.check_metrics(["sum(calc.x)"]) == [("sum", "\x00calc.x")]
    assert live_result_sets.metric_label(("sum", "\x00calc.x")) == "sum(calc.x)"
    rows = [{"a": 1, "\x00calc.x": 2, "\x00calc": {"s": {}, "u": []}},
            {"a": 2, "\x00calc.x": None, "\x00calc": {"s": {"x": "not read"}, "u": []}},
            {"a": 3, "\x00calc.x": 5, "\x00calc": {"s": {}, "u": []}}]
    ordered = live_reader.order_rows(rows, "\x00calc.x", True, "filter")
    assert [r["a"] for r in ordered] == [3, 1, 2]
    report = live_result_sets.group_report(
        [live_reader.flat_row(r) for r in (rows[0], rows[2])], ["calc.x"], [("count", None)],
    )
    assert [key for key, _ in report.groups] == [(2,), (5,)]


#: The expressions the reference guide's "Computed columns" section shows,
#: with the input each is written for (a groups handle or record rows).
GUIDE_EXAMPLES = [
    ({"fill": "attrs.used / attrs.total"}, None),
    ({"pb": "round(to_unit(sum(attrs.file_size), 'B', 'PB'), 3)"},
     ("count", "sum(attrs.file_size)")),
    ({"odd": "attrs['odd-key']", "age": "days_between(date.attrs.created_at, '2026-10-21')"},
     None),
    ({"label": "concat(lower(attrs.kind), '-', string(attrs.size))", "id": "len(node_id)"},
     None),
]


@pytest.mark.parametrize("compute,groups", GUIDE_EXAMPLES)
def test_the_guide_examples_compile(compute: dict, groups: Any) -> None:
    """24 (the guide's half; passes on the base, where the module exists):
    every expression the guide shows compiles over the input it is written
    for."""
    from okg.substrate.mcp import map_expressions as expr

    spec = expr.ColumnSpec(groups=groups) if groups else expr.ColumnSpec()
    program = expr.compile(compute, spec)
    assert [c.name for c in program.columns] == list(compute)


@pytest.mark.parametrize("name", ["node_id", "count", "subtype", "rank", "calc.count"])
def test_a_reserved_column_name_is_refused(name: str) -> None:
    """MF review L2 (failure case): a computed column named like a record
    column, a groups column or a word of the language is refused by name."""
    from okg.substrate.mcp import map_compute

    with pytest.raises(map_compute.MapComputeRefused) as raised:
        map_compute.first_read_program({name: "1"})
    assert raised.value.data["reason"] == "reserved_column_name"


def test_a_withheld_input_is_named_as_withheld() -> None:
    """MF review L3: an input whose state is `withheld` is named so."""
    from okg.substrate.mcp import map_compute

    program = map_compute.first_read_program({"fill": "attrs.static_used / attrs.static_total"})
    row = _typed(0, None, None, states={"static_used": "withheld"})
    computed = map_compute.computed_rows(program, None, [row])
    assert computed.columns["calc.fill"]["not_computed"][0]["why"] == "attrs.static_used withheld"


# ============================================================ #4181: the kept-handle block


def _kept_payload(template_chars: int) -> dict[str, Any]:
    return {
        "status": "refused", "operator_name": "map",
        "error": {
            "code": "invalid_operator_arguments",
            "message": "m" * 1000, "fix_hint": "f" * 1000,
            "data": {"reason": "map_output_too_large", "pad": "d" * 3000},
            "retry_call": {"tool": "map", "mode": "corrected",
                           "arguments": {"input": {"ref": LIVE}}},
            "kept_handle": {
                "id": LIVE, "rows": 3, "fields": ["attrs.a"], "upstream_calls": 3,
                "expires_at": 2.0, "note": "kept",
                "template": {"tool": "map", "arguments": {
                    "input": {"ref": LIVE}, "compute": {"x": "'" + "t" * template_chars + "'"},
                }},
            },
        },
    }


def test_a_kept_handle_block_shrinks_in_the_documented_order() -> None:
    """MF #4181 review L6: under byte fitting the block's `template` goes
    with `data` (step 1); the block itself goes last, after `retry_call`.
    Base: the template outlives `data` and the block is not handled."""
    import copy

    from okg.substrate.mcp import response_bounds as rb

    size = functools.partial(rb.public_call_tool_result_bytes, is_error=True)
    payload = _kept_payload(1500)
    step1 = copy.deepcopy(payload)
    step1["error"]["data"] = {"details_omitted": "response_byte_budget"}
    step1["error"]["kept_handle"] = {
        **{k: v for k, v in payload["error"]["kept_handle"].items() if k != "template"},
        "template_omitted": "response_byte_budget",
    }
    fitted = rb._fit_error_payload(copy.deepcopy(payload), budget=size(step1))
    assert fitted["error"]["message"] == "m" * 1000, "only step 1 was needed"
    assert "template" not in fitted["error"]["kept_handle"]
    assert fitted["error"]["kept_handle"]["template_omitted"] == "response_byte_budget"
    assert fitted["error"]["retry_call"]["tool"] == "map"
    last = copy.deepcopy(step1)
    last["error"]["message"] = "the request was refused by a typed substrate check"
    del last["error"]["fix_hint"], last["error"]["retry_call"]
    fitted = rb._fit_error_payload(copy.deepcopy(payload), budget=size(last))
    assert "retry_call" not in fitted["error"]
    assert fitted["error"]["kept_handle"]["id"] == LIVE, "the block outlives retry_call"


def test_a_large_template_is_left_out_of_the_kept_handle_block() -> None:
    """MF #4181 review L6: a template past 1,024 bytes is left out of the
    block, which keeps the id and counts. Base: no `kept_growth_block`."""
    import types

    from okg.substrate.mcp import live_resolver

    state = {
        "grow_from": "l1.src", "kept_now": True, "kept_id": LIVE,
        "set": types.SimpleNamespace(row_count=2), "calls_made": 2, "fetched": ["attrs.a"],
    }
    small = live_resolver.kept_growth_block(state, "map", {"compute": {"x": "attrs.a"}})
    assert small["template"]["arguments"] == {"input": {"ref": LIVE}, "compute": {"x": "attrs.a"}}
    large = live_resolver.kept_growth_block(
        state, "map", {"compute": {"x": "'" + "t" * 2000 + "'"}},
    )
    assert "template" not in large and large["template_omitted"] == "too_large"
    assert large["id"] == LIVE and large["upstream_calls"] == 2
