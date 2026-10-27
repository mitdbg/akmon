"""The expression module of ``map(compute=...)`` (PACT ``read-map-expressions``).

No database and no server: the module is pure. Every test fails on ``dev``
for one reason, the module does not exist (an import error), and passes on
the build. Rows use neutral names only (rule 22).
"""
from __future__ import annotations

import ast
import datetime as dt
import random
import re
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path

import pytest
import regex

from okg.substrate.mcp import map_expressions as mx
from okg.substrate.mcp.map_expressions import (
    Budget,
    ColumnSpec,
    ExpressionRefused,
    RowError,
    compile as compile_columns,
    store_number,
    to_number,
)

MODULE = Path(mx.__file__)

GROUPS = ColumnSpec(groups=(
    "attrs.site_kind", "count", "sum(attrs.file_size)", "max(attrs.size)",
    "min(attrs.size)", "rank", "occurrences", "match", "month(date.created)",
))


def rec(node_id: str = "site:alpha", fields: dict | None = None, **attrs):
    """A live typed row; ``fields`` holds field states."""

    return {
        "node_id": node_id, "subtype": "site", "attrs": attrs,
        "live": {"fields": dict(fields or {})},
    }


def run(expression: str, row=None, spec: ColumnSpec | None = None, states=None, name: str = "x"):
    """(state, value) of one column over one row."""

    program = compile_columns({name: expression}, spec)
    rows = [rec() if row is None else row]
    result = program.evaluate(rows, None if states is None else [states])
    column = result.columns[f"calc.{name}"]
    return column.states[0], column.values[0]


def value(expression: str, row=None, spec: ColumnSpec | None = None):
    state, got = run(expression, row, spec)
    assert state == "ok", (expression, state)
    return got


def refusal(expression: str, spec: ColumnSpec | None = None) -> ExpressionRefused:
    with pytest.raises(ExpressionRefused) as caught:
        compile_columns({"x": expression}, spec)
    return caught.value


# -- 1 -------------------------------------------------------------------------

#: Every expression the spec writes, with the row it runs on and its value.
DOCUMENTED = [
    ("attrs.static_used / attrs.static_total", rec(static_used=912, static_total=1000), None, 0.912),
    ("round(to_unit(attrs.static_total, 'B', 'PB'), 2)", rec(static_total=31_400_000_000_000_000), None, 31.4),
    ("coalesce(lower(string(attrs.keep_miniaod)), 'unset')", rec(keep_miniaod=True), None, "true"),
    ("coalesce(lower(string(attrs.keep_miniaod)), 'unset')", rec(), None, "unset"),
    ("coalesce(attrs.keep_miniaod, 'unset')", rec(), None, "unset"),
    ("attrs.used / attrs.total", rec(used=1, total=4), None, 0.25),
    ("attrs.static_used-attrs.static_total", rec(static_used=5, static_total=7), None, -2),
    ("days_between(date.created, '2026-10-21')", {"node_id": "t:1", "subtype": "t", "attrs": {},
                                                  "date.created": "2026-07-21"}, None, 92),
    ("round(to_unit(sum(attrs.file_size), 'B', 'PB'), 3)",
     {"count": 30, "sum(attrs.file_size)": 2_264_923_210_170_377}, GROUPS, 2.265),
    ("col['month(date.created)']", {"month(date.created)": "2026-10"}, GROUPS, "2026-10"),
    ("col['match']", {"match": "alpha"}, GROUPS, "alpha"),
    ("col['occurrences']", {"occurrences": 4}, GROUPS, 4),
    ("col['rank']", {"rank": 2}, GROUPS, 2),
    ("regex(attrs.name, '\\d+')", rec(name="site_42_north"), None, "42"),
    ("round(2.5)", rec(), None, 3),
    ("round(-2.5)", rec(), None, -3),
    ("-7 % 3", rec(), None, -1),
    ("null / 0", rec(), None, None),
    ("number(null)", rec(), None, None),
    ("round(1e37, 10)", rec(), None, 10 ** 37),
    ("calc.fill == 0.912", {**rec(), "calc.fill": 0.912}, ColumnSpec(calc=("fill",)), True),
]


@pytest.mark.parametrize("expression,row,spec,expected", DOCUMENTED)
def test_every_documented_expression_parses_and_evaluates(expression, row, spec, expected):
    assert value(expression, row, spec) == expected


def test_example_three_has_depth_three_and_is_accepted():
    program = compile_columns({"pb": "round(to_unit(sum(attrs.file_size), 'B', 'PB'), 3)"}, GROUPS)
    assert program.operands() == ["sum(attrs.file_size)"]


# -- 2 -------------------------------------------------------------------------


def test_precedence_parentheses_and_unchained_comparison():
    assert value("1 + 2 * 3") == 7
    assert value("(1 + 2) * 3") == 9
    assert value("10 - 4 - 3") == 3
    assert value("-2 * 3") == -6
    assert value("2 * -3") == -6
    assert value("not 1 < 2") is False
    assert value("1 < 2 and 2 < 3") is True
    assert value("true or false and false") is True
    assert value("(true or false) and false") is False
    assert value("1 + 1 == 2") is True
    for chained in ("1 < 2 < 3", "1 == 1 == true", "1 = 1 = 1"):
        assert refusal(chained).data["reason"] == "expression_syntax"


def test_a_single_equals_reads_as_double_with_a_note():
    program = compile_columns({"x": "attrs.a = 1"})
    assert program.evaluate([rec(a=1)]).columns["calc.x"].values == [True]
    assert any("'=' read as '=='" in note for note in program.notes)


def test_keywords_and_functions_are_case_insensitive_and_fields_are_not():
    assert value("ROUND(1.5) == 2 AND NOT FALSE") is True
    assert value("Coalesce(NULL, 3)") == 3
    assert value("attrs.Name", rec(Name="a", name="b")) == "a"


# -- 3 -------------------------------------------------------------------------


def test_an_unknown_function_is_refused_with_did_you_mean():
    refused = refusal("rond(attrs.a, 2)")
    assert refused.data["reason"] == "unknown_function"
    assert "did you mean round" in refused.message
    assert refusal("now()").data["reason"] == "unknown_function"
    assert refusal("text_of(attrs.a)").data["reason"] == "unknown_function"


# -- 4 -------------------------------------------------------------------------


def test_a_parse_error_names_the_column_position_and_caret():
    with pytest.raises(ExpressionRefused) as caught:
        compile_columns({"fill": "attrs.a / )"})
    refused = caught.value
    assert refused.data["reason"] == "expression_syntax"
    assert refused.data["column"] == "fill"
    assert refused.data["position"] == 11
    assert "calc.fill" in refused.message and "character 11" in refused.message
    assert refused.data["caret"] == "attrs.a / )\n          ^"
    assert refused.data["caret"] in refused.message


# -- 5 -------------------------------------------------------------------------


@pytest.mark.parametrize("expression", [
    "__import__('os')",
    "().__class__",
    "lambda: 1",
    "lambda x: x",
    "[x for x in y]",
    "{}",
    "1; 2",
    "`1`",
    "\x00",
    "attrs.a\x00",
    "'a\x00'",
    "attrs . a",
    "os.system('x')",
    "x.__class__",
    "1 if 2 else 3",
    "a ** 2",
])
def test_python_is_unreachable(expression):
    assert isinstance(refusal(expression), ExpressionRefused)


@pytest.mark.parametrize("expression", [
    "(" * 200 + "1" + ")" * 200, "-" * 399 + "1", "(" * 199 + "1" + ")" * 199, "not " * 99 + "true",
])
def test_deep_input_is_refused_by_name_never_a_recursion_error(expression, monkeypatch):
    # 401 characters is refused for its length first; with the length cap
    # lifted the depth rule refuses it on entry to level 21, never a
    # RecursionError.
    expected = "expression_too_long" if len(expression) > 400 else "too_deep"
    assert refusal(expression).data["reason"] == expected
    monkeypatch.setattr(mx, "MAX_EXPRESSION_CHARS", 100_000)
    assert refusal(expression * 1).data["reason"] == "too_deep"
    assert refusal("(" * 5_000 + "1" + ")" * 5_000).data["reason"] == "too_deep"


def test_a_dunder_key_is_a_plain_dict_key_never_an_attribute():
    assert value("attrs.__class__", rec()) is None
    assert value("attrs.__class__", rec(__class__="kept")) == "kept"
    assert value("is_null(attrs.__dict__)", rec()) is True


class _Leaky(dict):
    leak = "an attribute"

    @property
    def secret(self):  # pragma: no cover - must never run
        raise AssertionError("an attribute was read")


class _ObjectRow:
    node_id = "site:alpha"
    subtype = "site"
    attrs = {"a": 1}

    def __getattr__(self, name):  # pragma: no cover - must never run
        raise AssertionError(f"attribute {name} was read")


def test_rows_and_values_with_attributes_yield_only_dict_values():
    leaky = {"node_id": "site:alpha", "subtype": "site", "attrs": _Leaky(k=7), "live": {}}
    assert value("attrs.k", leaky) == 7
    assert value("attrs.leak", leaky) is None
    assert value("attrs.secret", leaky) is None
    assert value("node_id", _ObjectRow()) is None
    assert value("attrs.a", _ObjectRow()) is None
    assert run("attrs.obj", rec(obj=_ObjectRow())) == ("error:bad_argument", None)
    assert run("string(attrs.obj)", rec(obj=_ObjectRow())) == ("error:bad_argument", None)


def test_the_module_calls_no_python_evaluation_or_attribute_builtin():
    tree = ast.parse(MODULE.read_text())
    forbidden = {"getattr", "setattr", "delattr", "hasattr", "eval", "exec", "__import__",
                 "vars", "globals", "locals", "open", "breakpoint", "input"}
    used = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    assert not used & forbidden
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
    assert imported <= {
        "__future__", "datetime", "decimal", "difflib", "re", "time", "warnings", "dataclasses",
        "typing", "regex", "okg.substrate.errors", "okg.substrate.external_backed.live_dispatch",
        "okg.substrate.mcp.live_calls", "okg.substrate.mcp.handle_fields",
    }


def test_the_module_imports_nothing_from_the_server():
    code = (
        "import sys; import okg.substrate.mcp.map_expressions; "
        "print('okg.substrate.mcp.server' in sys.modules)"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "False"


# -- 6 -------------------------------------------------------------------------

_NUMBERS = ["0", "1", "2", "7", "2.5", "0.1", "1e-30", "1e37", "9" * 38, "12345678901234567890123456789012345678",
            "0.000001", "3.14159265358979323846"]
_ROW_VALUES = [0, 1, -1, 7, 2.5, 0.1, 1e-30, 1e37, 10 ** 38 - 1, -(10 ** 37), Decimal("1e-30"), float("nan"),
               float("inf"), "12", "abc", "", "TRUE", "1e37", True, False, None, "2026-10-21", "x" * 300,
               dt.datetime(2026, 10, 21, 9, 40), [1, 2], {"k": 1}]
_FIELDS = ["attrs.a", "attrs.b", "attrs.t", "node_id", "subtype", "attrs['odd-key']", "date.attrs.d", "c"]
_FUNCS = [(name, low if high is None else random.Random(name).randint(low, high))
          for name, (low, high) in mx.FUNCTIONS.items() if name not in ("regex", "to_unit")]


def _random_expression(rng: random.Random, depth: int) -> str:
    if depth <= 0 or rng.random() < 0.3:
        pick = rng.random()
        if pick < 0.4:
            return rng.choice(_NUMBERS)
        if pick < 0.75:
            return rng.choice(_FIELDS)
        return rng.choice(["'abc'", "'12'", "'2026-10-21'", "null", "true", "false", "'TRUE'", "''"])
    shape = rng.random()
    if shape < 0.35:
        op = rng.choice(["+", "-", "*", "/", "%", "==", "!=", "<", "<=", ">", ">=", "and", "or"])
        return f"({_random_expression(rng, depth - 1)} {op} {_random_expression(rng, depth - 1)})"
    if shape < 0.45:
        return f"-{_random_expression(rng, depth - 1)}"
    if shape < 0.5:
        return f"not {_random_expression(rng, depth - 1)}"
    if shape < 0.55:
        return f"regex({_random_expression(rng, depth - 1)}, '{rng.choice(['a', '[0-9]+', '(b)c?', '^x'])}')"
    if shape < 0.6:
        return f"to_unit({_random_expression(rng, depth - 1)}, 'KiB', 'PB')"
    name, arity = rng.choice(_FUNCS)
    args = ", ".join(_random_expression(rng, depth - 1) for _ in range(arity))
    return f"{name}({args})"


def _random_row(rng: random.Random):
    attrs = {key: rng.choice(_ROW_VALUES) for key in ("a", "b", "t", "odd-key", "d", "c")}
    states = {key: rng.choice(["live", "live", "live", "absent", "unavailable", "denied"]) for key in attrs}
    return rec(fields=states, **attrs)


def _assert_clean(result) -> None:
    for column in result.columns.values():
        for got, state in zip(column.values, column.states):
            assert not isinstance(got, Decimal)
            assert got is None or isinstance(got, (bool, int, float, str))
            assert state in ("ok", "not read") or state[len("error:"):] in mx.ROW_ERROR_KINDS
            if state != "ok":
                assert got is None


def test_a_fuzz_run_raises_only_refusals_or_row_errors():
    rng = random.Random(20261021)
    alphabet = "abcdeilmnorstux_.0123456789 +-*/%()[]'\",=<>!\\`;{}:#@$&|^~?\x00\téü"
    words = ["attrs.", "calc.", "date.", "col[", "and ", "or ", "not ", "round(", "regex(", "sum(", "'", "1e37"]
    for _ in range(10_000):
        parts = [rng.choice(words) if rng.random() < 0.2 else rng.choice(alphabet) for _ in range(rng.randint(0, 60))]
        text = "".join(parts)
        try:
            program = compile_columns({"x": text})
        except ExpressionRefused:
            continue
        _assert_clean(program.evaluate([_random_row(rng) for _ in range(3)]))
    compiled = 0
    for _ in range(10_000):
        text = _random_expression(rng, rng.randint(1, 5))
        try:
            program = compile_columns({"x": text})
        except ExpressionRefused:
            continue
        compiled += 1
        _assert_clean(program.evaluate([_random_row(rng) for _ in range(3)]))
    assert compiled > 5_000


# -- 7 -------------------------------------------------------------------------


def test_limits_refuse_before_evaluation():
    nine = {f"c{i}": "1" for i in range(9)}
    with pytest.raises(ExpressionRefused) as caught:
        compile_columns(nine)
    assert caught.value.data["reason"] == "too_many_columns"
    assert refusal("1" + " " * 400).data["reason"] == "expression_too_long"
    assert refusal("abs(" * 21 + "1" + ")" * 21).data["reason"] == "too_deep"
    assert value("abs(" * 20 + "1" + ")" * 20) == 1
    assert refusal("'" + "a" * 201 + "'").data["reason"] == "literal_too_long"
    assert value("'" + "a" * 200 + "'") == "a" * 200
    assert value("+".join(["1"] * 60)) == 60
    assert value("+".join(["1"] * 99)) == 99
    assert refusal("+".join(["1"] * 100)).data["reason"] == "too_many_nodes"
    with pytest.raises(ExpressionRefused) as caught:
        compile_columns({"a": "1", "b": "2", "c": "3"}, ColumnSpec(calc=tuple(f"k{i}" for i in range(30))))
    assert caught.value.data["reason"] == "too_many_calc_columns"
    with pytest.raises(ExpressionRefused) as caught:
        compile_columns({"x": "attrs.a"}).evaluate([{}] * 50_001)
    assert caught.value.data == {**caught.value.data, "reason": "too_many_rows", "rows": 50_001}


#: How many times slower a line tracer makes this pure-Python evaluator. The
#: coverage-linux and nightly-full-ci lanes run under ``--cov`` (branch
#: coverage); the per-PR lanes run with ``--no-cov``. Measured on a lab host,
#: 2026-10-25: the chain below takes 1.1-1.2 s untraced and about 5.6 s under
#: ``--cov=okg.substrate`` (a 5 s budget stopped it at row 1697-1833 of 2000).
TRACED_SLOWDOWN = 10.0


def _line_tracer_active() -> bool:
    """True when coverage (or a debugger) traces this thread."""
    if sys.gettrace() is not None:
        return True
    monitoring = getattr(sys, "monitoring", None)
    return monitoring is not None and monitoring.get_tool(monitoring.COVERAGE_ID) is not None


def test_a_maximum_length_same_level_chain_evaluates_within_budget():
    longest = "+".join(["attrs.a1"] * 44)
    assert len(longest) <= 400 and len("+".join(["attrs.a1"] * 45)) > 400
    widest = "+".join(["a"] * 99)  # 100 nodes, bare names
    rows = [rec(a1=i, a=i) for i in range(2_000)]
    # Untraced, the production budget holds as is. Under a line tracer the same
    # work is measured against the budget scaled by TRACED_SLOWDOWN, so the
    # coverage lanes still check the values and still catch a gross slowdown,
    # while the 5 s claim itself is gated by the untraced per-PR lanes.
    scale = TRACED_SLOWDOWN if _line_tracer_active() else 1.0
    budget = Budget(seconds=Budget().seconds * scale)
    started = time.monotonic()
    result = compile_columns({"longest": longest, "widest": widest}).evaluate(rows, budget=budget)
    elapsed = time.monotonic() - started
    assert result.columns["calc.longest"].values[3] == 44 * 3
    assert result.columns["calc.widest"].values[1_999] == 99 * 1_999
    assert elapsed < budget.seconds


# -- 8 -------------------------------------------------------------------------


def test_keys_split_on_minus_and_odd_keys_need_brackets():
    row = rec(**{"static_used": 3, "static_total": 10, "odd-key": 5, "a.b": "dotted", "with space": "s"})
    assert value("attrs.static_used-attrs.static_total", row) == -7
    assert value("attrs['odd-key'] * 2", row) == 10
    assert value("attrs.a.b", row) == "dotted"
    assert value("attrs[\"with space\"]", row) == "s"
    assert value("attrs.odd-key", rec(odd=9, key=None)) is None  # attrs.odd minus a bare key: null
    assert compile_columns({"x": "attrs['odd-key']"}).operands() == ["attrs.odd-key"]


# -- 9 -------------------------------------------------------------------------


def test_col_names_any_groups_column_and_is_refused_elsewhere():
    assert value("col['rank'] + 1", {"rank": 2}, GROUPS) == 3
    assert value("col['sum(attrs.file_size)']", {"sum(attrs.file_size)": 9}, GROUPS) == 9
    assert refusal("col['rank']").data["reason"] == "col_needs_groups"
    unknown = refusal("col['rnak']", GROUPS)
    assert unknown.data["reason"] == "unknown_column" and "did you mean rank" in unknown.message


# -- 10 ------------------------------------------------------------------------


def test_null_propagates_and_logic_is_three_valued():
    assert value("attrs.missing + 1") is None
    assert value("attrs.missing > 1") is None
    assert value("lower(attrs.missing)") is None
    assert value("null and false") is False
    assert value("null and true") is None
    assert value("null or true") is True
    assert value("null or false") is None
    assert value("not null") is None
    assert value("if(null, 1, 2)") == 2
    assert value("if(attrs.missing > 0, 'yes', 'no')") == "no"
    assert value("concat('a', null, 'b')") == "ab"
    assert value("is_null(null)") is True
    assert value("coalesce(null, attrs.missing, 3)") == 3
    assert value("if(attrs.b == 0, null, attrs.a / attrs.b)", rec(a=1, b=0)) is None


# -- 11 ------------------------------------------------------------------------


def test_not_read_is_never_turned_into_a_value():
    for state in ("unavailable", "denied"):
        row = rec(fields={"keep": state})
        assert run("coalesce(attrs.keep, 'unset')", row) == ("not read", None)
        assert run("is_null(attrs.keep)", row) == ("not read", None)
    assert run("coalesce(attrs.keep, 'unset')", rec(), states={"attrs.keep": "withheld"}) == ("not read", None)
    program = compile_columns({"a": "coalesce(attrs.keep, 'unset')", "b": "coalesce(calc.a, 'x')"})
    result = program.evaluate([rec(fields={"keep": "denied"}), rec()])
    assert result.columns["calc.a"].states == ["not read", "ok"]
    assert result.columns["calc.b"].states == ["not read", "ok"]
    assert result.columns["calc.a"].counts == {"not read": 1, "ok": 1}
    prior = compile_columns({"y": "coalesce(calc.fill, 0)"}, ColumnSpec(calc=("fill",)))
    out = prior.evaluate([rec()], [{"calc.fill": "error:division_by_zero"}])
    assert out.columns["calc.y"].states == ["error:division_by_zero"]
    absent = compile_columns({"y": "coalesce(attrs.k, 'none')"}).evaluate([rec(k=1)], [{"attrs.k": "absent"}])
    assert absent.columns["calc.y"].values == ["none"]
    # Review item 2: date.attrs.<k> takes the state of live.fields[k] too.
    gone = rec(fields={"x": "unavailable"}, x="2026-01-01")
    assert run("coalesce(date.attrs.x, '2026-01-01')", gone) == ("not read", None)
    assert run("coalesce(days_between(date.attrs.x, '2026-01-02'), -1)", gone) == ("not read", None)
    assert run("coalesce(date.attrs.x, 'a')", rec(x="2026-01-01"), states={"attrs.x": "denied"}) == ("not read", None)
    # Review item 6: an unknown or malformed state is refused by name.
    for bad in ("maybe", ["x"], 3):
        with pytest.raises(ExpressionRefused) as caught:
            compile_columns({"y": "attrs.k"}).evaluate([rec()], [{"attrs.k": bad}])
        assert caught.value.data["reason"] == "internal_live_state"
        with pytest.raises(ExpressionRefused) as caught:
            compile_columns({"y": "attrs.k"}).evaluate([rec(fields={"k": bad})])
        assert caught.value.data["reason"] == "internal_live_state"


# -- 12 ------------------------------------------------------------------------


def test_decimal_signals_are_row_errors():
    assert value("attrs.x / 0") is None
    assert value("attrs.x % 0") is None
    assert run("attrs.x / 0", rec(x=5)) == ("error:division_by_zero", None)
    assert run("attrs.x % 0", rec(x=5)) == ("error:division_by_zero", None)
    assert run("attrs.x / 0.0", rec(x=0)) == ("error:division_by_zero", None)
    assert run("attrs.x % 0", rec(x=2.5)) == ("error:division_by_zero", None)
    assert run("1e37 % 1e-30") == ("error:number_too_large", None)
    assert run("attrs.x % 1e-30", rec(x="1e37")) == ("error:number_too_large", None)
    assert run("attrs.x * attrs.x", rec(x="1e-999990")) == ("error:number_too_small", None)
    assert run("1 / attrs.x", rec(x="1e-999990")) == ("error:number_too_large", None)
    assert run("attrs.x / attrs.y", rec(x=10 ** 37, y="0.0000001")) == ("error:number_too_large", None)


# -- 13 ------------------------------------------------------------------------


def test_the_range_is_checked_after_every_operation():
    big = "12345678901234567890123456789012345678"
    assert len(big) == 38
    assert run(f"{big} * 100 / 100") == ("error:number_too_large", None)
    assert run(f"{'9' * 38} + 1") == ("error:number_too_large", None)
    assert run(f"{big} * 10") == ("error:number_too_large", None)
    assert value(f"{big} + {big}") == 2 * int(big)  # below 10^38: exact
    assert run(f"{'9' * 38} + 0.5") == ("error:number_too_large", None)
    assert value(f"{big} - 1") == int(big) - 1
    assert value("attrs.a + 1", rec(a=10 ** 37)) == 10 ** 37 + 1
    assert value("attrs.a * 3", rec(a=3 * 10 ** 36 + 7)) == 9 * 10 ** 36 + 21
    assert run("-attrs.a - 1", rec(a=10 ** 38 - 1)) == ("error:number_too_large", None)
    assert run("abs(attrs.a)", rec(a=-(10 ** 38))) == ("error:number_too_large", None)
    assert refusal("1" + "0" * 38).data["reason"] == "number_too_large"
    assert run("attrs.a", rec(a="1" + "0" * 38)) == ("ok", "1" + "0" * 38)  # text stays text
    assert run("number(attrs.a)", rec(a="1" + "0" * 38)) == ("error:number_too_large", None)


# -- 14 ------------------------------------------------------------------------


def test_modulo_truncates_and_round_halves_away_from_zero():
    assert value("-7 % 3") == -1
    assert value("7 % -3") == 1
    assert value("-7.5 % 2") == -1.5
    assert value("round(2.5)") == 3
    assert value("round(-2.5)") == -3
    assert value("round(0.5)") == 1
    assert value("round(1.25, 1)") == 1.3
    assert value("round(-1.25, 1)") == -1.3
    assert value("round(1234, -2)") == 1200
    assert value("round(1250, -2)") == 1300
    assert value("floor(-1.5)") == -2 and value("ceil(-1.5)") == -1
    # R7: a value that needs no rounding is returned unchanged.
    assert value("round(1e37, 10)") == 10 ** 37
    assert value("round(attrs.a, 10)", rec(a=1.5)) == 1.5
    assert value("round(attrs.a, 3)", rec(a="123456789012345678901234567890.5")) == pytest.approx(1.23456789012346e29)
    assert run("round(1.5, attrs.d)", rec(d=11)) == ("error:bad_argument", None)
    assert run("round(1.5, attrs.d)", rec(d=1.5)) == ("error:bad_argument", None)
    assert refusal("round(1.5, 11)").data["reason"] == "bad_argument"
    assert refusal("round(1.5, -11)").data["reason"] == "bad_argument"
    assert isinstance(value("round(2.4)"), int)


# -- 15 ------------------------------------------------------------------------


def test_a_column_stores_an_int_or_a_fifteen_digit_float_and_a_copied_value_matches():
    rows = [rec(a=1, b=3), rec(a=2, b=3), rec(a=912, b=1000), rec(a=6, b=3), rec(a=1, b=7)]
    result = compile_columns({"r": "attrs.a / attrs.b"}).evaluate(rows)
    stored = result.columns["calc.r"].values
    assert stored == [0.333333333333333, 0.666666666666667, 0.912, 2, 0.142857142857143]
    assert isinstance(stored[3], int)
    for got in stored:
        assert not isinstance(got, Decimal)
        if isinstance(got, float):
            assert len(repr(got).replace("0.", "", 1).lstrip("0")) <= 15
            assert to_number(got) == Decimal(repr(got))
    assert to_number(0.333333333333333) == Decimal("0.333333333333333")
    assert store_number(Decimal("2.0000000000000004")) == 2
    assert store_number(Decimal("0.1234567890123455")) == 0.123456789012346
    assert store_number(Decimal("1e-38")) == 1e-38
    assert run("attrs.a * 1", rec(a="1e-39")) == ("error:number_too_small", None)
    with pytest.raises(RowError) as caught:
        store_number(Decimal("-1e-40"))
    assert caught.value.kind == "number_too_small"
    # Review item 5: rounding to 15 digits can reach 10^38; the range is checked after it.
    for edge in (Decimal("99999999999999999999999999999999999999.5"), Decimal("1E+38"), 10 ** 38):
        with pytest.raises(RowError) as caught:
            store_number(edge)
        assert caught.value.kind == "number_too_large"
    copied = compile_columns({"m": "calc.fill == 0.912"}, ColumnSpec(calc=("fill",)))
    assert copied.evaluate([{**rec(), "calc.fill": stored[2]}]).columns["calc.m"].values == [True]
    assert value("string(1 / 3)") == "0.333333333333333"


# -- 16 ------------------------------------------------------------------------


def test_upstream_floats_read_through_their_text_and_nan_inf_are_not_numbers():
    assert value("attrs.x * 3", rec(x=0.1)) == 0.3
    assert value("attrs.x + attrs.y", rec(x=0.1, y=0.2)) == 0.3
    assert to_number(0.1) == Decimal("0.1")
    assert to_number(2.0) == 2 and isinstance(to_number(2.0), int)
    assert to_number(1e22) == 10 ** 22
    for bad in (float("nan"), float("inf"), float("-inf"), "nan", "inf", "Infinity", "1_000", "x" * 65,
                "1" * 65):
        with pytest.raises(RowError) as caught:
            to_number(bad)
        assert caught.value.kind == "not_a_number", bad
        assert run("attrs.x + 1", rec(x=bad)) == ("error:not_a_number", None)
    with pytest.raises(RowError) as caught:
        to_number(1e38)
    assert caught.value.kind == "number_too_large"


# -- 17 ------------------------------------------------------------------------


def test_booleans_are_not_numbers_and_number_abc_is_an_error():
    assert run("attrs.flag + 1", rec(flag=True)) == ("error:not_a_number", None)
    assert run("-attrs.flag", rec(flag=False)) == ("error:not_a_number", None)
    assert run("number(attrs.flag)", rec(flag=True)) == ("error:not_a_number", None)
    for literal in ("true + 1", "1 * false", "-true", "1 - true"):
        assert refusal(literal).data["reason"] == "expression_syntax"
    assert run("number('abc')") == ("error:not_a_number", None)
    assert value("number(null)") is None
    assert value("number(' 12 ')") == 12
    with pytest.raises(RowError):
        to_number(True)


# -- 18 ------------------------------------------------------------------------


def test_text_numbers_and_booleans_coerce_by_the_part1_rule():
    assert value("'12' > 9") is True
    assert value("attrs.n == 12", rec(n="12")) is True
    assert value("attrs.n + 1", rec(n="12.5")) == 13.5
    assert value("'TRUE' == true") is True
    assert value("attrs.f == false", rec(f="False")) is True
    # Decision Q5: kinds that cannot be compared are bad_argument, never null.
    assert run("'yes' == true") == ("error:bad_argument", None)
    assert value("'abc' == 'abc'") is True
    assert run("'abc' < 5") == ("error:bad_argument", None)
    assert run("attrs.n < 'abc'", rec(n=5)) == ("error:bad_argument", None)
    assert run("if(attrs.n < 'abc', 'yes', 'no')", rec(n=5)) == ("error:bad_argument", None)
    assert value("'10' < '9'") is True
    assert run("1 == true") == ("error:bad_argument", None)
    assert run("true == 1") == ("error:bad_argument", None)
    assert value("if('true', 1, 2)") == 1


# -- 19 ------------------------------------------------------------------------


@pytest.mark.parametrize("pattern,reason", [
    ("(a+)+", "regex_backtracks"),
    ("(a|aa)*", "regex_backtracks"),
    ("(?:abc){e<=2}", "regex_dialect"),
    ("(?:abc){e}", "regex_dialect"),
    ("(?:abc){1<=e<=3}", "regex_dialect"),
    ("(?:abc){2i+2d+1s<=4}", "regex_dialect"),
    ("[[:alpha:]]", "regex_dialect"),
    ("[[=a=]]", "regex_dialect"),
    ("\\p{L}", "regex_dialect"),
    ("(?R)", "regex_dialect"),
    ("(?|a)", "regex_dialect"),
    ("a\\B", "regex_dialect"),  # re and regex differ on empty text (found by the fuzz check)
    ("a{100000000}", "regex_too_complex"),  # the regex library expands a count at compile
    ("a{2,100000}", "regex_too_complex"),
    ("(?:ab){1001}", "regex_too_complex"),
    ("(", "regex_dialect"),
])
def test_a_backtracking_or_other_dialect_regex_is_refused_by_name(pattern, reason):
    assert refusal(f"regex(attrs.s, '{pattern}')").data["reason"] == reason


def test_a_backtracking_regex_is_refused_a_slow_match_is_a_row_error_and_the_call_budget_refuses():
    assert refusal("regex(attrs.s, '(a+)+$')").data["reason"] == "regex_backtracks"
    assert refusal("regex(attrs.s, attrs.p)").data["reason"] == "bad_argument"
    slow = compile_columns({"m": "regex(attrs.s, 'a.*a.*a.*[bc]')"})
    text = "a" * 60_000
    # R6: the regex library's TimeoutError becomes the row error.
    one = slow.evaluate([rec(s=text), rec(s="aaab")])
    assert one.columns["calc.m"].states == ["error:regex_timeout", "ok"]
    assert one.columns["calc.m"].values == [None, "aaab"]
    assert one.regex_timeouts == 1
    with pytest.raises(ExpressionRefused) as caught:
        slow.evaluate([rec(s=text)] * 150)
    assert caught.value.data["reason"] == "regex_budget"
    assert run("regex(attrs.s, 'a')", rec(s="a" * (64 * 1024 + 1))) == ("error:value_too_long", None)
    assert run("regex(attrs.s, 'é')", rec(s="é" * 40_000)) == ("error:value_too_long", None)


def test_regex_groups_flags_and_no_match():
    row = rec(s="Site_42_North")
    assert value("regex(attrs.s, '(?i)site_(\\d+)', 1)", row) == "42"
    assert value("regex(attrs.s, '(?P<n>\\d+)', 'n')", row) == "42"
    assert value("regex(attrs.s, 'zzz')", row) is None
    assert value("regex(attrs.s, 'x\\{2\\}|_4')", row) == "_4"
    assert refusal("regex(attrs.s, '(a)', 2)").data["reason"] == "bad_argument"
    assert refusal("regex(attrs.s, '(a)', 'nope')").data["reason"] == "bad_argument"


# -- 20 ------------------------------------------------------------------------


def test_unknown_escapes_are_kept_as_written():
    assert value("'\\d+'") == "\\d+"
    assert len(value("'\\d+'")) == 3
    assert value("'a\\'b'") == "a'b"
    assert value('"a\\"b"') == 'a"b'
    assert value("'a\\\\b'") == "a\\b"
    assert value("'\\n'") == "\\n"
    assert value("\"it's\"") == "it's"


# -- 21 ------------------------------------------------------------------------


def test_date_literals_read_as_utc_and_bad_text_is_bad_date():
    row = {"node_id": "t:1", "subtype": "t", "attrs": {"d": "2026-10-21 06:00:00"}, "date.created": "2026-10-21"}
    assert value("days_between('2026-10-20', '2026-10-21')", row) == 1
    assert value("days_between(date.created, '2026-10-21T12:00:00Z')", row) == 0.5
    assert value("days_between(date.created, date.attrs.d)", row) == 0.25
    assert value("date.created < '2026-10-22'", row) is True
    assert value("date.created == '2026-10-21T00:00:00Z'", row) is True
    assert value("date.created", row) == "2026-10-21T00:00:00Z"
    assert value("days_between('2026-10-21T02:00:00+02:00', '2026-10-21')", row) == 0
    for bad in ("'yesterday'", "'2026-13-01'", "'2026-10-21T25:00:00Z'", "3"):
        assert run(f"date.created < {bad}", row)[0] == "error:bad_date", bad
    assert run("days_between(date.created, 'soon')", row) == ("error:bad_date", None)
    # Review item 3: date.<name> takes only the named meanings.
    unknown = refusal("date.creatd")
    assert unknown.data["reason"] == "unknown_date_field" and "did you mean created" in unknown.message
    assert refusal("date.created_at").data["reason"] == "unknown_date_field"
    assert value("date.modified", {**row, "date.modified": "2026-10-20"}) == "2026-10-20T00:00:00Z"
    aware = {**row, "date.created": dt.datetime(2026, 10, 21, tzinfo=dt.timezone.utc)}
    assert value("days_between(date.created, '2026-10-22')", aware) == 1
    assert run("date.created + 1", row) == ("error:not_a_number", None)


# -- 22 ------------------------------------------------------------------------


def test_to_unit_split_part_substr_days_between():
    assert value("to_unit(2264923210170377, 'B', 'PB')") == 2.26492321017038
    assert value("to_unit(1, 'MiB', 'KiB')") == 1024
    assert value("to_unit(1, 'EiB', 'B')") == 1024 ** 6
    unit = refusal("to_unit(1, 'B', 'Pb')")
    assert unit.data["reason"] == "unknown_unit" and "did you mean PB" in unit.message
    assert refusal("to_unit(1, attrs.u, 'PB')").data["reason"] == "bad_argument"
    assert value("split_part('a_b_c', '_', 2)") == "b"
    assert value("split_part('a_b_c', '_', -1)") == "c"
    assert value("split_part('a_b_c', '_', 4)") is None
    assert value("split_part('a_b_c', '_', -4)") is None
    assert refusal("split_part('a_b_c', '_', 0)").data["reason"] == "bad_argument"
    assert refusal("split_part('a_b_c', '', 1)").data["reason"] == "bad_argument"
    assert run("split_part('a_b_c', '_', attrs.n)", rec(n=0)) == ("error:bad_argument", None)
    assert run("split_part('a_b_c', attrs.s, 1)", rec(s="")) == ("error:bad_argument", None)
    assert value("substr('hello', 2, 3)") == "ell"
    assert value("substr('hello', 2)") == "ello"
    assert value("substr('hello', 9)") == ""
    assert value("substr('hello', 1, 0)") == ""
    assert refusal("substr('hello', 0)").data["reason"] == "bad_argument"
    assert refusal("substr('hello', -1)").data["reason"] == "bad_argument"
    assert refusal("substr('hello', 1, -1)").data["reason"] == "bad_argument"
    assert run("substr('hello', attrs.n)", rec(n=0)) == ("error:bad_argument", None)
    assert run("substr('hello', 1, attrs.n)", rec(n=-1)) == ("error:bad_argument", None)
    assert run("substr('hello', attrs.n)", rec(n=1.5)) == ("error:bad_argument", None)
    assert value("days_between('2026-01-01', '2026-10-21')") == 293
    assert value("len('héllo') + len('')") == 5
    assert value("starts_with('alpha', 'al') and ends_with('alpha', 'ha') and contains('alpha', 'ph')") is True
    assert value("upper('ab') == 'AB' and lower('AB') == 'ab'") is True
    assert value("concat(1, '-', 2.5, '-', true)") == "1-2.5-true"
    assert value("min(3, 1, 2) + max(3, 1, 2)") == 4
    assert value("min('b', 'a')") == "a"
    assert run("min('b', 1)") == ("error:bad_argument", None)
    assert run("lower(attrs.n)", rec(n=5)) == ("error:bad_argument", None)
    assert run("upper(attrs.s)", rec(s="ß" * 2_049)) == ("error:value_too_long", None)
    assert run("attrs.s", rec(s="x" * 4_097)) == ("error:value_too_long", None)


# -- 23 ------------------------------------------------------------------------


def test_columns_compute_in_dependency_order_and_a_cycle_is_refused():
    program = compile_columns({"c": "calc.b + calc.a", "b": "calc.a * 2", "a": "attrs.x + 1"})
    assert [column.name for column in program.columns] == ["a", "b", "c"]
    result = program.evaluate([rec(x=1)])
    assert [result.columns[f"calc.{n}"].values[0] for n in "abc"] == [2, 4, 6]
    for compute in ({"a": "calc.b", "b": "calc.a"}, {"a": "calc.a + 1"}, {"a": "calc.b", "b": "calc.c", "c": "calc.a"}):
        with pytest.raises(ExpressionRefused) as caught:
            compile_columns(compute)
        assert caught.value.data["reason"] == "compute_cycle"
        assert "->" in caught.value.message
    assert refusal("calc.nope").data["reason"] == "unknown_column"
    renamed = compile_columns({"calc.fill": "1"})
    assert renamed.columns[0].name == "fill" and renamed.notes
    with pytest.raises(ExpressionRefused) as caught:
        compile_columns({"Fill": "1"})
    assert caught.value.data["reason"] == "bad_column_name"


# -- 24 ------------------------------------------------------------------------


def test_a_bare_name_reads_as_attrs_with_a_note_and_is_reported():
    program = compile_columns({"fill": "static_used / static_total"})
    assert program.bare_names == {"static_used": "attrs.static_used", "static_total": "attrs.static_total"}
    assert program.operands() == ["attrs.static_used", "attrs.static_total"]
    assert any("bare name 'static_used' read as attrs.static_used" in note for note in program.notes)
    assert program.evaluate([rec(static_used=1, static_total=4)]).columns["calc.fill"].values == [0.25]
    assert value("node_id", rec("site:beta")) == "site:beta"
    assert compile_columns({"x": "node_id"}).bare_names == {}
    assert refusal("and").data["reason"] == "expression_syntax"


# -- 25 ------------------------------------------------------------------------


def test_metric_names_resolve_only_over_a_groups_handle():
    for metric in ("count", "sum(attrs.x)", "min(attrs.x)", "max(x)", "COUNT + 1"):
        refused = refusal(metric)
        assert refused.data["reason"] == "metric_needs_groups", metric
        assert "aggregate first, or give two or more values" in refused.message
    assert refusal("min(1)").data["reason"] == "metric_needs_groups"
    assert value("min(attrs.x, attrs.y)", rec(x=3, y=2)) == 2
    row = {"count": 5, "max(attrs.size)": 9, "min(attrs.size)": 1, "attrs.site_kind": "disk"}
    assert value("count * 2", row, GROUPS) == 10
    assert value("max(attrs.size) - min(attrs['size'])", row, GROUPS) == 8
    assert value("attrs.site_kind", row, GROUPS) == "disk"
    assert value("site_kind", row, GROUPS) == "disk"
    assert refusal("sum(attrs.other)", GROUPS).data["reason"] == "unknown_column"
    assert refusal("attrs.other", GROUPS).data["reason"] == "unknown_column"
    assert refusal("sum(1)", GROUPS).data["reason"] == "expression_syntax"


# -- 26 ------------------------------------------------------------------------


def test_computed_bytes_past_eight_mib_refuse_by_name():
    text = "x" * 4_000
    program = compile_columns({"t": "attrs.t"})
    fits = program.evaluate([rec(t=text)] * 2_000)
    assert fits.bytes == 8_000_000
    with pytest.raises(ExpressionRefused) as caught:
        program.evaluate([rec(t=text)] * 2_200)
    assert caught.value.data["reason"] == "map_output_too_large"
    assert caught.value.data["row"] == 2_098
    numbers = compile_columns({"n": "1"}).evaluate([rec()] * 10)
    assert numbers.bytes == 10 * mx.NUMBER_BYTES


# -- 27 ------------------------------------------------------------------------

DOCUMENTED_PATTERNS = [
    "\\d+", "^S[0-9]_", "(?i)tape", "_(\\w+)$", "(?P<tier>S\\d)_(?P<area>[A-Z]{2})", "[A-Z]{2,3}", "a{,2}",
    "\\bdisk\\b", "x\\{y\\}", "[^_]+$", "\\N{LATIN SMALL LETTER A}", "[]a]", "[^]a]+", "\\s+\\S", "(?x) a b",
    "(?:north|south)$", "a.*b", "^$", "(?=a)a", "(?<!x)y", "a++b", "(?>a+)b",
]
CORPUS = [
    "S1_AB_north", "S2_CD_south_disk", "tape", "TAPE_buffer", "", "aaa", "x{y}", "disk only", "ünïcode_42",
    "a]b", "]]]", "ab yb xy", "a  b", "abba", "aaab",
]


@pytest.mark.parametrize("pattern", DOCUMENTED_PATTERNS)
def test_re_and_regex_agree_on_every_accepted_documented_pattern(pattern):
    escaped = pattern.replace("\\", "\\\\").replace("'", "\\'")
    compile_columns({"m": f"regex(attrs.s, '{escaped}')"})  # accepted
    ours = regex.compile(pattern, flags=regex.VERSION0)
    theirs = re.compile(pattern)
    for text in CORPUS:
        a, b = theirs.search(text), ours.search(text, timeout=1.0)
        assert (a is None) == (b is None), (pattern, text)
        if a is not None:
            assert a.span() == b.span() and a.groups() == b.groups(), (pattern, text)


# -- 28 ------------------------------------------------------------------------


def test_the_evaluation_clock_is_checked_before_every_row(monkeypatch):
    ticks = iter([0.0, 0.0, 1.0, 4.9, 5.1])  # start, then before rows 1, 2, 3 and 4
    monkeypatch.setattr(mx, "_clock", lambda: next(ticks))
    program = compile_columns({"x": "attrs.a + 1"})
    with pytest.raises(ExpressionRefused) as caught:
        program.evaluate([rec(a=i) for i in range(10)])
    assert caught.value.data["reason"] == "evaluation_budget"
    assert caught.value.data["row"] == 4
    assert "row 4 of 10" in caught.value.message


# -- code review round 1 -------------------------------------------------------


def test_a_huge_regex_repeat_count_is_refused_fast_before_compiling():
    for pattern in ("a{100000000}", "a{2,100000}", "a{,5000}", "(?:ab){1001}"):
        started = time.monotonic()
        refused = refusal(f"regex(attrs.s, '{pattern}')")
        assert refused.data["reason"] == "regex_too_complex", pattern
        assert time.monotonic() - started < 1.0, pattern
    assert value("regex(attrs.s, 'a{1000}')", rec(s="a" * 1000)) == "a" * 1000
    assert value("regex(attrs.s, 'a{2,1000}')", rec(s="aaa")) == "aaa"


def test_count_takes_no_argument_and_col_names_a_metric_field():
    refused = refusal("count(x)", GROUPS)
    assert refused.data["reason"] == "expression_syntax"
    assert "count takes no argument" in refused.message
    row = {"min(attrs.size)": 1, "max(attrs.size)": 9}
    assert value("max(col['attrs.size']) - min(col['attrs.size'])", row, GROUPS) == 8
    assert refusal("min(col['attrs.nope'])", GROUPS).data["reason"] == "unknown_column"
