"""The one number rule, the one grouping rule and the named skips (PACT
read-stored-metrics), without a database.

Live sums, min and max (`live_result_sets.group_report`), live filter
comparisons (`live_reader.Matcher`), computed columns' implicit operand reads
(`map_expressions.to_number`) and the SQL compiler (`operator_sql._NUMBER_TEXT`)
read text as a number by one rule. Each test fails on the base
(`3d646f6e53`): the names it imports do not exist there, and the rules it
pins differ (the base's `_number` reads " 10 " as 10).
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from okg.substrate.mcp import live_reader
from okg.substrate.mcp import live_result_sets as lrs
from okg.substrate.mcp import map_expressions as mx
from okg.substrate.mcp.operator_sql import _NUMBER_TEXT

#: Text the reader before the rule took as a number and the rule refuses.
REJECTED = [" 10 ", "+5", "010", ".5", "1_000", "١٠", "1.", "10 ", "1e"]
#: Text the rule takes, and the number it is.
ACCEPTED = {"10": 10, "-3": -3, "0": 0, "-0": 0, "2.5": 2.5, "1e2": 100.0, "1E-2": 0.01}


def _typed(node_id: str, **attrs) -> dict:
    return {"node_id": node_id, "subtype": "site", "attrs": attrs,
            "live": {"system": "example", "fields": {k: "live" for k in attrs}}}


def test_one_number_rule_for_live_stored_and_sql() -> None:
    # map_expressions imports nothing from the read path: its copy of the
    # rule is pinned equal to the SQL compiler's.
    assert mx._NUMBER_TEXT.pattern == _NUMBER_TEXT.pattern
    for text in REJECTED:
        assert _NUMBER_TEXT.fullmatch(text) is None, text
        assert lrs.is_number_text(text) is False, text
        assert lrs._number(text) is None, text
        # A computed column's implicit operand read refuses it too.
        with pytest.raises(mx.RowError):
            mx.to_number(text)
    for text, number in ACCEPTED.items():
        assert _NUMBER_TEXT.fullmatch(text) is not None, text
        assert lrs._number(text) == number, text
        assert float(mx.to_number(text)) == number, text
    # number() keeps its documented parse: spaces and a sign are taken.
    assert mx.to_number(" 10 ", strict=False) == 10
    assert mx.to_number("+5", strict=False) == 5
    # JSON numbers, exact decimals from the stored path, never a bool.
    assert lrs._number(Decimal("1E+2")) == 100 and isinstance(lrs._number(Decimal("1E+2")), int)
    assert lrs._number(Decimal("-0.0")) == 0
    assert lrs._number(True) is None and lrs._number(None) is None


def test_number_text_in_a_computed_column_reads_by_the_rule_and_number_reads_it() -> None:
    """An implicit operand read follows the rule; number() reads " 10 "."""
    program = mx.compile({"twice": "attrs.x * 2", "read": "number(attrs.x) * 2"}, None)
    rows = [_typed("site:a", x=" 10 "), _typed("site:b", x="10")]
    result = program.evaluate(rows, None)
    twice, read = result.columns["calc.twice"], result.columns["calc.read"]
    assert read.states == ["ok", "ok"] and read.values == [20, 20]
    assert twice.states[1] == "ok" and twice.values[1] == 20
    assert str(twice.states[0]).startswith("error"), twice.states


def test_numbers_group_by_numeric_value_and_print_canonically() -> None:
    same = [1, 1.0, Decimal("1"), Decimal("1.000"), Decimal("1E+0")]
    assert len({lrs.number_key(v) for v in same}) == 1
    assert lrs.number_key(100) == lrs.number_key(Decimal("1E+2")) == lrs.number_key(100.0)
    assert lrs.number_key(-0.0) == lrs.number_key(0) == lrs.number_key(Decimal("-0.0"))
    # Never keyed through a float (review N2).
    assert lrs.number_key(Decimal("0.1000000000000000055")) != lrs.number_key(0.1)
    assert lrs.number_key(9007199254740993) != lrs.number_key(9007199254740992.0)
    assert lrs.number_key("1") is None and lrs.number_key(True) is None
    assert lrs.canonical_number(Decimal("1E+2")) == 100
    assert isinstance(lrs.canonical_number(1.0), int)
    assert lrs.canonical_number(Decimal("2.50")) == 2.5
    exact = lrs.canonical_number(Decimal("0.1000000000000000055"))
    assert exact == Decimal("0.1000000000000000055") and isinstance(exact, lrs.ExactNumber)
    # Served as a number literal with all its digits, never as text (R1).
    from okg.substrate.mcp.result_text import compact_json, scalar

    assert compact_json({"n": exact, "t": "0.1000000000000000055"}) == (
        '{"n":0.1000000000000000055,"t":"0.1000000000000000055"}'
    )
    assert scalar(exact) == "0.1000000000000000055"
    assert lrs.number_key(exact) != lrs.number_key(0.1) and lrs.number_key(exact) is not None
    # Inside a list or object it keys apart from the same text (N1, N2).
    assert lrs._group_key([exact]) != lrs._group_key(["0.1000000000000000055"])
    assert lrs._group_key({"v": exact}) != lrs._group_key({"v": 0.1})
    assert lrs.exact_number_fields([{"k": [exact]}], ["k"]) == {"exact_numbers": ["k"]}
    report = lrs.group_report([{"x": exact}, {"x": Decimal("0.05")}], [], [("max", "x")])
    assert report.groups[0][1]["max(attrs.x)"] == exact  # N3: exact, not 0.1
    assert lrs.canonical_number([Decimal("1.0"), {"a": Decimal("0.5")}]) == [1, {"a": 0.5}]
    rows = [{"k": v} for v in (1, 1.0, -0.0, 0, 100, Decimal("1E+2"), "1", None)] + [{}]
    report = lrs.group_report(rows, ["attrs.k"], [("count", None)])
    groups = {(repr(lrs.ABSENT if key[0] is lrs.ABSENT_VALUE else key[0])): c["count"]
              for key, c in report.groups}
    assert groups == {"1": 2, "0": 2, "100": 2, "'1'": 1, "None": 1, "'(absent)'": 1}


def test_skipped_values_are_named_by_class_with_ids() -> None:
    rows = [
        live_reader.flat_row(_typed("site:a", used=5)),
        live_reader.flat_row(_typed("site:b", used=None)),
        live_reader.flat_row(_typed("site:c")),
        live_reader.flat_row(_typed("site:d", used=" 10 ")),
        live_reader.flat_row(_typed("site:e", used="+5")),
        live_reader.flat_row(_typed("site:f", used="abc")),
        live_reader.flat_row(_typed("site:g", used=True)),
        live_reader.flat_row(_typed("site:h", used="7")),
    ]
    report = lrs.group_report(rows, [], [("sum", "used")])
    assert report.groups[0][1]["sum(attrs.used)"] == 12
    skipped = lrs.skipped_payload(report)
    assert skipped == {"attrs.used": {
        "null": {"count": 1, "first_ids": ["site:b"]},
        "absent": {"count": 1, "first_ids": ["site:c"]},
        "not_a_number": {"count": 2, "first_ids": ["site:f", "site:g"]},
        "number_text_rejected": {"count": 2, "first_ids": ["site:d", "site:e"]},
        "read_from_text": 1,
    }}
    assert lrs.skipped_sentences(skipped) == [
        "attrs.used skipped: 1 null (first ids: site:b), 1 absent (first ids: site:c), "
        "2 not a number (first ids: site:f, site:g), 2 number text rejected (first ids: "
        'site:d, site:e); 1 read from text; read number text with map(compute={"n": '
        '"number(attrs.used)"})'
    ]
    # Rows with no node id are named by their place.
    raw = lrs.group_report([{"used": "x"}, {"used": 1}], [], [("max", "used")])
    assert lrs.skipped_payload(raw)["attrs.used"]["not_a_number"]["first_ids"] == ["row 1"]


def test_live_sum_and_live_filter_agree_on_number_text() -> None:
    """The rows a live sum counts are the rows a live filter compares: number
    text the rule refuses is out of both, and named by both."""
    values = {"a": " 10 ", "b": "+5", "c": "010", "d": ".5", "e": "1_000", "f": "10", "g": 12}
    rows = [_typed(f"site:{k}", used=v) for k, v in values.items()]
    report = lrs.group_report([live_reader.flat_row(r) for r in rows], [], [("sum", "used")])
    assert report.groups[0][1]["sum(attrs.used)"] == 22
    matched, not_comparable = live_reader.filter_rows(
        rows, {"op": "gt", "field": "attrs.used", "value": 5}, lambda row: False,
    )
    assert [r["node_id"] for r in matched] == ["site:f", "site:g"]
    note = live_reader.not_comparable_report(not_comparable)
    assert note is not None
    assert note["number_text_rejected"] == {"attrs.used": {
        "count": 5, "first_ids": ["site:a", "site:b", "site:c", "site:d", "site:e"],
    }}
    assert "number text the one number rule refuses" in note["note"]
    assert 'number(attrs.used)' in note["note"]
    # Under `not` a comparison that could not be made is false, so its row
    # matches; the note says so (review M2).
    negated, nc = live_reader.filter_rows(
        rows, {"op": "not", "clause": {"op": "gt", "field": "attrs.used", "value": 5}},
        lambda row: False,
    )
    assert [r["node_id"] for r in negated] == [
        "site:a", "site:b", "site:c", "site:d", "site:e",
    ]
    said = live_reader.not_comparable_report(nc)["note"]
    assert "under `not` it is true, so those rows match" in said
    assert "they do not match" not in said
    # Equality reads by the same rule.
    equal, _nc = live_reader.filter_rows(
        rows, {"op": "eq", "field": "attrs.used", "value": 10}, lambda row: False,
    )
    assert [r["node_id"] for r in equal] == ["site:f"]


def test_a_numeric_live_sort_puts_numbers_first_then_text() -> None:
    """Where values that are not numbers go in a sort: numbers by value,
    then text by code point (number text sorts as text), then booleans and
    anything else; a missing or null value last, in both directions."""
    rows = [_typed(f"site:{i}", used=v) for i, v in enumerate([" 10 ", 3, "2", True, 11])]
    rows.append(_typed("site:n", used=None))
    rows.append(_typed("site:m"))
    up = [r["node_id"] for r in live_reader.order_rows(rows, "used", False, "filter")]
    assert up == ["site:1", "site:4", "site:0", "site:2", "site:3", "site:n", "site:m"]
    down = [r["node_id"] for r in live_reader.order_rows(rows, "used", True, "filter")]
    assert down[-2:] == ["site:n", "site:m"]
