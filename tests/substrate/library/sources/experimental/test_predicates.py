"""Tests for the narrow `success_condition` predicate DSL (D2)."""

from __future__ import annotations

import pytest

from okg.substrate.library.sources.experimental.predicates import (
    Predicate,
    PredicateParseError,
)


# ---------- parsing ----------


@pytest.mark.parametrize(
    "text,op,rhs",
    [
        (">= 1.0", ">=", 1.0),
        ("<= 0.5", "<=", 0.5),
        ("> 0", ">", 0.0),
        ("< 100", "<", 100.0),
        ("== 0.95", "==", 0.95),
        ("!= 0", "!=", 0.0),
        ("  >=   2.5  ", ">=", 2.5),
        (">=1.0", ">=", 1.0),
        ("> -1.5", ">", -1.5),
        (">= 1e-3", ">=", 1e-3),
        (">= +2.0", ">=", 2.0),
    ],
)
def test_parse_comparison(text: str, op: str, rhs: float) -> None:
    p = Predicate.parse(text)
    assert p.op == op
    assert p.rhs == rhs
    assert p.rhs2 is None


@pytest.mark.parametrize(
    "text,op,lo,hi",
    [
        ("between 0.9 and 1.1", "between", 0.9, 1.1),
        ("BETWEEN 0 and 100", "between", 0.0, 100.0),
        ("not between -1 and 1", "not_between", -1.0, 1.0),
        ("not  between 0.0 and 0.5", "not_between", 0.0, 0.5),
    ],
)
def test_parse_between(text: str, op: str, lo: float, hi: float) -> None:
    p = Predicate.parse(text)
    assert p.op == op
    assert p.rhs == lo
    assert p.rhs2 == hi


@pytest.mark.parametrize(
    "text",
    [
        "median > 1 AND std < 0.1",
        ">=",
        "between 1 and",
        "abc",
        "",
        "  ",
        ">= NaN",  # `float('NaN')` would succeed; we keep DSL numeric-literal only via regex
    ],
)
def test_parse_rejects_unsupported(text: str) -> None:
    with pytest.raises(PredicateParseError) as exc_info:
        Predicate.parse(text)
    assert exc_info.value.fix_hint is not None
    assert "supported predicates" in exc_info.value.fix_hint


def test_parse_between_rejects_lo_gt_hi() -> None:
    with pytest.raises(PredicateParseError) as exc_info:
        Predicate.parse("between 2 and 1")
    assert "A <= B" in exc_info.value.message


def test_parse_rejects_non_string() -> None:
    with pytest.raises(PredicateParseError):
        Predicate.parse(1.0)  # type: ignore[arg-type]


# ---------- evaluation ----------


@pytest.mark.parametrize(
    "predicate_text,value,expected",
    [
        (">= 1.0", 1.0, True),
        (">= 1.0", 1.42, True),
        (">= 1.0", 0.99, False),
        ("<= 0.5", 0.5, True),
        ("<= 0.5", 0.6, False),
        ("> 0", 0.0, False),
        ("> 0", 0.01, True),
        ("< 100", 99.9, True),
        ("< 100", 100.0, False),
        ("== 0.95", 0.95, True),
        ("== 0.95", 0.96, False),
        ("!= 0", 1.0, True),
        ("!= 0", 0.0, False),
    ],
)
def test_evaluate_comparison(
    predicate_text: str, value: float, expected: bool
) -> None:
    p = Predicate.parse(predicate_text)
    assert p.evaluate(value) is expected


@pytest.mark.parametrize(
    "predicate_text,value,expected",
    [
        ("between 0.9 and 1.1", 0.9, True),
        ("between 0.9 and 1.1", 1.0, True),
        ("between 0.9 and 1.1", 1.1, True),
        ("between 0.9 and 1.1", 0.89, False),
        ("between 0.9 and 1.1", 1.11, False),
        ("not between 0.9 and 1.1", 1.0, False),
        ("not between 0.9 and 1.1", 1.5, True),
        ("not between 0.9 and 1.1", 0.5, True),
    ],
)
def test_evaluate_between(
    predicate_text: str, value: float, expected: bool
) -> None:
    p = Predicate.parse(predicate_text)
    assert p.evaluate(value) is expected
