"""JOIN on column equality runs as a hash join, with the nested loop's exact output.

The interpreter used to evaluate a JOIN's ``on`` for every (left, right) pair.
A Concord reader joining claims to claim text on claim id took 193 s at 8,000
claims and stalled for 9 h at 90,580. These tests pin three things: the work
grows linearly, the hash join returns exactly what the nested loop returned,
and every other ``on`` still takes the nested loop.
"""

from __future__ import annotations

import math
import sys

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from okg.substrate.transform import (
    Binding,
    Block,
    Effect,
    Op,
    OpKind,
    Terminator,
    TerminatorKind,
    TransformProgram,
    evaluate,
)
from okg.substrate.transform import interpreter as interp
from okg.substrate.transform.expr import and_, col, eq, eval_expr, lit, lt, or_

NAN = math.nan  # one object, so rows holding it compare equal as dicts
KEYS = [None, NAN, 0, 1, 1.0, True, False, -0.0, 0.0, 2, "1", "a", "b", ""]
HOWS = ["inner", "left", "full"]
NESTED_LOOP = interp._join  # the real one, before any spy replaces the module attribute


def _program(on: dict, how: str) -> TransformProgram:
    return TransformProgram(
        program_id="hash-join",
        effect=Effect.READ_ONLY,
        entry_block="entry",
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "L"}), output="l"),
                    Binding(Op(OpKind.FROM, {"surface": "R"}), output="r"),
                    Binding(Op(OpKind.JOIN, {"how": how, "on": on}), output="out", inputs=("l", "r")),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["out"]}),
            ),
        ),
    )


def _run(on: dict, how: str, left: list[dict], right: list[dict]) -> list[dict]:
    return evaluate(_program(on, how), {"L": left, "R": right}, kernels={}).values["out"]


def _nested(on: dict, how: str, left: list[dict], right: list[dict]) -> list[dict]:
    """The nested loop exactly as the interpreter ran every ``on`` before the hash join."""
    return NESTED_LOOP(
        [dict(r) for r in left],
        [dict(r) for r in right],
        how,
        lambda lr, rr: eval_expr(on, {**rr, **lr}, {}),
    )


@pytest.fixture
def join_spy(monkeypatch):
    """Count nested-loop runs; the JOIN dispatch looks ``_join`` up at call time."""
    calls: list[int] = []
    real = interp._join

    def spy(left, right, how, cond):
        calls.append(1)
        return real(left, right, how, cond)

    monkeypatch.setattr(interp, "_join", spy)
    return calls


# --------------------------------------------------------------------------- #
# Growth: the work at 4n is about 4x the work at n, not 16x
# --------------------------------------------------------------------------- #


def _profiled_calls(n: int) -> int:
    """Python and C function calls made while joining n claims to n texts.

    A call count, not a wall time: it does not depend on machine load, so the
    bound below cannot flake.
    """
    left = [{"claim_id": f"c{i}", "subject": i % 7} for i in range(n)]
    right = [{"c_claim": f"c{n - 1 - i}", "text": f"t{i}"} for i in range(n)]
    program = _program(eq(col("claim_id"), col("c_claim")), "inner")
    count = 0

    def tally(_frame, event, _arg):
        nonlocal count
        if event in ("call", "c_call"):
            count += 1

    previous = sys.getprofile()
    sys.setprofile(tally)
    try:
        out = evaluate(program, {"L": left, "R": right}, kernels={}).values["out"]
    finally:
        sys.setprofile(previous)
    assert len(out) == n
    return count


def test_equality_join_work_grows_linearly_not_quadratically():
    small, large = _profiled_calls(150), _profiled_calls(600)
    # Linear work gives a ratio near 4; the nested loop gives about 16.
    assert large <= 6 * small, (
        f"join made {small} calls at n=150 and {large} at n=600 "
        f"(ratio {large / small:.1f}); quadratic growth is back"
    )


# --------------------------------------------------------------------------- #
# Equivalence: same rows, same order, same NULL and NaN handling
# --------------------------------------------------------------------------- #

_key = st.sampled_from(KEYS)
_payload = st.sampled_from(["x", "y", None])


@st.composite
def _relations(draw):
    """Left rows (lk, lk2, v) and right rows (rk, rk2, v): duplicate, NULL, NaN and
    cross-type keys (1 == 1.0 == True), a shared column ``v`` whose left value
    must win in the merged row, and sometimes a left row lacking ``lk`` (the
    column then resolves per pair, so the nested loop must run)."""
    n_left = draw(st.integers(0, 12))
    n_right = draw(st.integers(0, 12))
    ragged = draw(st.booleans())
    left = []
    for i in range(n_left):
        row = {"lid": i, "lk": draw(_key), "lk2": draw(_key), "v": draw(_payload)}
        if ragged and draw(st.booleans()):
            del row["lk"]
        left.append(row)
    right = [
        {"rid": j, "rk": draw(_key), "rk2": draw(_key), "v": draw(_payload)}
        for j in range(n_right)
    ]
    return left, right


ONS = [
    eq(col("lk"), col("rk")),
    eq(col("rk"), col("lk")),                       # operands in either order
    eq(col("lk"), col("r.rk")),                     # alias-qualified: bare-name lookup
    and_(eq(col("lk"), col("rk")), eq(col("rk2"), col("lk2"))),  # multi-column
]


@settings(max_examples=400, deadline=None, derandomize=True)
@given(rels=_relations(), on=st.sampled_from(ONS), how=st.sampled_from(HOWS))
def test_hash_join_output_is_identical_to_nested_loop(rels, on, how):
    left, right = rels
    expected = _nested(on, how, left, right)
    assert _run(on, how, left, right) == expected
    hashed = interp._hash_join(
        [dict(r) for r in left], [dict(r) for r in right], how, on, null_matches=True
    )
    if left and right and all("lk" in r for r in left):
        assert hashed == expected  # the hash path itself ran, and agrees
    else:
        assert hashed is None  # empty side or ragged column: nested loop


@pytest.mark.parametrize("how", HOWS)
def test_empty_sides_match_nested_loop(how):
    rows = [{"lk": 1, "v": "l"}, {"lk": None, "v": "m"}]
    texts = [{"rk": 1, "v": "r"}, {"rk": None, "v": "s"}]
    on = eq(col("lk"), col("rk"))
    for left, right in (([], texts), (rows, []), ([], [])):
        assert _run(on, how, left, right) == _nested(on, how, left, right)


def test_null_key_matches_null_key_as_the_nested_loop_did(join_spy):
    """The interpreter's ``eq`` is Python ``==`` (expr.py header: ``NULL = NULL``
    keeps a row SQL would drop). The hash join keeps that, and NaN still
    matches nothing, NaN included."""
    left = [{"lk": None, "l": 1}, {"lk": NAN, "l": 2}, {"lk": 1, "l": 3}]
    right = [{"rk": NAN, "r": 1}, {"rk": None, "r": 2}, {"rk": True, "r": 3}]
    out = _run(eq(col("lk"), col("rk")), "full", left, right)
    assert join_spy == []
    assert out == [
        {"rk": None, "r": 2, "lk": None, "l": 1},
        {"lk": NAN, "l": 2},
        {"rk": True, "r": 3, "lk": 1, "l": 3},
        {"rk": NAN, "r": 1},
    ]


@settings(max_examples=300, deadline=None, derandomize=True)
@given(rels=_relations(), how=st.sampled_from(HOWS))
def test_sql_null_logic_mode_matches_a_nested_loop_where_null_equals_nothing(rels, how):
    """``null_matches=False`` is SQL equality: a NULL key matches no key."""
    left, right = rels
    left = [r for r in left if "lk" in r]

    def sql_eq(lr, rr):
        a, b = lr["lk"], rr["rk"]
        return a is not None and b is not None and a == b

    expected = NESTED_LOOP(left, right, how, sql_eq)
    got = interp._hash_join(left, right, how, eq(col("lk"), col("rk")), null_matches=False)
    if left and right:
        assert got == expected
    else:
        assert got is None  # an empty side goes to the (linear) nested loop


# --------------------------------------------------------------------------- #
# Fallback: anything but column equality keeps the nested loop
# --------------------------------------------------------------------------- #


def test_column_equality_takes_the_hash_join(join_spy):
    left = [{"lk": i} for i in range(5)]
    right = [{"rk": i} for i in range(5)]
    for on in (eq(col("lk"), col("rk")), and_(eq(col("lk"), col("rk")))):
        assert _run(on, "inner", left, right) == _nested(on, "inner", left, right)
    assert join_spy == []


FALLBACK_CASES = {
    "ordinal comparison": (lt(col("lk"), col("rk")), [{"lk": 1}, {"lk": 3}], [{"rk": 2}]),
    "column = literal": (eq(col("lk"), lit(1)), [{"lk": 1}, {"lk": 2}], [{"rk": 9}]),
    "both columns on the left": (eq(col("lk"), col("lk2")), [{"lk": 1, "lk2": 1}], [{"rk": 1}]),
    "both columns on the right": (eq(col("rk"), col("rk2")), [{"lk": 1}], [{"rk": 1, "rk2": 1}]),
    "or of equalities": (
        or_(eq(col("lk"), col("rk")), eq(col("lk2"), col("rk"))),
        [{"lk": 1, "lk2": 2}],
        [{"rk": 2}],
    ),
    "and with a non-equality": (
        and_(eq(col("lk"), col("rk")), lt(col("lk2"), col("rk2"))),
        [{"lk": 1, "lk2": 0}],
        [{"rk": 1, "rk2": 5}],
    ),
    "empty and": ({"and": []}, [{"lk": 1}], [{"rk": 2}]),
    "column missing from some left rows": (
        eq(col("lk"), col("rk")),
        [{"lk": 1}, {"rk": 1}],
        [{"rk": 1}],
    ),
    "unhashable key value": (eq(col("lk"), col("rk")), [{"lk": [1]}], [{"rk": [1]}]),
    "key type outside str/int/float/bool/None": (
        eq(col("lk"), col("rk")),
        [{"lk": (1, 2)}],
        [{"rk": (1, 2)}],
    ),
    # eval_expr reads a missing "r.k" through its bare name "k", and the merged
    # row takes "k" from the LEFT row: the value comes from the left side.
    "qualified right column whose bare name is on the left": (
        eq(col("lk"), col("r.k")),
        [{"lk": 1, "k": 5}, {"lk": 5, "k": 1}],
        [{"k": 1}, {"k": 5}],
    ),
}


def test_hash_join_rows_are_copies_not_the_input_rows():
    """Every output row is a new dict, matched or not, as the nested loop's were."""
    left = [{"lk": 1, "v": "a"}, {"lk": 2, "v": "b"}]
    right = [{"rk": 1, "w": "x"}, {"rk": 3, "w": "y"}]
    out = interp._hash_join(left, right, "full", eq(col("lk"), col("rk")), null_matches=True)
    assert out == NESTED_LOOP(left, right, "full", lambda lr, rr: lr["lk"] == rr["rk"])
    inputs = {id(r) for r in left + right}
    assert len(out) == 3
    assert not any(id(r) in inputs for r in out)


@pytest.mark.parametrize("case", sorted(FALLBACK_CASES))
def test_other_on_expressions_take_the_nested_loop(case, join_spy):
    on, left, right = FALLBACK_CASES[case]
    for how in HOWS:
        assert _run(on, how, left, right) == _nested(on, how, left, right)
    assert len(join_spy) == len(HOWS)  # one nested loop per JOIN evaluated


def test_kernel_condition_takes_the_nested_loop(join_spy):
    program = TransformProgram(
        program_id="kernel-join",
        effect=Effect.READ_ONLY,
        entry_block="entry",
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "L"}), output="l"),
                    Binding(Op(OpKind.FROM, {"surface": "R"}), output="r"),
                    Binding(
                        Op(OpKind.JOIN, {"how": "inner", "condition": "same"}),
                        output="out",
                        inputs=("l", "r"),
                    ),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["out"]}),
            ),
        ),
    )
    out = evaluate(
        program,
        {"L": [{"lk": 1}], "R": [{"rk": 1}]},
        kernels={"same": lambda lr, rr: lr["lk"] == rr["rk"]},
    ).values["out"]
    assert out == [{"rk": 1, "lk": 1}]
    assert join_spy == [1]
