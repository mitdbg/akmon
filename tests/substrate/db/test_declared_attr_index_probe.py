"""okg#3112 step 2, slice 1: cut-over gate 2 at the renderer level (PACT
lane-h-3112-step2, req.lane-h-3112-step2.probe; design.md sections 2.2,
2.4 and 7). Native PostgreSQL.

Against REAL expression indexes on the graph tables, read with sequential
scans off, at the head and at a pinned ordinal: for every filter, no row the
exact filter accepts is missing from the candidate probe
:func:`render_probe` renders. Corpora: text values of 1 to 512 characters,
longer values in groups sharing a 600-character prefix and bounds longer
than 512 characters; the E13 numbers (against today's exact filter where it
does not raise, and the revised one with ``okg.attr_num_f8``); the E14 ts
strings (against the v4 exact filter of design section 2.2; a bound v4
refuses is not pushable); bool. Failure cases: a number probe without the
one-ulp widening, and a float bound cast to numeric in SQL, each miss rows.

The probe is not wired into any reader in this slice; gate 3 (the reader's
SQL) is slice 2's.
"""

from __future__ import annotations

import math
from typing import Any, Callable

import pytest

psycopg = pytest.importorskip("psycopg")

from psycopg.rows import dict_row  # noqa: E402
from psycopg.types.json import Jsonb  # noqa: E402

import declared_attr_index_corpora as corpora  # noqa: E402
import declared_attr_index_support as s  # noqa: E402
from okg.substrate import declared_attr_indexes as dai  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402

pytestmark = pytest.mark.postgres

NUMS, NUM_FILTERS = corpora.e13()
TEXTS = corpora.text_corpus()
TS = corpora.E14_TS
#: Stored ts values: all of E14 but two kinds the step-2 publisher (with
#: okg.attr_index, removed by okg#3112 step 4b) could not carry through two
#: publishes (design section 13): the unknown zone raised 22023 and failed the
#: publish, and clock words (now, today, ...) read differently at the next
#: publish. v4 refuses all of them; they stay bounds below.
_CLOCK_WORDS = ("now", "today", "tomorrow", "yesterday", "allballs", "foo/bar")
TS_STORED = [t for t in TS if not any(w in t.lower() for w in _CLOCK_WORDS)]


def _node_attrs(i: int, rev: int) -> dict[str, Any]:
    shift = 0 if rev == 1 else 7
    attrs: dict[str, Any] = {
        "t_num": 0,  # replaced in place by the exact JSON number text
        "t_text": TEXTS[(i + shift) % len(TEXTS)],
        "t_ts": TS_STORED[(i + shift) % len(TS_STORED)],
        "t_bool": bool((i + rev) % 2),
    }
    if i % 50 == 49:
        # Mistyped values the type guards must keep out of every index.
        attrs.update({"t_num": "42", "t_text": 42, "t_ts": 5, "t_bool": "true"})
    return attrs


def _load(dsn: str, identity: Any, rev: int, ids: range) -> None:
    s.apply(dsn, [NodeFact(s.nid(i), "thing", _node_attrs(i, rev), {"row": i}, {"rev": rev})
                  for i in ids])
    table = s.physical(dsn)["node"]
    shift = 0 if rev == 1 else 7
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'SELECT max(live_hi) FROM public."{table}"')
        open_hi = cur.fetchone()[0]
        for i in ids:
            if i % 50 == 49:
                continue
            cur.execute(
                f'UPDATE public."{table}" SET attrs = jsonb_set(attrs, %s, %s::jsonb) '
                "WHERE node_id = %s AND live_hi = %s",
                (["t_num"], NUMS[(i + shift) % len(NUMS)], s.nid(i), open_hi),
            )
    s.publish(dsn, identity)


@pytest.fixture
def corpus(clean_runtime: str) -> dict[str, Any]:
    dsn = clean_runtime
    identity = s.seed(dsn)
    assert s.reconcile(dsn)["status"] == "converged"
    n = max(len(NUMS), len(TEXTS), len(TS)) + 20
    _load(dsn, identity, 1, range(n))
    _load(dsn, identity, 2, range(0, n, 3))
    pinned, head = [int(o["bp"]) for o in s.ordinals(dsn)]
    return {"dsn": dsn, "table": s.physical(dsn)["node"], "bps": {"pinned": pinned, "head": head}}


def _ids(cur: Any, table: str, where: str, params: tuple[Any, ...], bp: int, *, index: bool) -> set[str]:
    statement = (
        f'SELECT g.node_id FROM public."{table}" g WHERE ({where}) '
        "AND g.live_lo <= %s AND %s < g.live_hi"
    )
    cur.execute("BEGIN")
    try:
        if index:
            cur.execute("SET LOCAL enable_seqscan = off")
        else:
            cur.execute("SET LOCAL enable_indexscan = off")
            cur.execute("SET LOCAL enable_bitmapscan = off")
            cur.execute("SET LOCAL enable_indexonlyscan = off")
        cur.execute(statement, (*params, bp, bp))
        return {r["node_id"] for r in cur.fetchall()}
    finally:
        cur.execute("ROLLBACK")


def _plan(cur: Any, table: str, where: str, params: tuple[Any, ...], bp: int) -> str:
    cur.execute("BEGIN")
    try:
        cur.execute("SET LOCAL enable_seqscan = off")
        cur.execute(
            f'EXPLAIN (COSTS OFF) SELECT g.node_id FROM public."{table}" g WHERE ({where}) '
            "AND g.live_lo <= %s AND %s < g.live_hi",
            (*params, bp, bp),
        )
        return "\n".join(str(r["QUERY PLAN"]) for r in cur.fetchall())
    finally:
        cur.execute("ROLLBACK")


# --------------------------------------------------------------------------- #
# Exact filters                                                                #
# --------------------------------------------------------------------------- #

V = "(g.attrs -> %s)"


def _exact_eq(attr: str, values: list[Any]) -> tuple[str, tuple[Any, ...]]:
    """eq and in today: jsonb equality with ``Jsonb(value)``."""
    parts, params = [], []
    for value in values:
        parts.append(f"COALESCE({V} = %s, FALSE)")
        params += [attr, Jsonb(value)]
    return "(" + " OR ".join(parts) + ")", tuple(params)


def _exact_range(attr: str, guard: str, value_expr: Callable[[Any], str], op: str,
                 bounds: list[Any]) -> tuple[str, tuple[Any, ...]]:
    sym = {"lt": "<", "gt": ">"}
    params: list[Any] = [attr]
    if op == "between":
        lo, hi = bounds
        cond = f"{value_expr(lo)} >= %s AND {value_expr(hi)} <= %s"
        params += [attr, lo, attr, hi]
    else:
        cond = f"{value_expr(bounds[0])} {sym[op]} %s"
        params += [attr, bounds[0]]
    return f"CASE WHEN jsonb_typeof({V}) = '{guard}' THEN {cond} ELSE FALSE END", tuple(params)


def _num_exact(pred: dict[str, Any], *, revised: bool) -> tuple[str, tuple[Any, ...]]:
    op, value = pred["op"], pred["value"]
    if op in ("eq", "in"):
        return _exact_eq("t_num", [value] if op == "eq" else list(value))
    bounds = list(value) if op == "between" else [value]

    def value_expr(bound: Any) -> str:
        if revised and isinstance(bound, float):
            return f"okg.attr_num_f8(({V} #>> '{{}}')::numeric)"
        return f"(({V} #>> '{{}}')::numeric)"

    if op == "between" and not revised:
        # Today: one BETWEEN, both sides resolving on their own.
        params = ("t_num", "t_num", bounds[0], bounds[1])
        return (f"CASE WHEN jsonb_typeof({V}) = 'number' THEN "
                f"(({V} #>> '{{}}')::numeric) BETWEEN %s AND %s ELSE FALSE END", params)
    return _exact_range("t_num", "number", value_expr, op, bounds)


def _text_exact(op: str, value: Any) -> tuple[str, tuple[Any, ...]]:
    if op in ("eq", "in"):
        return _exact_eq("t_text", [value] if op == "eq" else list(value))
    bounds = list(value) if op == "between" else [value]
    return _exact_range("t_text", "string", lambda b: f"({V} #>> '{{}}')", op, bounds)


def _ts_exact(op: str, value: Any) -> tuple[str, tuple[Any, ...]]:
    """The v4 exact filter of design section 2.2: stored values and bounds
    read by okg.attr_ts_v4; equality falls back to raw text when the stored
    value is not a v4 timestamp."""
    text = f"({V} #>> '{{}}')"
    if op in ("eq", "in"):
        parts, params = [], []
        for bound in ([value] if op == "eq" else list(value)):
            parts.append(
                f"CASE WHEN jsonb_typeof({V}) = 'string' AND okg.attr_ts_v4({text}) IS NOT NULL "
                f"THEN okg.attr_ts_v4({text}) = okg.attr_ts_v4(%s) ELSE {text} = %s END"
            )
            params += ["t_ts", "t_ts", "t_ts", bound, "t_ts", bound]
        return "(" + " OR ".join(parts) + ")", tuple(params)
    bounds = list(value) if op == "between" else [value]
    sym = {"lt": "<", "gt": ">"}
    if op == "between":
        cond = f"okg.attr_ts_v4({text}) >= okg.attr_ts_v4(%s) AND okg.attr_ts_v4({text}) <= okg.attr_ts_v4(%s)"
        params = ("t_ts", "t_ts", bounds[0], "t_ts", bounds[1])
    else:
        cond = f"okg.attr_ts_v4({text}) {sym[op]} okg.attr_ts_v4(%s)"
        params = ("t_ts", "t_ts", bounds[0])
    return f"CASE WHEN jsonb_typeof({V}) = 'string' THEN coalesce({cond}, FALSE) ELSE FALSE END", params


# --------------------------------------------------------------------------- #
# The superset check                                                           #
# --------------------------------------------------------------------------- #


def _check(cur: Any, corpus: dict[str, Any], attr: str, value_type: str,
           exact: tuple[str, tuple[Any, ...]], op: str, value: Any,
           totals: dict[str, int], *, plan_check: bool = False) -> None:
    probe = dai.render_probe(attr, value_type, op, value, alias="g",
                             ts_accepts=lambda b: dai.ts_bounds_accepted(cur, b))
    for label, bp in corpus["bps"].items():
        accepted = _ids(cur, corpus["table"], exact[0], exact[1], bp, index=False)
        candidates = _ids(cur, corpus["table"], probe.sql, probe.params, bp, index=True)
        missing = accepted - candidates
        totals["filters"] += 1
        totals["exact_true"] += len(accepted)
        totals["missed"] += len(missing)
        if missing and len(totals.setdefault("examples", [])) < 5:  # type: ignore[arg-type]
            totals["examples"].append((attr, op, repr(value)[:80], label, sorted(missing)[:3]))  # type: ignore[union-attr]
        if plan_check:
            plan = _plan(cur, corpus["table"], probe.sql, probe.params, bp)
            assert dai.index_name("node", attr, value_type) in plan, plan


def _new_totals() -> dict[str, Any]:
    return {"filters": 0, "exact_true": 0, "missed": 0}


def test_gate2_number_probe_covers_the_exact_filter(corpus: dict[str, Any]) -> None:
    assert len(NUMS) == 698 and len(NUM_FILTERS) == 150
    revised, today = _new_totals(), _new_totals()
    today_raised = 0
    with psycopg.connect(corpus["dsn"], autocommit=True) as conn, \
            conn.cursor(row_factory=dict_row) as cur:
        for i, pred in enumerate(NUM_FILTERS):
            _check(cur, corpus, "t_num", "num", _num_exact(pred, revised=True),
                   pred["op"], pred["value"], revised, plan_check=(i % 25 == 0))
            try:
                _check(cur, corpus, "t_num", "num", _num_exact(pred, revised=False),
                       pred["op"], pred["value"], today)
            except psycopg.errors.NumericValueOutOfRange:
                today_raised += 1
    assert revised["missed"] == 0, revised
    assert today["missed"] == 0, today
    assert revised["filters"] == 2 * 150
    assert revised["exact_true"] > 10_000 and today["exact_true"] > 1_000
    # Today's range filter raises 22003 over numbers beyond double range.
    assert today_raised > 0


def _num_miss_count(corpus: dict[str, Any]) -> tuple[int, list[Any]]:
    totals = _new_totals()
    with psycopg.connect(corpus["dsn"], autocommit=True) as conn, \
            conn.cursor(row_factory=dict_row) as cur:
        for pred in NUM_FILTERS:
            _check(cur, corpus, "t_num", "num", _num_exact(pred, revised=True),
                   pred["op"], pred["value"], totals)
    return totals["missed"], totals.get("examples", [])


def test_gate2_failure_case_a_number_probe_without_widening_misses_rows(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(dai, "_num_range_value", lambda value, upper: dai._num_probe_value(value)
                        if not (isinstance(value, float) and math.isinf(value))
                        else dai.Decimal("Infinity" if value > 0 else "-Infinity"))
    missed, examples = _num_miss_count(corpus)
    assert missed > 0
    assert any(op == "between" and "0.1" in value for _, op, value, *_ in examples), examples


def test_gate2_failure_case_a_float_bound_cast_to_numeric_in_sql_misses_rows(
    corpus: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    # The float travels as float8 and `%s::numeric` casts it in SQL, which
    # keeps 15 significant digits.
    monkeypatch.setattr(dai, "_num_probe_value", lambda value: value)
    monkeypatch.setattr(dai, "_num_range_value", lambda value, upper: value)
    missed, examples = _num_miss_count(corpus)
    assert missed > 0
    assert any(op in ("eq", "in") for _, op, *_ in examples), examples


def _text_filters() -> list[tuple[str, Any]]:
    long_bounds = [t for t in TEXTS if len(t) > 512][:6]
    long_bounds += [long_bounds[0][:600] + "zzzz", long_bounds[0][:600], "a" * 700, ("a" * 512) + "b"]
    filters: list[tuple[str, Any]] = []
    for value in TEXTS + long_bounds:
        filters.append(("eq", value))
    for value in sorted(set(TEXTS[:25] + long_bounds)):
        filters += [("lt", value), ("gt", value)]
    ordered = sorted(set(TEXTS + long_bounds))
    for i in range(0, len(ordered) - 1, 3):
        filters.append(("between", [ordered[i], ordered[i + 1]]))
        filters.append(("in", [ordered[i], ordered[i + 1], "absent"]))
    return filters


def test_gate2_text_probe_covers_the_exact_filter(corpus: dict[str, Any]) -> None:
    totals = _new_totals()
    with psycopg.connect(corpus["dsn"], autocommit=True) as conn, \
            conn.cursor(row_factory=dict_row) as cur:
        for i, (op, value) in enumerate(_text_filters()):
            _check(cur, corpus, "t_text", "text", _text_exact(op, value), op, value, totals,
                   plan_check=(i % 20 == 0))
        # A bound of another JSON type is not pushable: the exact filter can
        # match a mistyped row no index holds.
        with pytest.raises(dai.NotPushable):
            dai.render_probe("t_text", "text", "eq", 42)
    assert totals["missed"] == 0, totals
    assert totals["exact_true"] > 100


def test_gate2_ts_probe_covers_the_v4_exact_filter(corpus: dict[str, Any]) -> None:
    totals = _new_totals()
    refused = 0
    with psycopg.connect(corpus["dsn"], autocommit=True) as conn, \
            conn.cursor(row_factory=dict_row) as cur:
        accepted_bounds = [b for b, ok in zip(TS, dai.ts_bounds_accepted(cur, TS)) if ok]
        filters: list[tuple[str, Any]] = []
        for value in TS:
            filters += [("eq", value), ("lt", value), ("gt", value)]
        for i in range(0, len(accepted_bounds) - 1, 2):
            filters.append(("between", sorted(accepted_bounds[i:i + 2])))
            filters.append(("in", accepted_bounds[i:i + 2]))
        for op, value in filters:
            try:
                _check(cur, corpus, "t_ts", "ts", _ts_exact(op, value), op, value, totals)
            except dai.NotPushable as exc:
                assert exc.reason == "bound_not_strict_iso"
                refused += 1
    assert totals["missed"] == 0, totals
    assert totals["exact_true"] > 100
    assert refused == 3 * (len(TS) - 27)


def test_gate2_bool_probe_covers_the_exact_filter(corpus: dict[str, Any]) -> None:
    totals = _new_totals()
    with psycopg.connect(corpus["dsn"], autocommit=True) as conn, \
            conn.cursor(row_factory=dict_row) as cur:
        for op, value in (("eq", True), ("eq", False), ("in", [True, False])):
            _check(cur, corpus, "t_bool", "bool", _exact_eq("t_bool", [value] if op == "eq"
                                                            else list(value)),
                   op, value, totals, plan_check=True)
    assert totals["missed"] == 0 and totals["exact_true"] > 500
    with pytest.raises(dai.NotPushable):
        dai.render_probe("t_bool", "bool", "lt", True)
