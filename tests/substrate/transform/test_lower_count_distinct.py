"""count_distinct — the aggregate corroboration counting needs.

Bucket-level corroboration asks "how many DISTINCT sources assert this
value", and the IR had no way to express it. The obvious workaround —
de-duplicating with a distinct SELECT ahead of a GROUP — does not merely
fail, it SILENTLY MIS-COMPILES: the interpreter dedups at the SELECT
binding and then aggregates, while the SQL lowering ignores a pre-SELECT
projection when GROUP is present (that branch is an `elif`) and hoists
DISTINCT to the outer keyword, emitting `SELECT DISTINCT k, COUNT(x) ...
GROUP BY k` — a count over duplicated rows. Production would over-count
while every interpreter-based test passed.

Proofs here (PACT claim-corroboration-signal, task.corrob.compute):
- the fixture DISCRIMINATES: count and count_distinct disagree on it, so a
  passing differential is not vacuous;
- interpreter == lowered SQL for count_distinct;
- the mis-compiling shape is REJECTED rather than silently accepted.
"""

from __future__ import annotations

import os

import pytest

from okg.substrate.transform import (
    Binding,
    Block,
    Effect,
    Op,
    OpKind,
    Registries,
    Surface,
    SurfaceKind,
    SurfaceRegistry,
    Terminator,
    TerminatorKind,
    TransformProgram,
    col,
    evaluate,
    lower_query,
)
from okg.substrate.transform.lower import LoweringError

psycopg = pytest.importorskip("psycopg")

DSN = os.environ.get("OKG_TEST_DSN")

# A corroboration-shaped fixture: (bucket, value) asserted by sources, with
# DUPLICATE source rows per (bucket, value) — the case that separates
# count from count_distinct. bucket "b1" value "x" is asserted by s1 twice
# and s2 once: 3 rows, 2 distinct sources.
CLAIMS = [
    {"bucket": "b1", "value": "x", "source_id": "s1"},
    {"bucket": "b1", "value": "x", "source_id": "s1"},  # same source restating
    {"bucket": "b1", "value": "x", "source_id": "s2"},
    {"bucket": "b2", "value": "y", "source_id": "s1"},
    {"bucket": "b2", "value": "y", "source_id": "s2"},
    {"bucket": "b2", "value": "y", "source_id": "s3"},
    {"bucket": "b3", "value": "z", "source_id": "s9"},
]


def _registry() -> Registries:
    return Registries(
        surfaces=SurfaceRegistry(
            [Surface("claims", SurfaceKind.LIVE, "tmp_corrob", pinned=True)]
        )
    )


def _program(agg_fn: str, distinct: bool = False) -> TransformProgram:
    bindings = [Binding(Op(OpKind.FROM, {"surface": "claims"}), output="c")]
    if distinct:
        bindings.append(
            Binding(
                Op(OpKind.SELECT, {"project": {
                    "bucket": col("bucket"), "source_id": col("source_id"),
                }}),
                output="d", inputs=("c",),
            )
        )
    bindings.append(
        Binding(
            Op(OpKind.GROUP, {
                "key": {"bucket": col("bucket")},
                "agg": {"support": {"fn": agg_fn, "expr": col("source_id")}},
            }),
            output="g", inputs=("d" if distinct else "c",),
        )
    )
    bindings.append(Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("g",)))
    prog = TransformProgram(
        program_id="consistency.corroboration_test",
        effect=Effect.READ_ONLY,
        outputs=("g",),
        entry_block="entry",
        blocks=(
            Block(
                block_id="entry",
                bindings=tuple(bindings),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["g"]}),
            ),
        ),
    )
    if distinct:
        # mark the SELECT distinct the way the lowering reads it
        object.__setattr__(
            prog.blocks[0].bindings[1].op, "args",
            {**prog.blocks[0].bindings[1].op.args, "distinct": True},
        )
    return prog


def _support(rows) -> dict[str, int]:
    return {r["bucket"]: r["support"] for r in rows}


def test_fixture_discriminates_count_from_count_distinct() -> None:
    """Non-vacuity: if count and count_distinct agreed on this fixture, the
    differential below would pass even against the mis-compiling shape."""
    reg = _registry()
    plain = _support(evaluate(_program("count"), {"claims": CLAIMS}, kernels={}).values["g"])
    distinct = _support(
        evaluate(_program("count_distinct"), {"claims": CLAIMS}, kernels={}).values["g"]
    )
    assert plain["b1"] == 3 and distinct["b1"] == 2, (
        "b1 must have a duplicate source, or the differential proves nothing"
    )
    assert plain != distinct
    assert reg.surfaces.resolve("claims") is not None


def test_count_distinct_interpreter_semantics() -> None:
    got = _support(
        evaluate(_program("count_distinct"), {"claims": CLAIMS}, kernels={}).values["g"]
    )
    assert got == {"b1": 2, "b2": 3, "b3": 1}


def test_select_distinct_before_group_is_rejected() -> None:
    """The workaround that silently mis-compiles must not be reachable."""
    with pytest.raises(LoweringError, match="count_distinct"):
        lower_query(_program("count", distinct=True), _registry())


def test_count_distinct_lowers_to_sql_count_distinct() -> None:
    sql, _params = lower_query(_program("count_distinct"), _registry())
    assert "COUNT(DISTINCT" in sql, sql


@pytest.mark.postgres
@pytest.mark.skipif(not DSN, reason="OKG_TEST_DSN not set")
def test_count_distinct_lowered_sql_equals_interpreter() -> None:
    prog, reg = _program("count_distinct"), _registry()
    assert prog.validate(reg) == []
    interp = _support(evaluate(prog, {"claims": CLAIMS}, kernels={}).values["g"])

    sql, params = lower_query(prog, reg)
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "CREATE TEMP TABLE tmp_corrob (bucket text, value text, source_id text)"
        )
        cur.executemany(
            "INSERT INTO tmp_corrob VALUES (%s, %s, %s)",
            [(c["bucket"], c["value"], c["source_id"]) for c in CLAIMS],
        )
        cur.execute(f"SELECT bucket, support FROM (\n{sql}\n) q", params)
        sql_rows = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute("DROP TABLE tmp_corrob")

    assert sql_rows == interp, (
        f"lowered SQL disagrees with the interpreter: {sql_rows} vs {interp}"
    )
