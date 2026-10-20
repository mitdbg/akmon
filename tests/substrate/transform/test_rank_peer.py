"""RANK ``peer``: the basis compares the leader to the best row in a DIFFERENT
peer group.

Without it the basis compares adjacent rows, so a leader whose own restatements
sit behind it reports a tie against itself — the defect that made
``independent_support`` unable to settle any bucket (see
tests/substrate/consistency/test_corroboration.py).

``LEAD`` cannot express "next row with a different peer", so the lowering grows
a second window level: the leader's peer is materialized by ``FIRST_VALUE``
one level down and consumed in an ``ORDER BY`` one level up, where
different-peer rows sort first so ``FIRST_VALUE`` lands on the best rival.
That is a real reimplementation, not a rename, so it is proven against the
interpreter on a fixture built to discriminate.
"""

from __future__ import annotations

import os
from typing import Any

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

psycopg = pytest.importorskip("psycopg")

DSN = os.environ.get("OKG_TEST_DSN")

# b1: value "x" is carried by two rows that TIE with each other (support 2),
#     and the rival "y" has support 1. Adjacent comparison ties; peer-aware
#     comparison settles on support. This is the corroboration shape.
# b2: a 1-vs-1 split — no majority, so it ties either way.
# b3: every row restates ONE peer group: no rival exists at all.
# b4: the leader's sibling is DEMOTED below the rival by an earlier component,
#     so adjacent and peer-aware comparison agree — guards against a peer
#     implementation that always skips a row.
ROWS = [
    {"bucket": "b1", "peer": "x", "auth": 0, "support": 2, "cid": "c1"},
    {"bucket": "b1", "peer": "x", "auth": 0, "support": 2, "cid": "c2"},
    {"bucket": "b1", "peer": "y", "auth": 0, "support": 1, "cid": "c3"},

    {"bucket": "b2", "peer": "x", "auth": 0, "support": 1, "cid": "d1"},
    {"bucket": "b2", "peer": "y", "auth": 0, "support": 1, "cid": "d2"},

    {"bucket": "b3", "peer": "x", "auth": 0, "support": 1, "cid": "e1"},
    {"bucket": "b3", "peer": "x", "auth": 0, "support": 1, "cid": "e2"},

    {"bucket": "b4", "peer": "x", "auth": 5, "support": 1, "cid": "f1"},
    {"bucket": "b4", "peer": "y", "auth": 3, "support": 1, "cid": "f2"},
    {"bucket": "b4", "peer": "x", "auth": 1, "support": 1, "cid": "f3"},
]

_COMPONENTS = [
    {"name": "authority", "expr": col("auth"), "desc": True},
    {"name": "support", "expr": col("support"), "desc": True},
]


def _registry() -> Registries:
    return Registries(
        surfaces=SurfaceRegistry(
            [Surface("rows", SurfaceKind.LIVE, "tmp_rank_peer", pinned=True)]
        )
    )


def _program(peer_aware: bool) -> TransformProgram:
    args: dict[str, Any] = {
        "partition": [col("bucket")],
        "components": _COMPONENTS,
        "tiebreak": {"expr": col("cid"), "desc": True},
        "rank_as": "rnk",
        "basis_as": "basis",
    }
    if peer_aware:
        args["peer"] = col("peer")
    return TransformProgram(
        program_id="transform.rank_peer_test",
        effect=Effect.READ_ONLY,
        outputs=("r",),
        entry_block="entry",
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "rows"}), output="s"),
                    Binding(Op(OpKind.RANK, args), output="r", inputs=("s",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["r"]}),
            ),
        ),
    )


def _leader_basis(rows) -> dict[str, str]:
    return {r["bucket"]: r["basis"] for r in rows if r["rnk"] == 1}


def _interp(peer_aware: bool) -> dict[str, str]:
    return _leader_basis(
        evaluate(_program(peer_aware), {"rows": ROWS}, kernels={}).values["r"]
    )


def test_program_validates() -> None:
    assert _program(True).validate(_registry()) == []


def test_peer_requires_basis_as() -> None:
    """``peer`` only affects the basis; accepting it without ``basis_as``
    would silently ignore a wiring mistake in the one place the basis
    vocabulary is decided."""
    prog = TransformProgram(
        program_id="transform.rank_peer_nobasis",
        effect=Effect.READ_ONLY,
        outputs=("r",),
        entry_block="entry",
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "rows"}), output="s"),
                    Binding(Op(OpKind.RANK, {
                        "partition": [col("bucket")],
                        "components": _COMPONENTS,
                        "rank_as": "rnk",
                        "peer": col("peer"),
                    }), output="r", inputs=("s",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["r"]}),
            ),
        ),
    )
    codes = [e.code for e in prog.validate(_registry())]
    assert "invalid_rank_peer" in codes, codes


def test_fixture_discriminates_peer_from_adjacent() -> None:
    """Non-vacuity: if both modes agreed on this fixture the differential
    below would pass against an implementation that ignored ``peer``."""
    adjacent, peer_aware = _interp(False), _interp(True)
    assert adjacent["b1"] == "tie", adjacent
    assert peer_aware["b1"] == "support", peer_aware
    assert adjacent != peer_aware


def test_peer_aware_interpreter_semantics() -> None:
    assert _interp(True) == {
        "b1": "support",    # majority beats the rival value
        "b2": "tie",        # 1-vs-1: no majority
        "b3": "sole",       # one peer group: no rival exists
        "b4": "authority",  # sibling demoted; both modes agree here
    }


def test_adjacent_mode_is_unchanged() -> None:
    """Existing RANK users must be untouched by the new arg."""
    assert _interp(False) == {
        "b1": "tie", "b2": "tie", "b3": "tie", "b4": "authority"}


@pytest.mark.postgres
@pytest.mark.skipif(not DSN, reason="OKG_TEST_DSN not set")
@pytest.mark.parametrize("peer_aware", [False, True])
def test_lowered_sql_equals_interpreter(peer_aware: bool) -> None:
    prog, reg = _program(peer_aware), _registry()
    assert prog.validate(reg) == []
    interp = _interp(peer_aware)

    sql, params = lower_query(prog, reg)
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "CREATE TEMP TABLE tmp_rank_peer ("
            "bucket text, peer text, auth int, support int, cid text)"
        )
        cur.executemany(
            "INSERT INTO tmp_rank_peer VALUES (%s, %s, %s, %s, %s)",
            [(r["bucket"], r["peer"], r["auth"], r["support"], r["cid"])
             for r in ROWS],
        )
        cur.execute(
            f"SELECT bucket, basis FROM (\n{sql}\n) q WHERE rnk = 1", params)
        from_sql = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute(f"SELECT * FROM (\n{sql}\n) q LIMIT 1", params)
        sql_columns = {d.name for d in cur.description or ()}
        cur.execute("DROP TABLE tmp_rank_peer")

    assert from_sql == interp, (
        f"lowered SQL disagrees with the interpreter (peer_aware="
        f"{peer_aware}): {from_sql} vs {interp}"
    )

    # Column PARITY, not just value parity. The peer lowering materializes the
    # leader's peer in an inner level; leaking it would give the SQL path a
    # different schema from the interpreter in the one subsystem whose whole
    # discipline is SQL/interpreter equivalence, and any later post-RANK
    # `SELECT *` consumer would inherit it silently.
    interp_columns = set(
        evaluate(_program(peer_aware), {"rows": ROWS}, kernels={}).values["r"][0]
    )
    assert sql_columns == interp_columns, (
        f"column sets differ (peer_aware={peer_aware}): "
        f"sql-only={sorted(sql_columns - interp_columns)}, "
        f"interp-only={sorted(interp_columns - sql_columns)}"
    )
