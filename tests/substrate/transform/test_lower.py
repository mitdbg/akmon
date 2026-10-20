"""task.gtir.normalize-and-lower (SQL half) -- interpreter == lowered SQL.

Proves the relational core lowers to SQL whose result equals BOTH the reference
interpreter and a hand-written reference query, on a seeded fixture graph. Uses
the antijoin shape (LEFT JOIN + WHERE IS NULL) that task 5's no_dangling_edges
checker reuses. Skips when no test DB is configured.
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
    eq,
    evaluate,
    is_null,
    lower_query,
)

psycopg = pytest.importorskip("psycopg")

DSN = os.environ.get("OKG_TEST_DSN")
pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(not DSN, reason="OKG_TEST_DSN not set"),
]

# Fixture graph: edge 3 dangles (dst 999 has no node).
EDGES = [{"edge_id": 1, "dst_id": 10}, {"edge_id": 2, "dst_id": 20}, {"edge_id": 3, "dst_id": 999}]
NODES = [{"node_id": 10}, {"node_id": 20}]


def _registry() -> Registries:
    return Registries(
        surfaces=SurfaceRegistry(
            [
                Surface("edges", SurfaceKind.LIVE, "tmp_edges", pinned=True),
                Surface("nodes", SurfaceKind.LIVE, "tmp_nodes", pinned=True),
            ]
        )
    )


def _antijoin_program() -> TransformProgram:
    return TransformProgram(
        program_id="consistency.no_dangling_edges",
        effect=Effect.READ_ONLY,
        outputs=("dangling",),
        entry_block="entry",
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "edges", "alias": "e"}), output="e"),
                    Binding(Op(OpKind.FROM, {"surface": "nodes", "alias": "n"}), output="n"),
                    Binding(
                        Op(OpKind.JOIN, {"how": "left", "on": eq(col("e.dst_id"), col("n.node_id"))}),
                        output="lj", inputs=("e", "n"),
                    ),
                    Binding(
                        Op(OpKind.WHERE, {"expr": is_null(col("n.node_id"))}),
                        output="dangling", inputs=("lj",),
                    ),
                    Binding(
                        Op(OpKind.SELECT, {"project": {"edge_id": col("e.edge_id")}}),
                        output="out", inputs=("dangling",),
                    ),
                    Binding(Op(OpKind.EMIT, {"target": "result"}), inputs=("out",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["out"]}),
            ),
        ),
    )


def test_lowered_sql_equals_interpreter_and_reference():
    prog = _antijoin_program()
    reg = _registry()

    # The program is well-formed.
    assert prog.validate(reg) == []

    # Interpreter result over the same fixture (as bags).
    interp = evaluate(prog, {"edges": EDGES, "nodes": NODES}, kernels={})
    interp_ids = sorted(r["edge_id"] for r in interp.values["out"])

    sql, params = lower_query(prog, reg)
    assert "LEFT JOIN tmp_nodes n" in sql
    assert "IS NULL" in sql

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tmp_nodes (node_id int)")
        cur.execute("CREATE TEMP TABLE tmp_edges (edge_id int, dst_id int)")
        cur.executemany("INSERT INTO tmp_nodes VALUES (%s)", [(n["node_id"],) for n in NODES])
        cur.executemany(
            "INSERT INTO tmp_edges VALUES (%s, %s)", [(e["edge_id"], e["dst_id"]) for e in EDGES]
        )
        # Lowered query.
        cur.execute(sql, params)
        sql_ids = sorted(row[0] for row in cur.fetchall())
        # Hand-written reference query.
        cur.execute(
            "SELECT e.edge_id FROM tmp_edges e "
            "LEFT JOIN tmp_nodes n ON e.dst_id = n.node_id WHERE n.node_id IS NULL"
        )
        ref_ids = sorted(row[0] for row in cur.fetchall())

    assert interp_ids == [3]            # the dangling edge
    assert sql_ids == interp_ids        # lowered SQL == interpreter
    assert ref_ids == interp_ids        # == hand-written reference
