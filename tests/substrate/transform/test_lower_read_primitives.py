"""task.rro.ir-primitives -- the read-operator lowering primitives agree with
the reference interpreter on a seeded fixture.

Covers the shapes the remaining AKMON read operators need and that lower_query
lacked before this PACT: GROUP (count) + coalesce func + multi-key ORDER (the
aggregate shape), and comparison + jsonb (exists) + SELECT DISTINCT + LIMIT
OFFSET (the filter shape). Each program is run through BOTH the interpreter and
the lowered SQL; the ordered results must match. Skips when no test DB is set.
"""

from __future__ import annotations

import json
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
    and_,
    col,
    eq,
    evaluate,
    func,
    gt,
    json_exists,
    lit,
    lower_query,
)

psycopg = pytest.importorskip("psycopg")

DSN = os.environ.get("OKG_TEST_DSN")
pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(not DSN, reason="OKG_TEST_DSN not set"),
]


def _reg(*surfaces: tuple[str, str]) -> Registries:
    return Registries(
        surfaces=SurfaceRegistry(
            [Surface(name, SurfaceKind.LIVE, binding, pinned=True) for name, binding in surfaces]
        )
    )


EDGES = [
    {"edge_id": 1, "edge_type": "cites", "dst_id": 10},
    {"edge_id": 2, "edge_type": "cites", "dst_id": 20},
    {"edge_id": 3, "edge_type": "cites", "dst_id": 999},  # dangling -> coalesce
    {"edge_id": 4, "edge_type": "authored", "dst_id": 10},
]
NODES = [{"node_id": 10, "subtype": "paper"}, {"node_id": 20, "subtype": "dataset"}]


def _group_program() -> TransformProgram:
    keys = {
        "edge_type": col("e.edge_type"),
        "dst_subtype": func("coalesce", col("n.subtype"), lit("<unresolved>")),
    }
    return TransformProgram(
        program_id="test.rro.group",
        effect=Effect.READ_ONLY,
        outputs=("o",),
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
                        Op(OpKind.GROUP, {"key": keys, "agg": {"cnt": {"fn": "count"}}}),
                        output="g", inputs=("lj",),
                    ),
                    Binding(
                        Op(OpKind.ORDER, {"keys": [
                            {"expr": col("cnt"), "desc": True},
                            {"expr": col("edge_type")},
                            {"expr": col("dst_subtype")},
                        ]}),
                        output="o", inputs=("g",),
                    ),
                    Binding(Op(OpKind.EMIT, {"target": "o"}), inputs=("o",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["o"]}),
            ),
        ),
    )


def test_group_count_coalesce_multikey_order_matches_interpreter():
    prog = _group_program()
    reg = _reg(("edges", "tmp_e"), ("nodes", "tmp_n"))
    assert prog.validate(reg) == []

    interp = evaluate(prog, {"edges": EDGES, "nodes": NODES}, kernels={})
    interp_rows = [(r["edge_type"], r["dst_subtype"], r["cnt"]) for r in interp.outputs["o"]]

    sql, params = lower_query(prog, reg)
    assert "GROUP BY" in sql and "COALESCE" in sql.upper() and "COUNT(*)" in sql.upper()

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tmp_n (node_id int, subtype text)")
        cur.execute("CREATE TEMP TABLE tmp_e (edge_id int, edge_type text, dst_id int)")
        cur.executemany("INSERT INTO tmp_n VALUES (%s,%s)", [(n["node_id"], n["subtype"]) for n in NODES])
        cur.executemany(
            "INSERT INTO tmp_e VALUES (%s,%s,%s)",
            [(e["edge_id"], e["edge_type"], e["dst_id"]) for e in EDGES],
        )
        cur.execute(sql, params)
        sql_rows = [tuple(row) for row in cur.fetchall()]

    # cites->paper(1), cites->dataset(1), cites-><unresolved>(1), authored->paper(1)
    assert sql_rows == interp_rows
    assert ("authored", "paper", 1) in interp_rows
    assert ("cites", "<unresolved>", 1) in interp_rows


FNODES = [
    {"node_id": 1, "subtype": "paper", "score": 9, "attrs": {"text": "hi"}},
    {"node_id": 2, "subtype": "paper", "score": 9, "attrs": {"text": "yo"}},   # dup subtype
    {"node_id": 3, "subtype": "dataset", "score": 9, "attrs": {"no_text": 1}},  # no 'text' key
    {"node_id": 4, "subtype": "model", "score": 2, "attrs": {"text": "lo"}},    # score<=5
    {"node_id": 5, "subtype": "evidence", "score": 7, "attrs": {"text": "x"}},
]


def _filter_program() -> TransformProgram:
    return TransformProgram(
        program_id="test.rro.filter",
        effect=Effect.READ_ONLY,
        outputs=("o",),
        entry_block="entry",
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "nodes", "alias": "n"}), output="n"),
                    Binding(
                        Op(OpKind.WHERE, {"expr": and_(
                            gt(col("n.score"), lit(5)),
                            json_exists(col("n.attrs"), "text"),
                        )}),
                        output="w", inputs=("n",),
                    ),
                    Binding(
                        Op(OpKind.SELECT, {"project": {"subtype": col("n.subtype")}, "distinct": True}),
                        output="s", inputs=("w",),
                    ),
                    Binding(Op(OpKind.ORDER, {"expr": col("subtype")}), output="o2", inputs=("s",)),
                    Binding(Op(OpKind.LIMIT, {"n": 10, "offset": 1}), output="o", inputs=("o2",)),
                    Binding(Op(OpKind.EMIT, {"target": "o"}), inputs=("o",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["o"]}),
            ),
        ),
    )


def test_comparison_jsonb_distinct_offset_matches_interpreter():
    prog = _filter_program()
    reg = _reg(("nodes", "tmp_fn"))
    assert prog.validate(reg) == []

    interp = evaluate(prog, {"nodes": FNODES}, kernels={})
    interp_rows = [r["subtype"] for r in interp.outputs["o"]]

    sql, params = lower_query(prog, reg)
    assert "SELECT DISTINCT" in sql and "jsonb_exists" in sql and "OFFSET 1" in sql

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tmp_fn (node_id int, subtype text, score int, attrs jsonb)")
        cur.executemany(
            "INSERT INTO tmp_fn VALUES (%s,%s,%s,%s::jsonb)",
            [(n["node_id"], n["subtype"], n["score"], json.dumps(n["attrs"])) for n in FNODES],
        )
        cur.execute(sql, params)
        sql_rows = [row[0] for row in cur.fetchall()]

    # qualifying subtypes (score>5 AND attrs?'text'): paper, paper, evidence
    # -> DISTINCT {paper, evidence} -> ORDER {evidence, paper} -> OFFSET 1 -> [paper]
    assert sql_rows == interp_rows == ["paper"]
