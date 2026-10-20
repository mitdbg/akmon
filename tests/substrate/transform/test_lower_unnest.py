"""UNNEST lateral op — interpreter == lowered SQL (checks-as-ir harness debt).

The per-link cern-twiki findings (unresolved_internal_link) need one output row
PER ARRAY ELEMENT, which the fourteen-op core could not express. UNNEST lowers
to ``CROSS JOIN LATERAL jsonb_array_elements_text(expr)``; this proves the
interpreter and the lowered SQL agree on a seeded fixture — including the NULL
array (no rows), NULL elements (dropped by the WHERE's NULL semantics), and
non-text elements (jsonb text form) — and that shapes the lowering cannot
compile faithfully are REJECTED, not mis-compiled.
"""

from __future__ import annotations

import json
import os

import pytest

from okg.substrate.transform import (
    Binding,
    Block,
    Effect,
    InterpreterError,
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
    lower_query,
    not_,
)
from okg.substrate.transform.expr import regex_match
from okg.substrate.transform.lower import LoweringError

psycopg = pytest.importorskip("psycopg")

DSN = os.environ.get("OKG_TEST_DSN")
pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(not DSN, reason="OKG_TEST_DSN not set"),
]

# Twiki-shaped fixture: page 1 has a sidebar link (filtered), a real target, a
# JSON null element, and a numeric element; page 2 an empty array; page 3 NULL.
PAGES = [
    {"page_id": 1, "name": "WebHome", "links": ["WebLeftBar", "TargetA", None, 7]},
    {"page_id": 2, "name": "Empty", "links": []},
    {"page_id": 3, "name": "NullLinks", "links": None},
]


def _registry() -> Registries:
    return Registries(
        surfaces=SurfaceRegistry(
            [Surface("pages", SurfaceKind.LIVE, "tmp_pages", pinned=True)]
        )
    )


def _per_link_program(bindings_override=None) -> TransformProgram:
    bindings = bindings_override or (
        Binding(Op(OpKind.FROM, {"surface": "pages", "alias": "p"}), output="p"),
        Binding(
            Op(OpKind.UNNEST, {"expr": col("p.links"), "field": "link"}),
            output="exploded", inputs=("p",),
        ),
        Binding(
            Op(OpKind.WHERE, {"expr": not_(regex_match(col("link"), r"^Web"))}),
            output="kept", inputs=("exploded",),
        ),
        Binding(
            Op(OpKind.SELECT, {"project": {"name": col("p.name"), "link": col("link")}}),
            output="out", inputs=("kept",),
        ),
        Binding(Op(OpKind.EMIT, {"target": "result"}), inputs=("out",)),
    )
    return TransformProgram(
        program_id="consistency.per_link",
        effect=Effect.READ_ONLY,
        outputs=("out",),
        entry_block="entry",
        blocks=(
            Block(
                block_id="entry",
                bindings=bindings,
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["out"]}),
            ),
        ),
    )


def _seed(cur, pages) -> None:
    cur.execute("CREATE TEMP TABLE tmp_pages (page_id int, name text, links jsonb)")
    cur.executemany(
        "INSERT INTO tmp_pages VALUES (%s, %s, %s)",
        [
            (
                p["page_id"],
                p["name"],
                json.dumps(p["links"]) if p["links"] is not None else None,
            )
            for p in pages
        ],
    )


def test_unnest_lowered_sql_equals_interpreter():
    prog = _per_link_program()
    reg = _registry()
    assert prog.validate(reg) == []

    interp = evaluate(prog, {"pages": PAGES}, kernels={})
    interp_rows = sorted((r["name"], r["link"]) for r in interp.values["out"])

    sql, params = lower_query(prog, reg)
    assert "CROSS JOIN LATERAL jsonb_array_elements_text" in sql

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        _seed(cur, PAGES)
        cur.execute(sql, params)
        sql_rows = sorted((row[0], row[1]) for row in cur.fetchall())
        cur.execute("DROP TABLE tmp_pages")

    # One row per surviving element: the null element and Web* are filtered,
    # the numeric element carries its jsonb text, empty/NULL arrays yield none.
    assert interp_rows == [("WebHome", "7"), ("WebHome", "TargetA")]
    assert sql_rows == interp_rows


def test_unnest_non_array_errors_on_both_evaluators():
    prog = _per_link_program()
    reg = _registry()
    bad = [{"page_id": 9, "name": "Scalar", "links": {"not": "array"}}]

    with pytest.raises(InterpreterError, match="not a JSON array"):
        evaluate(prog, {"pages": bad}, kernels={})

    sql, params = lower_query(prog, reg)
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        _seed(cur, bad)
        with pytest.raises(psycopg.errors.InvalidParameterValue):
            cur.execute(sql, params)
        cur.execute("DROP TABLE tmp_pages")


def test_lowering_rejects_where_before_unnest():
    bindings = (
        Binding(Op(OpKind.FROM, {"surface": "pages", "alias": "p"}), output="p"),
        Binding(
            Op(OpKind.WHERE, {"expr": eq(col("p.name"), col("p.name"))}),
            output="w", inputs=("p",),
        ),
        Binding(
            Op(OpKind.UNNEST, {"expr": col("p.links"), "field": "link"}),
            output="out", inputs=("w",),
        ),
        Binding(Op(OpKind.EMIT, {"target": "result"}), inputs=("out",)),
    )
    with pytest.raises(LoweringError, match="UNNEST before the WHERE"):
        lower_query(_per_link_program(bindings), _registry())


def test_lowering_rejects_join_after_unnest():
    bindings = (
        Binding(Op(OpKind.FROM, {"surface": "pages", "alias": "p"}), output="p"),
        Binding(Op(OpKind.FROM, {"surface": "pages", "alias": "q"}), output="q"),
        Binding(
            Op(OpKind.UNNEST, {"expr": col("p.links"), "field": "link"}),
            output="exploded", inputs=("p",),
        ),
        Binding(
            Op(OpKind.JOIN, {"how": "left", "on": eq(col("p.page_id"), col("q.page_id"))}),
            output="out", inputs=("exploded", "q"),
        ),
        Binding(Op(OpKind.EMIT, {"target": "result"}), inputs=("out",)),
    )
    with pytest.raises(LoweringError, match="JOIN before any UNNEST"):
        lower_query(_per_link_program(bindings), _registry())


def test_lowering_rejects_non_identifier_field():
    bindings = (
        Binding(Op(OpKind.FROM, {"surface": "pages", "alias": "p"}), output="p"),
        Binding(
            Op(OpKind.UNNEST, {"expr": col("p.links"), "field": "link); DROP TABLE x--"}),
            output="out", inputs=("p",),
        ),
        Binding(Op(OpKind.EMIT, {"target": "result"}), inputs=("out",)),
    )
    with pytest.raises(LoweringError, match="not a plain identifier"):
        lower_query(_per_link_program(bindings), _registry())


def test_validation_requires_args_and_arity():
    bindings = (
        Binding(Op(OpKind.FROM, {"surface": "pages", "alias": "p"}), output="p"),
        Binding(Op(OpKind.UNNEST, {"expr": col("p.links")}), output="out", inputs=()),
        Binding(Op(OpKind.EMIT, {"target": "result"}), inputs=("out",)),
    )
    errors = _per_link_program(bindings).validate(_registry())
    codes = {e.code for e in errors}
    assert "missing_op_arg" in codes  # no `field`
    assert "unnest_arity" in codes  # no relation input
