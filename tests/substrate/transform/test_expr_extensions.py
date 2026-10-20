"""Dual-eval parity for the expr forms added for IR check re-authoring.

PACT consistency-checks-as-ir, task.ir.equivalence-harness (the recorded
expressiveness debt): regex match (~/~*), CASE, string concat, boolean casts
with PG truthiness, and EXISTS-over-jsonb-array. Every case runs through BOTH
evaluators — eval_expr (interpreter) and render_sql executed on live Postgres
— and asserts they agree, including NULL propagation. The prior review proved
divergences here ship real bugs (btrim, LIKE escapes), so parity is asserted
against the database, not against expectations.
"""
from __future__ import annotations

import json

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.transform.expr import (  # noqa: E402
    case_,
    cast,
    col,
    concat,
    eq,
    eval_expr,
    jsonb_array_any,
    lit,
    not_,
    regex_match,
    render_sql,
    starts_with,
)

pytestmark = pytest.mark.postgres

# (expr-builder, row) cases. Column c is text; column j is jsonb.
CASES = [
    # regex: match, no-match, case-insensitivity, NULL propagation (incl. NOT)
    (regex_match(col("c"), r"^(Web)?(Left|Right)Bar$"), {"c": "WebLeftBar"}),
    (regex_match(col("c"), r"^(Web)?(Left|Right)Bar$"), {"c": "webleftbar"}),
    (regex_match(col("c"), r"^(web)?(left|right)bar$", case_insensitive=True),
     {"c": "WebLeftBar"}),
    (regex_match(col("c"), r"bar"), {"c": None}),
    (not_(regex_match(col("c"), r"bar")), {"c": None}),
    (not_(regex_match(col("c"), r"bar")), {"c": "foo"}),
    # CASE: first-true-wins, else, no-else -> NULL
    (case_([(eq(col("c"), lit("a")), lit("first")),
            (eq(col("c"), lit("b")), lit("second"))], lit("other")),
     {"c": "b"}),
    (case_([(eq(col("c"), lit("a")), lit("first"))], lit("other")), {"c": "z"}),
    (case_([(eq(col("c"), lit("a")), lit("first"))]), {"c": "z"}),
    # concat: plain, NULL poisons
    (concat(lit("x:"), col("c")), {"c": "tail"}),
    (concat(lit("x:"), col("c")), {"c": None}),
    # boolean cast truthiness (PG accepts t/true/1/yes/on & friends)
    (eq(cast(col("c"), "boolean"), lit(True)), {"c": "true"}),
    (eq(cast(col("c"), "boolean"), lit(True)), {"c": "T"}),
    (eq(cast(col("c"), "boolean"), lit(True)), {"c": "off"}),
    (eq(cast(col("c"), "boolean"), lit(False)), {"c": "0"}),
    # jsonb array any: like both-markers shape, regex mode, empty, NULL
    (jsonb_array_any(col("j"), "<<<<<<<%"), {"j": ["<<<<<<< HEAD", "======="]}),
    (jsonb_array_any(col("j"), ">>>>>>>%"), {"j": ["<<<<<<< HEAD", "======="]}),
    (jsonb_array_any(col("j"), r"^=+$", mode="regex"), {"j": ["=======", "x"]}),
    (jsonb_array_any(col("j"), r"^HEAD$", mode="iregex"), {"j": ["head"]}),
    (jsonb_array_any(col("j"), "%x%"), {"j": []}),
    (jsonb_array_any(col("j"), "%x%"), {"j": None}),
    # JSON null element is SQL NULL (never "null" text) — no match
    (jsonb_array_any(col("j"), "%null%"), {"j": [None]}),
    # starts_with: static prefix, column-to-column (heading-path subsumption
    # shape, both directions), NULL strictness (incl. NOT), %-in-data safety
    (starts_with(col("c"), lit("Reco > ")), {"c": "Reco > HCAL"}),
    (starts_with(col("c"), lit("Reco > ")), {"c": "Reco"}),
    (starts_with(col("c"), concat(col("d"), lit(" > "))),
     {"c": "Reco > HCAL", "d": "Reco"}),
    (starts_with(col("c"), concat(col("d"), lit(" > "))),
     {"c": "Reco", "d": "Reco > HCAL"}),
    (starts_with(col("c"), lit("x")), {"c": None}),
    (starts_with(col("c"), col("d")), {"c": "a", "d": None}),
    (not_(starts_with(col("c"), col("d"))), {"c": "a", "d": None}),
    (starts_with(col("c"), col("d")), {"c": "100% done", "d": "100%"}),
]


def _sql_eval(conn, expr: dict, row: dict):
    params: dict = {}
    rendered = render_sql(expr, params)
    params["c"] = row.get("c")
    params["d"] = row.get("d")
    params["j"] = json.dumps(row["j"]) if row.get("j") is not None else None
    sql = (
        f"SELECT {rendered} FROM (SELECT %(c)s::text AS c, "
        "%(d)s::text AS d, %(j)s::jsonb AS j) AS t"
    )
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def test_new_expr_forms_agree_with_postgres(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime) as conn:
        for expr, row in CASES:
            interpreted = eval_expr(expr, row, {})
            db = _sql_eval(conn, expr, row)
            assert interpreted == db, (
                f"divergence for {expr!r} on {row!r}: "
                f"interpreter={interpreted!r} postgres={db!r}"
            )


def test_boolean_cast_rejects_garbage_both_sides(clean_runtime: str) -> None:
    expr = cast(col("c"), "boolean")
    with pytest.raises(ValueError):
        eval_expr(expr, {"c": "maybe"}, {})
    with psycopg.connect(clean_runtime) as conn:
        with pytest.raises(psycopg.errors.InvalidTextRepresentation):
            _sql_eval(conn, expr, {"c": "maybe"})


def test_jsonb_array_any_non_array_errors_both_sides(clean_runtime: str) -> None:
    expr = jsonb_array_any(col("j"), "%x%")
    with pytest.raises(ValueError):
        eval_expr(expr, {"j": {"not": "array"}}, {})
    with psycopg.connect(clean_runtime) as conn:
        with pytest.raises(psycopg.errors.InvalidParameterValue):
            params: dict = {}
            rendered = render_sql(expr, params)
            params["j"] = json.dumps({"not": "array"})
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {rendered} FROM (SELECT %(j)s::jsonb AS j) AS t",
                    params,
                )


def test_jsonb_array_any_rejects_unknown_mode() -> None:
    with pytest.raises(ValueError, match="mode"):
        jsonb_array_any(col("j"), "x", mode="glob")
