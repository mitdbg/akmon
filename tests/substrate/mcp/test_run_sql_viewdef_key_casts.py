"""Viewdef key-cast normalization (fix-source-run-telemetry-reads
req.telemetry.v-source-runs-readable).

pg_get_viewdef renders `payload -> 'k'` as `payload -> 'k'::text`; the
cast binds to the KEY under Postgres precedence and is a no-op.
sqlglot (<= 30.10.0) parses it as `(payload -> 'k')::text` and
re-renders CAST(... AS TEXT), turning jsonb expressions into text —
okg.v_source_runs then fails at plan time with `COALESCE types text
and jsonb cannot be matched` through the MCP query tool.
"""

from __future__ import annotations

import sqlglot

from okg.substrate.mcp.run_sql import _normalize_viewdef_key_casts


def test_strips_redundant_key_casts_for_both_arrows() -> None:
    definition = (
        "SELECT COALESCE((payload -> 'timings_ms'::text), '{}'::jsonb) AS t,"
        " payload ->> 'status'::text AS s,"
        " payload -> 'a''b'::text AS quoted"
        " FROM okg.source_watermarks"
    )
    out = _normalize_viewdef_key_casts(definition)
    assert "'timings_ms'::text" not in out
    assert "'status'::text" not in out
    assert "'a''b'::text" not in out
    assert "-> 'timings_ms'" in out
    assert "->> 'status'" in out
    # Unrelated casts are untouched.
    assert "'{}'::jsonb" in out


def test_normalized_definition_round_trips_as_jsonb() -> None:
    # The live failure shape: after normalization sqlglot must NOT
    # introduce CAST(... AS TEXT) around the arrow expression.
    definition = _normalize_viewdef_key_casts(
        "SELECT COALESCE((payload -> 'timings_ms'::text), '{}'::jsonb) AS t "
        "FROM okg.source_watermarks"
    )
    rendered = sqlglot.parse_one(definition, read="postgres").sql(
        dialect="postgres"
    )
    assert "AS TEXT" not in rendered.upper()
    assert "JSONB" in rendered.upper()


def test_non_literal_and_unrelated_casts_survive() -> None:
    definition = (
        "SELECT (payload -> key_col)::text AS legit_cast,"
        " other::text AS plain"
        " FROM t"
    )
    assert _normalize_viewdef_key_casts(definition) == definition
