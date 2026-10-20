"""req.listwork.query-literals: literal '%s'-shaped LIKE patterns survive
the checkout path (operator-surface-list-work; found by the 2026-06-11
MCP dogfood wave as 'positional and named placeholders cannot be mixed')."""

from __future__ import annotations

from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter

ESCAPE = PostgresDatabaseAdapter._escape_pyformat_percents


def test_named_params_escape_bare_percent_s() -> None:
    # The wave's exact failure shape: '%supersed%' contains '%s', which the
    # escaper used to preserve as a positional placeholder, guaranteeing
    # psycopg's mixed-placeholder error next to %(name)s binds.
    sql = "SELECT 1 WHERE t LIKE '%supersed%' AND lo <= %(_chronos_branch_point)s"
    out = ESCAPE(sql, named_params=True)
    assert "'%%supersed%%'" in out
    assert "%(_chronos_branch_point)s" in out


def test_positional_params_preserve_placeholders() -> None:
    sql = "SELECT * FROM seg WHERE segment_id = %s AND note LIKE 'x%'"
    out = ESCAPE(sql, named_params=False)
    assert "= %s" in out          # real positional placeholder kept
    assert "LIKE 'x%%'" in out    # trailing literal escaped


def test_named_params_keep_generated_named_placeholders() -> None:
    sql = "SELECT %(a)s, '100%'"
    out = ESCAPE(sql, named_params=True)
    assert "%(a)s" in out
    assert "'100%%'" in out


def test_regexp_matches_with_flags_survives_named_binds() -> None:
    # The wave's other rejected shape: regexp_matches with a flags
    # argument and a percent-bearing pattern next to chronos named binds.
    sql = (
        "SELECT regexp_matches(attrs->>'text', '([a-z]+)%([0-9]+)', 'g') "
        "FROM okg.v_nodes WHERE lo <= %(_chronos_branch_point)s"
    )
    out = ESCAPE(sql, named_params=True)
    assert "'([a-z]+)%%([0-9]+)'" in out
    assert "'g'" in out
    assert "%(_chronos_branch_point)s" in out
