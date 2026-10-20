"""sqlglot-side check of the run_sql relation walk (#2406).

PARSER-SPECIFIC: replaces PR #2886's pglast-only
test_run_sql_pglast_scope.py. The parser-agnostic tables live in
test_run_sql_bypass_classes.py. The relation walk on its own (without the
refused-node walk that refuses writes first) must not let a same-named CTE
cover a write target, because PostgreSQL resolves a write target through the
search path, never to a CTE.
"""
from __future__ import annotations

import pytest

from okg.substrate.mcp.sql_safety import RunSqlDisallowedRelation

ISSUE_2406_REPRO = (
    "WITH external_probe_cache AS (SELECT 1), "
    + "y AS (UPDATE external_probe_cache SET status = 'ok' RETURNING 1) "
    + "SELECT * FROM y"
)


def _nodes(sql: str) -> list:
    from okg.substrate.postgres_sql import iter_nodes, parse

    return list(iter_nodes(parse(sql, strict=True).statements[0]))


@pytest.mark.parametrize(
    ("sql", "relation"),
    [
        (ISSUE_2406_REPRO, "external_probe_cache"),
        ("WITH t AS (SELECT 1) , w AS (DELETE FROM t RETURNING 1) "
         + "SELECT * FROM w", "t"),
        ("WITH t AS (SELECT 1), w AS (INSERT INTO t (x) VALUES (1) "
         + "RETURNING 1) SELECT * FROM w", "t"),
        ("WITH t AS (SELECT 1), w AS (INSERT INTO t VALUES (1) "
         + "RETURNING 1) SELECT * FROM w", "t"),
    ],
)
def test_write_target_never_takes_cte_scope(sql: str, relation: str) -> None:
    from okg.substrate.mcp.sql_safety import _check_relation_allow_list

    with pytest.raises(RunSqlDisallowedRelation) as err:
        _check_relation_allow_list(_nodes(sql))
    assert err.value.data["relation"] == relation


def test_a_read_of_a_visible_cte_still_passes_the_walk() -> None:
    """Failure-case control: the same CTE read (not written) resolves."""
    from okg.substrate.mcp.sql_safety import _check_relation_allow_list

    _check_relation_allow_list(
        _nodes("WITH t AS (SELECT 1) SELECT * FROM t")
    )
