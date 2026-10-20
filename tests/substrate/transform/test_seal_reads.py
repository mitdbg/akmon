"""seal-graph-reads-behind-the-ir (app_ro half) -- the bounded SQL escape hatch is
confined to the registered okg.v_* surfaces.

The bounded SQL escape hatch (run_sql / validate_run_sql) refuses ANY relation that
is not an okg.v_* registered surface -- a query naming a base graph table is rejected
with run_sql_disallowed_relation; a query over okg.v_nodes is accepted. This holds by
relation-name allowlist independent of which base tables physically exist.

NOTE: the app_ro direct-SELECT seal on okg.nodes_live/edges_live was retired in the
chronos-native cutover -- those base graph tables were dropped (the graph now lives in
okg.graph_nodes/graph_edges read through a Chronos checkout), so there is no longer a
base-table grant to revoke. The escape-hatch allowlist below is the surviving read-seal
enforcement.
"""

from __future__ import annotations

import pytest

from okg.substrate.mcp.sql_safety import (  # noqa: E402
    RunSqlDisallowedRelation,
    validate_run_sql,
)

psycopg = pytest.importorskip("psycopg")

pytestmark = pytest.mark.postgres

_BASE_GRAPH_TABLES = ("nodes_live", "edges_live", "node_facts", "edge_facts")


def test_escape_hatch_refuses_base_graph_tables() -> None:
    """run_sql rejects a SELECT naming a base graph table, accepts okg.v_* views."""
    for tbl in _BASE_GRAPH_TABLES:
        with pytest.raises(RunSqlDisallowedRelation) as exc:
            validate_run_sql(f"SELECT * FROM okg.{tbl} LIMIT 1")
        assert exc.value.code == "run_sql_disallowed_relation"
        assert f"okg.{tbl}" in str(exc.value.data.get("relation", ""))
    # Registered surfaces are accepted.
    assert validate_run_sql("SELECT node_id FROM okg.v_nodes LIMIT 1") is None
    assert validate_run_sql(
        "SELECT e.edge_id FROM okg.v_edges e JOIN okg.v_nodes n ON n.node_id = e.dst"
    ) is None
