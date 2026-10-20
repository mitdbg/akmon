"""Per-test Chronos reset shared by the substrate + views test conftests.

The relational `clean_runtime` fixtures TRUNCATE the okg runtime tables, but the
Chronos graph + its published-generation checkpoints live in separate
``public._chronos_*`` interval/branch/checkpoint tables that a relational TRUNCATE
doesn't cover (the checkout reconstructs state from the version log). Without this a
test that seeds okg.graph_nodes/graph_edges or publishes a generation leaks into later
tests in a shared DB. See PACT chronos-native-graph-cutover, finding G3.
"""
from __future__ import annotations

import psycopg
from psycopg import sql


def reset_chronos_graph(dsn: str) -> None:
    """Reset all Chronos state (graph + published checkpoints) to empty.

    TRUNCATEs every ``public._chronos_*`` table and lets ensure_schema re-create the
    empty ``main``/``default`` branches (deleting just the ``default`` branch clears
    graph content but NOT the published checkpoints, so the full TRUNCATE is required).
    No-op until Chronos has been initialized in this DB (its tables are created lazily
    by the first publish / apply_facts), so non-Chronos tests pay nothing.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                r"WHERE table_schema = 'public' AND table_name LIKE '\_chronos%'"
            )
            names = [r[0] for r in cur.fetchall()]
            if not names:
                return
            cur.execute(
                sql.SQL("TRUNCATE {} RESTART IDENTITY CASCADE").format(
                    sql.SQL(", ").join(sql.Identifier("public", n) for n in names)
                )
            )
            # The Chronos interval/segment tables above back the materialized
            # `okg.graph_nodes` / `okg.graph_edges` current-segment tables. A
            # checkout rebuilds those from the (now empty) version log, but any
            # rows written to the materialized tables OUTSIDE a checkout (e.g.
            # the standalone projector's direct upsert) are not version-tracked
            # and would survive the TRUNCATE above, leaking live nodes/edges
            # into the next test. Clear them here so the live graph is empty.
            for relation in ("okg.graph_nodes", "okg.graph_edges"):
                cur.execute("SELECT to_regclass(%s)", (relation,))
                if cur.fetchone()[0] is not None:
                    cur.execute(
                        sql.SQL("TRUNCATE {}").format(
                            sql.Identifier(*relation.split("."))
                        )
                    )
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.graph_store_internal.common import (
            clear_schema_ensured_cache,
        )
        from okg.substrate.graph_store_internal.schema import (
            _clear_mcp_grant_guard,
        )
        from chronos_core.branching._common import clear_all_registry_caches

        # The TRUNCATE above emptied _chronos_branch_tables out-of-band (not via
        # the register path that invalidates the registry cache), so drop the
        # process-level cache before re-registering — otherwise the re-register
        # would see a stale "already registered" view and skip the INSERT.
        clear_all_registry_caches()
        # Likewise the process-once schema-bootstrap guard and MCP-grant guard
        # must be dropped: the TRUNCATE removed the schema/tables out-of-band, so
        # without clearing them the re-construction below would skip the
        # bootstrap (leaving the graph unprovisioned) and the grant (leaving the
        # re-created _chronos* tables unreadable by okg_mcp).
        clear_schema_ensured_cache()
        _clear_mcp_grant_guard()
        # Re-create the empty main/default branches + register the graph tables.
        ChronosGraphStore.from_connection(conn, ensure_schema=True)
