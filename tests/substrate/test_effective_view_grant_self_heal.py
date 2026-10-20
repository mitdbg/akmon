"""okg#400 self-heal must not silently drop the okg_mcp read grant.

The effective-attribute view tables are granted SELECT to the constrained okg_mcp
read pool at migration (the allow-list in db/views.sql). The materialize self-heal
path DROPs + recreates okg.attribute_effective_view to reclaim disk; a recreated
table loses its grants, after which the MCP attr operators (filter/map/aggregate on
the okg_mcp pool) fail "permission denied for table attribute_effective_view" until
the deployment is re-migrated. ensure_attribute_schema now re-applies the grant on
(re)create, so operator reads survive a self-heal without a re-migration.
"""
from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.attributes import (  # noqa: E402
    ATTRIBUTE_EFFECTIVE_TABLE,
    ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
    ensure_attribute_schema,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_DEPLOYMENT = "default"


def _granted(conn: psycopg.Connection, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT has_table_privilege('okg_mcp', %s, 'SELECT')", (table,))
        return bool(cur.fetchone()[0])


def _exists(conn: psycopg.Connection, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (table,))
        return cur.fetchone()[0] is not None


def _publish_one(dsn: str, node_id: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    node_id,
                    "doc",
                    {"k": node_id},
                    {"id": node_id},
                    {"v": 1},
                )
            ],
            source="default",
            strict_endpoints=True,
        )
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()


def test_self_heal_recreate_restores_okg_mcp_grant(clean_runtime: str) -> None:
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _publish_one(clean_runtime, f"doc:{suffix}")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Baseline: the migration granted okg_mcp SELECT on the view.
        assert _granted(conn, ATTRIBUTE_EFFECTIVE_VIEW_TABLE), "baseline grant missing"
        # Simulate the okg#400 disk-reclaim DROP of the derived view cache.
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}")

    # Republish: materialize self-heals (recreates the view via ensure_attribute_schema).
    _publish_one(clean_runtime, f"doc2:{suffix}")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _exists(conn, ATTRIBUTE_EFFECTIVE_VIEW_TABLE), "view not self-healed"
        # The fix: the recreated table is granted to okg_mcp again. Without it the
        # recreate loses the grant and MCP attr reads break until a re-migration.
        assert _granted(
            conn, ATTRIBUTE_EFFECTIVE_VIEW_TABLE
        ), "okg_mcp SELECT not restored on self-heal recreate"


def test_ensure_attribute_schema_regrants_after_revoke(clean_runtime: str) -> None:
    """Direct: ensure_attribute_schema re-applies the okg_mcp grant even if it was
    lost (revoked / dropped-with-table), on BOTH effective-view tables."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"REVOKE SELECT ON {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}, "
                f"{ATTRIBUTE_EFFECTIVE_TABLE} FROM okg_mcp"
            )
        assert not _granted(conn, ATTRIBUTE_EFFECTIVE_VIEW_TABLE)
        assert not _granted(conn, ATTRIBUTE_EFFECTIVE_TABLE)

        ensure_attribute_schema(conn)

        assert _granted(conn, ATTRIBUTE_EFFECTIVE_VIEW_TABLE)
        assert _granted(conn, ATTRIBUTE_EFFECTIVE_TABLE)
