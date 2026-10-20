"""ivm-effective-attribute-view / task.interval-row-representation (read side).

With OKG_IVM_INTERVAL_VIEW on, the three effective-view readers (the ready probe,
the filter/map fast-row scan, and the aggregate group-by) STAB the shared interval
table instead of the per-generation snapshot. This proves they return IDENTICAL
results to the snapshot path (flag off) through the REAL operator surface
(aggregate + filter), so the representation swap is invisible to operators.
"""
from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.attributes import (  # noqa: E402
    ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
    sync_effective_interval_view_for_generation,
)
from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.mcp.server import build_server  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _agg(tools, gen: str):
    return tools["aggregate"].fn(
        input={"kind": "nodes", "subtype": "doc"},
        by=["attrs.k"],
        metrics=["count"],
        generation_id=gen,
    )


def _filter_ids(tools, gen: str) -> list[str]:
    res = tools["filter"].fn(
        input={"kind": "nodes", "subtype": "doc"},
        predicate={"op": "eq", "field": "attrs.k", "value": "v1"},
        generation_id=gen,
        limit=1000,
    )
    return sorted(item.get("node_id") for item in res["items"])


def test_interval_readers_match_snapshot(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_MCP_RO_DSN", clean_runtime)
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
        store = ChronosGraphStore.from_connection(conn)
        facts = [
            NodeFact(
                f"doc:{i}:{suffix}",
                "doc",
                {"k": f"v{i % 4}"},
                {"id": f"doc:{i}:{suffix}"},
                {"v": 1},
            )
            for i in range(20)
        ]
        store.apply_facts(facts, source="default", strict_endpoints=True)
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    gen = str(Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()["generation_id"])

    # Fold the published snapshot into the interval table (20 nodes -> 20 opens).
    # sync self-grants the okg_mcp read pool on the interval + ordinal tables; also
    # (re)assert okg_mcp's read on the SNAPSHOT tables so the flag-off comparison is
    # order-independent in the shared test DB (a sibling self-heal test can drop +
    # recreate okg.attribute_effective_view, which loses the migration-time grant).
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        db = ChronosGraphStore.from_connection(conn, ensure_schema=False).context.db
        r = sync_effective_interval_view_for_generation(conn, db, generation_id=gen)
        assert r["rows_written"] == 20, r
        with conn.cursor() as cur:
            cur.execute(
                "GRANT SELECT ON okg.attribute_effective_view, "
                "okg.attribute_effective_attrs TO okg_mcp"
            )

    mcp = build_server(dsn=clean_runtime)
    tools = mcp._tool_manager._tools
    try:
        monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
        agg_snap = {g["attrs.k"]: g["count"] for g in _agg(tools, gen)["groups"]}
        filt_snap = _filter_ids(tools, gen)

        monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
        agg_iv = {g["attrs.k"]: g["count"] for g in _agg(tools, gen)["groups"]}
        filt_iv = _filter_ids(tools, gen)

        monkeypatch.delenv("OKG_IVM_INTERVAL_VIEW", raising=False)
        agg_default = {
            g["attrs.k"]: g["count"] for g in _agg(tools, gen)["groups"]
        }
        filt_default = _filter_ids(tools, gen)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    # Aggregate group-by (exercises the ready probe + aggregate reader).
    assert agg_snap == {"v0": 5, "v1": 5, "v2": 5, "v3": 5}, agg_snap
    assert agg_iv == agg_snap, (agg_iv, agg_snap)
    assert agg_default == agg_snap, (agg_default, agg_snap)

    # Filter fast-row scan (exercises the ready probe + fast-rows reader).
    assert len(filt_snap) == 5, filt_snap
    assert filt_iv == filt_snap, (filt_iv, filt_snap)
    assert filt_default == filt_snap, (filt_default, filt_snap)


def test_interval_reader_fails_loud_without_backfill(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_MCP_RO_DSN", clean_runtime)
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    f"doc:{i}:{suffix}",
                    "doc",
                    {"k": "v"},
                    {"id": f"doc:{i}:{suffix}"},
                    {"v": 1},
                )
                for i in range(3)
            ],
            source="default",
            strict_endpoints=True,
        )
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    gen = str(Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()["generation_id"])

    mcp = build_server(dsn=clean_runtime)
    tools = mcp._tool_manager._tools
    try:
        monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
        with pytest.raises(SubstrateError, match="incomplete interval stab rows"):
            _agg(tools, gen)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_interval_reader_fails_loud_on_partial_interval_authority(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_MCP_RO_DSN", clean_runtime)
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    f"doc:{i}:{suffix}",
                    "doc",
                    {"k": "v"},
                    {"id": f"doc:{i}:{suffix}"},
                    {"v": 1},
                )
                for i in range(3)
            ],
            source="default",
            strict_endpoints=True,
        )
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    gen = str(Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()["generation_id"])

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        db = ChronosGraphStore.from_connection(conn, ensure_schema=False).context.db
        r = sync_effective_interval_view_for_generation(conn, db, generation_id=gen)
        assert r["rows_written"] == 3, r
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "WHERE ctid IN ("
                f"SELECT ctid FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "WHERE target_kind = 'node' LIMIT 1)"
            )

    mcp = build_server(dsn=clean_runtime)
    tools = mcp._tool_manager._tools
    try:
        monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
        with pytest.raises(SubstrateError, match="incomplete interval stab rows"):
            _agg(tools, gen)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
