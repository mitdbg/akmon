"""M4.6 — advance_session, preview_advance, and auto_advance
policy tests.

Tests for the retired `changes_since` / `list_generations_since`
tools were removed in `retire-redundant-mcp-tools` (2026-04-27);
those queries are now `mcp-views.md` recipes exercised via the
`run_sql` envelope tests.
"""

from __future__ import annotations

import psycopg
import pytest

pytest.importorskip("mcp")

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import (
    advance_session,
    inspect_graph,
    open_session,
    preview_advance,
)
from okg.substrate.mcp.server import build_server
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


# ---------- helpers ----------


def _insert_node_facts(
    conn: psycopg.Connection,
    nodes: list[tuple[str, str, str]],
) -> None:
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [
            NodeFact(
                node_id=nid,
                subtype=subtype,
                attrs={"text": text},
                source_record_id={"id": nid},
                source_revision={"v": 1},
            )
            for nid, subtype, text in nodes
        ]
    )


# ---------- advance_session ----------


def test_advance_session_returns_new_session(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:a", "page", "a")])
    r1 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s1 = open_session(conn, deployment_name="default")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_node_facts(conn, [("n:b", "page", "b")])
    r2 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s2 = advance_session(conn, s1, to="latest")
    assert s1.generation_id == r1["generation_id"]
    assert s2.generation_id == r2["generation_id"]
    # s1 is unchanged (frozen dataclass).
    assert s1.generation_id != s2.generation_id
    # Old session still points at old gen.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        stats_old = inspect_graph(conn, s1)
        stats_new = inspect_graph(conn, s2)
    assert stats_old["node_count"] == 1
    assert stats_new["node_count"] == 2


def test_advance_session_refuses_backwards(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:a", "page", "a")])
    r1 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_node_facts(conn, [("n:b", "page", "b")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s_latest = open_session(conn, deployment_name="default")
        with pytest.raises(SubstrateError, match="only moves forward"):
            advance_session(conn, s_latest, to=r1["generation_id"])


def test_advance_session_preserves_db_fingerprint(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:a", "page", "a")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s1 = open_session(conn, deployment_name="default")
        s2 = advance_session(conn, s1, to="latest")
    assert s1.db_fingerprint == s2.db_fingerprint


# ---------- preview_advance ----------


def test_preview_advance_does_not_mutate(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:a", "page", "a")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
    old_gen = s.generation_id

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_node_facts(conn, [("n:b", "page", "b")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        preview = preview_advance(conn, s, to="latest")
    assert s.generation_id == old_gen  # session not mutated
    assert preview["would_advance"] is True
    assert preview["added_node_count"] == 1


# ---------- auto_advance policy ----------


def test_auto_advance_never_uses_cache(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default policy: latest is cached for 500ms; a new publish
    within that window doesn't show up."""
    monkeypatch.setattr(
        "okg.substrate.mcp.server.LATEST_PIN_CACHE_TTL_S",
        60.0,
    )
    monkeypatch.setenv("OKG_MCP_RO_DSN", clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:a", "page", "a")])
    r1 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    mcp = build_server(
        dsn=clean_runtime,
        auto_advance="never",
    )
    tool = mcp._tool_manager._tools["inspect"]
    out1 = tool.fn(target={"kind": "graph", "legacy": "graph_stats"})
    assert out1["generation_id"] == r1["generation_id"]

    # Publish immediately.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_node_facts(conn, [("n:b", "page", "b")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    # Within the 500ms cache, we still see r1's gen.
    out2 = tool.fn(target={"kind": "graph", "legacy": "graph_stats"})
    assert out2["generation_id"] == r1["generation_id"]
    mcp._okg_pool.close()


def test_auto_advance_always_is_always_fresh(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Policy `always`: every call bypasses the cache."""
    monkeypatch.setenv("OKG_MCP_RO_DSN", clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:a", "page", "a")])
    r1 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    mcp = build_server(
        dsn=clean_runtime,
        auto_advance="always",
    )
    tool = mcp._tool_manager._tools["inspect"]
    out1 = tool.fn(target={"kind": "graph", "legacy": "graph_stats"})
    assert out1["generation_id"] == r1["generation_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_node_facts(conn, [("n:b", "page", "b")])
    r2 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    # Immediately (well within 500ms) — `always` ignores cache.
    out2 = tool.fn(target={"kind": "graph", "legacy": "graph_stats"})
    assert out2["generation_id"] == r2["generation_id"]
    assert out2["node_count"] == 2
    mcp._okg_pool.close()


def test_auto_advance_always_honors_explicit_pin(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Even the uncached live-picture mode cannot replace an explicit pin."""
    monkeypatch.setenv("OKG_MCP_RO_DSN", clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:0", "page", "t0")])
    r1 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_node_facts(conn, [("n:1", "page", "t1")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    mcp = build_server(dsn=clean_runtime, auto_advance="always")
    tool = mcp._tool_manager._tools["inspect"]
    out = tool.fn(
        target={"kind": "graph", "legacy": "graph_stats"},
        generation_id=r1["generation_id"],
    )
    assert out["generation_id"] == r1["generation_id"]
    assert out["node_count"] == 1
    assert out["pin_scope"] == "explicit_call"
    assert "pin_advanced_from" not in out
    mcp._okg_pool.close()


def test_auto_advance_when_behind_is_retired() -> None:
    with pytest.raises(ValueError, match="retired.*session.*refresh"):
        build_server(auto_advance="when_behind_by:2")


def test_build_server_rejects_bad_auto_advance() -> None:
    with pytest.raises(ValueError, match="auto_advance"):
        build_server(auto_advance="whenever_i_feel_like_it")
    with pytest.raises(ValueError, match="N >= 1"):
        build_server(auto_advance="when_behind_by:0")


# `advance_session` and `preview_advance` (M4.6 session-control tools)
# were removed by `shrink-mcp-to-operator-surface`. Their behavior —
# deliberate logical-session refresh on the server side — is exercised by the
# focused MCP transport tests. The request-scoped `test_auto_advance_*` tests
# above preserve the legacy cache/always boundary without moving explicit pins.
# The retired tools
# `list_generations_since` and `changes_since` were removed earlier
# by `retire-redundant-mcp-tools` (2026-04-27).
