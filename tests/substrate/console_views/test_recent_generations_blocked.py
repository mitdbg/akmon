"""Regression for #687 (remediate-audit-2026-07 / sweep-console).

The recent-generations console panel read per-generation node/edge counts via
``generation_counts``, which fails loud (``GenerationCountsUnavailable``) on any
non-published generation. The panel did NOT catch that, so a single BLOCKED
publish anywhere in the window turned the whole panel into an HTTP 500 —
exactly when the operator most needs to see what blocked.

Conformance (design.md Section 5, row ``sweep-console``): a blocked generation
degrades to a typed unknown-delta row carrying ``blocked_reason`` — never a 500,
never a silent zero. This test drives REAL publishes (one forced-blocked in the
middle of the window) and asserts the composer returns 200 with a degraded row.
It fails on the pre-fix code (uncaught ``GenerationCountsUnavailable``).
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_DEP = "default"
_FLAG = "OKG_IVM_CONSOLE_COUNT_ROLLUP"


def _node(node_id: str, subtype: str) -> NodeFact:
    return NodeFact(
        node_id=node_id, subtype=subtype, attrs={},
        source_record_id={"id": node_id}, source_revision={"v": 1},
    )


def _apply(dsn: str, facts, *, source: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(facts, source=source, strict_endpoints=False)


def _publish(dsn: str):
    from okg.substrate.db.pool import close_all

    close_all()
    publisher = Publisher(dsn, _DEP, catalog_identity=bind_test_catalog_writer(dsn, _DEP))
    result = publisher.cycle()
    close_all()
    return result


def _publish_with_block(dsn: str):
    """A cycle whose nomos publish-blocker trips, producing a BLOCKED generation
    with a real pinned graph but status='blocked' (no published stamped counts)."""
    from okg.substrate.db.pool import close_all
    import okg.substrate.publisher.publisher as pub_mod

    close_all()
    publisher = Publisher(dsn, _DEP, catalog_identity=bind_test_catalog_writer(dsn, _DEP))
    original = pub_mod.Publisher._nomos_generation_publish_blocker

    def _blocker(self, *a, **k):
        return {"reason": "probe_forced_block", "detail": "probe"}

    pub_mod.Publisher._nomos_generation_publish_blocker = _blocker
    try:
        result = publisher.cycle()
    finally:
        pub_mod.Publisher._nomos_generation_publish_blocker = original
        close_all()
    return result


def _seed_catalog(dsn: str) -> None:
    from tests.substrate._db_helpers import seed_catalog_version

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)


def test_recent_generations_degrades_on_blocked_window(
    clean_runtime: str, monkeypatch
) -> None:
    """A blocked generation in the window -> 200 with a degraded unknown-delta
    row carrying blocked_reason, NOT an HTTP 500 and NOT a fabricated zero."""
    from okg.substrate.console_views.generations import compose_generations

    _seed_catalog(clean_runtime)
    _apply(clean_runtime, [_node("doc:a", "document")], source="git")
    assert _publish(clean_runtime)["status"] == "published"
    _apply(clean_runtime, [_node("doc:b", "document")], source="git")
    assert _publish(clean_runtime)["status"] == "published"
    # A BLOCKED generation in the middle of the window.
    _apply(clean_runtime, [_node("doc:c", "document")], source="git")
    assert _publish_with_block(clean_runtime)["status"] == "blocked"
    _apply(clean_runtime, [_node("doc:d", "document")], source="git")
    assert _publish(clean_runtime)["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=_DEP)
        monkeypatch.setenv(_FLAG, "1")
        # Pre-fix: this raises GenerationCountsUnavailable -> HTTP 500.
        payload = compose_generations(conn, deployment_name=_DEP, session=session)

    rows = payload["generations"]
    assert rows, "expected at least one generation row"

    blocked = [r for r in rows if r["status"] == "blocked"]
    assert blocked, f"expected a blocked row in the window; got {[r['status'] for r in rows]}"

    for row in blocked:
        # Degraded, typed, unknown-delta — never a silent zero.
        assert row["delta_available"] is False
        assert row["delta_unavailable_reason"], "degraded row must carry a reason"
        assert row["nodes_added"] is None
        assert row["nodes_retracted"] is None
        assert row["edges_added"] is None
        assert row["edges_retracted"] is None
        # zero_delta is the silent-upstream-failure tell — an unknown delta is
        # NOT zero and must never masquerade as one.
        assert row["zero_delta"] is False
        # The blocked reason travels with the row.
        assert row["blocked_reason"], "blocked row must expose blocked_reason"

    # The published generation IMMEDIATELY after the blocked one cannot compute a
    # delta against the (unstamped) blocked previous — it degrades too, rather
    # than fabricating a delta off a zero baseline.
    published_rows = [r for r in rows if r["status"] == "published"]
    assert any(r["delta_available"] for r in published_rows), (
        "at least one published generation should still carry a real delta"
    )
