"""The console totals (status + map meta) are served from the pinned
generation's checkpoint metadata, and FAIL LOUD (never a default) when the
checkpoint lacks trustworthy counts.

accelerate-operator-read-path: req.totals-from-precomputed-metadata.
"""
from __future__ import annotations

import uuid
from typing import Any

import psycopg
import pytest

from okg.substrate.console_views._chronos import (
    GenerationCountsUnavailable,
    generation_counts,
)
from okg.substrate.console_views import _chronos
from okg.substrate.console_views.status import compose_status
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp.session import open_session
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _seed_and_publish(dsn: str) -> None:
    sfx = uuid.uuid4().hex[:8]
    c, p = f"case:{sfx}", f"person:{sfx}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn, subtypes=["case", "person"],
            narrowings=[("case", "reported_by", "person")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(c, "case", {"x": 1}, {"id": c}, {"v": 1}),
                NodeFact(p, "person", {"x": 1}, {"id": p}, {"v": 1}),
                EdgeFact(c, p, "reported_by", attrs={},
                         source_record_id={"id": f"e:{sfx}"}, source_revision={"v": 1}),
            ],
            source="default", strict_endpoints=True,
        )
    published = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert published["status"] == "published", published


def test_generation_counts_match_live_and_status_serves_them(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    _seed_and_publish(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        # The O(1) metadata read equals the live count over the pinned checkout.
        nc, ec = generation_counts(conn, session)
        g = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(session.generation_id)
        live_n = int(g.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"] or 0)
        live_e = int(g.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"] or 0)
        assert (nc, ec) == (live_n, live_e), (nc, ec, live_n, live_e)
        assert nc >= 2 and ec >= 1

        # compose_status surfaces the same totals.
        status = compose_status(
            conn, session=session, deployment_name="default",
            operator_email=None, operator_display_name=None,
        )
        assert status["nodes"] == live_n
        assert status["edges"] == live_e


class _FakeCheckpoint:
    def __init__(self, metadata: dict[str, Any]) -> None:
        self.metadata = metadata
        self.checkpoint_id = "gen:pinned"


class _FakeStore:
    """Stands in for the store; the counts read only asks it for the id."""

    @staticmethod
    def _generation_checkpoint_id(generation_id: Any) -> str:
        return str(generation_id)


class _FakeSession:
    generation_id = "gen:pinned"
    branch_name = "default"
    db_fingerprint = "fp"


def _patch_store(monkeypatch, checkpoint) -> None:
    """Serve `checkpoint` through the projected one-row read (issue #2546).

    The fake projects exactly the keys asked for, as the statement does, so a
    guard that started reading a key outside `GENERATION_COUNT_METADATA_KEYS`
    would see `None` here as it would in production.
    """
    monkeypatch.setattr(_chronos, "assert_session_matches", lambda *_a, **_k: None)
    monkeypatch.setattr(
        _chronos.ChronosGraphStore, "from_connection",
        classmethod(lambda cls, *_a, **_k: _FakeStore()),
    )
    monkeypatch.setattr(
        _chronos, "_raise_counts_unavailable_if_compacted", lambda *_a: None,
    )

    def _read(
        _conn: Any,
        checkpoint_id: str,
        keys: tuple[str, ...],
        nested: dict[str, tuple[str, ...]] | None = None,
    ) -> Any:
        # The counts read asks for top-level keys only.
        assert not nested
        assert checkpoint_id == checkpoint.checkpoint_id
        return "2026-09-27T00:00:00+00:00", {
            key: checkpoint.metadata.get(key) for key in keys
        }

    monkeypatch.setattr(_chronos, "read_checkpoint_fields", _read)


def test_fails_loud_when_counts_absent(monkeypatch) -> None:
    # Checkpoint exists but carries no node/edge counts (pre-stamp / compacted).
    _patch_store(monkeypatch, _FakeCheckpoint({"status": "published"}))
    with pytest.raises(GenerationCountsUnavailable):
        generation_counts(object(), _FakeSession())


def test_fails_loud_when_not_published(monkeypatch) -> None:
    _patch_store(
        monkeypatch,
        _FakeCheckpoint({"status": "blocked", "node_count": 5, "edge_count": 3}),
    )
    with pytest.raises(GenerationCountsUnavailable):
        generation_counts(object(), _FakeSession())


def test_serves_counts_when_present(monkeypatch) -> None:
    _patch_store(
        monkeypatch,
        _FakeCheckpoint({"status": "published", "node_count": 42, "edge_count": 7}),
    )
    assert generation_counts(object(), _FakeSession()) == (42, 7)


def test_never_returns_a_default_on_missing(monkeypatch) -> None:
    # Empty metadata MUST raise (loud), not silently yield 0/0 (the P0).
    _patch_store(monkeypatch, _FakeCheckpoint({}))
    with pytest.raises(GenerationCountsUnavailable):
        generation_counts(object(), _FakeSession())
