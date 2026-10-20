"""Issue #2941: the delta counts come from the dirty-scope scan's own rows.

A real two-generation publish whose second generation adds, retracts,
updates, resurrects and identically rewrites nodes and edges. With
OKG_SCOPE_COUNTS_VERIFY=strict the publisher also counts by key, as it did
before, and raises on any difference.
"""
from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")
from okg.substrate.db.connection import connect as db_connect  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

_DEPLOYMENT = "default"


def _node(
    suffix: str, i: int, *, text: object = "v", op: str = "I", subtype: str = "doc",
) -> NodeFact:
    node_id = f"doc:{i}:{suffix}"
    return NodeFact(
        node_id=node_id,
        subtype=subtype,
        attrs={"k": text},
        source_record_id={"id": node_id},
        source_revision={"v": 1},
        op=op,
    )


def _edge(suffix: str, src: int, dst: int, *, w: int = 1, op: str = "I") -> EdgeFact:
    return EdgeFact(
        src=f"doc:{src}:{suffix}",
        dst=f"doc:{dst}:{suffix}",
        edge_type="cites",
        attrs={"w": w},
        source_record_id={"id": f"{src}->{dst}"},
        source_revision={"v": 1},
        op=op,
    )


def _apply(dsn: str, facts: list) -> None:
    with db_connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source="default", strict_endpoints=True,
        )


@pytest.mark.parametrize("run_base_split", [False, True])
def test_scan_counts_equal_the_keyed_count_for_every_change_class(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, run_base_split: bool,
) -> None:
    """With ``run_base_split`` a run-base checkpoint (issue #2993) splits the
    segment between the second generation's writes, as every source run
    does, so the scan reads across segments."""
    monkeypatch.setenv("OKG_SCOPE_COUNTS_VERIFY", "strict")
    suffix = uuid.uuid4().hex[:8]
    with db_connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc", "note"])

    def publisher() -> Publisher:
        return Publisher(
            clean_runtime,
            _DEPLOYMENT,
            catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT),
        )

    _apply(clean_runtime, [
        *(_node(suffix, i) for i in range(8) if i != 6),
        _node(suffix, 6, text=True),
        _edge(suffix, 0, 1), _edge(suffix, 1, 2), _edge(suffix, 2, 3),
        _edge(suffix, 4, 6),
    ])
    # doc:7 is a tombstone at the first generation, so writing it again is
    # an add (a resurrection), not an update.
    _apply(clean_runtime, [_node(suffix, 7, op="R")])
    first = publisher().cycle()
    assert first["status"] == "published", first

    if run_base_split:
        from okg.substrate.ingest.run_base import release_run_base, take_run_base

        run_id = str(uuid.uuid4())
        _apply(clean_runtime, [_node(suffix, 9)])  # written before the split
        with db_connect(clean_runtime, autocommit=True) as conn:
            assert take_run_base(
                ChronosGraphStore.from_connection(conn),
                deployment=_DEPLOYMENT,
                branch="default",
                source="default",
                run_id=run_id,
            )
    _apply(clean_runtime, [
        _node(suffix, 8),                     # node added
        _node(suffix, 5, op="R"),             # node retracted
        _node(suffix, 1, text="changed"),     # node updated
        _node(suffix, 2),                     # node rewritten unchanged
        _node(suffix, 7),                     # node resurrected
        _node(suffix, 3, subtype="note"),     # node updated: type only
        _node(suffix, 6, text=1),             # node updated: true -> 1 (jsonb)
        _edge(suffix, 3, 4),                  # edge added
        _edge(suffix, 2, 3, op="R"),          # edge retracted
        _edge(suffix, 0, 1, w=2),             # edge updated
        _edge(suffix, 1, 2),                  # edge rewritten unchanged
    ])
    # Deleted and written again in the same generation: unchanged overall.
    _apply(clean_runtime, [_edge(suffix, 4, 6, op="R")])
    _apply(clean_runtime, [_edge(suffix, 4, 6)])
    if run_base_split:
        release_run_base(clean_runtime, branch="default", run_id=run_id)
        previous_ref = str(first["generation_id"])
        with db_connect(clean_runtime, autocommit=True) as conn:
            (base_segment,) = conn.execute(
                "SELECT segment_id FROM _chronos_branch_interval_checkpoints"
                " WHERE checkpoint_id = %s",
                (previous_ref,),
            ).fetchone()
            segments = Publisher._chronos_segments_written_since(
                conn, branch_name="default", base_segment_id=str(base_segment),
            )
        assert len(segments) > 1, segments  # the scan reads across a split
    second = publisher().cycle()
    assert second["status"] == "published", second
    timings = second["timings_ms"]
    assert timings.get("chronos_delta_counts.from_scope_scan") == 1.0, timings
    assert "chronos_delta_counts.from_scope_scan_mismatch" not in timings
    # The keyed count the scan count replaced did run, as the check.
    assert "chronos_delta_counts.scoped" in timings
    # And the counts are the right ones, not merely equal to each other.
    assert (
        second["nodes_added"], second["nodes_retracted"], second["nodes_updated"],
        second["edges_added"], second["edges_retracted"], second["edges_updated"],
    ) == (3 if run_base_split else 2, 1, 3, 1, 1, 1), second
