"""Cascade step 4: narrowing/dangling enforcement under chronos_native.

The legacy narrowing enforcement was a trigger on okg.edges_live INSERT (the
projector path), deleted in cascade step 1. The replacement is the Phase B
TransformConsistencyGate that lowers the no_dangling + narrowing programs over
the Chronos graph (okg.graph_nodes/graph_edges). deployment_stack now always
includes it, so this test pins the guarantee through the real publish path: a
narrowing-violating edge BLOCKS the publish; a compliant graph publishes.
"""

from __future__ import annotations

import time
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.consistency import (  # noqa: E402
    TransformConsistencyGate,
    summarize_consistency_gate_health,
)
from okg.substrate.graph_store import ChronosGraphStore, _edge_id  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.substrate.publisher.publisher import _DerivedFactBatch  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _publisher(dsn: str) -> Publisher:
    return Publisher(dsn, "default", consistency_gates=[TransformConsistencyGate()], catalog_identity=bind_test_catalog_writer(dsn, "default"))


def _publisher_prune(dsn: str) -> Publisher:
    return Publisher(
        dsn, "default",
        consistency_gates=[TransformConsistencyGate(dangling_policy="prune")],
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    )


def test_chronos_gate_blocks_narrowing_violation(clean_runtime: str) -> None:
    sfx = uuid.uuid4().hex[:8]
    a, b = f"t:{sfx}:a", f"t:{sfx}:b"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        # Edge whose triple is (ticket, reported_by, ticket) -- NOT in the catalog
        # (only ticket--reported_by-->person is allowed) -> narrowing violation.
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(a, "ticket", {}, {"id": a}, {"v": 1}),
                NodeFact(b, "ticket", {}, {"id": b}, {"v": 1}),
                EdgeFact(
                    a,
                    b,
                    "reported_by",
                    source_record_id={"id": "e"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "blocked", out


def test_chronos_gate_passes_clean(clean_runtime: str) -> None:
    sfx = uuid.uuid4().hex[:8]
    t, p = f"t:{sfx}", f"p:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(t, "ticket", {}, {"id": t}, {"v": 1}),
                NodeFact(p, "person", {}, {"id": p}, {"v": 1}),
                EdgeFact(
                    t,
                    p,
                    "reported_by",
                    source_record_id={"id": "e"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "published", out


def test_chronos_gate_auto_retracts_orphaned_derived_edge_and_publishes(
    clean_runtime: str,
) -> None:
    """Published-generation proof of orphan-retraction. A re-ingest retracts a node
    that a DERIVED edge points at; the edge dangles CROSS-SOURCE (the source-scoped
    incident-edge cleanup misses it -- the edge's source differs from the retracted
    node's). The no-dangling gate, in strict BLOCK mode, AUTO-RETRACTS the orphaned
    DERIVED edge and PUBLISHES the new generation instead of blocking forever; the
    surviving document persists live. This is the cern-twiki claim-landing unblock."""
    sfx = uuid.uuid4().hex[:8]
    keep, gone = f"keep:{sfx}", f"gone:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["document"],
            narrowings=[("document", "related", "document")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(keep, "document", {}, {"id": keep}, {"v": 1}),
                NodeFact(gone, "document", {}, {"id": gone}, {"v": 1}),
            ],
            source="ingest",
            strict_endpoints=True,
        )
        # Enricher-DERIVED edge INTO the soon-retracted doc, a DIFFERENT source than
        # the documents'. The no-dangling check covers both endpoints; this fixture
        # exercises its missing-destination half, matching the cern-twiki
        # org-candidate / mentions shape.
        store.apply_facts(
            [
                EdgeFact(
                    keep, gone, "related",
                    provenance="derived_deterministic",
                    source_record_id={"id": "r"}, source_revision={"v": 1},
                ),
            ],
            source="_publisher_derived",
            strict_endpoints=True,
        )
    assert _publisher(clean_runtime).cycle()["status"] == "published"

    # Re-ingest retracts the dst document (source-scoped); the cross-source derived
    # `related` edge survives and now dangles (its dst is gone).
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(gone, "document", {}, {"id": gone}, {"v": 2}, op="R")],
            source="ingest",
            strict_endpoints=True,
        )

    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "published", out  # auto-retracted, NOT blocked
    gate = out["consistency_gates"][0]
    assert (gate.get("details") or {}).get(
        "orphan_retracted_derived_edge_ids"
    ), gate

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        assert store.query(
            f"SELECT edge_id FROM okg.graph_edges WHERE dst = '{gone}'"
        ) == []
        assert {
            r["node_id"] for r in store.query("SELECT node_id FROM okg.graph_nodes")
        } == {keep}


def test_chunk_retraction_cannot_publish_dangling_enricher_citation(
    clean_runtime: str,
) -> None:
    """A missing citation source is repaired and every count reads post-gate."""
    sfx = uuid.uuid4().hex[:8]
    dead_chunk = f"chunk:{sfx}:dead"
    live_chunk = f"chunk:{sfx}:live"
    dead_claim = f"claim:{sfx}:dead"
    live_claim = f"claim:{sfx}:live"
    enricher = "_enricher:issue1151_fixture"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["chunk", "claim"],
            narrowings=[("chunk", "references", "claim")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(dead_chunk, "chunk", {}, {"id": dead_chunk}, {"v": 1}),
                NodeFact(live_chunk, "chunk", {}, {"id": live_chunk}, {"v": 1}),
                NodeFact(dead_claim, "claim", {}, {"id": dead_claim}, {"v": 1}),
                NodeFact(live_claim, "claim", {}, {"id": live_claim}, {"v": 1}),
            ],
            source="fixture",
        )
        store.apply_facts(
            [
                EdgeFact(
                    dead_chunk,
                    dead_claim,
                    "references",
                    provenance="explicit",
                    source_record_id={"id": "dead-citation"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    live_chunk,
                    live_claim,
                    "references",
                    provenance="explicit",
                    source_record_id={"id": "live-citation"},
                    source_revision={"v": 1},
                ),
            ],
            source=enricher,
        )

    first = _publisher(clean_runtime).cycle()
    assert first["status"] == "published", first
    assert first["consistency_gates"][0]["details"]["scoped"] is False

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(dead_chunk, "chunk", {}, {"id": dead_chunk}, {"v": 2}, op="R")],
            source="fixture",
        )

    second = _publisher(clean_runtime).cycle()
    assert second["status"] == "published", second
    gate = second["consistency_gates"][0]
    details = gate["details"]
    assert details["scope_mode"] == "dirty_edges+incident_dirty_nodes"
    assert details["published_induction_basis"]["verified"] is True
    assert details["retraction_marker_validation"]["verified"] is True
    assert details["orphan_retracted_derived_edge_count"] == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        generation = store.get_generation(str(second["generation_id"]))
        metadata = dict(generation.metadata)
        receipt = dict(metadata["projection_deltas"])
        checkout = store.checkout_generation(str(second["generation_id"]))
        dangling = checkout.query(
            """
            SELECT e.edge_id
              FROM okg.graph_edges e
              LEFT JOIN okg.graph_nodes src ON src.node_id = e.src
              LEFT JOIN okg.graph_nodes dst ON dst.node_id = e.dst
             WHERE src.node_id IS NULL OR dst.node_id IS NULL
            """
        )
        rollups = {
            (str(row["dimension_kind"]), str(row["dimension_value"])): int(
                row["item_count"]
            )
            for row in checkout.query(
                """
                SELECT dimension_kind, dimension_value, item_count
                  FROM okg.console_count_rollup
                 WHERE dimension_kind IN (
                       'edge_type', 'edge_source', 'enricher_edge'
                 )
                """
            )
        }

    assert dangling == []
    assert second["edge_count"] == metadata["edge_count"] == 1
    assert receipt["consistency_gate_mutations"]["edges_removed"] == 1
    assert (
        receipt["consistency_gate_mutations"]["orphaned_derived_edges_retracted"] == 1
    )
    assert rollups[("edge_type", "references")] == 1
    assert rollups[("edge_source", enricher)] == 1
    assert rollups[("enricher_edge", enricher)] == 1


def test_prune_policy_tombstones_dangling_edge_inherited_from_published_generation(
    clean_runtime: str,
) -> None:
    """A later-cycle explicit orphan is pruned without deleting prior history.

    The interval fast path may physically delete a never-published edge from the
    current mutable segment. An edge inherited from a published checkpoint needs
    a new tombstone instead; filtering the physical delete to the current segment
    and doing nothing else left it visible and blocked every subsequent publish.
    """
    sfx = uuid.uuid4().hex[:8]
    keep = f"keep:{sfx}"
    gone = [f"gone:{sfx}:1", f"gone:{sfx}:2"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["document"],
            narrowings=[("document", "related", "document")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(keep, "document", {}, {"id": keep}, {"v": 1}),
                *(
                    NodeFact(node_id, "document", {}, {"id": node_id}, {"v": 1})
                    for node_id in gone
                ),
            ],
            source="nodes",
            strict_endpoints=True,
        )
        store.apply_facts(
            [
                *(
                    EdgeFact(
                        keep,
                        node_id,
                        "related",
                        source_record_id={"id": f"explicit:{node_id}"},
                        source_revision={"v": 1},
                    )
                    for node_id in gone
                )
            ],
            source="external-edges",
            strict_endpoints=True,
        )
    first = _publisher_prune(clean_runtime).cycle()
    assert first["status"] == "published", first

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(node_id, "document", {}, {"id": node_id}, {"v": 2}, op="R")
                for node_id in gone
            ],
            source="nodes",
            strict_endpoints=True,
        )

    second = _publisher_prune(clean_runtime).cycle()
    assert second["status"] == "published", second
    details = second["consistency_gates"][0]["details"]
    assert details["pruned_dangling_edge_count"] == 2

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        assert store.query(
            "SELECT edge_id FROM okg.graph_edges WHERE dst = ANY(:dst)",
            {"dst": gone},
        ) == []
        # The pre-retraction published checkpoint remains byte-visible.
        assert store.checkout_generation(first["generation_id"]).query(
            "SELECT edge_id FROM okg.graph_edges WHERE dst = ANY(:dst)",
            {"dst": gone},
        )


def test_chronos_gate_blocks_node_with_unknown_subtype(clean_runtime: str) -> None:
    # worktree-fork-merge-back / publish-gate-and-node-subtype: a node whose subtype
    # is not in the active catalog must BLOCK at the publish gate. Nodes are not
    # narrowing-gated (only edges are), so without the node-subtype antijoin a
    # retired/unknown-subtype node would publish.
    sfx = uuid.uuid4().hex[:8]
    n = f"n:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["ticket"], narrowings=[])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(n, "ghost_subtype", {}, {"id": n}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "blocked", out
    gate = out["consistency_gates"][0]
    assert n in (gate.get("details") or {}).get(
        "node_subtype_violation_node_ids", []
    ), gate


def test_chronos_gate_passes_known_node_subtype(clean_runtime: str) -> None:
    # Control: a node whose subtype IS in the catalog passes the node-subtype check.
    sfx = uuid.uuid4().hex[:8]
    n = f"n:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["ticket"], narrowings=[])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(n, "ticket", {}, {"id": n}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "published", out


def test_chronos_gate_scopes_additive_delta_after_clean_publish(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    t, p = f"t:{sfx}", f"p:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )

    assert _publisher(clean_runtime).cycle()["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(t, "ticket", {}, {"id": t}, {"v": 1}),
                NodeFact(p, "person", {}, {"id": p}, {"v": 1}),
                EdgeFact(
                    t,
                    p,
                    "reported_by",
                    source_record_id={"id": "e"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )

    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "published", out
    gate = out["consistency_gates"][0]
    # The basis names the cardinality family as SKIPPED: this catalog carries
    # no at-most-one-target annotation, so that family never activated. An
    # inactive family is disclosed by name instead of being invisible.
    assert gate["coverage_basis"] == (
        "chronos_dirty_edges+incident_dirty_nodes"
        "+skipped_families:cardinality"
    )
    assert gate["details"]["runtime"] == "chronos_scoped_sql"
    assert gate["details"]["scope_mode"] == "dirty_edges+incident_dirty_nodes"
    assert gate["preflight"]["scoped_edge_count"] == 1


def test_first_v3_run_full_sweeps_destination_only_v2_basis(
    clean_runtime: str,
) -> None:
    """A published v2 pass cannot seed v3; inherited missing-src debt is cleaned."""
    sfx = uuid.uuid4().hex[:8]
    ghost = f"chunk:{sfx}:missing"
    claim = f"claim:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(
            conn,
            subtypes=["chunk", "claim"],
            narrowings=[("chunk", "references", "claim")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(claim, "claim", {}, {"id": claim}, {"v": 1})],
            source="source:legacy_fixture",
        )
        store.apply_facts(
            [
                EdgeFact(
                    ghost,
                    claim,
                    "references",
                    provenance="explicit",
                    source_record_id={"id": "legacy"},
                    source_revision={"v": 1},
                ),
            ],
            source="_enricher:legacy_fixture",
            strict_endpoints=False,
        )
        store.checkpoint_generation(
            deployment_name="default",
            catalog_version_id=cv,
            status="published",
            metadata={
                "invariant_summary": {
                    "consistency_gates": {
                        "results": [
                            {
                                "check_id": (
                                    "transform.consistency.no_dangling+narrowing+"
                                    "cardinality.v2"
                                ),
                                "gate_name": "transform_consistency_gate",
                                "result": "evaluated",
                                "blocks_publish": False,
                                "coverage_complete": True,
                                "details": {"scoped": False},
                            }
                        ],
                    },
                },
            },
        )
        evaluation = TransformConsistencyGate().evaluate_incremental(
            conn,
            target_generation_id=0,
            catalog_version_id=cv,
            deployment_name="default",
            previous_catalog_version_id=cv,
            delta_counts={
                "nodes_added": 0,
                "nodes_retracted": 0,
                "nodes_updated": 0,
                "edges_added": 0,
                "edges_retracted": 0,
                "edges_updated": 0,
            },
            dirty_scope=({}, {}, True, {}),
        )
        assert store.query("SELECT edge_id FROM okg.graph_edges") == []

    assert evaluation.blocks_publish is False
    assert evaluation.details["scope_mode"] == "full"
    assert evaluation.details["scope_fallback_reason"] == "no_full_induction_basis"
    assert evaluation.details["orphan_retracted_derived_edge_count"] == 1


@pytest.mark.parametrize(
    ("coverage_complete", "blocks_publish", "details"),
    [
        (True, True, {"scoped": False}),
        (
            False,
            False,
            {"scoped": True, "verdict_universe_valid_by_induction": False},
        ),
    ],
)
def test_blocking_or_unverified_v3_checkpoint_cannot_seed_scoped_run(
    clean_runtime: str,
    coverage_complete: bool,
    blocks_publish: bool,
    details: dict[str, object],
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["ticket"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.checkpoint_generation(
            deployment_name="default",
            catalog_version_id=cv,
            status="published",
            metadata={
                "invariant_summary": {
                    "consistency_gates": {
                        "results": [
                            {
                                "check_id": (
                                    "transform.consistency.no_dangling+narrowing+"
                                    "cardinality.v3"
                                ),
                                "gate_name": "transform_consistency_gate",
                                "result": "evaluated",
                                "blocks_publish": blocks_publish,
                                "coverage_complete": coverage_complete,
                                "details": details,
                            }
                        ],
                    },
                },
            },
        )
        evaluation = TransformConsistencyGate().evaluate_incremental(
            conn,
            target_generation_id=0,
            catalog_version_id=cv,
            deployment_name="default",
            previous_catalog_version_id=cv,
            delta_counts={
                "nodes_added": 0,
                "nodes_retracted": 0,
                "nodes_updated": 0,
                "edges_added": 0,
                "edges_retracted": 0,
                "edges_updated": 0,
            },
            dirty_scope=({}, {}, True, {}),
        )

    assert evaluation.details["scope_mode"] == "full"
    assert evaluation.details["scope_fallback_reason"] == "no_full_induction_basis"


def test_equal_count_wrong_deletion_marker_forces_full_sweep(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    deleted = f"chunk:{sfx}:deleted"
    irrelevant = f"chunk:{sfx}:irrelevant"
    claim = f"claim:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(
            conn,
            subtypes=["chunk", "claim"],
            narrowings=[("chunk", "references", "claim")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(deleted, "chunk", {}, {}, {}),
                NodeFact(irrelevant, "chunk", {}, {}, {}),
                NodeFact(claim, "claim", {}, {}, {}),
            ],
            source="fixture",
        )
        store.apply_facts(
            [EdgeFact(deleted, claim, "references", provenance="explicit")],
            source="_enricher:fixture",
        )
    assert _publisher(clean_runtime).cycle()["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(deleted, "chunk", {}, {}, {}, op="R")],
            source="fixture",
        )
        evaluation = TransformConsistencyGate().evaluate_incremental(
            conn,
            target_generation_id=0,
            catalog_version_id=cv,
            deployment_name="default",
            previous_catalog_version_id=cv,
            delta_counts={
                "nodes_added": 0,
                "nodes_retracted": 1,
                "nodes_updated": 0,
                "edges_added": 0,
                "edges_retracted": 0,
                "edges_updated": 0,
            },
            dirty_scope=(
                {"chunk": [irrelevant]},
                {},
                True,
                {"chunk": {irrelevant: ["__deleted__"]}},
            ),
        )

    assert evaluation.blocks_publish is False
    assert evaluation.details["scope_mode"] == "full"
    assert evaluation.details["scope_fallback_reason"] == (
        "dirty_node_retraction_scope_mismatch"
    )
    marker = evaluation.details["retraction_marker_validation"]
    assert marker["expected_retraction_count"] == 1
    assert marker["deleted_marker_count"] == 1
    assert marker["actual_marked_retraction_count"] == 0


def test_zero_retraction_delta_with_deletion_marker_forces_full_sweep(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    spurious = f"chunk:{sfx}:spurious"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["chunk"], narrowings=[])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(spurious, "chunk", {}, {}, {})],
            source="fixture",
        )
    assert _publisher(clean_runtime).cycle()["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        evaluation = TransformConsistencyGate().evaluate_incremental(
            conn,
            target_generation_id=0,
            catalog_version_id=cv,
            deployment_name="default",
            previous_catalog_version_id=cv,
            delta_counts={
                "nodes_added": 0,
                "nodes_retracted": 0,
                "nodes_updated": 0,
                "edges_added": 0,
                "edges_retracted": 0,
                "edges_updated": 0,
            },
            dirty_scope=(
                {},
                {},
                True,
                {"chunk": {spurious: ["__deleted__"]}},
            ),
        )

    assert evaluation.blocks_publish is False
    assert evaluation.details["scope_mode"] == "full"
    assert evaluation.details["scope_fallback_reason"] == (
        "dirty_node_retraction_scope_mismatch"
    )
    marker = evaluation.details["retraction_marker_validation"]
    assert marker == {
        "verified": False,
        "reason": "unexpected_deleted_markers_for_zero_retraction_delta",
        "expected_retraction_count": 0,
        "deleted_marker_count": 1,
        "actual_marked_retraction_count": None,
    }


def test_gate_cache_identity_change_prevents_noop_reuse(
    clean_runtime: str,
) -> None:
    class CountingGate(TransformConsistencyGate):
        def __init__(self, identity: str) -> None:
            super().__init__()
            self.identity = identity
            self.calls = 0

        def cache_identity(self) -> dict[str, str]:
            return {"fixture_identity": self.identity}

        def evaluate(self, *args: object, **kwargs: object):
            self.calls += 1
            return super().evaluate(*args, **kwargs)

        def evaluate_incremental(self, *args: object, **kwargs: object):
            self.calls += 1
            return super().evaluate_incremental(*args, **kwargs)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["ticket"], narrowings=[])

    old_gate = CountingGate("destination-only-v2")
    first = Publisher(clean_runtime, "default", consistency_gates=[old_gate], catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert first["status"] == "published"
    assert old_gate.calls == 1

    new_gate = CountingGate("both-endpoints-v3")
    second = Publisher(clean_runtime, "default", consistency_gates=[new_gate], catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert second["status"] == "published"
    assert new_gate.calls == 1, "changed gate semantics must invalidate reuse"


def test_chronos_gate_scoped_additive_delta_blocks_new_violation(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    a, b = f"t:{sfx}:a", f"t:{sfx}:b"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )

    assert _publisher(clean_runtime).cycle()["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(a, "ticket", {}, {"id": a}, {"v": 1}),
                NodeFact(b, "ticket", {}, {"id": b}, {"v": 1}),
                EdgeFact(
                    a,
                    b,
                    "reported_by",
                    source_record_id={"id": "e"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )

    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "blocked", out
    gate = out["consistency_gates"][0]
    assert gate["coverage_basis"] == (
        "chronos_dirty_edges+incident_dirty_nodes"
        "+skipped_families:cardinality"
    )
    assert _edge_id(a, "reported_by", b) in (
        gate["details"]["narrowing_violation_edge_ids"]
    )


def test_chronos_gate_scoped_dirty_node_subtype_change_checks_incident_edges(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    t, p = f"t:{sfx}", f"p:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(t, "ticket", {}, {"id": t}, {"v": 1}),
                NodeFact(p, "person", {}, {"id": p}, {"v": 1}),
                EdgeFact(
                    t,
                    p,
                    "reported_by",
                    source_record_id={"id": "e"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )

    assert _publisher(clean_runtime).cycle()["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(p, "ticket", {}, {"id": p}, {"v": 2})],
            source="default",
            strict_endpoints=True,
        )

    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "blocked", out
    gate = out["consistency_gates"][0]
    assert gate["coverage_basis"] == (
        "chronos_dirty_edges+incident_dirty_nodes"
        "+skipped_families:cardinality"
    )
    assert gate["preflight"]["dirty_node_count"] == 1
    assert gate["preflight"]["scoped_edge_count"] == 1
    assert _edge_id(t, "reported_by", p) in (
        gate["details"]["narrowing_violation_edge_ids"]
    )


def test_chronos_publish_scopes_delta_counts_to_dirty_keys(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    t, p = f"t:{sfx}", f"p:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )

    pub = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default"))
    assert pub.cycle()["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(t, "ticket", {}, {"id": t}, {"v": 1}),
                NodeFact(p, "person", {}, {"id": p}, {"v": 1}),
                EdgeFact(
                    t,
                    p,
                    "reported_by",
                    source_record_id={"id": "e"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )

    calls = 0

    def wrapped(self: Publisher, *args, **kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("full generation delta count should not run")

    monkeypatch.setattr(Publisher, "_chronos_generation_delta_counts", wrapped)

    out = pub.cycle()
    assert out["status"] == "published", out
    assert calls == 0
    assert out["nodes_added"] == 2
    assert out["nodes_updated"] == 0
    assert out["edges_added"] == 1
    assert "delta_counts_noop_reuse_probe" in out["timings_ms"]
    assert "delta_counts_before_views" in out["timings_ms"]


def test_chronos_publish_uses_source_write_set_dirty_scope_hint(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    t, p = f"t:{sfx}", f"p:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )

    assert Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        write_set = ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(t, "ticket", {}, {"id": t}, {"v": 1}),
                NodeFact(p, "person", {}, {"id": p}, {"v": 1}),
                EdgeFact(
                    t,
                    p,
                    "reported_by",
                    source_record_id={"id": "e"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        ).chronos_dirty_scope()

    def fail_dirty_scope_scan(*_args, **_kwargs):
        raise AssertionError("dirty scope should come from source write-set hint")

    monkeypatch.setattr(
        Publisher,
        "_query_dirty_from_chronos_current_segment_by_type",
        staticmethod(fail_dirty_scope_scan),
    )

    out = Publisher(
        clean_runtime,
        "default",
        chronos_dirty_scope_hint=write_set,
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert out["status"] == "published", out
    assert out["nodes_added"] == 2
    assert out["edges_added"] == 1
    assert out["nodes_updated"] == 0
    assert out["edges_updated"] == 0


def test_chronos_dirty_scope_extraction_covers_node_edge_changes(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    changed = f"t:{sfx}:changed"
    new = f"t:{sfx}:new"
    deleted = f"t:{sfx}:deleted"
    edge_deleted = f"t:{sfx}:edge-deleted"
    person = f"p:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    changed,
                    "ticket",
                    {"status": "old"},
                    {"id": changed},
                    {"v": 1},
                ),
                NodeFact(
                    deleted,
                    "ticket",
                    {"status": "gone"},
                    {"id": deleted},
                    {"v": 1},
                ),
                NodeFact(
                    edge_deleted,
                    "ticket",
                    {"status": "edge-gone"},
                    {"id": edge_deleted},
                    {"v": 1},
                ),
                NodeFact(person, "person", {}, {"id": person}, {"v": 1}),
                EdgeFact(
                    changed,
                    person,
                    "reported_by",
                    attrs={"weight": 1},
                    source_record_id={"id": "edge-update"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    deleted,
                    person,
                    "reported_by",
                    attrs={"weight": 1},
                    source_record_id={"id": "incident-delete"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    edge_deleted,
                    person,
                    "reported_by",
                    attrs={"weight": 1},
                    source_record_id={"id": "edge-delete"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )

    assert Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        previous = store.latest_generation(deployment_name="default")
        assert previous is not None
        write_set = store.apply_facts(
            [
                NodeFact(
                    changed,
                    "ticket",
                    {"status": "new"},
                    {"id": changed},
                    {"v": 2},
                ),
                NodeFact(new, "ticket", {"status": "new"}, {"id": new}, {"v": 1}),
                NodeFact(
                    deleted,
                    "ticket",
                    {},
                    {"id": deleted},
                    {"v": 2},
                    op="R",
                ),
                EdgeFact(
                    changed,
                    person,
                    "reported_by",
                    attrs={"weight": 2},
                    source_record_id={"id": "edge-update"},
                    source_revision={"v": 2},
                ),
                EdgeFact(
                    new,
                    person,
                    "reported_by",
                    attrs={"weight": 1},
                    source_record_id={"id": "edge-add"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    edge_deleted,
                    person,
                    "reported_by",
                    source_record_id={"id": "edge-delete"},
                    source_revision={"v": 2},
                    op="R",
                ),
            ],
            source="default",
            strict_endpoints=True,
        ).chronos_dirty_scope()

        segment_scope = Publisher._query_dirty_from_chronos_current_segment_by_type(
            conn,
            branch_name="default",
            previous=previous,
        )

    assert segment_scope is not None
    # perf-reimpl grew _ChronosDirtyScope from 4 to 6 fields, appending
    # edge_endpoints_by_type and edge_attr_changes_by_type. The segment-delta
    # extraction leaves both empty (it tracks edges at edge-id, not attr, level),
    # so this test asserts coverage on the first four fields only.
    nodes_by_subtype, edges_by_type, edge_table_available, attr_changes, *_ = segment_scope
    hinted_nodes, hinted_edges, hinted_edge_available, hinted_attr_changes, *_ = write_set

    assert edge_table_available is True
    assert hinted_edge_available is True
    for subtype, node_ids in hinted_nodes.items():
        assert set(node_ids) <= set(nodes_by_subtype[subtype])
    for edge_type, edge_ids in hinted_edges.items():
        assert set(edge_ids) <= set(edges_by_type[edge_type])
    for subtype, node_changes in hinted_attr_changes.items():
        for node_id, changed_fields in node_changes.items():
            assert set(changed_fields) <= set(attr_changes[subtype][node_id])

    assert set(nodes_by_subtype["ticket"]) >= {changed, new, deleted}
    assert set(edges_by_type["reported_by"]) >= {
        _edge_id(changed, "reported_by", person),
        _edge_id(new, "reported_by", person),
        _edge_id(deleted, "reported_by", person),
        _edge_id(edge_deleted, "reported_by", person),
    }
    assert set(attr_changes["ticket"][changed]) == {"status", LINEAGE_ATTR}
    assert "__new__" in attr_changes["ticket"][new]
    assert "__deleted__" in attr_changes["ticket"][deleted]


def test_chronos_gate_does_not_expand_incident_edges_for_additive_nodes(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    t, p = f"t:{sfx}", f"p:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            )
            cv = int(cur.fetchone()[0])

    assert _publisher(clean_runtime).cycle()["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        write_set = ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(t, "ticket", {}, {"id": t}, {"v": 1}),
                NodeFact(p, "person", {}, {"id": p}, {"v": 1}),
                EdgeFact(
                    t,
                    p,
                    "reported_by",
                    source_record_id={"id": "e"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        ).chronos_dirty_scope()
        gate = TransformConsistencyGate()
        evaluation = gate.evaluate_incremental(
            conn,
            target_generation_id=0,
            catalog_version_id=cv,
            deployment_name="default",
            previous_catalog_version_id=cv,
            delta_counts={
                "nodes_added": 2,
                "nodes_retracted": 0,
                "nodes_updated": 0,
                "edges_added": 1,
                "edges_retracted": 0,
                "edges_updated": 0,
            },
            dirty_scope=write_set,
        )

    assert evaluation.result == "evaluated"
    assert evaluation.preflight["dirty_node_count"] == 2
    assert evaluation.preflight["topology_dirty_node_count"] == 0
    assert evaluation.preflight["dirty_edge_count"] == 1
    assert evaluation.preflight["scoped_edge_count"] == 1


def test_chronos_gate_reuses_proof_across_equivalent_catalog_rows(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    t, p = f"t:{sfx}", f"p:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )

    # bind_test_catalog_writer clones the latest unbound catalog (seeded
    # above) into a deployment-bound row carrying the explicit test writer's
    # identity (PR #1334). That clone is the catalog the first publish
    # records, so it is the "previous" catalog the gate compares against.
    publisher = _publisher(clean_runtime)
    identity = (
        publisher.catalog_identity.catalog_fingerprint,
        publisher.catalog_identity.schema_fingerprint,
        publisher.catalog_identity.writer_fingerprint,
    )
    first = publisher.cycle()
    assert first["status"] == "published", first
    previous_cv = int(first["catalog_version_id"])

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        write_set = ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(t, "ticket", {}, {"id": t}, {"v": 1}),
                NodeFact(p, "person", {}, {"id": p}, {"v": 1}),
                EdgeFact(
                    t,
                    p,
                    "reported_by",
                    source_record_id={"id": "e"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        ).chronos_dirty_scope()
        current_cv = seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.catalog_versions
                   SET deployment_name = 'default',
                       catalog_fingerprint = %s,
                       schema_fingerprint = %s,
                       writer_fingerprint = %s
                 WHERE catalog_version_id = %s
                """,
                (*identity, current_cv),
            )

        evaluation = TransformConsistencyGate().evaluate_incremental(
            conn,
            target_generation_id=0,
            catalog_version_id=current_cv,
            deployment_name="default",
            previous_catalog_version_id=previous_cv,
            delta_counts={
                "nodes_added": 2,
                "nodes_retracted": 0,
                "nodes_updated": 0,
                "edges_added": 1,
                "edges_retracted": 0,
                "edges_updated": 0,
            },
            dirty_scope=write_set,
        )

    assert evaluation.result == "evaluated"
    assert evaluation.blocks_publish is False
    assert (
        evaluation.details["scope_mode"]
        == "dirty_edges+incident_dirty_nodes"
    )
    assert evaluation.details["catalog_equivalence"] == {
        "equivalent": True,
        "basis": "matching_applied_content_identity",
        "previous_catalog_version_id": previous_cv,
        "catalog_version_id": current_cv,
        "compared_fields": [
            "catalog_fingerprint",
            "schema_fingerprint",
            "writer_fingerprint",
        ],
    }


def test_chronos_gate_incremental_fallback_reasons_run_full_gate(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    a, b = f"t:{sfx}:a", f"t:{sfx}:b"
    edge_id = _edge_id(a, "reported_by", b)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            )
            cv = int(cur.fetchone()[0])

    assert _publisher(clean_runtime).cycle()["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(a, "ticket", {}, {"id": a}, {"v": 1}),
                NodeFact(b, "ticket", {}, {"id": b}, {"v": 1}),
                EdgeFact(
                    a,
                    b,
                    "reported_by",
                    source_record_id={"id": "e"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
        gate = TransformConsistencyGate()
        delta_counts = {
            "nodes_added": 2,
            "nodes_retracted": 0,
            "nodes_updated": 0,
            "edges_added": 1,
            "edges_retracted": 0,
            "edges_updated": 0,
        }
        valid_dirty_scope = (
            {"ticket": [a, b]},
            {"reported_by": [edge_id]},
            True,
            None,
        )
        cases = {
            "no_previous_generation": {
                "previous_catalog_version_id": None,
                "delta_counts": delta_counts,
                "dirty_scope": valid_dirty_scope,
            },
            "catalog_version_changed": {
                "previous_catalog_version_id": cv + 1,
                "delta_counts": delta_counts,
                "dirty_scope": valid_dirty_scope,
            },
            "delta_counts_unavailable": {
                "previous_catalog_version_id": cv,
                "delta_counts": None,
                "dirty_scope": valid_dirty_scope,
            },
            "dirty_scope_unavailable": {
                "previous_catalog_version_id": cv,
                "delta_counts": delta_counts,
                "dirty_scope": None,
            },
            "dirty_edge_scope_unavailable": {
                "previous_catalog_version_id": cv,
                "delta_counts": delta_counts,
                "dirty_scope": valid_dirty_scope[:2] + (False, None),
            },
            "dirty_node_scope_mismatch": {
                "previous_catalog_version_id": cv,
                "delta_counts": delta_counts,
                "dirty_scope": ({}, {"reported_by": [edge_id]}, True, {}),
            },
            "dirty_edge_scope_mismatch": {
                "previous_catalog_version_id": cv,
                "delta_counts": delta_counts,
                "dirty_scope": ({"ticket": [a, b]}, {}, True, {}),
            },
        }

        for expected_reason, kwargs in cases.items():
            evaluation = gate.evaluate_incremental(
                conn,
                target_generation_id=0,
                catalog_version_id=cv,
                deployment_name="default",
                **kwargs,
            )

            assert evaluation.result == "evaluated", expected_reason
            assert evaluation.blocks_publish is True, expected_reason
            assert evaluation.details["scope_mode"] == "full", expected_reason
            assert (
                evaluation.details["scope_fallback_reason"] == expected_reason
            )
            # Same disclosure contract as the assertions above: the seeded
            # catalog has no at_most_one_target annotation, so the skipped
            # cardinality family is named instead of a check that never ran.
            assert evaluation.details["runtime"] == (
                "chronos_transform_ir+skipped_families:cardinality"
            )
            assert edge_id in evaluation.details["narrowing_violation_edge_ids"]


def test_chronos_dirty_scope_unavailable_forces_full_work(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    a, b = f"t:{sfx}:a", f"t:{sfx}:b"
    edge_id = _edge_id(a, "reported_by", b)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            )
            cv = int(cur.fetchone()[0])

    assert _publisher(clean_runtime).cycle()["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(a, "ticket", {}, {"id": a}, {"v": 1}),
                NodeFact(b, "ticket", {}, {"id": b}, {"v": 1}),
                EdgeFact(
                    a,
                    b,
                    "reported_by",
                    source_record_id={"id": "e"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
        evaluation = TransformConsistencyGate().evaluate_incremental(
            conn,
            target_generation_id=0,
            catalog_version_id=cv,
            deployment_name="default",
            previous_catalog_version_id=cv,
            delta_counts={
                "nodes_added": 2,
                "nodes_retracted": 0,
                "nodes_updated": 0,
                "edges_added": 1,
                "edges_retracted": 0,
                "edges_updated": 0,
            },
            dirty_scope=None,
        )

    assert evaluation.result == "evaluated"
    assert evaluation.blocks_publish is True
    assert evaluation.details["scope_mode"] == "full"
    assert evaluation.details["scope_fallback_reason"] == "dirty_scope_unavailable"
    assert edge_id in evaluation.details["narrowing_violation_edge_ids"]


def test_chronos_delta_counts_can_scope_to_touched_keys(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    existing = f"t:{sfx}:existing"
    new = f"t:{sfx}:new"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["ticket"], narrowings=[])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(existing, "ticket", {"version": 1}, {"id": existing}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )

    pub = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default"))
    assert pub.cycle()["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        previous = store.latest_generation(deployment_name="default")
        assert previous is not None
        store.apply_facts(
            [
                NodeFact(
                    existing,
                    "ticket",
                    {"version": 2},
                    {"id": existing},
                    {"v": 2},
                ),
                NodeFact(new, "ticket", {"version": 1}, {"id": new}, {"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )

        full = pub._chronos_generation_delta_counts(
            conn,
            store=store,
            branch_name="default",
            previous=previous,
        )
        only_new = Publisher._chronos_current_segment_delta_counts_for_keys(
            conn,
            branch_name="default",
            previous=previous,
            node_ids=[new],
            edge_ids=[],
        )
        only_existing = Publisher._chronos_current_segment_delta_counts_for_keys(
            conn,
            branch_name="default",
            previous=previous,
            node_ids=[existing],
            edge_ids=[],
        )
        unrelated = Publisher._chronos_current_segment_delta_counts_for_keys(
            conn,
            branch_name="default",
            previous=previous,
            node_ids=[f"t:{sfx}:missing"],
            edge_ids=[],
        )

    assert full["nodes_added"] == 1
    assert full["nodes_updated"] == 1
    assert only_new == {
        "nodes_added": 1,
        "nodes_retracted": 0,
        "nodes_updated": 0,
        "edges_added": 0,
        "edges_retracted": 0,
        "edges_updated": 0,
    }
    assert only_existing == {
        "nodes_added": 0,
        "nodes_retracted": 0,
        "nodes_updated": 1,
        "edges_added": 0,
        "edges_retracted": 0,
        "edges_updated": 0,
    }
    assert unrelated == {
        "nodes_added": 0,
        "nodes_retracted": 0,
        "nodes_updated": 0,
        "edges_added": 0,
        "edges_retracted": 0,
        "edges_updated": 0,
    }


def test_chronos_publish_scopes_delta_counts_after_derived_fact_apply(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    t, p = f"t:{sfx}", f"p:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[
                ("ticket", "reported_by", "person"),
                ("ticket", "mentions", "person"),
            ],
        )

    assert _publisher(clean_runtime).cycle()["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(t, "ticket", {}, {"id": t}, {"v": 1}),
                NodeFact(p, "person", {}, {"id": p}, {"v": 1}),
                EdgeFact(
                    t,
                    p,
                    "reported_by",
                    source_record_id={"id": "reported_by"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )

    derived = EdgeFact(
        t,
        p,
        "mentions",
        source_record_id={"id": "derived-mentions"},
        source_revision={"v": 1},
    )
    load_calls = 0

    def fake_load_unapplied_derived_facts(*_args, **kwargs):
        # The publisher loads derived facts grouped by producer and applies
        # one FactBatch per producer run (PR #788), so the fake returns the
        # same shape the real loader returns under group_by_producer=True.
        assert kwargs.get("group_by_producer") is True
        nonlocal load_calls
        load_calls += 1
        if kwargs.get("generation_id") is None and load_calls == 1:
            producer_id = "test-derived-mentions"
            return [
                _DerivedFactBatch(
                    producer_id=producer_id,
                    producer_kind="enricher",
                    run_id=uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"okg:legacy-derived:{producer_id}",
                    ),
                    facts=(derived,),
                )
            ]
        return []

    def fail_full_delta_count(*_args, **_kwargs):
        raise AssertionError("full generation delta count should not run")

    monkeypatch.setattr(
        Publisher,
        "_load_unapplied_derived_facts",
        staticmethod(fake_load_unapplied_derived_facts),
    )
    monkeypatch.setattr(
        Publisher,
        "_chronos_generation_delta_counts",
        fail_full_delta_count,
    )

    out = _publisher(clean_runtime).cycle()

    assert out["status"] == "published", out
    assert out["nodes_added"] == 2
    assert out["edges_added"] == 2
    assert "delta_counts_before_views" in out["timings_ms"]


def test_chronos_gate_blocks_dangling_when_policy_block(clean_runtime: str) -> None:
    """Default policy: a dangling edge (dst node absent) BLOCKS the publish."""
    sfx = uuid.uuid4().hex[:8]
    t, p, ghost = f"t:{sfx}", f"p:{sfx}", f"ghost:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["ticket", "person"],
                     narrowings=[("ticket", "reported_by", "person")])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(t, "ticket", {}, {"id": t}, {"v": 1}),
                NodeFact(p, "person", {}, {"id": p}, {"v": 1}),
                EdgeFact(t, p, "reported_by",
                         source_record_id={"id": "ok"}, source_revision={"v": 1}),
                # dangling: `ghost` was never created (strict_endpoints=False lets it write)
                EdgeFact(t, ghost, "reported_by",
                         source_record_id={"id": "dangle"}, source_revision={"v": 1}),
            ],
            source="default", strict_endpoints=False,
        )
    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "blocked", out


def test_chronos_gate_prunes_dangling_when_policy_prune(clean_runtime: str) -> None:
    """`prune` policy: the dangling edge is dropped at publish (not blocked); the
    valid edge survives and the generation publishes."""
    sfx = uuid.uuid4().hex[:8]
    t, p, ghost = f"t:{sfx}", f"p:{sfx}", f"ghost:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["ticket", "person"],
                     narrowings=[("ticket", "reported_by", "person")])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(t, "ticket", {}, {"id": t}, {"v": 1}),
                NodeFact(p, "person", {}, {"id": p}, {"v": 1}),
                EdgeFact(t, p, "reported_by",
                         source_record_id={"id": "ok"}, source_revision={"v": 1}),
                EdgeFact(t, ghost, "reported_by",
                         source_record_id={"id": "dangle"}, source_revision={"v": 1}),
            ],
            source="default", strict_endpoints=False,
        )
    out = _publisher_prune(clean_runtime).cycle()
    assert out["status"] == "published", out
    # the dangling edge was pruned; the valid edge remains
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        edges = ChronosGraphStore.from_connection(conn, ensure_schema=False).query(
            "SELECT src, dst FROM okg.graph_edges"
        )
    pairs = {(e["src"], e["dst"]) for e in edges}
    assert (t, p) in pairs           # valid edge survived
    assert (t, ghost) not in pairs   # dangling edge pruned


def test_prune_receipt_reconciles_checkpoint_visible_net_edges(
    clean_runtime: str,
) -> None:
    """The checkpoint receipt distinguishes projected writes from post-gate net."""
    sfx = uuid.uuid4().hex[:8]
    ticket, person, ghost = f"t:{sfx}", f"p:{sfx}", f"ghost:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(ticket, "ticket", {}, {"id": ticket}, {"v": 1}),
                NodeFact(person, "person", {}, {"id": person}, {"v": 1}),
                EdgeFact(
                    ticket,
                    person,
                    "reported_by",
                    source_record_id={"id": "valid"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    ticket,
                    ghost,
                    "reported_by",
                    source_record_id={"id": "dangling"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=False,
        )

    out = _publisher_prune(clean_runtime).cycle()
    assert out["status"] == "published", out
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        generation = store.get_generation(str(out["generation_id"]))
        receipt = dict(generation.metadata)["projection_deltas"]

    assert receipt["edges_projected_net"] == 2
    assert receipt["consistency_gate_mutations"]["edges_removed"] == 1
    assert receipt["edges_checkpoint_visible_net"] == 1
    assert (
        receipt["edges_projected_net"]
        - receipt["consistency_gate_mutations"]["edges_removed"]
        == receipt["edges_checkpoint_visible_net"]
    )


def test_prune_receipt_attributes_sources_before_deletion(
    clean_runtime: str,
) -> None:
    """Every deleted edge is classified while its producer row still exists."""
    sfx = uuid.uuid4().hex[:8]
    ticket = f"ticket:{sfx}"
    ghosts = [f"ghost:{sfx}:{index}" for index in range(3)]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(ticket, "ticket", {}, {"id": ticket}, {"v": 1})],
            source="source-a",
            strict_endpoints=True,
        )
        for index, source in enumerate(("source-a", "source-b", "")):
            store.apply_facts(
                [
                    EdgeFact(
                        ticket,
                        ghosts[index],
                        "reported_by",
                        source_record_id={"id": f"dangling:{index}"},
                        source_revision={"v": 1},
                    ),
                ],
                source=source,
                strict_endpoints=False,
            )

    out = _publisher_prune(clean_runtime).cycle()
    assert out["status"] == "published", out
    details = out["consistency_gates"][0]["details"]
    attribution = details["pruned_dangling_edge_attribution"]

    assert details["pruned_dangling_edge_count"] == 3
    assert details["pruned_dangling_edge_ids_truncated"] is False
    assert attribution["total"] == 3
    assert attribution["attributed"] == 2
    assert attribution["unattributed"] == 1
    assert attribution["by_source"] == {
        "bucket_count": 3,
        "truncated": False,
        "buckets": [
            {"source": "__unattributed__", "count": 1},
            {"source": "source-a", "count": 1},
            {"source": "source-b", "count": 1},
        ],
    }
    classifications = attribution["by_classification"]["buckets"]
    assert sum(bucket["count"] for bucket in classifications) == 3
    assert {bucket["source"] for bucket in classifications} == {
        "__unattributed__",
        "source-a",
        "source-b",
    }
    assert {bucket["edge_type"] for bucket in classifications} == {"reported_by"}
    assert {bucket["src_subtype"] for bucket in classifications} == {"ticket"}
    assert {bucket["dst_subtype"] for bucket in classifications} == {"__missing__"}
    assert {bucket["missing_endpoint"] for bucket in classifications} == {"dst"}

    bounded = TransformConsistencyGate._prune_attribution([
        {
            "source": f"source-{index}",
            "edge_type": f"edge-{index}",
            "src_subtype": "ticket",
            "dst_subtype": None,
        }
        for index in range(101)
    ])
    assert bounded["by_source"]["truncated"] is True
    assert len(bounded["by_source"]["buckets"]) == 50
    assert sum(
        bucket["count"] for bucket in bounded["by_source"]["buckets"]
    ) == 101
    source_overflow = bounded["by_source"]["buckets"][-1]
    assert source_overflow["source"] == "__attributed_overflow__"
    assert source_overflow["lineage_status"] == "attributed"
    assert source_overflow["omitted_bucket_count"] == 52
    assert len(source_overflow["omitted_key_sample"]) == 5
    assert source_overflow["action"]
    assert bounded["by_classification"]["truncated"] is True
    assert len(bounded["by_classification"]["buckets"]) == 100
    assert sum(
        bucket["count"]
        for bucket in bounded["by_classification"]["buckets"]
    ) == 101
    classification_overflow = bounded["by_classification"]["buckets"][-1]
    assert classification_overflow["source"] == "__attributed_overflow__"
    assert classification_overflow["lineage_status"] == "attributed"


def test_prune_policy_blocks_when_requested_edge_was_not_removed(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed/segment-ineligible prune remains a violation; it never passes."""
    sfx = uuid.uuid4().hex[:8]
    ticket, ghost = f"ticket:{sfx}", f"ghost:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(ticket, "ticket", {}, {"id": ticket}, {"v": 1}),
                EdgeFact(
                    ticket,
                    ghost,
                    "reported_by",
                    source_record_id={"id": "unprunable"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=False,
        )
    gate = TransformConsistencyGate(dangling_policy="prune")
    monkeypatch.setattr(
        gate,
        "_prune_dangling",
        lambda *_args, **_kwargs: ([], gate._empty_prune_attribution()),
    )

    out = Publisher(clean_runtime, "default", consistency_gates=[gate], catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    assert out["status"] == "blocked", out
    details = out["consistency_gates"][0]["details"]
    assert details["pruned_dangling_edge_count"] == 0
    assert len(details["dangling_edge_ids"]) == 1


def test_chronos_gate_prune_still_blocks_narrowing(clean_runtime: str) -> None:
    """`prune` only forgives dangling edges — a narrowing violation still BLOCKS."""
    sfx = uuid.uuid4().hex[:8]
    a, b = f"t:{sfx}:a", f"t:{sfx}:b"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["ticket", "person"],
                     narrowings=[("ticket", "reported_by", "person")])
        # (ticket, reported_by, ticket) is not in the catalog -> narrowing violation
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(a, "ticket", {}, {"id": a}, {"v": 1}),
                NodeFact(b, "ticket", {}, {"id": b}, {"v": 1}),
                EdgeFact(a, b, "reported_by",
                         source_record_id={"id": "e"}, source_revision={"v": 1}),
            ],
            source="default", strict_endpoints=True,
        )
    out = _publisher_prune(clean_runtime).cycle()
    assert out["status"] == "blocked", out


# --- consistency-gate-durability task.transformgate.persist -----------------
#
# The always-on Phase B transform gate now persists its decision through the
# gate algebra: a real derived_artifact_runs row + a consistency_gate_results
# row backed by it (not a fabricated uuid4 discarded after the publish summary),
# so its verdict history, elapsed times, and block reasons are durably queryable
# and feed gate health.

# Pins the gate's stable synthetic check identity (transform_gate._CHECK_ID).
_TRANSFORM_CHECK_ID = (
    "transform.consistency.no_dangling+narrowing+cardinality+"
    "derived_node_liveness.v4"
)


def _transform_gate_rows(conn: "psycopg.Connection") -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.check_id, r.result, r.blocks_publish, r.reason,
                   r.is_probe, r.details, dar.status, dar.trigger_kind,
                   dar.definition_id, (dar.metrics ->> 'persist_ms')::int
              FROM okg.consistency_gate_results r
              JOIN okg.derived_artifact_runs dar ON dar.run_id = r.run_id
             WHERE r.gate_name = 'transform_consistency_gate'
             ORDER BY r.created_at, r.target_generation_id
            """
        )
        return cur.fetchall()


def test_transform_gate_persists_pass_case(clean_runtime: str) -> None:
    """A clean publish lands a durable transform_consistency_gate row backed by a
    real (completed) run, and the row feeds gate health."""
    sfx = uuid.uuid4().hex[:8]
    t, p = f"t:{sfx}", f"p:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn, subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(t, "ticket", {}, {"id": t}, {"v": 1}),
                NodeFact(p, "person", {}, {"id": p}, {"v": 1}),
                EdgeFact(t, p, "reported_by",
                         source_record_id={"id": "e"}, source_revision={"v": 1}),
            ],
            source="default", strict_endpoints=True,
        )
    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "published", out

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = _transform_gate_rows(conn)
        health = summarize_consistency_gate_health(
            conn,
            check_id=_TRANSFORM_CHECK_ID,
            gate_name="transform_consistency_gate",
            lookback=20,
        )

    assert len(rows) == 1, rows
    (check_id, result, blocks_publish, reason, is_probe, details,
     run_status, trigger_kind, definition_id, persist_ms) = rows[0]
    assert check_id == _TRANSFORM_CHECK_ID
    assert definition_id == _TRANSFORM_CHECK_ID  # run backs the same definition
    assert result == "evaluated"
    assert blocks_publish is False
    assert reason == "no_violations"
    assert is_probe is False
    assert run_status == "completed"           # a REAL run, driven to terminal
    assert trigger_kind == "full"              # first publish => full sweep
    assert details["dangling_edge_ids"] == []
    assert isinstance(persist_ms, int) and persist_ms >= 0
    # The durable row feeds gate health (previously the gate had NO history).
    assert health.total_runs == 1


def test_transform_gate_persists_blocked_case(clean_runtime: str) -> None:
    """A dangling edge the gate blocks on ALSO lands a durable row (backed by a
    real run) recording blocks_publish=True and the offending edge -- the row
    survives the blocked cycle (the persistence commits independently)."""
    sfx = uuid.uuid4().hex[:8]
    t, p, ghost = f"t:{sfx}", f"p:{sfx}", f"ghost:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn, subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(t, "ticket", {}, {"id": t}, {"v": 1}),
                NodeFact(p, "person", {}, {"id": p}, {"v": 1}),
                EdgeFact(t, p, "reported_by",
                         source_record_id={"id": "ok"}, source_revision={"v": 1}),
                # dangling: `ghost` never created -> the gate blocks under 'block'
                EdgeFact(t, ghost, "reported_by",
                         source_record_id={"id": "dangle"}, source_revision={"v": 1}),
            ],
            source="default", strict_endpoints=False,
        )
    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "blocked", out

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = _transform_gate_rows(conn)
        health = summarize_consistency_gate_health(
            conn,
            check_id=_TRANSFORM_CHECK_ID,
            gate_name="transform_consistency_gate",
            lookback=20,
        )

    assert len(rows) == 1, rows
    (check_id, result, blocks_publish, reason, is_probe, details,
     run_status, trigger_kind, definition_id, persist_ms) = rows[0]
    assert check_id == _TRANSFORM_CHECK_ID
    # Structural gate: evaluation-HEALTHY (the antijoin ran), block carried by
    # blocks_publish, not by a blocked_* result (task.policy.result-fields).
    assert result == "evaluated"
    assert blocks_publish is True
    assert reason == "transform_consistency_violation"
    assert len(details["dangling_edge_ids"]) == 1   # the ghost edge
    assert run_status == "completed"
    assert isinstance(persist_ms, int) and persist_ms >= 0
    assert health.total_runs == 1


def test_transform_gate_persistence_latency(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Measure the added publish-latency cost of persisting the transform gate:
    full evaluate (compute + persist) vs compute-only, over K iterations."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn, subtypes=["ticket", "person"],
            narrowings=[("ticket", "reported_by", "person")],
        )
        facts: list = []
        for i in range(20):
            ti, pi = f"t{i}", f"p{i}"
            facts += [
                NodeFact(ti, "ticket", {}, {"id": ti}, {"v": 1}),
                NodeFact(pi, "person", {}, {"id": pi}, {"v": 1}),
                EdgeFact(ti, pi, "reported_by",
                         source_record_id={"id": f"e{i}"}, source_revision={"v": 1}),
            ]
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source="default", strict_endpoints=True,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            )
            cv = int(cur.fetchone()[0])

        gate = TransformConsistencyGate()
        kw = dict(catalog_version_id=cv, deployment_name="default")
        k = 20
        # warm caches so the delta reflects steady-state persistence overhead
        gate.evaluate(conn, target_generation_id=1, **kw)

        t0 = time.monotonic()
        for _ in range(k):
            gate._evaluate_full_fields(conn, target_generation_id=1, **kw)
        compute_s = time.monotonic() - t0

        t0 = time.monotonic()
        for _ in range(k):
            gate.evaluate(conn, target_generation_id=1, **kw)
        full_s = time.monotonic() - t0

        rows = _transform_gate_rows(conn)

    compute_ms = compute_s / k * 1000
    full_ms = full_s / k * 1000
    added_ms = full_ms - compute_ms
    with capsys.disabled():
        print(
            "\n[transform-gate persistence bench] "
            f"compute-only={compute_ms:.2f}ms/call  "
            f"full(compute+persist)={full_ms:.2f}ms/call  "
            f"added-persistence={added_ms:.2f}ms/publish  (k={k})"
        )
    # The persistence path always records its own measured cost in run metrics.
    persist_ms_values = [r[9] for r in rows if r[9] is not None]
    assert persist_ms_values, "expected persist_ms recorded in run metrics"
    assert all(v >= 0 for v in persist_ms_values)


_DOC_FIX = (
    "add `document_starter` under `modules:` in deployment.yaml (if already "
    "listed, the applied catalog is stale), then re-apply the catalog: "
    "okg catalog load --deployment DEPLOYMENT_DIR --apply"
)


def _section_facts(sfx: str, sections: int) -> list:
    doc = f"doc:{sfx}"
    facts: list = [NodeFact(doc, "document", {}, {"id": doc}, {"v": 1})]
    for i in range(sections):
        sec = f"sec:{sfx}:{i}"
        facts.append(NodeFact(sec, "section", {}, {"id": sec}, {"v": 1}))
        facts.append(EdgeFact(
            doc, sec, "contains",
            source_record_id={"id": f"c{i}"}, source_revision={"v": 1},
        ))
    return facts


def test_gate_block_names_missing_narrowing_and_its_library_module(
    clean_runtime: str,
) -> None:
    """#3624: `section` is a known subtype but the catalog was composed without
    `document_starter`, so no `document contains section` narrowing exists. The
    block names that pattern, its count, and the module to add -- in the gate
    details, the persisted gate row, and the rendered block message."""
    from okg.substrate.cli.run_summary import _blocked_detail

    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["document", "section"],
            narrowings=[("document", "links_to", "document")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            _section_facts(sfx, 3), source="default", strict_endpoints=True,
        )
    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "blocked", out

    details = out["consistency_gates"][0]["details"]
    assert len(details["narrowing_violation_edge_ids"]) == 3
    assert details["narrowing_violation_patterns"] == [{
        "src_subtype": "document",
        "edge_type": "contains",
        "dst_subtype": "section",
        "pattern": "document contains section",
        "edge_count": 3,
        "library_modules": ["document_starter"],
        "library_module_sets": [["document_starter"]],
        "fix": _DOC_FIX,
    }]
    assert details["narrowing_violation_patterns_truncated"] is False
    assert details["node_subtype_violation_patterns"] == []

    message = _blocked_detail(out["blocked_reason"])
    assert (
        "3 edge(s) `document contains section` have no declared narrowing: "
        + _DOC_FIX
    ) in message, message

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        persisted = _transform_gate_rows(conn)[-1][5]
    assert persisted["narrowing_violation_patterns"][0]["library_modules"] == [
        "document_starter",
    ]


def test_gate_block_names_unknown_subtype_and_its_library_module(
    clean_runtime: str,
) -> None:
    """#3624: with `section` absent from the catalog too, the block also names
    the unknown subtype and the module that declares it; a subtype no library
    module declares is reported as the deployment's to declare."""
    sfx = uuid.uuid4().hex[:8]
    ghost = f"ghost:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["document"],
            narrowings=[("document", "links_to", "document")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                *_section_facts(sfx, 2),
                NodeFact(ghost, "ghost_subtype_3624", {}, {"id": ghost}, {"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "blocked", out

    details = out["consistency_gates"][0]["details"]
    by_subtype = {
        p["subtype"]: p for p in details["node_subtype_violation_patterns"]
    }
    assert by_subtype["section"]["node_count"] == 2
    assert by_subtype["section"]["library_modules"] == ["document_starter"]
    assert by_subtype["ghost_subtype_3624"]["library_modules"] == []
    assert by_subtype["ghost_subtype_3624"]["fix"].startswith(
        "no okg library module declares subtype `ghost_subtype_3624`"
    )
    assert details["narrowing_violation_patterns"][0]["pattern"] == (
        "document contains section"
    )


def test_clean_gate_run_reports_empty_pattern_fields(clean_runtime: str) -> None:
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["document", "section"],
            narrowings=[("document", "contains", "section")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            _section_facts(sfx, 1), source="default", strict_endpoints=True,
        )
    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "published", out
    details = out["consistency_gates"][0]["details"]
    assert details["narrowing_violation_patterns"] == []
    assert details["node_subtype_violation_patterns"] == []


@pytest.mark.parametrize("failure", ["library_index", "grouped_read"])
def test_pattern_lookup_failure_keeps_block_ids_and_row(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    """#3624: naming patterns is a diagnostic. If the library index raises or
    the grouped read fails in SQL, the gate still blocks on its ids, persists
    its row, and records the failure instead of turning into a gate error."""
    from okg.substrate.consistency import missing_patterns
    from okg.substrate.errors import CatalogModuleMissing

    if failure == "library_index":
        def _boom(*_a: object, **_k: object) -> None:
            raise CatalogModuleMissing("library index unreadable (test)")

        monkeypatch.setattr(missing_patterns, "library_declarations", _boom)
    else:
        monkeypatch.setattr(
            missing_patterns,
            "_NARROWING_GROUPS_SQL",
            "SELECT no_such_column_3624 FROM okg.graph_edges "
            "WHERE edge_id = ANY(:pattern_edge_ids)",
        )
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["document", "section"],
            narrowings=[("document", "links_to", "document")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            _section_facts(sfx, 2), source="default", strict_endpoints=True,
        )
    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "blocked", out

    gate = out["consistency_gates"][0]
    assert gate["reason"] == "transform_consistency_violation", gate
    details = gate["details"]
    assert len(details["narrowing_violation_edge_ids"]) == 2
    assert details["narrowing_violation_patterns"] == []
    error = details["violation_patterns_error"]
    assert error["type"] == (
        "CatalogModuleMissing" if failure == "library_index"
        else "UndefinedColumn"
    ), error

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = _transform_gate_rows(conn)
    assert rows[-1][2] is True  # blocks_publish
    assert len(rows[-1][5]["narrowing_violation_edge_ids"]) == 2
    assert rows[-1][5]["violation_patterns_error"]["type"] == error["type"]
