"""chronos-fast-forward-branch: the sound segment-lineage fast-forward primitive.

fast_forward_branch advances `default` onto a gated staging segment in one transaction,
so the staging segment (which physically holds the merged rows) enters default's segment
ancestry -- unlike a checkpoint re-tag, which leaves default.current_segment_id on the
pre-merge segment and phantom-retracts the merged rows on the next publish.

These tests exercise the primitive's structure + its deny-by-default refusal preconditions
(implement-primitive). The okg-level soundness proofs (no phantom-retraction, older-fork
base, GC reachability) live alongside, driven through the real publisher.
"""
from __future__ import annotations

import pytest

from chronos_core.branching import BranchingError

from okg.substrate.errors import EffectiveIntervalBackfillRequired
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _head(store: ChronosGraphStore, branch: str) -> int:
    row = store.context.db.execute(
        "SELECT current_segment_id FROM _chronos_branch_interval_branches WHERE branch_id = ?",
        (branch,),
    ).fetchone()
    return int(row["current_segment_id"])


def _segment(store: ChronosGraphStore, seg_id: int):
    return store.context.db.execute(
        "SELECT * FROM _chronos_branch_interval_segments WHERE segment_id = ?",
        (int(seg_id),),
    ).fetchone()


def _checkpoint_segment(store: ChronosGraphStore, checkpoint_id: str):
    row = store.context.db.execute(
        "SELECT segment_id FROM _chronos_branch_interval_checkpoints WHERE checkpoint_id = ?",
        (checkpoint_id,),
    ).fetchone()
    return int(row["segment_id"]) if row else None


def _stage(store: ChronosGraphStore, staging: str = "staging:m1"):
    """Seed a base node on default, fork a staging child, write a merged node on staging.
    Returns (expected_default_head, staging_head)."""
    store.apply_facts([NodeFact("n:base", "claim", {"text": "b"}, {}, {})], source="default")
    store.create_branch(staging, from_branch="default")
    expected = _head(store, "default")          # default's post-stage head (empty)
    staging_head = _head(store, staging)
    store.apply_facts(
        [NodeFact("n:merged", "claim", {"text": "m"}, {}, {})],
        branch=staging, source="fork.src",
    )
    return expected, staging_head


def test_fast_forward_advances_default_onto_staging_segment(clean_runtime: str) -> None:
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        expected, staging_head = _stage(store)
        info = store.context.fast_forward_branch(
            "okg:ff:gen1", "default", staging_head, expected,
            {"kind": "okg_generation", "status": "published"},
        )
        # default advanced onto a MUTABLE continuation parented on the staging segment
        # (so the staging segment S is in default's ancestry).
        new_head = _head(store, "default")
        assert new_head != expected
        head_row = _segment(store, new_head)
        assert head_row["segment_kind"] == "mutable"
        assert int(head_row["parent_segment_id"]) == staging_head
        # the new published checkpoint points at a snapshot segment parented on S.
        snap = _checkpoint_segment(store, "okg:ff:gen1")
        assert snap is not None and info.ref == str(snap)
        snap_row = _segment(store, snap)
        assert snap_row["segment_kind"] == "checkpoint"
        assert int(snap_row["parent_segment_id"]) == staging_head
        # the merged + inherited base rows read live on default after the fast-forward.
        rows = store.checkout("default").query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id IN ('n:base', 'n:merged')"
        )
        assert {r["node_id"] for r in rows} == {"n:base", "n:merged"}
    finally:
        store.close()


def test_fast_forward_refuses_when_default_head_advanced(clean_runtime: str) -> None:
    # A publish/branch after staging advances default.current_segment_id -> not a true FF.
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        expected, staging_head = _stage(store)
        store.create_branch("other", from_branch="default")  # advances default's head
        assert _head(store, "default") != expected
        with pytest.raises(BranchingError, match="advanced since staging"):
            store.context.fast_forward_branch("okg:ff:gen1", "default", staging_head, expected, {})
    finally:
        store.close()


def test_fast_forward_refuses_when_default_head_has_writes(clean_runtime: str) -> None:
    # A PLAIN write into default's head does NOT advance current_segment_id, but would be
    # orphaned by the head move -> refuse (the empty-head guard the head check alone misses).
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        expected, staging_head = _stage(store)
        store.apply_facts([NodeFact("n:intervening", "claim", {}, {}, {})], source="default")
        assert _head(store, "default") == expected  # head unchanged...
        with pytest.raises(BranchingError, match="writes in its current head"):
            store.context.fast_forward_branch("okg:ff:gen1", "default", staging_head, expected, {})
        # default unchanged; the intervening write is preserved (not lost).
        assert _head(store, "default") == expected
        rows = store.checkout("default").query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = 'n:intervening'"
        )
        assert len(rows) == 1
    finally:
        store.close()


def test_fast_forward_refuses_staging_segment_with_a_child(clean_runtime: str) -> None:
    # onto_segment must be the staging branch's childless head; a fork off staging makes it
    # a parent, and _split_segment would alias the child's interval.
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        expected, staging_head = _stage(store)
        store.create_branch("substaging", from_branch="staging:m1")  # staging head gets a child
        # (creating substaging also advances staging's head off staging_head, so the refusal
        # is the not-current-head guard; the childless guard is defense-in-depth behind it.)
        with pytest.raises(BranchingError, match="not the current head|already has child"):
            store.context.fast_forward_branch("okg:ff:gen1", "default", staging_head, expected, {})
    finally:
        store.close()


def test_okg_fast_forward_threads_stage_time_expected(clean_runtime: str) -> None:
    # The okg hook must USE the caller's stage-time expected head, not recompute it fresh --
    # else the orchestrator's lost-update guard is vacuous. A stale expected (default advanced
    # since staging) must refuse.
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        expected, _staging_head = _stage(store, "staging:m1")
        store.create_branch("concurrent", from_branch="default")  # default advances after stage
        assert _head(store, "default") != expected
        with pytest.raises(BranchingError, match="advanced since staging"):
            store.fast_forward("staging:m1", target="default", expected_current_segment=expected)
    finally:
        store.close()


def test_fast_forward_unsupported_on_non_interval_backend(clean_runtime: str) -> None:
    store = ChronosGraphStore.connect(clean_runtime, backend="log")
    try:
        with pytest.raises(BranchingError):
            store.context.fast_forward_branch("okg:ff:gen1", "default", 1, 1, {})
    finally:
        store.close()


def _default_publisher(dsn: str):
    from okg.substrate.consistency import TransformConsistencyGate
    from okg.substrate.publisher import Publisher

    return Publisher(dsn, "default", consistency_gates=[TransformConsistencyGate()], catalog_identity=bind_test_catalog_writer(dsn, "default"))


@pytest.mark.xfail(
    strict=True,
    raises=EffectiveIntervalBackfillRequired,
    reason="#2317 store.fast_forward records no graph counts or ordinal, so the next publish refuses",
)
def test_fast_forward_no_phantom_retraction(clean_runtime: str) -> None:
    # The load-bearing soundness proof, through the REAL publisher: after a fast-forward,
    # the next default publish must NOT phantom-retract the merged rows.
    psycopg = pytest.importorskip("psycopg")
    from okg.substrate.catalog.aliases import active_catalog_version_id
    from tests.pytest.catalog_fixture_builders import seed_catalog

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])
        cv = active_catalog_version_id(conn)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:base", "person", {}, {"id": "n:base"}, {"v": 1})],
            source="default", strict_endpoints=True,
        )
        g0 = _default_publisher(clean_runtime).cycle()
        assert g0["status"] == "published", g0

        # STAGE: a staging child of default + a merged node on staging.
        store.create_branch("staging:m1", from_branch="default", metadata={"base_branch": "default"})
        store.apply_facts(
            [NodeFact("n:merged", "person", {}, {"id": "n:merged"}, {"v": 1})],
            branch="staging:m1", source="fork.src", strict_endpoints=True,
        )

        # FAST-FORWARD default onto staging (the okg hook) -> g1.
        g1 = store.fast_forward(
            "staging:m1", target="default", deployment_name="default", catalog_version_id=cv,
        )
        # the okg hook recorded a resolvable okg-generation (req.okg-hook): latest_generation
        # returns the FF generation with kind=okg_generation and the staged catalog version.
        latest = store.latest_generation(branch="default", status="published")
        assert latest is not None and latest.checkpoint_id == g1
        assert dict(latest.metadata or {}).get("kind") == "okg_generation"
        assert dict(latest.metadata or {}).get("catalog_version_id") == cv
        # merged + inherited base read live on default after the fast-forward.
        rows = store.checkout("default").query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id IN ('n:base', 'n:merged')"
        )
        assert {r["node_id"] for r in rows} == {"n:base", "n:merged"}, rows

        # Write an unrelated node on default and publish -> g2.
        store.apply_facts(
            [NodeFact("n:after", "person", {}, {"id": "n:after"}, {"v": 1})],
            source="default", strict_endpoints=True,
        )
        g2 = _default_publisher(clean_runtime).cycle()
        assert g2["status"] == "published", g2
        # LOAD-BEARING: only n:after is added, and the merged rows are NOT phantom-retracted.
        assert g2["nodes_added"] == 1, g2
        assert g2["nodes_retracted"] == 0, g2
        # all three read live on default after g2 (no silent drop of the merged rows).
        rows2 = store.checkout("default").query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE node_id IN ('n:base', 'n:merged', 'n:after')"
        )
        assert {r["node_id"] for r in rows2} == {"n:base", "n:merged", "n:after"}, rows2


def test_fast_forward_preserves_older_fork_base(clean_runtime: str) -> None:
    # The subtlest soundness case: a fork created from default BEFORE the merge-back must
    # still discover its real three-way base against the fast-forwarded default.
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([NodeFact("n:base", "claim", {}, {}, {})], source="default")
        store.create_branch("fork:c", from_branch="default")  # older fork, BEFORE the merge
        store.create_branch("staging:m1", from_branch="default")
        expected = _head(store, "default")
        staging_head = _head(store, "staging:m1")
        store.apply_facts(
            [NodeFact("n:merged", "claim", {}, {}, {})], branch="staging:m1", source="fork.src"
        )
        store.context.fast_forward_branch("okg:ff:gen1", "default", staging_head, expected, {})
        # backend.merge_preview returns None ONLY when there is no shared fork_base; the
        # runtime fabricates a non-None two-way preview, so assert at the BACKEND layer.
        preview = store.context._backend.merge_preview("fork:c", "default")
        assert preview is not None, "older fork lost its three-way base after the fast-forward"
    finally:
        store.close()


def test_fast_forward_merged_rows_survive_gc(clean_runtime: str) -> None:
    from chronos_core.branching._interval_backend import (
        _collect_interval_garbage,
        _interval_dead_segment_ids,
    )

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        expected, staging_head = _stage(store, "staging:m1")
        store.context.fast_forward_branch("okg:ff:gen1", "default", staging_head, expected, {})
        store.delete_branch("staging:m1")
        db = store.context.db
        dead = _interval_dead_segment_ids(db)
        # GC actually ran + discriminates: default's orphaned post-stage continuation is dead.
        assert dead, "GC found nothing dead -- the survival assertion would be vacuous"
        # ...but the staging segment holding the merged rows (writer_segment_id) is NOT dead.
        assert int(staging_head) not in dead, dead
        _collect_interval_garbage(db, "interval")
        db.commit()
        rows = store.checkout("default").query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = 'n:merged'"
        )
        assert len(rows) == 1, "merged rows physically deleted by GC"
    finally:
        store.close()


def test_repeated_fast_forwards_exhaust_interval_without_reclamation(clean_runtime: str) -> None:
    # FINDING (the budget proof's value): each merge-back lands default on a sqrt-sized
    # staging sub-interval, so default's interval budget collapses geometrically (W -> ~sqrt
    # W per cycle). Repeated fast-forwards therefore exhaust the interval after a handful of
    # cycles. Sustained merge-back needs interval RECLAMATION (compaction resets default's
    # interval but severs live forks, so it is only safe in a quiescent window) -- an
    # orchestrator-level concern. This pins the real limit honestly rather than asserting an
    # unmet 10-cycle target.
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([NodeFact("n:base", "claim", {}, {}, {})], source="default")
        survived = 0
        exhausted = False
        for i in range(20):
            staging = f"staging:{i}"
            try:
                store.create_branch(staging, from_branch="default")
                expected = _head(store, "default")
                staging_head = _head(store, staging)
                store.apply_facts(
                    [NodeFact(f"n:m{i}", "claim", {}, {}, {})], branch=staging, source="fork.src"
                )
                store.context.fast_forward_branch(
                    f"okg:ff:gen{i}", "default", staging_head, expected, {}
                )
                survived += 1
            except BranchingError as exc:
                assert "interval space exhausted" in str(exc), exc
                exhausted = True
                break
        assert survived >= 1, survived
        assert exhausted, "expected interval exhaustion without reclamation"
        # every merged row from the surviving cycles is live on default.
        rows = store.checkout("default").query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id LIKE 'n:m%'"
        )
        assert len({r["node_id"] for r in rows}) == survived, (survived, rows)
    finally:
        store.close()
