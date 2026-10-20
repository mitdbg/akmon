from __future__ import annotations

from pathlib import Path

import pytest

from chronos_core.branching import BranchingError
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import (
    ChronosGraphStore,
    deployment_interval_continuation_percent,
)
from okg.substrate.library.sources.base import NodeFact


pytestmark = pytest.mark.postgres

_ROOT = Path(__file__).resolve().parents[2]


def _legacy_reroot_under_checkpoint(store: ChronosGraphStore, generation: str) -> None:
    """Point ``default`` at a fresh segment inside ``generation``'s snapshot.

    This is the pre-#2862 rollback's end state: the branch sees the checkpoint
    but owns only the upper half of its sqrt-sized snapshot interval.
    """
    backend = store.context._backend
    db = store.context.db
    checkpoint = db.execute(
        "SELECT segment_id FROM _chronos_branch_interval_checkpoints "
        "WHERE checkpoint_id = ?",
        (store._generation_checkpoint_id(generation),),
    ).fetchone()
    snapshot = backend._segment(checkpoint["segment_id"])
    lo = int(snapshot.live_lo) + (int(snapshot.live_hi) - int(snapshot.live_lo)) // 2
    hi = int(snapshot.live_hi)
    child = {
        "segment_id": backend._allocate_segment_ids(1)[0],
        "live_lo": lo,
        "live_hi": hi,
        "branch_point": lo + (hi - lo) // 2,
    }
    backend._insert_segment(
        child,
        int(snapshot.segment_id),
        "default",
        "mutable",
        "2026-09-28T00:00:00+00:00",
    )
    db.execute(
        "UPDATE _chronos_branch_interval_branches SET current_segment_id = ? "
        "WHERE branch_id = 'default'",
        (child["segment_id"],),
    )
    store.context._commit_autocommit()
    store.context._metadata_epoch += 1


def test_env_continuation_percent_preserves_checkpoint_history(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHRONOS_INTERVAL_CONTINUATION_PERCENT", "95")
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([
            NodeFact("n:1", "claim", {"text": "initial"}, {}, {}),
        ])

        for index in range(60):
            store.checkpoint_generation(
                f"checkpoint-{index}",
                deployment_name="interval-budget-test",
            )

        first_generation = store.checkout_generation("checkpoint-0")
        assert first_generation.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:1'"
        ) == [{"attrs": {"text": "initial"}}]
    finally:
        store.close()


def test_adaptive_interval_allocation_supports_many_sibling_branches(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHRONOS_INTERVAL_CONTINUATION_PERCENT", "5")
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        for index in range(128):
            store.create_branch(f"wide-{index}", from_branch="default")

        store.apply_facts(
            [NodeFact("n:wide", "claim", {"text": "branch-local"}, {}, {})],
            branch="wide-127",
        )

        assert store.checkout("wide-127").query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:wide'"
        ) == [{"attrs": {"text": "branch-local"}}]
        assert store.checkout("default").query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:wide'"
        ) == []

        checkpoint = store.checkpoint_generation(
            "wide-checkpoint",
            deployment_name="interval-branch-test",
        )
        assert checkpoint == "generation:wide-checkpoint"
    finally:
        store.close()


def test_okg_workspace_manifest_sets_interval_continuation_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CHRONOS_INTERVAL_CONTINUATION_PERCENT", raising=False)

    assert deployment_interval_continuation_percent("okg-workspace") == 95


def test_compaction_rebases_exhausted_branch_from_latest_generation(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHRONOS_INTERVAL_CONTINUATION_PERCENT", "5")
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([
            NodeFact("n:1", "claim", {"text": "compacted"}, {}, {}),
        ])
        latest = ""
        for index in range(20):
            latest = store.checkpoint_generation(
                f"pre-compact-{index}",
                deployment_name="interval-budget-test",
                catalog_version_id=7,
                metadata={"target_generation_id": 123},
            )
    finally:
        store.close()

    monkeypatch.setenv("CHRONOS_INTERVAL_CONTINUATION_PERCENT", "95")
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        result = store.compact_branch_from_generation(
            latest,
            deployment_name="interval-budget-test",
        )

        assert result["source_generation_id"] == latest
        assert result["node_count"] == 1
        assert result["edge_count"] == 0
        metadata = store.get_generation(result["new_generation_id"]).metadata
        assert metadata["catalog_version_id"] == 7
        assert metadata["target_generation_id"] == 123
        assert metadata["publisher"] == "chronos_compaction"
        compacted = store.checkout_generation(result["new_generation_id"])
        assert compacted.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:1'"
        ) == [{"attrs": {"text": "compacted"}}]
    finally:
        store.close()


def test_rollover_recovers_interval_budget_without_deleting_generations(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHRONOS_INTERVAL_CONTINUATION_PERCENT", "5")
    deployment = "interval-rollover-history-test"
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([
            NodeFact("n:1", "claim", {"text": "published"}, {}, {}),
        ])
        first = store.checkpoint_generation(
            "rollover-first",
            deployment_name=deployment,
        )

        # Reproduces the live failure mode: a branch re-rooted beneath a
        # checkpoint gets that checkpoint's much smaller interval budget. Before
        # issue #2862 the blocked merge-back rollback re-rooted this way; the
        # helper recreates such a database for the recovery path.
        _legacy_reroot_under_checkpoint(store, first)
        latest = first
        for index in range(32):
            try:
                latest = store.checkpoint_generation(
                    f"rollover-before-{index}",
                    deployment_name=deployment,
                )
            except BranchingError as exc:
                assert "interval space exhausted" in str(exc)
                break
        else:
            pytest.fail("restored branch did not exhaust its interval budget")

        generations_before = [
            generation.checkpoint_id
            for generation in store.list_generations(
                deployment_name=deployment,
                branch="default",
                status="published",
            )
        ]
        assert generations_before

        with pytest.raises(SubstrateError, match="metadata attempts"):
            store.rollover_branch_from_generation(
                latest,
                deployment_name=deployment,
                metadata={"rolled_over_from_generation": "forged"},
            )

        # This write is deliberately not checkpointed. Rollover restores the
        # selected published generation, not the dirty working head.
        store.apply_facts([
            NodeFact("n:unpublished", "claim", {"text": "discard me"}, {}, {}),
        ])
        result = store.rollover_branch_from_generation(
            latest,
            deployment_name=deployment,
        )

        assert result["source_generation_id"] == latest
        assert result["preserved_checkpoint_count"] == len(generations_before)
        generations_after_rollover = [
            generation.checkpoint_id
            for generation in store.list_generations(
                deployment_name=deployment,
                branch="default",
                status="published",
            )
        ]
        assert generations_after_rollover[:-1] == generations_before
        assert generations_after_rollover[-1] == result["new_generation_id"]
        assert result["checkpoint_count_after"] == len(generations_before) + 1
        rollover_metadata = store.get_generation(
            result["new_generation_id"]
        ).metadata
        assert rollover_metadata["publisher"] == "chronos_rollover"
        assert rollover_metadata["rolled_over_from_generation"] == latest
        assert store.checkout_generation(first).query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:1'"
        ) == [{"attrs": {"text": "published"}}]
        assert store.checkout().query(
            "SELECT node_id FROM okg.graph_nodes ORDER BY node_id"
        ) == [{"node_id": "n:1"}]

        branch = store.context.db.execute(
            """
            SELECT s.live_hi - s.live_lo AS width
              FROM _chronos_branch_interval_branches AS b
              JOIN _chronos_branch_interval_segments AS s
                ON s.segment_id = b.current_segment_id
             WHERE b.branch_id = 'default'
            """
        ).fetchone()
        assert int(branch["width"]) > 2**32

        after = store.checkpoint_generation(
            "rollover-after",
            deployment_name=deployment,
        )
        assert after == "generation:rollover-after"
        assert store.checkout_generation(after).query(
            "SELECT node_id FROM okg.graph_nodes ORDER BY node_id"
        ) == [{"node_id": "n:1"}]
        assert store.checkout_generation(first).query(
            "SELECT node_id FROM okg.graph_nodes ORDER BY node_id"
        ) == [{"node_id": "n:1"}]
    finally:
        store.close()


def test_compaction_is_scoped_to_target_branch_or_deployment(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHRONOS_INTERVAL_CONTINUATION_PERCENT", "5")
    deployment = "interval-compaction-scope-test"
    target_branch = "compact-target"
    untouched_branch = "compact-untouched"
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.create_branch(
            target_branch,
            from_branch="default",
            metadata={
                "kind": "okg_branch",
                "deployment_name": deployment,
                "branch_name": target_branch,
                "role": "experiment",
                "status": "active",
            },
        )
        store.create_branch(
            untouched_branch,
            from_branch="default",
            metadata={
                "kind": "okg_branch",
                "deployment_name": deployment,
                "branch_name": untouched_branch,
                "role": "experiment",
                "status": "active",
            },
        )
        store.apply_facts(
            [NodeFact("n:target", "claim", {"text": "target"}, {}, {})],
            branch=target_branch,
        )
        target_generation = store.checkpoint_generation(
            "scope-target-before-compact",
            branch=target_branch,
            deployment_name=deployment,
        )
        store.apply_facts(
            [NodeFact("n:untouched", "claim", {"text": "untouched"}, {}, {})],
            branch=untouched_branch,
        )
        untouched_generation = store.checkpoint_generation(
            "scope-untouched-before-compact",
            branch=untouched_branch,
            deployment_name=deployment,
        )
        untouched_before = store.checkout_generation(untouched_generation).query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:untouched'"
        )

        result = store.compact_branch_from_generation(
            target_generation,
            deployment_name=deployment,
            branch=target_branch,
        )

        assert store.context.get_branch(untouched_branch).metadata[
            "deployment_name"
        ] == deployment
        assert store.get_generation(untouched_generation).metadata[
            "deployment_name"
        ] == deployment
        assert store.checkout_generation(untouched_generation).query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:untouched'"
        ) == untouched_before
        assert store.checkout_generation(result["new_generation_id"]).query(
            "SELECT node_id, attrs FROM okg.graph_nodes ORDER BY node_id"
        ) == [{"node_id": "n:target", "attrs": {"text": "target"}}]
    finally:
        store.close()


def test_compaction_refuses_shared_deployment_branch(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHRONOS_INTERVAL_CONTINUATION_PERCENT", "5")
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([
            NodeFact("n:shared", "claim", {"text": "shared branch"}, {}, {}),
        ])
        target_generation = store.checkpoint_generation(
            "shared-branch-target",
            deployment_name="shared-branch-target-deployment",
        )
        other_generation = store.checkpoint_generation(
            "shared-branch-other",
            deployment_name="shared-branch-other-deployment",
        )

        with pytest.raises(SubstrateError, match="another deployment"):
            store.compact_branch_from_generation(
                target_generation,
                deployment_name="shared-branch-target-deployment",
            )

        assert store.get_generation(other_generation).metadata[
            "deployment_name"
        ] == "shared-branch-other-deployment"
    finally:
        store.close()


def test_compaction_refuses_child_branch_created_after_precheck(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHRONOS_INTERVAL_CONTINUATION_PERCENT", "5")
    deployment = "interval-compaction-race-test"
    target_branch = "compact-race-target"
    child_branch = "compact-race-child"
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.create_branch(
            target_branch,
            from_branch="default",
            metadata={
                "kind": "okg_branch",
                "deployment_name": deployment,
                "branch_name": target_branch,
                "role": "experiment",
                "status": "active",
            },
        )
        store.apply_facts(
            [NodeFact("n:race-target", "claim", {"text": "target"}, {}, {})],
            branch=target_branch,
        )
        target_generation = store.checkpoint_generation(
            "race-target-before-compact",
            branch=target_branch,
            deployment_name=deployment,
        )
        original_assert_scope = store._assert_branch_compaction_scope

        def _assert_scope_then_create_child(
            *,
            deployment_name: str,
            branch: str,
        ) -> None:
            original_assert_scope(
                deployment_name=deployment_name,
                branch=branch,
            )
            store.create_branch(
                child_branch,
                from_branch=branch,
                metadata={
                    "kind": "okg_branch",
                    "deployment_name": deployment,
                    "branch_name": child_branch,
                    "role": "experiment",
                    "status": "active",
                },
            )

        monkeypatch.setattr(
            store,
            "_assert_branch_compaction_scope",
            _assert_scope_then_create_child,
        )

        with pytest.raises(SubstrateError, match="child branches"):
            store.compact_branch_from_generation(
                target_generation,
                deployment_name=deployment,
                branch=target_branch,
            )

        assert store.context.get_branch(child_branch).metadata[
            "deployment_name"
        ] == deployment
        assert store.get_generation(target_generation).metadata[
            "deployment_name"
        ] == deployment
    finally:
        store.close()


def test_compaction_refuses_metadata_scope_override(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHRONOS_INTERVAL_CONTINUATION_PERCENT", "5")
    deployment = "interval-compaction-metadata-test"
    target_branch = "compact-metadata-target"
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.create_branch(
            target_branch,
            from_branch="default",
            metadata={
                "kind": "okg_branch",
                "deployment_name": deployment,
                "branch_name": target_branch,
                "role": "experiment",
                "status": "active",
            },
        )
        store.apply_facts(
            [NodeFact("n:metadata", "claim", {"text": "target"}, {}, {})],
            branch=target_branch,
        )
        target_generation = store.checkpoint_generation(
            "metadata-target-before-compact",
            branch=target_branch,
            deployment_name=deployment,
        )

        for metadata in (
            {"deployment_name": "other-deployment"},
            {"catalog_version_id": 999},
            {"target_generation_id": 999},
        ):
            with pytest.raises(SubstrateError, match="metadata attempts"):
                store.compact_branch_from_generation(
                    target_generation,
                    deployment_name=deployment,
                    branch=target_branch,
                    metadata=metadata,
                )

        assert store.get_generation(target_generation).metadata[
            "deployment_name"
        ] == deployment
    finally:
        store.close()


def test_compaction_formal_boundary_documented() -> None:
    report = _ROOT / (
        "pact/archive/scope-chronos-compaction-to-deployment-branch/"
        "reports/compaction-formal-boundary.md"
    )
    implementation = (
        _ROOT / "src/okg/substrate/graph_store_internal/generations.py"
    )

    report_text = report.read_text(encoding="utf-8")
    implementation_text = implementation.read_text(encoding="utf-8")
    normalized_report = " ".join(report_text.split())

    assert "No new TLA+/Chronos formal module was added" in normalized_report
    assert (
        "load-bearing proof is a scoped database/runtime proof"
        in normalized_report
    )
    assert "_assert_branch_compaction_scope" in implementation_text
    assert "TRUNCATE" not in implementation_text
