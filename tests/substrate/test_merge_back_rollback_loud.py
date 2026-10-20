"""Blocked-LAND rollback must degrade loudly (strengthen-formal-models wave 3).

The rollback is two sequential steps: delete the blocked generation, then
restore the target branch. The delete is best-effort by design (the restore
must run regardless), but a failed delete previously vanished into an
except-pass — the result claimed `land_gate_blocked_rolled_back` while the
blocked generation checkpoint stayed stranded, the exact state
MergeBackLandRollback.tla declares impossible and a silent-fallback P0 by
repo policy. The fixed contract: restore still runs, the result carries a
distinct rollback-incomplete marker naming the stranded generation, and a
warning is logged.
"""
from __future__ import annotations

import logging

import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.merge_back import merge_back

from tests.substrate.test_merge_back_orchestrator import (
    DECIDER,
    _BranchSensitiveGate,
    _default_node_ids,
    _default_publisher,
    _patch_merge_back_publisher_stack,
    merge_back_test_deployment,  # noqa: F401 - autouse fixture (issue #2866)
)

pytestmark = pytest.mark.postgres


def test_blocked_land_rollback_is_loud_when_delete_fails(
    clean_runtime: str,
    monkeypatch,
    caplog,
) -> None:
    psycopg = pytest.importorskip("psycopg")
    from tests.pytest.catalog_fixture_builders import seed_catalog

    gate = _BranchSensitiveGate(block_default=True)
    _patch_merge_back_publisher_stack(monkeypatch, gates=[gate])

    real_delete = ChronosGraphStore.delete_generation
    delete_attempts: list[str] = []

    def failing_delete(self, generation_id, *args, **kwargs):
        delete_attempts.append(str(generation_id))
        raise RuntimeError("simulated delete_generation outage")

    monkeypatch.setattr(ChronosGraphStore, "delete_generation", failing_delete)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["person"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:base", "person", {}, {"id": "n:base"}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
        base_gen = _default_publisher(clean_runtime).cycle()["generation_id"]
        before = _default_node_ids(store)

        store.create_branch(
            "experiment:loud",
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base_gen},
        )
        store.apply_facts(
            [
                NodeFact(
                    "n:stranded",
                    "person",
                    {"promote_intent": "durable"},
                    {"id": "n:stranded"},
                    {"v": 1},
                )
            ],
            branch="experiment:loud",
            source="fork.src",
            strict_endpoints=True,
        )

        with caplog.at_level(logging.WARNING, logger="okg.substrate.merge_back"):
            result = merge_back(
                store,
                "experiment:loud",
                dsn=clean_runtime,
                base_generation_id=base_gen,
                decider=DECIDER,
            )

        # Loud, never silent: the result must not claim a clean rollback.
        assert result.status == "blocked", result
        reason = str(result.blocked_reason)
        assert reason.startswith("land_gate_blocked_rollback_incomplete:"), (
            "a failed delete_generation must surface as rollback_incomplete, "
            f"not vanish into a clean rolled_back reason; got {reason!r}"
        )
        assert "stranded_generation=" in reason
        assert delete_attempts, "delete_generation was never attempted"
        assert any(
            "delete_generation" in record.message
            and record.name == "okg.substrate.merge_back"
            for record in caplog.records
        ), "the failed delete must log a warning from the merge_back logger"

        # The correctness-critical restore still ran: the fork's row is not
        # live on default and the published head is the pre-land generation.
        monkeypatch.setattr(ChronosGraphStore, "delete_generation", real_delete)
        assert _default_node_ids(store) == before
        assert "n:stranded" not in _default_node_ids(store)
        assert store.latest_generation(
            deployment_name="default",
            branch="default",
            status="published",
        ).checkpoint_id == base_gen
