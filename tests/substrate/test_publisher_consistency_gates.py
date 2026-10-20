"""Publisher integration for consistency publish gates."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

import psycopg
import pytest

from okg.substrate.consistency import (
    CandidateCoverageReport,
    ConsistencyCandidate,
    ConsistencyCheckDefinition,
    ConsistencyPublishGateEvaluation,
    ObservationEvent,
    append_observation_event,
    complete_consistency_run,
    evaluate_consistency_publish_gate,
    observation_id_for,
    record_candidate,
    record_candidate_coverage,
    start_consistency_run,
)
from okg.substrate.derived_artifacts import stable_hash
from okg.substrate.publisher import Publisher
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _definition(deployment_name: str) -> ConsistencyCheckDefinition:
    return ConsistencyCheckDefinition(
        check_id="test.publisher.consistency-gate",
        deployment_name=deployment_name,
        predicate_family="numeric_claim_conflict",
        candidate_rule_hash=stable_hash({"blocking": "subject+unit"}),
        evaluator_ref="okg.evaluator.sql.numeric_conflict.v1",
        evaluator_hash=stable_hash({"evaluator": "numeric_conflict_v1"}),
        confidence_type="deterministic_boolean",
        compute_hash=stable_hash({"candidate_sql": "claim_pairs"}),
        output_schema_hash=stable_hash({"outputs": "observations"}),
        renderer_hash=stable_hash({"renderer": "observation-json"}),
        dirty_rule_hash=stable_hash({"scope": "claim_bucket"}),
        coverage_contract={
            "universe": "claim_chunks",
            "coverage": "exact_sql",
        },
    )


def _candidate() -> ConsistencyCandidate:
    return ConsistencyCandidate(
        input_scope="bucket:fuel-minimum",
        blocking_rule_hash=stable_hash({"block": "fuel-minimum"}),
        participant_roles={
            "left": "claim:fuel-minimum:pub-a",
            "right": "claim:fuel-minimum:pub-b",
        },
        predicate_key="numeric_equal_under_condition",
        generation_policy="stable_source_anchor_v1",
    )


def _observation_event(
    definition: ConsistencyCheckDefinition,
    candidate_id: str,
) -> ObservationEvent:
    participants = {
        "left": "claim:fuel-minimum:pub-a",
        "right": "claim:fuel-minimum:pub-b",
    }
    observation_id = observation_id_for(
        definition,
        candidate_id=candidate_id,
        input_scope="bucket:fuel-minimum",
        participant_roles=participants,
        normalized_predicate="bingo_fuel_minimum_lb_equal",
        normalized_values={"left": 2000, "right": 1500},
        source_revision_policy="stable_source_anchor_v1",
    )
    return ObservationEvent(
        observation_id=observation_id,
        candidate_id=candidate_id,
        input_scope="bucket:fuel-minimum",
        predicate_key="numeric_equal_under_condition",
        normalized_predicate="bingo_fuel_minimum_lb_equal",
        normalized_values={"left": 2000, "right": 1500},
        participant_roles=participants,
        finding_event="observed",
        severity="high",
        confidence=Decimal("1"),
        evidence={"spans": ["page:12:bbox:1"]},
    )


class DemoConsistencyGate:
    name = "numeric_conflicts_gate"

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls = 0
        self.target_generation_ids: list[int] = []

    def evaluate(
        self,
        conn: psycopg.Connection[Any],
        *,
        target_generation_id: int,
        catalog_version_id: int,
        deployment_name: str,
        branch_name: str = "default",
    ) -> ConsistencyPublishGateEvaluation:
        self.calls += 1
        self.target_generation_ids.append(target_generation_id)
        definition = _definition(deployment_name)
        run_id = start_consistency_run(
            conn,
            definition,
            target_generation_id=target_generation_id,
            trigger_kind="dirty",
        )
        candidate = _candidate()
        candidate_id = record_candidate(
            conn,
            definition,
            target_generation_id=target_generation_id,
            run_id=run_id,
            candidate=candidate,
        )
        record_candidate_coverage(
            conn,
            definition,
            target_generation_id=target_generation_id,
            run_id=run_id,
            report=CandidateCoverageReport(
                universe_key=f"claim_chunks:{target_generation_id}",
                coverage_basis="exact_postgres",
                coverage_complete=True,
                eligible_input_count=2,
                candidate_count=1,
                candidate_count_by_block={"fuel-minimum": 1},
                evaluator_invocation_count=1,
            ),
        )
        if self.fail:
            append_observation_event(
                conn,
                definition,
                target_generation_id=target_generation_id,
                run_id=run_id,
                event=_observation_event(definition, candidate_id),
            )
        evaluation = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=target_generation_id,
            run_id=run_id,
            gate_name=self.name,
            budget_ms=3000,
            elapsed_ms=12,
            preflight={
                "catalog_version_id": catalog_version_id,
                "estimated_candidates": 1,
            },
        )
        complete_consistency_run(
            conn,
            run_id=run_id,
            metrics={"publisher_gate": self.name},
        )
        return evaluation


def test_publisher_records_passing_consistency_gate_in_summary(
    clean_runtime: str,
) -> None:
    gate = DemoConsistencyGate()

    out = Publisher(
        clean_runtime,
        "default",
        consistency_gates=[gate],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert out["status"] == "published"
    assert out["phase_b_blocked"] is False
    assert out["consistency_gates"][0]["result"] == "evaluated"
    assert gate.calls == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        checkpoint = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).get_generation(out["generation_id"])
        summary = checkpoint.metadata["invariant_summary"]
        cur.execute(
            """
            SELECT result, blocks_publish, target_generation_id
              FROM okg.consistency_gate_results
            """
        )
        gate_row = cur.fetchone()

    assert summary["consistency_gates"]["total"] == 1
    assert summary["consistency_gates"]["passed"] == 1
    assert summary["consistency_gates"]["blocked"] == 0
    assert gate_row == ("evaluated", False, gate.target_generation_ids[0])


def test_publisher_blocks_on_failing_consistency_gate(
    clean_runtime: str,
) -> None:
    gate = DemoConsistencyGate(fail=True)

    out = Publisher(
        clean_runtime,
        "default",
        consistency_gates=[gate],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert out["status"] == "blocked"
    assert out["phase_b_blocked"] is True
    assert out["consistency_gates"][0]["result"] == "evaluated"
    assert out["consistency_gates"][0]["open_observation_count"] == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        checkpoint = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).get_generation(out["generation_id"])
        reason = checkpoint.metadata["blocked_reason"]
        cur.execute(
            """
            SELECT result, blocks_publish, reason
              FROM okg.consistency_gate_results
            """
        )
        gate_row = cur.fetchone()

    assert reason["reason"] == "consistency_publish_gate_blocked"
    failed = reason["failed_consistency_gates"]
    assert failed[0]["gate_name"] == "numeric_conflicts_gate"
    assert failed[0]["result"] == "evaluated"
    assert gate_row == ("evaluated", True, "open_observations")


def test_publisher_reuses_same_catalog_noop_and_regates_dirty_changes(
    clean_runtime: str,
) -> None:
    gate = DemoConsistencyGate()
    pub = Publisher(
        clean_runtime,
        "default",
        consistency_gates=[gate],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    )

    first = pub.cycle()
    second = pub.cycle()

    assert first["status"] == "published"
    assert second["status"] == "published"
    assert second["generation_id"] == first["generation_id"]
    assert second["noop"] is True
    assert second["reused_generation"] is True
    assert second["reuse_reason"] == "chronos_noop_same_catalog"
    assert second["consistency_gates"] == []
    assert gate.calls == 1
    assert len(gate.target_generation_ids) == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.consistency_gate_results
             WHERE gate_name = 'numeric_conflicts_gate'
            """
        )
        gate_count = cur.fetchone()[0]
        second_summary = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).get_generation(second["generation_id"]).metadata["invariant_summary"]

    assert gate_count == 1
    assert "gate_reuse" not in second_summary

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([
            NodeFact(
                node_id="n:dirty",
                subtype="concept",
                attrs={"name": "dirty"},
                source_record_id={"id": "dirty"},
                source_revision={"rev": 1},
            ),
        ])
    finally:
        store.close()

    third = pub.cycle()

    assert third["status"] == "published"
    assert third["generation_id"] != first["generation_id"]
    assert third["noop"] is False
    assert third["consistency_gates"][0]["result"] == "evaluated"
    assert gate.calls == 2
    assert len(gate.target_generation_ids) == 2
    assert gate.target_generation_ids[0] != gate.target_generation_ids[1]

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.consistency_gate_results
             WHERE gate_name = 'numeric_conflicts_gate'
            """
        )
        gate_count = cur.fetchone()[0]

    assert gate_count == 2
