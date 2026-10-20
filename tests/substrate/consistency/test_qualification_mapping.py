"""Witnesses for the Concord "qualified" predicate ↔ engine blocking policy map.

The Concord paper (formalism-adjudication-v2 F1.1) defines a result as
*qualified* iff no unresolved infinite-cost (mandatory) violation remains. The
ratified adjudication maps this onto the engine's EXISTING blocking machinery
with no new state: a finding is *mandatory* iff leaving an open instance of it
unresolved forces the publish gate to block, under the check's declared blocking
config (``block_on`` / ``block_on_min_severity``) plus the deployment's blocking
policy. The derived facet is ``finding_is_mandatory``; the gate that enforces it
is ``evaluate_consistency_publish_gate``.

These tests witness the mapping through the REAL gate-evaluation path and prove
the pure facet agrees with the gate's verdict (so it cannot drift):

  * ``test_finding_is_mandatory_truth_table`` — the pure derivation (no DB).
  * ``test_open_finding_on_blocking_check_is_mandatory_and_blocks`` — (a) an open
    finding on a blocking-declared check blocks publish.
  * ``test_resolving_the_mandatory_finding_unblocks_publish`` — (b) resolving it
    unblocks the same path.
  * ``test_finite_severity_finding_is_not_mandatory_and_never_blocks`` — (c) a
    finite-severity (non-blocking-declared) finding never blocks under the same
    deployment policy.
"""
from __future__ import annotations

from collections import namedtuple
from uuid import UUID

import psycopg
import pytest

from okg.substrate.consistency import (
    CandidateCoverageReport,
    ConsistencyCandidate,
    ConsistencyCheckDefinition,
    ObservationEvent,
    append_observation_event,
    evaluate_consistency_publish_gate,
    finding_is_mandatory,
    observation_id_for,
    record_candidate,
    record_candidate_coverage,
    start_consistency_run,
)
from okg.substrate.derived_artifacts import stable_hash

_GATE_NAME = "qualification_gate"

_GateRow = namedtuple(
    "_GateRow", "result blocks_publish reason open_observation_count details"
)


def _definition() -> ConsistencyCheckDefinition:
    return ConsistencyCheckDefinition(
        check_id="test.consistency.qualification-mapping",
        deployment_name="test",
        predicate_family="numeric_claim_conflict",
        candidate_rule_hash=stable_hash({"blocking": "subject+unit"}),
        evaluator_ref="okg.evaluator.sql.numeric_conflict.v1",
        evaluator_hash=stable_hash({"evaluator": "numeric_conflict_v1"}),
        confidence_type="deterministic_boolean",
        compute_hash=stable_hash({"candidate_sql": "claim_pairs"}),
        output_schema_hash=stable_hash({"outputs": "observations"}),
        renderer_hash=stable_hash({"renderer": "observation-json"}),
        dirty_rule_hash=stable_hash({"scope": "claim_bucket"}),
        coverage_contract={"universe": "claim_chunks", "coverage": "exact_sql"},
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


def _insert_generation(conn: psycopg.Connection, generation_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations (
              generation_id, deployment_name, status, catalog_version_id,
              published_at, correlation_id
            ) VALUES (
              %(generation_id)s, 'test', 'published', 1, now(),
              %(correlation_id)s
            )
            """,
            {
                "generation_id": generation_id,
                "correlation_id": f"test:qualification:{generation_id}",
            },
        )
    conn.commit()


def _start_run(
    conn: psycopg.Connection,
    definition: ConsistencyCheckDefinition,
    generation_id: int,
) -> tuple[str, UUID]:
    run_id = start_consistency_run(
        conn, definition, target_generation_id=generation_id
    )
    candidate_id = record_candidate(
        conn,
        definition,
        target_generation_id=generation_id,
        run_id=run_id,
        candidate=_candidate(),
    )
    return candidate_id, run_id


def _record_exact_coverage(
    conn: psycopg.Connection,
    definition: ConsistencyCheckDefinition,
    *,
    generation_id: int,
    run_id: UUID,
) -> None:
    record_candidate_coverage(
        conn,
        definition,
        target_generation_id=generation_id,
        run_id=run_id,
        report=CandidateCoverageReport(
            universe_key=f"claim_chunks:{generation_id}",
            coverage_basis="exact_postgres",
            coverage_complete=True,
            eligible_input_count=2,
            candidate_count=1,
            candidate_count_by_block={"fuel-minimum": 1},
            evaluator_invocation_count=1,
        ),
    )


def _observation_event(
    definition: ConsistencyCheckDefinition,
    candidate_id: str,
    *,
    severity: str = "high",
    finding_event: str = "observed",
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
        finding_event=finding_event,
        severity=severity,
        confidence=1,
        evidence={"spans": ["page:12:bbox:1"]},
    )


def _persisted_gate_row(conn: psycopg.Connection, run_id: UUID) -> _GateRow:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT result, blocks_publish, reason, open_observation_count,
                   details
              FROM okg.consistency_gate_results
             WHERE run_id = %(run_id)s
            """,
            {"run_id": run_id},
        )
        return _GateRow(*cur.fetchone())


def test_finding_is_mandatory_truth_table() -> None:
    # The pure derivation, independent of the DB. ``never`` is never mandatory;
    # ``any_open`` always is; ``new_findings`` is severity-gated by the floor.
    for sev in ("low", "medium", "high", None):
        assert finding_is_mandatory(block_on="never", severity=sev) is False
        assert finding_is_mandatory(block_on="any_open", severity=sev) is True
        # No floor => any severity of a new finding is mandatory.
        assert finding_is_mandatory(block_on="new_findings", severity=sev) is True

    # min_severity floor: below it is finite (not mandatory), at/above it is.
    assert (
        finding_is_mandatory(
            block_on="new_findings", severity="low", block_on_min_severity="medium"
        )
        is False
    )
    assert (
        finding_is_mandatory(
            block_on="new_findings", severity="medium", block_on_min_severity="medium"
        )
        is True
    )
    assert (
        finding_is_mandatory(
            block_on="new_findings", severity="high", block_on_min_severity="medium"
        )
        is True
    )

    # Unknown / NULL severity ranks MAX (fail-safe): never silently let through.
    assert (
        finding_is_mandatory(
            block_on="new_findings", severity=None, block_on_min_severity="high"
        )
        is True
    )
    assert (
        finding_is_mandatory(
            block_on="new_findings", severity="bogus", block_on_min_severity="high"
        )
        is True
    )

    # Case-folding matches the gate SQL's lower(severity): mixed case ranks
    # by meaning, never as unknown-MAX (the review's residual finding).
    assert (
        finding_is_mandatory(
            block_on="new_findings", severity="Low", block_on_min_severity="medium"
        )
        is False
    )
    assert (
        finding_is_mandatory(
            block_on="new_findings", severity="HIGH", block_on_min_severity="Medium"
        )
        is True
    )

    # The deployment blocking policy overrides block_on in both directions:
    # advisory (False) => nothing mandatory; forced (True) => everything is.
    assert (
        finding_is_mandatory(
            block_on="any_open", severity="high", blocks_publish=False
        )
        is False
    )
    assert (
        finding_is_mandatory(
            block_on="never", severity="low", blocks_publish=True
        )
        is True
    )

    # Backlog under new_findings: a finding the current run did NOT open is
    # not mandatory (the gate publishes over it) -- any severity. any_open
    # ignores newly-opened-ness entirely.
    assert (
        finding_is_mandatory(
            block_on="new_findings", severity="high", newly_opened=False
        )
        is False
    )
    assert (
        finding_is_mandatory(
            block_on="any_open", severity="low", newly_opened=False
        )
        is True
    )


@pytest.mark.postgres
def test_open_finding_on_blocking_check_is_mandatory_and_blocks(
    clean_runtime: str,
) -> None:
    # (a) A blocking-declared check (block_on: any_open) with one open finding
    # blocks publish -- an unresolved mandatory violation leaves the generation
    # not qualified.
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 810)
        candidate_id, run_id = _start_run(conn, definition, 810)
        _record_exact_coverage(conn, definition, generation_id=810, run_id=run_id)
        event = _observation_event(definition, candidate_id, severity="high")
        append_observation_event(
            conn,
            definition,
            target_generation_id=810,
            run_id=run_id,
            event=event,
        )

        evaluation = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=810,
            run_id=run_id,
            gate_name=_GATE_NAME,
            block_on="any_open",
        )
        row = _persisted_gate_row(conn, run_id)

    # The real gate path blocks publish (generation-blocked).
    assert evaluation.blocks_publish is True
    assert row.blocks_publish is True
    assert row.open_observation_count == 1
    # The pure facet agrees with the gate verdict (proves no drift).
    assert finding_is_mandatory(block_on="any_open", severity="high") is True
    # ...and the finding view carries the derived facet.
    sample = row.details["open_observations"]
    assert sample[0]["observation_id"] == event.observation_id
    assert sample[0]["mandatory"] is True


@pytest.mark.postgres
def test_resolving_the_mandatory_finding_unblocks_publish(
    clean_runtime: str,
) -> None:
    # (b) The same blocking check: once the finding is resolved (next generation,
    # a 'resolved' event, nothing left open), the identical gate path no longer
    # blocks -- the generation becomes qualified.
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        # Gen 820: open the finding -> mandatory -> blocks.
        _insert_generation(conn, 820)
        candidate_id, run_open = _start_run(conn, definition, 820)
        _record_exact_coverage(conn, definition, generation_id=820, run_id=run_open)
        append_observation_event(
            conn,
            definition,
            target_generation_id=820,
            run_id=run_open,
            event=_observation_event(definition, candidate_id, severity="high"),
        )
        eval_open = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=820,
            run_id=run_open,
            gate_name=_GATE_NAME,
            block_on="any_open",
        )

        # Gen 821: resolve the finding (same content-derived candidate/observation
        # id) -> nothing open -> unblocks.
        _insert_generation(conn, 821)
        candidate_id_b, run_resolved = _start_run(conn, definition, 821)
        _record_exact_coverage(
            conn, definition, generation_id=821, run_id=run_resolved
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=821,
            run_id=run_resolved,
            event=_observation_event(
                definition,
                candidate_id_b,
                severity="high",
                finding_event="resolved",
            ),
        )
        eval_resolved = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=821,
            run_id=run_resolved,
            gate_name=_GATE_NAME,
            block_on="any_open",
        )
        resolved_row = _persisted_gate_row(conn, run_resolved)

    # Before: the open mandatory finding blocked.
    assert eval_open.blocks_publish is True
    # After resolution: same gate path, nothing open, publish no longer blocked.
    assert eval_resolved.blocks_publish is False
    assert resolved_row.blocks_publish is False
    assert resolved_row.open_observation_count == 0


@pytest.mark.postgres
def test_finite_severity_finding_is_not_mandatory_and_never_blocks(
    clean_runtime: str,
) -> None:
    # (c) A finite-severity, non-blocking-declared finding never blocks under the
    # same deployment policy -- witnessed on both axes the policy distinguishes.
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        # (c1) block_on: never -- the check declares its findings advisory
        # (finite cost), so an open HIGH-severity finding does not block.
        _insert_generation(conn, 830)
        candidate_id, run_never = _start_run(conn, definition, 830)
        _record_exact_coverage(conn, definition, generation_id=830, run_id=run_never)
        append_observation_event(
            conn,
            definition,
            target_generation_id=830,
            run_id=run_never,
            event=_observation_event(definition, candidate_id, severity="high"),
        )
        eval_never = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=830,
            run_id=run_never,
            gate_name=_GATE_NAME,
            block_on="never",
        )
        never_row = _persisted_gate_row(conn, run_never)

        # (c2) block_on: new_findings, min_severity=high -- a new LOW-severity
        # finding is below the blocking floor (finite), so it does not block.
        _insert_generation(conn, 831)
        candidate_id_low, run_low = _start_run(conn, definition, 831)
        _record_exact_coverage(conn, definition, generation_id=831, run_id=run_low)
        append_observation_event(
            conn,
            definition,
            target_generation_id=831,
            run_id=run_low,
            event=_observation_event(definition, candidate_id_low, severity="low"),
        )
        eval_low = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=831,
            run_id=run_low,
            gate_name=_GATE_NAME,
            block_on="new_findings",
            block_on_min_severity="high",
        )
        low_row = _persisted_gate_row(conn, run_low)

    # (c1) never: an open finding is present but not mandatory; publish not blocked.
    assert eval_never.blocks_publish is False
    assert never_row.blocks_publish is False
    assert never_row.open_observation_count == 1
    assert finding_is_mandatory(block_on="never", severity="high") is False
    assert never_row.details["open_observations"][0]["mandatory"] is False

    # (c2) below-floor severity: finite, not mandatory; publish not blocked.
    assert eval_low.blocks_publish is False
    assert low_row.blocks_publish is False
    assert (
        finding_is_mandatory(
            block_on="new_findings", severity="low", block_on_min_severity="high"
        )
        is False
    )
    assert low_row.details["open_observations"][0]["mandatory"] is False


@pytest.mark.postgres
def test_backlog_under_new_findings_is_not_mandatory_and_does_not_block(
    clean_runtime: str,
) -> None:
    # (d) Under block_on: new_findings, ONLY findings the current run opened
    # can block. A finding opened by a PRIOR run that is still open (backlog)
    # publishes over -- and its sampled facet must say mandatory=False, in
    # agreement with the gate (the adversarial-review drift case: stamping
    # backlog mandatory while the gate publishes would be dishonest telemetry).
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        # Gen 840: the run OPENS the finding -> newly-opened -> mandatory,
        # blocks.
        _insert_generation(conn, 840)
        candidate_id, run_open = _start_run(conn, definition, 840)
        _record_exact_coverage(conn, definition, generation_id=840, run_id=run_open)
        event = _observation_event(definition, candidate_id, severity="high")
        append_observation_event(
            conn,
            definition,
            target_generation_id=840,
            run_id=run_open,
            event=event,
        )
        eval_open = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=840,
            run_id=run_open,
            gate_name=_GATE_NAME,
            block_on="new_findings",
        )
        open_row = _persisted_gate_row(conn, run_open)

        # Gen 841: the same check runs, opens NOTHING; the finding is still
        # open backlog. The gate publishes; the sampled facet agrees.
        _insert_generation(conn, 841)
        _candidate_b, run_backlog = _start_run(conn, definition, 841)
        _record_exact_coverage(
            conn, definition, generation_id=841, run_id=run_backlog
        )
        eval_backlog = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=841,
            run_id=run_backlog,
            gate_name=_GATE_NAME,
            block_on="new_findings",
        )
        backlog_row = _persisted_gate_row(conn, run_backlog)

    # Gen 840: newly-opened -> blocks, and the sampled facet says mandatory.
    assert eval_open.blocks_publish is True
    assert open_row.blocks_publish is True
    assert open_row.details["open_observations"][0]["mandatory"] is True

    # Gen 841: same open finding, not opened THIS run -> publishes, and the
    # facet agrees with the gate: not mandatory.
    assert eval_backlog.blocks_publish is False
    assert backlog_row.blocks_publish is False
    assert backlog_row.open_observation_count == 1
    sample = backlog_row.details["open_observations"]
    assert sample[0]["observation_id"] == event.observation_id
    assert sample[0]["mandatory"] is False
