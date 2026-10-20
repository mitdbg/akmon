"""Consistency publish-gate proof-slice tests."""
from __future__ import annotations

from decimal import Decimal
from uuid import UUID

import psycopg
import pytest

from okg.substrate.consistency import (
    CandidateCoverageReport,
    ConsistencyCandidate,
    ConsistencyCheckDefinition,
    ObservationEvent,
    append_observation_event,
    consistency_gate_summary,
    evaluate_consistency_publish_gate,
    observation_id_for,
    record_candidate,
    record_candidate_coverage,
    start_consistency_run,
    summarize_consistency_gate_health,
)
from okg.substrate.derived_artifacts import stable_hash


pytestmark = pytest.mark.postgres


def _definition() -> ConsistencyCheckDefinition:
    return ConsistencyCheckDefinition(
        check_id="test.consistency.publish-gate",
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
                "correlation_id": f"test:consistency_gate:{generation_id}",
            },
        )
    conn.commit()


def _start_run(
    conn: psycopg.Connection,
    definition: ConsistencyCheckDefinition,
    generation_id: int,
) -> tuple[str, UUID]:
    run_id = start_consistency_run(
        conn,
        definition,
        target_generation_id=generation_id,
    )
    candidate = _candidate()
    candidate_id = record_candidate(
        conn,
        definition,
        target_generation_id=generation_id,
        run_id=run_id,
        candidate=candidate,
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
        confidence=1,
        evidence={"spans": ["page:12:bbox:1"]},
    )


def test_exact_complete_gate_passes_without_open_observations(
    clean_runtime: str,
) -> None:
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 701)
        _, run_id = _start_run(conn, definition, 701)
        _record_exact_coverage(
            conn,
            definition,
            generation_id=701,
            run_id=run_id,
        )

        evaluation = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=701,
            run_id=run_id,
            gate_name="numeric_conflicts_gate",
            budget_ms=3000,
            elapsed_ms=12,
            preflight={"estimated_candidates": 1},
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT result, blocks_publish, reason, coverage_complete,
                       coverage_basis, open_observation_count, preflight
                  FROM okg.consistency_gate_results
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": run_id},
            )
            persisted = cur.fetchone()

    assert evaluation.result == "evaluated"
    assert evaluation.blocks_publish is False
    assert persisted == (
        "evaluated",
        False,
        "complete_no_open_observations",
        True,
        "exact_postgres",
        0,
        {"estimated_candidates": 1},
    )


def test_exact_complete_gate_fails_on_open_observations(
    clean_runtime: str,
) -> None:
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 702)
        candidate_id, run_id = _start_run(conn, definition, 702)
        _record_exact_coverage(
            conn,
            definition,
            generation_id=702,
            run_id=run_id,
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=702,
            run_id=run_id,
            event=_observation_event(definition, candidate_id),
        )

        evaluation = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=702,
            run_id=run_id,
            gate_name="numeric_conflicts_gate",
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT result, blocks_publish, reason,
                       open_observation_count, details
                  FROM okg.consistency_gate_results
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": run_id},
            )
            result, blocks_publish, reason, open_count, details = cur.fetchone()

    assert evaluation.result == "evaluated"
    assert evaluation.blocks_publish is True
    assert result == "evaluated"
    assert blocks_publish is True
    assert reason == "open_observations"
    assert open_count == 1
    assert details["open_observations"][0]["candidate_id"] == candidate_id


def test_post_publish_check_failure_does_not_block_publish(
    clean_runtime: str,
) -> None:
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 709)
        candidate_id, run_id = _start_run(conn, definition, 709)
        _record_exact_coverage(
            conn,
            definition,
            generation_id=709,
            run_id=run_id,
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=709,
            run_id=run_id,
            event=_observation_event(definition, candidate_id),
        )

        evaluation = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=709,
            run_id=run_id,
            gate_name="numeric_conflicts_post_publish",
            blocks_publish=False,
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT result, blocks_publish, reason
                  FROM okg.consistency_gate_results
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": run_id},
            )
            persisted = cur.fetchone()

    assert evaluation.result == "evaluated"
    assert evaluation.blocks_publish is False
    assert persisted == ("evaluated", False, "open_observations")


def test_incomplete_coverage_blocks_publish_gate(
    clean_runtime: str,
) -> None:
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 703)
        _, run_id = _start_run(conn, definition, 703)
        record_candidate_coverage(
            conn,
            definition,
            target_generation_id=703,
            run_id=run_id,
            report=CandidateCoverageReport(
                universe_key="claim_chunks:703",
                coverage_basis="heuristic",
                coverage_complete=False,
                eligible_input_count=1000,
                candidate_count=50,
                excluded_input_counts={"missing_embedding": 500},
                missing_embedding_count=500,
                candidate_count_by_block={"vector-top-k": 50},
            ),
        )

        evaluation = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=703,
            run_id=run_id,
            gate_name="numeric_conflicts_gate",
        )

    assert evaluation.result == "blocked_incomplete"
    assert evaluation.blocks_publish is True
    assert evaluation.coverage_complete is False
    assert evaluation.coverage_basis == "heuristic"
    assert evaluation.details["coverage"]["missing_embedding_count"] == 500


def test_timeout_and_error_results_block_and_feed_health(
    clean_runtime: str,
) -> None:
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        for generation_id, kwargs in [
            (704, {"elapsed_ms": 10}),
            (705, {"timed_out": True, "elapsed_ms": 3001}),
            (706, {"error": {"code": "evaluator_failed"}, "elapsed_ms": 80}),
            (707, {"timed_out": True, "elapsed_ms": 3002}),
        ]:
            _insert_generation(conn, generation_id)
            _, run_id = _start_run(conn, definition, generation_id)
            _record_exact_coverage(
                conn,
                definition,
                generation_id=generation_id,
                run_id=run_id,
            )
            evaluate_consistency_publish_gate(
                conn,
                definition,
                target_generation_id=generation_id,
                run_id=run_id,
                gate_name="numeric_conflicts_gate",
                budget_ms=3000,
                **kwargs,
            )

        health = summarize_consistency_gate_health(
            conn,
            check_id=definition.check_id,
            gate_name="numeric_conflicts_gate",
            lookback=4,
            max_consecutive_blocked=2,
        )

    assert health.total_runs == 4
    assert health.timeout_rate == Decimal("0.500000")
    assert health.error_rate == Decimal("0.250000")
    assert health.blocked_rate == Decimal("0.750000")
    assert health.operational_blocked_rate == Decimal("0.750000")
    assert health.consecutive_blocked == 3
    assert health.consecutive_operational_blocked == 3
    assert health.degraded is True
    assert health.degraded_reason == "consecutive_operational_blocks"


def _persist_health_row(
    conn: psycopg.Connection,
    definition: ConsistencyCheckDefinition,
    gen: int,
    *,
    kind: str,
    is_probe: bool = False,
) -> None:
    """Persist one gate-result row of a chosen shape for health-summary tests.

    kind: op_block (real operational failure) / short_circuit (backpressure skip)
    / probe_fail (a probe that re-ran and still failed) / evaluated (a clean run;
    as a passing probe when is_probe=True)."""
    run_id = start_consistency_run(
        conn, definition, target_generation_id=gen, trigger_kind="manual",
    )
    kwargs: dict = {}
    if kind == "op_block" or kind == "probe_fail":
        kwargs = {"error": {"code": "seed"}}
    elif kind == "short_circuit":
        kwargs = {"backpressure": {"health": {"degraded": True}}}
    elif kind == "evaluated":
        _record_exact_coverage(
            conn, definition, generation_id=gen, run_id=run_id,
        )
    else:  # pragma: no cover - test authoring guard
        raise AssertionError(f"unknown kind {kind!r}")
    evaluate_consistency_publish_gate(
        conn, definition, target_generation_id=gen, run_id=run_id,
        gate_name="numeric_conflicts_gate", elapsed_ms=5,
        is_probe=is_probe, **kwargs,
    )


def test_backpressure_probe_every_exceeding_lookback_is_clamped_not_latched(
    clean_runtime: str,
) -> None:
    """Review F1: short_circuits_since_probe is capped by the LIMIT lookback
    window, so a probe_every GREATER than lookback would make should_probe
    UNREACHABLE — a silent permanent latch (the exact defect this breaker
    removes). It must clamp to lookback and still probe."""
    definition = _definition()
    with psycopg.connect(clean_runtime) as conn:
        # Two real operational blocks (older) then 3 short-circuits (newest).
        _persist_health_row(conn, definition, 900, kind="op_block")
        _persist_health_row(conn, definition, 901, kind="op_block")
        for gen in (902, 903, 904):
            _persist_health_row(conn, definition, gen, kind="short_circuit")
        # probe_every=5 > lookback=3: unclamped this NEVER probes (latch).
        health = summarize_consistency_gate_health(
            conn, check_id=definition.check_id,
            gate_name="numeric_conflicts_gate",
            lookback=3, max_consecutive_blocked=1, probe_every=5,
        )
    assert health.degraded is True
    assert health.short_circuits_since_probe == 3       # capped by lookback
    assert health.probe_every == 3                      # clamped to lookback
    assert health.should_probe is True                  # fires despite 5 > 3


def test_backpressure_probe_health_distinguishes_probe_outcomes(
    clean_runtime: str,
) -> None:
    definition = _definition()
    with psycopg.connect(clean_runtime) as conn:
        # oldest -> newest: two real blocks, two short-circuits, a FAILED probe,
        # two more short-circuits.
        sequence = [
            (800, "op_block", False),
            (801, "op_block", False),
            (802, "short_circuit", False),
            (803, "probe_fail", True),
            (804, "short_circuit", False),
            (805, "short_circuit", False),
        ]
        for gen, kind, is_probe in sequence:
            _persist_health_row(conn, definition, gen, kind=kind, is_probe=is_probe)

        health = summarize_consistency_gate_health(
            conn, check_id=definition.check_id, gate_name="numeric_conflicts_gate",
            lookback=10, max_consecutive_blocked=1, probe_every=2,
        )
        # Probes are identifiable AND counted separately from ordinary runs.
        assert health.total_runs == 6
        assert health.probe_run_count == 1
        assert health.probe_block_count == 1
        assert health.probe_pass_count == 0
        # Two short-circuits since the last probe -> a probe is due (cadence).
        assert health.short_circuits_since_probe == 2
        assert health.degraded is True
        assert health.should_probe is True

        # A PASSING probe now breaks the streak and clears degradation.
        _persist_health_row(conn, definition, 806, kind="evaluated", is_probe=True)
        recovered = summarize_consistency_gate_health(
            conn, check_id=definition.check_id, gate_name="numeric_conflicts_gate",
            lookback=10, max_consecutive_blocked=1, probe_every=2,
        )

    assert recovered.probe_run_count == 2          # both probes counted
    assert recovered.probe_pass_count == 1         # split by outcome
    assert recovered.probe_block_count == 1
    assert recovered.short_circuits_since_probe == 0
    assert recovered.degraded is False             # passing probe cleared it
    assert recovered.should_probe is False


# --- consistency-blocking-policy: declarative block_on over findings ---
#
# The gate algebra must accept block_on: any_open (default) | new_findings
# (optionally min_severity) | never. any_open reproduces the historical
# "every non-pass result of a blocking gate blocks" contract byte-for-byte;
# new_findings blocks only when the run OPENS an observation this generation
# (a finding_event='observed' row at target_generation_id — the
# bounded-event-growth invariant suppresses the re-observed event for a
# still-open unchanged finding, so a pre-existing backlog emits none).


def _variant_observation_event(
    definition: ConsistencyCheckDefinition,
    candidate_id: str,
    *,
    variant: str,
    finding_event: str = "observed",
    severity: str = "high",
) -> ObservationEvent:
    """An observation event for a distinct finding within one candidate.

    ``variant`` distinguishes observation ids (each variant is its own
    finding) so a test can open, e.g., a low-severity and a high-severity
    finding independently.
    """

    participants = {
        "left": f"claim:{variant}:pub-a",
        "right": f"claim:{variant}:pub-b",
    }
    observation_id = observation_id_for(
        definition,
        candidate_id=candidate_id,
        input_scope="bucket:fuel-minimum",
        participant_roles=participants,
        normalized_predicate=f"bingo_fuel_minimum_lb_equal:{variant}",
        normalized_values={"left": 2000, "right": 1500},
        source_revision_policy="stable_source_anchor_v1",
    )
    return ObservationEvent(
        observation_id=observation_id,
        candidate_id=candidate_id,
        input_scope="bucket:fuel-minimum",
        predicate_key="numeric_equal_under_condition",
        normalized_predicate=f"bingo_fuel_minimum_lb_equal:{variant}",
        normalized_values={"left": 2000, "right": 1500},
        participant_roles=participants,
        finding_event=finding_event,
        severity=severity,
        confidence=1,
        evidence={"spans": ["page:12:bbox:1"]},
    )


def _run_and_cover(
    conn: psycopg.Connection,
    definition: ConsistencyCheckDefinition,
    generation_id: int,
) -> UUID:
    """Start a run for ``generation_id`` and record exact-complete coverage."""

    _insert_generation(conn, generation_id)
    _, run_id = _start_run(conn, definition, generation_id)
    _record_exact_coverage(
        conn,
        definition,
        generation_id=generation_id,
        run_id=run_id,
    )
    return run_id


def _persisted_gate_row(conn: psycopg.Connection, run_id: UUID) -> tuple:
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
        return cur.fetchone()


def test_block_on_any_open_reproduces_fail_and_block_byte_for_byte(
    clean_runtime: str,
) -> None:
    # The reference scenario from
    # test_exact_complete_gate_fails_on_open_observations, re-evaluated with
    # block_on explicitly defaulted/any_open: result and blocks_publish must
    # be identical (open findings => result='evaluated', blocks_publish=True;
    # finding presence is in open_observation_count, not result).
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        run_id = _run_and_cover(conn, definition, 720)
        # _run_and_cover already recorded this candidate via _start_run;
        # re-record (idempotent) to recover its candidate_id for the event.
        candidate = _candidate()
        candidate_id = record_candidate(
            conn,
            definition,
            target_generation_id=720,
            run_id=run_id,
            candidate=candidate,
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=720,
            run_id=run_id,
            event=_observation_event(definition, candidate_id),
        )

        evaluation = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=720,
            run_id=run_id,
            gate_name="numeric_conflicts_gate",
            block_on="any_open",
        )

        result, blocks_publish, reason, open_count, details = _persisted_gate_row(
            conn, run_id
        )

    assert evaluation.result == "evaluated"
    assert evaluation.blocks_publish is True
    assert result == "evaluated"
    assert blocks_publish is True
    assert reason == "open_observations"
    assert open_count == 1
    # any_open is auditable in details but the decision matches the legacy path.
    assert details["block_on"] == "any_open"
    assert "new_findings" not in details


def test_block_on_new_findings_passes_pre_existing_backlog(
    clean_runtime: str,
) -> None:
    # (2a) A finding opened in a PRIOR generation, still open, unchanged this
    # generation (no new 'observed' event) => result='evaluated' (ran cleanly;
    # the open backlog is in open_observation_count) but blocks_publish=False
    # under new_findings: the backlog does not block.
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        # Generation 730: open the finding.
        run_a = _run_and_cover(conn, definition, 730)
        candidate = _candidate()
        candidate_id = record_candidate(
            conn,
            definition,
            target_generation_id=730,
            run_id=run_a,
            candidate=candidate,
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=730,
            run_id=run_a,
            event=_observation_event(definition, candidate_id),
        )

        # Generation 731: finding still open, NO new observed event this gen.
        run_b = _run_and_cover(conn, definition, 731)
        evaluation = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=731,
            run_id=run_b,
            gate_name="numeric_conflicts_gate",
            block_on="new_findings",
        )

        result, blocks_publish, reason, open_count, details = _persisted_gate_row(
            conn, run_b
        )

    # Cleanly evaluated with an open backlog: result is 'evaluated' (health is
    # decoupled from findings; the open backlog lives in open_observation_count),
    # and the policy does not block.
    assert evaluation.result == "evaluated"
    assert evaluation.blocks_publish is False
    assert result == "evaluated"
    assert blocks_publish is False
    assert open_count == 1
    assert details["block_on"] == "new_findings"
    assert details["new_findings_count"] == 0
    assert "new_findings" not in details


def test_block_on_new_findings_blocks_and_names_a_new_observation(
    clean_runtime: str,
) -> None:
    # (2b) A run that OPENS a new observation this generation blocks, and the
    # newly-opened finding is named in details['new_findings'].
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        run_id = _run_and_cover(conn, definition, 740)
        candidate = _candidate()
        candidate_id = record_candidate(
            conn,
            definition,
            target_generation_id=740,
            run_id=run_id,
            candidate=candidate,
        )
        event = _observation_event(definition, candidate_id)
        append_observation_event(
            conn,
            definition,
            target_generation_id=740,
            run_id=run_id,
            event=event,
        )

        evaluation = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=740,
            run_id=run_id,
            gate_name="numeric_conflicts_gate",
            block_on="new_findings",
        )

        result, blocks_publish, reason, open_count, details = _persisted_gate_row(
            conn, run_id
        )

    assert evaluation.result == "evaluated"
    assert evaluation.blocks_publish is True
    assert result == "evaluated"
    assert blocks_publish is True
    assert open_count == 1
    assert details["block_on"] == "new_findings"
    assert details["new_findings_count"] == 1
    named = {row["observation_id"] for row in details["new_findings"]}
    assert event.observation_id in named


def test_block_on_new_findings_unblocks_after_resolution(
    clean_runtime: str,
) -> None:
    # (2c) After the observation resolves (next gen, no new 'observed'),
    # blocks_publish=False and the finding is no longer open.
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        # Gen 750: open.
        run_a = _run_and_cover(conn, definition, 750)
        candidate = _candidate()
        candidate_id = record_candidate(
            conn,
            definition,
            target_generation_id=750,
            run_id=run_a,
            candidate=candidate,
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=750,
            run_id=run_a,
            event=_observation_event(definition, candidate_id),
        )

        # Gen 751: resolve the finding (a 'resolved' event, not 'observed').
        run_b = _run_and_cover(conn, definition, 751)
        candidate_id_b = record_candidate(
            conn,
            definition,
            target_generation_id=751,
            run_id=run_b,
            candidate=candidate,
        )
        resolved = _observation_event(definition, candidate_id_b)
        resolved = ObservationEvent(
            observation_id=resolved.observation_id,
            candidate_id=resolved.candidate_id,
            input_scope=resolved.input_scope,
            predicate_key=resolved.predicate_key,
            normalized_predicate=resolved.normalized_predicate,
            normalized_values=resolved.normalized_values,
            participant_roles=resolved.participant_roles,
            finding_event="resolved",
            severity=resolved.severity,
            confidence=resolved.confidence,
            evidence=resolved.evidence,
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=751,
            run_id=run_b,
            event=resolved,
        )

        evaluation = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=751,
            run_id=run_b,
            gate_name="numeric_conflicts_gate",
            block_on="new_findings",
        )

        result, blocks_publish, reason, open_count, details = _persisted_gate_row(
            conn, run_b
        )

    assert evaluation.result == "evaluated"
    assert evaluation.blocks_publish is False
    assert result == "evaluated"
    assert blocks_publish is False
    assert open_count == 0


def test_block_on_new_findings_min_severity_filters(
    clean_runtime: str,
) -> None:
    # (3) min_severity=medium: a NEW low-severity finding does not block; a NEW
    # high-severity finding blocks.
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        # Gen 760: open a NEW low-severity finding; min_severity=medium => no
        # block.
        run_low = _run_and_cover(conn, definition, 760)
        candidate = _candidate()
        candidate_id_low = record_candidate(
            conn,
            definition,
            target_generation_id=760,
            run_id=run_low,
            candidate=candidate,
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=760,
            run_id=run_low,
            event=_variant_observation_event(
                definition,
                candidate_id_low,
                variant="low-sev",
                severity="low",
            ),
        )
        eval_low = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=760,
            run_id=run_low,
            gate_name="numeric_conflicts_gate",
            block_on="new_findings",
            block_on_min_severity="medium",
        )

        # Gen 761: open a NEW high-severity finding; min_severity=medium =>
        # block.
        run_high = _run_and_cover(conn, definition, 761)
        candidate_id_high = record_candidate(
            conn,
            definition,
            target_generation_id=761,
            run_id=run_high,
            candidate=candidate,
        )
        high_event = _variant_observation_event(
            definition,
            candidate_id_high,
            variant="high-sev",
            severity="high",
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=761,
            run_id=run_high,
            event=high_event,
        )
        eval_high = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=761,
            run_id=run_high,
            gate_name="numeric_conflicts_gate",
            block_on="new_findings",
            block_on_min_severity="medium",
        )

        low_row = _persisted_gate_row(conn, run_low)
        high_row = _persisted_gate_row(conn, run_high)

    # Low-severity new finding: cleanly 'evaluated' (it is open, so
    # open_observation_count > 0) but the severity floor keeps the policy from
    # blocking.
    assert eval_low.result == "evaluated"
    assert eval_low.blocks_publish is False
    assert low_row[1] is False
    assert low_row[4]["block_on_min_severity"] == "medium"
    assert low_row[4]["new_findings_count"] == 0

    # High-severity new finding: blocks.
    assert eval_high.blocks_publish is True
    assert high_row[1] is True
    assert high_row[4]["new_findings_count"] == 1
    named = {row["observation_id"] for row in high_row[4]["new_findings"]}
    assert high_event.observation_id in named


def test_block_on_never_does_not_block_open_findings(
    clean_runtime: str,
) -> None:
    # (4a) block_on: never — open findings do not block a clean evaluation.
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        run_id = _run_and_cover(conn, definition, 770)
        candidate = _candidate()
        candidate_id = record_candidate(
            conn,
            definition,
            target_generation_id=770,
            run_id=run_id,
            candidate=candidate,
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=770,
            run_id=run_id,
            event=_observation_event(definition, candidate_id),
        )

        evaluation = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=770,
            run_id=run_id,
            gate_name="numeric_conflicts_gate",
            block_on="never",
        )

        result, blocks_publish, reason, open_count, details = _persisted_gate_row(
            conn, run_id
        )

    assert evaluation.result == "evaluated"
    assert evaluation.blocks_publish is False
    assert result == "evaluated"
    assert blocks_publish is False
    assert open_count == 1
    assert details["block_on"] == "never"


def test_block_on_never_still_blocks_operational_failures(
    clean_runtime: str,
) -> None:
    # (4b) An errored / timed-out run STILL blocks under block_on: never — the
    # finding policy governs findings, not operational health.
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        run_err = _run_and_cover(conn, definition, 780)
        eval_err = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=780,
            run_id=run_err,
            gate_name="numeric_conflicts_gate",
            error={"code": "evaluator_failed"},
            block_on="never",
        )

        run_to = _run_and_cover(conn, definition, 781)
        eval_to = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=781,
            run_id=run_to,
            gate_name="numeric_conflicts_gate",
            budget_ms=3000,
            timed_out=True,
            elapsed_ms=3001,
            block_on="never",
        )

    assert eval_err.result == "blocked_error"
    assert eval_err.blocks_publish is True
    assert eval_to.result == "blocked_timeout"
    assert eval_to.blocks_publish is True


def test_block_on_loader_rejects_invalid_policy_and_severity() -> None:
    # (5) Loader validation: bad policy, and min_severity without new_findings,
    # and bad min_severity value, all raise a load error.
    from okg.substrate.consistency.sql_gate import (
        ConsistencyGateLoadError,
        _parse_sql_gate,
    )

    base = {
        "name": "g",
        "check_id": "c",
        "predicate_family": "pf",
        "confidence_type": "deterministic_boolean",
        "candidate_sql": "SELECT 1",
        "coverage_sql": "SELECT 1",
    }

    # Valid: defaults and shipped shapes parse.
    assert _parse_sql_gate(dict(base), where="w").block_on == "any_open"
    spec = _parse_sql_gate(
        {**base, "block_on": {"policy": "new_findings", "min_severity": "high"}},
        where="w",
    )
    assert spec.block_on == "new_findings"
    assert spec.block_on_min_severity == "high"

    with pytest.raises(ConsistencyGateLoadError):
        _parse_sql_gate({**base, "block_on": "sometimes"}, where="w")

    with pytest.raises(ConsistencyGateLoadError):
        _parse_sql_gate(
            {**base, "block_on": {"policy": "any_open", "min_severity": "low"}},
            where="w",
        )

    with pytest.raises(ConsistencyGateLoadError):
        _parse_sql_gate(
            {**base, "block_on": {"policy": "new_findings", "min_severity": "urgent"}},
            where="w",
        )


def test_blocking_policy_doc_examples_match_shipped_keys() -> None:
    from pathlib import Path

    doc = Path(__file__).resolve().parents[2] / (
        "docs/develop/architecture/consistency.md"
    )
    body = doc.read_text(encoding="utf-8")
    for token in (
        "block_on",
        "any_open",
        "new_findings",
        "never",
        "block_on_min_severity",
    ):
        assert token in body, f"doc missing shipped key/token {token!r}"
    for token in ("evaluated", "newly_opened_count", "resolved_count"):
        assert token in body, f"doc missing shipped result-field token {token!r}"
    assert "still reports `fail`" not in body, (
        "doc still claims the pre-split behavior (result reports 'fail')"
    )


# --- task.policy.result-fields: evaluation health split from finding counts ---


def test_result_is_evaluation_health_with_first_class_finding_counts(
    clean_runtime: str,
) -> None:
    # A cleanly-evaluated run with an OPEN finding is evaluation-HEALTHY
    # ('evaluated', NOT the old 'fail'); the finding is carried in the
    # first-class count columns and the block decision in blocks_publish -- none
    # of it encoded in the result string (req.policy.result-vocabulary).
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        run_id = _run_and_cover(conn, definition, 760)
        candidate_id = record_candidate(
            conn,
            definition,
            target_generation_id=760,
            run_id=run_id,
            candidate=_candidate(),
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=760,
            run_id=run_id,
            event=_observation_event(definition, candidate_id),
        )

        evaluation = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=760,
            run_id=run_id,
            gate_name="numeric_conflicts_gate",
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT result, open_observation_count,
                       newly_opened_count, resolved_count
                  FROM okg.consistency_gate_results
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": run_id},
            )
            persisted = cur.fetchone()

    # Health, not a finding-conflated verdict.
    assert evaluation.result == "evaluated"
    # Findings are first-class counts.
    assert evaluation.open_observation_count == 1
    assert evaluation.newly_opened_count == 1  # opened THIS run
    assert evaluation.resolved_count == 0
    # ...persisted to the new columns (result + all three counts).
    assert persisted == ("evaluated", 1, 1, 0)
    # The default any_open policy still blocks on the open finding -- the
    # blocking decision is decoupled from the (healthy) result.
    assert evaluation.blocks_publish is True
    # The operator summary surfaces the new fields.
    summary = consistency_gate_summary(evaluation)
    assert summary["result"] == "evaluated"
    assert summary["newly_opened_count"] == 1
    assert summary["resolved_count"] == 0


def test_backlog_has_zero_newly_opened_and_resolution_is_counted(
    clean_runtime: str,
) -> None:
    # The counts track ledger TRANSITIONS, not the result string: a finding
    # carried forward reports open>0 but newly_opened=0, and the run that
    # resolves it reports resolved=1 with open=0 -- both evaluation-healthy.
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        # Gen 770: open the finding.
        run_a = _run_and_cover(conn, definition, 770)
        candidate_id = record_candidate(
            conn,
            definition,
            target_generation_id=770,
            run_id=run_a,
            candidate=_candidate(),
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=770,
            run_id=run_a,
            event=_observation_event(definition, candidate_id),
        )
        eval_a = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=770,
            run_id=run_a,
            gate_name="numeric_conflicts_gate",
        )

        # Gen 771: resolve the finding (a 'resolved' event, no new 'observed').
        run_b = _run_and_cover(conn, definition, 771)
        candidate_id_b = record_candidate(
            conn,
            definition,
            target_generation_id=771,
            run_id=run_b,
            candidate=_candidate(),
        )
        opened = _observation_event(definition, candidate_id_b)
        resolved = ObservationEvent(
            observation_id=opened.observation_id,
            candidate_id=opened.candidate_id,
            input_scope=opened.input_scope,
            predicate_key=opened.predicate_key,
            normalized_predicate=opened.normalized_predicate,
            normalized_values=opened.normalized_values,
            participant_roles=opened.participant_roles,
            finding_event="resolved",
            severity=opened.severity,
            confidence=opened.confidence,
            evidence=opened.evidence,
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=771,
            run_id=run_b,
            event=resolved,
        )
        eval_b = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=771,
            run_id=run_b,
            gate_name="numeric_conflicts_gate",
        )

    # Run A opened the finding this run.
    assert eval_a.result == "evaluated"
    assert eval_a.newly_opened_count == 1
    assert eval_a.resolved_count == 0
    # Run B resolved it: healthy eval, nothing open, resolution counted, and
    # nothing NEWLY opened this run.
    assert eval_b.result == "evaluated"
    assert eval_b.open_observation_count == 0
    assert eval_b.newly_opened_count == 0
    assert eval_b.resolved_count == 1
    assert eval_b.blocks_publish is False


def test_operational_results_unchanged_and_counts_zero(
    clean_runtime: str,
) -> None:
    # The split does NOT touch operational health: a timed-out run still
    # persists 'blocked_timeout' and an errored run 'blocked_error', and both
    # report zero finding counts (they wrote nothing to the ledger).
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        run_t = _run_and_cover(conn, definition, 780)
        timed_out = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=780,
            run_id=run_t,
            gate_name="numeric_conflicts_gate",
            timed_out=True,
        )
        run_e = _run_and_cover(conn, definition, 781)
        errored = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=781,
            run_id=run_e,
            gate_name="numeric_conflicts_gate",
            error={"code": "evaluator_boom"},
        )

    assert timed_out.result == "blocked_timeout"
    assert timed_out.blocks_publish is True
    assert timed_out.newly_opened_count == 0
    assert timed_out.resolved_count == 0
    assert errored.result == "blocked_error"
    assert errored.blocks_publish is True
    assert errored.newly_opened_count == 0
    assert errored.resolved_count == 0


# --- task.scope.first-class-basis-and-health: scoped basis + gate health ------


def _record_scoped_exact_coverage(
    conn: psycopg.Connection,
    definition: ConsistencyCheckDefinition,
    *,
    generation_id: int,
    run_id: UUID,
    subject_count: int = 2,
) -> None:
    """Record an engine-certified scoped-exact coverage row: an EXACT basis over
    a derived subset, scope_restricted, NOT universe-complete."""

    record_candidate_coverage(
        conn,
        definition,
        target_generation_id=generation_id,
        run_id=run_id,
        report=CandidateCoverageReport(
            universe_key=f"claim_chunks:{generation_id}:scoped",
            coverage_basis="exact_postgres",
            coverage_complete=False,
            scope_restricted=True,
            scope_subject_count=subject_count,
            eligible_input_count=subject_count,
            candidate_count=1,
        ),
    )


def test_scoped_exact_run_maps_to_healthy_result(clean_runtime: str) -> None:
    # An engine-certified scoped-exact run (scoped + an exact coverage_basis,
    # NOT universe-complete) is HEALTHY -- result 'evaluated', not the old
    # 'blocked_incomplete' -- so operators can tell a healthy incremental run
    # from genuinely broken coverage. Proven across CONSECUTIVE scoped publishes,
    # with the scoped basis + subject count read from first-class columns.
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        for generation_id in (760, 761):
            _insert_generation(conn, generation_id)
            candidate_id, run_id = _start_run(conn, definition, generation_id)
            _record_scoped_exact_coverage(
                conn, definition, generation_id=generation_id, run_id=run_id
            )
            append_observation_event(
                conn,
                definition,
                target_generation_id=generation_id,
                run_id=run_id,
                event=_observation_event(definition, candidate_id),
            )

            evaluation = evaluate_consistency_publish_gate(
                conn,
                definition,
                target_generation_id=generation_id,
                run_id=run_id,
                gate_name="numeric_conflicts_gate",
            )

            # Evaluation-HEALTHY, not blocked_incomplete, though not
            # universe-complete.
            assert evaluation.result == "evaluated", (
                generation_id,
                evaluation.result,
            )
            assert evaluation.coverage_complete is False
            cov = evaluation.details["coverage"]
            assert cov["scoped"] is True
            assert cov["scope_subject_count"] == 2
            assert cov["scoped_exact"] is True
            # First-class columns the gate algebra reads directly (not payload).
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT scoped, scope_subject_count
                      FROM okg.consistency_candidate_coverage
                     WHERE run_id = %(run_id)s
                    """,
                    {"run_id": run_id},
                )
                assert cur.fetchone() == (True, 2)


def test_db_rejects_universe_complete_scoped_coverage(clean_runtime: str) -> None:
    # The DB CHECK enforces the write-boundary invariant even if the Python guard
    # (_validate_coverage) were bypassed: a scope-restricted run can never claim
    # coverage_complete. Flip coverage_complete on a persisted scoped row with a
    # raw UPDATE and confirm the DB rejects it.
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 762)
        _, run_id = _start_run(conn, definition, 762)
        _record_scoped_exact_coverage(
            conn, definition, generation_id=762, run_id=run_id
        )
        with conn.cursor() as cur, pytest.raises(psycopg.errors.CheckViolation):
            cur.execute(
                """
                UPDATE okg.consistency_candidate_coverage
                   SET coverage_complete = true
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": run_id},
            )


def test_scoped_run_with_coverage_gaps_is_not_certified_healthy(
    clean_runtime: str,
) -> None:
    # A scoped run that skipped subjects WITHIN its scope (cache-miss text /
    # embedding gaps) is scoped-INCOMPLETE, not scoped-exact: the gate must NOT
    # certify it healthy just because it is scoped with an exact basis. Guards
    # against a future scoped-with-embeddings producer tripping a false-healthy
    # (the coverage_complete=>no-missing DB CHECK does not constrain a scoped
    # run, which has coverage_complete=false).
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 763)
        _, run_id = _start_run(conn, definition, 763)
        record_candidate_coverage(
            conn,
            definition,
            target_generation_id=763,
            run_id=run_id,
            report=CandidateCoverageReport(
                universe_key="claim_chunks:763:scoped-gappy",
                coverage_basis="exact_postgres",
                coverage_complete=False,
                scope_restricted=True,
                scope_subject_count=5,
                eligible_input_count=5,
                candidate_count=1,
                missing_text_count=2,  # gaps WITHIN the scope
            ),
        )

        evaluation = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=763,
            run_id=run_id,
            gate_name="numeric_conflicts_gate",
        )

    assert evaluation.result == "blocked_incomplete"
    assert evaluation.blocks_publish is True
    assert evaluation.details["coverage"]["scoped_exact"] is False
