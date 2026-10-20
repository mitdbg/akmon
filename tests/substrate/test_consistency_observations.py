"""Consistency observation lifecycle proof-slice tests."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import psycopg
import pytest

from okg.substrate.consistency import (
    CandidateCoverageReport,
    ConsistencyCandidate,
    ConsistencyCheckDefinition,
    ObservationEvent,
    append_observation_event,
    candidate_id_for,
    observation_id_for,
    record_candidate,
    record_candidate_coverage,
    register_consistency_check,
    start_consistency_run,
)
from okg.substrate.derived_artifacts import stable_hash
from okg.substrate.mcp.run_sql import RunSqlInvalid, run_sql
from okg.substrate.mcp.session import open_session
from tests.substrate._db_helpers import ensure_published_generation


pytestmark = pytest.mark.postgres


def _definition() -> ConsistencyCheckDefinition:
    coverage_contract = {
        "universe": "claim_chunks",
        "coverage": "exact_sql",
    }
    severity_policy = {"default": "high"}
    return ConsistencyCheckDefinition(
        check_id="test.consistency.numeric-conflict",
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
        coverage_contract=coverage_contract,
        severity_policy=severity_policy,
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
        candidate_payload={"unit": "lb", "subject": "bingo_fuel"},
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
                "correlation_id": f"test:observation:{generation_id}",
            },
        )
    conn.commit()


def _observation_event(
    definition: ConsistencyCheckDefinition,
    candidate_id: str,
    *,
    finding_event: str | None = None,
    evaluation_event: str | None = None,
    coverage_complete: bool = True,
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
        finding_event=finding_event,  # type: ignore[arg-type]
        evaluation_event=evaluation_event,  # type: ignore[arg-type]
        severity="high",
        confidence=1,
        evidence={"spans": ["page:12:bbox:1", "page:9:bbox:4"]},
        payload={"message": "Bingo fuel minimums differ"},
        coverage_complete=coverage_complete,
    )


def test_migrate_creates_consistency_observation_tables(
    okg_migrated: str,
) -> None:
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT tablename FROM pg_tables
             WHERE schemaname = 'okg'
               AND tablename IN (
                 'consistency_check_definitions',
                 'consistency_candidates',
                 'consistency_candidate_coverage',
                 'observation_events',
                 'observation_current'
               )
            """
        )
        tables = {row[0] for row in cur.fetchall()}
    assert tables == {
        "consistency_check_definitions",
        "consistency_candidates",
        "consistency_candidate_coverage",
        "observation_events",
        "observation_current",
    }


def test_candidate_and_observation_ids_are_deterministic(
    clean_runtime: str,
) -> None:
    definition = _definition()
    candidate = _candidate()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 101)
        run_id = start_consistency_run(
            conn,
            definition,
            target_generation_id=101,
        )
        candidate_id = record_candidate(
            conn,
            definition,
            target_generation_id=101,
            run_id=run_id,
            candidate=candidate,
        )
        assert candidate_id == candidate_id_for(definition, candidate)

        event = _observation_event(
            definition,
            candidate_id,
            finding_event="observed",
        )
        assert event.observation_id == observation_id_for(
            definition,
            candidate_id=candidate_id,
            input_scope=event.input_scope,
            participant_roles=event.participant_roles,
            normalized_predicate=event.normalized_predicate,
            normalized_values=event.normalized_values,
            source_revision_policy="stable_source_anchor_v1",
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT candidate_id, participant_anchors
                  FROM okg.consistency_candidates
                 WHERE check_id = %(check_id)s
                   AND target_generation_id = 101
                """,
                {"check_id": definition.check_id},
            )
            row = cur.fetchone()

    assert row[0] == candidate_id
    assert row[1] == [
        {"role": "left", "anchor": "claim:fuel-minimum:pub-a"},
        {"role": "right", "anchor": "claim:fuel-minimum:pub-b"},
    ]


def test_observation_lifecycle_resolves_and_reopens(
    clean_runtime: str,
) -> None:
    definition = _definition()
    candidate = _candidate()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 201)
        _insert_generation(conn, 202)
        _insert_generation(conn, 203)
        register_consistency_check(conn, definition)
        run_201 = start_consistency_run(
            conn,
            definition,
            target_generation_id=201,
        )
        candidate_id = record_candidate(
            conn,
            definition,
            target_generation_id=201,
            run_id=run_201,
            candidate=candidate,
        )
        observed = _observation_event(
            definition,
            candidate_id,
            finding_event="observed",
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=201,
            run_id=run_201,
            event=observed,
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=201,
            run_id=run_201,
            event=_observation_event(
                definition,
                candidate_id,
                evaluation_event="evaluated",
            ),
        )

        run_202 = start_consistency_run(
            conn,
            definition,
            target_generation_id=202,
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=202,
            run_id=run_202,
            event=_observation_event(
                definition,
                candidate_id,
                finding_event="resolved",
            ),
        )

        run_203 = start_consistency_run(
            conn,
            definition,
            target_generation_id=203,
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=203,
            run_id=run_203,
            event=_observation_event(
                definition,
                candidate_id,
                finding_event="observed",
            ),
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT finding_status, evaluation_status,
                       latest_generation_id, severity, confidence
                  FROM okg.observation_current
                 WHERE observation_id = %(observation_id)s
                """,
                {"observation_id": observed.observation_id},
            )
            current = cur.fetchone()
            cur.execute(
                """
                SELECT event_seq, finding_event, evaluation_event
                  FROM okg.observation_events
                 WHERE observation_id = %(observation_id)s
                 ORDER BY target_generation_id, event_seq
                """,
                {"observation_id": observed.observation_id},
            )
            events = cur.fetchall()

    assert current == ("open", "evaluated", 203, "high", 1)
    assert events == [
        (1, "observed", None),
        (2, None, "evaluated"),
        (1, "resolved", None),
        (1, "observed", None),
    ]


def test_consistency_observations_are_exposed_through_read_view(
    clean_runtime: str,
) -> None:
    definition = _definition()
    candidate = _candidate()

    with psycopg.connect(clean_runtime) as conn:
        ensure_published_generation(conn, 211, deployment_name="test")
        run_211 = start_consistency_run(
            conn,
            definition,
            target_generation_id=211,
        )
        candidate_id = record_candidate(
            conn,
            definition,
            target_generation_id=211,
            run_id=run_211,
            candidate=candidate,
        )
        event = _observation_event(
            definition,
            candidate_id,
            finding_event="observed",
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=211,
            run_id=run_211,
            event=event,
        )

    # run_sql takes no parameters; the id is generated, never quote-bearing.
    assert "'" not in event.observation_id
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(
            conn,
            deployment_name="test",
            requested_generation=211,
        )
        out = run_sql(
            conn,
            session,
            f"""
            SELECT deployment_name, check_id, predicate_family,
                   finding_status, latest_generation_id,
                   normalized_values->>'left' AS left_value
              FROM okg.v_consistency_observations
             WHERE observation_id = '{event.observation_id}'
            """,
            row_cap=5,
        )

        with pytest.raises(RunSqlInvalid, match="observation_current"):
            run_sql(
                conn,
                session,
                "SELECT observation_id FROM okg.observation_current",
            )

    assert out["columns"] == [
        "deployment_name",
        "check_id",
        "predicate_family",
        "finding_status",
        "latest_generation_id",
        "left_value",
    ]
    assert out["rows"] == [[
        "test",
        definition.check_id,
        definition.predicate_family,
        "open",
        211,
        "2000",
    ]]


def test_incomplete_coverage_does_not_resolve_open_observation(
    clean_runtime: str,
) -> None:
    definition = _definition()
    candidate = _candidate()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 301)
        _insert_generation(conn, 302)
        run_301 = start_consistency_run(
            conn,
            definition,
            target_generation_id=301,
        )
        candidate_id = record_candidate(
            conn,
            definition,
            target_generation_id=301,
            run_id=run_301,
            candidate=candidate,
        )
        event = _observation_event(
            definition,
            candidate_id,
            finding_event="observed",
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=301,
            run_id=run_301,
            event=event,
        )

        run_302 = start_consistency_run(
            conn,
            definition,
            target_generation_id=302,
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=302,
            run_id=run_302,
            event=_observation_event(
                definition,
                candidate_id,
                evaluation_event="coverage_incomplete",
                coverage_complete=False,
            ),
        )

        with pytest.raises(ValueError, match="complete coverage"):
            append_observation_event(
                conn,
                definition,
                target_generation_id=302,
                run_id=run_302,
                event=_observation_event(
                    definition,
                    candidate_id,
                    finding_event="resolved",
                    coverage_complete=False,
                ),
            )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT finding_status, evaluation_status
                  FROM okg.observation_current
                 WHERE observation_id = %(observation_id)s
                """,
                {"observation_id": event.observation_id},
            )
            current = cur.fetchone()

    assert current == ("open", "coverage_incomplete")


def test_candidate_and_observation_run_must_match_check_and_generation(
    clean_runtime: str,
) -> None:
    definition = _definition()
    other_definition = replace(
        definition,
        check_id="test.consistency.other",
        predicate_family="other_predicate",
    )
    candidate = _candidate()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 501)
        _insert_generation(conn, 502)
        run_501 = start_consistency_run(
            conn,
            definition,
            target_generation_id=501,
        )
        other_run_501 = start_consistency_run(
            conn,
            other_definition,
            target_generation_id=501,
        )

        with pytest.raises(ValueError, match="targets generation"):
            record_candidate(
                conn,
                definition,
                target_generation_id=502,
                run_id=run_501,
                candidate=candidate,
            )

        candidate_id = record_candidate(
            conn,
            definition,
            target_generation_id=501,
            run_id=run_501,
            candidate=candidate,
        )

        with pytest.raises(ValueError, match="belongs to definition"):
            append_observation_event(
                conn,
                definition,
                target_generation_id=501,
                run_id=other_run_501,
                event=_observation_event(
                    definition,
                    candidate_id,
                    finding_event="observed",
                ),
            )


def test_candidate_coverage_records_auditable_counts(
    clean_runtime: str,
) -> None:
    definition = _definition()
    first_candidate = _candidate()
    second_candidate = replace(
        first_candidate,
        input_scope="bucket:crew-rest",
        participant_roles={
            "left": "claim:crew-rest:pub-a",
            "right": "claim:crew-rest:pub-b",
        },
        candidate_payload={"subject": "crew_rest"},
    )

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 601)
        run_id = start_consistency_run(
            conn,
            definition,
            target_generation_id=601,
        )
        record_candidate(
            conn,
            definition,
            target_generation_id=601,
            run_id=run_id,
            candidate=first_candidate,
        )
        record_candidate(
            conn,
            definition,
            target_generation_id=601,
            run_id=run_id,
            candidate=second_candidate,
        )

        record_candidate_coverage(
            conn,
            definition,
            target_generation_id=601,
            run_id=run_id,
            report=CandidateCoverageReport(
                universe_key="claim_chunks:601",
                coverage_basis="exact_postgres",
                coverage_complete=True,
                eligible_input_count=4,
                candidate_count=2,
                excluded_input_counts={"out_of_scope": 1},
                candidate_count_by_block={
                    "fuel-minimum": 1,
                    "crew-rest": 1,
                },
                evaluator_invocation_count=2,
                evaluator_cost={"sql_ms": 7},
                false_negative_audit={
                    "mode": "none",
                    "reason": "finite exact enumeration",
                },
            ),
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT universe_key, coverage_basis, coverage_complete,
                       eligible_input_count, excluded_input_counts,
                       missing_text_count, missing_embedding_count,
                       block_count, candidate_count,
                       candidate_count_by_block, largest_block_size,
                       p95_block_size, all_pairs_upper_bound,
                       reduction_ratio, evaluator_invocation_count,
                       evaluator_cost, false_negative_audit
                  FROM okg.consistency_candidate_coverage
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": run_id},
            )
            coverage = cur.fetchone()

    assert coverage == (
        "claim_chunks:601",
        "exact_postgres",
        True,
        4,
        {"out_of_scope": 1},
        0,
        0,
        2,
        2,
        {"crew-rest": 1, "fuel-minimum": 1},
        1,
        Decimal("1.0000"),
        6,
        Decimal("0.3333333333"),
        2,
        {"sql_ms": 7},
        {"mode": "none", "reason": "finite exact enumeration"},
    )


def test_heuristic_coverage_persists_blind_spots(
    clean_runtime: str,
) -> None:
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 602)
        run_id = start_consistency_run(
            conn,
            definition,
            target_generation_id=602,
        )
        record_candidate_coverage(
            conn,
            definition,
            target_generation_id=602,
            run_id=run_id,
            report=CandidateCoverageReport(
                universe_key="claim_chunks:602",
                coverage_basis="heuristic",
                coverage_complete=False,
                eligible_input_count=1000,
                candidate_count=50,
                excluded_input_counts={"missing_embedding": 500},
                missing_embedding_count=500,
                candidate_count_by_block={"vector-top-k": 50},
                evaluator_invocation_count=50,
                false_negative_audit={
                    "mode": "monitoring",
                    "sampler": "uniform",
                    "sample_size": 100,
                },
            ),
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT coverage_basis, coverage_complete,
                       excluded_input_counts, missing_embedding_count,
                       all_pairs_upper_bound, reduction_ratio,
                       false_negative_audit
                  FROM okg.consistency_candidate_coverage
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": run_id},
            )
            coverage = cur.fetchone()

    assert coverage == (
        "heuristic",
        False,
        {"missing_embedding": 500},
        500,
        499500,
        Decimal("0.0001001001"),
        {
            "mode": "monitoring",
            "sampler": "uniform",
            "sample_size": 100,
        },
    )


def test_complete_coverage_requires_exact_basis_and_no_blind_spots(
    clean_runtime: str,
) -> None:
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 603)
        run_id = start_consistency_run(
            conn,
            definition,
            target_generation_id=603,
        )

        with pytest.raises(ValueError, match="requires exact"):
            record_candidate_coverage(
                conn,
                definition,
                target_generation_id=603,
                run_id=run_id,
                report=CandidateCoverageReport(
                    universe_key="claim_chunks:603",
                    coverage_basis="heuristic",
                    coverage_complete=True,
                    eligible_input_count=10,
                    candidate_count=2,
                ),
            )

        with pytest.raises(ValueError, match="zero missing"):
            record_candidate_coverage(
                conn,
                definition,
                target_generation_id=603,
                run_id=run_id,
                report=CandidateCoverageReport(
                    universe_key="claim_chunks:603",
                    coverage_basis="exact_postgres",
                    coverage_complete=True,
                    eligible_input_count=10,
                    candidate_count=2,
                    missing_embedding_count=1,
                ),
            )


def test_scope_restricted_run_cannot_claim_universe_complete(
    clean_runtime: str,
) -> None:
    """task.scope.coverage-honesty: 'exact over my derived scope' is not
    'universe-complete'. The write boundary rejects the dishonest claim, and
    an honest scoped run persists an auditable scope_restricted signal."""
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 604)
        run_id = start_consistency_run(
            conn, definition, target_generation_id=604,
        )

        # Dishonest: exact-over-scope claiming universe-completeness.
        with pytest.raises(ValueError, match="scope-restricted run cannot"):
            record_candidate_coverage(
                conn,
                definition,
                target_generation_id=604,
                run_id=run_id,
                report=CandidateCoverageReport(
                    universe_key="claim_chunks:604:scoped",
                    coverage_basis="exact_postgres",
                    coverage_complete=True,
                    scope_restricted=True,
                    eligible_input_count=3,
                    candidate_count=1,
                ),
            )

        # Honest scoped run: exact over the derived scope, NOT complete. A
        # scope-aware runtime records the scoped signal in its own payload
        # (as ir_executor does) for auditability on the persisted row.
        record_candidate_coverage(
            conn,
            definition,
            target_generation_id=604,
            run_id=run_id,
            report=CandidateCoverageReport(
                universe_key="claim_chunks:604:scoped",
                coverage_basis="exact_postgres",
                coverage_complete=False,
                scope_restricted=True,
                eligible_input_count=3,
                candidate_count=1,
                payload={"scope_restricted": True, "scope_subject_count": 3},
            ),
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT coverage_complete,
                       payload->>'scope_restricted',
                       payload->>'scope_subject_count'
                  FROM okg.consistency_candidate_coverage
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": run_id},
            )
            row = cur.fetchone()
    # Scoped-ness + subject count are auditable on the persisted row, not
    # only via the free-form universe_key string.
    assert row == (False, "true", "3")


def test_invalid_observation_transition_is_rejected(
    clean_runtime: str,
) -> None:
    definition = _definition()
    candidate = _candidate()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 401)
        run_id = start_consistency_run(
            conn,
            definition,
            target_generation_id=401,
        )
        candidate_id = record_candidate(
            conn,
            definition,
            target_generation_id=401,
            run_id=run_id,
            candidate=candidate,
        )
        with pytest.raises(ValueError, match="invalid observation"):
            append_observation_event(
                conn,
                definition,
                target_generation_id=401,
                run_id=run_id,
                event=_observation_event(
                    definition,
                    candidate_id,
                    finding_event="resolved",
                ),
            )

        with pytest.raises(ValueError, match="exactly one"):
            append_observation_event(
                conn,
                definition,
                target_generation_id=401,
                run_id=run_id,
                event=ObservationEvent(
                    observation_id="observation:invalid",
                    candidate_id=candidate_id,
                    input_scope="bucket:fuel-minimum",
                    predicate_key="numeric_equal_under_condition",
                    normalized_predicate="bingo_fuel_minimum_lb_equal",
                    normalized_values={"left": 2000, "right": 1500},
                    participant_roles={
                        "left": "claim:fuel-minimum:pub-a",
                        "right": "claim:fuel-minimum:pub-b",
                    },
                    finding_event="observed",
                    evaluation_event="evaluated",
                ),
            )
