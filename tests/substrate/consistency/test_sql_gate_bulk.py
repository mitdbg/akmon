"""Bulk deterministic SQL consistency materialization tests."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from okg.substrate.consistency import (
    CandidateCoverageReport,
    ConsistencyCandidate,
    ConsistencyCandidateObservation,
    ConsistencyCheckDefinition,
    DeterministicSqlConsistencyGate,
    ObservationEvent,
    SqlConsistencyGateSpec,
    SqlGateScopeFrontier,
    candidate_id_for,
    evaluate_consistency_publish_gate,
    observation_id_for,
    record_candidate_coverage,
    record_candidate_observations_bulk,
    start_consistency_run,
)
from okg.substrate.derived_artifacts import stable_hash


pytestmark = pytest.mark.postgres


def _definition() -> ConsistencyCheckDefinition:
    return ConsistencyCheckDefinition(
        check_id=f"test.consistency.bulk.{uuid4().hex}",
        deployment_name="test",
        predicate_family="bulk_numeric_claim_conflict",
        candidate_rule_hash=stable_hash({"blocking": "bulk-subject"}),
        evaluator_ref="okg.evaluator.sql.bulk_numeric_conflict.v1",
        evaluator_hash=stable_hash({"evaluator": "bulk_numeric_conflict_v1"}),
        confidence_type="deterministic_boolean",
        compute_hash=stable_hash({"candidate_sql": "bulk_claim_pairs"}),
        output_schema_hash=stable_hash({"outputs": "bulk_observations"}),
        renderer_hash=stable_hash({"renderer": "observation-json"}),
        dirty_rule_hash=stable_hash({"scope": "bulk_claim_bucket"}),
    )


def _candidate(suffix: str = "fuel") -> ConsistencyCandidate:
    return ConsistencyCandidate(
        input_scope=f"bucket:{suffix}",
        blocking_rule_hash=stable_hash({"block": suffix}),
        participant_roles={
            "left": f"claim:{suffix}:a",
            "right": f"claim:{suffix}:b",
        },
        predicate_key="numeric_equal_under_condition",
        generation_policy="stable_source_anchor_v1",
        candidate_payload={"subject": suffix},
    )


def _insert_generation(
    conn: psycopg.Connection,
    generation_id: int,
    *,
    deployment_name: str = "test",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations (
              generation_id, deployment_name, status, catalog_version_id,
              published_at, correlation_id
            ) VALUES (
              %(generation_id)s, %(deployment_name)s, 'published', 1,
              now(), %(correlation_id)s
            )
            """,
            {
                "generation_id": generation_id,
                "deployment_name": deployment_name,
                "correlation_id": f"test:bulk:{generation_id}",
            },
        )
    conn.commit()


def _create_current_graph_mirror(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.chronos_graph_nodes_current (
              node_id TEXT PRIMARY KEY,
              subtype TEXT NOT NULL,
              attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
              text_hash TEXT,
              embedding_model TEXT,
              source TEXT,
              source_record_id JSONB NOT NULL DEFAULT '{}'::jsonb,
              source_revision JSONB NOT NULL DEFAULT '{}'::jsonb,
              run_id TEXT,
              observed_at TIMESTAMPTZ
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.chronos_graph_edges_current (
              edge_id BIGINT PRIMARY KEY,
              src TEXT NOT NULL,
              dst TEXT NOT NULL,
              edge_type TEXT NOT NULL,
              attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
              provenance TEXT NOT NULL DEFAULT 'explicit',
              confidence REAL NOT NULL DEFAULT 1.0,
              source TEXT,
              source_record_id JSONB NOT NULL DEFAULT '{}'::jsonb,
              source_revision JSONB NOT NULL DEFAULT '{}'::jsonb,
              run_id TEXT,
              observed_at TIMESTAMPTZ
            )
            """
        )
        cur.execute(
            """
            TRUNCATE okg.chronos_graph_nodes_current,
                     okg.chronos_graph_edges_current
            """
        )
    conn.commit()


def test_sql_gate_stages_source_watermark_scope_frontier(
    clean_runtime: str,
) -> None:
    generation_id = 9204
    source_name = f"test.frontier.{uuid4().hex}"
    spec = SqlConsistencyGateSpec(
        name="scoped_frontier_gate",
        check_id=f"test.sql.scoped-frontier.{uuid4().hex}",
        predicate_family="scoped_frontier",
        confidence_type="deterministic_boolean",
        scope_frontier=SqlGateScopeFrontier(source=source_name),
        candidate_sql="""
            SELECT
              ('scope:' || subject_id)::text AS input_scope,
              'block:scope'::text AS blocking_rule_hash,
              jsonb_build_object('page', subject_id) AS participant_roles,
              'frontier_subject_checked'::text AS predicate_key,
              'generation_live_view_v1'::text AS generation_policy,
              true AS emit_observation,
              'observed'::text AS finding_event,
              'frontier_subject_seen'::text AS normalized_predicate,
              jsonb_build_object('node_id', subject_id) AS normalized_values,
              'low'::text AS severity,
              1.0::numeric AS confidence,
              jsonb_build_object('node_id', subject_id) AS evidence,
              jsonb_build_object('scope_active', true) AS payload
              FROM pg_temp.okg_sql_gate_subject_scope
             ORDER BY subject_id
        """,
        coverage_sql="""
            SELECT
              ('scoped-frontier:' || %(target_generation_id)s::text)
                AS universe_key,
              'exact_postgres'::text AS coverage_basis,
              true AS coverage_complete,
              (SELECT subject_count
                 FROM pg_temp.okg_sql_gate_scope_context)
                AS eligible_input_count,
              (SELECT count(*)::bigint
                 FROM pg_temp.okg_sql_gate_candidate_rows)
                AS candidate_count,
              0::bigint AS evaluator_invocation_count,
              (SELECT subject_count
                 FROM pg_temp.okg_sql_gate_scope_context)
                AS all_pairs_upper_bound,
              jsonb_build_object(
                'scope_active',
                  (SELECT scope_active
                     FROM pg_temp.okg_sql_gate_scope_context),
                'subject_count',
                  (SELECT subject_count
                     FROM pg_temp.okg_sql_gate_scope_context)
              ) AS payload
        """,
        timeout_ms=3000,
    )
    gate = DeterministicSqlConsistencyGate(spec)

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.source_watermarks (
                  source, last_applied_rev, last_reconcile_at
                ) VALUES (%s, %s, now())
                """,
                (
                    source_name,
                    Jsonb({
                        "schema": "test.source_cursor.v1",
                        "content_hash": "hash:scoped",
                        "scoped": True,
                        "scoped_subject_node_ids": [
                            "subject:two",
                            "subject:one",
                            "subject:two",
                        ],
                    }),
                ),
            )
        conn.commit()

        evaluation = gate.evaluate(
            conn,
            target_generation_id=generation_id,
            catalog_version_id=1,
            deployment_name="test",
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT participant_roles->>'page'
                  FROM okg.observation_current
                 WHERE check_id = %(check_id)s
                   AND latest_generation_id = %(generation_id)s
                 ORDER BY participant_roles->>'page'
                """,
                {
                    "check_id": spec.check_id,
                    "generation_id": generation_id,
                },
            )
            pages = [row[0] for row in cur.fetchall()]
            cur.execute(
                """
                SELECT payload
                  FROM okg.consistency_candidate_coverage
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": evaluation.run_id},
            )
            coverage_payload = cur.fetchone()[0]

    scope = evaluation.preflight["scope_frontier"]
    assert scope["configured"] is True
    assert scope["scope_active"] is True
    assert scope["subject_count"] == 2
    # Scope IDENTITY, not just the count above: the frontier names WHICH
    # subjects it staged, so a reader can compute the unevaluated complement
    # (the check's subject universe minus these ids). A count of 2 cannot.
    assert scope["subject_ids"] == ["subject:one", "subject:two"], scope
    assert scope["subject_ids_truncated"] is False, scope
    assert scope["subject_digest_sha256"], scope
    assert "MINUS" in scope["scope_complement_recipe"], scope
    assert scope["reason"] == "scoped_cursor"
    assert scope["cursor_schema"] == "test.source_cursor.v1"
    assert evaluation.preflight["candidate_row_count"] == 2
    assert pages == ["subject:one", "subject:two"]
    assert coverage_payload == {
        "scope_active": True,
        "subject_count": 2,
    }


def test_sql_gate_scoped_rerun_override_uses_subject_frontier(
    clean_runtime: str,
) -> None:
    generation_id = 9207
    source_name = f"test.frontier.{uuid4().hex}"
    spec = SqlConsistencyGateSpec(
        name="override_frontier_gate",
        check_id=f"test.sql.override-frontier.{uuid4().hex}",
        predicate_family="scoped_frontier",
        confidence_type="deterministic_boolean",
        scope_frontier=SqlGateScopeFrontier(source=source_name),
        candidate_sql="""
            SELECT
              ('scope:' || subject_id)::text AS input_scope,
              'block:scope'::text AS blocking_rule_hash,
              jsonb_build_object('page', subject_id) AS participant_roles,
              'frontier_subject_checked'::text AS predicate_key,
              'generation_live_view_v1'::text AS generation_policy,
              true AS emit_observation,
              'observed'::text AS finding_event,
              'frontier_subject_seen'::text AS normalized_predicate,
              jsonb_build_object('node_id', subject_id) AS normalized_values,
              'low'::text AS severity,
              1.0::numeric AS confidence,
              jsonb_build_object('node_id', subject_id) AS evidence,
              jsonb_build_object('scope_active', true) AS payload
              FROM pg_temp.okg_sql_gate_subject_scope
             ORDER BY subject_id
        """,
        coverage_sql="""
            SELECT
              ('override-frontier:' || %(target_generation_id)s::text)
                AS universe_key,
              'exact_postgres'::text AS coverage_basis,
              true AS coverage_complete,
              (SELECT subject_count
                 FROM pg_temp.okg_sql_gate_scope_context)
                AS eligible_input_count,
              (SELECT count(*)::bigint
                 FROM pg_temp.okg_sql_gate_candidate_rows)
                AS candidate_count,
              0::bigint AS evaluator_invocation_count,
              (SELECT subject_count
                 FROM pg_temp.okg_sql_gate_scope_context)
                AS all_pairs_upper_bound,
              jsonb_build_object(
                'scope_active',
                  (SELECT scope_active
                     FROM pg_temp.okg_sql_gate_scope_context),
                'subject_count',
                  (SELECT subject_count
                     FROM pg_temp.okg_sql_gate_scope_context),
                'reason',
                  (SELECT reason
                     FROM pg_temp.okg_sql_gate_scope_context)
              ) AS payload
        """,
        timeout_ms=3000,
    )
    gate = DeterministicSqlConsistencyGate(spec)

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        evaluation = gate.evaluate(
            conn,
            target_generation_id=generation_id,
            catalog_version_id=1,
            deployment_name="test",
            scope_subject_ids=[
                "subject:two",
                "subject:one",
                "subject:two",
            ],
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT participant_roles->>'page'
                  FROM okg.observation_current
                 WHERE check_id = %(check_id)s
                   AND latest_generation_id = %(generation_id)s
                 ORDER BY participant_roles->>'page'
                """,
                {
                    "check_id": spec.check_id,
                    "generation_id": generation_id,
                },
            )
            pages = [row[0] for row in cur.fetchall()]
            cur.execute(
                """
                SELECT payload
                  FROM okg.consistency_candidate_coverage
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": evaluation.run_id},
            )
            coverage_payload = cur.fetchone()[0]

    scope = evaluation.preflight["scope_frontier"]
    assert scope["scope_active"] is True
    assert scope["reason"] == "scope_override"
    assert scope["cursor_schema"] == "okg.sql_gate.scope_override.v1"
    assert scope["subject_count"] == 2
    assert evaluation.preflight["candidate_row_count"] == 2
    assert pages == ["subject:one", "subject:two"]
    assert coverage_payload == {
        "scope_active": True,
        "subject_count": 2,
        "reason": "scope_override",
    }


def test_sql_gate_stages_scoped_graph_frontier_relations(
    clean_runtime: str,
) -> None:
    generation_id = 9206
    source_name = f"test.frontier.{uuid4().hex}"
    spec = SqlConsistencyGateSpec(
        name="scoped_graph_frontier_gate",
        check_id=f"test.sql.scoped-graph-frontier.{uuid4().hex}",
        predicate_family="scoped_graph_frontier",
        confidence_type="deterministic_boolean",
        scope_frontier=SqlGateScopeFrontier(source=source_name),
        candidate_sql="""
            SELECT
              ('scope:' || n.node_id)::text AS input_scope,
              'block:scope-graph'::text AS blocking_rule_hash,
              jsonb_build_object('page', n.node_id) AS participant_roles,
              'scoped_graph_frontier_visible'::text AS predicate_key,
              'generation_live_view_v1'::text AS generation_policy,
              true AS emit_observation,
              'observed'::text AS finding_event,
              'scoped_graph_frontier_visible'::text
                AS normalized_predicate,
              jsonb_build_object(
                'node_id', n.node_id,
                'out_edge_count', count(DISTINCT out_e.edge_id),
                'in_edge_count', count(DISTINCT in_e.edge_id)
              ) AS normalized_values,
              'low'::text AS severity,
              1.0::numeric AS confidence,
              jsonb_build_object(
                'node_id', n.node_id,
                'out_edge_count', count(DISTINCT out_e.edge_id),
                'in_edge_count', count(DISTINCT in_e.edge_id)
              ) AS evidence,
              '{}'::jsonb AS payload
              FROM pg_temp.okg_sql_gate_scoped_nodes n
              LEFT JOIN pg_temp.okg_sql_gate_scoped_out_edges out_e
                ON out_e.src = n.node_id
              LEFT JOIN pg_temp.okg_sql_gate_scoped_in_edges in_e
                ON in_e.dst = n.node_id
             GROUP BY n.node_id
        """,
        coverage_sql="""
            SELECT
              ('scoped-graph-frontier:' ||
                %(target_generation_id)s::text) AS universe_key,
              'exact_postgres'::text AS coverage_basis,
              true AS coverage_complete,
              (SELECT scoped_node_count
                 FROM pg_temp.okg_sql_gate_scope_context)
                AS eligible_input_count,
              (SELECT count(*)::bigint
                 FROM pg_temp.okg_sql_gate_candidate_rows)
                AS candidate_count,
              0::bigint AS evaluator_invocation_count,
              (SELECT scoped_node_count
                 FROM pg_temp.okg_sql_gate_scope_context)
                AS all_pairs_upper_bound,
              jsonb_build_object(
                'scope_active',
                  (SELECT scope_active
                     FROM pg_temp.okg_sql_gate_scope_context),
                'subject_count',
                  (SELECT subject_count
                     FROM pg_temp.okg_sql_gate_scope_context),
                'scoped_node_count',
                  (SELECT scoped_node_count
                     FROM pg_temp.okg_sql_gate_scope_context),
                'scoped_out_edge_count',
                  (SELECT scoped_out_edge_count
                     FROM pg_temp.okg_sql_gate_scope_context),
                'scoped_in_edge_count',
                  (SELECT scoped_in_edge_count
                     FROM pg_temp.okg_sql_gate_scope_context)
              ) AS payload
        """,
        timeout_ms=3000,
    )
    gate = DeterministicSqlConsistencyGate(spec)

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _create_current_graph_mirror(conn)
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO okg.chronos_graph_nodes_current (
                  node_id, subtype, attrs
                ) VALUES (%s, %s, %s)
                """,
                [
                    (
                        "doc:one",
                        "document",
                        Jsonb({"source_family": "test"}),
                    ),
                    (
                        "doc:two",
                        "document",
                        Jsonb({"source_family": "test"}),
                    ),
                ],
            )
            cur.executemany(
                """
                INSERT INTO okg.chronos_graph_edges_current (
                  edge_id, src, dst, edge_type
                ) VALUES (%s, %s, %s, %s)
                """,
                [
                    (1, "doc:one", "doc:target", "topic_parent"),
                    (2, "doc:two", "doc:target", "topic_parent"),
                    (3, "doc:ref", "doc:one", "links_to"),
                ],
            )
            cur.execute(
                """
                INSERT INTO okg.source_watermarks (
                  source, last_applied_rev, last_reconcile_at
                ) VALUES (%s, %s, now())
                """,
                (
                    source_name,
                    Jsonb({
                        "schema": "test.source_cursor.v1",
                        "content_hash": "hash:scoped-graph",
                        "scoped": True,
                        "scoped_subject_node_ids": ["doc:one"],
                    }),
                ),
            )
        conn.commit()

        evaluation = gate.evaluate(
            conn,
            target_generation_id=generation_id,
            catalog_version_id=1,
            deployment_name="test",
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT participant_roles->>'page', evidence
                  FROM okg.observation_current
                 WHERE check_id = %(check_id)s
                   AND latest_generation_id = %(generation_id)s
                """,
                {
                    "check_id": spec.check_id,
                    "generation_id": generation_id,
                },
            )
            observation_page, evidence = cur.fetchone()
            cur.execute(
                """
                SELECT payload
                  FROM okg.consistency_candidate_coverage
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": evaluation.run_id},
            )
            coverage_payload = cur.fetchone()[0]

    scope = evaluation.preflight["scope_frontier"]
    assert scope["graph_frontier_available"] is True
    assert scope["subject_count"] == 1
    assert scope["scoped_node_count"] == 1
    assert scope["scoped_out_edge_count"] == 1
    assert scope["scoped_in_edge_count"] == 1
    assert evaluation.preflight["candidate_row_count"] == 1
    assert observation_page == "doc:one"
    assert evidence == {
        "node_id": "doc:one",
        "out_edge_count": 1,
        "in_edge_count": 1,
    }
    assert coverage_payload == {
        "scope_active": True,
        "subject_count": 1,
        "scoped_node_count": 1,
        "scoped_out_edge_count": 1,
        "scoped_in_edge_count": 1,
    }


def test_sql_gate_scope_frontier_falls_back_for_unscoped_cursor(
    clean_runtime: str,
) -> None:
    generation_id = 9205
    source_name = f"test.frontier.{uuid4().hex}"
    spec = SqlConsistencyGateSpec(
        name="unscoped_frontier_gate",
        check_id=f"test.sql.unscoped-frontier.{uuid4().hex}",
        predicate_family="scoped_frontier",
        confidence_type="deterministic_boolean",
        scope_frontier=SqlGateScopeFrontier(source=source_name),
        candidate_sql="""
            WITH scope_context AS (
              SELECT scope_active
                FROM pg_temp.okg_sql_gate_scope_context
            ),
            subjects AS (
              SELECT subject_id
                FROM pg_temp.okg_sql_gate_subject_scope
               WHERE (SELECT scope_active FROM scope_context)
              UNION ALL
              SELECT subject_id
                FROM (VALUES ('full:one'), ('full:two')) AS full_rows(subject_id)
               WHERE NOT (SELECT scope_active FROM scope_context)
            )
            SELECT
              ('scope:' || subject_id)::text AS input_scope,
              'block:scope'::text AS blocking_rule_hash,
              jsonb_build_object('page', subject_id) AS participant_roles,
              'frontier_subject_checked'::text AS predicate_key,
              'generation_live_view_v1'::text AS generation_policy,
              true AS emit_observation,
              'observed'::text AS finding_event,
              'frontier_subject_seen'::text AS normalized_predicate,
              jsonb_build_object('node_id', subject_id) AS normalized_values,
              'low'::text AS severity,
              1.0::numeric AS confidence,
              '{}'::jsonb AS evidence,
              '{}'::jsonb AS payload
              FROM subjects
             ORDER BY subject_id
        """,
        coverage_sql="""
            SELECT
              ('unscoped-frontier:' || %(target_generation_id)s::text)
                AS universe_key,
              'exact_postgres'::text AS coverage_basis,
              true AS coverage_complete,
              2::bigint AS eligible_input_count,
              (SELECT count(*)::bigint
                 FROM pg_temp.okg_sql_gate_candidate_rows)
                AS candidate_count,
              0::bigint AS evaluator_invocation_count,
              2::bigint AS all_pairs_upper_bound
        """,
        timeout_ms=3000,
    )
    gate = DeterministicSqlConsistencyGate(spec)

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.source_watermarks (
                  source, last_applied_rev, last_reconcile_at
                ) VALUES (%s, %s, now())
                """,
                (
                    source_name,
                    Jsonb({
                        "schema": "test.source_cursor.v1",
                        "content_hash": "hash:full",
                        "scoped": False,
                        "scoped_subject_node_ids": ["subject:scoped"],
                    }),
                ),
            )
        conn.commit()

        evaluation = gate.evaluate(
            conn,
            target_generation_id=generation_id,
            catalog_version_id=1,
            deployment_name="test",
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT participant_roles->>'page'
                  FROM okg.observation_current
                 WHERE check_id = %(check_id)s
                   AND latest_generation_id = %(generation_id)s
                 ORDER BY participant_roles->>'page'
                """,
                {
                    "check_id": spec.check_id,
                    "generation_id": generation_id,
                },
            )
            pages = [row[0] for row in cur.fetchall()]

    scope = evaluation.preflight["scope_frontier"]
    assert scope["configured"] is True
    assert scope["scope_active"] is False
    assert scope["subject_count"] == 0
    assert scope["reason"] == "cursor_not_scoped"
    assert evaluation.preflight["candidate_row_count"] == 2
    assert pages == ["full:one", "full:two"]


def _event(
    definition: ConsistencyCheckDefinition,
    candidate: ConsistencyCandidate,
    candidate_id: str,
    *,
    finding_event: str | None = None,
    evaluation_event: str | None = None,
) -> ObservationEvent:
    observation_id = observation_id_for(
        definition,
        candidate_id=candidate_id,
        input_scope=candidate.input_scope,
        participant_roles=candidate.participant_roles,
        normalized_predicate="numeric_claims_match",
        normalized_values={"left": 2000, "right": 1500},
        source_revision_policy=candidate.generation_policy,
    )
    return ObservationEvent(
        observation_id=observation_id,
        candidate_id=candidate_id,
        input_scope=candidate.input_scope,
        predicate_key=candidate.predicate_key,
        normalized_predicate="numeric_claims_match",
        normalized_values={"left": 2000, "right": 1500},
        participant_roles=candidate.participant_roles,
        finding_event=finding_event,  # type: ignore[arg-type]
        evaluation_event=evaluation_event,  # type: ignore[arg-type]
        severity="high",
        confidence=1,
        evidence={"spans": ["page:12"]},
        payload={"message": "claim values differ"},
    )


def test_new_findings_backlog_passes_through_real_growth_guard(
    clean_runtime: str,
) -> None:
    """block_on: new_findings relies on the bounded-event-growth guard end to
    end: an unchanged open backlog re-materialized through the REAL bulk path
    (record_candidate_observations_bulk, which carries the growth guard) emits
    NO 'observed' event this run, so _newly_opened_this_run (run_id-scoped)
    returns 0 and the gate does not block. This exercises the production
    write path, not the unguarded single-event append the other tests use."""
    definition = _definition()
    candidate = _candidate("backlog")
    cid = candidate_id_for(definition, candidate)

    def _materialize(conn: psycopg.Connection, gen: int, run_id) -> None:
        record_candidate_observations_bulk(
            conn, definition, target_generation_id=gen, run_id=run_id,
            records=[ConsistencyCandidateObservation(
                candidate=candidate, candidate_id=cid,
                event=_event(definition, candidate, cid,
                             finding_event="observed"))],
        )

    def _cover(conn: psycopg.Connection, gen: int, run_id) -> None:
        record_candidate_coverage(
            conn, definition, target_generation_id=gen, run_id=run_id,
            report=CandidateCoverageReport(
                universe_key=f"backlog:{gen}", coverage_basis="exact_postgres",
                coverage_complete=True, eligible_input_count=1,
                candidate_count=1),
        )

    with psycopg.connect(clean_runtime) as conn:
        for g in (9201, 9202):
            _insert_generation(conn, g)
        # gen 9201: open the finding — run 1 writes its 'observed' event.
        run1 = start_consistency_run(conn, definition, target_generation_id=9201)
        _materialize(conn, 9201, run1)
        _cover(conn, 9201, run1)
        # gen 9202: re-materialize the SAME unchanged finding through the real
        # bulk path — the growth guard suppresses the re-'observed' event.
        run2 = start_consistency_run(conn, definition, target_generation_id=9202)
        _materialize(conn, 9202, run2)
        _cover(conn, 9202, run2)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.observation_events "
                "WHERE run_id = %s AND finding_event = 'observed'",
                (run2,),
            )
            assert cur.fetchone()[0] == 0, (
                "growth guard must suppress the unchanged re-observed event"
            )
        ev = evaluate_consistency_publish_gate(
            conn, definition, target_generation_id=9202, run_id=run2,
            blocks_publish=None, block_on="new_findings",
        )
        # The backlog is still open (result='evaluated'; finding presence is in
        # open_observation_count, not result), but new_findings does not block
        # it — the run opened nothing new.
        assert ev.result == "evaluated"
        assert ev.blocks_publish is False
        assert ev.details.get("new_findings_count") == 0


def test_bulk_records_candidates_events_and_current_lifecycle(
    clean_runtime: str,
) -> None:
    definition = _definition()
    candidate = _candidate("fuel")
    candidate_id = candidate_id_for(definition, candidate)
    other_candidate = _candidate("crew")
    other_candidate_id = candidate_id_for(definition, other_candidate)

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 9101)
        _insert_generation(conn, 9102)
        run_9101 = start_consistency_run(
            conn,
            definition,
            target_generation_id=9101,
        )

        observation_count = record_candidate_observations_bulk(
            conn,
            definition,
            target_generation_id=9101,
            run_id=run_9101,
            records=[
                ConsistencyCandidateObservation(
                    candidate=candidate,
                    candidate_id=candidate_id,
                    event=_event(
                        definition,
                        candidate,
                        candidate_id,
                        finding_event="observed",
                    ),
                ),
                ConsistencyCandidateObservation(
                    candidate=candidate,
                    candidate_id=candidate_id,
                    event=_event(
                        definition,
                        candidate,
                        candidate_id,
                        evaluation_event="evaluated",
                    ),
                ),
                ConsistencyCandidateObservation(
                    candidate=other_candidate,
                    candidate_id=other_candidate_id,
                ),
            ],
        )
        assert observation_count == 2

        run_9102 = start_consistency_run(
            conn,
            definition,
            target_generation_id=9102,
        )
        record_candidate_observations_bulk(
            conn,
            definition,
            target_generation_id=9102,
            run_id=run_9102,
            records=[
                ConsistencyCandidateObservation(
                    candidate=candidate,
                    candidate_id=candidate_id,
                    event=_event(
                        definition,
                        candidate,
                        candidate_id,
                        finding_event="resolved",
                    ),
                )
            ],
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.consistency_candidates
                 WHERE check_id = %(check_id)s
                   AND target_generation_id = 9101
                """,
                {"check_id": definition.check_id},
            )
            candidate_count = cur.fetchone()[0]
            cur.execute(
                """
                SELECT event_seq, finding_event, evaluation_event
                  FROM okg.observation_events
                 WHERE observation_id = %(observation_id)s
                 ORDER BY target_generation_id, event_seq
                """,
                {
                    "observation_id": _event(
                        definition,
                        candidate,
                        candidate_id,
                        finding_event="observed",
                    ).observation_id
                },
            )
            events = cur.fetchall()
            cur.execute(
                """
                SELECT finding_status, evaluation_status,
                       latest_generation_id, severity, confidence,
                       evidence, payload
                  FROM okg.observation_current
                 WHERE observation_id = %(observation_id)s
                """,
                {
                    "observation_id": _event(
                        definition,
                        candidate,
                        candidate_id,
                        finding_event="observed",
                    ).observation_id
                },
            )
            current = cur.fetchone()

    assert candidate_count == 2
    assert events == [
        (1, "observed", None),
        (2, None, "evaluated"),
        (1, "resolved", None),
    ]
    assert current == (
        "resolved",
        "evaluated",
        9102,
        "high",
        Decimal("1.000000"),
        {"spans": ["page:12"]},
        {"message": "claim values differ"},
    )


def test_sql_gate_uses_bulk_materialization_path(clean_runtime: str) -> None:
    generation_id = 9201
    spec = SqlConsistencyGateSpec(
        name="bulk_sql_gate",
        check_id=f"test.sql.bulk.{uuid4().hex}",
        predicate_family="bulk_sql_conflict",
        confidence_type="deterministic_boolean",
        candidate_sql="""
            SELECT
              ('bucket:' || item.id)::text AS input_scope,
              ('block:' || item.id)::text AS blocking_rule_hash,
              jsonb_build_object(
                'left', 'claim:' || item.id || ':a',
                'right', 'claim:' || item.id || ':b'
              ) AS participant_roles,
              'numeric_equal_under_condition'::text AS predicate_key,
              'stable_source_anchor_v1'::text AS generation_policy,
              true AS emit_observation,
              'numeric_claims_match'::text AS normalized_predicate,
              jsonb_build_object('left', item.left_value, 'right', 1500)
                AS normalized_values,
              'high'::text AS severity,
              1.0::numeric AS confidence,
              jsonb_build_object('span', item.id) AS evidence,
              jsonb_build_object('message', 'values differ') AS payload
              FROM (VALUES ('fuel', 2000), ('crew', 1700))
                AS item(id, left_value)
        """,
        coverage_sql="""
            SELECT
              ('bulk-sql:' || %(target_generation_id)s::text)
                AS universe_key,
              'exact_postgres'::text AS coverage_basis,
              true AS coverage_complete,
              2::bigint AS eligible_input_count,
              2::bigint AS candidate_count,
              1::bigint AS all_pairs_upper_bound,
              2::bigint AS evaluator_invocation_count
        """,
        timeout_ms=3000,
        materialization_chunk_size=1,
    )
    gate = DeterministicSqlConsistencyGate(spec)

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        evaluation = gate.evaluate(
            conn,
            target_generation_id=generation_id,
            catalog_version_id=1,
            deployment_name="test",
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.observation_current
                 WHERE check_id = %(check_id)s
                   AND latest_generation_id = %(generation_id)s
                   AND finding_status = 'open'
                """,
                {
                    "check_id": spec.check_id,
                    "generation_id": generation_id,
                },
            )
            open_count = cur.fetchone()[0]
            cur.execute(
                """
                SELECT event_seq
                  FROM okg.observation_events
                 WHERE check_id = %(check_id)s
                   AND target_generation_id = %(generation_id)s
                 ORDER BY event_seq
                """,
                {
                    "check_id": spec.check_id,
                    "generation_id": generation_id,
                },
            )
            event_seqs = [row[0] for row in cur.fetchall()]
            cur.execute(
                """
                SELECT metrics->'phase_timings_ms'
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": evaluation.run_id},
            )
            run_phase_timings = cur.fetchone()[0]

    assert evaluation.result == "evaluated"
    assert evaluation.open_observation_count == 2
    assert evaluation.details["coverage"]["candidate_count"] == 2
    assert open_count == 2
    assert event_seqs == [1, 2]
    expected_preflight_phases = {
        "stage_candidate_rows_ms",
        "materialize_chunked_python_ms",
        "coverage_sql_ms",
        "record_coverage_ms",
        "total_elapsed_ms",
    }
    expected_run_phases = expected_preflight_phases | {
        "evaluate_publish_gate_ms",
        "complete_run_ms",
    }
    preflight_phase_timings = evaluation.preflight["phase_timings_ms"]
    assert expected_preflight_phases <= set(preflight_phase_timings)
    assert expected_run_phases <= set(run_phase_timings)
    for name in expected_run_phases:
        assert isinstance(run_phase_timings[name], int)
        assert run_phase_timings[name] >= 0


def test_sql_gate_chunk_boundaries_preserve_current_lifecycle(
    clean_runtime: str,
) -> None:
    generation_id = 9202
    spec = SqlConsistencyGateSpec(
        name="chunked_lifecycle_sql_gate",
        check_id=f"test.sql.chunked-lifecycle.{uuid4().hex}",
        predicate_family="chunked_sql_conflict",
        confidence_type="deterministic_boolean",
        candidate_sql="""
            SELECT
              'bucket:fuel'::text AS input_scope,
              'block:fuel'::text AS blocking_rule_hash,
              jsonb_build_object(
                'left', 'claim:fuel:a',
                'right', 'claim:fuel:b'
              ) AS participant_roles,
              'numeric_equal_under_condition'::text AS predicate_key,
              'stable_source_anchor_v1'::text AS generation_policy,
              true AS emit_observation,
              event_row.finding_event::text AS finding_event,
              event_row.evaluation_event::text AS evaluation_event,
              'numeric_claims_match'::text AS normalized_predicate,
              jsonb_build_object('left', 2000, 'right', 1500)
                AS normalized_values,
              'high'::text AS severity,
              1.0::numeric AS confidence,
              jsonb_build_object('span', event_row.ord) AS evidence,
              jsonb_build_object('message', 'values differ') AS payload
              FROM (VALUES
                (1, 'observed', NULL),
                (2, NULL, 'evaluated')
              ) AS event_row(ord, finding_event, evaluation_event)
             ORDER BY event_row.ord
        """,
        coverage_sql="""
            SELECT
              ('chunked-sql:' || %(target_generation_id)s::text)
                AS universe_key,
              'exact_postgres'::text AS coverage_basis,
              true AS coverage_complete,
              1::bigint AS eligible_input_count,
              1::bigint AS candidate_count,
              1::bigint AS all_pairs_upper_bound,
              2::bigint AS evaluator_invocation_count
        """,
        timeout_ms=3000,
        materialization_chunk_size=1,
    )
    gate = DeterministicSqlConsistencyGate(spec)

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        evaluation = gate.evaluate(
            conn,
            target_generation_id=generation_id,
            catalog_version_id=1,
            deployment_name="test",
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_seq, finding_event, evaluation_event
                  FROM okg.observation_events
                 WHERE check_id = %(check_id)s
                   AND target_generation_id = %(generation_id)s
                 ORDER BY event_seq
                """,
                {
                    "check_id": spec.check_id,
                    "generation_id": generation_id,
                },
            )
            events = cur.fetchall()
            cur.execute(
                """
                SELECT finding_status, evaluation_status
                  FROM okg.observation_current
                 WHERE check_id = %(check_id)s
                   AND latest_generation_id = %(generation_id)s
                """,
                {
                    "check_id": spec.check_id,
                    "generation_id": generation_id,
                },
            )
            current = cur.fetchone()
            cur.execute(
                """
                SELECT candidate_count, candidate_count_by_block
                  FROM okg.consistency_candidate_coverage
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": evaluation.run_id},
            )
            coverage = cur.fetchone()

    assert evaluation.result == "evaluated"
    assert evaluation.open_observation_count == 1
    assert evaluation.details["coverage"]["candidate_count"] == 1
    assert coverage == (1, {})
    assert events == [(1, "observed", None), (2, None, "evaluated")]
    assert current == ("open", "evaluated")


def test_sql_gate_sql_native_materialization_matches_python_ids(
    clean_runtime: str,
) -> None:
    generation_id = 9203
    spec = SqlConsistencyGateSpec(
        name="sql_native_gate",
        check_id=f"test.sql.native.{uuid4().hex}",
        predicate_family="sql_native_conflict",
        confidence_type="deterministic_boolean",
        candidate_sql=r"""
            WITH event_row AS (
              SELECT *
                FROM (VALUES
                  (
                    1,
                    'bucket:cafe'::text,
                    'block:cafe'::text,
                    jsonb_build_object(
                      'right', 'claim:cafe:b',
                      'left', 'claim:fuel:a'
                    ),
                    false,
                    'observed'::text,
                    NULL::text,
                    'high'::text,
                    1.0::numeric,
                    jsonb_build_object(
                      'left', 2000,
                      'right', 1500,
                      'note', E'café\nquote"'
                    ),
                    jsonb_build_object('span', 'page:12'),
                    jsonb_build_object('message', 'values differ')
                  ),
                  (
                    2,
                    'bucket:cafe'::text,
                    'block:cafe'::text,
                    jsonb_build_object(
                      'right', 'claim:cafe:b',
                      'left', 'claim:fuel:a'
                    ),
                    false,
                    NULL::text,
                    'evaluated'::text,
                    NULL::text,
                    NULL::numeric,
                    jsonb_build_object(
                      'left', 2000,
                      'right', 1500,
                      'note', E'café\nquote"'
                    ),
                    '{}'::jsonb,
                    '{}'::jsonb
                  ),
                  (
                    3,
                    'bucket:symmetric'::text,
                    'block:symmetric'::text,
                    jsonb_build_object(
                      'b', 'claim:z',
                      'a', 'claim:a'
                    ),
                    true,
                    'observed'::text,
                    NULL::text,
                    'medium'::text,
                    1.0::numeric,
                    jsonb_build_object('status', 'mismatch'),
                    jsonb_build_object('span', 'page:13'),
                    jsonb_build_object('message', 'symmetric mismatch')
                  )
                ) AS rows(
                  ord, input_scope, blocking_rule_hash, participant_roles,
                  symmetric_predicate, finding_event, evaluation_event,
                  severity, confidence, normalized_values, evidence, payload
                )
            )
            SELECT
              input_scope,
              blocking_rule_hash,
              participant_roles,
              'numeric_equal_under_condition'::text AS predicate_key,
              'stable_source_anchor_v1'::text AS generation_policy,
              symmetric_predicate,
              true AS emit_observation,
              finding_event,
              evaluation_event,
              'numeric_claims_match'::text AS normalized_predicate,
              normalized_values,
              severity,
              confidence,
              evidence,
              payload
              FROM event_row
             ORDER BY ord
        """,
        coverage_sql="""
            SELECT
              ('sql-native:' || %(target_generation_id)s::text)
                AS universe_key,
              'exact_postgres'::text AS coverage_basis,
              true AS coverage_complete,
              3::bigint AS eligible_input_count,
              3::bigint AS candidate_count,
              3::bigint AS all_pairs_upper_bound,
              3::bigint AS evaluator_invocation_count
        """,
        timeout_ms=3000,
        materialization_strategy="sql_native",
    )
    definition = spec.definition("test")
    cafe_candidate = ConsistencyCandidate(
        input_scope="bucket:cafe",
        blocking_rule_hash="block:cafe",
        participant_roles={
            "right": "claim:cafe:b",
            "left": "claim:fuel:a",
        },
        predicate_key="numeric_equal_under_condition",
        generation_policy="stable_source_anchor_v1",
    )
    symmetric_candidate = ConsistencyCandidate(
        input_scope="bucket:symmetric",
        blocking_rule_hash="block:symmetric",
        participant_roles={"b": "claim:z", "a": "claim:a"},
        predicate_key="numeric_equal_under_condition",
        generation_policy="stable_source_anchor_v1",
        symmetric_predicate=True,
    )
    cafe_candidate_id = candidate_id_for(definition, cafe_candidate)
    symmetric_candidate_id = candidate_id_for(
        definition,
        symmetric_candidate,
    )
    cafe_observation_id = observation_id_for(
        definition,
        candidate_id=cafe_candidate_id,
        input_scope=cafe_candidate.input_scope,
        participant_roles=cafe_candidate.participant_roles,
        normalized_predicate="numeric_claims_match",
        normalized_values={
            "left": 2000,
            "right": 1500,
            "note": 'café\nquote"',
        },
        source_revision_policy=cafe_candidate.generation_policy,
    )
    symmetric_observation_id = observation_id_for(
        definition,
        candidate_id=symmetric_candidate_id,
        input_scope=symmetric_candidate.input_scope,
        participant_roles=symmetric_candidate.participant_roles,
        normalized_predicate="numeric_claims_match",
        normalized_values={"status": "mismatch"},
        source_revision_policy=symmetric_candidate.generation_policy,
        symmetric_predicate=True,
    )
    gate = DeterministicSqlConsistencyGate(spec)

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        evaluation = gate.evaluate(
            conn,
            target_generation_id=generation_id,
            catalog_version_id=1,
            deployment_name="test",
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT candidate_id
                  FROM okg.consistency_candidates
                 WHERE check_id = %(check_id)s
                   AND target_generation_id = %(generation_id)s
                 ORDER BY candidate_id
                """,
                {
                    "check_id": spec.check_id,
                    "generation_id": generation_id,
                },
            )
            candidate_ids = [row[0] for row in cur.fetchall()]
            cur.execute(
                """
                SELECT event_seq, observation_id, finding_event,
                       evaluation_event
                  FROM okg.observation_events
                 WHERE check_id = %(check_id)s
                   AND target_generation_id = %(generation_id)s
                 ORDER BY event_seq
                """,
                {
                    "check_id": spec.check_id,
                    "generation_id": generation_id,
                },
            )
            events = cur.fetchall()
            cur.execute(
                """
                SELECT observation_id, finding_status, evaluation_status,
                       severity, confidence, evidence, payload
                  FROM okg.observation_current
                 WHERE check_id = %(check_id)s
                   AND latest_generation_id = %(generation_id)s
                 ORDER BY observation_id
                """,
                {
                    "check_id": spec.check_id,
                    "generation_id": generation_id,
                },
            )
            current_rows = cur.fetchall()
            cur.execute(
                """
                SELECT metrics->>'materialization_strategy'
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": evaluation.run_id},
            )
            (strategy,) = cur.fetchone()
            cur.execute(
                """
                SELECT candidate_count, candidate_count_by_block
                  FROM okg.consistency_candidate_coverage
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": evaluation.run_id},
            )
            coverage = cur.fetchone()

    assert evaluation.result == "evaluated"
    assert evaluation.open_observation_count == 2
    assert strategy == "sql_native"
    assert candidate_ids == sorted([cafe_candidate_id, symmetric_candidate_id])
    assert events == [
        (1, cafe_observation_id, "observed", None),
        (2, cafe_observation_id, None, "evaluated"),
        (3, symmetric_observation_id, "observed", None),
    ]
    current_by_id = {row[0]: row[1:] for row in current_rows}
    assert current_by_id[cafe_observation_id] == (
        "open",
        "evaluated",
        "high",
        Decimal("1.000000"),
        {"span": "page:12"},
        {"message": "values differ"},
    )
    assert current_by_id[symmetric_observation_id] == (
        "open",
        "not_evaluated",
        "medium",
        Decimal("1.000000"),
        {"span": "page:13"},
        {"message": "symmetric mismatch"},
    )
    assert coverage == (
        3,
        {"block:cafe": 2, "block:symmetric": 1},
    )


def test_bulk_current_dedupes_logical_observations_across_candidate_versions(
    clean_runtime: str,
) -> None:
    definition_v1 = _definition()
    definition_v2 = replace(
        definition_v1,
        candidate_rule_hash=stable_hash({"blocking": "bulk-subject-v2"}),
    )
    candidate = _candidate("logical-dedupe")
    candidate_id_v1 = candidate_id_for(definition_v1, candidate)
    candidate_id_v2 = candidate_id_for(definition_v2, candidate)

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 9301)
        _insert_generation(conn, 9302)
        run_v1 = start_consistency_run(
            conn,
            definition_v1,
            target_generation_id=9301,
        )
        record_candidate_observations_bulk(
            conn,
            definition_v1,
            target_generation_id=9301,
            run_id=run_v1,
            records=[
                ConsistencyCandidateObservation(
                    candidate=candidate,
                    candidate_id=candidate_id_v1,
                    event=_event(
                        definition_v1,
                        candidate,
                        candidate_id_v1,
                        finding_event="observed",
                    ),
                )
            ],
        )

        run_v2 = start_consistency_run(
            conn,
            definition_v2,
            target_generation_id=9302,
        )
        record_candidate_observations_bulk(
            conn,
            definition_v2,
            target_generation_id=9302,
            run_id=run_v2,
            records=[
                ConsistencyCandidateObservation(
                    candidate=candidate,
                    candidate_id=candidate_id_v2,
                    event=_event(
                        definition_v2,
                        candidate,
                        candidate_id_v2,
                        finding_event="observed",
                    ),
                )
            ],
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)::int, count(DISTINCT observation_id)::int
                  FROM okg.observation_events
                 WHERE check_id = %(check_id)s
                """,
                {"check_id": definition_v1.check_id},
            )
            event_counts = cur.fetchone()
            cur.execute(
                """
                SELECT observation_id, candidate_id, latest_generation_id,
                       finding_status
                  FROM okg.observation_current
                 WHERE check_id = %(check_id)s
                """,
                {"check_id": definition_v1.check_id},
            )
            current_rows = cur.fetchall()

    assert candidate_id_v1 != candidate_id_v2
    assert event_counts == (2, 2)
    assert len(current_rows) == 1
    assert current_rows[0][1:] == (candidate_id_v2, 9302, "open")


def test_bulk_resolves_existing_candidate_after_definition_revision(
    clean_runtime: str,
) -> None:
    definition_v1 = _definition()
    definition_v2 = replace(
        definition_v1,
        candidate_rule_hash=stable_hash({"blocking": "bulk-resolve-v2"}),
    )
    candidate = _candidate("definition-revision-resolution")
    candidate_id_v1 = candidate_id_for(definition_v1, candidate)
    observed = _event(
        definition_v1,
        candidate,
        candidate_id_v1,
        finding_event="observed",
    )
    resolved = replace(observed, finding_event="resolved")

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 9351)
        _insert_generation(conn, 9352)
        run_v1 = start_consistency_run(
            conn,
            definition_v1,
            target_generation_id=9351,
        )
        record_candidate_observations_bulk(
            conn,
            definition_v1,
            target_generation_id=9351,
            run_id=run_v1,
            records=[
                ConsistencyCandidateObservation(
                    candidate=candidate,
                    candidate_id=candidate_id_v1,
                    event=observed,
                )
            ],
        )

        run_v2 = start_consistency_run(
            conn,
            definition_v2,
            target_generation_id=9352,
        )
        record_candidate_observations_bulk(
            conn,
            definition_v2,
            target_generation_id=9352,
            run_id=run_v2,
            records=[
                ConsistencyCandidateObservation(
                    candidate=candidate,
                    candidate_id=candidate_id_v1,
                    event=resolved,
                )
            ],
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT finding_status, latest_generation_id
                  FROM okg.observation_current
                 WHERE observation_id = %(observation_id)s
                """,
                {"observation_id": observed.observation_id},
            )
            assert cur.fetchone() == ("resolved", 9352)
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.consistency_candidates
                 WHERE check_id = %(check_id)s
                   AND target_generation_id = 9352
                """,
                {"check_id": definition_v1.check_id},
            )
            assert cur.fetchone()[0] == 0


def test_sql_native_current_dedupes_logical_observations_across_candidate_versions(
    clean_runtime: str,
) -> None:
    check_id = f"test.sql.logical-current.{uuid4().hex}"

    def spec_for(candidate_rule_hash: str) -> SqlConsistencyGateSpec:
        return SqlConsistencyGateSpec(
            name="sql_native_logical_current_dedupe",
            check_id=check_id,
            predicate_family="logical_current_dedupe",
            confidence_type="deterministic_boolean",
            candidate_rule_hash=candidate_rule_hash,
            evaluator_hash=stable_hash({"evaluator": "logical-current-v1"}),
            candidate_sql="""
                SELECT
                  'bucket:logical-dedupe'::text AS input_scope,
                  'block:logical-dedupe'::text AS blocking_rule_hash,
                  jsonb_build_object(
                    'left', 'claim:logical:a',
                    'right', 'claim:logical:b'
                  ) AS participant_roles,
                  'numeric_equal_under_condition'::text AS predicate_key,
                  'stable_source_anchor_v1'::text AS generation_policy,
                  true AS emit_observation,
                  'observed'::text AS finding_event,
                  'numeric_claims_match'::text AS normalized_predicate,
                  jsonb_build_object('left', 2000, 'right', 1500)
                    AS normalized_values,
                  'high'::text AS severity,
                  1.0::numeric AS confidence,
                  jsonb_build_object('span', 'page:dedupe') AS evidence,
                  jsonb_build_object('message', 'values differ') AS payload
            """,
            coverage_sql="""
                SELECT
                  ('logical-current:' || %(target_generation_id)s::text)
                    AS universe_key,
                  'exact_postgres'::text AS coverage_basis,
                  true AS coverage_complete,
                  1::bigint AS eligible_input_count,
                  1::bigint AS candidate_count,
                  1::bigint AS all_pairs_upper_bound,
                  1::bigint AS evaluator_invocation_count
            """,
            timeout_ms=3000,
            materialization_strategy="sql_native",
        )

    spec_v1 = spec_for(stable_hash({"candidate": "logical-current-v1"}))
    spec_v2 = spec_for(stable_hash({"candidate": "logical-current-v2"}))
    candidate = ConsistencyCandidate(
        input_scope="bucket:logical-dedupe",
        blocking_rule_hash="block:logical-dedupe",
        participant_roles={
            "left": "claim:logical:a",
            "right": "claim:logical:b",
        },
        predicate_key="numeric_equal_under_condition",
        generation_policy="stable_source_anchor_v1",
    )
    candidate_id_v1 = candidate_id_for(spec_v1.definition("test"), candidate)
    candidate_id_v2 = candidate_id_for(spec_v2.definition("test"), candidate)

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, 9303)
        _insert_generation(conn, 9304)
        DeterministicSqlConsistencyGate(spec_v1).evaluate(
            conn,
            target_generation_id=9303,
            catalog_version_id=1,
            deployment_name="test",
        )
        DeterministicSqlConsistencyGate(spec_v2).evaluate(
            conn,
            target_generation_id=9304,
            catalog_version_id=1,
            deployment_name="test",
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)::int, count(DISTINCT observation_id)::int
                  FROM okg.observation_events
                 WHERE check_id = %(check_id)s
                """,
                {"check_id": check_id},
            )
            event_counts = cur.fetchone()
            cur.execute(
                """
                SELECT observation_id, candidate_id, latest_generation_id,
                       finding_status
                  FROM okg.observation_current
                 WHERE check_id = %(check_id)s
                """,
                {"check_id": check_id},
            )
            current_rows = cur.fetchall()

    assert candidate_id_v1 != candidate_id_v2
    assert event_counts == (2, 2)
    assert len(current_rows) == 1
    assert current_rows[0][1:] == (candidate_id_v2, 9304, "open")


def _observed_event(
    definition: ConsistencyCheckDefinition,
    candidate: ConsistencyCandidate,
    candidate_id: str,
    *,
    severity: str = "high",
    evidence: dict[str, object] | None = None,
) -> ObservationEvent:
    observation_id = observation_id_for(
        definition,
        candidate_id=candidate_id,
        input_scope=candidate.input_scope,
        participant_roles=candidate.participant_roles,
        normalized_predicate="numeric_claims_match",
        normalized_values={"left": 2000, "right": 1500},
        source_revision_policy=candidate.generation_policy,
    )
    return ObservationEvent(
        observation_id=observation_id,
        candidate_id=candidate_id,
        input_scope=candidate.input_scope,
        predicate_key=candidate.predicate_key,
        normalized_predicate="numeric_claims_match",
        normalized_values={"left": 2000, "right": 1500},
        participant_roles=candidate.participant_roles,
        finding_event="observed",
        evaluation_event=None,
        severity=severity,
        confidence=1,
        evidence=evidence if evidence is not None else {"spans": ["page:12"]},
        payload={"message": "claim values differ"},
    )


def test_bulk_reobservation_of_unchanged_open_finding_is_noop(
    clean_runtime: str,
) -> None:
    """Python bulk path: re-observing an unchanged open finding appends
    zero new observation_events rows, but a genuinely changed finding does."""
    definition = _definition()
    candidate = _candidate("noop-bulk")
    candidate_id = candidate_id_for(definition, candidate)
    observation_id = _observed_event(
        definition, candidate, candidate_id
    ).observation_id

    with psycopg.connect(clean_runtime) as conn:
        for generation_id in (9401, 9402, 9403):
            _insert_generation(conn, generation_id)

        # G1: first observation of an open finding.
        run_g1 = start_consistency_run(
            conn, definition, target_generation_id=9401
        )
        record_candidate_observations_bulk(
            conn,
            definition,
            target_generation_id=9401,
            run_id=run_g1,
            records=[
                ConsistencyCandidateObservation(
                    candidate=candidate,
                    candidate_id=candidate_id,
                    event=_observed_event(definition, candidate, candidate_id),
                )
            ],
        )

        def event_count() -> int:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT count(*)
                      FROM okg.observation_events
                     WHERE observation_id = %(observation_id)s
                    """,
                    {"observation_id": observation_id},
                )
                return cur.fetchone()[0]

        count_after_g1 = event_count()
        assert count_after_g1 == 1

        # G2: byte-identical re-observation -> skipped (zero new rows).
        run_g2 = start_consistency_run(
            conn, definition, target_generation_id=9402
        )
        record_candidate_observations_bulk(
            conn,
            definition,
            target_generation_id=9402,
            run_id=run_g2,
            records=[
                ConsistencyCandidateObservation(
                    candidate=candidate,
                    candidate_id=candidate_id,
                    event=_observed_event(definition, candidate, candidate_id),
                )
            ],
        )
        assert event_count() == count_after_g1

        # Finding is still open in the current projection after the no-op.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT finding_status, latest_generation_id
                  FROM okg.observation_current
                 WHERE observation_id = %(observation_id)s
                """,
                {"observation_id": observation_id},
            )
            status_after_noop = cur.fetchone()
        # latest_generation_id stays at the last generation that actually
        # changed the finding (G1), because the no-op skips the current
        # projection update as well.
        assert status_after_noop == ("open", 9401)

        # G3: genuinely changed finding (different severity + evidence)
        # -> a new event row appends.
        run_g3 = start_consistency_run(
            conn, definition, target_generation_id=9403
        )
        record_candidate_observations_bulk(
            conn,
            definition,
            target_generation_id=9403,
            run_id=run_g3,
            records=[
                ConsistencyCandidateObservation(
                    candidate=candidate,
                    candidate_id=candidate_id,
                    event=_observed_event(
                        definition,
                        candidate,
                        candidate_id,
                        severity="low",
                        evidence={"spans": ["page:99"]},
                    ),
                )
            ],
        )
        assert event_count() == count_after_g1 + 1

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT finding_status, severity, evidence,
                       latest_generation_id
                  FROM okg.observation_current
                 WHERE observation_id = %(observation_id)s
                """,
                {"observation_id": observation_id},
            )
            status_after_change = cur.fetchone()

    assert status_after_change == (
        "open",
        "low",
        {"spans": ["page:99"]},
        9403,
    )


def _noop_spec(
    check_id: str,
    *,
    candidate_rule_hash: str,
    severity: str,
    evidence_span: str,
) -> SqlConsistencyGateSpec:
    return SqlConsistencyGateSpec(
        name="sql_native_noop_reobservation",
        check_id=check_id,
        predicate_family="sql_native_noop",
        confidence_type="deterministic_boolean",
        candidate_rule_hash=candidate_rule_hash,
        evaluator_hash=stable_hash({"evaluator": "sql-native-noop-v1"}),
        candidate_sql=f"""
            SELECT
              'bucket:noop-native'::text AS input_scope,
              'block:noop-native'::text AS blocking_rule_hash,
              jsonb_build_object(
                'left', 'claim:noop:a',
                'right', 'claim:noop:b'
              ) AS participant_roles,
              'numeric_equal_under_condition'::text AS predicate_key,
              'stable_source_anchor_v1'::text AS generation_policy,
              true AS emit_observation,
              'observed'::text AS finding_event,
              'numeric_claims_match'::text AS normalized_predicate,
              jsonb_build_object('left', 2000, 'right', 1500)
                AS normalized_values,
              '{severity}'::text AS severity,
              1.0::numeric AS confidence,
              jsonb_build_object('span', '{evidence_span}') AS evidence,
              jsonb_build_object('message', 'values differ') AS payload
        """,
        coverage_sql="""
            SELECT
              ('sql-native-noop:' || %(target_generation_id)s::text)
                AS universe_key,
              'exact_postgres'::text AS coverage_basis,
              true AS coverage_complete,
              1::bigint AS eligible_input_count,
              1::bigint AS candidate_count,
              1::bigint AS all_pairs_upper_bound,
              1::bigint AS evaluator_invocation_count
        """,
        timeout_ms=3000,
        materialization_strategy="sql_native",
    )


def test_sql_native_reobservation_of_unchanged_open_finding_is_noop(
    clean_runtime: str,
) -> None:
    """SQL-native path: re-observing an unchanged open finding appends
    zero new observation_events rows, but a genuinely changed finding does."""
    check_id = f"test.sql.noop-native.{uuid4().hex}"
    candidate_rule_hash = stable_hash({"candidate": "sql-native-noop"})
    candidate = ConsistencyCandidate(
        input_scope="bucket:noop-native",
        blocking_rule_hash="block:noop-native",
        participant_roles={
            "left": "claim:noop:a",
            "right": "claim:noop:b",
        },
        predicate_key="numeric_equal_under_condition",
        generation_policy="stable_source_anchor_v1",
    )
    spec_open = _noop_spec(
        check_id,
        candidate_rule_hash=candidate_rule_hash,
        severity="high",
        evidence_span="page:12",
    )
    definition = spec_open.definition("test")
    candidate_id = candidate_id_for(definition, candidate)
    observation_id = observation_id_for(
        definition,
        candidate_id=candidate_id,
        input_scope=candidate.input_scope,
        participant_roles=candidate.participant_roles,
        normalized_predicate="numeric_claims_match",
        normalized_values={"left": 2000, "right": 1500},
        source_revision_policy=candidate.generation_policy,
    )

    with psycopg.connect(clean_runtime) as conn:
        for generation_id in (9411, 9412, 9413):
            _insert_generation(conn, generation_id)

        def event_count() -> int:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT count(*)
                      FROM okg.observation_events
                     WHERE observation_id = %(observation_id)s
                    """,
                    {"observation_id": observation_id},
                )
                return cur.fetchone()[0]

        # G1: first observation of an open finding.
        DeterministicSqlConsistencyGate(spec_open).evaluate(
            conn,
            target_generation_id=9411,
            catalog_version_id=1,
            deployment_name="test",
        )
        count_after_g1 = event_count()
        assert count_after_g1 == 1

        # G2: identical re-observation (same candidate_rule_hash keeps the
        # observation_id stable) -> skipped, zero new rows.
        DeterministicSqlConsistencyGate(spec_open).evaluate(
            conn,
            target_generation_id=9412,
            catalog_version_id=1,
            deployment_name="test",
        )
        assert event_count() == count_after_g1

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT finding_status, latest_generation_id
                  FROM okg.observation_current
                 WHERE observation_id = %(observation_id)s
                """,
                {"observation_id": observation_id},
            )
            status_after_noop = cur.fetchone()
        assert status_after_noop == ("open", 9411)

        # G3: genuinely changed finding (different severity + evidence)
        # -> a new event row appends.
        spec_changed = _noop_spec(
            check_id,
            candidate_rule_hash=candidate_rule_hash,
            severity="low",
            evidence_span="page:99",
        )
        DeterministicSqlConsistencyGate(spec_changed).evaluate(
            conn,
            target_generation_id=9413,
            catalog_version_id=1,
            deployment_name="test",
        )
        assert event_count() == count_after_g1 + 1

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT finding_status, severity, evidence,
                       latest_generation_id
                  FROM okg.observation_current
                 WHERE observation_id = %(observation_id)s
                """,
                {"observation_id": observation_id},
            )
            status_after_change = cur.fetchone()

    assert status_after_change == (
        "open",
        "low",
        {"span": "page:99"},
        9413,
    )
