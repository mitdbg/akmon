"""Historical consistency-candidate identity across program revisions (#793)."""

from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.consistency import (  # noqa: E402
    IrConsistencyCheckSpec,
    IrPostPublishCheck,
)
from okg.substrate.consistency.observations import (  # noqa: E402
    ConsistencyCandidate,
    ConsistencyCandidateObservation,
    HistoricalCandidateIdentityUnverifiable,
    ObservationEvent,
    candidate_id_for,
    record_candidate_observations_bulk,
    start_consistency_run,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.substrate.transform import (  # noqa: E402
    Binding,
    Block,
    Effect,
    Op,
    OpKind,
    Terminator,
    TerminatorKind,
    TransformProgram,
)
from okg.substrate.transform.expr import (  # noqa: E402
    and_,
    cast,
    col,
    eq,
    func,
    json_member,
    like,
    lit,
)
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


def _text(value: str) -> dict:
    return cast(lit(value), "text")


def _program(*, narrow_population: bool) -> TransformProgram:
    links = func(
        "coalesce",
        json_member(col("n.attrs"), "links"),
        cast(lit("[]"), "jsonb"),
    )
    population = and_(
        eq(col("n.subtype"), lit("page")),
        like(col("link"), "bad:%"),
    )
    if narrow_population:
        population = and_(
            population,
            eq(col("n.node_id"), lit("page:kept-by-v2")),
        )
    return TransformProgram(
        program_id="fixture.issue_793.bad_link",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={
            "check": {
                "predicate_key": "page_links_resolve",
                "normalized_predicate": "bad_link",
                "participant_roles": {"page": "node_id"},
                "normalized_value_columns": ["node_id", "link"],
                "finding_scope_columns": ["link"],
                "severity": "medium",
            }
        },
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(
                        Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}),
                        output="n",
                    ),
                    Binding(
                        Op(OpKind.UNNEST, {"expr": links, "field": "link"}),
                        output="exploded",
                        inputs=("n",),
                    ),
                    Binding(
                        Op(OpKind.WHERE, {"expr": population}),
                        output="flagged",
                        inputs=("exploded",),
                    ),
                    Binding(
                        Op(
                            OpKind.SELECT,
                            {
                                "project": {
                                    "node_id": col("n.node_id"),
                                    "link": col("link"),
                                    "finding_event": _text("observed"),
                                }
                            },
                        ),
                        output="offending",
                        inputs=("flagged",),
                    ),
                    Binding(
                        Op(OpKind.EMIT, {"target": "offending"}),
                        inputs=("offending",),
                    ),
                ),
                terminator=Terminator(
                    TerminatorKind.RETURN,
                    {"values": ["offending"]},
                ),
            ),
        ),
    )


def _spec(check_id: str, *, narrow_population: bool) -> IrConsistencyCheckSpec:
    return IrConsistencyCheckSpec(
        name="issue_793_program_change",
        check_id=check_id,
        predicate_family="fixture_issue_793",
        confidence_type="deterministic_boolean",
        program=_program(narrow_population=narrow_population),
        program_ref="tests.fixture.issue_793_program_change",
    )


def _seed_and_publish(dsn: str, page_id: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    page_id,
                    "page",
                    {"links": ["bad:outside-v2-population"]},
                    {"id": page_id},
                    {"v": 1},
                )
            ],
            source="default",
            strict_endpoints=True,
        )
    result = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert result["status"] == "published", result
    return int(result["compat_generation_id"])


def _evaluate(conn, spec: IrConsistencyCheckSpec, generation_id: int):
    return IrPostPublishCheck(spec).evaluate(
        conn,
        target_generation_id=generation_id,
        catalog_version_id=1,
        deployment_name="default",
    )


def _status_counts(conn, check_id: str) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FILTER (WHERE finding_status = 'open'),
                   count(*) FILTER (WHERE finding_status = 'resolved')
              FROM okg.observation_current
             WHERE check_id = %s
            """,
            (check_id,),
        )
        open_count, resolved_count = cur.fetchone()
    return int(open_count), int(resolved_count)


def _event_counts(conn, check_id: str) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FILTER (WHERE finding_event = 'observed'),
                   count(*) FILTER (WHERE finding_event = 'resolved')
              FROM okg.observation_events
             WHERE check_id = %s
            """,
            (check_id,),
        )
        observed_count, resolved_count = cur.fetchone()
    return int(observed_count), int(resolved_count)


def _run_status(conn, run_id) -> str:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM okg.derived_artifact_runs WHERE run_id = %s",
            (run_id,),
        )
        return str(cur.fetchone()[0])


def _open_candidate(conn, check_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT current.observation_id, current.candidate_id,
                   current.input_scope, current.predicate_key,
                   current.normalized_predicate, current.normalized_values,
                   current.participant_roles, candidate.blocking_rule_hash,
                   candidate.generation_policy
              FROM okg.observation_current current
              JOIN okg.consistency_candidates candidate
                ON candidate.check_id = current.check_id
               AND candidate.candidate_id = current.candidate_id
             WHERE current.check_id = %s
               AND current.finding_status = 'open'
             ORDER BY candidate.target_generation_id
             LIMIT 1
            """,
            (check_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return {
        "observation_id": row[0],
        "candidate_id": row[1],
        "input_scope": row[2],
        "predicate_key": row[3],
        "normalized_predicate": row[4],
        "normalized_values": row[5],
        "participant_roles": row[6],
        "blocking_rule_hash": row[7],
        "generation_policy": row[8],
    }


@pytest.mark.postgres
def test_program_change_resolves_open_observation_with_historical_candidate_id(
    clean_runtime: str,
) -> None:
    check_id = f"fixture.issue793.{uuid.uuid4().hex[:8]}"
    page_id = f"page:excluded-by-v2:{uuid.uuid4().hex[:8]}"
    generation_id = _seed_and_publish(clean_runtime, page_id)
    v1 = _spec(check_id, narrow_population=False)
    v2 = _spec(check_id, narrow_population=True)
    definition_v1 = v1.definition("default")
    definition_v2 = v2.definition("default")

    assert v1.program.checksum() != v2.program.checksum()
    assert definition_v1.candidate_rule_hash != definition_v2.candidate_rule_hash
    assert definition_v1.evaluator_hash != definition_v2.evaluator_hash

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        evaluation_v1 = _evaluate(conn, v1, generation_id)
        assert evaluation_v1.result == "evaluated", evaluation_v1.details
        assert _run_status(conn, evaluation_v1.run_id) == "completed"
        assert _status_counts(conn, check_id) == (1, 0)
        assert _event_counts(conn, check_id) == (1, 0)
        historical = _open_candidate(conn, check_id)

        v2_candidate = ConsistencyCandidate(
            input_scope=historical["input_scope"],
            blocking_rule_hash=v2.program.checksum(),
            participant_roles=historical["participant_roles"],
            predicate_key=historical["predicate_key"],
            generation_policy=historical["generation_policy"],
        )
        assert (
            candidate_id_for(definition_v2, v2_candidate) != historical["candidate_id"]
        )

        evaluation_v2 = _evaluate(conn, v2, generation_id)
        assert evaluation_v2.result == "evaluated", evaluation_v2.details
        assert _run_status(conn, evaluation_v2.run_id) == "completed"
        assert _status_counts(conn, check_id) == (0, 1)
        assert _event_counts(conn, check_id) == (1, 1)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT observation_id, candidate_id
                  FROM okg.observation_current
                 WHERE check_id = %s
                """,
                (check_id,),
            )
            assert cur.fetchone() == (
                historical["observation_id"],
                historical["candidate_id"],
            )
            cur.execute(
                "SELECT count(*) FROM okg.consistency_candidates WHERE run_id = %s",
                (evaluation_v2.run_id,),
            )
            assert cur.fetchone()[0] == 0


@pytest.mark.postgres
def test_resolution_accepts_repeated_identical_historical_candidate_rows(
    clean_runtime: str,
) -> None:
    check_id = f"fixture.issue793.duplicate.{uuid.uuid4().hex[:8]}"
    generation_id = _seed_and_publish(
        clean_runtime,
        f"page:duplicate-history:{uuid.uuid4().hex[:8]}",
    )
    v1 = _spec(check_id, narrow_population=False)
    v2 = _spec(check_id, narrow_population=True)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _evaluate(conn, v1, generation_id).result == "evaluated"
        historical = _open_candidate(conn, check_id)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.consistency_candidates (
                  check_id, target_generation_id, candidate_id, run_id,
                  input_scope, blocking_rule_hash, participant_roles,
                  participant_anchors, predicate_key, generation_policy,
                  coverage_status, exclusion_reason, candidate_payload
                )
                SELECT check_id, target_generation_id + 10000000,
                       candidate_id, NULL, input_scope, blocking_rule_hash,
                       participant_roles, participant_anchors, predicate_key,
                       generation_policy, coverage_status, exclusion_reason,
                       candidate_payload
                  FROM okg.consistency_candidates
                 WHERE check_id = %s AND candidate_id = %s
                 ORDER BY target_generation_id
                 LIMIT 1
                """,
                (check_id, historical["candidate_id"]),
            )

        evaluation_v2 = _evaluate(conn, v2, generation_id)
        assert evaluation_v2.result == "evaluated", evaluation_v2.details
        assert _status_counts(conn, check_id) == (0, 1)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) FROM okg.consistency_candidates
                 WHERE check_id = %s AND candidate_id = %s
                """,
                (check_id, historical["candidate_id"]),
            )
            assert cur.fetchone()[0] == 2


@pytest.mark.postgres
@pytest.mark.parametrize("history_case", ["missing", "contradictory"])
def test_resolution_refuses_unverifiable_historical_candidate_id(
    clean_runtime: str,
    history_case: str,
) -> None:
    check_id = f"fixture.issue793.{history_case}.{uuid.uuid4().hex[:8]}"
    generation_id = _seed_and_publish(
        clean_runtime,
        f"page:{history_case}:{uuid.uuid4().hex[:8]}",
    )
    v1 = _spec(check_id, narrow_population=False)
    v2 = _spec(check_id, narrow_population=True)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _evaluate(conn, v1, generation_id).result == "evaluated"
        historical = _open_candidate(conn, check_id)
        if history_case == "missing":
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM okg.consistency_candidates "
                    "WHERE check_id = %s AND candidate_id = %s",
                    (check_id, historical["candidate_id"]),
                )
        else:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO okg.consistency_candidates (
                      check_id, target_generation_id, candidate_id, run_id,
                      input_scope, blocking_rule_hash, participant_roles,
                      participant_anchors, predicate_key, generation_policy,
                      coverage_status, exclusion_reason, candidate_payload
                    )
                    SELECT check_id, target_generation_id + 10000000,
                           candidate_id, NULL, input_scope,
                           blocking_rule_hash || ':contradiction',
                           participant_roles, participant_anchors, predicate_key,
                           generation_policy, coverage_status, exclusion_reason,
                           candidate_payload
                      FROM okg.consistency_candidates
                     WHERE check_id = %s AND candidate_id = %s
                     ORDER BY target_generation_id
                     LIMIT 1
                    """,
                    (check_id, historical["candidate_id"]),
                )

        before_events = _event_counts(conn, check_id)
        evaluation_v2 = _evaluate(conn, v2, generation_id)
        assert evaluation_v2.result == "blocked_error", evaluation_v2.details
        assert evaluation_v2.details["error"]["type"] == (
            "HistoricalCandidateIdentityUnverifiable"
        )
        assert _run_status(conn, evaluation_v2.run_id) == "failed"
        assert _status_counts(conn, check_id) == (1, 0)
        assert _event_counts(conn, check_id) == before_events == (1, 0)


@pytest.mark.postgres
def test_resolution_refuses_a_non_open_current_observation(
    clean_runtime: str,
) -> None:
    check_id = f"fixture.issue793.nonopen.{uuid.uuid4().hex[:8]}"
    generation_id = _seed_and_publish(
        clean_runtime,
        f"page:nonopen:{uuid.uuid4().hex[:8]}",
    )
    v1 = _spec(check_id, narrow_population=False)
    v2 = _spec(check_id, narrow_population=True)
    definition_v2 = v2.definition("default")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _evaluate(conn, v1, generation_id).result == "evaluated"
        historical = _open_candidate(conn, check_id)
        assert _evaluate(conn, v2, generation_id).result == "evaluated"
        assert _status_counts(conn, check_id) == (0, 1)
        before_events = _event_counts(conn, check_id)
        run_id = start_consistency_run(
            conn,
            definition_v2,
            target_generation_id=generation_id,
        )
        candidate = ConsistencyCandidate(
            input_scope=historical["input_scope"],
            blocking_rule_hash=historical["blocking_rule_hash"],
            participant_roles=historical["participant_roles"],
            predicate_key=historical["predicate_key"],
            generation_policy=historical["generation_policy"],
        )
        record = ConsistencyCandidateObservation(
            candidate=candidate,
            candidate_id=historical["candidate_id"],
            event=ObservationEvent(
                observation_id=historical["observation_id"],
                candidate_id=historical["candidate_id"],
                input_scope=historical["input_scope"],
                predicate_key=historical["predicate_key"],
                normalized_predicate=historical["normalized_predicate"],
                normalized_values=historical["normalized_values"],
                participant_roles=historical["participant_roles"],
                finding_event="resolved",
                coverage_complete=True,
            ),
        )
        with pytest.raises(HistoricalCandidateIdentityUnverifiable, match="not open"):
            record_candidate_observations_bulk(
                conn,
                definition_v2,
                target_generation_id=generation_id,
                run_id=run_id,
                records=[record],
            )
        assert _status_counts(conn, check_id) == (0, 1)
        assert _event_counts(conn, check_id) == before_events == (1, 1)
