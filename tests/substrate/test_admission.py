"""Admission-envelope persistence tests."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Json

from okg.substrate.admission import (
    apply_generation_feedback,
    attach_fact_range_refs_for_run,
    attach_payload_ref,
    ensure_envelope,
)
from okg.substrate.cli.main import main
from okg.substrate.errors import SubstrateError


pytestmark = pytest.mark.postgres


def test_envelope_idempotency_and_fact_ref_recovery(clean_runtime: str) -> None:
    run_id = str(uuid4())
    observed_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES
                  ('admission:test:1', 'document', '{}'::jsonb,
                   'admission_test', '{}'::jsonb, '{}'::jsonb,
                   %s, %s, %s::uuid, 'I')
                """,
                (observed_at, f"admission_test:{run_id}", run_id),
            )

        with conn.transaction():
            first = ensure_envelope(
                conn,
                deployment_name="test",
                producer_id="admission_test",
                producer_kind="source",
                intent="source_sync",
                admission_mode="fast_track",
                status="pending",
                idempotency_key="same-key",
                source="admission_test",
                run_id=run_id,
            )
            second = ensure_envelope(
                conn,
                deployment_name="test",
                producer_id="admission_test",
                producer_kind="source",
                intent="source_sync",
                admission_mode="fast_track",
                status="pending",
                idempotency_key="same-key",
                source="admission_test",
                run_id=run_id,
            )
            refs = attach_fact_range_refs_for_run(
                conn,
                admission_id=first.admission_id,
                source="admission_test",
                run_id=run_id,
            )
            refs_again = attach_fact_range_refs_for_run(
                conn,
                admission_id=first.admission_id,
                source="admission_test",
                run_id=run_id,
            )

        assert first.inserted is True
        assert second.inserted is False
        assert second.admission_id == first.admission_id
        assert len(refs) == 1
        assert len(refs_again) == 1
        assert refs[0].record_count == 1

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  (SELECT count(*) FROM okg.admission_envelopes),
                  (SELECT count(*) FROM okg.admission_record_refs),
                  (
                    SELECT count(*) FROM okg.admission_events
                     WHERE event_type = 'duplicate_coalesced'
                  ),
                  (
                    SELECT count(*) FROM okg.admission_events
                     WHERE event_type = 'refs_attached'
                  )
                """
            )
            row = cur.fetchone()
    assert row == (1, 1, 1, 1)


def test_generation_feedback_publishes_matching_admission(
    clean_runtime: str,
) -> None:
    run_id = str(uuid4())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES
                  ('admission:pub:1', 'document', '{}'::jsonb,
                   'admission_pub', '{}'::jsonb, '{}'::jsonb,
                   now(), %s, %s::uuid, 'I')
                RETURNING fact_id
                """,
                (f"admission_pub:{run_id}", run_id),
            )
            fact_id = int(cur.fetchone()[0])
            generation_id = _insert_generation(
                cur,
                deployment_name="test_pub",
                status="published",
                fact_watermark=fact_id,
                edge_fact_watermark=0,
            )

        with conn.transaction():
            envelope = ensure_envelope(
                conn,
                deployment_name="test_pub",
                producer_id="admission_pub",
                producer_kind="source",
                intent="source_sync",
                admission_mode="fast_track",
                status="accepted",
                idempotency_key="pub-key",
                source="admission_pub",
                run_id=run_id,
            )
            attach_fact_range_refs_for_run(
                conn,
                admission_id=envelope.admission_id,
                source="admission_pub",
                run_id=run_id,
            )
            first = apply_generation_feedback(
                conn,
                deployment_name="test_pub",
                generation_id=generation_id,
            )
            second = apply_generation_feedback(
                conn,
                deployment_name="test_pub",
                generation_id=generation_id,
            )

        assert first["updated"] == 1
        assert second["updated"] == 0
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, target_generation_ref, verdict
                  FROM okg.admission_envelopes
                 WHERE admission_id = %s::uuid
                """,
                (envelope.admission_id,),
            )
            status, target_generation_ref, verdict = cur.fetchone()
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.admission_events
                 WHERE admission_id = %s::uuid
                   AND event_type = 'published'
                """,
                (envelope.admission_id,),
            )
            event_count = int(cur.fetchone()[0])

    assert status == "published"
    assert target_generation_ref == str(generation_id)
    assert verdict["generation_status"] == "published"
    assert event_count == 1


def test_generation_feedback_blocks_unpublished_admission(
    clean_runtime: str,
) -> None:
    run_id = str(uuid4())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES
                  ('admission:block:1', 'document', '{}'::jsonb,
                   'admission_block', '{}'::jsonb, '{}'::jsonb,
                   now(), %s, %s::uuid, 'I')
                """,
                (f"admission_block:{run_id}", run_id),
            )
            generation_id = _insert_generation(
                cur,
                deployment_name="test_block",
                status="blocked",
                blocked_reason={"reason": "fixture_blocked"},
            )

        with conn.transaction():
            envelope = ensure_envelope(
                conn,
                deployment_name="test_block",
                producer_id="admission_block",
                producer_kind="source",
                intent="source_sync",
                admission_mode="fast_track",
                status="accepted",
                idempotency_key="blocked-key",
                source="admission_block",
                run_id=run_id,
            )
            attach_fact_range_refs_for_run(
                conn,
                admission_id=envelope.admission_id,
                source="admission_block",
                run_id=run_id,
            )
            feedback = apply_generation_feedback(
                conn,
                deployment_name="test_block",
                generation_id=generation_id,
            )

        assert feedback["updated"] == 1
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, target_generation_ref, verdict
                  FROM okg.admission_envelopes
                 WHERE admission_id = %s::uuid
                """,
                (envelope.admission_id,),
            )
            status, target_generation_ref, verdict = cur.fetchone()

    assert status == "blocked"
    assert target_generation_ref == str(generation_id)
    assert verdict["blocked_reason"]["reason"] == "fixture_blocked"


def test_chronos_generation_feedback_isolates_target_branch(
    clean_runtime: str,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    deployment = "test_branch_feedback"
    default_run_id = str(uuid4())
    experiment_run_id = str(uuid4())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch("experiment", from_branch="default")
        generation_id = store.checkpoint_generation(
            branch="experiment",
            deployment_name=deployment,
            status="published",
        )

        with conn.transaction():
            default_envelope = ensure_envelope(
                conn,
                deployment_name=deployment,
                producer_id="default_source",
                producer_kind="source",
                intent="source_sync",
                admission_mode="fast_track",
                status="accepted",
                idempotency_key="default-key",
                source="default_source",
                run_id=default_run_id,
            )
            experiment_envelope = ensure_envelope(
                conn,
                deployment_name=deployment,
                producer_id="experiment_source",
                producer_kind="source",
                intent="source_sync",
                admission_mode="fast_track",
                status="accepted",
                idempotency_key="experiment-key",
                source="experiment_source",
                run_id=experiment_run_id,
                target_branch_name="experiment",
            )
            attach_payload_ref(
                conn,
                admission_id=default_envelope.admission_id,
                ref_key="default-run",
                payload={
                    "kind": "chronos_graph_run",
                    "source": "default_source",
                    "run_id": default_run_id,
                },
            )
            attach_payload_ref(
                conn,
                admission_id=experiment_envelope.admission_id,
                ref_key="experiment-run",
                payload={
                    "kind": "chronos_graph_run",
                    "source": "experiment_source",
                    "run_id": experiment_run_id,
                },
            )
            with pytest.raises(SubstrateError) as mismatch:
                apply_generation_feedback(
                    conn,
                    deployment_name=deployment,
                    generation_id=generation_id,
                    expected_admission_ids=[],
                )
            first = apply_generation_feedback(
                conn,
                deployment_name=deployment,
                generation_id=generation_id,
                expected_admission_ids=[experiment_envelope.admission_id],
            )
            second = apply_generation_feedback(
                conn,
                deployment_name=deployment,
                generation_id=generation_id,
                expected_admission_ids=[experiment_envelope.admission_id],
            )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT admission_id::text, status, target_generation_ref
                  FROM okg.admission_envelopes
                 WHERE admission_id IN (%s::uuid, %s::uuid)
                 ORDER BY admission_id
                """,
                (
                    default_envelope.admission_id,
                    experiment_envelope.admission_id,
                ),
            )
            observed = {
                admission_id: (status, target_generation_ref)
                for admission_id, status, target_generation_ref in cur.fetchall()
            }

    assert mismatch.value.data["reason"] == "expected_admission_set_mismatch"
    assert first["branch_name"] == "experiment"
    assert first["updated"] == 1
    assert first["admission_ids"] == [experiment_envelope.admission_id]
    assert first["verified_admission_ids"] == [
        experiment_envelope.admission_id
    ]
    assert second["updated"] == 0
    assert observed[default_envelope.admission_id] == ("accepted", None)
    assert observed[experiment_envelope.admission_id] == (
        "published",
        generation_id,
    )


def test_recovery_promotion_publishes_only_frozen_admission_set(
    clean_runtime: str,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    deployment = "test_exact_recovery_feedback"
    frozen_run_id = str(uuid4())
    later_run_id = str(uuid4())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        frozen = ensure_envelope(
            conn,
            deployment_name=deployment,
            producer_id="frozen_source",
            producer_kind="source",
            intent="source_sync",
            admission_mode="fast_track",
            status="accepted",
            idempotency_key="frozen-key",
            source="frozen_source",
            run_id=frozen_run_id,
        )
        attach_payload_ref(
            conn,
            admission_id=frozen.admission_id,
            ref_key="frozen-run",
            payload={
                "kind": "chronos_graph_run",
                "source": "frozen_source",
                "run_id": frozen_run_id,
            },
        )
        frozen_generation = store.checkpoint_generation(
            deployment_name=deployment,
            status="blocked",
            metadata={
                "blocked_reason": {"reason": "ingest_source_incomplete"}
            },
        )
        apply_generation_feedback(
            conn,
            deployment_name=deployment,
            generation_id=frozen_generation,
            expected_admission_ids=[frozen.admission_id],
        )

        later = ensure_envelope(
            conn,
            deployment_name=deployment,
            producer_id="later_source",
            producer_kind="source",
            intent="source_sync",
            admission_mode="fast_track",
            status="accepted",
            idempotency_key="later-key",
            source="later_source",
            run_id=later_run_id,
        )
        attach_payload_ref(
            conn,
            admission_id=later.admission_id,
            ref_key="later-run",
            payload={
                "kind": "chronos_graph_run",
                "source": "later_source",
                "run_id": later_run_id,
            },
        )
        successor = store.checkpoint_generation(
            deployment_name=deployment,
            status="published",
        )
        feedback = apply_generation_feedback(
            conn,
            deployment_name=deployment,
            generation_id=successor,
            expected_admission_ids=[frozen.admission_id],
            recovery_from_generation_id=frozen_generation,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT admission_id::text, status, target_generation_ref "
                "FROM okg.admission_envelopes "
                "WHERE admission_id = ANY(%s::uuid[]) ORDER BY admission_id",
                ([frozen.admission_id, later.admission_id],),
            )
            observed = {
                str(admission_id): (status, generation_ref)
                for admission_id, status, generation_ref in cur.fetchall()
            }

    assert feedback["verified_admission_ids"] == [frozen.admission_id]
    assert observed[frozen.admission_id] == ("published", successor)
    assert observed[later.admission_id] == ("accepted", None)


def test_recovery_promotion_refuses_frozen_admission_set_drift(
    clean_runtime: str,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    deployment = "test_recovery_feedback_drift"
    run_ids = [str(uuid4()), str(uuid4())]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        envelopes = []
        for index, run_id in enumerate(run_ids):
            envelope = ensure_envelope(
                conn,
                deployment_name=deployment,
                producer_id=f"source_{index}",
                producer_kind="source",
                intent="source_sync",
                admission_mode="fast_track",
                status="accepted",
                idempotency_key=f"key-{index}",
                source=f"source_{index}",
                run_id=run_id,
            )
            attach_payload_ref(
                conn,
                admission_id=envelope.admission_id,
                ref_key=f"run-{index}",
                payload={
                    "kind": "chronos_graph_run",
                    "source": f"source_{index}",
                    "run_id": run_id,
                },
            )
            envelopes.append(envelope)
        frozen_generation = store.checkpoint_generation(
            deployment_name=deployment,
            status="blocked",
            metadata={
                "blocked_reason": {"reason": "ingest_source_incomplete"}
            },
        )
        apply_generation_feedback(
            conn,
            deployment_name=deployment,
            generation_id=frozen_generation,
            expected_admission_ids=[
                envelope.admission_id for envelope in envelopes
            ],
        )
        successor = store.checkpoint_generation(
            deployment_name=deployment,
            status="published",
        )
        with pytest.raises(SubstrateError) as drift:
            apply_generation_feedback(
                conn,
                deployment_name=deployment,
                generation_id=successor,
                expected_admission_ids=[envelopes[0].admission_id],
                recovery_from_generation_id=frozen_generation,
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, target_generation_ref "
                "FROM okg.admission_envelopes ORDER BY admission_id"
            )
            observed = list(cur.fetchall())

    assert drift.value.data["reason"] == "recovery_admission_set_changed"
    assert observed == [
        ("blocked", frozen_generation),
        ("blocked", frozen_generation),
    ]


def test_chronos_generation_feedback_refuses_intervening_branch_write(
    clean_runtime: str,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    deployment = "test_intervening_feedback"
    old_run_id = str(uuid4())
    new_run_id = str(uuid4())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "feedback:old",
                    "document",
                    {"title": "old"},
                    {"id": "feedback:old"},
                    {},
                )
            ],
            source="old_source",
            run_id=old_run_id,
            strict_endpoints=True,
        )
        generation_id = store.checkpoint_generation(
            deployment_name=deployment,
            status="published",
        )
        with conn.transaction():
            old_envelope = ensure_envelope(
                conn,
                deployment_name=deployment,
                producer_id="old_source",
                producer_kind="source",
                intent="source_sync",
                admission_mode="fast_track",
                status="accepted",
                idempotency_key="old-key",
                source="old_source",
                run_id=old_run_id,
            )
            attach_payload_ref(
                conn,
                admission_id=old_envelope.admission_id,
                ref_key="old-run",
                payload={
                    "kind": "chronos_graph_run",
                    "source": "old_source",
                    "run_id": old_run_id,
                },
            )

        store.apply_facts(
            [
                NodeFact(
                    "feedback:new",
                    "document",
                    {"title": "new"},
                    {"id": "feedback:new"},
                    {},
                )
            ],
            source="new_source",
            run_id=new_run_id,
            strict_endpoints=True,
        )
        with conn.transaction():
            new_envelope = ensure_envelope(
                conn,
                deployment_name=deployment,
                producer_id="new_source",
                producer_kind="source",
                intent="source_sync",
                admission_mode="fast_track",
                status="accepted",
                idempotency_key="new-key",
                source="new_source",
                run_id=new_run_id,
            )
            attach_payload_ref(
                conn,
                admission_id=new_envelope.admission_id,
                ref_key="new-run",
                payload={
                    "kind": "chronos_graph_run",
                    "source": "new_source",
                    "run_id": new_run_id,
                },
            )

        with pytest.raises(SubstrateError) as stale:
            apply_generation_feedback(
                conn,
                deployment_name=deployment,
                generation_id=generation_id,
                expected_admission_ids=[old_envelope.admission_id],
            )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT admission_id::text, status, target_generation_ref
                  FROM okg.admission_envelopes
                 WHERE admission_id IN (%s::uuid, %s::uuid)
                 ORDER BY admission_id
                """,
                (old_envelope.admission_id, new_envelope.admission_id),
            )
            observed = {
                str(admission_id): (status, generation_ref)
                for admission_id, status, generation_ref in cur.fetchall()
            }

    assert stale.value.data["reason"] == "intervening_graph_write"
    assert observed == {
        old_envelope.admission_id: ("accepted", None),
        new_envelope.admission_id: ("accepted", None),
    }


def test_admission_cli_lists_and_shows_envelope(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction():
            envelope = ensure_envelope(
                conn,
                deployment_name="test_cli",
                producer_id="cli_source",
                producer_kind="source",
                intent="source_sync",
                admission_mode="fast_track",
                status="accepted",
                idempotency_key="cli-key",
                source="cli_source",
                run_id=str(uuid4()),
            )

    rc = main([
        "--json",
        "admission",
        "list",
        "--dsn",
        clean_runtime,
        "--deployment",
        "test_cli",
    ])
    assert rc == 0
    listed = json.loads(capsys.readouterr().out)
    assert listed["count"] == 1
    assert listed["admissions"][0]["admission_id"] == envelope.admission_id
    assert listed["admissions"][0]["status"] == "accepted"

    rc = main([
        "--json",
        "admission",
        "show",
        envelope.admission_id,
        "--dsn",
        clean_runtime,
        "--deployment",
        "test_cli",
    ])
    assert rc == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["admission"]["admission_id"] == envelope.admission_id
    assert shown["admission"]["producer_id"] == "cli_source"
    assert [event["event_type"] for event in shown["events"]] == ["created"]


# --- Issue #2966: a fork elsewhere must not block the parent's feedback ----


def _issue2966_publisher(dsn: str, deployment: str, *, branch: str = "default"):
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    return Publisher(
        dsn,
        deployment,
        branch_name=branch,
        catalog_identity=bind_test_catalog_writer(dsn, deployment),
    )


def _issue2966_seed(dsn: str) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from tests.pytest.catalog_fixture_builders import seed_catalog

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["i2966_doc"])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact("i2966:a", "i2966_doc", {}, {"fixture": "a"}, {"revision": 1})],
            source="i2966_fixture",
        )


def _issue2966_write(dsn: str, *, branch: str, key: str) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(key, "i2966_doc", {}, {"fixture": key}, {"revision": 1})],
            branch=branch,
            source="i2966_fixture",
        )


def _issue2966_fork(
    dsn: str, deployment: str, branch: str, *, from_branch: str
) -> None:
    """Fork like `okg branch create`, which can only fork from default."""
    if from_branch == "default":
        rc = main([
            "--json",
            "branch",
            "create",
            branch,
            "--role",
            "experiment",
            "--deployment",
            deployment,
            "--dsn",
            dsn,
        ])
        assert rc == 0
        return

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        base = store.latest_generation(
            deployment_name=deployment, branch=from_branch, status="published"
        )
        store.create_branch(
            branch,
            from_branch=from_branch,
            metadata={
                "kind": "okg_branch",
                "deployment_name": deployment,
                "branch_name": branch,
                "role": "experiment",
                "status": "active",
                "base_branch": from_branch,
                "base_generation_id": base.checkpoint_id if base else None,
                "created_by": "test_admission issue 2966",
            },
        )


def _issue2966_run_once(dsn: str, deployment: str, *, branch: str = "default"):
    """One inline `okg run --once` publish plus the CLI's feedback step."""
    from okg.substrate.cli.main import _apply_inline_publish_admission_feedback

    summary = _issue2966_publisher(dsn, deployment, branch=branch).cycle()
    feedback = _apply_inline_publish_admission_feedback(
        dsn=dsn, deployment=deployment, summary=summary
    )
    return summary, feedback


def _issue2966_feedback_refusal(dsn: str, deployment: str, generation_id: str):
    with psycopg.connect(dsn, autocommit=True) as conn:
        with pytest.raises(SubstrateError) as refused:
            with conn.transaction():
                apply_generation_feedback(
                    conn,
                    deployment_name=deployment,
                    generation_id=generation_id,
                )
    return refused.value.data


def test_default_publishes_again_after_an_experiment_branch_is_created(
    clean_runtime: str,
) -> None:
    deployment = "i2966_after_fork"
    _issue2966_seed(clean_runtime)
    first, first_feedback = _issue2966_run_once(clean_runtime, deployment)
    assert first["status"] == "published"
    assert first_feedback is not None and first_feedback["status"] == "published"

    _issue2966_fork(clean_runtime, deployment, "exp1", from_branch="default")

    # Nothing changed on default, so each later run reuses the generation and
    # re-applies its feedback. Before #2966 every one of these was refused
    # with reason branch_structure_changed.
    for _ in range(3):
        again, feedback = _issue2966_run_once(clean_runtime, deployment)
        assert again["status"] == "published"
        assert again["generation_id"] == first["generation_id"]
        assert feedback is not None
        assert feedback["status"] == "published"
        assert feedback["branch_name"] == "default"


def test_fork_of_a_fork_and_an_experiment_publish_keep_feedback_working(
    clean_runtime: str,
) -> None:
    deployment = "i2966_fork_of_fork"
    _issue2966_seed(clean_runtime)
    first, _ = _issue2966_run_once(clean_runtime, deployment)

    _issue2966_fork(clean_runtime, deployment, "exp1", from_branch="default")
    _issue2966_fork(clean_runtime, deployment, "exp2", from_branch="exp1")
    _issue2966_fork(clean_runtime, deployment, "exp3", from_branch="default")
    # A write on a child branch is outside default's lineage.
    _issue2966_write(clean_runtime, branch="exp1", key="i2966:exp1-only")

    again, feedback = _issue2966_run_once(clean_runtime, deployment)
    assert again["generation_id"] == first["generation_id"]
    assert feedback is not None and feedback["status"] == "published"

    exp_first, exp_feedback = _issue2966_run_once(
        clean_runtime, deployment, branch="exp1"
    )
    assert exp_first["status"] == "published"
    assert exp_first["generation_id"] != first["generation_id"]
    assert exp_feedback is not None
    assert exp_feedback["branch_name"] == "exp1"
    assert exp_feedback["status"] == "published"

    _issue2966_fork(clean_runtime, deployment, "exp4", from_branch="exp1")
    exp_again, exp_again_feedback = _issue2966_run_once(
        clean_runtime, deployment, branch="exp1"
    )
    assert exp_again["generation_id"] == exp_first["generation_id"]
    assert exp_again_feedback is not None
    assert exp_again_feedback["status"] == "published"


@pytest.mark.parametrize("write_before_fork", [True, False])
def test_feedback_still_refuses_a_default_write_across_a_fork(
    clean_runtime: str,
    write_before_fork: bool,
) -> None:
    deployment = "i2966_write_across_fork"
    _issue2966_seed(clean_runtime)
    first, _ = _issue2966_run_once(clean_runtime, deployment)

    if write_before_fork:
        _issue2966_write(clean_runtime, branch="default", key="i2966:late")
        _issue2966_fork(clean_runtime, deployment, "exp1", from_branch="default")
    else:
        _issue2966_fork(clean_runtime, deployment, "exp1", from_branch="default")
        _issue2966_write(clean_runtime, branch="default", key="i2966:late")

    refused = _issue2966_feedback_refusal(
        clean_runtime, deployment, str(first["generation_id"])
    )
    assert refused["reason"] == "intervening_graph_write"
    assert len(refused["lineage_segment_ids"]) == 3


def test_feedback_still_refuses_a_rerooted_default_head(
    clean_runtime: str,
) -> None:
    """A head moved off the checkpoint's lineage cannot be proven unchanged."""
    deployment = "i2966_rerooted"
    _issue2966_seed(clean_runtime)
    first, _ = _issue2966_run_once(clean_runtime, deployment)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction(), conn.cursor() as cur:
            # Re-root default directly beneath its checkpoint segment, the
            # shape the pre-#2862 restore produced.
            cur.execute(
                """
                SELECT c.segment_id, head.live_lo, head.live_hi
                  FROM _chronos_branch_interval_checkpoints c
                  JOIN _chronos_branch_interval_branches b
                    ON b.branch_id = c.branch_id
                  JOIN _chronos_branch_interval_segments head
                    ON head.segment_id = b.current_segment_id
                 WHERE c.branch_id = 'default'
                 ORDER BY c.created_at DESC
                 LIMIT 1
                """
            )
            checkpoint_segment_id, live_lo, live_hi = cur.fetchone()
            cur.execute(
                "SELECT max(segment_id) + 1 FROM _chronos_branch_interval_segments"
            )
            (rerooted_id,) = cur.fetchone()
            cur.execute(
                """
                INSERT INTO _chronos_branch_interval_segments
                  (segment_id, parent_segment_id, owner_branch_id, segment_kind,
                   live_lo, live_hi, branch_point, created_at, metadata)
                VALUES (%s, %s, 'default', 'mutable', %s, %s, %s, now(), '{}')
                """,
                (
                    rerooted_id,
                    checkpoint_segment_id,
                    live_lo,
                    live_hi,
                    live_lo + (live_hi - live_lo) // 2,
                ),
            )
            cur.execute(
                """
                UPDATE _chronos_branch_interval_branches
                   SET current_segment_id = %s
                 WHERE branch_id = 'default'
                """,
                (rerooted_id,),
            )

    refused = _issue2966_feedback_refusal(
        clean_runtime, deployment, str(first["generation_id"])
    )
    assert refused["reason"] == "branch_structure_changed"
    assert refused["current_segment_id"] == rerooted_id
    assert refused["lineage_stop"] == "not_a_fork_lineage"


def _issue2966_insert_segment(
    cur, segment_id: int, parent: int | None, owner: str | None, kind: str,
    live_lo: int, live_hi: int, created_at=None,
) -> None:
    cur.execute(
        """
        INSERT INTO _chronos_branch_interval_segments
          (segment_id, parent_segment_id, owner_branch_id, segment_kind,
           live_lo, live_hi, branch_point, created_at, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, coalesce(%s, now()), '{}')
        """,
        (
            segment_id, parent, owner, kind, live_lo, live_hi,
            live_lo + (live_hi - live_lo) // 2, created_at,
        ),
    )


def _issue2966_checkpoint_segment(cur):
    """(parent, live_hi, created_at) of default's latest checkpoint segment."""
    cur.execute(
        """
        SELECT snap.parent_segment_id, snap.live_hi, snap.created_at
          FROM _chronos_branch_interval_checkpoints c
          JOIN _chronos_branch_interval_segments snap
            ON snap.segment_id = c.segment_id
         WHERE c.branch_id = 'default'
         ORDER BY c.created_at DESC
         LIMIT 1
        """
    )
    return cur.fetchone()


def _issue2966_rebuild_default_head(dsn: str, build) -> None:
    """Replace default's head with segments ``build`` inserts.

    ``build(cur, head, next_id, live_lo, live_hi)`` inserts segments above the
    current head and returns the new head segment id.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.segment_id, s.live_lo, s.live_hi
                  FROM _chronos_branch_interval_branches b
                  JOIN _chronos_branch_interval_segments s
                    ON s.segment_id = b.current_segment_id
                 WHERE b.branch_id = 'default'
                """
            )
            head, live_lo, live_hi = cur.fetchone()
            cur.execute(
                "SELECT max(segment_id) + 1 FROM _chronos_branch_interval_segments"
            )
            (next_id,) = cur.fetchone()
            new_head = build(cur, head, next_id, live_lo, live_hi)
            cur.execute(
                """
                UPDATE _chronos_branch_interval_branches
                   SET current_segment_id = %s
                 WHERE branch_id = 'default'
                """,
                (new_head,),
            )


@pytest.mark.parametrize(
    ("middle_kind", "middle_owner"),
    [
        ("checkpoint", None),
        ("mutable", None),
        ("fork_base", "default"),
        ("fork_base", "exp9"),
        ("compacted", None),
    ],
)
def test_feedback_refuses_a_lineage_segment_a_fork_does_not_make(
    clean_runtime: str,
    middle_kind: str,
    middle_owner: str | None,
) -> None:
    """head(default) -> <middle> -> the checkpoint's continuation."""
    deployment = "i2966_bad_middle"
    _issue2966_seed(clean_runtime)
    first, _ = _issue2966_run_once(clean_runtime, deployment)

    def build(cur, head, next_id, live_lo, live_hi):
        _issue2966_insert_segment(
            cur, next_id, head, middle_owner, middle_kind, live_lo, live_lo + 1
        )
        _issue2966_insert_segment(
            cur, next_id + 1, next_id, "default", "mutable", live_lo + 1, live_hi
        )
        return next_id + 1

    _issue2966_rebuild_default_head(clean_runtime, build)
    refused = _issue2966_feedback_refusal(
        clean_runtime, deployment, str(first["generation_id"])
    )
    assert refused["reason"] == "branch_structure_changed"
    assert refused["lineage_stop"] == "not_a_fork_lineage"


@pytest.mark.parametrize(
    ("inner_owner", "inner_kind"), [("exp9", "mutable"), ("default", "checkpoint")]
)
def test_feedback_refuses_an_inner_segment_that_is_not_this_branchs_own(
    clean_runtime: str,
    inner_owner: str,
    inner_kind: str,
) -> None:
    """head(default) -> fork base -> <inner> -> fork base -> continuation."""
    deployment = "i2966_inner_segment"
    _issue2966_seed(clean_runtime)
    first, _ = _issue2966_run_once(clean_runtime, deployment)

    def build(cur, head, next_id, live_lo, live_hi):
        _issue2966_insert_segment(
            cur, next_id, head, None, "fork_base", live_lo, live_lo + 1
        )
        _issue2966_insert_segment(
            cur, next_id + 1, next_id, inner_owner, inner_kind, live_lo + 1,
            live_hi,
        )
        _issue2966_insert_segment(
            cur, next_id + 2, next_id + 1, None, "fork_base", live_lo + 1,
            live_lo + 2,
        )
        _issue2966_insert_segment(
            cur, next_id + 3, next_id + 2, "default", "mutable", live_lo + 2,
            live_hi,
        )
        return next_id + 3

    _issue2966_rebuild_default_head(clean_runtime, build)
    refused = _issue2966_feedback_refusal(
        clean_runtime, deployment, str(first["generation_id"])
    )
    assert refused["reason"] == "branch_structure_changed"
    assert refused["lineage_stop"] == "not_a_fork_lineage"


def test_feedback_refuses_a_sibling_that_is_not_the_checkpoint_continuation(
    clean_runtime: str,
) -> None:
    """Right parent and owner, but not the segment the checkpoint split off."""
    deployment = "i2966_sibling"
    _issue2966_seed(clean_runtime)
    first, _ = _issue2966_run_once(clean_runtime, deployment)

    def build(cur, head, next_id, live_lo, live_hi):
        cur.execute(
            "SELECT parent_segment_id FROM _chronos_branch_interval_segments "
            "WHERE segment_id = %s",
            (head,),
        )
        (parent,) = cur.fetchone()
        _issue2966_insert_segment(
            cur, next_id, parent, "default", "mutable", live_lo + 1, live_hi
        )
        return next_id

    _issue2966_rebuild_default_head(clean_runtime, build)
    refused = _issue2966_feedback_refusal(
        clean_runtime, deployment, str(first["generation_id"])
    )
    assert refused["reason"] == "branch_structure_changed"
    assert refused["lineage_stop"] == "not_the_checkpoint_continuation"


def test_feedback_refuses_a_lineage_that_ends_on_a_fork_base(
    clean_runtime: str,
) -> None:
    """A fork base in the continuation's place, even with its interval and time."""
    deployment = "i2966_ends_on_fork_base"
    _issue2966_seed(clean_runtime)
    first, _ = _issue2966_run_once(clean_runtime, deployment)

    def build(cur, head, next_id, live_lo, live_hi):
        parent, snap_hi, snap_created = _issue2966_checkpoint_segment(cur)
        _issue2966_insert_segment(
            cur, next_id, parent, None, "fork_base", snap_hi, snap_hi + 1,
            snap_created,
        )
        _issue2966_insert_segment(
            cur, next_id + 1, next_id, "default", "mutable", live_lo + 1, live_hi
        )
        return next_id + 1

    _issue2966_rebuild_default_head(clean_runtime, build)
    refused = _issue2966_feedback_refusal(
        clean_runtime, deployment, str(first["generation_id"])
    )
    assert refused["reason"] == "branch_structure_changed"
    assert refused["lineage_stop"] == "not_a_fork_lineage"


def test_feedback_refuses_a_head_whose_parent_is_missing(
    clean_runtime: str,
) -> None:
    """The continuation's interval and time, but a parent that does not exist."""
    deployment = "i2966_missing_parent"
    _issue2966_seed(clean_runtime)
    first, _ = _issue2966_run_once(clean_runtime, deployment)

    def build(cur, head, next_id, live_lo, live_hi):
        _parent, snap_hi, snap_created = _issue2966_checkpoint_segment(cur)
        _issue2966_insert_segment(
            cur, next_id, next_id + 1000, "default", "mutable", snap_hi, live_hi,
            snap_created,
        )
        return next_id

    _issue2966_rebuild_default_head(clean_runtime, build)
    refused = _issue2966_feedback_refusal(
        clean_runtime, deployment, str(first["generation_id"])
    )
    assert refused["reason"] == "branch_structure_changed"
    assert refused["lineage_stop"] == "not_a_fork_lineage"


def test_feedback_refuses_a_parent_cycle(clean_runtime: str) -> None:
    """head -> fork base F -> mutable M -> F -> M ... never reaches the checkpoint."""
    deployment = "i2966_cycle"
    _issue2966_seed(clean_runtime)
    first, _ = _issue2966_run_once(clean_runtime, deployment)

    def build(cur, head, next_id, live_lo, live_hi):
        fork_base, looped, new_head = next_id, next_id + 1, next_id + 2
        _issue2966_insert_segment(
            cur, fork_base, None, None, "fork_base", live_lo, live_lo + 1
        )
        _issue2966_insert_segment(
            cur, looped, fork_base, "default", "mutable", live_lo + 1, live_hi
        )
        cur.execute(
            "UPDATE _chronos_branch_interval_segments "
            "SET parent_segment_id = %s WHERE segment_id = %s",
            (looped, fork_base),
        )
        _issue2966_insert_segment(
            cur, new_head, fork_base, "default", "mutable", live_lo + 1, live_hi
        )
        return new_head

    _issue2966_rebuild_default_head(clean_runtime, build)
    refused = _issue2966_feedback_refusal(
        clean_runtime, deployment, str(first["generation_id"])
    )
    assert refused["reason"] == "branch_structure_changed"
    assert refused["lineage_stop"] == "parent_cycle"


def test_feedback_refuses_a_lineage_longer_than_the_depth_limit(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import admission as admission_mod

    deployment = "i2966_depth"
    _issue2966_seed(clean_runtime)
    first, _ = _issue2966_run_once(clean_runtime, deployment)
    _issue2966_fork(clean_runtime, deployment, "exp1", from_branch="default")
    _issue2966_fork(clean_runtime, deployment, "exp2", from_branch="default")

    # Two forks leave a five-segment lineage: head, fork base, continuation,
    # fork base, the checkpoint's continuation.
    monkeypatch.setattr(admission_mod, "_CHRONOS_FEEDBACK_LINEAGE_MAX_DEPTH", 5)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction():
            accepted = apply_generation_feedback(
                conn,
                deployment_name=deployment,
                generation_id=str(first["generation_id"]),
            )
    assert accepted["status"] == "published"

    monkeypatch.setattr(admission_mod, "_CHRONOS_FEEDBACK_LINEAGE_MAX_DEPTH", 3)
    refused = _issue2966_feedback_refusal(
        clean_runtime, deployment, str(first["generation_id"])
    )
    assert refused["reason"] == "branch_structure_changed"
    assert refused["lineage_stop"] == "depth_limit"
    assert len(refused["lineage_segment_ids"]) == 4


def _insert_generation(
    cur,
    *,
    deployment_name: str,
    status: str,
    fact_watermark: int = 0,
    edge_fact_watermark: int = 0,
    blocked_reason: dict | None = None,
) -> int:
    cur.execute(
        """
        INSERT INTO okg.graph_generations
          (deployment_name, status, fact_watermark, edge_fact_watermark,
           catalog_version_id, published_at, blocked_reason,
           correlation_id)
        VALUES
          (%s, %s, %s, %s, 1, now(), %s::jsonb, %s)
        RETURNING generation_id
        """,
        (
            deployment_name,
            status,
            fact_watermark,
            edge_fact_watermark,
            Json(blocked_reason) if blocked_reason is not None else None,
            f"admission-test-{uuid4()}",
        ),
    )
    return int(cur.fetchone()[0])
