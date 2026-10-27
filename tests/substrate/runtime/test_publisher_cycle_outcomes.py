"""Every publisher cycle records a typed outcome (publisher-starvation-4039).

A cycle that yields, or finds a run still writing, ends as a DBOS SUCCESS: the
status alone cannot tell it from a publish. The publisher is the third
participant of the operational-outcome contract (beside ``okg_embed_batch`` and
``okg_source_sync``): ``clean`` for a publish, a blocked checkpoint or a no-op,
``skipped`` with a reason and a subject for every skip. It has its own
cutover, from migration 0045, so its SUCCESS rows from before that are legacy.

The workflow cases run the registered publisher workflow on a real DBOS
runtime, as the worker does, and read ``okg.workflow_operational_outcomes``.
"""
from __future__ import annotations

import contextlib
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from tests.substrate.runtime.test_publisher_cycle_overdue import (  # noqa: F401
    CONTROL_QUEUE,
    DEPLOYMENT,
    _Cycle,
    cyc,
)

# `cyc` is a fixture imported by name; each test takes it as an argument.
# ruff: noqa: F811

pytestmark = pytest.mark.postgres


def _outcome(dsn: str, workflow_id: str) -> tuple[str, dict]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT outcome, details, succeeded_count, failed_count "
            "FROM okg.workflow_operational_outcomes WHERE workflow_uuid = %s",
            (workflow_id,),
        ).fetchone()
    assert row is not None, f"no outcome recorded for {workflow_id}"
    return row[0], dict(row[1])


def _status(dsn: str, workflow_id: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return conn.execute(
            "SELECT status FROM dbos.workflow_status WHERE workflow_uuid = %s",
            (workflow_id,),
        ).fetchone()[0]


@pytest.mark.dbos_integration
def test_each_cycle_records_clean_or_skipped_with_reason_and_subject(
    cyc: _Cycle, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("dbos")
    from dbos import DBOS, DBOSConfig

    from okg.substrate.ingest import run_base
    from okg.substrate.publisher.publisher import Publisher
    from okg.substrate.runtime import partition_queue, publisher_workflow
    from okg.substrate.runtime.dbos_enqueue import enqueue_deployment_workflow
    from okg.substrate.runtime.partition_queue import (
        register_deployment_partition_queue,
    )
    from okg.substrate.runtime.publisher_schedule import (
        publisher_deduplication_id,
        read_publisher_freshness,
    )
    from okg.substrate.runtime.workflow_operational_outcomes import (
        check_workflow_operational_outcomes,
    )

    dsn = cyc.dsn
    with contextlib.suppress(Exception):
        DBOS.destroy()
    partition_queue._REGISTERED.clear()
    DBOS(config=DBOSConfig(
        name="okg-publisher-outcome-test",
        system_database_url=dsn,
        database_url=dsn,
        run_admin_server=False,
        application_version="publisher-outcome-test-v1",
    ))
    register_deployment_partition_queue(deployment=DEPLOYMENT)
    publisher_workflow.register_publisher_workflow(deployment=DEPLOYMENT)
    DBOS.launch()
    ticks = iter(range(1, 100))

    def cron_cycle() -> str:
        """Enqueue a cycle as the cron tick does (its deduplication id) and
        wait for it."""
        enqueued = enqueue_deployment_workflow(
            deployment=DEPLOYMENT,
            dsn=dsn,
            workflow_name=publisher_workflow.PUBLISHER_WORKFLOW_NAME,
            deduplication_id=publisher_deduplication_id(DEPLOYMENT) + f":{next(ticks)}",
        )
        DBOS.retrieve_workflow(enqueued.workflow_id).get_result()
        return enqueued.workflow_id

    try:
        # 1. A publish is clean, and carries the generation it cut.
        cyc.stage_unpublished()
        before = cyc.generation()
        published = cron_cycle()
        assert _status(dsn, published) == "SUCCESS"
        outcome, details = _outcome(dsn, published)
        assert outcome == "clean"
        assert details["status"] == "published"
        assert details["generation_id"] == cyc.generation() != before
        assert details["branch_name"] == "default"
        clean_recorded_at = read_publisher_freshness(
            deployment=DEPLOYMENT, dsn=dsn,
        ).last_cycle_at
        assert clean_recorded_at is not None

        # 2. A current publisher yields to a pending sync: skipped, naming the
        #    sync; the DBOS status is still SUCCESS.
        cyc.stage_unpublished()
        sync_id = cyc.sync_row("PENDING")
        skipped = cron_cycle()
        assert _status(dsn, skipped) == "SUCCESS"
        outcome, details = _outcome(dsn, skipped)
        assert outcome == "skipped"
        assert (details["reason"], details["subject"]) == (
            "source_sync_active", sync_id,
        )

        # 3. Overdue, with a run still writing: skipped naming the run.
        cyc.age_publisher()
        run_id = "run-writing-outcome"
        cyc.start_run(run_id, "aaa:outcome-row")
        in_progress = cron_cycle()
        assert _status(dsn, in_progress) == "SUCCESS"
        outcome, details = _outcome(dsn, in_progress)
        assert outcome == "skipped"
        assert (details["reason"], details["subject"]) == (
            "source_run_in_progress", run_id,
        )
        assert details["overdue"] is True
        cyc.end_run(run_id)

        # 4. A run that starts after the counts is refused by the checkpoint
        #    cut recheck: a recorded skip, a DBOS SUCCESS, never an ERROR.
        state = {"cut": False}
        real_refuse = Publisher._chronos_refuse_writes_since_counted
        real_running = run_base.runs_in_progress

        def refuse(self, conn, signature, stage="checkpoint_cut", lock=True):
            if lock and stage == "checkpoint_cut":
                state["cut"] = True
            return real_refuse(self, conn, signature, stage=stage, lock=lock)

        def running(conn, *, deployment, own_run_ids=()):
            if state["cut"]:
                return [{
                    "run_id": "run-late", "source": "svc.aaa",
                    "checkpoint_id": "okg-run-base:default:run-late",
                    "owner": {"state": "alive", "pid": 1, "backend_start": "now"},
                }]
            return real_running(
                conn, deployment=deployment, own_run_ids=own_run_ids,
            )

        monkeypatch.setattr(
            Publisher, "_chronos_refuse_writes_since_counted", refuse,
        )
        monkeypatch.setattr(run_base, "runs_in_progress", running)
        late = cron_cycle()
        monkeypatch.setattr(
            Publisher, "_chronos_refuse_writes_since_counted", real_refuse,
        )
        monkeypatch.setattr(run_base, "runs_in_progress", real_running)
        assert state["cut"], "the cut recheck never ran"
        assert _status(dsn, late) == "SUCCESS"
        outcome, details = _outcome(dsn, late)
        assert outcome == "skipped"
        assert (details["reason"], details["subject"]) == (
            "source_run_in_progress", "run-late",
        )

        # 5. The staging replay gate is a skip with its own reason.
        monkeypatch.setenv("OKG_ENVIRONMENT_TIER", "staging")
        staging = cron_cycle()
        monkeypatch.delenv("OKG_ENVIRONMENT_TIER")
        assert _status(dsn, staging) == "SUCCESS"
        outcome, details = _outcome(dsn, staging)
        assert outcome == "skipped"
        assert details["reason"] == "staging_replay_unarmed"
        assert details["subject"] != "unspecified"

        # 6. The skips never moved freshness; only the publish did.
        fresh = read_publisher_freshness(deployment=DEPLOYMENT, dsn=dsn)
        # (step 3 moved the recorded times back two hours.)
        assert fresh.last_cycle_at == clean_recorded_at - timedelta(hours=2)

        # 7. The reader sees a typed outcome for every publisher SUCCESS.
        verdict = check_workflow_operational_outcomes(
            deployment=DEPLOYMENT, dsn=dsn,
        )
        assert verdict.code == "workflow_outcome_clean", verdict.to_dict()
        # (The two cycles aged back two hours in step 3 now precede this
        # database's cutover, so they count as legacy.)
        assert verdict.checked_count + verdict.legacy_count == 5
        assert verdict.unknown_count == verdict.global_unknown_count == 0
    finally:
        DBOS.destroy()


def test_a_no_run_base_guard_skip_names_the_sync(cyc: _Cycle) -> None:
    from okg.substrate.runtime import publisher_workflow

    summary = {
        "deployment": DEPLOYMENT, "branch_name": "default",
        "status": "skipped", "publish_skipped": True,
        "skipped": "no_run_base_guard", "blocking_workflow_id": "sync-1",
        "scheduled": {"overdue": True},
    }
    mapped = publisher_workflow._publisher_operational_outcome(
        summary, branch_name="default",
    )
    assert mapped["outcome"] == "skipped"
    assert (mapped["succeeded_count"], mapped["failed_count"]) == (0, 0)
    assert mapped["details"]["reason"] == "no_run_base_guard"
    assert mapped["details"]["subject"] == "sync-1"


def test_a_blocked_checkpoint_and_a_noop_are_clean(cyc: _Cycle) -> None:
    from okg.substrate.runtime import publisher_workflow

    for status in ("blocked", "published"):
        mapped = publisher_workflow._publisher_operational_outcome(
            {"status": status, "generation_id": "gen-9"}, branch_name="default",
        )
        assert mapped["outcome"] == "clean"
        assert mapped["details"]["generation_id"] == "gen-9"


def test_a_summary_with_no_status_is_not_recorded_as_clean(cyc: _Cycle) -> None:
    from okg.substrate.runtime import publisher_workflow
    from okg.substrate.runtime.workflow_operational_outcomes import (
        WorkflowOperationalEvidenceError,
    )

    with pytest.raises(WorkflowOperationalEvidenceError):
        publisher_workflow._publisher_operational_outcome(
            {"generation_id": "gen-9"}, branch_name="default",
        )
    with pytest.raises(WorkflowOperationalEvidenceError):
        publisher_workflow._publisher_operational_outcome(
            {"status": "skipped"}, branch_name="default",
        )


def test_publisher_rows_before_the_cutover_are_legacy_and_later_ones_unknown(
    cyc: _Cycle,
) -> None:
    """The publisher's cutover is its first registered outcome, never earlier
    than its migration. A SUCCESS row that completed before it could not have
    recorded an outcome (a cycle a worker on the old code finishes during a
    rolling upgrade included): legacy. One after it with no outcome is
    unknown, as for the other participants. Before any cycle has registered,
    nothing is owed."""
    from okg.substrate.runtime.workflow_operational_outcomes import (
        PUBLISHER_MIGRATION_ID,
        check_workflow_operational_outcomes,
    )

    migration_at = datetime.now(timezone.utc) - timedelta(hours=3)
    cutover = datetime.now(timezone.utc) - timedelta(hours=1)
    with psycopg.connect(cyc.dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.schema_migrations SET applied_at = %s "
            "WHERE migration_id = %s",
            (migration_at, PUBLISHER_MIGRATION_ID),
        )

        def add(at: datetime) -> str:
            workflow_id = f"cycle-{uuid.uuid4().hex[:10]}"
            ms = int(at.timestamp() * 1000)
            conn.execute(
                "INSERT INTO dbos.workflow_status (workflow_uuid, name, status, "
                "queue_name, created_at, updated_at, completed_at) VALUES "
                "(%s, 'okg_publisher_cycle', 'SUCCESS', %s, %s, %s, %s)",
                (workflow_id, CONTROL_QUEUE, ms - 100, ms, ms),
            )
            return workflow_id

        # After the migration, before any registration: the old code's cycles.
        add(cutover - timedelta(minutes=30))
        add(cutover - timedelta(minutes=10))
        verdict = check_workflow_operational_outcomes(
            deployment=DEPLOYMENT, dsn=cyc.dsn,
        )
        assert verdict.legacy_count == 2
        assert verdict.unknown_count == 0
        assert verdict.code != "workflow_outcome_unknown", verdict.to_dict()

        # The first cycle of the new code registers and records an outcome.
        first = add(cutover)
        conn.execute(
            "INSERT INTO okg.workflow_operational_registrations "
            "(workflow_uuid, deployment_name, workflow_name, queue_name, "
            "contract_version, registered_at) "
            "VALUES (%s, %s, 'okg_publisher_cycle', %s, 1, %s)",
            (first, DEPLOYMENT, CONTROL_QUEUE, cutover),
        )
        conn.execute(
            "INSERT INTO okg.workflow_operational_outcomes "
            "(workflow_uuid, contract_version, outcome, succeeded_count, "
            "failed_count, semantic_units, details, recorded_at) "
            "VALUES (%s, 1, 'clean', 1, 0, '{}'::jsonb, '{}'::jsonb, %s)",
            (first, cutover),
        )
        verdict = check_workflow_operational_outcomes(
            deployment=DEPLOYMENT, dsn=cyc.dsn,
        )
        assert verdict.legacy_count == 2 and verdict.checked_count == 1
        assert verdict.unknown_count == 0
        assert verdict.code == "workflow_outcome_clean", verdict.to_dict()

        add(cutover + timedelta(minutes=30))
        verdict = check_workflow_operational_outcomes(
            deployment=DEPLOYMENT, dsn=cyc.dsn,
        )
        assert verdict.legacy_count == 2
        assert verdict.global_unknown_count + verdict.unknown_count == 1
        assert verdict.code == "workflow_outcome_unknown"


def test_before_any_registration_a_publisher_success_is_legacy(
    cyc: _Cycle,
) -> None:
    """Rolling upgrade, first minutes: the migration has run, no worker has the
    new code yet, and the old code's cycles keep finishing. None is unknown."""
    from okg.substrate.runtime.workflow_operational_outcomes import (
        check_workflow_operational_outcomes,
    )

    ms = int((datetime.now(timezone.utc) - timedelta(minutes=5)).timestamp() * 1000)
    with psycopg.connect(cyc.dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO dbos.workflow_status (workflow_uuid, name, status, "
            "queue_name, created_at, updated_at, completed_at) VALUES "
            "('old-code-cycle', 'okg_publisher_cycle', 'SUCCESS', %s, %s, %s, %s)",
            (CONTROL_QUEUE, ms - 100, ms, ms),
        )

    verdict = check_workflow_operational_outcomes(deployment=DEPLOYMENT, dsn=cyc.dsn)

    assert verdict.legacy_count == 1 and verdict.unknown_count == 0
    assert verdict.global_unknown_count == 0, verdict.to_dict()
