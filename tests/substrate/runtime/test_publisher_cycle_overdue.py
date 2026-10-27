"""A scheduled publisher that is overdue publishes past source-sync rows.

PACT publisher-starvation-4039 (issue #4039). The scheduled publisher used to
yield to any ``okg_source_sync`` workflow row ENQUEUED, PENDING or DELAYED,
without bound, at the cron tick, at the cycle start and again inside the
publish lock. A deployment whose syncs overlap every tick, or whose sync runs
for days or is stranded, never published.

These cases run the tick and ``_run_one_cycle`` against the real publisher,
the real DBOS system schema and a real ingest fixture deployment. The yield
bound is the manifest default (600 s); a publisher is made overdue by moving
the generation ledger back in time, so the same cases run unchanged against
origin/dev, where the overdue ones fail because the cycle still skips.
"""
from __future__ import annotations

import logging
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest.service import IngestOptions, run_ingest
from okg.substrate.library.sources.base import NodeFact
from tests.pytest.dbos_system_schema import create_dbos_system_schema
from tests.substrate.ingest.test_run_base_undo import (
    AAA,
    AAA_RECORDS,
    DEPLOYMENT,
    _Fixture,
    _records,
)

pytestmark = pytest.mark.postgres

SYNC_QUEUE = f"okg-{DEPLOYMENT}-source-sync"
CONTROL_QUEUE = f"okg-{DEPLOYMENT}"
WORKFLOW_LOGGER = "okg.substrate.runtime.publisher_workflow"
SCHEDULE_LOGGER = "okg.substrate.runtime.publisher_schedule"


def _ms_ago(seconds: float) -> int:
    return int((time.time() - seconds) * 1000)


class _Cycle:
    """The ingest fixture plus the helpers the cases share."""

    def __init__(self, fx: _Fixture, dsn: str, monkeypatch) -> None:
        self.fx = fx
        self.dsn = dsn
        self.monkeypatch = monkeypatch
        self.edits = 0
        self.live: list[Any] = []

    # -- state ------------------------------------------------------------

    def generation(self) -> str:
        return self.fx.published()["generation"]

    def published_texts(self) -> dict[str, Any]:
        return self.fx.published()["texts"]

    def sync_row(
        self, status: str = "PENDING", *, age_seconds: float = 30.0,
    ) -> str:
        workflow_id = f"sync-{uuid.uuid4().hex[:12]}"
        at = _ms_ago(age_seconds)
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            conn.execute(
                "INSERT INTO dbos.workflow_status "
                "(workflow_uuid, name, status, queue_name, created_at, "
                "updated_at) VALUES (%s, 'okg_source_sync', %s, %s, %s, %s)",
                (workflow_id, status, SYNC_QUEUE, at, at),
            )
        return workflow_id

    def age_publisher(self, seconds: float = 7200.0) -> None:
        """Make the publisher overdue: every generation is older than the
        bound (the manifest default, 600 s)."""
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            # The ledger keeps ``created_at`` as ISO text.
            conn.execute(
                "UPDATE _chronos_branch_interval_checkpoints SET created_at = "
                "to_char((created_at::timestamptz - make_interval(secs => %s)) "
                "AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"')",
                (seconds,),
            )
            # Cycles recorded so far are older too, as a whole: the row, its
            # registration and its outcome keep their order.
            conn.execute(
                "UPDATE okg.workflow_operational_outcomes "
                "SET recorded_at = recorded_at - make_interval(secs => %s)",
                (seconds,),
            )
            conn.execute(
                "UPDATE okg.workflow_operational_registrations "
                "SET registered_at = registered_at - make_interval(secs => %s)",
                (seconds,),
            )
            conn.execute(
                "UPDATE dbos.workflow_status SET "
                "created_at = created_at - %s, updated_at = updated_at - %s, "
                "completed_at = completed_at - %s "
                "WHERE name = 'okg_publisher_cycle'",
                (int(seconds * 1000),) * 3,
            )

    def stage_unpublished(self) -> str:
        """Commit a changed aaa record that no generation holds yet."""
        self.edits += 1
        text = f"changed before the cycle {self.edits}"
        self.fx.write("aaa", {"records": _records(AAA_RECORDS, changed={2: text})})
        result = run_ingest(IngestOptions(
            dsn=self.dsn, deployment=DEPLOYMENT, deployment_dir=self.fx.dep,
            include={"aaa"}, include_predecessors=False,
            include_dependents=False, publish=False,
        ))
        assert result.any_failed is False, result.report
        return text

    # -- the scheduled cycle ----------------------------------------------

    def as_scheduled_cycle(self) -> str:
        """Run as the workflow the cron tick enqueued: a row on the control
        queue carrying the tick's deduplication id."""
        workflow_id = f"cycle-{uuid.uuid4().hex[:12]}"
        at = _ms_ago(1.0)
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            conn.execute(
                "INSERT INTO dbos.workflow_status "
                "(workflow_uuid, name, status, queue_name, created_at, "
                "updated_at, deduplication_id) "
                "VALUES (%s, 'okg_publisher_cycle', 'PENDING', %s, %s, %s, %s)",
                (
                    workflow_id, CONTROL_QUEUE, at, at,
                    f"publisher:{DEPLOYMENT}:cycle:{uuid.uuid4().int % 10**6}",
                ),
            )
        from okg.substrate.runtime import publisher_workflow

        self.monkeypatch.setattr(
            publisher_workflow, "_current_dbos_workflow_id", lambda: workflow_id,
        )
        return workflow_id

    def run_cycle(self) -> dict[str, Any]:
        from okg.substrate.runtime import publisher_workflow

        return publisher_workflow._run_one_cycle(deployment=DEPLOYMENT)

    # -- a source run that is still writing ---------------------------------

    def start_run(self, run_id: str, node_id: str) -> None:
        """Cut a run base under its liveness lock and write one row, then
        leave the run "still going"."""
        from okg.substrate.ingest.run_base import (
            hold_run_liveness,
            run_base_checkpoint_id,
            take_run_base,
        )

        checkpoint_id = run_base_checkpoint_id("default", run_id)
        self.live.append(hold_run_liveness(self.dsn, checkpoint_id))
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            assert take_run_base(
                store, deployment=DEPLOYMENT, branch="default", source=AAA,
                run_id=run_id,
            ) is not None
            store.apply_facts(
                [NodeFact(
                    node_id, "document", {"text": "written by a live run"},
                    {"id": node_id}, {"v": 1},
                )],
                source=AAA, run_id=run_id, strict_endpoints=False,
            )

    def close_runs(self) -> None:
        for conn in self.live:
            conn.close()
        self.live.clear()

    def end_run(self, run_id: str) -> None:
        """The run finishes: its liveness lock goes and its run base is
        released, as the runner does when the run's admission is final."""
        from okg.substrate.ingest.run_base import run_base_checkpoint_id

        self.close_runs()
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            conn.execute(
                "DELETE FROM _chronos_branch_interval_checkpoints "
                "WHERE checkpoint_id = %s",
                (run_base_checkpoint_id("default", run_id),),
            )


@pytest.fixture
def cyc(tmp_path: Path, clean_runtime: str, monkeypatch):
    import okg.substrate.deployments as deployments

    fx = _Fixture(tmp_path, clean_runtime, monkeypatch)
    first = fx.ingest()
    assert first.any_failed is False, first.report
    create_dbos_system_schema(clean_runtime)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(fx.dep.parent))
    monkeypatch.setattr(
        deployments, "require_postgres_dsn",
        lambda _manifest, deployment=None: clean_runtime,
    )
    cycle = _Cycle(fx, clean_runtime, monkeypatch)
    try:
        yield cycle
    finally:
        cycle.close_runs()


@contextmanager
def _logged(level: int, *names: str):
    records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = _Capture(level)
    loggers = [logging.getLogger(name) for name in names]
    previous = [logger.level for logger in loggers]
    for logger in loggers:
        logger.addHandler(handler)
        logger.setLevel(min(level, logger.level or level))
    try:
        yield records
    finally:
        for logger, old in zip(loggers, previous):
            logger.removeHandler(handler)
            logger.setLevel(old)


def _messages(records, level: int) -> list[str]:
    return [r.getMessage() for r in records if r.levelno == level]


# ---------------------------------------------------------------------------
# The cycle
# ---------------------------------------------------------------------------


def test_a_current_publisher_still_yields_to_a_pending_sync(cyc: _Cycle) -> None:
    """Not overdue: today's behaviour. The cycle skips, publishes nothing, and
    logs the yield at INFO with the sync, its status, the age and the bound."""
    cyc.stage_unpublished()
    before = cyc.generation()
    sync_id = cyc.sync_row("PENDING")
    cyc.as_scheduled_cycle()

    with _logged(logging.DEBUG, WORKFLOW_LOGGER) as records:
        summary = cyc.run_cycle()

    assert summary["status"] == "skipped"
    assert summary["skipped"] == "source_sync_active"
    assert summary["blocking_workflow_id"] == sync_id
    assert cyc.generation() == before
    info = [m for m in _messages(records, logging.INFO) if sync_id in m]
    assert info, [r.getMessage() for r in records]
    assert "source_sync_status=PENDING" in info[0]
    assert "freshness_age_seconds=" in info[0]
    assert "bound_seconds=600" in info[0]
    assert not _messages(records, logging.WARNING)


def test_an_overdue_publisher_publishes_past_a_pending_sync(cyc: _Cycle) -> None:
    """Overdue: the cycle ignores the sync row at the start check and inside
    the publish lock, and cuts a NEW generation holding the rows committed
    before it. It logs at WARNING with the same fields."""
    text = cyc.stage_unpublished()
    before = cyc.generation()
    assert cyc.published_texts()[f"{AAA[-3:]}:2"] != text
    cyc.age_publisher()
    sync_id = cyc.sync_row("PENDING")
    cyc.as_scheduled_cycle()

    with _logged(logging.DEBUG, WORKFLOW_LOGGER) as records:
        summary = cyc.run_cycle()

    assert summary["status"] == "published", summary
    assert summary["generation_id"] != before
    assert cyc.generation() == summary["generation_id"]
    assert cyc.published_texts()[f"{AAA[-3:]}:2"] == text
    warnings = [m for m in _messages(records, logging.WARNING) if sync_id in m]
    assert warnings, [r.getMessage() for r in records]
    assert "overdue" in warnings[0]
    assert "source_sync_status=PENDING" in warnings[0]
    assert "freshness_age_seconds=" in warnings[0]
    assert "bound_seconds=600" in warnings[0]
    assert summary["scheduled"]["overdue"] is True


@pytest.mark.parametrize("status", ["ENQUEUED", "DELAYED"])
def test_an_overdue_publisher_publishes_past_a_stranded_sync(
    cyc: _Cycle, status: str,
) -> None:
    """A stranded row (days old, no run of the deployment alive) holds nothing
    once the publisher is overdue."""
    text = cyc.stage_unpublished()
    cyc.age_publisher()
    cyc.sync_row(status, age_seconds=3 * 86400)
    cyc.as_scheduled_cycle()

    summary = cyc.run_cycle()

    assert summary["status"] == "published", summary
    assert cyc.published_texts()[f"{AAA[-3:]}:2"] == text


def test_a_manual_publish_still_yields_to_a_pending_sync(cyc: _Cycle) -> None:
    """A cycle the cron tick did not enqueue keeps today's behaviour even when
    the publisher is overdue."""
    cyc.stage_unpublished()
    cyc.age_publisher()
    before = cyc.generation()
    sync_id = cyc.sync_row("PENDING")

    summary = cyc.run_cycle()

    assert summary["skipped"] == "source_sync_active"
    assert summary["blocking_workflow_id"] == sync_id
    assert cyc.generation() == before


def test_an_overdue_cycle_stops_at_once_for_a_run_still_writing(
    cyc: _Cycle,
) -> None:
    """The row yield is off but the run-base guard is not: a run that has
    written chunks and not finished ends the cycle as a recorded skip naming
    the run, with no wait, and none of its rows are published."""
    cyc.stage_unpublished()
    cyc.age_publisher()
    before = cyc.generation()
    cyc.sync_row("PENDING")
    run_id = "run-still-writing"
    cyc.start_run(run_id, "aaa:live-row")
    cyc.as_scheduled_cycle()

    started = time.monotonic()
    with _logged(logging.DEBUG, WORKFLOW_LOGGER) as records:
        summary = cyc.run_cycle()
    elapsed = time.monotonic() - started

    assert summary["status"] == "skipped"
    assert summary["skipped"] == "source_run_in_progress"
    assert [item["run_id"] for item in summary["in_progress"]] == [run_id]
    assert elapsed < 60, "the cycle waited for the run"
    assert cyc.generation() == before
    assert "aaa:live-row" not in cyc.published_texts()
    warnings = [m for m in _messages(records, logging.WARNING) if run_id in m]
    assert warnings, [r.getMessage() for r in records]
    assert "freshness_age_seconds=" in warnings[0]

    # The run ends; the next tick publishes its rows with everything else.
    cyc.end_run(run_id)
    cyc.as_scheduled_cycle()
    again = cyc.run_cycle()
    assert again["status"] == "published", again
    assert cyc.published_texts()["aaa:live-row"] == "written by a live run"


def test_a_run_that_starts_after_the_counts_is_a_skip_not_an_error(
    cyc: _Cycle,
) -> None:
    """The top check saw no run. A run starts after the counts; the
    checkpoint-cut recheck refuses the publish. The cycle ends as a recorded
    skip, not an exception, and the next tick retries."""
    from okg.substrate.ingest import run_base
    from okg.substrate.publisher.publisher import Publisher

    cyc.stage_unpublished()
    cyc.age_publisher()
    before = cyc.generation()
    cyc.as_scheduled_cycle()
    state = {"cut": False, "late_calls": 0}

    real_refuse = Publisher._chronos_refuse_writes_since_counted
    real_running = run_base.runs_in_progress

    def refuse(self, conn, signature, stage="checkpoint_cut", lock=True):
        if lock and stage == "checkpoint_cut":
            state["cut"] = True
        return real_refuse(self, conn, signature, stage=stage, lock=lock)

    def running(conn, *, deployment, own_run_ids=()):
        if state["cut"]:
            state["late_calls"] += 1
            return [{
                "run_id": "run-started-late", "source": AAA,
                "checkpoint_id": "okg-run-base:default:run-started-late",
                "owner": {"state": "alive", "pid": 1, "backend_start": "now"},
            }]
        return real_running(conn, deployment=deployment, own_run_ids=own_run_ids)

    self_monkeypatch = cyc.monkeypatch
    self_monkeypatch.setattr(
        Publisher, "_chronos_refuse_writes_since_counted", refuse,
    )
    self_monkeypatch.setattr(run_base, "runs_in_progress", running)

    summary = cyc.run_cycle()

    assert state["late_calls"] >= 1, "the cut recheck never ran"
    assert summary["status"] == "skipped"
    assert summary["skipped"] == "source_run_in_progress"
    assert [item["run_id"] for item in summary["in_progress"]] == [
        "run-started-late",
    ]
    assert cyc.generation() == before


def test_an_overdue_cycle_keeps_the_admission_intent(cyc: _Cycle) -> None:
    """The source-sync signal is a live lock, never stale. An overdue cycle
    still yields to it at once."""
    from okg.substrate.runtime import source_sync_priority

    cyc.stage_unpublished()
    cyc.age_publisher()
    before = cyc.generation()
    cyc.as_scheduled_cycle()
    cyc.monkeypatch.setattr(
        source_sync_priority, "source_sync_admission_intent_workflow_id",
        lambda **_kw: "source-sync-admission-intent",
    )

    summary = cyc.run_cycle()

    assert summary["skipped"] == "source_sync_active"
    assert summary["blocking_workflow_id"] == "source-sync-admission-intent"
    assert cyc.generation() == before


def test_a_branch_with_no_generation_keeps_yielding_to_a_sync(
    tmp_path: Path, clean_runtime: str, monkeypatch,
) -> None:
    """Where the run-base guard is blind (no published generation, so a run
    cuts no run base) the row yield is the only thing that sees a writing
    sync. It stays on, without bound, and the skip says why."""
    import okg.substrate.deployments as deployments

    fx = _Fixture(tmp_path, clean_runtime, monkeypatch)
    create_dbos_system_schema(clean_runtime)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(fx.dep.parent))
    monkeypatch.setattr(
        deployments, "require_postgres_dsn",
        lambda _manifest, deployment=None: clean_runtime,
    )
    cycle = _Cycle(fx, clean_runtime, monkeypatch)
    cycle.stage_unpublished()
    sync_id = cycle.sync_row("PENDING", age_seconds=5 * 86400)
    cycle.as_scheduled_cycle()

    with _logged(logging.DEBUG, WORKFLOW_LOGGER) as records:
        summary = cycle.run_cycle()

    assert summary["status"] == "skipped"
    assert summary["skipped"] == "no_run_base_guard"
    assert summary["blocking_workflow_id"] == sync_id
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        assert store.latest_generation(deployment_name=DEPLOYMENT) is None
    assert [m for m in _messages(records, logging.WARNING) if sync_id in m]

    # The skip repeats every tick while the bootstrap runs: WARNING once per
    # sync, then INFO.
    cycle.as_scheduled_cycle()
    with _logged(logging.DEBUG, WORKFLOW_LOGGER) as again:
        repeat = cycle.run_cycle()
    assert repeat["skipped"] == "no_run_base_guard"
    assert not [m for m in _messages(again, logging.WARNING) if sync_id in m]
    assert [m for m in _messages(again, logging.INFO) if sync_id in m]


# ---------------------------------------------------------------------------
# The tick
# ---------------------------------------------------------------------------


def _tick(cyc: _Cycle) -> str | None:
    from okg.substrate.runtime.publisher_schedule import (
        publisher_blocking_source_sync_workflow_id,
    )

    return publisher_blocking_source_sync_workflow_id(
        deployment=DEPLOYMENT, dsn=cyc.dsn, workflow_name="okg_publisher_cycle",
    )


def test_the_tick_yields_while_current_and_enqueues_once_overdue(
    cyc: _Cycle,
) -> None:
    sync_id = cyc.sync_row("PENDING")

    with _logged(logging.DEBUG, SCHEDULE_LOGGER) as records:
        assert _tick(cyc) == sync_id
    info = [m for m in _messages(records, logging.INFO) if sync_id in m]
    assert info and "source_sync_status=PENDING" in info[0]
    assert "freshness_age_seconds=" in info[0] and "bound_seconds=600" in info[0]
    assert not _messages(records, logging.WARNING)

    cyc.age_publisher()
    with _logged(logging.DEBUG, SCHEDULE_LOGGER) as records:
        assert _tick(cyc) is None
    warnings = [m for m in _messages(records, logging.WARNING) if sync_id in m]
    assert warnings and "enqueueing the cycle" in warnings[0]
    assert "source_sync_status=PENDING" in warnings[0]


def test_the_tick_is_not_fooled_by_a_skipped_cycle(cyc: _Cycle) -> None:
    """A skipped cycle is a DBOS SUCCESS but proves nothing: only a publish,
    a blocked checkpoint or a no-op moves freshness."""
    cyc.sync_row("PENDING")
    cyc.age_publisher()
    now = time.time()
    with psycopg.connect(cyc.dsn, autocommit=True) as conn:
        workflow_id = f"skip-{uuid.uuid4().hex[:8]}"
        conn.execute(
            "INSERT INTO dbos.workflow_status (workflow_uuid, name, status, "
            "queue_name, created_at, updated_at) VALUES "
            "(%s, 'okg_publisher_cycle', 'SUCCESS', %s, %s, %s)",
            (workflow_id, CONTROL_QUEUE, int(now * 1000), int(now * 1000)),
        )
        conn.execute(
            "INSERT INTO okg.workflow_operational_registrations "
            "(workflow_uuid, deployment_name, workflow_name, queue_name, "
            "contract_version) VALUES (%s, %s, 'okg_publisher_cycle', %s, 1)",
            (workflow_id, DEPLOYMENT, CONTROL_QUEUE),
        )
        conn.execute(
            "INSERT INTO okg.workflow_operational_outcomes "
            "(workflow_uuid, contract_version, outcome, succeeded_count, "
            "failed_count, semantic_units, details) VALUES "
            "(%s, 1, 'skipped', 0, 0, '{}'::jsonb, "
            "'{\"reason\": \"source_sync_active\"}'::jsonb)",
            (workflow_id,),
        )

    assert _tick(cyc) is None, "a skipped cycle must not keep the publisher current"

    with psycopg.connect(cyc.dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.workflow_operational_outcomes SET outcome = 'clean', "
            "succeeded_count = 1 WHERE workflow_uuid = %s",
            (workflow_id,),
        )
    assert _tick(cyc) is not None, "a clean cycle just now keeps it current"


@pytest.mark.parametrize("overdue", [False, True])
def test_a_cycle_already_queued_stops_the_tick(
    cyc: _Cycle, overdue: bool, monkeypatch,
) -> None:
    """A publisher cycle ENQUEUED or PENDING on the control queue stops the
    tick whether or not a sync is active and whether or not the publisher is
    overdue."""
    from okg.deployment.testing import mock_dbos_context
    from okg.substrate.runtime.publisher_schedule import (
        _DECORATED,
        register_publisher_schedule,
    )

    cyc.sync_row("PENDING")
    if overdue:
        cyc.age_publisher()
    queued = f"cycle-queued-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(cyc.dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO dbos.workflow_status (workflow_uuid, name, status, "
            "queue_name, created_at, updated_at) VALUES "
            "(%s, 'okg_publisher_cycle', 'ENQUEUED', %s, %s, %s)",
            (queued, CONTROL_QUEUE, _ms_ago(5), _ms_ago(5)),
        )
    with mock_dbos_context() as mock:
        register_publisher_schedule(deployment=DEPLOYMENT, dsn=cyc.dsn)
        schedule_fn = _DECORATED[next(iter(_DECORATED))]
        result = schedule_fn(datetime.now(timezone.utc), None)
        assert result == queued
        assert mock.enqueued == []


def test_the_tick_enqueues_an_overdue_cycle_past_an_active_sync(
    cyc: _Cycle, monkeypatch,
) -> None:
    from contextlib import contextmanager as _cm
    from okg.deployment.testing import mock_dbos_context
    from okg.substrate.runtime.publisher_schedule import (
        _DECORATED,
        register_publisher_schedule,
    )

    @_cm
    def permit(**_kwargs):
        yield

    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.promotion_enqueue_permit", permit,
    )
    cyc.sync_row("PENDING")
    with mock_dbos_context() as mock:
        register_publisher_schedule(deployment=DEPLOYMENT, dsn=cyc.dsn)
        schedule_fn = _DECORATED[next(iter(_DECORATED))]
        assert schedule_fn(datetime.now(timezone.utc), None) is not None
        assert mock.enqueued == [], "a current publisher yields to the sync"

        cyc.age_publisher()
        result = schedule_fn(datetime.now(timezone.utc), None)
        assert result.startswith("mock-wf-")
        mock.assert_workflow_enqueued(
            "okg_publisher_cycle", queue_name=CONTROL_QUEUE,
        )
