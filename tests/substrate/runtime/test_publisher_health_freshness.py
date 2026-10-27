"""The doctor judges the publisher by freshness, and reports stuck syncs.

PACT publisher-starvation-4039 (issue #4039). A publisher cycle that yielded
to a source sync, or found a run still writing, ends as a DBOS SUCCESS. The
doctor used to take the newest SUCCESS row as the last publish, so a run of
skipped cycles looked like a fresh publisher. It now measures the newest
non-skipped cycle outcome or published generation against
``max_source_sync_yield_seconds`` + the source-run publish wait + two cron
intervals, and names the newest skip. A second check, ``source_sync_stuck``,
reports each ``okg_source_sync`` row waiting or running past the yield bound.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid

import psycopg
import pytest

from okg.substrate.runtime.publisher_health_workflow_liveness import (
    check_publisher_workflow_health,
    check_source_sync_stuck,
)
from tests.pytest.dbos_system_schema import create_dbos_system_schema

pytestmark = pytest.mark.postgres

#: A five-minute cron: two intervals are 600 s.
_CRON = {"okg_publisher_cycle": "*/5 * * * *"}
_WAIT = 600.0
_BOUND = 600.0


@pytest.fixture(autouse=True)
def _publish_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_PUBLISH_WAIT_FOR_SOURCE_RUNS_SECONDS", str(int(_WAIT)))


@pytest.fixture
def dsn(clean_runtime: str) -> str:
    create_dbos_system_schema(clean_runtime)
    # The publisher's cutover is its migration's applied_at. A deployment that
    # has run for weeks has it far behind; a fixture database has it minutes
    # back, which would make hour-old cycles "legacy". Put it in the past.
    from okg.substrate.runtime.workflow_operational_outcomes import (
        PUBLISHER_MIGRATION_ID,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.schema_migrations SET applied_at = now() - interval "
            "'30 days' WHERE migration_id = %s",
            (PUBLISHER_MIGRATION_ID,),
        )
    return clean_runtime


def _deployment() -> str:
    return f"fresh-{uuid.uuid4().hex[:8]}"


def _ms_ago(seconds: float) -> int:
    return int((time.time() - seconds) * 1000)


def _cycle(
    dsn: str,
    deployment: str,
    *,
    age_seconds: float,
    outcome: str | None,
    reason: str | None = None,
    subject: str | None = None,
) -> str:
    """One finished (SUCCESS) publisher cycle and, when ``outcome`` is given,
    the typed outcome the workflow records for it."""
    workflow_id = f"cycle-{uuid.uuid4().hex[:12]}"
    at = _ms_ago(age_seconds)
    queue = f"okg-{deployment}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO dbos.workflow_status (workflow_uuid, name, status, "
            "queue_name, created_at, updated_at, completed_at) "
            "VALUES (%s, 'okg_publisher_cycle', 'SUCCESS', %s, %s, %s, %s)",
            (workflow_id, queue, at, at, at),
        )
        if outcome is None:
            return workflow_id
        conn.execute(
            "INSERT INTO okg.workflow_operational_registrations "
            "(workflow_uuid, deployment_name, workflow_name, queue_name, "
            "contract_version, registered_at) "
            "VALUES (%s, %s, 'okg_publisher_cycle', %s, 1, "
            "to_timestamp(%s / 1000.0))",
            (workflow_id, deployment, queue, at),
        )
        details = {"branch_name": "default"}
        if reason:
            details.update(reason=reason, subject=subject or "unspecified")
        conn.execute(
            "INSERT INTO okg.workflow_operational_outcomes "
            "(workflow_uuid, contract_version, outcome, succeeded_count, "
            "failed_count, semantic_units, details, recorded_at) "
            "VALUES (%s, 1, %s, %s, 0, '{}'::jsonb, %s::jsonb, "
            "to_timestamp(%s / 1000.0))",
            (
                workflow_id, outcome, 1 if outcome == "clean" else 0,
                json.dumps(details), at,
            ),
        )
    return workflow_id


def _generation(dsn: str, deployment: str, *, age_seconds: float) -> None:
    """A published generation of ``deployment`` cut ``age_seconds`` ago, from
    a branch holding one row."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from tests.pytest.catalog_fixture_builders import seed_catalog

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.apply_facts(
            [NodeFact(
                f"{deployment}:1", "document", {"text": "x"}, {"id": "1"},
                {"v": 1},
            )],
            source=f"{deployment}.src", run_id=str(uuid.uuid4()),
            strict_endpoints=False,
        )
        store.checkpoint_generation(1, deployment_name=deployment)
        conn.execute(
            "UPDATE _chronos_branch_interval_checkpoints SET created_at = "
            "to_char((created_at::timestamptz - make_interval(secs => %s)) "
            "AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"')",
            (age_seconds,),
        )


def _probe(dsn: str, deployment: str, *, bound: float = _BOUND):
    return check_publisher_workflow_health(
        deployment=deployment, dsn=dsn, schedule_overrides=_CRON,
        yield_bound_seconds=bound,
    )


# ---------------------------------------------------------------------------
# publisher_workflow_stale
# ---------------------------------------------------------------------------


def test_a_run_of_skipped_cycles_after_an_old_publish_is_stale_and_named(
    dsn: str,
) -> None:
    """Three skipped SUCCESS cycles a minute ago must not hide a publish that
    is an hour old. The verdict names the newest skip's reason and subject."""
    deployment = _deployment()
    _cycle(dsn, deployment, age_seconds=3600, outcome="clean")
    for age, sync in ((170, "sync-a"), (110, "sync-b"), (50, "sync-c")):
        _cycle(
            dsn, deployment, age_seconds=age, outcome="skipped",
            reason="source_sync_active", subject=sync,
        )

    verdict = _probe(dsn, deployment)

    assert (verdict.ok, verdict.code) == (False, "publisher_workflow_stale")
    assert "reason=source_sync_active" in verdict.message
    assert "subject=sync-c" in verdict.message
    assert verdict.seconds_since_last_success > 3000


def test_a_stale_verdict_says_when_no_skip_was_recorded(dsn: str) -> None:
    deployment = _deployment()
    _cycle(dsn, deployment, age_seconds=3600, outcome="clean")

    verdict = _probe(dsn, deployment)

    assert verdict.code == "publisher_workflow_stale"
    assert "no skipped cycle was recorded" in verdict.message


def test_a_sync_owned_generation_inside_the_threshold_is_fresh(dsn: str) -> None:
    """The source sync published by itself a minute ago; the publisher's own
    last publish is old. The branch is fresh."""
    deployment = _deployment()
    _cycle(dsn, deployment, age_seconds=3600, outcome="clean")
    _cycle(
        dsn, deployment, age_seconds=30, outcome="skipped",
        reason="source_sync_active", subject="sync-a",
    )
    _generation(dsn, deployment, age_seconds=60)

    verdict = _probe(dsn, deployment)

    assert (verdict.ok, verdict.code) == (True, "publisher_workflow_healthy")


def test_a_clean_cycle_just_now_is_fresh_whatever_the_skips(dsn: str) -> None:
    deployment = _deployment()
    _cycle(dsn, deployment, age_seconds=200, outcome="skipped",
           reason="source_run_in_progress", subject="run-1")
    _cycle(dsn, deployment, age_seconds=20, outcome="clean")

    assert _probe(dsn, deployment).code == "publisher_workflow_healthy"


def test_the_threshold_is_bound_plus_run_wait_plus_two_intervals(
    dsn: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_PUBLISH_WAIT_FOR_SOURCE_RUNS_SECONDS", "77")
    deployment = _deployment()
    _cycle(dsn, deployment, age_seconds=10, outcome="clean")

    verdict = _probe(dsn, deployment, bound=123.0)

    assert verdict.threshold_seconds == 123.0 + 77.0 + 600.0
    # Just inside the threshold is fresh; just outside is stale.
    inside = _deployment()
    _cycle(dsn, inside, age_seconds=790, outcome="clean")
    outside = _deployment()
    _cycle(dsn, outside, age_seconds=820, outcome="clean")
    assert _probe(dsn, inside, bound=123.0).code == "publisher_workflow_healthy"
    assert _probe(dsn, outside, bound=123.0).code == "publisher_workflow_stale"


def test_rows_before_the_cutover_keep_todays_verdict(dsn: str) -> None:
    """A SUCCESS row that completed before the publisher's cutover has no
    outcome to read. It counts as it did: the newest SUCCESS is the last
    publish."""
    from okg.substrate.runtime.workflow_operational_outcomes import (
        PUBLISHER_MIGRATION_ID,
    )

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.schema_migrations SET applied_at = now() + interval "
            "'1 hour' WHERE migration_id = %s",
            (PUBLISHER_MIGRATION_ID,),
        )
    fresh, old = _deployment(), _deployment()
    _cycle(dsn, fresh, age_seconds=30, outcome=None)
    _cycle(dsn, old, age_seconds=5000, outcome=None)

    assert _probe(dsn, fresh).code == "publisher_workflow_healthy"
    assert _probe(dsn, old).code == "publisher_workflow_stale"


def test_a_success_after_the_cutover_with_no_outcome_proves_nothing(
    dsn: str,
) -> None:
    deployment = _deployment()
    # The new code's first cycle registered an hour ago: the cutover. A SUCCESS
    # after it that recorded nothing is not a publish.
    _cycle(dsn, deployment, age_seconds=3600, outcome="clean")
    _cycle(dsn, deployment, age_seconds=30, outcome=None)

    verdict = _probe(dsn, deployment)

    assert (verdict.ok, verdict.code) == (False, "publisher_workflow_stale")
    assert verdict.seconds_since_last_success > 3000


def test_cycles_of_the_old_code_before_the_first_registration_are_legacy(
    dsn: str,
) -> None:
    """Rolling upgrade: a worker still on the old code finishes SUCCESS rows
    after the migration. With no registered outcome yet they count as they
    always did."""
    deployment = _deployment()
    _cycle(dsn, deployment, age_seconds=30, outcome=None)

    assert _probe(dsn, deployment).code == "publisher_workflow_healthy"


# ---------------------------------------------------------------------------
# source_sync_stuck
# ---------------------------------------------------------------------------


def _sync_row(dsn: str, deployment: str, status: str, age_seconds: float) -> str:
    workflow_id = f"sync-{uuid.uuid4().hex[:10]}"
    at = _ms_ago(age_seconds)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO dbos.workflow_status (workflow_uuid, name, status, "
            "queue_name, created_at, updated_at) "
            "VALUES (%s, 'okg_source_sync', %s, %s, %s, %s)",
            (workflow_id, status, f"okg-{deployment}-source-sync", at, at),
        )
    return workflow_id


def test_stuck_syncs_are_a_warning_with_ids_ages_and_the_cancel_command(
    dsn: str,
) -> None:
    deployment = _deployment()
    _generation(dsn, deployment, age_seconds=60)
    pending = _sync_row(dsn, deployment, "PENDING", 7200)
    enqueued = _sync_row(dsn, deployment, "ENQUEUED", 3000)
    _sync_row(dsn, deployment, "PENDING", 120)          # younger than the bound
    _sync_row(dsn, deployment, "SUCCESS", 99999)        # finished
    other = _deployment()
    _sync_row(dsn, other, "PENDING", 99999)             # another deployment

    verdict = check_source_sync_stuck(
        deployment=deployment, dsn=dsn, bound_seconds=_BOUND,
    )

    assert (verdict.ok, verdict.severity, verdict.code) == (
        True, "warning", "source_sync_stuck",
    )
    rows = {row["workflow_id"]: row for row in verdict.rows}
    assert set(rows) == {pending, enqueued}
    assert rows[pending]["status"] == "PENDING"
    assert 7100 < rows[pending]["age_seconds"] < 7400
    assert rows[enqueued]["status"] == "ENQUEUED"
    assert rows[pending]["cancel_command"].endswith(
        f"runtime cancel-workflow --deployment {deployment} {pending}"
    )
    assert verdict.live_source_run is False
    assert verdict.branch_has_generation is True
    assert pending in verdict.message and enqueued in verdict.message
    assert "not live" in verdict.message


def test_stuck_syncs_on_a_branch_with_no_generation_are_fatal(dsn: str) -> None:
    deployment = _deployment()
    stuck = _sync_row(dsn, deployment, "PENDING", 7200)

    verdict = check_source_sync_stuck(
        deployment=deployment, dsn=dsn, bound_seconds=_BOUND,
    )

    assert (verdict.ok, verdict.severity) == (False, "fatal")
    assert verdict.branch_has_generation is False
    assert verdict.live_source_run is False
    assert stuck in verdict.message
    assert "without bound" in verdict.message


def test_a_first_bootstrap_that_is_still_writing_is_only_a_warning(
    dsn: str,
) -> None:
    """No first generation, but a source run holds its liveness lock: a healthy
    long bootstrap. No run base exists yet (none is cut before the first
    generation), so the run is found by its named liveness session."""
    from okg.substrate.ingest.run_base import hold_run_liveness

    deployment = _deployment()
    _sync_row(dsn, deployment, "PENDING", 7200)
    live = hold_run_liveness(dsn, "okg-run-base:default:first-bootstrap")
    try:
        verdict = check_source_sync_stuck(
            deployment=deployment, dsn=dsn, bound_seconds=_BOUND,
        )
    finally:
        live.close()

    assert (verdict.ok, verdict.severity) == (True, "warning")
    assert verdict.branch_has_generation is False
    assert verdict.live_source_run is True
    assert "until the live run finishes" in verdict.message


def test_no_stuck_sync_is_ok(dsn: str) -> None:
    deployment = _deployment()
    _sync_row(dsn, deployment, "PENDING", 30)

    verdict = check_source_sync_stuck(
        deployment=deployment, dsn=dsn, bound_seconds=_BOUND,
    )

    assert (verdict.ok, verdict.severity, verdict.rows) == (True, "ok", ())


def test_a_live_source_run_is_reported(dsn: str) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest.run_base import (
        hold_run_liveness,
        run_base_checkpoint_id,
        take_run_base,
    )

    deployment = _deployment()
    _generation(dsn, deployment, age_seconds=60)
    _sync_row(dsn, deployment, "PENDING", 7200)
    checkpoint_id = run_base_checkpoint_id("default", "run-live")
    live = hold_run_liveness(dsn, checkpoint_id)
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            assert take_run_base(
                store, deployment=deployment, branch="default",
                source=f"{deployment}.src", run_id="run-live",
            ) is not None
        verdict = check_source_sync_stuck(
            deployment=deployment, dsn=dsn, bound_seconds=_BOUND,
        )
    finally:
        live.close()

    assert verdict.live_source_run is True
    assert "is live" in verdict.message


def test_a_probe_that_raises_is_failed_never_healthy(
    dsn: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import publisher_schedule

    deployment = _deployment()
    _sync_row(dsn, deployment, "PENDING", 7200)

    def boom(**_kwargs):
        raise RuntimeError("generation ledger unreadable")

    monkeypatch.setattr(publisher_schedule, "read_publisher_freshness", boom)

    verdict = check_source_sync_stuck(
        deployment=deployment, dsn=dsn, bound_seconds=_BOUND,
    )

    assert (verdict.ok, verdict.severity) == (False, "failed")
    assert verdict.code == "source_sync_stuck_probe_failed"
    assert "generation ledger unreadable" in verdict.message


def test_the_doctor_reports_the_stuck_sync_and_uses_the_manifest_bound(
    dsn: str,
) -> None:
    """`okg doctor` runs both checks. On a deployment with no generation a
    stuck sync is an error; the report carries the check's rows."""
    deployment = "okg-workspace"
    stuck = _sync_row(dsn, deployment, "PENDING", 7200)
    env = os.environ.copy()
    env.update({"OKG_DSN": dsn, "OKG_DEPLOYMENT": deployment})

    done = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main", "doctor",
         "--deployment", deployment, "--json"],
        check=False, capture_output=True, text=True, env=env,
    )

    payload = json.loads(done.stdout)
    check = payload["source_sync_stuck"]
    assert check["severity"] == "fatal" and check["ok"] is False
    assert [row["workflow_id"] for row in check["rows"]] == [stuck]
    assert check["bound_seconds"] == 600
    assert any(
        error.get("check") == "source_sync_stuck" for error in payload["errors"]
    ), payload["errors"]
    assert payload["ok"] is False
    # The publisher probe is the freshness one: it names its threshold terms.
    assert payload["publisher_workflow_health"]["threshold_seconds"] >= 1200
