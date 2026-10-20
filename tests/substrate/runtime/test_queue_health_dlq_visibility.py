"""Terminal FAILURE-class runtime work must make queue health go red.

Regression guard for issue #680 (PACT task.audit-2026-07.dlq-visibility, P0):
``summarize_runtime_queue_health`` used to derive ``ok`` / ``status`` from
availability + staleness only, so terminal failure work (ERROR / FAILED /
MAX_RECOVERY_ATTEMPTS*) sat in the queue while every consumer read green. The
verdict is now keyed on the FAILURE CLASS only (the view's ``failed_count``).

CANCELLED is NOT a failure: it is steady-state worker-hygiene work (the worker
cancels stale replaceable wakeups at boot + periodically and never deletes the
rows), so a healthy okg-workspace holds thousands of CANCELLED rows. Cancels
are tracked separately in ``runtime_cancelled_count`` and must NEVER redden the
verdict. These tests pin both directions: a failure row drives red (and each
consumer that reads the summary refuses), while a cancels-only queue stays
green.

The DB-backed tests exercise the real DBOS view through the same
``clean_runtime`` fixture the other runtime-queue tests use; the cheap unit
test drives the deployment-readiness blocker chain with a fabricated
dead-letter summary (mirrors ``tests/substrate/test_readiness_fail_loud.py``).
"""
from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from okg.substrate.runtime_queue_health import summarize_runtime_queue_health
from okg.substrate.source_sync import SourceSyncJob
from okg.substrate.source_sync_enqueue import (
    _serialize_source_sync_dbos_args,
    _source_sync_workflow_args,
)


_DBOS_TABLE_DDL = """
CREATE SCHEMA IF NOT EXISTS dbos;
CREATE TABLE IF NOT EXISTS dbos.workflow_status (
  workflow_uuid text PRIMARY KEY,
  queue_name text,
  status text NOT NULL,
  name text,
  created_at timestamptz,
  updated_at timestamptz,
  started_at_epoch_ms bigint,
  priority int,
  application_version text,
  inputs text,
  serialization text
);
ALTER TABLE dbos.workflow_status
  ADD COLUMN IF NOT EXISTS application_version text;
ALTER TABLE dbos.workflow_status
  ADD COLUMN IF NOT EXISTS inputs text;
ALTER TABLE dbos.workflow_status
  ADD COLUMN IF NOT EXISTS serialization text;
CREATE TABLE IF NOT EXISTS dbos.application_versions (
  version_id uuid PRIMARY KEY,
  version_name text NOT NULL,
  version_timestamp bigint,
  created_at bigint
);
DELETE FROM dbos.application_versions;
INSERT INTO dbos.application_versions (
  version_id, version_name, version_timestamp, created_at
) VALUES (
  '00000000-0000-0000-0000-000000000002', 'app-v2', 200, 200
);
ALTER TABLE dbos.workflow_status
  ADD COLUMN IF NOT EXISTS application_version text;
ALTER TABLE dbos.workflow_status
  ADD COLUMN IF NOT EXISTS inputs text;
CREATE TABLE IF NOT EXISTS dbos.application_versions (
  version_id uuid PRIMARY KEY,
  version_name text NOT NULL,
  version_timestamp bigint,
  created_at bigint
);
"""

# One FAILED (ERROR) row, one CANCELLED row, and a healthy SUCCESS row so the
# queue is not merely empty. The ERROR row drives the red verdict; the CANCELLED
# row is present precisely to prove it is tracked separately and does NOT inflate
# the failure-class dlq_count. name is NULL so the rows are not filtered by any
# consumer's ignored-workflow set (deployment readiness ignores okg_source_sync
# et al.).
_SEED_DEAD_LETTER = """
DELETE FROM dbos.workflow_status;
INSERT INTO dbos.workflow_status (
  workflow_uuid, queue_name, status, name,
  created_at, updated_at, started_at_epoch_ms, priority
)
VALUES
  ('wf-failed',    'q-work', 'ERROR',     NULL, now(), now(), NULL, 100),
  ('wf-cancelled', 'q-work', 'CANCELLED', NULL, now(), now(), NULL, 100),
  ('wf-success',   'q-work', 'SUCCESS',   NULL, now(), now(), NULL, 100);
"""

# Only CANCELLED (worker-hygiene) rows plus a SUCCESS row: NO failure-class work.
# This must stay green — cancels are steady-state and never redden the verdict.
_SEED_CANCELLED_ONLY = """
DELETE FROM dbos.workflow_status;
INSERT INTO dbos.workflow_status (
  workflow_uuid, queue_name, status, name,
  created_at, updated_at, started_at_epoch_ms, priority
)
VALUES
  ('wf-cancelled-a', 'q-work', 'CANCELLED', NULL, now(), now(), NULL, 100),
  ('wf-cancelled-b', 'q-work', 'CANCELLED', NULL, now(), now(), NULL, 100),
  ('wf-success',     'q-work', 'SUCCESS',   NULL, now(), now(), NULL, 100);
"""

_SEED_HISTORICAL_FAILURE = """
DELETE FROM dbos.workflow_status;
INSERT INTO dbos.workflow_status (
  workflow_uuid, queue_name, status, name, application_version,
  created_at, updated_at, started_at_epoch_ms, priority
)
VALUES
  ('wf-old-failed', 'q-work', 'ERROR', 'okg_publisher_cycle', 'app-v1',
   now(), now(), NULL, 100),
  ('wf-current-ok', 'q-work', 'SUCCESS', 'okg_publisher_cycle', 'app-v2',
   now(), now(), NULL, 100);
"""


def _seed_dead_letter(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute(_DBOS_TABLE_DDL)
        cur.execute(_SEED_DEAD_LETTER)


def _seed_cancelled_only(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute(_DBOS_TABLE_DDL)
        cur.execute(_SEED_CANCELLED_ONLY)


def _seed_historical_failure(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        # Drop first, like the two seeders above. `_DBOS_TABLE_DDL` is all
        # CREATE TABLE IF NOT EXISTS, so without this the seed runs against
        # whatever `dbos` schema a previously-run test left behind — and its
        # `now()` values do not fit the real SDK's bigint `created_at`.
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute(_DBOS_TABLE_DDL)
        cur.execute(_SEED_HISTORICAL_FAILURE)


@pytest.mark.postgres
def test_summarize_flags_dead_letter_as_unhealthy(clean_runtime: str) -> None:
    """A terminal ERROR row drives red; a co-present CANCELLED row is tracked
    separately and does NOT inflate the failure-class dlq_count."""
    _seed_dead_letter(clean_runtime)
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        summary = summarize_runtime_queue_health(cur)

    assert summary["ok"] is False
    assert summary["status"] == "dead_letter"
    assert "dead_letter" in str(summary["reason"])
    # dlq_count is the FAILURE-CLASS count only (ERROR/FAILED/MAX_RECOVERY*).
    # The one ERROR row counts; the one CANCELLED row does NOT.
    assert summary["dlq_count"] == 1
    assert summary["terminal_history_dlq_count"] == 1
    assert summary["runtime_failed_count"] == 1
    # The cancel is still visible as data, just never folded into the verdict.
    assert summary["runtime_cancelled_count"] == 1


@pytest.mark.postgres
def test_summarize_stays_green_with_only_cancels(clean_runtime: str) -> None:
    """Worker-hygiene CANCELLED rows must never redden the verdict.

    A queue holding only CANCELLED (+ SUCCESS) terminal history is healthy: the
    cancels are counted in ``runtime_cancelled_count`` and ``terminal_history_
    count`` as data, but ``ok`` stays True, ``dlq_count`` is 0, and the status
    is not ``dead_letter``.
    """
    _seed_cancelled_only(clean_runtime)
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        summary = summarize_runtime_queue_health(cur)

    assert summary["ok"] is True
    assert summary["status"] != "dead_letter"
    assert summary["dlq_count"] == 0
    assert summary["terminal_history_dlq_count"] == 0
    assert summary["runtime_failed_count"] == 0
    # The cancels are still visible as data.
    assert summary["runtime_cancelled_count"] == 2
    assert summary["terminal_history_count"] >= 2


@pytest.mark.postgres
def test_summarize_stays_green_without_dead_letter(clean_runtime: str) -> None:
    """A queue with only SUCCESS terminal history is healthy."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(_DBOS_TABLE_DDL)
        cur.execute("DELETE FROM dbos.workflow_status")
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, queue_name, status, name,
              created_at, updated_at, started_at_epoch_ms, priority
            )
            VALUES ('wf-ok', 'q-work', 'SUCCESS', NULL, now(), now(), NULL, 20)
            """
        )
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        summary = summarize_runtime_queue_health(cur)

    assert summary["ok"] is True
    assert summary["status"] == "available"
    assert summary["dlq_count"] == 0


@pytest.mark.postgres
def test_summarize_keeps_old_application_failure_as_green_history(
    clean_runtime: str,
) -> None:
    """A retained old-version failure is audit history, not current DLQ."""
    _seed_historical_failure(clean_runtime)
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        summary = summarize_runtime_queue_health(cur)

    assert summary["ok"] is True
    assert summary["status"] == "available"
    assert summary["dlq_count"] == 0
    assert summary["runtime_failed_count"] == 0
    assert summary["terminal_history_dlq_count"] == 1
    assert summary["current_runtime_dlq"]["historical_count"] == 1


@pytest.mark.postgres
def test_source_failure_stays_red_until_explicit_matching_success(
    clean_runtime: str,
) -> None:
    """Current source failure clears only through explicit responsibility."""
    failed_job = SourceSyncJob(
        deployment="okg-ci",
        sources=("git-history",),
        run_mode="scope_complete",
    )
    failed_inputs, failed_serialization = _serialize_source_sync_dbos_args(
        args=_source_sync_workflow_args(failed_job),
        serialization=None,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(_DBOS_TABLE_DDL)
        cur.execute("DELETE FROM dbos.workflow_status")
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, queue_name, status, name, application_version,
              inputs, serialization, created_at, updated_at, priority
            ) VALUES (
              'wf-source-failed', 'okg-okg-ci-source-sync', 'ERROR',
              'okg_source_sync', 'app-v2', %s, %s, now(), now(), 100
            )
            """,
            (failed_inputs, failed_serialization),
        )
        red = summarize_runtime_queue_health(cur)

        success_job = SourceSyncJob(
            deployment="okg-ci",
            sources=("git-history",),
            run_mode="scope_complete",
            supersedes_workflow_id="wf-source-failed",
        )
        success_inputs, success_serialization = (
            _serialize_source_sync_dbos_args(
                args=_source_sync_workflow_args(success_job),
                serialization=None,
            )
        )
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, queue_name, status, name, application_version,
              inputs, serialization, created_at, updated_at, priority
            ) VALUES (
              'wf-source-success', 'okg-okg-ci-source-sync', 'SUCCESS',
              'okg_source_sync', 'app-v2', %s, %s, now(), now(), 100
            )
            """,
            (success_inputs, success_serialization),
        )
        green = summarize_runtime_queue_health(cur)

    assert red["status"] == "dead_letter"
    assert red["dlq_count"] == 1
    assert green["ok"] is True
    assert green["dlq_count"] == 0
    assert green["terminal_history_dlq_count"] == 1
    assert green["current_runtime_dlq"]["historical_count"] == 1


@pytest.mark.postgres
def test_publish_proof_dlq_count_and_gate_refuse(clean_runtime: str) -> None:
    from okg.substrate.publish_proof import assert_publish_proof, dlq_count

    _seed_dead_letter(clean_runtime)

    count, depth = dlq_count(clean_runtime)
    # Failure-class only: the ERROR row counts, the co-present CANCELLED does not.
    assert count == 1
    assert depth["dead_letter"] == 1
    # dead_letter must NOT be collapsed into a generic runtime_* error: the
    # dedicated dead-letter branch in assert_publish_proof must own the refusal.
    assert not any(
        key.startswith("runtime_") and value
        for key, value in depth.items()
    )

    verdict = assert_publish_proof(
        {
            "status": "published",
            "generation_id": "okg:demo:branch:default:gen:1",
            "nodes_added": 5,
            "edges_added": 5,
        },
        dsn=clean_runtime,
    )
    assert verdict["ok"] is False
    assert "dead-letter" in verdict["reason"]
    assert verdict["dlq_count"] == 1


@pytest.mark.postgres
def test_publish_proof_delegates_only_named_readiness_workflows(
    clean_runtime: str,
) -> None:
    from okg.substrate.publish_proof import assert_publish_proof, dlq_count
    from okg.substrate.runtime_queue_health import (
        DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES,
    )

    failed_job = SourceSyncJob(
        deployment="okg-ci",
        sources=("git-history",),
        run_mode="scope_complete",
    )
    inputs, serialization = _serialize_source_sync_dbos_args(
        args=_source_sync_workflow_args(failed_job),
        serialization=None,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(_DBOS_TABLE_DDL)
        cur.execute("DELETE FROM dbos.workflow_status")
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, queue_name, status, name, application_version,
              inputs, serialization, created_at, updated_at, priority
            ) VALUES
              ('wf-source-failed', 'okg-okg-ci-source-sync', 'ERROR',
               'okg_source_sync', 'app-v2', %s, %s, now(), now(), 100),
              ('wf-publisher-failed', 'okg-okg-ci', 'ERROR',
               'okg_publisher_cycle', 'app-v2', NULL, NULL, now(), now(), 100)
            """,
            (inputs, serialization),
        )

    delegated = set(
        DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES
    )
    generic_count, _ = dlq_count(clean_runtime)
    delegated_count, _ = dlq_count(
        clean_runtime,
        ignored_workflow_names=delegated,
    )
    payload = {
        "status": "published",
        "generation_id": "okg:demo:branch:default:gen:1",
        "nodes_added": 5,
        "edges_added": 5,
        # Issue #1291: a healthy publish receipt also carries a measured lock
        # hold well inside the configured timeout headroom.
        "timing_details": {
            "lock": {
                "coordination_mode": "dbos",
                "hold_ms": 12_000.0,
                "wait_ms": 4.0,
                "hard_timeout_ms": 900_000,
            },
        },
    }

    assert generic_count == 2
    assert delegated_count == 1
    assert assert_publish_proof(
        payload,
        dsn=clean_runtime,
        ignored_runtime_workflow_names=delegated,
    )["ok"] is False

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "DELETE FROM dbos.workflow_status "
            "WHERE workflow_uuid = 'wf-publisher-failed'"
        )

    assert assert_publish_proof(
        payload,
        dsn=clean_runtime,
        ignored_runtime_workflow_names=delegated,
    )["ok"] is True
    assert assert_publish_proof(
        payload,
        dsn=clean_runtime,
    )["ok"] is False


@pytest.mark.postgres
def test_publish_proof_passes_with_only_cancels(clean_runtime: str) -> None:
    """The publish gate must NOT refuse a good publish over worker-hygiene
    cancels: a cancels-only queue leaves dlq_count at 0 and the proof passes."""
    from okg.substrate.publish_proof import assert_publish_proof, dlq_count

    _seed_cancelled_only(clean_runtime)

    count, depth = dlq_count(clean_runtime)
    assert count == 0
    assert depth["dead_letter"] == 0

    verdict = assert_publish_proof(
        {
            "status": "published",
            "generation_id": "okg:demo:branch:default:gen:1",
            "nodes_added": 5,
            "edges_added": 5,
            # Issue #1291: a healthy publish receipt also carries a measured
            # lock hold well inside the configured timeout headroom.
            "timing_details": {
                "lock": {
                    "coordination_mode": "dbos",
                    "hold_ms": 12_000.0,
                    "wait_ms": 4.0,
                    "hard_timeout_ms": 900_000,
                },
            },
        },
        dsn=clean_runtime,
    )
    assert verdict["ok"] is True
    assert verdict["dlq_count"] == 0


@pytest.mark.postgres
def test_ready_queue_status_reports_dead_letter(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.cli.deployment_readiness import _ready_queue_status

    _seed_dead_letter(clean_runtime)
    with psycopg.connect(clean_runtime) as conn:
        status = _ready_queue_status(
            conn,
            deployment_dir=tmp_path,
            max_active_queue_depth=0,
        )

    assert status["ok"] is False
    assert status["status"] == "dead_letter"
    assert status["dlq_count"] == 1


@pytest.mark.postgres
def test_ready_queue_status_demotes_prior_application_failure_to_history(
    clean_runtime: str,
    tmp_path: Path,
    record_property,
) -> None:
    from okg.substrate.cli.deployment_readiness import _ready_queue_status

    record_property(
        "requirements",
        "req.runtime-authority.readiness test.runtime-authority.readiness",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute(_DBOS_TABLE_DDL)
        cur.execute("DELETE FROM dbos.application_versions")
        cur.execute(
            """
            INSERT INTO dbos.application_versions (
              version_id, version_name, version_timestamp, created_at
            ) VALUES
              ('00000000-0000-0000-0000-0000000000a1', 'v1', 100, 100),
              ('00000000-0000-0000-0000-0000000000a2', 'v2', 200, 200)
            """
        )
        cur.execute(
            """
            INSERT INTO dbos.workflow_status (
              workflow_uuid, queue_name, status, name,
              application_version, inputs,
              created_at, updated_at, started_at_epoch_ms, priority
            ) VALUES
              ('wf-old-failure', 'q-work', 'ERROR', 'legacy_workflow',
               'v1', '[]', now(), now(), NULL, 100),
              ('wf-current-success', 'q-work', 'SUCCESS', 'current_workflow',
               'v2', '[]', now(), now(), NULL, 100)
            """
        )
    try:
        with psycopg.connect(clean_runtime) as conn:
            status = _ready_queue_status(
                conn,
                deployment_dir=tmp_path,
                max_active_queue_depth=0,
            )

        assert status["ok"] is True
        assert status["status"] == "available"
        assert status["dlq_count"] == 0
        assert status["terminal_history_dlq_count"] == 1
        assert status["current_version_dlq"][
            "current_application_version"
        ] == "v2"
        assert status["current_version_dlq"]["historical_count"] == 1
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")


@pytest.mark.postgres
def test_console_runtime_snapshot_reports_dead_letter(
    clean_runtime: str,
) -> None:
    from okg.substrate.console_views.health import _runtime_queue_snapshot

    _seed_dead_letter(clean_runtime)
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        snapshot = _runtime_queue_snapshot(cur)

    assert snapshot["ok"] is False
    assert snapshot["status"] == "dead_letter"
    assert int(snapshot["dlq_count"]) == 1


class _FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


def _clean_generation(deployment: str = "demo") -> dict[str, object]:
    generation_id = f"okg:{deployment}:branch:default:gen:test"
    return {
        "status": "published",
        "generation_id": generation_id,
        "node_count": 10,
        "edge_count": 20,
        "count_integrity": {
            "schema": "okg.generation_count_integrity.v1",
            "ok": True,
            "status": "clean",
            "generation_id": generation_id,
            "checkpoint": {"node_count": 10, "edge_count": 20},
            "pinned": {"node_count": 10, "edge_count": 20},
            "mismatches": [],
        },
    }


def test_deployment_ready_emits_dlq_blocker(
    monkeypatch,
    tmp_path,
    record_property,
) -> None:
    """The deployment-ready check surfaces a queues.dlq blocker (not a
    misleading queues.unavailable) when the shared summary reports a
    dead-letter queue with ok=False."""
    record_property(
        "requirements",
        "req.runtime-authority.readiness test.runtime-authority.readiness",
    )
    import psycopg as _psycopg
    from okg.substrate.cli import deployment as deployment_cli
    from okg.substrate.nomos import doctor as nomos_doctor

    monkeypatch.setattr(
        _psycopg, "connect", lambda *_a, **_k: _FakeConnection()
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_database_status",
        lambda _conn: {"missing_relations": []},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_generation_status",
        lambda _conn, _deployment: _clean_generation(),
    )
    monkeypatch.setattr(
        nomos_doctor,
        "source_lineage_readiness_report",
        lambda *_a, **_k: {"ok": True},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_queue_status",
        # Failure-class dlq_count drives the blocker; the cancels present in
        # runtime_cancelled_count are separate data and never redden readiness.
        lambda *_a, **_k: {
            "ok": False,
            "status": "dead_letter",
            "dlq_count": 1,
            "terminal_history_dlq_count": 1,
            "runtime_failed_count": 1,
            "runtime_cancelled_count": 2,
            "active_queue_depth": 0,
            "max_active_queue_depth": 0,
        },
    )

    payload = deployment_cli._deployment_ready_live_checks(
        deployment="demo",
        deployment_dir=tmp_path,
        dsn="postgres://okg/test",
        max_active_queue_depth=0,
        max_source_age_seconds=60,
        readiness_profile="general",
        skip_doctor=True,
    )

    dlq_blockers = [
        check for check in payload["blockers"]
        if check["id"] == "queues.dlq"
    ]
    assert dlq_blockers, payload["blockers"]
    assert dlq_blockers[0]["status"] == "blocker"
    assert "dead-letter" in dlq_blockers[0]["message"]
    assert "queues.unavailable" not in {
        check["id"] for check in payload["blockers"]
    }
