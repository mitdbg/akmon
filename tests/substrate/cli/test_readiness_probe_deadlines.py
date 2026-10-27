"""One slow probe must not suppress every other probe — issue #1290.

On 2026-08-15 a DLQ query in the dogfood status path ran past 90 seconds.
Readiness executed its probes serially with no per-probe deadline, so the
operator received nothing: worker, generation, source, policy and database
all read as unavailable although several were independently readable. The
row-count limits in ``runtime_dlq`` bound how many rows are reduced, not how
long the scan takes; only a ``statement_timeout`` does that.

The fixture here is a real PostgreSQL block, not a mocked slow call: a second
connection holds an ``ACCESS EXCLUSIVE`` lock on ``dbos.application_versions``,
which ``current_runtime_dlq`` reads and ``okg.runtime_queue_health()`` does
not. That blocks the DLQ read and nothing else — so the assertions about
"only that probe" are measurements, not assumptions.
"""
from __future__ import annotations

import threading
import time
from pathlib import Path

import psycopg
import pytest

from okg.substrate.cli.deployment_readiness import (
    READINESS_DEADLINE_EXCEEDED,
    READINESS_PROBE_TIMEOUT,
    ReadinessDeadline,
    _ready_probe_timed_out,
    _ready_queue_status,
    _ready_required_dependency,
    ready_statement_timeout,
)

pytestmark = pytest.mark.postgres


_DBOS_DDL = """
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
CREATE TABLE IF NOT EXISTS dbos.application_versions (
  version_id uuid PRIMARY KEY,
  version_name text NOT NULL,
  version_timestamp bigint,
  created_at bigint
);
INSERT INTO dbos.application_versions (
  version_id, version_name, version_timestamp, created_at
) VALUES (
  '00000000-0000-0000-0000-000000000002', 'app-v2', 200, 200
) ON CONFLICT DO NOTHING;
INSERT INTO dbos.workflow_status (
  workflow_uuid, queue_name, status, name,
  created_at, updated_at, started_at_epoch_ms, priority
)
VALUES
  ('wf-failed', 'q-work', 'ERROR', NULL, now(), now(), NULL, 100)
ON CONFLICT DO NOTHING;
"""


@pytest.fixture()
def dbos_runtime(clean_runtime: str) -> str:
    with psycopg.connect(
        clean_runtime, autocommit=True,
    ) as conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
        cur.execute(_DBOS_DDL)
    return clean_runtime


class _DlqBlocker:
    """Hold a real ACCESS EXCLUSIVE lock on the DLQ-only relation.

    ``current_runtime_dlq`` reads ``dbos.application_versions``;
    ``okg.runtime_queue_health()`` reads only ``dbos.workflow_status``. Locking
    the former blocks the dead-letter read and leaves queue health alone.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._locked = threading.Event()
        self._release = threading.Event()
        self._thread = threading.Thread(target=self._hold, daemon=True)

    def _hold(self) -> None:
        with psycopg.connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "LOCK TABLE dbos.application_versions IN ACCESS EXCLUSIVE MODE"
            )
            self._locked.set()
            self._release.wait(timeout=60)
            conn.rollback()

    def __enter__(self) -> "_DlqBlocker":
        self._thread.start()
        assert self._locked.wait(timeout=10), "lock holder never acquired"
        return self

    def __exit__(self, *_exc) -> None:
        self._release.set()
        self._thread.join(timeout=15)


def _dbos_snapshot(dsn: str) -> tuple[int, int]:
    with psycopg.connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM dbos.workflow_status")
        workflows = int(cur.fetchone()[0])
        cur.execute("SELECT count(*) FROM dbos.application_versions")
        versions = int(cur.fetchone()[0])
    return workflows, versions


def test_blocked_dlq_read_leaves_queue_health_measured(
    dbos_runtime: str, tmp_path: Path,
) -> None:
    """The instance: a blocked DLQ read costs the DLQ answer only."""
    before = _dbos_snapshot(dbos_runtime)

    with _DlqBlocker(dbos_runtime):
        with psycopg.connect(
            dbos_runtime, autocommit=True, connect_timeout=5,
        ) as conn:
            started = time.monotonic()
            with ready_statement_timeout(conn, 1.0):
                status = _ready_queue_status(
                    conn,
                    deployment_dir=tmp_path,
                    max_active_queue_depth=0,
                )
            elapsed = time.monotonic() - started

    # Bounded: the incident ran past 90s with no bound at all.
    assert elapsed < 20.0, f"probe was not bounded: {elapsed:.1f}s"
    # Unknown, not a fabricated count and not a fabricated failure.
    assert status["dlq_status"] == "unknown"
    assert status["dlq_count"] is None
    assert status["current_version_dlq"]["reason"] == READINESS_PROBE_TIMEOUT
    assert status["current_version_dlq"]["probe_id"] == "current_runtime_dlq"
    # The half that WAS measured survives.
    assert status["queue_health_measured"] is True
    assert "runtime_queue_health" in status
    assert status["max_active_queue_depth"] == 0
    # Read-only: the probe changed nothing.
    assert _dbos_snapshot(dbos_runtime) == before


def test_blocked_dlq_read_is_unknown_not_a_blocker(
    dbos_runtime: str, tmp_path: Path,
) -> None:
    """Through the probe runner: `unknown`, and the run continues."""
    payload: dict = {"checks": [], "blockers": [], "warnings": []}
    deadline = ReadinessDeadline(total_seconds=30.0, per_probe_seconds=1.0)

    with _DlqBlocker(dbos_runtime):
        with psycopg.connect(
            dbos_runtime, autocommit=True, connect_timeout=5,
        ) as conn, ready_statement_timeout(conn, deadline.per_probe_seconds):
            ok, result = _ready_required_dependency(
                payload,
                check_id="queues.unavailable",
                dependency="_ready_queue_status",
                message="queue readiness check failed",
                call=lambda: _ready_dlq_only(conn),
                deadline=deadline,
            )

    assert ok is False
    assert _ready_probe_timed_out(result)
    assert result["status"] == "unknown"
    assert result["reason"] == READINESS_PROBE_TIMEOUT
    # Never a blocker: nothing was proven broken, it was not read.
    assert payload["blockers"] == []
    assert [row["id"] for row in payload["unknowns"]] == ["queues.unavailable"]
    assert payload["checks"][0]["status"] == "unknown"
    assert payload["checks"][0]["ok"] is None
    probe = payload["probes"][0]
    assert probe["probe_id"] == "_ready_queue_status"
    assert probe["outcome"] == "timeout"
    assert probe["timeout_seconds"] == pytest.approx(1.0, abs=0.5)
    assert probe["elapsed_seconds"] > 0


def _ready_dlq_only(conn) -> dict:
    from okg.substrate.runtime_dlq import current_runtime_dlq

    with conn.cursor() as cur:
        return current_runtime_dlq(cur)


def test_independent_probes_still_run_after_a_blocked_dlq(
    dbos_runtime: str, tmp_path: Path,
) -> None:
    """Evidence collected after the slow probe is not suppressed."""
    payload: dict = {"checks": [], "blockers": [], "warnings": []}
    deadline = ReadinessDeadline(total_seconds=30.0, per_probe_seconds=1.0)
    later_probe_ran = False

    with _DlqBlocker(dbos_runtime):
        with psycopg.connect(
            dbos_runtime, autocommit=True, connect_timeout=5,
        ) as conn, ready_statement_timeout(conn, deadline.per_probe_seconds):
            ok, queues = _ready_required_dependency(
                payload,
                check_id="queues.unavailable",
                dependency="_ready_queue_status",
                message="queue readiness check failed",
                call=lambda: _ready_dlq_only(conn),
                deadline=deadline,
            )
            assert not ok and _ready_probe_timed_out(queues)
            # An unrelated read on the SAME connection still works: the
            # cancelled statement did not poison the session.
            later_ok, later = _ready_required_dependency(
                payload,
                check_id="database.migrations",
                dependency="_probe_after_timeout",
                message="post-timeout probe failed",
                call=lambda: _count_workflows(conn),
                deadline=deadline,
            )
            later_probe_ran = later_ok

    assert later_probe_ran is True
    assert later == 1
    assert [row["outcome"] for row in payload["probes"]] == ["timeout", "ok"]


def _count_workflows(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM dbos.workflow_status")
        return int(cur.fetchone()[0])


def test_total_deadline_preempts_a_probe_instead_of_running_it(
    dbos_runtime: str,
) -> None:
    """A blown total deadline reports `unknown`; it does not run and hang."""
    payload: dict = {"checks": [], "blockers": [], "warnings": []}
    ticks = iter([0.0, 100.0, 100.0, 100.0, 100.0, 100.0])
    deadline = ReadinessDeadline(
        total_seconds=10.0, per_probe_seconds=5.0, clock=lambda: next(ticks),
    )

    def _must_not_run() -> dict:  # pragma: no cover - asserted below
        raise AssertionError("probe ran after the total deadline expired")

    ok, result = _ready_required_dependency(
        payload,
        check_id="sources.freshness.unavailable",
        dependency="_ready_source_freshness_status",
        message="source freshness readiness check failed",
        call=_must_not_run,
        deadline=deadline,
    )

    assert ok is False
    assert result["reason"] == READINESS_DEADLINE_EXCEEDED
    assert _ready_probe_timed_out(result)
    assert payload["blockers"] == []
    assert payload["probes"][0]["outcome"] == "deadline_exceeded"


def test_a_genuine_dependency_failure_is_still_a_blocker() -> None:
    """Timeouts became `unknown`; real failures must stay blockers."""
    payload: dict = {"checks": [], "blockers": [], "warnings": []}
    deadline = ReadinessDeadline(total_seconds=30.0, per_probe_seconds=5.0)

    def _boom() -> dict:
        raise RuntimeError("relation okg.nodes does not exist")

    ok, result = _ready_required_dependency(
        payload,
        check_id="database.migrations",
        dependency="_ready_database_status",
        message="database readiness check failed",
        call=_boom,
        deadline=deadline,
    )

    assert ok is False
    assert not _ready_probe_timed_out(result)
    assert result["reason"] == "readiness_dependency_failed"
    assert [row["id"] for row in payload["blockers"]] == ["database.migrations"]
    assert not payload.get("unknowns")
    assert payload["probes"][0]["outcome"] == "failed"


def test_unknown_denies_ok_and_names_its_own_status() -> None:
    """An unproven probe must never be absorbed into a clean result."""
    from okg.substrate.cli.deployment import (
        _ready_add_check,
        _ready_finalize_payload_state,
    )

    payload: dict = {"checks": [], "blockers": [], "warnings": []}
    _ready_add_check(payload, "queues.dlq", "unknown", "not measured", {
        "status": "unknown", "reason": READINESS_PROBE_TIMEOUT,
    })
    _ready_add_check(payload, "database.migrations", "pass", "migrated")
    _ready_finalize_payload_state(payload)

    assert payload["ok"] is False
    assert payload["status"] == "unknown"
    assert payload["blockers"] == []
    assert len(payload["unknowns"]) == 1


def _write_probe_fixture(root: Path) -> Path:
    import yaml

    deployment_dir = root / "probe-demo"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n", encoding="utf-8",
    )
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "probe-demo",
            "description": "readiness probe deadline fixture",
            "postgres": {"dsn": "${PROBE_DEMO_DSN}"},
            "schema_dir": "schemas",
            "source_registry": "source_registry.yaml",
            "modules": [],
            "enrichers": [],
        }),
        encoding="utf-8",
    )
    return deployment_dir


def test_live_checks_report_a_partial_result_when_the_dlq_blocks(
    dbos_runtime: str, tmp_path: Path,
) -> None:
    """End to end: one blocked probe, every other probe still reported.

    This is the shape of the 2026-08-15 incident. Before the fix the whole
    status surface returned nothing usable; the assertions below are that
    the surface still carries the probes that came AFTER the blocked one.
    """
    from okg.substrate.cli.deployment import (
        _deployment_ready_live_checks,
        _ready_finalize_payload_state,
    )

    deployment_dir = _write_probe_fixture(tmp_path)
    before = _dbos_snapshot(dbos_runtime)
    deadline = ReadinessDeadline(total_seconds=90.0, per_probe_seconds=2.0)

    with _DlqBlocker(dbos_runtime):
        started = time.monotonic()
        live = _deployment_ready_live_checks(
            deployment="probe-demo",
            deployment_dir=deployment_dir,
            dsn=dbos_runtime,
            max_active_queue_depth=0,
            max_source_age_seconds=86400,
            readiness_profile="general",
            skip_doctor=True,
            deadline=deadline,
        )
        elapsed = time.monotonic() - started

    assert elapsed < deadline.total_seconds, "run exceeded its total deadline"
    check_ids = [str(row.get("id")) for row in live["checks"]]

    # The blocked probe reads unknown — not a pass, not a blocker.
    assert "queues.dlq" in check_ids
    dlq_check = next(
        row for row in live["checks"] if row.get("id") == "queues.dlq"
    )
    assert dlq_check["status"] == "unknown"
    assert dlq_check["ok"] is None
    assert dlq_check not in live["blockers"]
    assert dlq_check in live["unknowns"]

    # Probes that run AFTER the queue probe are still reported. This is the
    # whole point: they were suppressed before.
    assert "mcp.graph_read" in check_ids or "mcp.graph_read.unavailable" in check_ids

    # Probes that ran BEFORE it are intact.
    assert "database.migrations" in check_ids

    # Every probe carries an execution record.
    probe_ids = {row["probe_id"] for row in live["probes"]}
    assert "_ready_queue_status" in probe_ids
    assert "_ready_mcp_status" in probe_ids
    assert all(row["elapsed_seconds"] >= 0 for row in live["probes"])
    assert live["deadline"]["per_probe_seconds"] == 2.0

    # Readiness is not established, and the exit code derived from it is
    # nonzero.
    payload = {
        "checks": list(live["checks"]),
        "blockers": list(live["blockers"]),
        "warnings": list(live["warnings"]),
        "unknowns": list(live["unknowns"]),
    }
    _ready_finalize_payload_state(payload)
    assert payload["ok"] is False
    assert (0 if payload["ok"] else 1) == 1

    # And the probe wrote nothing.
    assert _dbos_snapshot(dbos_runtime) == before


def test_environment_readiness_is_not_ready_while_a_probe_is_unknown(
    monkeypatch,
) -> None:
    """The third surface computing `ok` from blockers alone (#1290).

    `environment_readiness` folds the operational probes' checks into its
    own verdict. An `unknown` row is not a blocker, so `ok = not blockers`
    reported a ready environment on evidence never collected. Every other
    probe is stubbed passing so the unknown is the only thing under test.
    """
    from types import SimpleNamespace

    from okg.substrate import environment_readiness as env

    ok_payload = {"ok": True, "status": "ready"}
    for name in (
        "environment_release_status",
        "environment_process_authority_status",
        "environment_service_installation_status",
        "environment_release_handoff_status",
        "_probe_pact_prerequisites",
        "_probe_webhook_owner",
        "_load_mcp_registration",
    ):
        monkeypatch.setattr(
            env, name, lambda *_a, **_k: dict(ok_payload), raising=True,
        )
    monkeypatch.setattr(
        env, "_probe_ui_http",
        lambda *_a, **_k: {**ok_payload, "generation_id": "gen:1"},
    )
    monkeypatch.setattr(
        env, "_probe_mcp_registration",
        lambda *_a, **_k: {
            **ok_payload,
            "release_authority_ok": True,
            "inspect_ok": True,
            "search_ok": True,
            "generation_aligned": True,
        },
    )
    monkeypatch.setattr(
        env,
        "_probe_database_readiness",
        lambda *_a, **_k: {
            "generation": {"ok": True, "status": "published",
                           "generation_id": "gen:1"},
            "nomos_read_path": dict(ok_payload),
        },
    )
    monkeypatch.setattr(
        env,
        "_probe_operational_readiness",
        lambda *_a, **_k: {
            "checks": [
                {
                    "id": "database.migrations", "status": "pass",
                    "ok": True, "message": "migrated",
                },
                {
                    "id": "queues.dlq", "status": "unknown", "ok": None,
                    "message": "dead-letter count was not measured",
                    "details": {"reason": READINESS_PROBE_TIMEOUT},
                },
            ],
            "blockers": [], "warnings": [], "unknowns": [],
        },
    )

    report = env.environment_readiness_report(
        SimpleNamespace(
            id="probe-env",
            tier="production",
            graph_deployment_name="probe-demo",
            # The gate resolves the readiness profile from the composed
            # manifest (#3958); this one declares none, so no problem.
            manifest=SimpleNamespace(effective_raw={}),
        ),
        {},
    )

    assert report["ok"] is False, "an unmeasured probe reported as ready"
    assert report["status"] == "unknown"
    assert report["blockers"] == []
    assert [row["id"] for row in report["unknowns"]] == ["queues.dlq"]
