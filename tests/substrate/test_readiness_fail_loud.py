from __future__ import annotations

import json
import subprocess

from okg.substrate.cli import dogfood as dogfood_cli
from okg.substrate.cli import deployment as deployment_cli


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


def test_readiness_dependency_failure_is_not_clean(monkeypatch, tmp_path) -> None:
    import psycopg
    from okg.substrate.nomos import doctor as nomos_doctor

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *_args, **_kwargs: _FakeConnection(),
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
        lambda *_args, **_kwargs: {"ok": True},
    )

    def _queue_failure(*_args, **_kwargs):
        raise RuntimeError("queue probe exploded")

    monkeypatch.setattr(
        deployment_cli,
        "_ready_queue_status",
        _queue_failure,
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

    assert payload["blockers"]
    assert "database.connectivity" not in {
        check["id"] for check in payload["checks"]
    }
    check = payload["blockers"][0]
    assert check["id"] == "queues.unavailable"
    assert check["status"] == "blocker"
    assert check["ok"] is False
    assert check["details"] == {
        "ok": False,
        "status": "unavailable",
        "reason": "readiness_dependency_failed",
        "dependency": "_ready_queue_status",
        "error": "RuntimeError",
        "message": "queue probe exploded",
        "state_semantics": deployment_cli.READY_STATE_SEMANTICS,
    }


def test_readiness_dependency_import_failure_keeps_dependency_identity(
    monkeypatch,
    tmp_path,
) -> None:
    import psycopg

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *_args, **_kwargs: _FakeConnection(),
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

    def _lineage_failure(*_args, **_kwargs):
        raise ImportError("nomos readiness import failed")

    monkeypatch.setattr(
        deployment_cli,
        "_ready_nomos_source_lineage_status",
        _lineage_failure,
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

    assert "database.connectivity" not in {
        check["id"] for check in payload["checks"]
    }
    check = payload["blockers"][0]
    assert check["id"] == "nomos.source_lineage.unavailable"
    assert check["details"]["dependency"] == "_ready_nomos_source_lineage_status"
    assert check["details"]["error"] == "ImportError"
    assert check["details"]["message"] == "nomos readiness import failed"


def test_deployment_ready_blocks_on_current_runtime_dlq(
    monkeypatch,
    tmp_path,
) -> None:
    import psycopg
    from okg.substrate.nomos import doctor as nomos_doctor

    monkeypatch.setattr(
        "okg.substrate.runtime_dlq.current_runtime_dlq",
        lambda _cur, **_kwargs: {
            "current_count": 2,
            "historical_count": 0,
            "current_application_version": "v2",
            "current_failures": [
                {"workflow_id": "failed-a"},
                {"workflow_id": "failed-b"},
            ],
        },
    )

    class _Cursor:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, query: str) -> None:
            self.queries.append(str(query))

        def fetchone(self):
            return (True,)

        def fetchall(self):
            return [
                (
                    "okg-demo-runtime",
                    "failed",
                    "available",
                    0,
                    0,
                    0,
                    0,
                    0,
                    2,
                    1,
                    3,
                    3,
                    0,
                    None,
                    None,
                    600,
                    False,
                    None,
                    None,
                    "legacy_failed_workflow",
                )
            ]

    class _Conn(_FakeConnection):
        def __init__(self) -> None:
            self.cursor_obj = _Cursor()

        def cursor(self):
            return self.cursor_obj

    conn = _Conn()
    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *_args, **_kwargs: conn,
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
        lambda *_args, **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_source_groups_for_profile",
        # merged signature: (readiness_profile, *, deployment, deployment_dir)
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_mcp_status",
        lambda *_args, **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        "okg.substrate.runtime_queue_health.current_runtime_dlq",
        lambda _cur: {
            "current_count": 2,
            "historical_count": 0,
            "current_failures": [
                {
                    "workflow_id": f"failed-{index}",
                    "workflow_name": "legacy_failed_workflow",
                    "status": "ERROR",
                    "application_version": None,
                    "responsibility": {},
                }
                for index in range(2)
            ],
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

    # Current terminal failures are a real readiness failure. The view retains
    # failed_count=2 + cancelled_count=1 as audit data, while current-DLQ
    # authority supplies the two gate-visible failures.
    dlq_blockers = [c for c in payload["blockers"] if c["id"] == "queues.dlq"]
    assert dlq_blockers, payload["blockers"]
    check = dlq_blockers[0]
    assert check["status"] == "blocker"
    assert check["message"] == "dead-letter queue is non-empty"
    assert check["details"]["dlq_count"] == 2
    assert check["details"]["terminal_history_dlq_count"] == 2
    assert check["details"]["runtime_cancelled_count"] == 1
    assert check["details"]["status"] == "dead_letter"
    assert check["details"]["source"] == "okg.v_runtime_queue_health"
    joined = "\n".join(conn.cursor_obj.queries)
    assert "okg.v_runtime_queue_health" in joined
    assert "okg.v_job_queue" not in joined


def test_deployment_ready_blocks_on_stale_dbos_work_with_depth_allowance(
    monkeypatch,
    tmp_path,
) -> None:
    import psycopg
    from okg.substrate.nomos import doctor as nomos_doctor

    monkeypatch.setattr(
        "okg.substrate.runtime_dlq.current_runtime_dlq",
        lambda _cur, **_kwargs: {
            "current_count": 0,
            "historical_count": 0,
            "current_application_version": "v2",
            "current_failures": [],
        },
    )

    class _Cursor:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, query: str) -> None:
            self.queries.append(str(query))

        def fetchone(self):
            return (True,)

        def fetchall(self):
            return [
                (
                    "okg-demo-source-sync",
                    "stale",
                    "available",
                    1,
                    0,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    1200.0,
                    1200.0,
                    600,
                    True,
                    None,
                    None,
                        "legacy_runtime_workflow",
                )
            ]

    class _Conn(_FakeConnection):
        def cursor(self):
            return _Cursor()

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *_args, **_kwargs: _Conn(),
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
        lambda *_args, **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_source_groups_for_profile",
        # merged signature: (readiness_profile, *, deployment, deployment_dir)
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_mcp_status",
        lambda *_args, **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        "okg.substrate.runtime_queue_health.current_runtime_dlq",
        lambda _cur: {
            "current_count": 0,
            "historical_count": 0,
            "current_failures": [],
        },
    )

    payload = deployment_cli._deployment_ready_live_checks(
        deployment="demo",
        deployment_dir=tmp_path,
        dsn="postgres://okg/test",
        max_active_queue_depth=10,
        max_source_age_seconds=60,
        readiness_profile="general",
        skip_doctor=True,
    )

    check = payload["blockers"][0]
    assert check["id"] == "queues.stale"
    assert check["details"]["status"] == "stale"
    assert check["details"]["reason"] == "runtime_queue_health_stale"


def test_deployment_ready_blocks_on_non_ignored_active_depth(
    monkeypatch,
    tmp_path,
) -> None:
    import psycopg
    from okg.substrate.nomos import doctor as nomos_doctor

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *_args, **_kwargs: _FakeConnection(),
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
        lambda *_args, **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_queue_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "available",
            "active_queue_depth": 1,
            "max_active_queue_depth": 0,
            "dlq_count": 0,
            "queue_depth": {
                "embed": 0,
                "enrich": 0,
                "ingest": 1,
                "link": 0,
                "publish": 0,
            },
        },
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_source_groups_for_profile",
        # merged signature: (readiness_profile, *, deployment, deployment_dir)
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_mcp_status",
        lambda *_args, **_kwargs: {"ok": True},
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

    check = payload["blockers"][0]
    assert check["id"] == "queues.active_depth"
    assert check["details"]["active_queue_depth"] == 1


def test_dogfood_trust_unavailable_when_mcp_dependency_unavailable() -> None:
    trust_level = dogfood_cli._trust_level(
        environment={"ok": True},
        resolved_dsn=True,
        live={
            "generation": {
                "status": "published",
                "generation_id": "okg:demo:branch:default:gen:test",
            },
        },
        blockers=[{
            "id": "mcp.graph_read.unavailable",
            "status": "blocker",
            "ok": False,
            "details": {
                "status": "unavailable",
                "reason": "readiness_dependency_failed",
            },
        }],
    )

    assert trust_level == "unavailable"


def test_dogfood_trust_stale_context_requires_successful_mcp_read() -> None:
    blocker = {
        "id": "sources.freshness",
        "status": "blocker",
        "ok": False,
    }
    assert dogfood_cli._trust_level(
        environment={"ok": True},
        resolved_dsn=True,
        live={
            "generation": {"status": "published"},
            "mcp": {"ok": True},
        },
        blockers=[blocker],
    ) == "stale_context"
    assert dogfood_cli._trust_level(
        environment={"ok": True},
        resolved_dsn=True,
        live={
            "generation": {"status": "published"},
            "mcp": {"ok": False},
        },
        blockers=[blocker],
    ) == "unavailable"


def test_dogfood_doctor_helper_failure_is_unavailable(
    monkeypatch,
    tmp_path,
) -> None:
    import psycopg
    from okg.substrate.nomos import doctor as nomos_doctor

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *_args, **_kwargs: _FakeConnection(),
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
        lambda *_args, **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_queue_status",
        lambda *_args, **_kwargs: {"active_queue_depth": 0, "dlq_count": 0},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_source_freshness_status",
        lambda *_args, **_kwargs: {"ok": True, "missing": [], "stale": []},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_source_sync_liveness_status",
        lambda **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_mcp_status",
        lambda *_args, **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_build_source_sync_status_contract",
        lambda *_args, **_kwargs: {"ok": True, "status": "healthy"},
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_source_optimization_status",
        lambda **_kwargs: {"ok": True},
    )

    def _doctor_failure(*_args, **_kwargs):
        raise OSError("doctor helper exploded")

    monkeypatch.setattr(
        deployment_cli,
        "_ready_doctor_status",
        _doctor_failure,
    )

    payload = deployment_cli._deployment_ready_live_checks(
        deployment="demo",
        deployment_dir=tmp_path,
        dsn="postgres://okg/test",
        max_active_queue_depth=0,
        max_source_age_seconds=60,
        readiness_profile="dogfood",
        skip_doctor=False,
    )

    check = next(
        blocker for blocker in payload["blockers"]
        if blocker["id"] == "doctor.okg_workspace.unavailable"
    )
    assert check["id"] == "doctor.okg_workspace.unavailable"
    assert check["details"]["dependency"] == "_ready_doctor_status"
    assert check["details"]["error"] == "OSError"
    assert check["details"]["message"] == "doctor helper exploded"
    assert payload["doctor"] == check["details"]


def test_ready_doctor_status_reports_command_launch_failure(monkeypatch) -> None:
    dsn = "postgres://operator:real-secret@staging/graph"

    def _run_failure(*_args, **_kwargs):
        raise OSError(f"exec failed for {dsn}")

    monkeypatch.setattr(subprocess, "run", _run_failure)

    payload = deployment_cli._ready_doctor_status(
        "okg-workspace", dsn=dsn, skip=False
    )

    assert payload["ok"] is False
    assert payload["status"] == "command_failed"
    assert payload["reason"] == "doctor_command_failed"
    assert payload["error"] == "OSError"
    assert dsn not in payload["message"]
    assert "real-secret" not in payload["message"]
    assert payload["state_semantics"] == deployment_cli.READY_STATE_SEMANTICS


def test_ready_doctor_status_reports_command_timeout(monkeypatch) -> None:
    def _run_timeout(*_args, **_kwargs):
        raise subprocess.TimeoutExpired(cmd="okg doctor", timeout=120)

    monkeypatch.setattr(subprocess, "run", _run_timeout)

    payload = deployment_cli._ready_doctor_status(
        "okg-workspace", dsn="postgres://secret/graph", skip=False
    )

    assert payload["ok"] is False
    assert payload["status"] == "command_failed"
    assert payload["reason"] == "doctor_command_timeout"
    assert payload["timeout_seconds"] == 120
    assert payload["state_semantics"] == deployment_cli.READY_STATE_SEMANTICS

    ready_payload = {
        "blockers": [{
            "id": "doctor.okg_workspace",
            "status": "blocker",
            "ok": False,
            "details": payload,
        }],
    }
    deployment_cli._ready_finalize_payload_state(ready_payload)
    assert ready_payload["status"] == "command_failed"


def test_ready_doctor_status_reports_nonzero_command(monkeypatch) -> None:
    dsn = "postgres://operator:real-secret@staging/graph"

    def _run_failure(command, *_args, **_kwargs):
        return subprocess.CompletedProcess(
            command,
            returncode=2,
            stdout=json.dumps({"ok": False, "check": "demo", "dsn": dsn}),
            stderr=f"doctor failed for {dsn}",
        )

    monkeypatch.setattr(subprocess, "run", _run_failure)

    payload = deployment_cli._ready_doctor_status(
        "okg-workspace", dsn=dsn, skip=False
    )

    assert payload["ok"] is False
    assert payload["status"] == "command_failed"
    assert payload["reason"] == "doctor_command_nonzero"
    assert payload["returncode"] == 2
    assert payload["json"]["ok"] is False
    assert payload["json"]["check"] == "demo"
    assert "real-secret" not in payload["json"]["dsn"]
    assert "real-secret" not in payload["stderr"]
    assert payload["state_semantics"] == deployment_cli.READY_STATE_SEMANTICS

    ready_payload = {
        "blockers": [{
            "id": "doctor.okg_workspace",
            "status": "blocker",
            "ok": False,
            "details": payload,
        }],
    }
    deployment_cli._ready_finalize_payload_state(ready_payload)
    assert ready_payload["status"] == "command_failed"


def test_ready_payload_state_distinguishes_failure_modes() -> None:
    payload = {"blockers": []}
    deployment_cli._ready_finalize_payload_state(payload)
    assert payload["ok"] is True
    assert payload["status"] == "clean"
    assert payload["state_semantics"] == deployment_cli.READY_STATE_SEMANTICS

    payload = {
        "blockers": [{
            "id": "sources.freshness",
            "status": "blocker",
            "ok": False,
            "details": {"ok": False, "stale": ["pact"], "missing": []},
        }],
    }
    deployment_cli._ready_finalize_payload_state(payload)
    assert payload["ok"] is False
    assert payload["status"] == "stale"

    payload = {
        "blockers": [{
            "id": "queues.unavailable",
            "status": "blocker",
            "ok": False,
            "details": {
                "ok": False,
                "status": "unavailable",
                "reason": "readiness_dependency_failed",
            },
        }],
    }
    deployment_cli._ready_finalize_payload_state(payload)
    assert payload["ok"] is False
    assert payload["status"] == "unavailable"

    payload = {
        "blockers": [{
            "id": "doctor.okg_workspace",
            "status": "blocker",
            "ok": False,
            "details": {
                "ok": False,
                "status": "command_failed",
                "reason": "doctor_command_failed",
            },
        }],
    }
    deployment_cli._ready_finalize_payload_state(payload)
    assert payload["ok"] is False
    assert payload["status"] == "command_failed"


def test_readiness_state_semantics_are_explicit() -> None:
    assert deployment_cli.READY_STATE_SEMANTICS == {
        "clean": "the check ran and proved the dependency is ready",
        # Issue #1290: a probe that exceeded its deadline is neither a pass
        # nor a failure. The vocabulary has to be able to say so, or the
        # payload has to lie about it.
        "unknown": (
            "the check did not run to completion within its deadline, so "
            "this state is unproven -- it is neither a pass nor a failure"
        ),
        "stale": (
            "the check ran and found reachable runtime state outside the "
            "readiness bounds"
        ),
        "unavailable": (
            "the check could not prove runtime state because a dependency failed"
        ),
        "command_failed": (
            "a subprocess or helper command ran and returned a non-zero result"
        ),
    }
