"""Slice 3 of #2573 (PACT ``unknown-s3-read-ok-sites``).

The ten non-PACT sites that read a check verdict with ``.get("ok", True)``
used to count a payload with no ``ok`` as a pass. Each now reads the verdict
with ``read_ok``; a missing verdict is reported as unknown by name and fails
closed (decision D1 option 1: not ok, exit 1, code ends in ``_unknown``).

The promise test is parametrized per site: a payload with no ``ok`` key yields
an ``*_unknown`` outcome, never a pass.
"""
from __future__ import annotations

import argparse
import io
import json
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import pytest

# --------------------------------------------------------------------------
# One runner per site. Each feeds the site a payload with no `ok` key and
# returns {"passed": bool, "unknown": str | None}: whether the site still
# called the check a pass, and the `*_unknown` code or reason it reported.
# --------------------------------------------------------------------------


def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("OKG_ENVIRONMENT_DESCRIPTOR", "OKG_DEPLOYMENT", "OKG_DSN"):
        monkeypatch.delenv(name, raising=False)


def _cli_outcome(rc: int, capsys: pytest.CaptureFixture[str], code: str) -> dict:
    captured = capsys.readouterr()
    stdout_lines = [line for line in captured.out.splitlines() if line.strip()]
    payload = json.loads(stdout_lines[-1]) if stdout_lines else {}
    reasons = payload.get("unknown_reasons") or {}
    reported = code in captured.err and "ok" in reasons and payload.get("ok") is None
    return {"passed": rc == 0, "unknown": code if reported else None}


def _site_metrics_disk(monkeypatch, capsys) -> dict:
    from okg.substrate.cli import metrics
    from okg.substrate.db import management

    _clean_env(monkeypatch)
    monkeypatch.setattr(
        management,
        "disk_usage_report",
        lambda *a, **kw: {"total_bytes": 10, "findings": []},
    )
    args = argparse.Namespace(dsn="postgresql://unused/x", deployment=None, top=20, deep=False)
    rc = metrics._cmd_metrics_disk(args, as_json=True)
    return _cli_outcome(rc, capsys, "metrics_disk_unknown")


def _site_deployment_bootstrap(monkeypatch, capsys) -> dict:
    from okg.substrate import deployment_bootstrap
    from okg.substrate.cli import deployment

    _clean_env(monkeypatch)
    monkeypatch.setattr(
        deployment_bootstrap, "status", lambda **kw: {"stage": "drafted"}
    )
    args = argparse.Namespace(
        subcmd="bootstrap", bootstrap_subcmd="status", deployment="dep"
    )
    rc = deployment._cmd_deployment(args, as_json=True)
    return _cli_outcome(rc, capsys, "deployment_bootstrap_unknown")


def _site_db_doctor(monkeypatch, capsys) -> dict:
    from okg.substrate.cli import main
    from okg.substrate.db import management

    _clean_env(monkeypatch)
    monkeypatch.setattr(
        management,
        "database_health_report",
        lambda *a, **kw: {"checks": [{"name": "size"}]},
    )
    args = argparse.Namespace(
        db_subcmd="doctor", subcmd="doctor", dsn="postgresql://unused/x", deployment=None
    )
    rc = main._cmd_db(args, as_json=True)
    return _cli_outcome(rc, capsys, "db_doctor_unknown")


def _microbench_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "source_name": "git",
        "pipeline_mode": "source-only",
        "sample_count": 3,
        "latency_ms": {"p95": 10.0},
    }
    row.update(overrides)
    return row


def _gate_outcome(out: dict, reason: str) -> dict:
    reasons = [v.get("reason") for v in out["gate"]["violations"]]
    return {
        "passed": bool(out["ok"]) or bool(out["gate"]["ok"]),
        "unknown": reason if reason in reasons else None,
    }


def _site_microbench_row(monkeypatch, capsys) -> dict:
    from okg.substrate.cli.main import _microbench_gate_result

    out = _microbench_gate_result(
        {"ok": True, "rows": [_microbench_row()]}, max_p95_ms=1000.0
    )
    return _gate_outcome(out, "row_ok_unknown")


def _site_microbench_report(monkeypatch, capsys) -> dict:
    from okg.substrate.cli.main import _microbench_gate_result

    out = _microbench_gate_result(
        {"rows": [_microbench_row(ok=True)]}, max_p95_ms=1000.0
    )
    return _gate_outcome(out, "report_ok_unknown")


def _site_microbench_out_ok(monkeypatch, capsys) -> dict:
    """The top-level `ok` the gate writes back (cli/main.py, `out["ok"]`)."""
    from okg.substrate.cli.main import _microbench_gate_result

    out = _microbench_gate_result(
        {"rows": [_microbench_row(ok=True)]}, max_p95_ms=1000.0
    )
    return {
        "passed": out["ok"] is not False,
        "unknown": "report_ok_unknown" if out["ok"] is False else None,
    }


def _site_mcp_principal(monkeypatch, capsys) -> dict:
    from okg.substrate.cli import deployment_readiness as readiness

    monkeypatch.setattr(
        readiness,
        "_ready_mcp_principal_login_status",
        lambda conn: {"status": "blocker", "role": "okg_mcp"},
    )

    reads: list[str] = []

    class _GraphReadConn:
        """Records whether the check went on to the graph read."""

        def __getattr__(self, name: str) -> Any:
            reads.append(name)
            raise RuntimeError("no database in this test")

    result = readiness._ready_mcp_graph_status(_GraphReadConn(), "dep")
    # The old path treated the principal as passing and went on to the graph
    # read; whatever that read returned, the principal verdict was a pass.
    return {
        "passed": bool(reads) or result.get("ok") is not False,
        "unknown": result.get("reason")
        if str(result.get("reason", "")).endswith("_unknown")
        else None,
    }


class _DoctorCursor:
    def __init__(self, conn: "_DoctorConn") -> None:
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        if "to_regclass" in sql:
            self._conn.one = ("dbos.workflow_status",)
        else:
            self._conn.all = [("wf-1", self._conn.updated_ms)]

    def fetchone(self):
        return self._conn.one

    def fetchall(self):
        return self._conn.all


class _DoctorConn:
    def __init__(self) -> None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        self.updated_ms = now_ms - 60 * 1000
        self.one: Any = None
        self.all: list = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return _DoctorCursor(self)


def _site_deployment_doctor_checks(monkeypatch, capsys) -> dict:
    from okg.substrate.runtime import publisher_health_doctor_overlay as pdo

    monkeypatch.setattr(pdo, "db_connect", lambda *a, **kw: _DoctorConn())
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_get_event",
        lambda **_kw: {
            "deployment": "dep",
            "ok": True,
            "deployment_checks": {
                "sources_fresh": {"ok": True},
                "webhook_probe": {"detail": "printed no verdict"},
            },
        },
    )
    verdict = pdo.check_deployment_doctor_health(deployment="dep", dsn="postgresql://x")
    return {
        "passed": verdict.ok,
        "unknown": verdict.code if verdict.code.endswith("_unknown") else None,
    }


def _site_runtime_services_overlay(monkeypatch, capsys) -> dict:
    from okg.substrate.runtime import publisher_health_doctor_overlay as pdo

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    monkeypatch.setattr(
        pdo,
        "_latest_doctor_verdict_field",
        lambda **kw: ({"watcher": {"alive": True}, "webhook": {}}, now_ms - 60_000),
    )
    out = pdo._try_doctor_verdict_overlay(
        deployment="dep", dsn="postgresql://x", threshold_minutes=10.0
    )
    assert out is not None
    return {
        "passed": out.ok,
        "unknown": out.code if out.code.endswith("_unknown") else None,
    }


def _site_scheduled_check(monkeypatch, capsys) -> dict:
    from okg.substrate.runtime.scheduled import _check_payload_ok
    from okg.substrate.unknown import Unknown

    val = _check_payload_ok({"check_name": "webhook_probe"})
    if isinstance(val, Unknown):
        return {"passed": False, "unknown": val.reason}
    return {"passed": bool(val), "unknown": None}


SITES: dict[str, tuple[Callable[..., dict], str]] = {
    "cli/metrics.py:disk": (_site_metrics_disk, "metrics_disk_unknown"),
    "cli/deployment.py:bootstrap": (
        _site_deployment_bootstrap,
        "deployment_bootstrap_unknown",
    ),
    "cli/main.py:db-doctor": (_site_db_doctor, "db_doctor_unknown"),
    "cli/main.py:microbench-row": (_site_microbench_row, "row_ok_unknown"),
    "cli/main.py:microbench-report": (_site_microbench_report, "report_ok_unknown"),
    "cli/main.py:microbench-out-ok": (_site_microbench_out_ok, "report_ok_unknown"),
    "cli/deployment_readiness.py:mcp-principal": (
        _site_mcp_principal,
        "mcp_principal_login_unknown",
    ),
    "runtime/publisher_health_doctor_overlay.py:deployment-checks": (
        _site_deployment_doctor_checks,
        "deployment_doctor_check_unknown",
    ),
    "runtime/publisher_health_doctor_overlay.py:runtime-services": (
        _site_runtime_services_overlay,
        "runtime_services_overlay_unknown",
    ),
    "runtime/scheduled.py:deployment-check": (
        _site_scheduled_check,
        "deployment_check_reported_no_ok",
    ),
}


@pytest.mark.parametrize("site", sorted(SITES))
def test_missing_ok_is_unknown_not_a_pass(site, monkeypatch, capsys) -> None:
    runner, expected = SITES[site]
    outcome = runner(monkeypatch, capsys)
    assert outcome == {"passed": False, "unknown": expected}, site


# --------------------------------------------------------------------------
# The known verdicts still read as before, and the roll-ups name the unknown.
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("payload", "expected_code"),
    [({"ok": True}, 0), ({"ok": False}, 1)],
)
def test_payload_verdict_exit_keeps_known_verdicts(payload, expected_code) -> None:
    from okg.substrate.cli._output import payload_verdict_exit

    err = io.StringIO()
    out, code = payload_verdict_exit(payload, source="probe", stream=err)
    assert (out, code, err.getvalue()) == (payload, expected_code, "")


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        ({"ok": "yes"}, "probe_ok_not_boolean"),
        ({"ok": None}, "probe_ok_not_boolean"),
        ({}, "probe_reported_no_ok"),
    ],
)
def test_payload_verdict_exit_fails_closed_on_unknown(payload, reason) -> None:
    from okg.substrate.cli._output import emit_result, payload_verdict_exit

    err = io.StringIO()
    out, code = payload_verdict_exit(payload, source="probe", stream=err)
    assert code == 1
    assert err.getvalue().startswith("error: probe_unknown: ")
    assert reason in err.getvalue()
    wire = io.StringIO()
    emit_result(out, as_json=True, stream=wire)
    doc = json.loads(wire.getvalue())
    assert doc["ok"] is None
    assert doc["unknown_reasons"] == {"ok": reason}


def test_payload_verdict_exit_non_mapping_is_unknown() -> None:
    from okg.substrate.cli._output import payload_verdict_exit

    err = io.StringIO()
    out, code = payload_verdict_exit(["not", "a", "mapping"], source="probe", stream=err)
    assert code == 1
    assert out == ["not", "a", "mapping"]
    assert "probe_not_an_object" in err.getvalue()


def test_cli_site_keeps_exit_zero_for_a_real_pass(monkeypatch, capsys) -> None:
    from okg.substrate.cli import metrics
    from okg.substrate.db import management

    _clean_env(monkeypatch)
    monkeypatch.setattr(
        management, "disk_usage_report", lambda *a, **kw: {"ok": True, "total_bytes": 1}
    )
    args = argparse.Namespace(dsn="postgresql://unused/x", deployment=None, top=20, deep=False)
    assert metrics._cmd_metrics_disk(args, as_json=True) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "unknown_reasons" not in json.loads(captured.out.strip().splitlines()[-1])


def test_scheduled_roll_up_names_unknown_checks_and_fails_closed() -> None:
    from okg.substrate.runtime.scheduled import _roll_up_deployment_checks

    verdict: dict[str, Any] = {"ok": True}
    counted = _roll_up_deployment_checks(
        verdict,
        {
            "sources_fresh": {"ok": True},
            "webhook_probe": {"check_name": "webhook_probe"},
            "disk": {"ok": False},
        },
    )
    assert counted == 2
    assert verdict["ok"] is False
    assert verdict["deployment_checks_unknown"] == {
        "webhook_probe": "deployment_check_reported_no_ok",
    }


def test_scheduled_roll_up_keeps_a_clean_run_ok() -> None:
    from okg.substrate.runtime.scheduled import _roll_up_deployment_checks

    verdict: dict[str, Any] = {"ok": True}
    assert _roll_up_deployment_checks(verdict, {"a": {"ok": True}}) == 0
    assert verdict == {"ok": True}


def test_deployment_doctor_message_counts_unknown_apart_from_failed(monkeypatch) -> None:
    from okg.substrate.runtime import publisher_health_doctor_overlay as pdo

    monkeypatch.setattr(pdo, "db_connect", lambda *a, **kw: _DoctorConn())
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_get_event",
        lambda **_kw: {
            "deployment": "dep",
            "deployment_checks": {
                "a": {"ok": True},
                "b": {"ok": False},
                "webhook_probe": {"detail": "x"},
                "garbled": "not an object",
            },
        },
    )
    verdict = pdo.check_deployment_doctor_health(deployment="dep", dsn="postgresql://x")
    assert verdict.ok is False
    # A failed check outranks an unknown one in the code; both are counted.
    assert verdict.code == "deployment_doctor_check_failed"
    assert (verdict.checks_total, verdict.checks_failed, verdict.checks_unknown) == (4, 1, 2)
    assert "1 of 4 deployment doctor check(s) failed" in verdict.message
    assert "2 of 4 reported no result (garbled, webhook_probe)" in verdict.message
    assert verdict.to_dict()["checks_unknown"] == 2


def test_deployment_doctor_all_passing_stays_healthy(monkeypatch) -> None:
    from okg.substrate.runtime import publisher_health_doctor_overlay as pdo

    monkeypatch.setattr(pdo, "db_connect", lambda *a, **kw: _DoctorConn())
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_get_event",
        lambda **_kw: {"deployment": "dep", "deployment_checks": {"a": {"ok": True}}},
    )
    verdict = pdo.check_deployment_doctor_health(deployment="dep", dsn="postgresql://x")
    assert (verdict.ok, verdict.code, verdict.checks_unknown) == (
        True,
        "deployment_doctor_healthy",
        0,
    )


def test_microbench_gate_passes_a_clean_report() -> None:
    from okg.substrate.cli.main import _microbench_gate_result

    out = _microbench_gate_result(
        {"ok": True, "rows": [_microbench_row(ok=True)]}, max_p95_ms=1000.0
    )
    assert out["ok"] is True
    assert out["gate"]["violations"] == []
    json.dumps(out)  # the gate artifact is written with json.dumps


def test_microbench_gate_names_unknown_report_beside_environment_blockers() -> None:
    from okg.substrate.cli.main import _microbench_gate_result

    out = _microbench_gate_result(
        {"rows": [], "environment_blockers": [{"reason": "host_busy"}]},
        max_p95_ms=1000.0,
    )
    reasons = [v["reason"] for v in out["gate"]["violations"]]
    assert reasons == ["host_busy", "report_ok_unknown"]
    assert out["ok"] is False
    json.dumps(out)
