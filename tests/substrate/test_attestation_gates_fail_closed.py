"""Attestation gates must attest positively; absence of proof is a failure.

Regression coverage for the remediate-audit-2026-07 `sweep-gates` task
(issues #684, #690, #670). Each test constructs the exact silent-pass shape the
audit found and asserts the gate now fails closed. No DB fixture is used: every
gate is exercised through a pure entrypoint or a hand-built fake connection, so
the module carries no `postgres` marker.
"""

from __future__ import annotations

import os
import time

import yaml

from okg.substrate import formal_assurance
from okg.substrate.cli import deployment as deployment_cli


# --------------------------------------------------------------------------
# #684 — formal-assurance normal-model gate consumes the strict TLC report
# --------------------------------------------------------------------------

def _write_poros_fixture(root, *, results, stdout: str) -> None:
    """Lay out a minimal Poros tree whose coverage is waived so the model
    status reaches the normal-model report check, then write the strict
    formal-report.yaml (and stdout log) the gate must consume."""
    (root / "formal" / "poros").mkdir(parents=True)
    # One real input file so `_existing_paths` resolves; make it OLDER than the
    # artifacts so the freshness check passes.
    cfg = root / "formal" / "poros" / "PorosExchange.cfg"
    cfg.write_text("INVARIANT Safety\n", encoding="utf-8")
    old = time.time() - 10_000
    os.utime(cfg, (old, old))

    scratch = root / ".scratch" / "formal" / "poros"
    scratch.mkdir(parents=True)
    (scratch / "tlc.stdout.log").write_text(stdout, encoding="utf-8")
    (scratch / "formal-report.yaml").write_text(
        yaml.safe_dump({"results": results}, sort_keys=False),
        encoding="utf-8",
    )


def _waived_registration() -> formal_assurance.FormalModelRegistration:
    waiver = formal_assurance.FormalCoverageWaiver(reason="test", owner="test")
    return formal_assurance.FormalModelRegistration(
        model_id="test-poros",
        capability="poros",
        model_path="formal/poros/PorosExchange.tla",
        checker_command="make verify-poros",
        assurance_tier="tier2",
        assumptions=(),
        required_artifacts=(".scratch/formal/poros/tlc.stdout.log",),
        counterexample_artifact=(
            ".scratch/formal/poros-counterexample/counterexample.txt"
        ),
        coverage_requirements=(
            formal_assurance.FormalCoverageRequirement(
                coverage_id="cx",
                kind="intentional_counterexample",
                description="d",
                artifact=(
                    ".scratch/formal/poros-counterexample/counterexample.txt"
                ),
                waiver=waiver,
            ),
            formal_assurance.FormalCoverageRequirement(
                coverage_id="nv",
                kind="non_vacuity_refinement",
                description="d",
                artifact=(
                    ".scratch/formal/poros-refinement/refinement-state.json"
                ),
                waiver=waiver,
            ),
        ),
    )


def _evaluate(root):
    return formal_assurance.evaluate_formal_assurance(
        root, registrations=(_waived_registration(),)
    )


def _pass_row(**overrides):
    row = {
        "property_id": "Safety",
        "status": "pass",
        "proof_status": "freshly_proven",
        "timed_out": False,
        "distinct_states": 641,
    }
    row.update(overrides)
    return row


def test_formal_gate_passes_on_genuine_freshly_proven_report(tmp_path) -> None:
    """Positive control: a freshly-proven, non-vacuous report is accepted so the
    strict consumer does not reject a real pass."""
    _write_poros_fixture(
        tmp_path,
        results=[_pass_row()],
        stdout="Model checking completed. No error has been found.\n",
    )
    result = _evaluate(tmp_path)
    assert result["ok"] is True
    assert result["models"][0]["latest_status"] in {"pass", "waived"}


def test_formal_gate_fails_on_oom_report(tmp_path) -> None:
    """#684: an OOM run (stdout has no violation strings) must FAIL, not pass on
    the absence of `Error: Invariant`."""
    _write_poros_fixture(
        tmp_path,
        results=[
            _pass_row(
                status="fail",
                proof_status="not_freshly_proven",
                failure_class="tlc_error",
            )
        ],
        stdout="Starting... java.lang.OutOfMemoryError: Java heap space\n",
    )
    result = _evaluate(tmp_path)
    assert result["ok"] is False
    assert result["models"][0]["latest_status"] == "fail"


def test_formal_gate_fails_on_timeout_report(tmp_path) -> None:
    """#684: a timed-out run must FAIL."""
    # A report that claims a fresh pass but also carries timed_out=True is an
    # inconsistent artifact; the timeout must win and the gate must FAIL.
    _write_poros_fixture(
        tmp_path,
        results=[_pass_row(timed_out=True)],
        stdout="Progress(12) ... still exploring\n",
    )
    result = _evaluate(tmp_path)
    assert result["ok"] is False
    assert result["models"][0]["latest_status"] == "fail"
    assert "timed out" in result["models"][0]["status_reason"]


def test_formal_gate_fails_on_zero_distinct_states_report(tmp_path) -> None:
    """#684: a vacuous run (below the distinct-states floor) must FAIL even when
    the row claims a pass."""
    _write_poros_fixture(
        tmp_path,
        results=[_pass_row(distinct_states=1)],
        stdout="1 states generated, 1 distinct states found.\n",
    )
    result = _evaluate(tmp_path)
    assert result["ok"] is False
    assert result["models"][0]["latest_status"] == "fail"
    assert "vacuous" in result["models"][0]["status_reason"]


def test_formal_gate_fails_on_missing_report(tmp_path) -> None:
    """#684: a stdout log with no violation strings but NO strict report must
    FAIL rather than infer a pass from log silence."""
    _write_poros_fixture(
        tmp_path,
        results=[_pass_row()],
        stdout="Model checking completed. No error has been found.\n",
    )
    (tmp_path / ".scratch" / "formal" / "poros" / "formal-report.yaml").unlink()
    result = _evaluate(tmp_path)
    assert result["ok"] is False
    assert result["models"][0]["latest_status"] == "fail"
    assert "missing" in result["models"][0]["status_reason"]


# --------------------------------------------------------------------------
# #690 — deployment-readiness doctor gate treats absent JSON as a blocker
# --------------------------------------------------------------------------

def _completed(returncode: int, stdout: str, stderr: str = ""):
    class _Result:
        pass

    r = _Result()
    r.returncode = returncode
    r.stdout = stdout
    r.stderr = stderr
    return r


# `_ready_doctor_status` takes the deployment DSN so it can REDACT the DSN
# out of doctor stdout/stderr before it lands in a readiness payload. These
# gates assert the #690 fail-closed verdict logic, not redaction, but they
# must still supply one -- the parameter is deliberately required so a
# caller cannot silently emit un-redacted doctor output.
_DOCTOR_DSN = "postgresql://doctor:secret@127.0.0.1:5432/demo"


def test_doctor_gate_blocks_on_unparseable_json(monkeypatch) -> None:
    """#690: returncode 0 with an unparseable/empty doctor payload must fail
    closed, not pass on `(parsed or {}).get('ok', True)`."""
    import subprocess

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **k: _completed(0, "not-json-at-all", ""),
    )
    payload = deployment_cli._ready_doctor_status(
        "demo", skip=False, dsn=_DOCTOR_DSN
    )
    assert payload["ok"] is False
    assert payload["status"] == "command_failed"
    assert payload["reason"] == "doctor_json_unparseable"


def test_doctor_gate_blocks_on_empty_stdout(monkeypatch) -> None:
    """#690: a zero-exit doctor that printed nothing is not a pass."""
    import subprocess

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **k: _completed(0, "", ""),
    )
    payload = deployment_cli._ready_doctor_status(
        "demo", skip=False, dsn=_DOCTOR_DSN
    )
    assert payload["ok"] is False
    assert payload["reason"] == "doctor_json_unparseable"


def test_doctor_gate_passes_on_positive_json(monkeypatch) -> None:
    """Positive control: a real `{"ok": true}` doctor payload passes."""
    import subprocess

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **k: _completed(0, '{"ok": true, "check": "demo"}', ""),
    )
    payload = deployment_cli._ready_doctor_status(
        "demo", skip=False, dsn=_DOCTOR_DSN
    )
    assert payload["ok"] is True
    assert payload["status"] == "pass"


# --------------------------------------------------------------------------
# #690 — zero readiness_required sources is a blocker, not a blanket pass
# --------------------------------------------------------------------------

def test_readiness_blocks_on_zero_required_sources() -> None:
    """#690: a ledger with source rows but none readiness_required must NOT
    certify ok=True on having measured nothing."""
    ledger = {
        "deployment": "demo",
        "generated_at": "2026-07-14T00:00:00+00:00",
        "source_count": 2,
        "rows": [
            {"row_kind": "source", "name": "git", "readiness_required": False},
            {"row_kind": "source", "name": "pact", "readiness_required": False},
        ],
    }
    status = deployment_cli._ready_source_optimization_status_from_ledger(
        ledger, latency_budget_ms=1000.0,
    )
    assert status["ok"] is False
    assert status["status"] == "blocked"
    assert any(
        v.get("reason") == "no_readiness_required_sources"
        for v in status["violations"]
    )


# --------------------------------------------------------------------------
# #690 — a source with no latency measurement is not-passing
# --------------------------------------------------------------------------

def test_readiness_source_without_latency_measurement_is_not_passing() -> None:
    """#690: a full-pipeline proof recorded with NO latency measurement must
    return a violation reason, not fall through to the terminal pass (None)."""
    row = {
        "name": "git",
        "readiness_required": True,
        "row_kind": "source",
        # Has a full-pipeline proof (scenario present) and a claimed benchmark
        # pass, but NO latest_full_pipeline_latency_p95_ms — nothing measured.
        "proof_status": "full_pipeline_benchmark_passed",
        "latest_full_pipeline_scenario": "incremental",
        "latest_full_pipeline_expected_delta_source": "explicit",
        "latest_full_pipeline_expected_delta_units": 5,
        "latest_full_pipeline_observed_delta_units": 5,
        "latest_full_pipeline_source_work_units": 5,
        "latest_full_pipeline_work_amplification_ratio": 1.0,
    }
    reason = deployment_cli._ready_source_optimization_violation_reason(
        row, latency_budget_ms=1000.0,
    )
    assert reason == "missing_latency_measurement"


# --------------------------------------------------------------------------
# #670 — readiness probes the EFFECTIVE MCP principal (okg_mcp) login authority
# --------------------------------------------------------------------------

class _FakeCursor:
    def __init__(self, rolcanlogin):
        self._rolcanlogin = rolcanlogin

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params=None):
        self._sql = sql
        self._params = params

    def fetchone(self):
        return (self._rolcanlogin,)


class _FakeConn:
    def __init__(self, rolcanlogin):
        self._rolcanlogin = rolcanlogin

    def cursor(self):
        return _FakeCursor(self._rolcanlogin)


def test_mcp_gate_blocks_on_nologin_principal() -> None:
    """#670: an okg_mcp role that cannot log in (rolcanlogin=false) must fail
    readiness with a remediation message naming rolcanlogin — not pass because
    the owner connection can still read."""
    payload = deployment_cli._ready_mcp_status(_FakeConn(False), "demo")
    assert payload["ok"] is False
    assert payload["reason"] == "required_authority_unavailable"
    assert payload["rolcanlogin"] is False
    assert "rolcanlogin" in payload["message"]
    assert payload["next_command"] == "ALTER ROLE okg_mcp LOGIN"
