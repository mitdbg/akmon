"""REPORT-SCHEMA tests for TLC's constant-level invariant failure line.

FORMAL_REPORT_SCHEMA_TEST = True -- these tests monkeypatch ``subprocess.run``
to feed canned TLC output through ``run_tlc_formal_report``. They validate how
the report reads a violation, NOT any model property; they are not proofs.

TLC reports an invariant with no state variables as "The invariant of X is
equal to FALSE" before the search starts, not as "Invariant X is violated".
The declassify-external-effect witness of check_formal_deployment_backup_policy
relies on the first form (EffectClassificationPartitionsFailures is constant).
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from okg.substrate import formal_tlc
from okg.substrate.formal_tlc import TLCProperty, run_tlc_formal_report

FORMAL_REPORT_SCHEMA_TEST = True

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = REPO_ROOT / "formal" / "deployment_backup_policy"

CONSTANT_FAILURE = (
    "TLC2 Version 2.19 of 08 August 2024 (rev: 5a47802)\n"
    "Starting... (2026-10-25 20:13:42)\n"
    "Error: The invariant of EffectClassificationPartitionsFailures is equal to FALSE\n"
    "Finished in 00s at (2026-10-25 20:13:42)\n"
)


def _fake_tlc(monkeypatch, stdout: str, *, returncode: int) -> None:
    monkeypatch.setattr(
        formal_tlc,
        "find_tla2tools_jar",
        lambda _repo_root: REPO_ROOT / ".tools" / "tla2tools.jar",
    )
    monkeypatch.setattr(formal_tlc.shutil, "which", lambda _name: "/usr/bin/java")
    monkeypatch.setattr(
        formal_tlc.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=returncode, stdout=stdout, stderr="",
        ),
    )


def _run(tmp_path, invariant: str, *, expect_counterexample: bool):
    return run_tlc_formal_report(
        repo_root=REPO_ROOT,
        model_dir=MODEL_DIR,
        model=Path("DeploymentBackupPolicy.tla"),
        config=Path("DeploymentBackupPolicy.cfg"),
        out_dir=tmp_path / "out",
        properties=[
            TLCProperty("Probe", "constant-invariant probe",
                        expected_invariants=(invariant,)),
        ],
        checker_command="constant-invariant-probe",
        expect_counterexample=expect_counterexample,
        timeout_seconds=5,
    )


def test_constant_invariant_failure_is_the_expected_counterexample(
    tmp_path, monkeypatch
):
    _fake_tlc(monkeypatch, CONSTANT_FAILURE, returncode=151)
    result = _run(
        tmp_path,
        "EffectClassificationPartitionsFailures",
        expect_counterexample=True,
    )
    assert not result.run_failed
    assert result.invariant_violated


def test_constant_failure_of_another_invariant_is_not_the_witness(
    tmp_path, monkeypatch
):
    _fake_tlc(monkeypatch, CONSTANT_FAILURE, returncode=151)
    result = _run(tmp_path, "TypeInvariant", expect_counterexample=True)
    assert result.run_failed
    assert "EffectClassificationPartitionsFailures" in result.rows[0]["summary"]


def test_constant_invariant_failure_fails_a_proof_run(tmp_path, monkeypatch):
    _fake_tlc(monkeypatch, CONSTANT_FAILURE, returncode=151)
    result = _run(
        tmp_path,
        "EffectClassificationPartitionsFailures",
        expect_counterexample=False,
    )
    assert result.run_failed
    assert result.invariant_violated
