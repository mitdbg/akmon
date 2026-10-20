"""REPORT-SCHEMA tests for the TLC distinct-states vacuity floor.

FORMAL_REPORT_SCHEMA_TEST = True -- these tests monkeypatch ``subprocess.run``
to feed canned TLC output through ``run_tlc_formal_report``. They validate the
floor's fail-closed decision logic and report-row shape, NOT any model
property; they are not proofs.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate import formal_tlc
from okg.substrate.formal_tlc import TLCProperty, run_tlc_formal_report

FORMAL_REPORT_SCHEMA_TEST = True

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = REPO_ROOT / "formal" / "admission"

PROPERTIES = [
    TLCProperty(
        "FloorProbe",
        "states-floor plumbing probe",
        expected_invariants=("TypeOK",),
    ),
]


def _fake_tlc(monkeypatch, stdout: str, *, returncode: int = 0) -> None:
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


def _run(tmp_path, *, min_distinct_states=None, expect_counterexample=False):
    return run_tlc_formal_report(
        repo_root=REPO_ROOT,
        model_dir=MODEL_DIR,
        model=Path("Admission.tla"),
        config=Path("Admission.cfg"),
        out_dir=tmp_path / "out",
        properties=PROPERTIES,
        checker_command="floor-probe",
        min_distinct_states=min_distinct_states,
        expect_counterexample=expect_counterexample,
        timeout_seconds=5,
    )


PASS_WITH_STATES = (
    "Model checking completed. No error has been found.\n"
    "1000 states generated, 500 distinct states found, 0 states left on queue.\n"
)


def test_below_floor_run_fails_closed(tmp_path, monkeypatch):
    _fake_tlc(monkeypatch, PASS_WITH_STATES)
    result = _run(tmp_path, min_distinct_states=501)
    assert result.run_failed
    assert result.exit_code != 0
    assert result.distinct_states == 500
    row = result.rows[0]
    assert row["status"] == "fail"
    assert row["failure_class"] == "vacuous_state_space"
    assert row["proof_status"] == "not_freshly_proven"
    assert row["fresh"] is False


def test_missing_states_line_fails_closed(tmp_path, monkeypatch):
    _fake_tlc(monkeypatch, "Model checking completed. No error has been found.\n")
    result = _run(tmp_path)
    assert result.run_failed
    assert result.distinct_states is None
    assert result.rows[0]["failure_class"] == "vacuous_state_space"


def test_passing_run_carries_distinct_states(tmp_path, monkeypatch):
    _fake_tlc(monkeypatch, PASS_WITH_STATES)
    result = _run(tmp_path, min_distinct_states=500)
    assert not result.run_failed
    assert result.distinct_states == 500
    row = result.rows[0]
    assert row["status"] == "pass"
    assert row["distinct_states"] == 500
    assert row["proof_status"] == "freshly_proven"


def test_default_floor_applies_when_unset(tmp_path, monkeypatch):
    _fake_tlc(
        monkeypatch,
        "Model checking completed. No error has been found.\n"
        "1 states generated, 1 distinct states found, 0 states left on queue.\n",
    )
    result = _run(tmp_path)
    assert result.run_failed, (
        "a missing floor must default to the conservative minimum, "
        "not to skipping the check"
    )
    assert result.rows[0]["failure_class"] == "vacuous_state_space"


def test_counterexample_runs_are_not_floored(tmp_path, monkeypatch):
    # A witness run halts at the first violation; its state count says
    # nothing about coverage, so the floor must not apply.
    _fake_tlc(
        monkeypatch,
        "Error: Invariant TypeOK is violated.\n"
        "2 states generated, 1 distinct states found, 0 states left on queue.\n",
        returncode=1,
    )
    result = _run(tmp_path, min_distinct_states=1000, expect_counterexample=True)
    assert not result.run_failed
    assert result.rows[0]["status"] == "pass"


@pytest.mark.parametrize(
    ("stdout", "expected"),
    [
        (PASS_WITH_STATES, 500),
        ("no state line here", None),
        (
            "7 states generated, 3 distinct states found, 0 states left on queue.\n"
            "Progress: 90 states generated, 41 distinct states found.\n"
            "100 states generated, 42 distinct states found, 0 states left on queue.\n",
            42,
        ),
    ],
)
def test_distinct_states_parser(stdout, expected):
    assert formal_tlc._tlc_distinct_states(stdout) == expected


def _fake_tlc_timeout(monkeypatch, stdout: str) -> None:
    """TLC is killed by the timeout after printing `stdout`."""
    import subprocess as _subprocess

    monkeypatch.setattr(
        formal_tlc,
        "find_tla2tools_jar",
        lambda _repo_root: REPO_ROOT / ".tools" / "tla2tools.jar",
    )
    monkeypatch.setattr(formal_tlc.shutil, "which", lambda _name: "/usr/bin/java")

    def _raise(*_args, **_kwargs):
        raise _subprocess.TimeoutExpired(
            cmd=["java"], timeout=5, output=stdout, stderr=b"",
        )

    monkeypatch.setattr(formal_tlc.subprocess, "run", _raise)


TIMEOUT_AFTER_VIOLATION = (
    "Error: Invariant TypeOK is violated.\n"
    "Error: The behavior up to this point is:\n"
)


def test_a_violation_seen_before_the_kill_is_not_classified_as_a_timeout(
    tmp_path, monkeypatch,
):
    """A kill mid-error-trace is a semantic failure, not `not proven in time`.

    `timed_out` used to take precedence over `invariant_violated`, so this run
    reported failure_class="timeout" with exit code 124 -- the exact shape
    run_formal_suite.py's bounded not-proven bucket accepts. A real invariant
    violation could then stop reding its lane.
    """
    _fake_tlc_timeout(monkeypatch, TIMEOUT_AFTER_VIOLATION)
    result = _run(tmp_path)
    assert result.rows[0]["failure_class"] == "invariant_violation"
    assert result.exit_code != formal_tlc.TLC_TIMEOUT_EXIT_CODE
    assert result.rows[0]["status"] == "fail"


def test_a_timeout_with_no_violation_is_still_a_timeout(tmp_path, monkeypatch):
    _fake_tlc_timeout(monkeypatch, "Progress(12) at ...\n")
    result = _run(tmp_path)
    assert result.rows[0]["failure_class"] == "timeout"
    assert result.exit_code == formal_tlc.TLC_TIMEOUT_EXIT_CODE
