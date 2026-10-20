"""Lint-clean guard (enforce-lint-hygiene-gate).

Runs `ruff check src/ tests/` as a pytest so the lint gate is enforced on the
(already-blocking) pytest lane in addition to the dedicated `ruff` CI step.
Pure subprocess — no DB — so it runs in the no-DB smoke/fast lanes.

This is the second of two enforcement layers: the CI "Lint Python" step is now
blocking (its `continue-on-error: true` was removed), and this test catches a
regression even if someone re-softens that step.
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

# Repo root: tests/docs/test_lint_clean.py -> parents[2].
ROOT = Path(__file__).resolve().parents[2]


def test_ruff_check_is_clean() -> None:
    ruff = shutil.which("ruff")
    if ruff is None:  # ruff is a declared dev dependency; absence is a setup bug
        pytest.fail("ruff is not installed; it is a required dev dependency")
    result = subprocess.run(
        [ruff, "check", "src/", "tests/"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "ruff found lint violations in src/ or tests/ (the lint gate is now "
        "blocking — fix them or add a scoped per-file-ignore with a rationale):\n"
        f"{result.stdout}\n{result.stderr}"
    )
