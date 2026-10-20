"""The proof scripts take their deployment and report paths from the caller.

Their old defaults named files the public copy does not carry: a private
deployment (``deployments/okg-ci``) and an evidence path inside one of this
repository's PACT change records. A default that points at an absent file
would fail deep inside the proof, or write a report into a record directory
that does not exist; a required argument refuses at once and says what it
needs (#2823).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    ("script", "argv", "required"),
    [
        ("prove_deployment_apply.py", ["--dsn", "postgresql://unused/okg_test"], "--deployment-dir"),
        (
            "prove_migration_census_integration.py",
            ["--dsn", "postgresql://unused/okg_test", "--out", "unused.json"],
            "--deployment-dir",
        ),
        ("prove_library_config_core_integration.py", [], "--out"),
    ],
)
def test_proof_script_requires_the_path_it_used_to_default(
    script: str, argv: list[str], required: str
) -> None:
    completed = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / script), *argv],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )

    assert completed.returncode == 2, completed.stderr
    assert "the following arguments are required" in completed.stderr
    assert required in completed.stderr
