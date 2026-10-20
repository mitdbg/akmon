from __future__ import annotations

import subprocess
from pathlib import Path


GENERATED_PACT_ARTIFACT_PATHS = (
    "pact/changes/*/pact.md",
    "pact/archive/*/pact.md",
    "pact/changes/*/pact.yaml.lock",
    "pact/archive/*/pact.yaml.lock",
)


def test_generated_pact_artifacts_are_not_tracked() -> None:
    repo = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "ls-files",
            "--",
            *GENERATED_PACT_ARTIFACT_PATHS,
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.splitlines() == []
