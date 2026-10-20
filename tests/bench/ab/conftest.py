"""Shared fixtures for the okg-bench (ab) suite."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


def git(cwd: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(cwd), *args], capture_output=True, text=True, check=True
    )
    return proc.stdout


@pytest.fixture
def tiny_repo(tmp_path: Path) -> Path:
    """A local git repo with three commits and a tag — enough surface for
    checkout-surgery and workdir tests."""
    repo = tmp_path / "tiny-repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-b", "main", str(repo)],
                   capture_output=True, check=True)
    git(repo, "config", "user.email", "bench@test")
    git(repo, "config", "user.name", "Bench Test")
    for i in range(3):
        (repo / "file.txt").write_text(f"revision {i}\n")
        git(repo, "add", "file.txt")
        git(repo, "commit", "-m", f"commit {i}")
    git(repo, "tag", "v1.0")
    return repo


@pytest.fixture
def swebench_fixture() -> Path:
    return FIXTURES / "swebench_two_instances.json"
