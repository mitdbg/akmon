"""Synthetic repository fixture for differential replay tests.

The fixture is deliberately local and small. It gives the replay harness a
stable commit sequence whose changes are known before any source-sync or
publisher work runs.
"""
from __future__ import annotations

import subprocess
from dataclasses import dataclass
import os
from pathlib import Path


SYNTHETIC_REPLAY_EXPERIMENT_ID = "synthetic-differential-e2e"
SYNTHETIC_REPO_NAME = "synthetic-differential"


@dataclass(frozen=True)
class SyntheticReplayCommit:
    """One replayable commit after the baseline."""

    label: str
    sha: str
    subject: str
    expected_changed_paths: tuple[str, ...]
    expected_stage_tags: tuple[str, ...]


@dataclass(frozen=True)
class SyntheticReplayRepo:
    """Created synthetic repo plus the replay commits after baseline."""

    repo_root: Path
    baseline_sha: str
    commits: tuple[SyntheticReplayCommit, ...]

    @property
    def replay_commit_shas(self) -> tuple[str, ...]:
        return tuple(commit.sha for commit in self.commits)

    def write_commits_file(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(self.replay_commit_shas) + "\n")
        return path


def create_synthetic_differential_repo(root: Path) -> SyntheticReplayRepo:
    """Create a deterministic git repo for differential replay proof tests."""
    repo_root = root / SYNTHETIC_REPO_NAME
    repo_root.mkdir(parents=True, exist_ok=True)
    _git(repo_root, "init", "-b", "main")
    _git(repo_root, "config", "user.name", "OKG Differential Replay")
    _git(repo_root, "config", "user.email", "okg-differential@example.com")

    _write(
        repo_root / "README.md",
        "# Synthetic Differential Replay\n\n"
        "The baseline package exposes `toy.core.compute_total`.\n",
    )
    _write(
        repo_root / "pyproject.toml",
        "[project]\n"
        'name = "synthetic-differential"\n'
        'version = "0.1.0"\n'
        "dependencies = []\n",
    )
    _write(repo_root / "src/toy/__init__.py", "from .core import compute_total\n")
    _write(
        repo_root / "src/toy/core.py",
        "def compute_total(values: list[int]) -> int:\n"
        "    return sum(values)\n",
    )
    _write(
        repo_root / "src/toy/legacy.py",
        "def old_total(values):\n"
        "    return sum(values)\n",
    )
    _write(
        repo_root / "docs/notes.md",
        "# Notes\n\n"
        "The docs mention the baseline compute_total helper.\n",
    )
    _commit_all(repo_root, "baseline package", index=0)
    baseline_sha = _head(repo_root)

    commits: list[SyntheticReplayCommit] = []

    _write(
        repo_root / "README.md",
        "# Synthetic Differential Replay\n\n"
        "The package exposes `toy.core.compute_total`.\n\n"
        "The docs now mention `toy.core.WeightedTotal` so code-reference "
        "linkers have a doc-only dirty scope.\n",
    )
    commits.append(_commit_spec(
        repo_root,
        "doc-only symbol mention",
        index=1,
        label="doc_only_symbol_mention",
        expected_changed_paths=("README.md",),
        expected_stage_tags=(
            "git-files",
            "doc-corpus",
            "code-reference-linker",
            "native-view",
        ),
    ))

    _write(
        repo_root / "src/toy/core.py",
        "class WeightedTotal:\n"
        "    def __init__(self, weight: int = 1) -> None:\n"
        "        self.weight = weight\n\n"
        "    def apply(self, values: list[int]) -> int:\n"
        "        return sum(values) * self.weight\n\n"
        "def compute_total(values: list[int]) -> int:\n"
        "    return WeightedTotal().apply(values)\n",
    )
    commits.append(_commit_spec(
        repo_root,
        "add weighted total symbol",
        index=2,
        label="code_symbol_change",
        expected_changed_paths=("src/toy/core.py",),
        expected_stage_tags=(
            "git-files",
            "code-structure",
            "interfaces",
            "code-reference-linker",
            "consistency-gate",
        ),
    ))

    _git(repo_root, "mv", "docs/notes.md", "docs/design.md")
    (repo_root / "src/toy/legacy.py").unlink()
    _git(repo_root, "rm", "src/toy/legacy.py")
    _write(
        repo_root / "docs/design.md",
        "# Design\n\n"
        "The design doc replaces notes and still references "
        "`toy.core.WeightedTotal`.\n",
    )
    commits.append(_commit_spec(
        repo_root,
        "rename docs and remove legacy module",
        index=3,
        label="rename_and_delete",
        expected_changed_paths=(
            "docs/design.md",
            "docs/notes.md",
            "src/toy/legacy.py",
        ),
        expected_stage_tags=(
            "git-files",
            "doc-corpus",
            "code-structure",
            "retraction",
            "derived-relation",
        ),
    ))

    _write(
        repo_root / "pyproject.toml",
        "[project]\n"
        'name = "synthetic-differential"\n'
        'version = "0.1.0"\n'
        'dependencies = ["typing-extensions>=4.0"]\n',
    )
    commits.append(_commit_spec(
        repo_root,
        "add package dependency",
        index=4,
        label="package_manifest_change",
        expected_changed_paths=("pyproject.toml",),
        expected_stage_tags=(
            "git-files",
            "package-manifests",
            "dependency-relation",
            "native-view",
        ),
    ))

    return SyntheticReplayRepo(
        repo_root=repo_root,
        baseline_sha=baseline_sha,
        commits=tuple(commits),
    )


def changed_paths(repo_root: Path, before: str, after: str) -> tuple[str, ...]:
    """Return git's renamed-aware changed paths between two commits."""
    out = _git(
        repo_root,
        "diff",
        "--name-only",
        "--find-renames",
        before,
        after,
        capture=True,
    )
    return tuple(sorted(line for line in out.splitlines() if line.strip()))


def _commit_spec(
    repo_root: Path,
    subject: str,
    *,
    index: int,
    label: str,
    expected_changed_paths: tuple[str, ...],
    expected_stage_tags: tuple[str, ...],
) -> SyntheticReplayCommit:
    _commit_all(repo_root, subject, index=index)
    return SyntheticReplayCommit(
        label=label,
        sha=_head(repo_root),
        subject=subject,
        expected_changed_paths=tuple(sorted(expected_changed_paths)),
        expected_stage_tags=expected_stage_tags,
    )


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _commit_all(repo_root: Path, subject: str, *, index: int) -> None:
    _git(repo_root, "add", "-A")
    env = {
        "GIT_AUTHOR_DATE": f"2026-01-01T00:00:{index:02d}+00:00",
        "GIT_COMMITTER_DATE": f"2026-01-01T00:00:{index:02d}+00:00",
    }
    _git(repo_root, "commit", "-m", subject, env=env)


def _head(repo_root: Path) -> str:
    return _git(repo_root, "rev-parse", "HEAD", capture=True).strip()


def _git(
    repo_root: Path,
    *args: str,
    capture: bool = False,
    env: dict[str, str] | None = None,
) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        env=None if env is None else {**os.environ, **env},
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout if capture else ""
