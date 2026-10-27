"""Read-only git status checks must not touch the inspected repo's index.

Plain ``git status`` opportunistically refreshes stat data and rewrites
``.git/index`` under ``.git/index.lock``. A status check that only reads must
pass ``--no-optional-locks`` so it never takes that lock (issue #4086).
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from okg.substrate.catalog import ownership

pytestmark = pytest.mark.skipif(
    subprocess.run(["git", "--version"], capture_output=True).returncode != 0,
    reason="git is required",
)


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@example.invalid",
            *args,
        ],
        cwd=repo,
        check=True,
        capture_output=True,
    )


def _stale_index_repo(tmp_path: Path) -> Path:
    """A repo whose index stat data is stale, so plain status would rewrite it."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    tracked = repo / "a.txt"
    tracked.write_text("one\n")
    _git(repo, "add", "a.txt")
    _git(repo, "commit", "-q", "-m", "init")
    # Same content, new mtime: the index entry is stat-stale but not dirty.
    stat = tracked.stat()
    os.utime(tracked, ns=(stat.st_atime_ns, stat.st_mtime_ns + 5_000_000_000))
    return repo


def _index_state(repo: Path) -> tuple[bytes, int]:
    index = repo / ".git" / "index"
    return index.read_bytes(), index.stat().st_mtime_ns


def test_plain_status_would_rewrite_the_stale_index(tmp_path: Path) -> None:
    """Control: without the flag this fixture really does rewrite the index."""
    repo = _stale_index_repo(tmp_path)
    before = _index_state(repo)
    subprocess.run(
        ["git", "status", "--short"], cwd=repo, check=True, capture_output=True
    )
    assert _index_state(repo) != before


def test_git_dirty_leaves_the_index_and_lock_untouched(tmp_path: Path) -> None:
    repo = _stale_index_repo(tmp_path)
    before = _index_state(repo)

    # A clean tree answers None here (empty output), not False.
    assert not ownership._git_dirty(repo)

    assert _index_state(repo) == before
    assert not (repo / ".git" / "index.lock").exists()


def test_git_dirty_still_reports_a_real_change(tmp_path: Path) -> None:
    repo = _stale_index_repo(tmp_path)
    (repo / "a.txt").write_text("two\n")
    before = _index_state(repo)

    assert ownership._git_dirty(repo) is True
    assert _index_state(repo) == before


def test_git_dirty_passes_no_optional_locks_before_the_subcommand(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _stale_index_repo(tmp_path)
    seen: list[list[str]] = []
    real_run = subprocess.run

    def spy(argv, *args, **kwargs):  # type: ignore[no-untyped-def]
        seen.append(list(argv))
        return real_run(argv, *args, **kwargs)

    monkeypatch.setattr(ownership.subprocess, "run", spy)
    ownership._git_dirty(repo)

    assert seen == [["git", "--no-optional-locks", "status", "--short"]]


def test_git_dirty_is_none_outside_a_repo(tmp_path: Path) -> None:
    assert ownership._git_dirty(tmp_path / "missing") is None
