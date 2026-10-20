"""A diff that cannot be read is reported as unknown, never as "no change".

Moved out of test_changed_files_agreement.py, whose subject is agreement
across the CI scripts under scripts/ci/ and which therefore does not ship in
the public copy of this repository. These two tests need only the product
readers (``pact.git`` and ``okg.substrate.pact.scope_drift``), so they ship.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from okg.substrate.pact import scope_drift
from pact import git as pact_git


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A repository with a base commit on ``agreement-base`` and one change on HEAD."""
    root = tmp_path / "work"
    root.mkdir()
    _git(root, "init", "-q", "-b", "dev")
    _git(root, "config", "user.email", "ci-test@example.invalid")
    _git(root, "config", "user.name", "CI Test")
    (root / "src").mkdir()
    (root / "src" / "keep.py").write_text("VALUE = 1\n", encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "base")
    _git(root, "branch", "agreement-base")
    (root / "src" / "keep.py").write_text("VALUE = 2\n", encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "change")
    return root


def test_an_unreadable_diff_is_not_drift_clean(
    repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """scope_drift used `if committed:`, so a failed diff read as no change.

    Reachable for real: in a blob-less partial clone with an unreachable
    remote, `--name-only` exits 0 but copy detection needs blobs and exits
    128. Simulated here by a reader that raises.
    """

    def unreadable(_root: Path, *range_args: str, **_kwargs: Any) -> list[Any]:
        raise pact_git.DiffUnavailable(
            f"git diff {' '.join(range_args)} exited 128: fatal: unable to fetch blob"
        )

    monkeypatch.setattr(scope_drift.pact_git, "diff_records", unreadable)
    changed = scope_drift.changed_paths(repo, base_branch="agreement-base")
    assert changed["diff_unreadable"]
    assert "unable to fetch blob" in changed["diff_unreadable"][0]

    manifest = SimpleNamespace(
        change=SimpleNamespace(id="agreement", affected_code=["src/"]),
        test_plan=[],
        evidence=[],
    )
    report = scope_drift.drift_report(repo, manifest, base_branch="agreement-base")
    assert report["ok"] is False
    assert report["diff_unreadable"]
    assert "NOT evaluated" in report["warning"]


def test_unparseable_name_status_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    """A truncated rename record (one path where two are due) raises."""
    monkeypatch.setattr(pact_git, "_diff_output", lambda root, args: "R100\0only_one_path\0")
    with pytest.raises(pact_git.DiffUnavailable, match="incomplete name-status record"):
        pact_git.diff_records(Path("."), "a..b")
