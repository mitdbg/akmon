"""`pact check` verifies only the approvals that can change its verdict (#2376).

With no diff base the trailer read spans all history; every other change's
`Pact-Approved` line used to be verified too (several git calls each), which
cost ~200 s on the dev tip. Only this change's approvals can bind, so only
those are verified, and a missing one still fails closed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pact import approval as approvals
from pact import check as checker
from pact import git as pact_git
from pact.digest import intent_digest
from pact.store import load
from _v5_repo import MANIFEST, commit_all, git, make_repo, write_manifest

OTHER = "sha256:" + "0" * 64


@pytest.fixture
def repo(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    root = make_repo(tmp_path)
    write_manifest(root, MANIFEST)
    (root / "src").mkdir()
    (root / "src" / "a.py").write_text("x = 1\n", encoding="utf-8")
    commit_all(root, "feat")
    return root


def _approve(root: Path) -> str:
    digest = intent_digest(load(root / "pact/changes/demo/pact.yaml"))
    git(root, "commit", "-q", "--allow-empty", "-m", "approve", "-m", f"Pact-Approved: demo {digest}")
    return git(root, "rev-parse", "HEAD")


def _other_approvals(root: Path, count: int) -> None:
    for index in range(count):
        git(root, "commit", "-q", "--allow-empty", "-m", f"other {index}", "-m", f"Pact-Approved: other-{index} {OTHER}")


def _count_verifications(monkeypatch) -> list[str]:
    seen: list[str] = []
    real = approvals.code_rejection

    def spy(root, approval, head_ref="HEAD"):
        seen.append(approval.change_id)
        return real(root, approval, head_ref)

    monkeypatch.setattr(approvals, "code_rejection", spy)
    return seen


def test_approval_in_range_binds(repo: Path, monkeypatch) -> None:
    _other_approvals(repo, 3)
    approved = _approve(repo)
    seen = _count_verifications(monkeypatch)
    result = checker.run(repo, "demo", strict=True)
    assert result.state == "approved", result.blocked_by
    assert result.approval["commit"] == approved
    assert seen == ["demo"]


def test_missing_approval_fails_closed(repo: Path) -> None:
    _other_approvals(repo, 3)
    result = checker.run(repo, "demo", strict=True)
    assert "approval_missing" in result.blocked_by
    assert result.approval is None and result.state == "open"


def test_no_supplied_base_uses_the_derived_merge_base(repo: Path, monkeypatch) -> None:
    """Without --base the range starts at the merge base with origin/dev, so
    approvals already on dev are never read."""
    git(repo, "checkout", "-q", "dev")
    _other_approvals(repo, 2)
    git(repo, "push", "-q", "origin", "dev")
    git(repo, "fetch", "-q", "origin", "dev")
    git(repo, "checkout", "-q", "feature")
    git(repo, "rebase", "-q", "dev")
    merge_base = git(repo, "merge-base", "origin/dev", "HEAD")
    _approve(repo)
    ranges: list[str | None] = []
    real = pact_git.trailer_entries

    def spy(root, key, base, head_ref="HEAD"):
        if key == "Pact-Approved":
            ranges.append(base)
        return real(root, key, base, head_ref)

    monkeypatch.setattr(pact_git, "trailer_entries", spy)
    result = checker.run(repo, "demo", strict=True)
    assert ranges == [merge_base]
    assert result.state == "approved", result.blocked_by


def test_verification_cost_does_not_grow_with_history(repo: Path, monkeypatch) -> None:
    """The dev-tip case: head == dev, no base. Other changes' approvals in
    history are read by one `git log` but never verified one by one."""
    approved = _approve(repo)
    _other_approvals(repo, 40)
    git(repo, "branch", "-f", "dev", "HEAD")
    git(repo, "fetch", "-q", "origin", "dev")
    seen = _count_verifications(monkeypatch)
    result = checker.run(repo, "demo")
    assert "diff_base_unavailable" in result.warnings
    assert seen == ["demo"]
    assert result.approval is not None and result.approval["commit"] == approved
