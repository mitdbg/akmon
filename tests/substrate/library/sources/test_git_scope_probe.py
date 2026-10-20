"""Tests for GitScopeProbe (#323) — the churn-immune reference probe.

The keystone test is ``test_mtime_churn_does_not_bust_token``: a worktree touch
that resets mtime without changing content must yield changed=False. That is the
exact regime that defeated the per-file FileWalk mtime fast-path and regressed
code-data-flow to ~106s.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from okg.substrate.library.sources.change_probe import run_probe
from okg.substrate.library.sources.git_scope_probe import (
    CURSOR_TOKEN_KEY,
    GitScopeProbe,
)


def _git(repo, *args):
    subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
    )


def _commit_all(repo, msg):
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", msg)


def _emit_logic_a(x):
    return x


@pytest.fixture
def repo(tmp_path):
    r = tmp_path / "repo"
    r.mkdir()
    _git(r, "init", "-q")
    _git(r, "config", "user.email", "t@example.com")
    _git(r, "config", "user.name", "Tester")
    (r / "a.py").write_text("print(1)\n")
    (r / "b.py").write_text("print(2)\n")
    _commit_all(r, "init")
    return r


def _probe(repo, config=None, emit=_emit_logic_a):
    return GitScopeProbe(
        repo_root=repo,
        config=config or {"include_globs": ["*.py"], "repo": "demo"},
        emit_targets=emit,
    )


def _cursor(token):
    return {CURSOR_TOKEN_KEY: token}


def test_first_run_is_changed_and_yields_token(repo):
    v = _probe(repo).probe(cursor=None, sync_scope=None)
    assert v.changed is True
    assert v.token  # a composed token
    assert v.reason == "no_prior_token"


def test_unchanged_scope_short_circuits(repo):
    p = _probe(repo)
    first = p.probe(cursor=None, sync_scope=None)
    second = p.probe(cursor=_cursor(first.token), sync_scope=None)
    assert second.changed is False
    assert second.reason == "git_scope_unchanged"


def test_mtime_churn_does_not_bust_token(repo):
    # THE keystone: a worktree touch (mtime moves, content identical) must NOT
    # be seen as a change. This is what the per-file mtime fast-path got wrong.
    p = _probe(repo)
    first = p.probe(cursor=None, sync_scope=None)
    far_future = 2_000_000_000  # move every tracked file's mtime
    for name in ("a.py", "b.py"):
        os.utime(repo / name, (far_future, far_future))
    second = p.probe(cursor=_cursor(first.token), sync_scope=None)
    assert second.changed is False, "mtime churn must not bust a content-identity token"


def test_content_edit_busts_token(repo):
    p = _probe(repo)
    first = p.probe(cursor=None, sync_scope=None)
    (repo / "a.py").write_text("print(1)\nprint(3)\n")  # real content change (uncommitted)
    second = p.probe(cursor=_cursor(first.token), sync_scope=None)
    assert second.changed is True


def test_untracked_file_busts_token(repo):
    p = _probe(repo)
    first = p.probe(cursor=None, sync_scope=None)
    (repo / "new.py").write_text("print('new')\n")
    second = p.probe(cursor=_cursor(first.token), sync_scope=None)
    assert second.changed is True


def test_dirty_file_outside_include_globs_does_not_bust_token(repo):
    (repo / "README.md").write_text("# clean\n")
    _commit_all(repo, "add readme")
    p = _probe(repo)
    first = p.probe(cursor=None, sync_scope=None)
    (repo / "README.md").write_text("# dirty but out of scope\n")
    second = p.probe(cursor=_cursor(first.token), sync_scope=None)
    assert second.changed is False


def test_ignored_untracked_file_does_not_bust_token(repo):
    (repo / ".gitignore").write_text("ignored.log\n")
    _commit_all(repo, "add ignore")
    p = _probe(repo)
    first = p.probe(cursor=None, sync_scope=None)
    (repo / "ignored.log").write_text("ignored\n")
    second = p.probe(cursor=_cursor(first.token), sync_scope=None)
    assert second.changed is False
    assert second.reason == "git_scope_unchanged"


def test_excluded_untracked_dir_is_pruned_before_scan(
    repo,
    monkeypatch: pytest.MonkeyPatch,
):
    p = _probe(repo, config={"repo": "demo", "exclude_globs": [".claude/**"]})
    first = p.probe(cursor=None, sync_scope=None)

    cache = repo / ".claude/worktrees/local/src"
    cache.mkdir(parents=True)
    (cache / "slow.py").write_text("print('slow')\n")

    original_scandir = os.scandir
    claude_root = (repo / ".claude").resolve()

    def guarded_scandir(path):  # noqa: ANN001
        scan_path = Path(path).resolve()
        if scan_path == claude_root or claude_root in scan_path.parents:
            raise AssertionError(f"probe descended into excluded path {scan_path}")
        return original_scandir(path)

    monkeypatch.setattr(os, "scandir", guarded_scandir)

    second = p.probe(cursor=_cursor(first.token), sync_scope=None)
    assert second.changed is False


def test_repo_local_claude_dir_is_pruned_without_config_exclude(
    repo,
    monkeypatch: pytest.MonkeyPatch,
):
    p = _probe(repo, config={"repo": "demo"})
    first = p.probe(cursor=None, sync_scope=None)

    cache = repo / ".claude/worktrees/local/src"
    cache.mkdir(parents=True)
    (cache / "slow.py").write_text("print('slow')\n")

    original_scandir = os.scandir
    claude_root = (repo / ".claude").resolve()

    def guarded_scandir(path):  # noqa: ANN001
        scan_path = Path(path).resolve()
        if scan_path == claude_root or claude_root in scan_path.parents:
            raise AssertionError(f"probe descended into local cache: {scan_path}")
        return original_scandir(path)

    monkeypatch.setattr(os, "scandir", guarded_scandir)

    second = p.probe(cursor=_cursor(first.token), sync_scope=None)
    assert second.changed is False


def test_subtree_probe_walks_only_subtree_for_untracked_paths(
    repo,
    monkeypatch: pytest.MonkeyPatch,
):
    openspec = repo / "openspec"
    openspec.mkdir()
    (openspec / "tracked.md").write_text("# tracked\n")
    _commit_all(repo, "add openspec")

    p = GitScopeProbe.for_subtree(
        openspec,
        config={"repo": "demo"},
        emit_targets=_emit_logic_a,
    )
    first = p.probe(cursor=None, sync_scope=None)

    cache = repo / ".claude/worktrees/local/openspec/archive"
    cache.mkdir(parents=True)
    (cache / "slow.md").write_text("# outside source scope\n")

    original_scandir = os.scandir
    claude_root = (repo / ".claude").resolve()

    def guarded_scandir(path):  # noqa: ANN001
        scan_path = Path(path).resolve()
        if scan_path == claude_root or claude_root in scan_path.parents:
            raise AssertionError(f"subtree probe scanned outside subtree: {scan_path}")
        return original_scandir(path)

    monkeypatch.setattr(os, "scandir", guarded_scandir)

    second = p.probe(cursor=_cursor(first.token), sync_scope=None)
    assert second.changed is False

    (openspec / "new.md").write_text("# new\n")
    third = p.probe(cursor=_cursor(second.token), sync_scope=None)
    assert third.changed is True


def test_edit_while_already_dirty_busts_token(repo):
    # The dirty path-set is unchanged across two edits to the same file; only a
    # content hash of the dirty file catches the second edit.
    p = _probe(repo)
    (repo / "a.py").write_text("dirty-1\n")
    v1 = p.probe(cursor=None, sync_scope=None)
    (repo / "a.py").write_text("dirty-2\n")  # still dirty, different content
    v2 = p.probe(cursor=_cursor(v1.token), sync_scope=None)
    assert v2.changed is True


def test_head_move_to_identical_tree_busts_token(repo):
    p = _probe(repo)
    first = p.probe(cursor=None, sync_scope=None)
    _git(repo, "commit", "-q", "--allow-empty", "-m", "empty")  # tree identical, HEAD moves
    second = p.probe(cursor=_cursor(first.token), sync_scope=None)
    assert second.changed is True, "identical-tree HEAD move must be detected via head sha"


def test_checkout_other_branch_at_same_head_busts_token(repo):
    p = _probe(repo)
    first = p.probe(cursor=None, sync_scope=None)
    _git(repo, "branch", "same-head")
    _git(repo, "checkout", "-q", "same-head")
    second = p.probe(cursor=_cursor(first.token), sync_scope=None)
    assert second.changed is True


def test_tracked_to_untracked_transition_busts_token(repo):
    p = _probe(repo)
    (repo / "a.py").write_text("same dirty bytes\n")
    dirty_tracked = p.probe(cursor=None, sync_scope=None)
    _git(repo, "rm", "--cached", "-q", "a.py")
    dirty_untracked = p.probe(cursor=_cursor(dirty_tracked.token), sync_scope=None)
    assert dirty_untracked.changed is True


def test_config_change_busts_token(repo):
    a = _probe(repo, config={"include_globs": ["*.py"]})
    first = a.probe(cursor=None, sync_scope=None)
    b = _probe(repo, config={"include_globs": ["*.py", "*.md"]})  # widened scope
    second = b.probe(cursor=_cursor(first.token), sync_scope=None)
    assert second.changed is True


def test_emission_logic_change_busts_token(repo):
    def emit_v2(x):
        return x + 1  # different body

    a = _probe(repo, emit=_emit_logic_a)
    first = a.probe(cursor=None, sync_scope=None)
    b = _probe(repo, emit=emit_v2)
    second = b.probe(cursor=_cursor(first.token), sync_scope=None)
    assert second.changed is True


def test_working_tree_delete_busts_token(repo):
    p = _probe(repo)
    first = p.probe(cursor=None, sync_scope=None)
    # Remove a tracked file in the working tree (uncommitted) — it appears in
    # porcelain status as deleted, so the dirty-set component busts the token.
    (repo / "b.py").unlink()
    second = p.probe(cursor=_cursor(first.token), sync_scope=None)
    assert second.changed is True, "a working-tree delete must be detected"


def test_committed_delete_busts_token(repo):
    p = _probe(repo)
    first = p.probe(cursor=None, sync_scope=None)
    _git(repo, "rm", "-q", "b.py")
    _git(repo, "commit", "-q", "-m", "rm b")
    second = p.probe(cursor=_cursor(first.token), sync_scope=None)
    assert second.changed is True, "a committed delete must be detected (tree sha)"


def test_run_probe_on_non_repo_yields_changed_true(tmp_path):
    # A probe pointed at a non-git dir raises inside; the runner boundary must
    # resolve it to changed=True (fail toward a full read), never changed=False.
    not_a_repo = tmp_path / "plain"
    not_a_repo.mkdir()
    p = _probe(not_a_repo)
    v = run_probe(p, cursor=None, sync_scope=None)
    assert v.changed is True
    assert v.reason.startswith("probe_error:")
