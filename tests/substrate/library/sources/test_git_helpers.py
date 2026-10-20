from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pygit2

from okg.substrate.library.sources._git_helpers import _commit_name_status


class _FakeFile:
    def __init__(self, path: str) -> None:
        self.path = path


class _FakeDelta:
    def __init__(
        self,
        status: int,
        *,
        old_path: str = "old.py",
        new_path: str = "new.py",
    ) -> None:
        self.status = status
        self.old_file = _FakeFile(old_path)
        self.new_file = _FakeFile(new_path)


class _FakeDiff:
    def __init__(self, deltas: list[_FakeDelta]) -> None:
        self.deltas = deltas
        self.find_similar_calls = 0

    def find_similar(self) -> None:
        self.find_similar_calls += 1


class _FakeTree:
    def __init__(self, diff: _FakeDiff | None = None) -> None:
        self._diff = diff

    def diff_to_tree(self, _tree: object) -> _FakeDiff:
        assert self._diff is not None
        return self._diff


class _FakeCommit:
    def __init__(self, diff: _FakeDiff) -> None:
        parent_tree = _FakeTree(diff)
        self.parents = [type("Parent", (), {"tree": parent_tree})()]
        self.tree = _FakeTree()


def test_commit_name_status_does_not_find_similar_by_default() -> None:
    diff = _FakeDiff(
        [
            _FakeDelta(
                pygit2.GIT_DELTA_ADDED,
                old_path="missing.py",
                new_path="added.py",
            ),
            _FakeDelta(
                pygit2.GIT_DELTA_DELETED,
                old_path="deleted.py",
                new_path="missing.py",
            ),
        ]
    )

    name_status = _commit_name_status(repo=object(), commit=_FakeCommit(diff))  # type: ignore[arg-type]

    assert diff.find_similar_calls == 0
    assert name_status == [("A", "added.py", None), ("D", "deleted.py", None)]


def test_commit_name_status_find_similar_is_explicit_opt_in() -> None:
    diff = _FakeDiff(
        [
            _FakeDelta(
                pygit2.GIT_DELTA_RENAMED,
                old_path="old_name.py",
                new_path="new_name.py",
            )
        ]
    )

    name_status = _commit_name_status(
        repo=object(),  # type: ignore[arg-type]
        commit=_FakeCommit(diff),  # type: ignore[arg-type]
        detect_renames=True,
    )

    assert diff.find_similar_calls == 1
    assert name_status == [("R", "new_name.py", "old_name.py")]


# ---------------------------------------------------------------------------
# Issue #3291: the per-thread cached handle must see later index changes.
# ---------------------------------------------------------------------------

_GIT_IDENTITY = {
    "GIT_AUTHOR_NAME": "Index Test",
    "GIT_AUTHOR_EMAIL": "index-test@example.invalid",
    "GIT_COMMITTER_NAME": "Index Test",
    "GIT_COMMITTER_EMAIL": "index-test@example.invalid",
}


def _git(root: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        env={
            **os.environ,
            **_GIT_IDENTITY,
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
        },
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _repo_with(root: Path, *names: str) -> Path:
    root.mkdir()
    _git(root, "init", "-q")
    for name in names:
        (root / name).write_text(f"{name}\n", encoding="utf-8")
    _git(root, "add", *names)
    _git(root, "commit", "-q", "-m", "seed")
    return root


def test_list_tracked_sees_a_subprocess_rm_and_add(tmp_path: Path) -> None:
    from okg.substrate.library.sources._git_helpers import in_proc_list_tracked

    repo = _repo_with(tmp_path / "repo", "alpha.zz", "beta.zz", "gamma.zz")
    # The first call opens and caches this thread's handle and its index.
    assert in_proc_list_tracked(repo) == {"alpha.zz", "beta.zz", "gamma.zz"}

    _git(repo, "rm", "-q", "beta.zz")
    assert in_proc_list_tracked(repo) == {"alpha.zz", "gamma.zz"}

    (repo / "delta.zz").write_text("delta\n", encoding="utf-8")
    _git(repo, "add", "delta.zz")
    assert in_proc_list_tracked(repo) == {"alpha.zz", "gamma.zz", "delta.zz"}

    # An unchanged index reads the same on the next call.
    assert in_proc_list_tracked(repo) == {"alpha.zz", "gamma.zz", "delta.zz"}


def test_list_tracked_sees_index_changes_in_a_linked_worktree(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources._git_helpers import in_proc_list_tracked

    main = _repo_with(tmp_path / "main", "alpha.zz", "beta.zz")
    linked = tmp_path / "linked"
    _git(main, "worktree", "add", "-q", "-b", "side", str(linked))
    assert (linked / ".git").is_file()
    assert in_proc_list_tracked(linked) == {"alpha.zz", "beta.zz"}

    _git(linked, "rm", "-q", "alpha.zz")
    (linked / "epsilon.zz").write_text("epsilon\n", encoding="utf-8")
    _git(linked, "add", "epsilon.zz")
    assert in_proc_list_tracked(linked) == {"beta.zz", "epsilon.zz"}
    # The main worktree's index is its own and did not change.
    assert in_proc_list_tracked(main) == {"alpha.zz", "beta.zz"}


def test_list_tracked_reads_a_deleted_index_as_empty(tmp_path: Path) -> None:
    from okg.substrate.library.sources._git_helpers import in_proc_list_tracked

    repo = _repo_with(tmp_path / "repo", "alpha.zz")
    assert in_proc_list_tracked(repo) == {"alpha.zz"}

    (repo / ".git" / "index").unlink()
    result = subprocess.run(
        ["git", "-C", str(repo), "ls-files"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout == ""
    assert in_proc_list_tracked(repo) == set()


def test_status_reads_a_deleted_index_as_empty(tmp_path: Path) -> None:
    from okg.substrate.library.sources._git_helpers import (
        in_proc_status_porcelain,
        in_proc_status_porcelain_paths,
    )

    repo = _repo_with(tmp_path / "repo", "alpha.zz")
    # The first calls open and cache this thread's handle and its index.
    assert in_proc_status_porcelain(repo) == set()
    assert in_proc_status_porcelain_paths(repo, ["alpha.zz"]) == set()

    # With no index, git reports the committed file as deleted from the
    # index and present as untracked.
    (repo / ".git" / "index").unlink()
    assert in_proc_status_porcelain(repo) == {"alpha.zz"}
    assert in_proc_status_porcelain_paths(repo, ["alpha.zz"]) == {"alpha.zz"}


def test_index_paths_sees_later_changes_and_a_deleted_index(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources._git_helpers import in_proc_index_paths

    repo = _repo_with(tmp_path / "repo", "alpha.zz", "beta.zz")
    asked = ["alpha.zz", "beta.zz", "delta.zz"]
    assert in_proc_index_paths(repo, asked) == {"alpha.zz", "beta.zz"}

    _git(repo, "rm", "-q", "beta.zz")
    (repo / "delta.zz").write_text("delta\n", encoding="utf-8")
    _git(repo, "add", "delta.zz")
    assert in_proc_index_paths(repo, asked) == {"alpha.zz", "delta.zz"}

    (repo / ".git" / "index").unlink()
    assert in_proc_index_paths(repo, asked) == set()
