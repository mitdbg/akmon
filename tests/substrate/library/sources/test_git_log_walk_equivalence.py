"""The streamed `git log` walks answer exactly as the per-commit libgit2 walks.

`in_proc_path_history` and `in_proc_walk_commits` read each commit's changed
files from one `git log --raw` subprocess instead of a libgit2 diff per
commit. These tests build a repository with every shape that could make the
two disagree -- merges (clean, one-sided, evil), an octopus merge, a second
root, renames exact and inexact, a copy, deletes and re-adds, mode and type
changes, a file replaced by a directory, submodule pointers, empty commits,
equal and out-of-order commit dates, and paths git would quote -- under a
repository configuration set to change `git log` output, and compare every
answer with the old implementation kept in `_git_walk_reference`.
"""

from __future__ import annotations

import os
import random
import subprocess
import sys
from pathlib import Path

import pygit2
import pytest

from okg.substrate.library.sources import _git_helpers
from okg.substrate.library.sources._git_helpers import (
    GitInvocationError,
    GitLogStreamError,
    _GitLogChanges,
    in_proc_ls_tree,
    in_proc_path_history,
    in_proc_walk_commits,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _git_walk_reference import (  # noqa: E402
    reference_path_history,
    reference_walk_commits,
)


class _Repo:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.clock = 1_700_000_000

    def git(self, *args: str | bytes, stdin: bytes | None = None, date: int | None = None) -> str:
        env = {
            **os.environ,
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "Test",
            "GIT_AUTHOR_EMAIL": "test@example.com",
            "GIT_COMMITTER_NAME": "Test",
            "GIT_COMMITTER_EMAIL": "test@example.com",
        }
        if date is None:
            self.clock += 60
            date = self.clock
        env["GIT_AUTHOR_DATE"] = f"@{date} +0200"
        env["GIT_COMMITTER_DATE"] = f"@{date} -0500"
        out = subprocess.run(
            ["git", "-C", str(self.root), *args],
            input=stdin,
            capture_output=True,
            check=True,
            env=env,
        )
        return out.stdout.decode("utf-8", errors="replace").strip()

    def write(self, path: str, text: str) -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

    def commit(self, message: str, *, date: int | None = None) -> str:
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "--no-verify", "-m", message, date=date)
        return self.git("rev-parse", "HEAD")

    def blob(self, data: bytes) -> str:
        return self.git("hash-object", "-w", "--stdin", stdin=data)

    def index_add(self, mode: str, sha: str, path: bytes) -> None:
        self.git(
            "update-index", "--add", "--cacheinfo", mode.encode() + b"," + sha.encode() + b"," + path
        )

    def index_commit(self, message: str, *, date: int | None = None) -> str:
        """Commit the index as is (paths the file system may not hold)."""
        self.git("commit", "-q", "--allow-empty", "--no-verify", "-m", message, date=date)
        return self.git("rev-parse", "HEAD")


_BODY = "".join(f"line {i} of a file long enough to score a rename\n" for i in range(40))


@pytest.fixture(scope="module")
def history(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, list[str]]:
    root = tmp_path_factory.mktemp("gitlog-equivalence")
    r = _Repo(root)
    r.git("init", "-q", "-b", "main")
    # Configuration that changes what a bare `git log` prints.
    order_file = root / ".git" / "order"
    order_file.write_text("z*\n*.md\n", encoding="utf-8")
    for key, value in (
        ("diff.renames", "copies"),
        ("diff.orderFile", str(order_file)),
        ("diff.relative", "true"),
        ("diff.ignoreSubmodules", "all"),
        ("diff.noprefix", "true"),
        ("log.showRoot", "false"),
        ("log.decorate", "full"),
        ("core.quotePath", "true"),
        ("i18n.logOutputEncoding", "UTF-16"),
    ):
        r.git("config", key, value)
    commits: list[str] = []

    r.write("a.txt", "a\n")
    r.write("b.txt", "b\n")
    r.write("dir/c.txt", _BODY)
    r.write("dir/exact.txt", _BODY + "exact\n")
    r.write("keep.txt", "never touched again\n")
    r.write("zeta.md", "z\n")
    r.write("sp ace\ttab \"q\".txt", "odd\n")
    r.write("ünï.txt", "unicode\n")
    commits.append(r.commit("root"))
    # A path that is not valid UTF-8 (plumbing: some file systems refuse it).
    r.index_add("100644", r.blob(b"latin-1\n"), b"caf\xe9.txt")
    r.index_add("100644", r.blob(b"newline\n"), b"new\nline.txt")
    commits.append(r.index_commit("odd paths"))

    r.write("a.txt", "a2\n")
    (root / "b.txt").chmod(0o755)
    r.write("zeta.md", "z2\n")
    commits.append(r.commit("modify and mode change"))
    r.git("mv", "dir/c.txt", "dir/c2.txt")
    r.write("dir/c2.txt", _BODY + "a small edit\n")
    (root / "moved").mkdir()
    r.git("mv", "dir/exact.txt", "moved/exact.txt")
    commits.append(r.commit("inexact and exact rename"))
    r.write("copy.txt", _BODY + "a small edit\n")
    r.write("dir/c2.txt", _BODY + "a second edit\n")
    commits.append(r.commit("copy of a modified file"))
    (root / "b.txt").unlink()
    commits.append(r.commit("delete b"))
    r.write("b.txt", "b is back\n")
    commits.append(r.commit("re-add b"))
    fork = commits[-1]

    # Two branches with equal commit dates, merged without fast-forward.
    tie = r.clock + 600
    r.git("checkout", "-q", "-b", "feature")
    r.write("f.txt", "feature\n")
    r.write("a.txt", "a from feature\n")
    commits.append(r.commit("feature work", date=tie))
    r.write("f.txt", "feature 2\n")
    commits.append(r.commit("feature work 2", date=tie))
    r.git("checkout", "-q", "main")
    r.write("m.txt", "main\n")
    commits.append(r.commit("main work", date=tie))
    r.git("merge", "-q", "--no-ff", "-X", "ours", "-m", "merge feature", "feature")
    commits.append(r.git("rev-parse", "HEAD"))

    # A merge that changes a file neither side changed (an evil merge).
    r.git("checkout", "-q", "-b", "side", fork)
    r.write("side.txt", "side\n")
    commits.append(r.commit("side work"))
    r.git("checkout", "-q", "main")
    r.git("merge", "-q", "--no-ff", "--no-commit", "side")
    r.write("keep2.txt", "added only in the merge\n")
    r.git("add", "-A")
    r.git("commit", "-q", "--no-verify", "-m", "evil merge")
    commits.append(r.git("rev-parse", "HEAD"))

    # An octopus merge of three branches.
    base = r.git("rev-parse", "HEAD")
    for name in ("o1", "o2", "o3"):
        r.git("checkout", "-q", "-b", name, base)
        r.write(f"{name}.txt", f"{name}\n")
        commits.append(r.commit(f"{name} work"))
    r.git("checkout", "-q", "main")
    r.git("merge", "-q", "--no-ff", "-m", "octopus", "o1", "o2", "o3")
    commits.append(r.git("rev-parse", "HEAD"))

    # A second root, merged in.
    r.git("checkout", "-q", "--orphan", "other")
    r.git("rm", "-q", "-rf", "--cached", ".")
    for leftover in list(root.iterdir()):
        if leftover.name != ".git":
            subprocess.run(["rm", "-rf", str(leftover)], check=True)
    r.write("other/root.txt", "second root\n")
    r.write("a.txt", "a from the other root\n")
    commits.append(r.commit("second root"))
    r.git("checkout", "-q", "-f", "main")
    r.git(
        "merge", "-q", "--no-ff", "--allow-unrelated-histories", "-X", "ours",
        "-m", "merge second root", "other",
    )
    commits.append(r.git("rev-parse", "HEAD"))

    # Type changes: file -> symlink, file -> directory, file -> submodule.
    (root / "f.txt").unlink()
    os.symlink("a.txt", root / "f.txt")
    commits.append(r.commit("f.txt becomes a symlink"))
    (root / "m.txt").unlink()
    r.write("m.txt/inner.txt", "a directory now\n")
    commits.append(r.commit("m.txt becomes a directory"))
    r.git("rm", "-q", "--cached", "side.txt")
    (root / "side.txt").unlink()
    r.index_add("160000", commits[0], b"side.txt")
    r.index_add("160000", commits[0], b"vendor/sub")
    commits.append(r.index_commit("submodule pointers"))
    r.index_add("160000", commits[1], b"vendor/sub")
    commits.append(r.index_commit("submodule pointer moves"))
    r.git("rm", "-q", "--cached", "vendor/sub")
    commits.append(r.index_commit("submodule removed"))

    commits.append(r.commit("empty commit"))
    # A commit dated before its parent (clock skew), then a normal one.
    r.write("a.txt", "skewed\n")
    commits.append(r.commit("skewed", date=1_600_000_000))
    r.write("zeta.md", "after skew\n")
    commits.append(r.commit("after skew"))
    # A replace ref that git would honour but libgit2 does not.
    r.git("replace", commits[3], commits[4])
    return root, commits


def _all_paths(root: Path) -> set[str]:
    repo = pygit2.Repository(str(root))
    paths: set[str] = set()
    for commit in repo.walk(repo.head.target):
        paths |= _git_helpers._walk_tree_paths(repo, commit.tree)
    return paths


def test_repository_covers_the_risky_shapes(history: tuple[Path, list[str]]) -> None:
    root, commits = history
    repo = pygit2.Repository(str(root))
    parent_counts = {len(repo[sha].parents) for sha in commits}
    assert {0, 1, 2, 4} <= parent_counts  # roots, commits, merges, octopus
    listing = subprocess.run(
        ["git", "--no-replace-objects", "-C", str(root), "log", "--raw", "-M", "-C", "--format=", "--diff-merges=first-parent"],
        capture_output=True, check=True, env={**os.environ, "GIT_CONFIG_GLOBAL": os.devnull},
    ).stdout
    for status in (b" R", b" C", b" T", b" D", b" A", b" M"):
        assert status in listing, status
    assert b"160000" in listing
    replaced = subprocess.run(
        ["git", "-C", str(root), "replace", "--list"], capture_output=True, check=True, text=True
    ).stdout.split()
    assert replaced == [commits[3]]


def test_path_history_matches_reference_from_every_head(
    history: tuple[Path, list[str]],
) -> None:
    root, commits = history
    targets = _all_paths(root) | {"never/touched.txt", "vendor/sub", "side.txt"}
    checked = 0
    for head in commits:
        expected = reference_path_history(root, targets, head_sha=head)
        assert in_proc_path_history(root, targets, head_sha=head) == expected, head
        checked += 1
    # Default head, and one-path targets.
    assert in_proc_path_history(root, targets) == reference_path_history(root, targets)
    for path in sorted(targets):
        assert in_proc_path_history(root, [path]) == reference_path_history(root, [path]), path
    # A few paths at once: one path-limited `git log` per path.
    ordered = sorted(targets)
    for i in range(0, len(ordered), 5):
        group = ordered[i : i + 5]
        assert in_proc_path_history(root, group) == reference_path_history(root, group), group
    assert checked == len(commits)
    assert "keep.txt" in in_proc_path_history(root, targets)
    assert "never/touched.txt" not in in_proc_path_history(root, targets)


def test_path_history_without_pathspec_matches_reference(
    history: tuple[Path, list[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Above the pathspec limit git diffs every path; the answers agree."""
    root, commits = history
    targets = _all_paths(root) | {"never/touched.txt", "vendor/sub", "side.txt"}
    monkeypatch.setattr(_git_helpers, "_PATHSPEC_PATH_LIMIT", 0)
    for head in commits:
        assert in_proc_path_history(root, targets, head_sha=head) == (
            reference_path_history(root, targets, head_sha=head)
        ), head
    for path in sorted(targets):
        assert in_proc_path_history(root, [path]) == reference_path_history(
            root, [path]
        ), path


@pytest.mark.parametrize("detect_renames", [False, True])
def test_walk_commits_matches_reference(
    history: tuple[Path, list[str]],
    detect_renames: bool,
) -> None:
    root, commits = history
    tips = [
        subprocess.run(
            ["git", "-C", str(root), "rev-parse", ref],
            capture_output=True, check=True, text=True,
        ).stdout.strip()
        for ref in ("main", "feature", "side", "o1", "other")
    ]
    ranges: list[tuple[list[str], list[str]]] = [
        ([tips[0]], []),
        (tips, []),
        ([tips[0]], [commits[5]]),
        ([tips[0]], [commits[9], commits[12]]),
        ([tips[0], tips[1]], [tips[4]]),
        ([tips[0]], ["0" * 40]),  # an unknown negative is skipped by both
    ]
    ranges += [([tips[0]], [sha]) for sha in commits[::3]]
    patch_options: list[dict[str, object]] = [
        {},
        {"with_patches": True},
        {"with_patches": True, "patches_since": 1_700_000_600},
        {"with_patches": True, "max_patch_blob_bytes": 100},
    ]
    compared = 0
    for positive, negative in ranges:
        for options in patch_options:
            expected = list(
                reference_walk_commits(
                    root, positive, negative, detect_renames=detect_renames, **options
                )
            )
            actual = list(
                in_proc_walk_commits(
                    root, positive, negative, detect_renames=detect_renames, **options
                )
            )
            assert actual == expected, (positive, negative, options)
            compared += len(expected)
    assert compared > 200


def test_walk_commits_renames_come_from_libgit2(history: tuple[Path, list[str]]) -> None:
    root, commits = history
    records = {
        rec.sha: rec.name_status
        for rec in in_proc_walk_commits(root, [commits[-1]], detect_renames=True)
    }
    assert ("R", "dir/c2.txt", "dir/c.txt") in records[commits[3]]
    assert ("R", "moved/exact.txt", "dir/exact.txt") in records[commits[3]]
    plain = {rec.sha: rec.name_status for rec in in_proc_walk_commits(root, [commits[-1]])}
    assert ("D", "dir/c.txt", None) in plain[commits[3]]
    assert ("A", "dir/c2.txt", None) in plain[commits[3]]


def test_stream_refuses_a_commit_out_of_order(history: tuple[Path, list[str]]) -> None:
    root, commits = history
    with pytest.raises(GitLogStreamError, match="where the libgit2 walk has"):
        with _GitLogChanges(root, [commits[3], commits[4]]) as changes:
            changes.for_commit(commits[4])
    with pytest.raises(GitLogStreamError, match="ended before commit"):
        with _GitLogChanges(root, [commits[3]]) as changes:
            changes.for_commit(commits[3])
            changes.for_commit(commits[4])


def test_caller_exception_is_not_masked(history: tuple[Path, list[str]]) -> None:
    root, commits = history
    walk = in_proc_walk_commits(root, [commits[-1]])
    next(walk)
    with pytest.raises(ValueError, match="consumer failed"):
        walk.throw(ValueError("consumer failed"))


def test_repo_root_below_the_top_level(history: tuple[Path, list[str]]) -> None:
    """Old and new both refuse a ``repo_root`` below the top level.

    ``diff.relative=true`` would make git print paths relative to such a
    root. Both walks open the repository rooted exactly at ``repo_root``
    with no parent search (``open_repository_at``), so a subdirectory is
    the same failed open for both, never paths from the enclosing
    repository and never a git subprocess.
    """
    root, commits = history
    sub = root / "dir"
    assert sub.is_dir()
    targets = _all_paths(root)
    calls = [
        (in_proc_path_history, reference_path_history, (sub, targets)),
        (in_proc_walk_commits, reference_walk_commits, (sub, [commits[-1]])),
    ]
    for new_fn, old_fn, args in calls:
        with pytest.raises(pygit2.GitError) as new_exc:
            list(new_fn(*args))
        with pytest.raises(pygit2.GitError) as old_exc:
            list(old_fn(*args))
        assert type(new_exc.value) is type(old_exc.value)
        assert str(new_exc.value) == str(old_exc.value)


def _random_history(root: Path, seed: int) -> list[str]:
    """60 commits, some merges, about 30% with dates far before their parents."""
    rnd = random.Random(seed)
    repo = pygit2.Repository(pygit2.init_repository(str(root), bare=True).path)
    shas: list[pygit2.Oid] = []
    for i in range(60):
        builder = repo.TreeBuilder()
        builder.insert(f"f{i % 7}", repo.create_blob(f"{i}\n".encode()), pygit2.GIT_FILEMODE_BLOB)
        builder.insert(
            f"g{rnd.randrange(5)}", repo.create_blob(f"x{i}\n".encode()), pygit2.GIT_FILEMODE_BLOB
        )
        tree = builder.write()
        count = 0 if not shas else (1 if rnd.random() < 0.75 or len(shas) < 2 else 2)
        parents = rnd.sample(shas[-10:], min(count, len(shas[-10:]))) if shas else []
        when = 1_700_000_000 + (i * 100 if rnd.random() > 0.3 else -rnd.randrange(10**7))
        sig = pygit2.Signature("t", "t@example.com", when, 0)
        shas.append(repo.create_commit(None, sig, sig, f"c{i}", tree, parents))
    repo.references.create("refs/heads/main", shas[-1])
    repo.set_head("refs/heads/main")
    return [str(sha) for sha in shas]


def test_skewed_dates_with_excluded_commits(tmp_path: Path) -> None:
    """With commit dates out of order, git's own `A --not B` range can differ
    from libgit2's hide; the stream must follow libgit2's commit list."""
    paths = {f"f{i}" for i in range(7)} | {f"g{i}" for i in range(5)}
    ranges = 0
    for seed in range(24):
        root = tmp_path / f"skew-{seed}"
        shas = _random_history(root, seed)
        rnd = random.Random(1000 + seed)
        for _ in range(8):
            positive = rnd.sample(shas, 2)
            negative = rnd.sample(shas, rnd.randrange(1, 4))
            assert list(in_proc_walk_commits(root, positive, negative)) == list(
                reference_walk_commits(root, positive, negative)
            ), (seed, positive, negative)
            ranges += 1
        for head in shas[::5]:
            assert in_proc_path_history(root, paths, head_sha=head) == (
                reference_path_history(root, paths, head_sha=head)
            ), (seed, head)
    assert ranges == 192


def test_stream_raises_git_failure(
    history: tuple[Path, list[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, _commits = history
    monkeypatch.setattr(
        _git_helpers,
        "_GIT_LOG_CHANGE_ARGS",
        (*_git_helpers._GIT_LOG_CHANGE_ARGS, "--no-such-option"),
    )
    with pytest.raises(GitInvocationError, match="no-such-option"):
        in_proc_path_history(root, ["a.txt"])


def test_ls_tree_sizes_come_from_object_headers(history: tuple[Path, list[str]]) -> None:
    root, _commits = history
    repo = pygit2.Repository(str(root))
    tree = repo.head.peel(pygit2.Tree)
    paths = sorted(_git_helpers._walk_tree_paths(repo, tree))
    listed = in_proc_ls_tree(root, [*paths, "gone.txt"])
    assert sorted(listed) == paths
    assert listed
    for path, (sha, size) in listed.items():
        assert sha == str(tree[path].id)
        assert size == repo[tree[path].id].size
    # A directory path fails as it did when the whole object was read.
    with pytest.raises(AttributeError):
        in_proc_ls_tree(root, ["dir"])
