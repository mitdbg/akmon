"""Shared git helpers for the git_graph module.

Two callable surfaces:

  1. ``in_proc_*`` — in-process via pygit2 (libgit2 bindings). USE
     THESE on the per-commit hot path of :class:`GitFilesSource` and
     :class:`GitHistorySource`. Each call is sub-millisecond after
     a one-time ~5-15 ms cold-cache open. Per
     `add-pygit2-in-process-git-helpers` (2026-05-12).

  2. ``run_git`` / ``run_git_bytes`` — subprocess shell-outs. Still
     used by bench orchestration (`git clone`, `git reset --hard`,
     `git fetch`) and by any rare read not yet covered by an
     ``in_proc_*`` helper. Subprocess git's fork+exec floor is
     ~11-17 ms per call on macOS; do NOT use these on the hot
     path.

Centralizes the ``git -C <root> ...`` invocation pattern so both
:class:`GitFilesSource` and :class:`GitHistorySource` use a single
subprocess shape. All subprocess callers pass structured argument
lists; no shell quoting.
"""

from __future__ import annotations

import logging
import fnmatch
import os
import re
import shutil
import subprocess  # nosec B404
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, NamedTuple

import pygit2

logger = logging.getLogger(__name__)

# pygit2 1.20 removed the legacy module-level
# ``GIT_REPOSITORY_OPEN_NO_SEARCH`` alias. The enum is the public surface in
# both the minimum supported 1.19.3 release and 1.20+, so keep repository
# boundary detection on the version-stable API.
_REPOSITORY_OPEN_NO_SEARCH = pygit2.enums.RepositoryOpenFlag.NO_SEARCH


# ---------------------------------------------------------------------------
# In-process Repository cache (per-thread)
# ---------------------------------------------------------------------------

# `pygit2.Repository` wraps a libgit2 handle that is NOT thread-safe:
# concurrent calls on one handle can serialize on internal mutexes
# AND/OR corrupt internal state (libgit2's documented behavior).
#
# The substrate's ingest service runs sources in a
# `ThreadPoolExecutor` within each dependency layer, so multiple
# threads concurrently hit `in_proc_*` helpers. The prior shape was
# `@functools.lru_cache(maxsize=8)` over the path string, returning
# ONE Repository per path shared across threads — the call sites
# noted "single-threaded so sharing is safe" but that has been false
# since per-layer parallel source-sync landed.
#
# Symptom: on the palimpzest-tier1 full-history bench (~75-minute
# worker run, 895 commits, 194K live nodes by the end), per-source
# `elapsed_s` for `git-files` reached 9.5 s. The same workflow run
# in a fresh probe Python process completed `git-files` in 387 ms.
# Worker `elapsed_s` is wall-clock around `SourceRunner.run()`;
# 25× inflation versus an isolated probe is consistent with libgit2
# serializing concurrent reads on the shared handle plus repeated
# stalls behind whichever thread happened to hit the slow path next.
#
# Fix: each thread gets its own Repository, opened on first use in
# that thread, cached in `threading.local()`. The handles are cheap
# (~5-15 ms to open cold). Subsequent calls in the same thread reuse
# the cached handle. Threads don't share handles, so there's no
# contention between sources running in parallel.
_repo_tls = threading.local()


class RepositoryRootMismatch(pygit2.GitError):
    """The repository at a path is not rooted at that path."""


def open_repository_at(repo_root: Path) -> pygit2.Repository:
    """Open the repository rooted exactly at ``repo_root``.

    libgit2 opens without searching parent directories, and the opened
    working tree (or, for a bare repository, its git directory) must be
    ``repo_root`` itself. A repository nested inside another whose own
    ``.git`` is damaged -- HEAD deleted, ``.git`` emptied, ``objects``
    removed -- is therefore a failed open, never the parent repository
    answered under the child's name. Raises ``pygit2.GitError`` (or
    :class:`RepositoryRootMismatch`, a subclass) when no repository is
    rooted there.
    """
    root = Path(repo_root).resolve()
    handle = pygit2.Repository(str(root), flags=_REPOSITORY_OPEN_NO_SEARCH)
    opened_at = Path(handle.workdir or handle.path).resolve()
    # Compare the directories themselves, not their spellings: on a
    # case-insensitive filesystem `resolve()` keeps the case the caller
    # typed while libgit2 reports the on-disk case, so `.../repo` and
    # `.../Repo` are one directory. A really different directory (a parent
    # found by a search) still differs here.
    if not os.path.samefile(opened_at, root):
        raise RepositoryRootMismatch(
            f"repository at {root} is rooted at {opened_at}, not at {root}"
        )
    return handle


def _r(repo_root: Path) -> pygit2.Repository:
    """Per-thread cached ``pygit2.Repository`` for ``repo_root``.

    Each thread caches its own handles keyed by absolute path. Safe
    under concurrent source-sync (multiple sources in a layer all
    hitting in_proc_* helpers in parallel) because libgit2 is not
    safe to share across threads.

    The handle is opened by :func:`open_repository_at`: no parent search,
    and the repository must be rooted at ``repo_root``. A damaged child
    repository inside a parent that has commits used to be read as the
    parent, publishing the parent's files and commits under the child's
    name.
    """
    cache: dict[str, pygit2.Repository] = getattr(_repo_tls, "cache", None)
    if cache is None:
        cache = {}
        _repo_tls.cache = cache
    key = str(Path(repo_root).resolve())
    handle = cache.get(key)
    if handle is None:
        handle = open_repository_at(Path(key))
        cache[key] = handle
    return handle


_GITLINK_FILEMODE = int(getattr(pygit2, "GIT_FILEMODE_COMMIT", 0o160000))


def _entry_filemode(entry: object) -> int:
    """Return the libgit2 file mode for tree and index entries."""
    mode = getattr(entry, "filemode", None)
    if mode is None:
        mode = getattr(entry, "mode", 0)
    try:
        return int(mode)
    except (TypeError, ValueError):
        return 0


def _is_gitlink_entry(entry: object) -> bool:
    """True for git submodule entries.

    A gitlink stores the submodule HEAD commit in the parent tree, but
    that commit lives in the submodule object database. Dereferencing it
    through the parent repository raises KeyError in pygit2.
    """
    return _entry_filemode(entry) == _GITLINK_FILEMODE


# ---------------------------------------------------------------------------
# In-process git helpers (preferred — bypass fork+exec)
# ---------------------------------------------------------------------------


def in_proc_head_tree_sha(repo_root: Path) -> str:
    """``git rev-parse HEAD^{tree}`` in-process."""
    return in_proc_head_snapshot(repo_root).tree_sha


def in_proc_head_sha(repo_root: Path) -> str:
    """``git rev-parse HEAD`` in-process."""
    return in_proc_head_snapshot(repo_root).head_sha


def in_proc_head_branch(repo_root: Path) -> str:
    """``git rev-parse --abbrev-ref HEAD`` in-process.

    Returns the branch shorthand (e.g. ``"master"``) when HEAD is
    on a branch, or ``"HEAD"`` when detached.
    """
    return in_proc_head_snapshot(repo_root).branch


class HeadSnapshot(NamedTuple):
    head_sha: str
    tree_sha: str
    branch: str


def in_proc_head_snapshot(repo_root: Path) -> HeadSnapshot:
    """Capture HEAD commit, tree, and branch from one resolved ref target."""
    repo = _r(repo_root)
    head = repo.head
    resolved = head.resolve()
    commit = repo[resolved.target].peel(pygit2.Commit)
    return HeadSnapshot(
        head_sha=str(commit.id),
        tree_sha=str(commit.tree.id),
        branch=("HEAD" if repo.head_is_detached else head.shorthand),
    )


def in_proc_ref_tips(
    repo_root: Path,
    *,
    include_remotes: bool = False,
) -> dict[str, str]:
    """Map each ref to its resolved target sha (issue #323 change-probe).

    Covers local branches (``refs/heads/*``), tags (``refs/tags/*``), optionally
    remote-tracking refs (``refs/remotes/*``), plus the current HEAD target when
    it is detached. This is the sound change-identity for a
    history walk: the set of reachable commits is a function of the ref tips, so
    any branch/tag move, create, or delete changes the digest. Stash/notes refs
    are excluded (they do not feed the history projection).
    """
    repo = _r(repo_root)
    prefixes = ["refs/heads/", "refs/tags/"]
    if include_remotes:
        prefixes.append("refs/remotes/")
    out: dict[str, str] = {}
    for name in repo.references:
        if not any(name.startswith(p) for p in prefixes):
            continue
        try:
            target = repo.references[name].resolve().target
        except (KeyError, ValueError, pygit2.GitError):
            continue
        out[name] = str(target)
    # An attached HEAD is already represented by its refs/heads entry. Keeping
    # a synthetic HEAD entry in that case would make a checkout between two
    # already-enumerated branches look like a history change. A detached HEAD,
    # however, can be the only ref-like root that makes a commit reachable.
    if repo.head_is_detached:
        try:
            out["HEAD"] = str(repo.head.peel(pygit2.Commit).id)
        except (pygit2.GitError, KeyError, TypeError, ValueError):
            pass
    return out


def in_proc_workdir(path: Path) -> Path | None:
    """``git rev-parse --show-toplevel`` for an arbitrary path inside a repo.

    Returns the work-tree root containing ``path`` (so a source rooted at a
    subdirectory can resolve the git toplevel its scope probe must run at), or
    None when ``path`` is not inside a git work-tree.
    """
    discovered = pygit2.discover_repository(str(Path(path).resolve()))
    if discovered is None:
        return None
    workdir = pygit2.Repository(discovered).workdir
    return Path(workdir).resolve() if workdir else None


def in_proc_status_porcelain(repo_root: Path) -> set[str]:
    """``git status --porcelain -z`` in-process.

    Returns the set of paths in any non-clean state (modified,
    added, untracked, ignored, etc.). Equivalent to what
    `parse_porcelain_z` extracts from the subprocess output.
    """
    repo = _r(repo_root)
    # libgit2's status refresh keeps stale entries when the index file is
    # gone; read it the way git does first (issue #3291).
    _current_index(repo_root)
    return {
        path
        for path, flags in repo.status().items()
        if flags & ~pygit2.GIT_STATUS_CURRENT
    }


def in_proc_status_porcelain_paths(
    repo_root: Path,
    paths: Iterable[str],
) -> set[str]:
    """Path-scoped ``git status --porcelain`` in-process."""
    repo = _r(repo_root)
    _current_index(repo_root)
    dirty: set[str] = set()
    for raw_path in paths:
        path = str(raw_path).replace("\\", "/").lstrip("/")
        if not path:
            continue
        try:
            flags = repo.status_file(path)
        except (KeyError, ValueError, pygit2.GitError):
            continue
        if flags & ~pygit2.GIT_STATUS_CURRENT:
            dirty.add(path)
    return dirty


def _current_index(repo_root: Path) -> pygit2.Index:
    """The cached handle's index, re-read if the index file changed on disk.

    libgit2 loads a repository's index once and keeps it with the handle,
    and :func:`_r` keeps the handle for the life of the thread. A ``git add``
    or ``git rm`` run by a subprocess afterwards would stay invisible to
    every later read (issue #3291). ``read(False)`` reloads only when the
    file's timestamp, size or checksum changed, so an unchanged index costs
    a stat and a short checksum read. It keeps the loaded entries when the
    file is gone, which git reads as an empty index, so a missing file is a
    forced (clearing) read. Every in-process index reader goes through here.
    """
    repo = _r(repo_root)
    index = repo.index
    if (Path(repo.path) / "index").exists():
        index.read(False)
    else:
        index.read(True)
    return index


def in_proc_list_tracked(repo_root: Path) -> set[str]:
    """``git ls-files`` in-process, excluding gitlink entries."""
    index = _current_index(repo_root)
    return {entry.path for entry in index if not _is_gitlink_entry(entry)}


def path_included_by_globs(
    rel_path: str,
    *,
    include_globs: Iterable[str] | None = None,
    exclude_globs: Iterable[str] = (),
) -> bool:
    """Return true when a repo-relative path passes include/exclude globs."""
    rel = str(rel_path).replace("\\", "/").lstrip("/")
    includes = tuple(include_globs or ())
    if includes and not any(fnmatch.fnmatch(rel, pattern) for pattern in includes):
        return False
    return not any(fnmatch.fnmatch(rel, pattern) for pattern in exclude_globs)


def worktree_dir_excluded_by_globs(
    rel_path: str,
    *,
    include_globs: Iterable[str] | None = None,
    exclude_globs: Iterable[str] = (),
) -> bool:
    """Return true when a worktree directory should be pruned before descent."""
    rel = str(rel_path).replace("\\", "/").strip("/")
    if not rel:
        return False
    if rel == ".git" or rel.startswith(".git/"):
        return True
    if rel == ".claude" or rel.startswith(".claude/"):
        return True
    return not path_included_by_globs(
        f"{rel}/.okg-prune-probe",
        include_globs=include_globs,
        exclude_globs=exclude_globs,
    )


def git_ignored_paths(repo_root: Path, paths: set[str]) -> set[str]:
    """Return the subset of ``paths`` ignored by gitignore/exclude rules."""
    if not paths:
        return set()
    args = ["check-ignore", "--stdin", "-z"]
    # check-ignore reads each name as a pathspec, so a path starting with `:`
    # (`:x/a`, `:(y/a`) would be pathspec magic -- matching nothing or
    # exiting 128 -- and it refuses `--literal-pathspecs` outright. A leading
    # `./` names the same path without magic; git echoes the name as given,
    # so map it back.
    spelled = {
        (f"./{path}" if path.startswith(":") else path): path for path in paths
    }
    payload = "\0".join(sorted(spelled)).encode("utf-8") + b"\0"
    returncode, stdout, stderr = run_git_stdin(
        repo_root,
        args,
        stdin=payload,
        check=False,
    )
    if returncode not in (0, 1):
        raise GitInvocationError(args, returncode, stderr)
    return {spelled.get(path, path) for path in stdout.split("\0") if path}


def git_untracked_worktree_paths(
    repo_root: Path,
    *,
    include_globs: Iterable[str] | None = None,
    exclude_globs: Iterable[str] = (),
    path_filter: Callable[[str], bool] | None = None,
    pathspecs: Iterable[str] = (),
) -> set[str]:
    """Return untracked, non-ignored files using git's exclude machinery.

    The change probe needs a content-sound untracked set but must not walk
    ignored repo-local caches such as `.venv/` or `.scratch/` in Python on every
    no-op. `git ls-files --others --exclude-standard` uses Git's index and
    ignore engine for that filtering, while the post-filter below preserves the
    source-specific `.claude/` pruning contract from the Python walker.
    """
    args = ["ls-files", "--others", "--exclude-standard", "-z"]
    clean_pathspecs = tuple(
        str(path).replace("\\", "/").strip("/")
        for path in pathspecs
        if str(path).strip("/")
    )
    if clean_pathspecs:
        args.extend(["--", *clean_pathspecs])
    out = run_git(repo_root, args)
    paths: set[str] = set()
    for raw in out.split("\0"):
        rel = raw.replace("\\", "/").lstrip("/")
        if not rel:
            continue
        # Git reports directory placeholders and broken symlinks that the
        # source cannot emit as file facts.  The filesystem-backed reader
        # applies the same emit-capable-file boundary.
        if not (repo_root / rel).is_file():
            continue
        if _path_has_pruned_parent(
            rel,
            include_globs=include_globs,
            exclude_globs=exclude_globs,
        ):
            continue
        if not path_included_by_globs(
            rel,
            include_globs=include_globs,
            exclude_globs=exclude_globs,
        ):
            continue
        if path_filter is not None and not path_filter(rel):
            continue
        paths.add(rel)
    return paths


def _path_has_pruned_parent(
    rel_path: str,
    *,
    include_globs: Iterable[str] | None = None,
    exclude_globs: Iterable[str] = (),
) -> bool:
    parts = Path(rel_path).parts[:-1]
    for index in range(1, len(parts) + 1):
        parent = "/".join(parts[:index])
        if worktree_dir_excluded_by_globs(
            parent,
            include_globs=include_globs,
            exclude_globs=exclude_globs,
        ):
            return True
    return False


def pruned_untracked_worktree_paths(
    repo_root: Path,
    *,
    tracked_paths: set[str],
    include_globs: Iterable[str] | None = None,
    exclude_globs: Iterable[str] = (),
    walk_roots: Iterable[Path] | None = None,
    path_filter: Callable[[str], bool] | None = None,
) -> set[str]:
    """Return untracked files without asking git/libgit2 to scan excluded trees."""
    candidates: set[str] = set()
    parents = _ParentDirectories(Path(repo_root))
    roots = tuple(walk_roots or (repo_root,))
    for walk_root in roots:
        if not walk_root.is_dir():
            continue
        for dirpath, dirnames, filenames in os.walk(walk_root):
            try:
                rel_dir = Path(dirpath).relative_to(repo_root).as_posix()
            except ValueError:
                continue
            if rel_dir == ".":
                rel_dir = ""
            if rel_dir and (
                parents.is_gitlink(rel_dir)
                or _is_nested_git_repository(Path(dirpath))
            ):
                # Git treats embedded repositories and linked worktrees as an
                # atomic untracked directory in the outer repository, and a
                # submodule as its gitlink alone -- also when it is not
                # checked out, where no `.git` marks it and `git check-ignore`
                # would refuse a path inside it with exit 128. No nested file
                # is part of this source's outer worktree scope.
                dirnames[:] = []
                continue
            dirnames[:] = [
                dirname
                for dirname in dirnames
                if dirname != ".git"
                if not worktree_dir_excluded_by_globs(
                    f"{rel_dir}/{dirname}" if rel_dir else dirname,
                    include_globs=include_globs,
                    exclude_globs=exclude_globs,
                )
            ]
            for filename in filenames:
                rel_path = f"{rel_dir}/{filename}" if rel_dir else filename
                # A linked worktree stores its repository control pointer as a
                # root-level ``.git`` file rather than a directory.  Git never
                # reports that control file as untracked source content, so the
                # pruned filesystem walk must preserve the same boundary.
                if Path(rel_path).name == ".git":
                    continue
                if rel_path in tracked_paths:
                    continue
                if not (repo_root / rel_path).is_file():
                    continue
                if not path_included_by_globs(
                    rel_path,
                    include_globs=include_globs,
                    exclude_globs=exclude_globs,
                ):
                    continue
                if path_filter is not None and not path_filter(rel_path):
                    continue
                candidates.add(rel_path)
    return candidates - git_ignored_paths(repo_root, candidates)


def scoped_untracked_worktree_paths(
    repo_root: Path,
    paths: Iterable[str],
    *,
    tracked_paths: set[str],
    include_globs: Iterable[str] | None = None,
    exclude_globs: Iterable[str] = (),
) -> set[str]:
    """The subset of ``paths`` that `pruned_untracked_worktree_paths` selects.

    A scoped run resolves only watcher-named paths, but it must admit exactly
    the untracked files the full pruned walk would (issue #3259): no path under
    a directory the walk prunes (``.git``, ``.claude``, a glob-excluded
    directory), under a nested repository, linked worktree or submodule
    (checked out or not), or under a symlinked directory (``os.walk`` does
    not descend into one); no ``.git``
    control file; only files passing the include and exclude globs; and
    nothing git ignores.
    """
    root = Path(repo_root)
    includes = tuple(include_globs or ())
    excludes = tuple(exclude_globs)
    parents = _ParentDirectories(root)
    candidates: set[str] = set()
    for raw in paths:
        rel = str(raw).replace("\\", "/").strip("/")
        if not rel or rel in tracked_paths:
            continue
        parts = rel.split("/")
        if ".git" in parts or any(part in ("", ".", "..") for part in parts):
            continue
        if not path_included_by_globs(
            rel, include_globs=includes, exclude_globs=excludes,
        ):
            continue
        if _path_has_pruned_parent(
            rel, include_globs=includes, exclude_globs=excludes,
        ):
            continue
        if parents.blocked_for_walk(rel):
            continue
        if not (root / rel).is_file():
            continue
        candidates.add(rel)
    return candidates - git_ignored_paths(root, candidates)


def in_proc_index_paths(repo_root: Path, paths: Iterable[str]) -> set[str]:
    """The subset of ``paths`` the index tracks, at any stage.

    A conflicted path (stages 1-3 during a merge or rebase) is tracked, and
    so is a gitlink: `git diff --name-only`, which the full crawl's dirty
    set reads, lists both. The cached handle's index is re-read when the
    file changed on disk, so the answer never depends on an earlier call
    having refreshed it.
    """
    index = _current_index(repo_root)
    conflicts = index.conflicts
    out: set[str] = set()
    for raw in paths:
        path = str(raw).replace("\\", "/").lstrip("/")
        if not path:
            continue
        try:
            index[path]
        except KeyError:
            if conflicts is None:
                continue
            try:
                conflicts[path]
            except KeyError:
                continue
        out.add(path)
    return out


def in_proc_git_visible_paths(repo_root: Path, paths: Iterable[str]) -> set[str]:
    """The subset of ``paths`` a git-visible code snapshot reads.

    Mirrors `_GitVisibleCodeSnapshotProbe`: a blob in HEAD's tree is read,
    except a clean tracked symlink (mode 120000 with no working-tree change);
    a path outside HEAD is read only when it is untracked (absent from the
    index at every stage) and not gitignored. A staged-but-uncommitted file
    is neither, so the snapshot does not read it. Raises when HEAD cannot be
    read; the caller decides what an unborn repository means.
    """
    repo = _r(repo_root)
    tree = repo.head.peel(pygit2.Tree)
    requested = {
        str(raw).replace("\\", "/").lstrip("/") for raw in paths if raw
    }
    requested.discard("")
    visible: set[str] = set()
    outside_head: set[str] = set()
    for path in requested:
        try:
            entry = tree[path]
        except KeyError:
            outside_head.add(path)
            continue
        if _is_gitlink_entry(entry) or entry.type_str != "blob":
            continue
        if _entry_filemode(entry) == int(pygit2.enums.FileMode.LINK):
            try:
                flags = repo.status_file(path)
            except (KeyError, ValueError, pygit2.GitError):
                continue
            if not flags & ~pygit2.GIT_STATUS_CURRENT:
                continue
        visible.add(path)
    # A path git does not list as untracked -- inside a submodule, beyond a
    # symlink, or inside a nested repository the outer index tracks nothing
    # under -- is never part of the outer snapshot, and `git check-ignore`
    # refuses some of them with exit 128. Drop them first.
    parents = _ParentDirectories(Path(repo_root))
    outside_head = {
        path for path in outside_head if not parents.blocked_for_git(path)
    }
    untracked = outside_head - in_proc_index_paths(repo_root, outside_head)
    untracked = {path for path in untracked if (Path(repo_root) / path).is_file()}
    return visible | (untracked - git_ignored_paths(Path(repo_root), untracked))


class _ParentDirectories:
    """Per-call, memoized answers about the parent directories of keys.

    Scoped runs check every parent of every key, and thousands of keys share
    parents, so each question is asked once per directory per call.
    """

    def __init__(self, repo_root: Path) -> None:
        self.root = Path(repo_root)
        self._index: Any = None
        self._symlink: dict[str, bool] = {}
        self._nested: dict[str, bool] = {}
        self._gitlink: dict[str, bool] = {}
        self._tracked_under: dict[str, bool] = {}

    def _index_entries(self) -> Any:
        if self._index is None:
            self._index = _current_index(self.root)
        return self._index

    def is_symlink(self, prefix: str) -> bool:
        if prefix not in self._symlink:
            self._symlink[prefix] = (self.root / prefix).is_symlink()
        return self._symlink[prefix]

    def is_nested_repository(self, prefix: str) -> bool:
        if prefix not in self._nested:
            self._nested[prefix] = _is_nested_git_repository(self.root / prefix)
        return self._nested[prefix]

    def is_gitlink(self, prefix: str) -> bool:
        if prefix not in self._gitlink:
            try:
                entry = self._index_entries()[prefix]
            except KeyError:
                entry = None
            self._gitlink[prefix] = (
                entry is not None and _is_gitlink_entry(entry)
            )
        return self._gitlink[prefix]

    def has_tracked_under(self, prefix: str) -> bool:
        """Whether the outer index tracks any path below ``prefix/``."""
        if prefix not in self._tracked_under:
            # Literal: a directory named `:x` or `:(y` is a path, not
            # pathspec magic (which would match nothing or exit 128).
            listed = run_git(
                self.root,
                ["--literal-pathspecs", "ls-files", "-z", "--", f"{prefix}/"],
            )
            self._tracked_under[prefix] = bool(listed.strip("\0"))
        return self._tracked_under[prefix]

    def blocked_for_walk(self, rel_path: str) -> bool:
        """The git-files pruned walk's rule: it never descends a symlinked
        directory, a nested repository or linked worktree, or a gitlink
        (a submodule, checked out or not)."""
        parts = rel_path.split("/")
        for depth in range(1, len(parts)):
            prefix = "/".join(parts[:depth])
            if (
                self.is_symlink(prefix)
                or self.is_gitlink(prefix)
                or self.is_nested_repository(prefix)
            ):
                return True
        return False

    def blocked_for_git(self, rel_path: str) -> bool:
        """Git's own untracked rule, which the git-visible snapshot follows:
        it never lists a path below a symlinked directory or a gitlink, and
        treats a directory holding its own repository as one opaque entry --
        unless the outer index already tracks paths below it, in which case
        git lists that directory's untracked files like any other."""
        parts = rel_path.split("/")
        for depth in range(1, len(parts)):
            prefix = "/".join(parts[:depth])
            if self.is_symlink(prefix) or self.is_gitlink(prefix):
                return True
            if self.is_nested_repository(prefix) and not self.has_tracked_under(
                prefix,
            ):
                return True
        return False


def _is_nested_git_repository(directory: Path) -> bool:
    """Return whether ``directory`` owns a valid nested Git worktree."""
    control = directory / ".git"
    try:
        if control.is_dir():
            pygit2.Repository(
                str(control),
                flags=_REPOSITORY_OPEN_NO_SEARCH,
            )
        elif control.is_file():
            raw = control.read_text(encoding="utf-8").strip()
            if not raw.startswith("gitdir: "):
                return False
            gitdir = Path(raw.removeprefix("gitdir: ").strip())
            if not gitdir.is_absolute():
                gitdir = directory / gitdir
            pygit2.Repository(
                str(gitdir.resolve()),
                flags=_REPOSITORY_OPEN_NO_SEARCH,
            )
        else:
            return False
    except (OSError, UnicodeError, ValueError, pygit2.GitError):
        return False
    # The outer Git repository treats a syntactically valid control pointer as
    # an atomic nested repository even when the target records a different
    # worktree location (for example a copied or stale linked worktree).
    return True


def tracked_dirty_worktree_paths(
    repo_root: Path,
    *,
    include_globs: Iterable[str] | None = None,
    exclude_globs: Iterable[str] = (),
    path_filter: Callable[[str], bool] | None = None,
) -> set[str]:
    """Return modified/staged tracked paths using git's cheap diff views."""
    dirty: set[str] = set()
    for args in (
        ["diff", "--name-only"],
        ["diff", "--name-only", "--cached"],
    ):
        out = run_git(repo_root, args)
        for line in out.splitlines():
            if not line:
                continue
            if not path_included_by_globs(
                line,
                include_globs=include_globs,
                exclude_globs=exclude_globs,
            ):
                continue
            if path_filter is not None and not path_filter(line):
                continue
            dirty.add(line)
    return dirty


def in_proc_diff_name_status(
    repo_root: Path,
    prior_tree_sha: str,
    cur_tree_sha: str,
) -> tuple[set[str], set[str]]:
    """``git diff --name-status -z prior cur`` in-process.

    Returns ``(added_or_modified, deleted)``. Renames + copies
    are split into delete(old) + add(new) — same as the subprocess
    parser does. Type-changes count as modified, and a path that is a
    file or a symlink in ``cur`` is never in the deleted set.
    """
    repo = _r(repo_root)
    prior = repo[prior_tree_sha].peel(pygit2.Tree)
    cur = repo[cur_tree_sha].peel(pygit2.Tree)
    diff = prior.diff_to_tree(cur)
    diff.find_similar()  # detect renames+copies the same way `git diff` does
    added: set[str] = set()
    deleted: set[str] = set()
    DELETED = pygit2.GIT_DELTA_DELETED
    ADDED = pygit2.GIT_DELTA_ADDED
    MODIFIED = pygit2.GIT_DELTA_MODIFIED
    TYPECHANGE = pygit2.GIT_DELTA_TYPECHANGE
    RENAMED = pygit2.GIT_DELTA_RENAMED
    COPIED = pygit2.GIT_DELTA_COPIED
    # Paths that are a file or a symlink in `cur` (not a submodule entry).
    current_files: set[str] = set()
    for delta in diff.deltas:
        s = delta.status
        if s == DELETED:
            deleted.add(delta.old_file.path)
        elif s in (ADDED, MODIFIED, TYPECHANGE):
            added.add(delta.new_file.path)
        elif s in (RENAMED, COPIED):
            deleted.add(delta.old_file.path)
            added.add(delta.new_file.path)
        if s != DELETED and delta.new_file.mode != pygit2.GIT_FILEMODE_COMMIT:
            current_files.add(delta.new_file.path)
    # libgit2's default diff splits a type change (file <-> symlink) into a
    # delete and an add of one path, and rename detection may pair either
    # half with another file. A path that is still a file or a symlink in
    # `cur` was not deleted: it counts once, as added or modified (issue
    # #3666). A file that became a submodule entry is still deleted: the
    # files source keeps no record for a submodule.
    return added, deleted - current_files


def in_proc_ls_tree(
    repo_root: Path,
    paths: Iterable[str],
    *,
    tree_sha: str | None = None,
) -> dict[str, tuple[str, int]]:
    """``git ls-tree -r -l HEAD -- <paths...>`` in-process.

    For each requested path that exists in HEAD's tree, returns
    ``(blob_sha, size_bytes)``. is_text classification stays in
    the caller (extension-based, matching legacy behavior).
    """
    repo = _r(repo_root)
    head_tree = (
        repo[tree_sha].peel(pygit2.Tree)
        if tree_sha is not None
        else repo.head.peel(pygit2.Tree)
    )
    out: dict[str, tuple[str, int]] = {}
    for path in paths:
        try:
            entry = head_tree[path]
        except KeyError:
            continue
        if _is_gitlink_entry(entry):
            continue
        # The size from the object header, without inflating the blob.
        object_type, size = repo.odb.read_header(entry.id)
        if object_type != pygit2.enums.ObjectType.BLOB:
            # A directory path: read the object as before (a tree has no
            # size, so this raises exactly as the old full read did).
            size = repo[entry.id].size
        out[path] = (str(entry.id), int(size))
    return out


def in_proc_head_blob_bytes(
    repo_root: Path,
    paths: Iterable[str],
    *,
    tree_sha: str | None = None,
) -> dict[str, bytes]:
    """Return exact tree blob bytes for requested repo-relative paths."""
    repo = _r(repo_root)
    head_tree = (
        repo[tree_sha].peel(pygit2.Tree)
        if tree_sha is not None
        else repo.head.peel(pygit2.Tree)
    )
    out: dict[str, bytes] = {}
    for raw_path in paths:
        path = str(raw_path).replace("\\", "/").lstrip("/")
        if not path:
            continue
        try:
            entry = head_tree[path]
        except KeyError:
            continue
        if _is_gitlink_entry(entry):
            continue
        blob = repo[entry.id]
        if isinstance(blob, pygit2.Blob):
            out[path] = bytes(blob.data)
    return out


def in_proc_tree_paths(
    repo_root: Path,
    *,
    tree_sha: str,
) -> set[str]:
    """Return every non-gitlink blob path from one immutable tree object."""
    repo = _r(repo_root)
    tree = repo[tree_sha].peel(pygit2.Tree)
    return _walk_tree_paths(repo, tree)


#: Up to this many paths, `in_proc_path_history` runs one `git log` per path,
#: each diffing only its path; above it, one `git log` diffs every path.
#: git's path filter is cheap for one path and slows sharply with more: on a
#: 17k-commit history one path took 0.15 s, two paths in different folders
#: 1.7 s, fifty 20 s, against 3.6 s for an unfiltered diff.
_PATHSPEC_PATH_LIMIT = 8


class PathHistory(NamedTuple):
    """When a path was last changed and first added, by authored commits.

    ``last_modified_*`` is the newest non-merge commit whose first-parent
    diff touches the path (committer time); ``created_*`` is the oldest
    non-merge commit whose first-parent diff adds it, or the root commit
    holding it (author time, as a commit node's own ``created_at``). A merge
    is named only for a path no non-merge commit touched (or added): its own
    conflict resolution made it. ``created_*`` is None when no commit in
    the walk adds the path.
    """

    last_modified_sha: str
    last_modified_at: str
    created_sha: str | None
    created_at: str | None


def in_proc_path_history(
    repo_root: Path,
    paths: Iterable[str],
    *,
    head_sha: str | None = None,
) -> dict[str, PathHistory]:
    """Last-modified and created commit of each path, skipping merges.

    A merge diffed against its first parent "adds" or "modifies" every file
    its branch changed, so taking the newest commit that touches a path names
    the merge, not the commit that made the change (issue #3468). The walk
    goes newest first (topological, ties by commit time, as ``git log``), so
    a mainline and a branch edit of one file are compared by date. A delete
    never counts as a path's last change: the path exists, so a merge undid
    it. Finding the oldest add needs the whole history, so there is no early
    exit; for a few paths (``_PATHSPEC_PATH_LIMIT``) each gets its own
    ``git log`` that diffs only that path. Paths never touched are absent from the result.
    """
    repo = _r(repo_root)
    target = set(paths)
    if not target:
        return {}
    start = head_sha if head_sha is not None else repo.head.target
    # The libgit2 walk fixes the commit set, order and metadata; the changed
    # files of each commit come from one streamed `git log` fed exactly that
    # commit list. git diffs in its own process, so the Python GIL is free
    # (a per-commit libgit2 diff held it about 16 ms per commit and stalled
    # every other source thread).
    commits = list(
        repo.walk(start, pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_TIME)
    )
    last: dict[str, tuple[str, str]] = {}
    last_by_merge: dict[str, tuple[str, str]] = {}
    created: dict[str, tuple[str, str]] = {}
    created_by_merge: dict[str, tuple[str, str]] = {}
    shas = [str(commit.id) for commit in commits]
    passes: list[set[str] | None] = (
        [{p} for p in sorted(target)]
        if len(target) <= _PATHSPEC_PATH_LIMIT
        else [None]
    )
    for only in passes:
        wanted = target if only is None else only
        changes = _GitLogChanges(
            repo_root, shas, sorted(only) if only is not None else None
        )
        with changes:
            for commit in commits:
                logged = changes.for_commit(str(commit.id))
                # Changes against the first parent: the same convention as
                # `git log --diff-merges=first-parent`.
                if commit.parents:
                    touched = {
                        change.path for change in logged if change.status != "D"
                    } & wanted
                    added = {
                        change.path for change in logged if change.status == "A"
                    } & wanted
                else:
                    # Root commit: every file in its tree counts as added.
                    touched = added = _walk_tree_paths(repo, commit.tree) & wanted
                if not touched:
                    continue
                sha = str(commit.id)
                is_merge = len(commit.parents) >= 2
                newest = last_by_merge if is_merge else last
                for p in touched:
                    if p not in newest:
                        newest[p] = (
                            sha,
                            _iso_from_offset(
                                commit.commit_time, commit.commit_time_offset
                            ),
                        )
                # Newest first: each later add found is older, so it overwrites.
                oldest = created_by_merge if is_merge else created
                for p in added:
                    oldest[p] = (
                        sha,
                        _iso_from_offset(commit.author.time, commit.author.offset),
                    )
    result: dict[str, PathHistory] = {}
    for p in target:
        modified = last.get(p) or last_by_merge.get(p)
        if modified is None:
            continue
        first = created.get(p) or created_by_merge.get(p)
        result[p] = PathHistory(
            modified[0],
            modified[1],
            first[0] if first else None,
            first[1] if first else None,
        )
    return result


class PathDeletion(NamedTuple):
    """The commit that deleted a path absent from the head tree.

    ``deleted_sha`` is the newest non-merge commit that changed the path,
    when that change is its delete and no newer merge added the path back;
    otherwise (a merge removed it) the newest merge whose first-parent diff
    deletes it, and failing that the newest non-merge delete. ``deleted_at`` is the commit's committer time, as
    ``last_modified_at``.
    """

    deleted_sha: str
    deleted_at: str


def in_proc_path_deletions(
    repo_root: Path,
    paths: Iterable[str],
    *,
    head_sha: str | None = None,
) -> dict[str, PathDeletion]:
    """Which commit deleted each path that the head tree no longer holds.

    Paths still in the head tree (submodules included) are absent from the
    result, and so is a path no commit reachable from the head deleted (one that only ever
    lived on another branch). The walk and the merge rule are those of
    :func:`in_proc_path_history`: newest first (topological, ties by commit
    time) and first-parent diffs; :class:`PathDeletion` gives the merge rule
    (issue #3468). Renames are not followed, so a moved file's old path
    reads as deleted by the move.
    """
    target = set(paths)
    if not target:
        return {}
    repo = _r(repo_root)
    start = head_sha if head_sha is not None else repo.head.target
    head = repo[start].peel(pygit2.Commit)
    # A submodule the head holds is a path that exists, not a deleted one.
    target -= _walk_tree_paths(repo, head.tree, include_gitlinks=True)
    if not target:
        return {}
    commits = list(
        repo.walk(head.id, pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_TIME)
    )
    # Per path, by walk position (0 = newest): its newest authored
    # (non-merge) change, delete or not; its newest authored delete; the
    # newest merge that deleted it; and the newest merge that added it back.
    authored: dict[str, tuple[int, str, str, bool]] = {}
    deleted: dict[str, tuple[str, str]] = {}
    deleted_by_merge: dict[str, tuple[str, str]] = {}
    added_by_merge: dict[str, int] = {}
    shas = [str(commit.id) for commit in commits]
    passes: list[set[str] | None] = (
        [{p} for p in sorted(target)]
        if len(target) <= _PATHSPEC_PATH_LIMIT
        else [None]
    )
    for only in passes:
        wanted = target if only is None else only
        changes = _GitLogChanges(
            repo_root, shas, sorted(only) if only is not None else None
        )
        with changes:
            for position, commit in enumerate(commits):
                logged = changes.for_commit(str(commit.id))
                if not commit.parents:
                    continue
                if len(commit.parents) >= 2:
                    for change in logged:
                        if change.path not in wanted:
                            continue
                        if change.status == "D":
                            deleted_by_merge.setdefault(change.path, _when(commit))
                        elif change.status == "A":
                            added_by_merge.setdefault(change.path, position)
                    continue
                for change in logged:
                    if change.path not in wanted:
                        continue
                    if change.path not in authored:
                        authored[change.path] = (
                            position,
                            *_when(commit),
                            change.status == "D",
                        )
                    if change.status == "D":
                        deleted.setdefault(change.path, _when(commit))
    result: dict[str, PathDeletion] = {}
    for p in target:
        newest = authored.get(p)
        if (
            newest is not None
            and newest[3]
            # A merge that added the path back after this delete makes a
            # later merge the one that removed it.
            and added_by_merge.get(p, len(commits)) > newest[0]
        ):
            result[p] = PathDeletion(newest[1], newest[2])
        elif p in deleted_by_merge:
            # No authored delete after the path's last authored add or
            # edit: a merge removed it.
            result[p] = PathDeletion(*deleted_by_merge[p])
        elif p in deleted:
            # A branch edit newer than the delete, dropped by a merge that
            # kept the first parent's side (no change on the first parent).
            result[p] = PathDeletion(*deleted[p])
    return result


def paths_touched_between(repo_root: Path, old_sha: str, new_sha: str) -> set[str]:
    """Every path a commit reachable from one head but not the other touched.

    Covers the commits a head move brought in or left behind (both sides of
    ``old...new``), first-parent diffs as elsewhere, renames off. A file a
    branch added and removed again before a fast-forward or merge is in this
    set though the two head trees agree on it.
    """
    # Same flags, environment and path decoding as `_GitLogChanges`, so a
    # path reads exactly as the commit walk spells it.
    args = [
        "log",
        "--format=",
        "--name-only",
        "-z",
        "--no-renames",
        "--diff-merges=first-parent",
        "--ignore-submodules=none",
        "--no-relative",
        "--no-notes",
        "--no-color",
        f"{old_sha}...{new_sha}",
        "--",
    ]
    env = {k: v for k, v in _git_env().items() if k not in _GIT_LOG_ENV_DROP}
    env["GIT_NO_REPLACE_OBJECTS"] = "1"
    proc = subprocess.run(  # nosec B603
        [_git_executable(), "-C", str(repo_root), *args],
        capture_output=True,
        check=False,
        env=env,
    )
    if proc.returncode != 0:
        raise GitInvocationError(
            args, proc.returncode, proc.stderr.decode("utf-8", errors="replace")
        )
    return {os.fsdecode(p) for p in proc.stdout.split(b"\0") if p}


def _when(commit: pygit2.Commit) -> tuple[str, str]:
    """A commit's sha and committer time, as ISO 8601."""
    return (
        str(commit.id),
        _iso_from_offset(commit.commit_time, commit.commit_time_offset),
    )


class GitLogChange(NamedTuple):
    """One changed path of a commit, against its first parent, from git log."""

    status: str  # git's raw status letter: A, M, D or T (renames are off)
    path: str


#: `git log` options that make its raw first-parent changes match a libgit2
#: tree-to-tree diff with default options, whatever the repository or user
#: configuration says. The commits come from stdin and are listed as given
#: (`--no-walk=unsorted`), so git never decides range membership or order.
#: Renames off (libgit2 finds none unless asked); merges diffed against their
#: first parent only; submodule pointer changes kept; no diff order file, no
#: relative paths, no signatures or notes; UTF-8 output whatever
#: `i18n.logOutputEncoding` says; NUL-separated paths so no path is quoted.
_GIT_LOG_CHANGE_ARGS = (
    "log",
    "--stdin",
    "--no-walk=unsorted",
    "--format=%x01%H",
    "--encoding=UTF-8",
    "--raw",
    "-z",
    "--no-renames",
    "--diff-merges=first-parent",
    "--ignore-submodules=none",
    "--no-relative",
    "-O/dev/null",
    "--no-show-signature",
    "--no-notes",
    "--no-color",
)

#: Environment variables that would point git at another repository, object
#: store or configuration than the libgit2 handle opened on ``repo_root``.
_GIT_LOG_ENV_DROP = (
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_NAMESPACE",
    "GIT_COMMON_DIR",
    "GIT_CONFIG_PARAMETERS",
    "GIT_CONFIG_COUNT",
    "GIT_GRAFT_FILE",
    "GIT_REPLACE_REF_BASE",
    "GIT_SHALLOW_FILE",
)

_GIT_LOG_READ_BYTES = 1 << 16


class _GitLogChanges:
    """Per-commit first-parent changed paths, streamed from one ``git log``.

    The caller passes the exact commit list of its libgit2 walk, in walk
    order; ``git log --stdin --no-walk=unsorted --raw -z`` lists those
    commits and no others, in that order, and is parsed as it arrives, never
    buffered whole. ``for_commit`` must be called for each commit in the same
    order; any disagreement raises ``GitLogStreamError`` and a git failure
    raises ``GitInvocationError``. Nothing falls back to another reader.

    With ``paths``, git diffs only those paths (taken literally, never as
    patterns) and still lists every commit (``--full-history --sparse``), so
    the order check holds; a commit that touches none of them has no changes.
    For one path that costs a small fraction of a full diff; git's path
    filter slows sharply as paths are added.

    Use as a context manager: leaving it stops git. Leaving normally checks
    git's exit status; leaving on an exception stops git without masking it.
    """

    def __init__(
        self,
        repo_root: Path,
        commits: list[str],
        paths: Iterable[str] | None = None,
    ) -> None:
        args = (
            [*_GIT_LOG_CHANGE_ARGS, "--full-history", "--sparse", "--", *paths]
            if paths is not None
            else [*_GIT_LOG_CHANGE_ARGS, "--"]
        )
        env = {k: v for k, v in _git_env().items() if k not in _GIT_LOG_ENV_DROP}
        # libgit2 does not read `refs/replace/`; neither may git.
        env["GIT_NO_REPLACE_OBJECTS"] = "1"
        # A path is a path: `*`, `?`, `[` and a leading `:` match themselves.
        env["GIT_LITERAL_PATHSPECS"] = "1"
        self._args = args
        self._eof = False
        self._closed = False
        self._stderr = tempfile.TemporaryFile()
        self._proc = subprocess.Popen(  # nosec B603
            [_git_executable(), "-C", str(repo_root), *args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self._stderr,
            env=env,
        )
        # git reads the whole revision list before it writes anything, so
        # writing it all first cannot deadlock against a full stdout pipe.
        assert self._proc.stdin is not None
        try:
            self._proc.stdin.write("".join(f"{sha}\n" for sha in commits).encode())
            self._proc.stdin.close()
        except BrokenPipeError:
            # git exited early (a bad option, an old git): `close` reports it.
            try:
                self._proc.stdin.close()
            except BrokenPipeError:
                pass
        self._records = self._parse()

    def __enter__(self) -> "_GitLogChanges":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close(check=exc_type is None)

    def for_commit(self, sha: str) -> list[GitLogChange]:
        """The first-parent changes of ``sha``, the next commit in order."""
        record = next(self._records, None)
        if record is None:
            # A git failure raises with git's own message first.
            self.close()
            raise GitLogStreamError(
                f"git log ended before commit {sha}: `git {' '.join(self._args)}`"
            )
        listed_sha, changes = record
        if listed_sha != sha:
            raise GitLogStreamError(
                f"git log listed commit {listed_sha} where the libgit2 walk "
                f"has {sha}"
            )
        return changes

    def close(self, *, check: bool = True) -> None:
        """Stop git if it is still writing and release its pipes.

        With ``check``, a git that finished on its own with a non-zero status
        raises ``GitInvocationError``. A git stopped early (the caller had its
        answer, or is already raising) is killed and its status ignored.
        """
        self._stop_git(check=check)
        self._records.close()

    def _stop_git(self, *, check: bool) -> None:
        if self._closed:
            return
        self._closed = True
        proc = self._proc
        if not self._eof:
            proc.kill()
        assert proc.stdout is not None
        proc.stdout.close()
        returncode = proc.wait()
        self._stderr.seek(0)
        stderr = self._stderr.read().decode("utf-8", errors="replace")
        self._stderr.close()
        if check and self._eof and returncode != 0:
            raise GitInvocationError(self._args, returncode, stderr)

    def _parse(self) -> Iterator[tuple[str, list[GitLogChange]]]:
        """Yield ``(sha, changes)`` per commit as git writes them.

        The stream is NUL-separated tokens: ``\\x01<sha>`` opens a commit;
        then pairs of a raw metadata token (``:<modes> <ids> <status>``,
        the first one after a newline) and a path token. A commit is yielded
        once the next one opens; the last one only after git exited 0, so a
        truncated stream never yields a partial commit.
        """
        assert self._proc.stdout is not None
        stdout = self._proc.stdout
        sha: str | None = None
        changes: list[GitLogChange] = []
        status: str | None = None  # set while the next token is a path
        tail = b""
        while True:
            chunk = stdout.read1(_GIT_LOG_READ_BYTES)  # type: ignore[attr-defined]
            if not chunk:
                self._eof = True
                break
            tokens = (tail + chunk).split(b"\0")
            tail = tokens.pop()
            for token in tokens:
                if status is not None:
                    changes.append(GitLogChange(status, os.fsdecode(token)))
                    status = None
                    continue
                token = token.lstrip(b"\n")
                if not token:
                    continue
                if token[:1] == b"\x01":
                    if sha is not None:
                        yield sha, changes
                    sha = token[1:].decode("ascii")
                    changes = []
                elif token[:1] == b":" and sha is not None:
                    status = token.rsplit(b" ", 1)[-1].decode("ascii")
                    if status not in ("A", "M", "D", "T"):
                        raise GitLogStreamError(
                            f"git log listed status {status!r} for commit {sha}; "
                            "expected A, M, D or T with renames off"
                        )
                else:
                    raise GitLogStreamError(
                        f"unexpected git log output after commit {sha}: {token[:80]!r}"
                    )
        # Check git's status before trusting the last record.
        self._stop_git(check=True)
        if tail or status is not None:
            raise GitLogStreamError(
                f"git log output ended mid-record after commit {sha}"
            )
        if sha is not None:
            yield sha, changes


class GitLogStreamError(RuntimeError):
    """The streamed ``git log`` disagreed with the libgit2 walk or could not
    be parsed."""


def _may_pair_renames(changes: list[GitLogChange]) -> bool:
    """Whether libgit2's rename/copy detection could pair anything here.

    ``find_similar`` (with the repository's ``diff.renames`` setting: renames,
    or renames and copies) only turns an added file into a rename or copy of
    a deleted or modified one, so a commit without an add, or with nothing
    but adds, keeps its plain name-status. A type change (``T``) is neither:
    the path exists before and after, and ``_first_parent_diff`` never lets
    rename detection pair either side of it (issue #3666).
    """
    has_add = any(change.status == "A" for change in changes)
    has_source = any(change.status in ("D", "M") for change in changes)
    return has_add and has_source


def _git_log_name_status(
    changes: list[GitLogChange],
) -> list[tuple[str, str, str | None]]:
    """A non-root commit's name-status in the shape of
    ``_commit_name_status`` without rename detection.

    A type change (file <-> symlink or submodule) stays one ``T`` row, as
    git reports it and as ``_commit_name_status`` returns it (issue #3666).
    """
    return [(change.status, change.path, None) for change in changes]


def _walk_tree_paths(
    repo: pygit2.Repository,
    tree: pygit2.Tree,
    prefix: str = "",
    *,
    include_gitlinks: bool = False,
) -> set[str]:
    """Recursive list of every blob path under a tree (and, when asked,
    every submodule entry's path)."""
    out: set[str] = set()
    for entry in tree:
        full = f"{prefix}{entry.name}" if not prefix else f"{prefix}/{entry.name}"
        if _is_gitlink_entry(entry):
            if include_gitlinks:
                out.add(full)
            continue
        obj = repo[entry.id]
        if isinstance(obj, pygit2.Tree):
            out |= _walk_tree_paths(
                repo, obj, prefix=full, include_gitlinks=include_gitlinks
            )
        else:
            out.add(full)
    return out


def _iso_from_offset(commit_time: int, offset_minutes: int) -> str:
    """Format a commit time + offset as an ISO 8601 string matching
    what `git log --format=%cI` produces."""
    tz = timezone(timedelta(minutes=offset_minutes))
    return datetime.fromtimestamp(commit_time, tz=tz).isoformat()


class RefBinding(NamedTuple):
    refname: str
    sha: str


def in_proc_for_each_ref(
    repo_root: Path,
    prefixes: tuple[str, ...] = ("refs/heads/", "refs/tags/"),
) -> list[RefBinding]:
    """``git for-each-ref --format=… refs/...`` in-process.

    Returns ``[(refname, commit_sha), ...]`` for refs whose name
    starts with any of the given prefixes. Annotated tags are
    peeled to their target commit so the caller sees a commit sha
    in the second column.
    """
    repo = _r(repo_root)
    out: list[RefBinding] = []
    for ref in repo.references.iterator():
        if not ref.name.startswith(prefixes):
            continue
        try:
            target = ref.peel(pygit2.Commit)
            out.append(RefBinding(ref.name, str(target.id)))
        except (TypeError, KeyError, pygit2.GitError):
            # Symbolic ref or non-commit target — skip; legacy
            # parser skips these too.
            continue
    return out


class CommitRecord(NamedTuple):
    sha: str
    parents: list[str]
    author_email: str
    author_name: str
    author_iso: str
    committer_iso: str
    subject: str
    # name_status entries: (status_char, new_path, old_path_for_rename_or_none)
    # For non-rename deltas, old_path is None and new_path holds the file's
    # only relevant path. For 'D' deletes, new_path is the deleted file's
    # path. Matches `_git_graph_history_emit._CommitRecord.file_changes`.
    name_status: list[tuple[str, str, str | None]]
    # Everything after the subject line, leading blank lines and trailing
    # whitespace removed; "" when the message is a subject only. Unredacted
    # and uncapped here: the history emitter redacts and caps it.
    body: str = ""
    # Per-file unified diffs against the first parent, or None when the
    # walk was not asked for them (or the commit is a merge, or older than
    # `patches_since`). Unredacted and uncapped here.
    patches: "list[FilePatch] | None" = None
    # Changed paths whose old or new blob exceeds `max_patch_blob_bytes`, or
    # is missing from the local object store (a partial clone): never
    # diffed, reported so the emitter can count them.
    oversized_patch_paths: tuple[str, ...] = ()


class FilePatch(NamedTuple):
    """One changed text file's unified diff (from ``in_proc_walk_commits``)."""

    status: str  # name-status letter: A, M, D, R, C or T
    path: str  # the changed path (the deleted path for D)
    old_path: str | None  # the source path of a rename or copy
    text: str  # the patch, decoded as UTF-8 with replacement characters


#: Largest blob (either side of a change) the walk will diff. A bigger file
#: is reported in `CommitRecord.oversized_patch_paths` and never diffed, so
#: one generated or vendored file cannot stall the walk.
MAX_PATCH_BLOB_BYTES = 1_048_576


def split_commit_message(message: str) -> tuple[str, str]:
    """Split a raw commit message into ``(subject, body)``.

    The subject is the first line, as the history source has always stored
    it (not git's ``%s``, which joins a multi-line first paragraph). The
    body is everything after that line with leading blank (or
    whitespace-only) lines and trailing whitespace removed, or ``""`` for
    a subject-only message. The first kept line keeps its indentation.
    """
    subject, _, rest = message.partition("\n")
    lines = rest.split("\n")
    while lines and not lines[0].strip():
        lines.pop(0)
    return subject, "\n".join(lines).rstrip()


def in_proc_walk_commits(
    repo_root: Path,
    positive_shas: Iterable[str],
    negative_shas: Iterable[str] = (),
    *,
    detect_renames: bool = False,
    with_patches: bool = False,
    patches_since: int | None = None,
    max_patch_blob_bytes: int = MAX_PATCH_BLOB_BYTES,
) -> Iterator[CommitRecord]:
    """``git rev-list <positive> --not <negative> --pretty=… --name-status``
    in-process.

    Yields commits in reverse-topological order (root-first, matching
    the legacy ``--reverse`` walk). Each record carries the same
    fields the subprocess parser extracted plus a name_status list
    of ``(status_char, path)`` tuples.

    ``with_patches`` also fills ``CommitRecord.patches`` with each changed
    text file's unified diff, taken from the same first-parent diff as the
    name-status list (a root commit diffs against the empty tree). Merge
    commits and commits whose committer time is before ``patches_since``
    (a Unix timestamp) get ``patches=None``: a merge's first-parent diff
    repeats its branch's commits.
    """
    repo = _r(repo_root)
    pos_list = list(positive_shas)
    if not pos_list:
        return
    walker = repo.walk(
        pos_list[0],
        pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_REVERSE,
    )
    for sha in pos_list[1:]:
        walker.push(sha)
    for neg in negative_shas:
        try:
            walker.hide(neg)
        except (pygit2.GitError, KeyError):
            # Unreachable negative ref — log and continue; legacy
            # falls back to a full walk minus already-seen shas, but
            # at the moment our callers pre-filter so this is rare.
            logger.warning(
                "in_proc_walk_commits: hide() unreachable sha %s",
                neg,
            )
    # libgit2 decides the commit set and order; the changed files of each
    # commit come from one streamed `git log` fed exactly that list (see
    # `_GitLogChanges`), so git never judges range membership itself (with
    # skewed commit dates its `A --not B` can differ from libgit2's hide).
    # libgit2 still diffs a commit whose patches are wanted, and, with rename
    # detection on, a commit that adds a file next to other changes: git's
    # rename scoring is not libgit2's.
    commits = list(walker)
    with _GitLogChanges(repo_root, [str(c.id) for c in commits]) as changes:
        yield from _walk_commit_records(
            repo,
            commits,
            changes,
            detect_renames=detect_renames,
            with_patches=with_patches,
            patches_since=patches_since,
            max_patch_blob_bytes=max_patch_blob_bytes,
        )


def _walk_commit_records(
    repo: pygit2.Repository,
    commits: list[pygit2.Commit],
    changes: "_GitLogChanges",
    *,
    detect_renames: bool,
    with_patches: bool,
    patches_since: int | None,
    max_patch_blob_bytes: int,
) -> Iterator[CommitRecord]:
    for commit in commits:
        wants_patches = (
            with_patches
            and len(commit.parents) <= 1
            and (patches_since is None or commit.commit_time >= patches_since)
        )
        diff = (
            _first_parent_diff(commit, detect_renames=detect_renames)
            if wants_patches
            else None
        )
        # Read every commit's record, used or not, so the stream stays in
        # step with the walk.
        logged = changes.for_commit(str(commit.id))
        if diff is None and commit.parents:
            if detect_renames and _may_pair_renames(logged):
                name_status = _commit_name_status(
                    repo, commit, detect_renames=True
                )
            else:
                name_status = _git_log_name_status(logged)
        else:
            name_status = _commit_name_status(
                repo,
                commit,
                detect_renames=detect_renames,
                diff=diff,
            )
        patches: list[FilePatch] | None = None
        oversized: tuple[str, ...] = ()
        if diff is not None:
            patches, oversized = _diff_patches(
                repo, diff, max_blob_bytes=max_patch_blob_bytes
            )
        subject, body = split_commit_message(commit.message or "")
        yield CommitRecord(
            sha=str(commit.id),
            parents=[str(p.id) for p in commit.parents],
            author_email=commit.author.email or "",
            author_name=commit.author.name or "",
            author_iso=_iso_from_offset(commit.author.time, commit.author.offset),
            committer_iso=_iso_from_offset(
                commit.commit_time, commit.commit_time_offset
            ),
            subject=subject,
            name_status=name_status,
            body=body,
            patches=patches,
            oversized_patch_paths=oversized,
        )


_DELTA_STATUS_CODES = {
    pygit2.GIT_DELTA_ADDED: "A",
    pygit2.GIT_DELTA_MODIFIED: "M",
    pygit2.GIT_DELTA_DELETED: "D",
    pygit2.GIT_DELTA_RENAMED: "R",
    pygit2.GIT_DELTA_COPIED: "C",
    pygit2.GIT_DELTA_TYPECHANGE: "T",
}


class _DiffChange(NamedTuple):
    """One name-status row of a first-parent diff and the deltas behind it.

    ``parts`` holds ``(diff, delta index, delta)`` for each libgit2 delta the
    row stands for: one, or two for a type change (the delete, then the add).
    """

    status: str  # A, M, D, R, C or T
    path: str  # the changed path (the deleted path for D)
    old_path: str | None  # the source path of a rename or copy
    parts: tuple[tuple[pygit2.Diff, int, Any], ...]


def _diff_changes(diff: pygit2.Diff) -> list[_DiffChange]:
    """The rows of a libgit2 diff, with a type change kept as one ``T`` row.

    libgit2's default diff reports a type change (file <-> symlink or
    submodule) as a delete and an add of the same path, where git reports
    one ``T``. A same-path delete and add arise only that way: a path that
    keeps its type is a modification.
    """
    changes: list[_DiffChange] = []
    added_or_deleted: dict[str, int] = {}
    for index, delta in enumerate(diff.deltas):
        code = _DELTA_STATUS_CODES.get(delta.status)
        if code is None:
            continue
        part = (diff, index, delta)
        if code == "D":
            path, old_path = delta.old_file.path, None
        elif code in ("R", "C"):
            path, old_path = delta.new_file.path, delta.old_file.path
        else:
            path, old_path = delta.new_file.path, None
        if code in ("A", "D"):
            prior = added_or_deleted.get(path)
            if prior is not None and changes[prior].status in ("A", "D"):
                if changes[prior].status == code:
                    raise RuntimeError(
                        f"libgit2 listed {path!r} as {code} twice in one diff"
                    )
                changes[prior] = _DiffChange(
                    "T", path, None, changes[prior].parts + (part,)
                )
                continue
            added_or_deleted[path] = len(changes)
        changes.append(_DiffChange(code, path, old_path, (part,)))
    return changes


def _first_parent_diff(
    commit: pygit2.Commit,
    *,
    detect_renames: bool = False,
) -> list[_DiffChange]:
    """The commit's changes against its first parent, or against the empty
    tree for a root commit.

    A type change is one ``T`` row. With ``detect_renames``, libgit2's
    ``find_similar`` may pair the delete half of a type change with an
    added file, or its add half with a deleted file. git keeps a type change
    one ``T`` and never pairs it, so neither does this: the path existed
    before and after (issue #3666). The other side of such a pair stays the
    plain add or delete it was before detection.
    """
    if commit.parents:
        parent_tree = commit.parents[0].tree
        diff = parent_tree.diff_to_tree(commit.tree)
    else:
        # No argument diffs against the empty tree; `swap` makes the empty
        # tree the old side, so every path reads as added.
        diff = commit.tree.diff_to_tree(swap=True)
    plain = _diff_changes(diff)
    if not detect_renames:
        return plain
    type_changed = {change.path for change in plain if change.status == "T"}
    if not type_changed:
        diff.find_similar()
        return _diff_changes(diff)
    similar = parent_tree.diff_to_tree(commit.tree)
    similar.find_similar()
    return _unpair_type_changes(plain, _diff_changes(similar), type_changed)


def _unpair_type_changes(
    plain: list[_DiffChange],
    similar: list[_DiffChange],
    type_changed: set[str],
) -> list[_DiffChange]:
    """``similar`` (renames detected) with every type change restored from
    ``plain`` (no detection) as one unpaired ``T`` row.

    A rename or copy that used a half of a type change goes back to the plain
    add of its new path and, for a rename, the plain delete of its old path,
    unless that path is itself a type change.
    """
    plain_halves = {
        (change.status, change.path): change
        for change in plain
        if change.status in ("A", "D")
    }
    out = [change for change in plain if change.status == "T"]
    for change in similar:
        if change.status in ("R", "C") and (
            change.path in type_changed or change.old_path in type_changed
        ):
            if change.path not in type_changed:
                out.append(plain_halves[("A", change.path)])
            if change.status == "R" and change.old_path not in type_changed:
                out.append(plain_halves[("D", change.old_path)])
            continue
        if change.path in type_changed:
            # A leftover half of a type change; its T row is already in.
            continue
        out.append(change)
    return sorted(out, key=lambda change: (change.path, change.status))


def _blob_size(repo: pygit2.Repository, oid: pygit2.Oid) -> int:
    """Size of the blob ``oid`` from its object header (0 for the zero id)."""
    if not any(oid.raw):
        return 0
    return int(repo.odb.read_header(oid)[1])


def _diff_patches(
    repo: pygit2.Repository,
    diff: list[_DiffChange],
    *,
    max_blob_bytes: int,
) -> tuple[list[FilePatch], tuple[str, ...]]:
    """Unified diffs of the text files in ``diff`` (from
    ``_first_parent_diff``).

    Returns ``(patches, oversized_paths)``. A change whose old or new blob
    is bigger than ``max_blob_bytes`` is not diffed and its path is
    returned in ``oversized_paths``. Binary files, gitlinks (submodule
    pointers) and changes with no text hunk (a mode-only change, an empty
    file) produce nothing: they have no text to store. A type change is one
    ``T`` patch: the delete's text, then the add's.
    """
    patches: list[FilePatch] = []
    oversized: list[str] = []
    for change in diff:
        texts: list[str] = []
        not_diffed = False
        for part_diff, index, delta in change.parts:
            if pygit2.GIT_FILEMODE_COMMIT in (
                delta.old_file.mode,
                delta.new_file.mode,
            ):
                continue
            try:
                size = max(
                    _blob_size(repo, delta.old_file.id),
                    _blob_size(repo, delta.new_file.id),
                )
                if size > max_blob_bytes:
                    not_diffed = True
                    break
                patch = part_diff[index]
            except (KeyError, pygit2.GitError) as exc:
                # A blob missing from the local object store (a partial clone
                # whose promisor remote libgit2 cannot fetch from, a pruned
                # object) cannot be diffed. Count it like an oversized file
                # instead of failing the whole history walk.
                logger.warning(
                    "commit diff: blob for %s is not readable locally (%s); "
                    "counting it as not diffed",
                    change.path,
                    exc,
                )
                not_diffed = True
                break
            if patch is None or patch.delta.is_binary or not patch.hunks:
                continue
            texts.append(patch.data.decode("utf-8", errors="replace"))
        if not_diffed:
            oversized.append(change.path)
            continue
        if texts:
            patches.append(
                FilePatch(
                    status=change.status,
                    path=change.path,
                    old_path=change.old_path,
                    text="".join(texts),
                )
            )
    return patches, tuple(dict.fromkeys(oversized))


def _commit_name_status(
    repo: pygit2.Repository,
    commit: pygit2.Commit,
    *,
    detect_renames: bool = False,
    diff: list[_DiffChange] | None = None,
) -> list[tuple[str, str, str | None]]:
    """Compute the per-commit name-status list against the first
    parent (or against an empty tree for the root commit).

    Returns ``[(status, new_path, old_path_or_none), ...]`` matching
    the legacy ``_CommitRecord.file_changes`` shape. For renames and
    copies, `old_path_or_none` is the source path; for everything
    else it's None. For pure deletes, new_path holds the deleted
    file's path and old_path_or_none is None. A type change is one
    ``T`` row, never a delete plus an add (issue #3666).

    ``diff`` is the already computed first-parent diff (from
    ``_first_parent_diff``, renames already detected when asked), so the
    patches and this list come from one diff.
    """
    parents = commit.parents
    if not parents:
        # Root commit: every entry in its tree is added.
        return [("A", p, None) for p in sorted(_walk_tree_paths(repo, commit.tree))]
    if diff is None:
        diff = _first_parent_diff(commit, detect_renames=detect_renames)
    return [(change.status, change.path, change.old_path) for change in diff]


# ---------------------------------------------------------------------------
# Subprocess git helpers (still used by bench orchestration + writes)
# ---------------------------------------------------------------------------


class GitInvocationError(RuntimeError):
    """A ``git`` subprocess returned non-zero exit code."""

    def __init__(self, args: list[str], returncode: int, stderr: str) -> None:
        super().__init__(
            f"git {' '.join(args)!r} failed (exit {returncode}): {stderr.strip()}"
        )
        self.args_list = args
        self.returncode = returncode
        self.stderr = stderr


def _git_executable() -> str:
    git = shutil.which("git")
    if git is None:
        raise FileNotFoundError("git")
    return git


def run_git(repo_root: Path, args: list[str], *, check: bool = True) -> str:
    """Invoke ``git -C <repo_root> <args...>`` and return stdout.

    Parameters
    ----------
    repo_root : Path
        Working directory the git invocation runs in.
    args : list[str]
        Arguments to git (e.g., ``["ls-tree", "-r", "-l", "HEAD"]``).
    check : bool
        If True (default), raise :class:`GitInvocationError` on
        non-zero exit. If False, return stdout regardless and let the
        caller inspect the exit code via the returned process.

    Returns
    -------
    str
        Stdout decoded as UTF-8 (replacing undecodable bytes).
    """
    cmd = [_git_executable(), "-C", str(repo_root), *args]
    proc = subprocess.run(  # nosec B603
        cmd,
        capture_output=True,
        check=False,
    )
    stdout = proc.stdout.decode("utf-8", errors="replace")
    if check and proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", errors="replace")
        raise GitInvocationError(args, proc.returncode, stderr)
    return stdout


def run_git_stdin(
    repo_root: Path,
    args: list[str],
    *,
    stdin: bytes,
    check: bool = True,
) -> tuple[int, str, str]:
    """Invoke git with stdin and return (returncode, stdout, stderr)."""
    cmd = [_git_executable(), "-C", str(repo_root), *args]
    proc = subprocess.run(  # nosec B603
        cmd,
        input=stdin,
        capture_output=True,
        check=False,
    )
    stdout = proc.stdout.decode("utf-8", errors="replace")
    stderr = proc.stderr.decode("utf-8", errors="replace")
    if check and proc.returncode != 0:
        raise GitInvocationError(args, proc.returncode, stderr)
    return proc.returncode, stdout, stderr


def run_git_bytes(repo_root: Path, args: list[str], *, check: bool = True) -> bytes:
    """Invoke git and return raw stdout bytes (for NUL-delimited output)."""
    cmd = [_git_executable(), "-C", str(repo_root), *args]
    proc = subprocess.run(  # nosec B603
        cmd,
        capture_output=True,
        check=False,
    )
    if check and proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", errors="replace")
        raise GitInvocationError(args, proc.returncode, stderr)
    return proc.stdout


def current_tree_sha(repo_root: Path) -> str:
    """Return the SHA of the tree pointed at by HEAD."""
    return run_git(repo_root, ["rev-parse", "HEAD^{tree}"]).strip()


def parse_porcelain_z(stdout: bytes) -> set[str]:
    """Parse `git status --porcelain -z` output into a set of dirty paths.

    Format: each entry is `XY <path>\\0`, where XY is the 2-char status
    code. Renames produce a 2-entry pair (`R  newpath\\0oldpath\\0`).
    """
    dirty: set[str] = set()
    if not stdout:
        return dirty
    # Split on NUL, drop trailing empty
    parts = stdout.split(b"\x00")
    if parts and parts[-1] == b"":
        parts = parts[:-1]
    i = 0
    while i < len(parts):
        entry = parts[i]
        # Each entry begins with 2 status chars + 1 space, then path
        if len(entry) < 4:
            i += 1
            continue
        status = entry[:2]
        path = entry[3:].decode("utf-8", errors="replace")
        dirty.add(path)
        # Rename status `R ` or `RM` consumes a second NUL-delimited path
        # (the OLD path) which we also count as dirty.
        if status[:1] in (b"R", b"C"):
            i += 1
            if i < len(parts):
                old_path = parts[i].decode("utf-8", errors="replace")
                dirty.add(old_path)
        i += 1
    return dirty


def is_repo(repo_root: Path) -> bool:
    """True if `repo_root` is inside a git repository."""
    try:
        run_git(repo_root, ["rev-parse", "--is-inside-work-tree"])
        return True
    except (GitInvocationError, FileNotFoundError):
        return False


# ---------------------------------------------------------------------------
# Checkout acquisition (clone-if-missing / fast-forward-pull)
# ---------------------------------------------------------------------------
#
# Used by sources that manage their own checkout (``ensure_checkout``). This is
# the ONLY write path in this module; everything else reads. It never raises for
# operational failures (network/auth/non-fast-forward) — it returns a structured
# outcome the caller maps to source health — so a bad clone/pull degrades a
# single source instead of aborting the whole ingest layer.


class AcquireOutcome(NamedTuple):
    """Result of :func:`clone_or_pull`.

    ``status`` is one of:
      - ``"cloned"``  — a missing checkout was freshly cloned.
      - ``"updated"`` — an existing checkout was fast-forwarded (or already current).
      - ``"stale"``   — an existing checkout could NOT be fast-forwarded
        (diverged / detached / dirty / no upstream / wrong branch); the working
        tree was left untouched.
      - ``"failed"``  — a missing checkout could not be materialised.
    """

    status: str
    reason: str | None = None


def _git_env() -> dict[str, str]:
    """os.environ + ``GIT_TERMINAL_PROMPT=0`` so a missing credential fails fast
    instead of blocking on an interactive prompt."""
    env = dict(os.environ)
    env["GIT_TERMINAL_PROMPT"] = "0"
    return env


def _run_git_raw(
    args: list[str],
    *,
    cwd: str | None = None,
    timeout: float | None = None,
) -> tuple[int, str, str]:
    """Run ``git <args...>`` (no ``-C`` wrapper — needed for ``clone`` where the
    target does not exist yet) and return ``(returncode, stdout, stderr)``."""
    cmd = [_git_executable(), *args]
    proc = subprocess.run(  # nosec B603
        cmd,
        capture_output=True,
        check=False,
        cwd=cwd,
        env=_git_env(),
        timeout=timeout,
    )
    return (
        proc.returncode,
        proc.stdout.decode("utf-8", errors="replace"),
        proc.stderr.decode("utf-8", errors="replace"),
    )


# Per-resolved-path locks so two sources sharing a ``repo_root`` cannot collide
# on ``index.lock`` when both call clone_or_pull in parallel layers.
_acquire_locks_guard = threading.Lock()
_acquire_locks: dict[str, threading.Lock] = {}


def _path_lock(repo_root: Path) -> threading.Lock:
    key = str(Path(repo_root).resolve())
    with _acquire_locks_guard:
        lock = _acquire_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _acquire_locks[key] = lock
        return lock


def _clone(
    repo_root: Path, url: str, tracked_branch: str | None, timeout: float | None
) -> AcquireOutcome:
    existed = repo_root.exists()
    repo_root.parent.mkdir(parents=True, exist_ok=True)
    args = ["clone"]
    if tracked_branch:
        args += ["--branch", tracked_branch]
    # Clone with a credential-free URL so no token is persisted into .git/config.
    args += [_clean_clone_url(url), str(repo_root)]
    try:
        returncode, _stdout, stderr = _run_git_raw(args, timeout=timeout)
    except subprocess.TimeoutExpired:
        # A killed clone can leave a partial `.git`; remove it (only when git
        # created the dir) so the next run re-clones cleanly rather than pulling a
        # corrupt checkout forever.
        if not existed:
            shutil.rmtree(repo_root, ignore_errors=True)
        raise
    if returncode == 0:
        return AcquireOutcome("cloned")
    if not existed:
        shutil.rmtree(repo_root, ignore_errors=True)
    return AcquireOutcome(
        "failed", _scrub_userinfo(stderr.strip())[:500] or f"git clone exit {returncode}"
    )


_USERINFO_RE = re.compile(r"(://)[^/@\s]+@")

# A finite default so a hung clone/pull can't block ingest forever. Overridable
# per call; ``None`` at the clone_or_pull boundary resolves to this.
_DEFAULT_ACQUIRE_TIMEOUT = 300.0


def _scrub_userinfo(text: str) -> str:
    """Mask ``user:pass@`` credentials in a git URL echoed in an error string
    before it is stored on source health (keeps the host visible)."""
    return _USERINFO_RE.sub(r"\1***@", text)


# Only http(s) userinfo is a credential (`token@` or `user:pass@`); the `git@`
# in an ssh URL (`ssh://git@host` / `git@host:path`) is the required login, not a
# secret, so it must be preserved.
_HTTP_USERINFO_RE = re.compile(r"^(https?://)[^/@\s]+@")


def _clean_clone_url(url: str) -> str:
    """Drop http(s) ``user:pass@`` / ``token@`` userinfo from a URL, so no
    credential is written into ``.git/config`` by a fresh clone. Auth comes from
    the ambient git credential helper. SSH URLs are left untouched."""
    return _HTTP_USERINFO_RE.sub(r"\1", url.strip())


def _norm_remote(url: str) -> str:
    """Normalise a remote URL for equality checks: strip credentials, a trailing
    slash and a ``.git`` suffix so ``host/x`` and ``host/x.git/`` compare equal
    (avoids a needless ``set-url`` on a cosmetic difference)."""
    return _clean_clone_url(url).rstrip("/").removesuffix(".git")


def _pull_ff_only(
    repo_root: Path, url: str, tracked_branch: str | None, timeout: float | None
) -> AcquireOutcome:
    # Guard against indexing the WRONG repo: if the checkout's origin no longer
    # matches the configured url (a repo whose URL changed, or a path reused for a
    # different repo), re-point origin to the configured url. A plain ff-pull then
    # either fast-forwards the same repo's new location or cleanly refuses on
    # unrelated history (-> "stale") — instead of silently pulling the old remote
    # and reporting "updated".
    #
    # `git -C` searches parent directories. A checkout whose own .git is
    # damaged, inside another repository, would otherwise re-point and pull
    # the PARENT repository. Only a repository rooted at repo_root is pulled.
    try:
        open_repository_at(repo_root)
    except pygit2.GitError as exc:
        return AcquireOutcome(
            "failed",
            f"{repo_root} is not a git repository rooted there: {exc}"[:500],
        )
    rc_o, origin_url, _ = _run_git_raw(
        ["remote", "get-url", "origin"], cwd=str(repo_root), timeout=timeout
    )
    if rc_o == 0 and _norm_remote(origin_url) != _norm_remote(url):
        _run_git_raw(
            ["remote", "set-url", "origin", _clean_clone_url(url)],
            cwd=str(repo_root),
            timeout=timeout,
        )
    # Never fast-forward a branch other than the one currently checked out: a
    # detached HEAD or a working tree on a different branch than tracked_branch
    # must degrade to "stale" (tree untouched), NOT silently rewrite the current
    # branch onto another branch's history. So run a plain `git pull --ff-only`
    # (which only advances the current branch's own upstream) after verifying the
    # branch, rather than `pull origin <tracked_branch>`.
    rc, current, _ = _run_git_raw(
        ["rev-parse", "--abbrev-ref", "HEAD"], cwd=str(repo_root), timeout=timeout
    )
    current = current.strip()
    if rc != 0 or current == "HEAD":
        return AcquireOutcome("stale", "detached HEAD; not fast-forwarding")
    if tracked_branch and current != tracked_branch:
        return AcquireOutcome(
            "stale",
            f"on branch {current!r}, not tracked {tracked_branch!r}; "
            f"not fast-forwarding",
        )
    returncode, _stdout, stderr = _run_git_raw(
        ["pull", "--ff-only"], cwd=str(repo_root), timeout=timeout
    )
    if returncode == 0:
        return AcquireOutcome("updated")
    # ff-only refuses cleanly on divergence/dirty/no-upstream — the tree is untouched.
    return AcquireOutcome(
        "stale", _scrub_userinfo(stderr.strip())[:500] or f"git pull exit {returncode}"
    )


def clone_or_pull(
    repo_root: Path,
    url: str,
    *,
    tracked_branch: str | None = None,
    timeout: float | None = None,
) -> AcquireOutcome:
    """Materialise ``repo_root`` from ``url``; never raises operationally.

    - No ``.git`` -> ``git clone`` (``--branch`` when ``tracked_branch`` set);
      success -> ``"cloned"``, failure -> ``"failed"``.
    - Existing checkout -> ``git pull --ff-only``; success -> ``"updated"``,
      non-fast-forward -> ``"stale"`` (tree left untouched).

    Credentials come only from the ambient git credential configuration; http(s)
    userinfo in ``url`` is stripped before cloning, so a fresh clone writes no
    token into ``.git/config`` (a pre-existing checkout cloned with a token in its
    origin keeps it until re-pointed). Every git op runs under a finite ``timeout``
    (defaults
    to ``_DEFAULT_ACQUIRE_TIMEOUT``) so a hung network op can't block ingest.
    Concurrent calls on the same ``repo_root`` serialise via a per-path lock.
    """
    repo_root = Path(repo_root)
    if timeout is None:
        timeout = _DEFAULT_ACQUIRE_TIMEOUT
    with _path_lock(repo_root):
        try:
            if (repo_root / ".git").exists():
                return _pull_ff_only(repo_root, url, tracked_branch, timeout)
            return _clone(repo_root, url, tracked_branch, timeout)
        except subprocess.TimeoutExpired:
            logger.warning("clone_or_pull(%s) timed out after %ss", repo_root, timeout)
            return AcquireOutcome("failed", f"git operation timed out after {timeout}s")
        except Exception as exc:  # noqa: BLE001 — acquisition must never raise out
            logger.warning("clone_or_pull(%s) unexpected error: %s", repo_root, exc)
            return AcquireOutcome("failed", str(exc)[:500])


# ---------------------------------------------------------------------------
# Static lookups used by GitFilesSource (kept here because GitHistorySource
# also wants the same language inference for its AFFECTS edges).
# ---------------------------------------------------------------------------
EXTENSION_LANGUAGES: dict[str, str] = {
    ".py": "python",
    ".pyx": "python",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".js": "javascript",
    ".jsx": "javascript",
    ".md": "markdown",
    ".markdown": "markdown",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".sh": "shell",
    ".bash": "shell",
    ".zsh": "shell",
    ".json": "json",
    ".jsonl": "json",
    ".toml": "toml",
    ".rs": "rust",
    ".go": "go",
    ".sql": "sql",
    ".html": "html",
    ".htm": "html",
    ".css": "css",
    ".scss": "scss",
    ".java": "java",
    ".kt": "kotlin",
    ".rb": "ruby",
    ".c": "c",
    ".h": "c",
    ".cpp": "cpp",
    ".hpp": "cpp",
    ".cc": "cpp",
    ".tex": "latex",
    ".dockerfile": "dockerfile",
    ".proto": "protobuf",
}


BINARY_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".svg",
        ".pdf",
        ".bin",
        ".ico",
        ".woff",
        ".woff2",
        ".ttf",
        ".otf",
        ".zip",
        ".tar",
        ".gz",
        ".tgz",
        ".bz2",
        ".xz",
        ".7z",
        ".so",
        ".dylib",
        ".dll",
        ".a",
        ".o",
        ".wasm",
        ".mp3",
        ".mp4",
        ".mov",
        ".avi",
        ".webm",
    }
)


DEFAULT_EXCLUDE_GLOBS: tuple[str, ...] = (
    "*.lock",
    "*.png",
    "*.jpg",
    "*.jpeg",
    "*.gif",
    "*.svg",
    "*.pdf",
    "*.bin",
    "*.ico",
    "*.woff",
    "*.woff2",
    "*.ttf",
    "*.otf",
)


def language_for_path(path: str) -> str | None:
    """Map a path's extension to a language label, or None if unknown."""
    suffix = Path(path).suffix.lower()
    return EXTENSION_LANGUAGES.get(suffix)
