"""The per-commit libgit2 walks that `lane-h-git-log-walks` replaced.

Kept only as a reference for the equivalence tests: production reads the
per-commit changed files from one streamed ``git log`` instead. These are the
pre-change bodies of ``in_proc_last_modified`` and ``in_proc_walk_commits``
(dev 535e261fac), unchanged except for their names. ``reference_path_history``
is the per-commit libgit2 form of ``in_proc_path_history`` (issue #3468),
which replaced ``in_proc_last_modified``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Iterator

import pygit2

from okg.substrate.library.sources._git_helpers import (
    MAX_PATCH_BLOB_BYTES,
    CommitRecord,
    FilePatch,
    PathHistory,
    _commit_name_status,
    _diff_patches,
    _first_parent_diff,
    _iso_from_offset,
    _r,
    _walk_tree_paths,
    split_commit_message,
)


def reference_last_modified(
    repo_root: Path,
    paths: Iterable[str],
    *,
    head_sha: str | None = None,
) -> dict[str, tuple[str, str]]:
    repo = _r(repo_root)
    target = set(paths)
    if not target:
        return {}
    result: dict[str, tuple[str, str]] = {}
    walker = repo.walk(
        head_sha if head_sha is not None else repo.head.target,
        pygit2.GIT_SORT_TOPOLOGICAL,
    )
    for commit in walker:
        if not target:
            break
        parents = commit.parents
        if parents:
            diff = parents[0].tree.diff_to_tree(commit.tree)
            paths_touched = {d.new_file.path for d in diff.deltas} | {
                d.old_file.path for d in diff.deltas
            }
        else:
            paths_touched = _walk_tree_paths(repo, commit.tree)
        iso = _iso_from_offset(commit.commit_time, commit.commit_time_offset)
        for p in paths_touched & target:
            if p not in result:
                result[p] = (str(commit.id), iso)
        target -= set(result.keys())
    return result


def reference_path_history(
    repo_root: Path,
    paths: Iterable[str],
    *,
    head_sha: str | None = None,
) -> dict[str, PathHistory]:
    """``in_proc_path_history`` with a libgit2 diff per commit."""
    repo = _r(repo_root)
    target = set(paths)
    if not target:
        return {}
    walker = repo.walk(
        head_sha if head_sha is not None else repo.head.target,
        pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_TIME,
    )
    newest: dict[bool, dict[str, tuple[str, str]]] = {False: {}, True: {}}
    oldest: dict[bool, dict[str, tuple[str, str]]] = {False: {}, True: {}}
    for commit in walker:
        is_merge = len(commit.parents) >= 2
        if commit.parents:
            diff = commit.parents[0].tree.diff_to_tree(commit.tree)
            touched = {
                d.new_file.path
                for d in diff.deltas
                if d.status != pygit2.enums.DeltaStatus.DELETED
            }
            added = {
                d.new_file.path
                for d in diff.deltas
                if d.status == pygit2.enums.DeltaStatus.ADDED
            }
        else:
            touched = added = _walk_tree_paths(repo, commit.tree)
        sha = str(commit.id)
        for p in touched & target:
            newest[is_merge].setdefault(
                p, (sha, _iso_from_offset(commit.commit_time, commit.commit_time_offset))
            )
        for p in added & target:
            oldest[is_merge][p] = (
                sha,
                _iso_from_offset(commit.author.time, commit.author.offset),
            )
    result: dict[str, PathHistory] = {}
    for p in target:
        modified = newest[False].get(p) or newest[True].get(p)
        if modified is None:
            continue
        first = oldest[False].get(p) or oldest[True].get(p)
        result[p] = PathHistory(
            *modified, *(first if first else (None, None))
        )
    return result


def reference_walk_commits(
    repo_root: Path,
    positive_shas: Iterable[str],
    negative_shas: Iterable[str] = (),
    *,
    detect_renames: bool = False,
    with_patches: bool = False,
    patches_since: int | None = None,
    max_patch_blob_bytes: int = MAX_PATCH_BLOB_BYTES,
) -> Iterator[CommitRecord]:
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
            pass
    for commit in walker:
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
