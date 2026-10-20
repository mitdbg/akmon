"""Helpers for bounding Claude Code project-directory scans."""

from __future__ import annotations

import fnmatch
import posixpath
from pathlib import Path
from typing import Any, Iterable


def normalize_claude_path(value: Any) -> str:
    """Normalize a cwd/path before classification or authorization."""
    raw = str(value).replace("\\", "/")
    if raw == "~" or raw.startswith("~/"):
        raw = f"{Path.home()}{raw[1:]}"
    normalized = posixpath.normpath(raw)
    return normalized if normalized == "/" else normalized.rstrip("/")


def encode_claude_project_pattern(value: Any) -> str:
    """Encode a cwd/path glob the way Claude names project directories.

    Claude's encoding is lossy: every non-alphanumeric path character except
    ``-`` becomes ``-``. Glob metacharacters are retained so callers can build
    a bounded *candidate* set. The recorded transcript cwd must still be used
    as the authorization boundary.
    """
    raw = normalize_claude_path(value)
    return "".join(
        char
        if (
            (char.isascii() and char.isalnum())
            or char == "-"
            or char in "*?[]"
        )
        else "-"
        for char in raw
    )


def encode_claude_project_path(value: Any) -> str:
    """Encode one exact cwd/path the way Claude names project directories."""
    raw = normalize_claude_path(value)
    return "".join(
        char
        if ((char.isascii() and char.isalnum()) or char == "-")
        else "-"
        for char in raw
    )


def claude_worktree_project_globs(
    roots: Iterable[Path | str],
) -> tuple[str, ...]:
    """Candidate encoded project directories for allowlisted worktree roots."""
    out: list[str] = []
    for root in roots:
        base = encode_claude_project_path(
            f"{normalize_claude_path(root)}/.claude/worktrees"
        )
        out.extend((base, f"{base}-*"))
    return tuple(dict.fromkeys(out))


def claude_project_scope_globs(
    *,
    cwd_filter: Iterable[Path | str] = (),
    cwd_globs: Iterable[str] = (),
    claude_worktree_roots: Iterable[Path | str] = (),
) -> tuple[str, ...]:
    """Return bounded top-level Claude project-directory candidate globs."""
    out: list[str] = []
    for cwd in cwd_filter:
        encoded = encode_claude_project_path(cwd)
        out.extend((encoded, f"{encoded}-*"))
    for cwd in cwd_globs:
        encoded = encode_claude_project_pattern(cwd)
        out.extend((encoded, f"{encoded}-*"))
    out.extend(claude_worktree_project_globs(claude_worktree_roots))
    return tuple(dict.fromkeys(out))


def claude_transcript_watch_globs(
    *,
    cwd_filter: Iterable[Path | str] = (),
    cwd_globs: Iterable[str] = (),
    claude_worktree_roots: Iterable[Path | str] = (),
) -> tuple[str, ...]:
    """Return watcher globs for parent/subagent transcripts and metadata."""
    project_globs = claude_project_scope_globs(
        cwd_filter=cwd_filter,
        cwd_globs=cwd_globs,
        claude_worktree_roots=claude_worktree_roots,
    )
    out: list[str] = []
    for project_glob in project_globs:
        out.extend(
            (
                f"{project_glob}/*.jsonl",
                f"{project_glob}/*/subagents/*.jsonl",
                f"{project_glob}/*/subagents/*.meta.json",
            )
        )
    return tuple(dict.fromkeys(out))


def is_repo_local_claude_worktree_cwd(cwd: Any) -> bool:
    """True for sessions rooted inside a repo-local `.claude/worktrees` cache."""
    if cwd is None:
        return False
    cwd_norm = normalize_claude_path(cwd)
    return (
        "/.claude/worktrees/" in cwd_norm
        or cwd_norm.endswith("/.claude/worktrees")
    )


def is_repo_local_claude_worktree_project_dir(path: Path) -> bool:
    """True for Claude's encoded project dir for `.claude/worktrees/...` cwd."""
    name = path.name
    return "--claude-worktrees-" in name or name.endswith("--claude-worktrees")


def is_allowed_claude_worktree_cwd(
    cwd: Any,
    roots: Iterable[Path | str],
) -> bool:
    """True only when cwd is under an exact allowlisted worktree parent."""
    if cwd is None:
        return False
    cwd_norm = normalize_claude_path(cwd)
    for root in roots:
        worktrees = f"{normalize_claude_path(root)}/.claude/worktrees"
        if cwd_norm != worktrees and not cwd_norm.startswith(f"{worktrees}/"):
            continue
        try:
            worktrees_resolved = Path(worktrees).expanduser().resolve()
            cwd_resolved = Path(cwd_norm).expanduser().resolve()
        except OSError:
            return False
        if (
            cwd_resolved == worktrees_resolved
            or worktrees_resolved in cwd_resolved.parents
        ):
            return True
    return False


def is_claude_project_dir_candidate(
    path: Path,
    *,
    projects_root: Path | str | None = None,
    cwd_filter: Iterable[Path | str] = (),
    cwd_globs: Iterable[str] = (),
    claude_worktree_roots: Iterable[Path | str] = (),
) -> bool:
    """Bound top-level candidates; transcript cwd remains final authority."""
    cwd_filter = tuple(cwd_filter)
    cwd_globs = tuple(cwd_globs)
    claude_worktree_roots = tuple(claude_worktree_roots)
    if path.is_symlink():
        return False
    if projects_root is not None:
        try:
            root = Path(projects_root).expanduser().resolve()
            resolved = path.resolve()
        except OSError:
            return False
        if resolved.parent != root:
            return False

    if is_repo_local_claude_worktree_project_dir(path):
        patterns = claude_worktree_project_globs(claude_worktree_roots)
    elif cwd_filter or cwd_globs:
        patterns = claude_project_scope_globs(
            cwd_filter=cwd_filter,
            cwd_globs=cwd_globs,
        )
    else:
        # Preserve the library's legacy all-project behavior when callers do
        # not configure any ordinary cwd scope. Repo-local Claude worktrees
        # above remain an explicit, fail-closed opt-in.
        return True
    return any(fnmatch.fnmatchcase(path.name, pattern) for pattern in patterns)
