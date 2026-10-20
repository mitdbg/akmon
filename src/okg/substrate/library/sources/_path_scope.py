"""Shared path-scope helpers for file-backed source adapters."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Mapping, Sequence


def path_scope_keys(
    sync_scope: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    """Return non-empty advisory path keys, deduped in input order."""
    seen: set[str] = set()
    out: list[str] = []
    for raw in (sync_scope or {}).get("scope_keys", ()):
        if raw is None:
            continue
        key = str(raw).replace("\\", "/")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return tuple(out)


def path_scope_requested(sync_scope: Mapping[str, Any] | None) -> bool:
    """Whether the caller supplied at least one path scope key."""
    return bool(path_scope_keys(sync_scope))


def resolve_scoped_paths(
    sync_scope: Mapping[str, Any] | None,
    *,
    roots: Sequence[Path | str],
    suffixes: Sequence[str] | None = None,
    existing_only: bool = True,
) -> tuple[Path, ...]:
    """Resolve advisory path scope keys under allowlisted roots.

    Invalid keys, ``..`` escapes, wrong suffixes, and missing files are
    ignored. Absolute keys are accepted only when they resolve inside one of
    the supplied roots.
    """
    resolved_roots = tuple(_resolve_root(root) for root in roots)
    suffix_filter = (
        {suffix.lower() for suffix in suffixes}
        if suffixes is not None else None
    )
    seen: set[Path] = set()
    out: list[Path] = []

    for key in path_scope_keys(sync_scope):
        raw_path = Path(key).expanduser()
        candidates = (
            (raw_path.resolve(),)
            if raw_path.is_absolute()
            else tuple(
                (root / key.lstrip("/")).resolve()
                for root in resolved_roots
            )
        )
        for candidate in candidates:
            if not _within_any_root(candidate, resolved_roots):
                continue
            if existing_only and not candidate.is_file():
                continue
            if (
                suffix_filter is not None
                and candidate.suffix.lower() not in suffix_filter
            ):
                continue
            if candidate in seen:
                continue
            seen.add(candidate)
            out.append(candidate)

    return tuple(out)


def glob_matches_path(rel_path: str | Path, pattern: str) -> bool:
    """Match a normalized relative path against a recursive glob pattern."""
    rel = str(rel_path).replace("\\", "/")
    if pattern in ("**/*", "**", "*"):
        return True
    return bool(_glob_to_regex(pattern).match(rel))


def _glob_to_regex(pattern: str) -> "re.Pattern[str]":
    out = ["^"]
    i = 0
    while i < len(pattern):
        char = pattern[i]
        if char == "*":
            if pattern[i:i + 2] == "**":
                if pattern[i + 2:i + 3] == "/":
                    out.append(r"(?:.*/)?")
                    i += 3
                    continue
                out.append(r".*")
                i += 2
                continue
            out.append(r"[^/]*")
            i += 1
            continue
        if char == "?":
            out.append(r"[^/]")
            i += 1
            continue
        out.append(re.escape(char))
        i += 1
    out.append("$")
    return re.compile("".join(out))


def _resolve_root(root: Path | str) -> Path:
    return Path(root).expanduser().resolve()


def _within_any_root(path: Path, roots: Sequence[Path]) -> bool:
    for root in roots:
        try:
            path.relative_to(root)
        except ValueError:
            continue
        return True
    return False


__all__ = [
    "glob_matches_path",
    "path_scope_keys",
    "path_scope_requested",
    "resolve_scoped_paths",
]
