"""Shared discovery heuristics — common patterns multiple
sources reuse.

Per `improve-add-source-operator-experience`: each source
owns its own `discover()` classmethod, but they often share
the same idioms ("find dirs containing N+ files of certain
suffixes, rank by count + recency"). Factor those out here.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Iterable

from .candidate import Candidate, Confidence
from .env import DiscoveryEnv


# Common locations the academic-researcher profile expects to find
# operator-provided content. Sources pick the subset relevant
# to them. Ordered by likelihood (operator's primary content
# directory first).
COMMON_NOTES_ROOTS = (
    "Documents/notes",
    "Documents/Notes",
    "notes",
    "Notes",
    "reviewer",
    "reviewing",
    "Reviews",
    "Teaching",
    "teaching",
    "Documents/Teaching",
    "Documents/talks",
    "talks",
    "Talks",
    ".research-notes",
    "zettelkasten",
    "Zettelkasten",
    ".obsidian",
    "Documents/Obsidian Vault",
    "Obsidian",
)

COMMON_PAPER_ROOTS = (
    "Documents/Papers",
    "Documents/papers",
    "Papers",
    "papers",
    "Zotero",
    "Mendeley",
    "Documents/Mendeley",
)

COMMON_CV_PATHS = (
    "cv.pdf",
    "cv.tex",
    "CV.pdf",
    "CV.tex",
    "Documents/cv.pdf",
    "Documents/CV.pdf",
    "Documents/cv.tex",
    "Documents/CV/cv.pdf",
    "Documents/CV/cv.tex",
)

COMMON_BIB_ROOTS = (
    "Documents",
    "Documents/References",
    "References",
    "Documents/bib",
    "bib",
)


def find_dirs_with_content(
    env: DiscoveryEnv,
    *,
    candidate_roots: Iterable[str],
    suffixes: tuple[str, ...],
    min_files: int = 1,
) -> list[tuple[Path, int, dict[str, int]]]:
    """For each candidate dir under `home_dir`, count how
    many files of the given suffixes it contains (recursive
    bounded scan). Return only those with ≥ min_files.

    Result entries: `(absolute_path, total_file_count,
    {suffix: count})`. Sorted by total_file_count desc.
    """
    hits: list[tuple[Path, int, dict[str, int]]] = []
    suffix_set = tuple(s.lower() for s in suffixes)
    for rel in candidate_roots:
        abs_path = env.home_dir / rel
        if not env.dir_exists(abs_path):
            continue
        files = env.list_files(abs_path, suffixes=suffix_set)
        if len(files) < min_files:
            continue
        by_suffix: dict[str, int] = {}
        for f in files:
            by_suffix[f.suffix.lower()] = by_suffix.get(f.suffix.lower(), 0) + 1
        hits.append((abs_path, len(files), by_suffix))
    hits.sort(key=lambda h: -h[1])
    return hits


def score_dir_confidence(
    *,
    file_count: int,
    high_threshold: int = 10,
    medium_threshold: int = 3,
) -> str:
    """Default confidence-from-file-count heuristic. Sources
    override with their own thresholds when appropriate."""
    if file_count >= high_threshold:
        return Confidence.HIGH
    if file_count >= medium_threshold:
        return Confidence.MEDIUM
    return Confidence.LOW


def recent_mtime_evidence(env: DiscoveryEnv, root: Path) -> str:
    """Return a one-phrase recency descriptor for `root`'s
    contents. E.g. "modified within 7 days" / "last modified
    3 months ago" / "stale (>1 year)". Used to flesh out a
    Candidate's evidence string."""
    try:
        mtimes = [
            f.stat().st_mtime
            for f in env.list_files(root, max_files=50)
        ]
    except (OSError, PermissionError):
        return ""
    if not mtimes:
        return ""
    youngest = max(mtimes)
    age_days = (time.time() - youngest) / 86400
    if age_days < 7:
        return "modified within 7 days"
    if age_days < 30:
        return f"modified within {int(age_days)} days"
    if age_days < 365:
        return f"last modified {int(age_days / 30)} months ago"
    return "stale (>1 year)"


def make_path_candidate(
    *,
    param_key: str,
    path: Path,
    file_count: int,
    by_suffix: dict[str, int],
    confidence: str,
    source_label: str,
    env: DiscoveryEnv,
) -> Candidate:
    """Build a Candidate for a path-based source with a
    standard evidence + summary shape."""
    recency = recent_mtime_evidence(env, path)
    ext_summary = ", ".join(
        f"{n} {ext.lstrip('.') or 'file'}"
        for ext, n in sorted(by_suffix.items(), key=lambda kv: -kv[1])[:3]
    )
    evidence = ext_summary
    if recency:
        evidence = f"{ext_summary}; {recency}"
    summary = f"{ext_summary} at ~/{path.relative_to(env.home_dir)}"
    if len(summary) > 80:
        summary = summary[:77] + "..."
    return Candidate(
        params={param_key: str(path)},
        path=str(path),
        confidence=confidence,
        evidence=evidence,
        summary=summary,
        extra={
            "file_count": file_count,
            "extensions": by_suffix,
            "recency": recency,
        },
    )
