"""Regression guard for Workspace Graph deployment naming."""
from __future__ import annotations

from pathlib import Path

from tests.mirror_scope import is_private, is_public_copy


REPO_ROOT = Path(__file__).resolve().parents[2]
RETIRED_DASH = "local" + "-project"
RETIRED_UNDERSCORE = "local" + "_project"
RETIRED_TITLE = "Local" + " Project"
RETIRED_PHRASE = "local" + " project"

SCAN_ROOTS = [
    "AGENTS.md",
    "CLAUDE.md",
    "Makefile",
    "README.md",
    "bench",
    "bin",
    "deployments",
    "docs",
    "history/openspec/AGENTS.md",
    "history/openspec/project.md",
    "history/openspec/specs",
    "history/openspec/templates",
    "ops",
    "pact/AGENTS.md",
    "pyproject.toml",
    "scripts",
    "src",
    "tests",
]

SKIP_PART_PREFIXES = {
    ("docs", "reports"),
    ("history", "openspec", "changes"),
    ("pact", "changes"),
    ("history", "openspec", "changes", "archive"),
    ("deployments", "okg-workspace", "self_play", "reports"),
    ("deployments", "okg-workspace", "workflow_mining"),
    ("deployments", "okg-workspace", "supervisor_console", "qa"),
    ("deployments", "okg-workspace", "supervisor_console", "node_modules"),
}

SKIP_PART_NAMES = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "dist",
    "node_modules",
}

SKIP_SUFFIXES = {
    ".gif",
    ".ico",
    ".jpeg",
    ".jpg",
    ".lock",
    ".map",
    ".pdf",
    ".png",
    ".pyc",
    ".webp",
    ".zip",
}

# Deliberate foreign-identity fixtures and exact historical audit records:
# files that MUST embed the retired identity because it is either the
# *foreign* value under test or a revision-pinned Git path, not the current
# surface's own identity. The packet fault-isolation regression
# (harden-producer-emission-coherence) proves okg-workspace's source skips a
# foreign deployment's retired-format coordination packet loudly; rewriting the
# foreign format string would defeat the test. The repository fallback census
# likewise records every audited path exactly and is not an executable surface.
# Keyed by repo-relative path.
SKIP_RELPATHS = {
    ("tests", "docs", "ledgers", "repository-fallback-findings.yaml"): (
        "revision-pinned census records exact historical Git paths"
    ),
    ("tests", "okg-workspace", "test_coordination_packet_fault_isolation.py"): (
        "foreign-identity fixture proves retired packet rejection"
    ),
}


def _has_prefix(parts: tuple[str, ...], prefix: tuple[str, ...]) -> bool:
    return parts[:len(prefix)] == prefix


def _iter_scanned_files() -> list[Path]:
    files: list[Path] = []
    for root in SCAN_ROOTS:
        path = REPO_ROOT / root
        if not path.exists():
            continue
        if path.is_file():
            candidates = [path]
        else:
            candidates = [p for p in path.rglob("*") if p.is_file()]
        for candidate in candidates:
            rel = candidate.relative_to(REPO_ROOT)
            if candidate.suffix.lower() in SKIP_SUFFIXES:
                continue
            if any(part in SKIP_PART_NAMES for part in rel.parts):
                continue
            if any(_has_prefix(rel.parts, skip) for skip in SKIP_PART_PREFIXES):
                continue
            if rel.parts in SKIP_RELPATHS:
                continue
            files.append(candidate)
    return sorted(files)


def test_current_surfaces_do_not_use_retired_workspace_identity() -> None:
    offenders: list[str] = []
    terms = (
        RETIRED_DASH,
        RETIRED_UNDERSCORE,
        RETIRED_TITLE,
        RETIRED_PHRASE,
    )
    for path in _iter_scanned_files():
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            if any(term in line for term in terms):
                offenders.append(
                    f"{path.relative_to(REPO_ROOT)}:{lineno}: {line.strip()}",
                )

    assert offenders == []


def test_skipped_identity_records_are_earned() -> None:
    terms = (
        RETIRED_DASH,
        RETIRED_UNDERSCORE,
        RETIRED_TITLE,
        RETIRED_PHRASE,
    )
    stale: list[str] = []
    for parts, reason in SKIP_RELPATHS.items():
        path = REPO_ROOT.joinpath(*parts)
        if is_public_copy() and is_private("/".join(parts)) and not path.exists():
            # A private file the public copy leaves out (tests/mirror_scope.py).
            continue
        if not reason.strip() or not path.is_file():
            stale.append("/".join(parts))
            continue
        text = path.read_text(encoding="utf-8")
        if not any(term in text for term in terms):
            stale.append("/".join(parts))

    assert stale == [], f"unearned retired-identity exemptions: {stale}"
