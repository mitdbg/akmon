"""Let a test keep checking shipped files in the public copy of this repository.

The public-mirror export leaves out this repository's private material: its
own deployments, change records, agent instructions and CI tooling. A test
whose subject is product code but whose inputs mix shipped and private files
uses these helpers so the shipped part still runs there.

In this repository every helper is the identity (or a no-op), so a missing
file still fails its test loudly. Only in the public copy -- recognised by
``tests/mirror_deselect.txt``, which its overlay ships and this repository
does not have -- is an absent path dropped or its check skipped, and only
when it lies under one of :data:`PRIVATE_ROOTS`. A shipped page that was
deleted still fails there too. ``tests/scripts/test_public_mirror_export.py``
checks that the export's allowlist excludes every root listed here.

Some tests also branch on :func:`is_public_copy` directly with an expectation
written for the public copy -- a smaller count (deployment manifests that
load, TLC launch sites), a shorter expected list (in-repo deployments,
smoke-source deployments, declared themes, persona seeds), or entries of a
list that are skipped when private and absent. Those expectations are
hard-coded, not derived from the export's allowlist; each sits next to a
comment naming what the public copy leaves out.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import TypeVar

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Repository-relative roots the public copy leaves out. A directory ends in "/".
PRIVATE_ROOTS: tuple[str, ...] = (
    "deployments/okg-workspace/",
    "deployments/okg-ci/",
    "deployments/okg-ci-dev/",
    "ops/environments/",
    "tests/okg-workspace/",
    "tests/ci/",
    "tests/docs/ledgers/",
    "tests/pact/test_v5_migration.py",
    "tests/substrate/contracts/test_external_distribution_conformance_contract.py",
    "pact/changes/",
    "docs/runbooks/agents/",
    "docs/runbooks/quality/",
    "docs/runbooks/ci/",
    "scripts/ci/",
    "src/okg/bench/agent_memory/",
    "bench/agent_memory/",
    "experiments/",
    ".github/",
    ".agents/",
    ".claude/",
    "tests/private_deployments.yaml",
    "tests/substrate/fixtures/deployment_manifest_golden_private.json",
    "CLAUDE.md",
    "AGENTS.md",
    "src/okg/AGENTS.md",
    # Native coordination's formal models refine the private workspace
    # deployment's coordination code, so they and their checkers stay out.
    "formal/coordination/",
    "scripts/check_formal_coordination_protocols.py",
    "scripts/check_formal_native_coordination.py",
)

T = TypeVar("T", str, Path)


def is_public_copy(root: Path = REPO_ROOT) -> bool:
    """True in the exported public copy, which ships the deselect list."""

    return (root / "tests" / "mirror_deselect.txt").is_file()


def _relative(path: str | Path, root: Path) -> str:
    candidate = Path(path)
    if candidate.is_absolute():
        try:
            candidate = candidate.relative_to(root)
        except ValueError:
            return candidate.as_posix()
    return candidate.as_posix()


def is_private(path: str | Path, *, root: Path = REPO_ROOT) -> bool:
    """True when ``path`` lies under one of :data:`PRIVATE_ROOTS`."""

    rel = _relative(path, root).rstrip("/")
    return any(
        rel == p.rstrip("/") or (p.endswith("/") and rel.startswith(p)) for p in PRIVATE_ROOTS
    )


def _absent_private(path: str | Path, root: Path) -> bool:
    full = Path(path) if Path(path).is_absolute() else root / path
    return not full.exists() and is_private(path, root=root)


def shipped(paths: Iterable[T], *, root: Path = REPO_ROOT) -> list[T]:
    """Return ``paths``; in the public copy, without the absent private ones.

    Paths may be absolute or relative to ``root``. The result is never
    empty: a filter that removed everything would let the caller's
    assertions pass over nothing.
    """

    items = list(paths)
    if not is_public_copy(root):
        return items
    kept = [p for p in items if not _absent_private(p, root)]
    assert kept, f"none of {items!r} ships in the public copy; the check would be vacuous"
    return kept


def private_path(relative: str, *, root: Path = REPO_ROOT) -> Path:
    """Return ``root / relative``; in the public copy, skip when it is absent and private."""

    if is_public_copy(root) and _absent_private(relative, root):
        pytest.skip(f"{relative} is private to the development repository and does not ship")
    return root / relative


def skip_in_public_copy(reason: str, *, root: Path = REPO_ROOT) -> None:
    """Skip the calling test in the public copy only, naming what it needs."""

    if is_public_copy(root):
        pytest.skip(f"needs {reason}, which does not ship in the public copy")
