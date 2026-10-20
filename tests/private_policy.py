"""Read this repository's own PACT and runtime policy files in tests.

``pact/project.yaml``, ``pact/AGENTS.md`` and ``.okg/coordination.yaml`` are
this repository's development policy. They stay out of the public copy: the
mirror export excludes them. A test that checks what one of these files says
reads it through :func:`private_policy_path`.

In this repository the path is returned as is, so a missing file fails the
test loudly. Only in the public copy -- recognised by the file its overlay
ships, ``tests/mirror_deselect.txt``, which does not exist here -- is the
test skipped, with a reason naming the file. Tests that only need *a* valid
policy, not this repository's, build a synthetic one instead of calling this.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def is_public_copy(root: Path = REPO_ROOT) -> bool:
    """True in the exported public copy, which ships the deselect list."""

    return (root / "tests" / "mirror_deselect.txt").is_file()


def private_policy_path(relative: str, *, root: Path = REPO_ROOT) -> Path:
    """Return ``root / relative``; skip only when the public copy lacks it."""

    path = root / relative
    if not path.exists() and is_public_copy(root):
        pytest.skip(
            f"{relative} is this repository's own policy and does not ship "
            "in the public copy"
        )
    return path
