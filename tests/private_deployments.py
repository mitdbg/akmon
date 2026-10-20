"""Name this repository's own deployments without writing their names here.

A few product tests also check deployments that exist only in this
repository. Those names must not appear in the public copy: the public-mirror
export leaves out ``tests/private_deployments.yaml``, which holds them, and
its scan refuses the names anywhere else. The tests ask for a deployment by
role through these helpers instead.

In this repository the file must exist and name every role a test asks for,
so a missing file or role fails that test loudly. In the public copy
(:func:`tests.mirror_scope.is_public_copy`) the file is absent by design:
:func:`names` returns no names, so a parameter list loses only its private
cases, and :func:`name` skips the calling test.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tests.mirror_scope import REPO_ROOT, is_public_copy

#: Repository-relative path of the role-to-name map. The export excludes it.
NAMES_FILE = "tests/private_deployments.yaml"


def _load(root: Path) -> dict[str, str] | None:
    path = root / NAMES_FILE
    if not path.is_file():
        if is_public_copy(root):
            return None
        raise FileNotFoundError(f"{NAMES_FILE} is missing; it names this repository's private deployments")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not all(
        isinstance(key, str) and isinstance(value, str) and value for key, value in data.items()
    ):
        raise ValueError(f"{NAMES_FILE} must map each role to a non-empty name")
    return data


def names(*roles: str, root: Path = REPO_ROOT) -> list[str]:
    """Return the names for ``roles``, in order; in the public copy, none."""

    data = _load(root)
    if data is None:
        return []
    missing = [role for role in roles if role not in data]
    if missing:
        raise KeyError(f"{NAMES_FILE} has no entry for {missing}")
    return [data[role] for role in roles]


#: Why a check of a private deployment does not run in the public copy.
SKIP_REASON = "checks a deployment of the development repository, which does not ship"


def name(role: str, *, root: Path = REPO_ROOT) -> str:
    """Return the name for ``role``; in the public copy, skip the calling test."""

    if _load(root) is None:
        pytest.skip(SKIP_REASON)
    return names(role, root=root)[0]


def name_params(*roles: str, root: Path = REPO_ROOT) -> list:
    """Parameters for a test that checks only private deployments.

    Here, the names for ``roles`` (so the test ids are the names). In the public
    copy, one parameter marked skip with :data:`SKIP_REASON`, so the report
    says why instead of pytest's generic "got empty parameter set".
    """

    if _load(root) is None:
        return [pytest.param(None, marks=pytest.mark.skip(reason=SKIP_REASON), id="private-deployment")]
    return names(*roles, root=root)
