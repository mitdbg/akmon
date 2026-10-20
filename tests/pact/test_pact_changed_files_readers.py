"""The shipped change-set readers agree on every shape of change.

The product half of tests/pact/test_changed_files_agreement.py, which also
checks the CI scripts under scripts/ci/ and so does not ship in the public
copy. Same history, same cases: a rename, a delete, a copy, a pull request
that merged dev, a push with no pull request, and non-ASCII and
newline-bearing file names, read by ``pact.git``, ``scope_drift`` and the
retired v4 ``proof_identity``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from okg.substrate.pact import proof_identity, scope_drift
from pact import git as pact_git
from tests.pact._changed_files_history import CASES, build_history, product_readers


@pytest.fixture(scope="module")
def history(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return build_history(tmp_path_factory.mktemp("product-changed-files") / "work")


@pytest.mark.parametrize(
    ("case", "base_key", "head_key", "expected"), CASES, ids=[case[0] for case in CASES]
)
def test_every_product_reader_agrees(
    history: dict[str, Any], case: str, base_key: str, head_key: str, expected: set[str]
) -> None:
    root = history["root"]
    base, head = history[base_key], history[head_key]
    readers = product_readers(root)
    if head_key == "odd":
        # proof_identity refuses a control-character path outright (it binds
        # proof to exact file names): failing closed, not disagreeing.
        with pytest.raises(proof_identity.ProofIdentityError, match="control-character"):
            readers.pop("proof_identity._changed_paths")(base, head)
    answers = {name: read(base, head) for name, read in readers.items()}
    assert len(answers) >= 3, "the table must compare several readers"
    wrong = {name: sorted(paths) for name, paths in answers.items() if paths != expected}
    assert not wrong, f"{case}: expected {sorted(expected)}, disagreeing readers: {wrong}"


def test_an_unknown_base_fails_closed_in_the_product_readers(history: dict[str, Any]) -> None:
    """A range git cannot resolve is never read as "nothing changed"."""
    root: Path = history["root"]
    missing = "0" * 40
    head = history["head"]
    with pytest.raises(pact_git.DiffUnavailable, match="exited 128"):
        pact_git.diff_records(root, f"{missing}..{head}")
    assert pact_git.changed_paths(root, missing, head) is None
    # scope_drift: an unresolvable base branch is reported, never drift-clean.
    unresolved = scope_drift.changed_paths(root, base_branch="no-such-branch")
    assert unresolved["base_ref"] is None and unresolved["git_available"] is True
