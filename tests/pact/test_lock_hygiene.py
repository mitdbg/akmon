"""Regression tests for PACT manifest-lock hygiene.

A mutate-existing operation on a NONEXISTENT change must raise a clean
'PACT change not found' error WITHOUT fabricating a phantom
`pact/changes/<id>/` directory or a stray `pact.yaml.lock` inside it.

Before the fix, `manifest_write_lock` unconditionally `mkdir`-ed the
change directory and opened the lock file BEFORE the in-lock
`load_manifest` discovered the change didn't exist — so a bad change_id
(e.g. from a /console/pact-review POST) left a phantom directory behind.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import operations as ops
from okg.substrate.pact.store import change_dir, manifest_write_lock

from tests.pact.test_cli import _seed_root


def test_approve_intent_on_nonexistent_change_creates_no_dir(
    tmp_path: Path,
) -> None:
    """The exact repro: approving a change that doesn't exist 4xx's
    cleanly and leaves no phantom directory / lock behind."""
    root = _seed_root(tmp_path)
    ghost = "this-change-does-not-exist"

    with pytest.raises(SubstrateError) as excinfo:
        ops.approve_intent(root, ghost, reviewer="reviewer@example.com")
    assert "PACT change not found" in str(excinfo.value)

    # Nothing fabricated: no directory, hence no pact.yaml.lock.
    assert not change_dir(root, ghost).exists()


def test_mutate_existing_op_on_nonexistent_change_creates_no_dir(
    tmp_path: Path,
) -> None:
    """The fix is systemic to mutate-existing ops, not just approve —
    advance_change goes through the same lock and must also create
    nothing for a missing change."""
    root = _seed_root(tmp_path)
    ghost = "no-such-change"

    with pytest.raises(SubstrateError) as excinfo:
        ops.advance_change(root, ghost, status="implementation")
    assert "PACT change not found" in str(excinfo.value)
    assert not change_dir(root, ghost).exists()


def test_create_change_still_creates_its_directory(tmp_path: Path) -> None:
    """The create path is preserved: create_change opts out of the
    existence guard (must_exist=False) so it can bring a change into
    being."""
    root = _seed_root(tmp_path)
    manifest = ops.create_change(root, "real-change", template="feature")
    assert manifest.change.id == "real-change"
    assert change_dir(root, "real-change").exists()


def test_lock_must_exist_false_allows_creation(tmp_path: Path) -> None:
    """manifest_write_lock(must_exist=False) creates the change dir (the
    contract create_change / migrate_openspec rely on); the default
    (must_exist=True) refuses to."""
    root = _seed_root(tmp_path)

    # default refuses + creates nothing
    with pytest.raises(SubstrateError):
        with manifest_write_lock(root, "ghost-default"):
            pass
    assert not change_dir(root, "ghost-default").exists()

    # opt-out creates the directory
    with manifest_write_lock(root, "born-here", must_exist=False):
        pass
    assert change_dir(root, "born-here").exists()
