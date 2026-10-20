"""tests/mirror_scope.py: identity here, a narrow filter only in the public copy."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests import mirror_scope
from tests.mirror_scope import is_private, is_public_copy, private_path, shipped


def _public_copy(tmp_path: Path) -> Path:
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "mirror_deselect.txt").write_text("", encoding="utf-8")
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "page.md").write_text("# Page\n", encoding="utf-8")
    return tmp_path


def test_the_public_copy_is_recognised_by_its_deselect_list(tmp_path: Path) -> None:
    assert not is_public_copy(tmp_path)
    assert is_public_copy(_public_copy(tmp_path))


def test_private_roots_match_files_and_bare_directories() -> None:
    assert is_private("deployments/okg-workspace")
    assert is_private("deployments/okg-workspace/")
    assert is_private("deployments/okg-workspace/deployment.yaml")
    assert is_private("CLAUDE.md")
    assert not is_private("deployments/okg-workspace-demo/deployment.yaml")
    assert not is_private("docs/runbooks/workspace-graph/setup.md")


def test_helpers_are_the_identity_outside_the_public_copy(tmp_path: Path) -> None:
    missing = ["deployments/okg-workspace/README.md", "docs/gone.md"]
    assert shipped(missing, root=tmp_path) == missing
    assert private_path("deployments/okg-workspace/README.md", root=tmp_path) == (
        tmp_path / "deployments/okg-workspace/README.md"
    )


def test_public_copy_drops_only_absent_private_paths(tmp_path: Path) -> None:
    root = _public_copy(tmp_path)
    kept = shipped(
        ["docs/page.md", "deployments/okg-workspace/README.md", "docs/deleted.md"], root=root
    )
    # A deleted shipped page is kept, so the caller's read still fails loudly.
    assert kept == ["docs/page.md", "docs/deleted.md"]


def test_public_copy_refuses_a_filter_that_removes_everything(tmp_path: Path) -> None:
    root = _public_copy(tmp_path)
    with pytest.raises(AssertionError, match="vacuous"):
        shipped(["deployments/okg-workspace/README.md"], root=root)


def test_public_copy_skips_an_absent_private_path_with_its_name(tmp_path: Path) -> None:
    root = _public_copy(tmp_path)
    with pytest.raises(pytest.skip.Exception, match="deployments/okg-workspace"):
        private_path("deployments/okg-workspace", root=root)
    # A shipped path that is missing is not skipped.
    assert private_path("docs/gone.md", root=root) == root / "docs/gone.md"


def test_the_development_checkout_has_no_stray_public_copy_marker() -> None:
    """A stray tests/mirror_deselect.txt here would switch every helper to mirror mode.

    The public copy is told apart independently of that marker: it never
    contains scripts/release/ (the export's private files), and there this
    test is skipped.
    """
    root = mirror_scope.REPO_ROOT
    if not (root / "scripts" / "release" / "export_public_mirror.py").is_file():
        pytest.skip("public copy: it ships tests/mirror_deselect.txt on purpose")
    assert not (root / "tests" / "mirror_deselect.txt").exists()
    assert not is_public_copy()


def test_private_roots_is_a_tuple_of_relative_paths() -> None:
    assert all(not p.startswith("/") for p in mirror_scope.PRIVATE_ROOTS)
