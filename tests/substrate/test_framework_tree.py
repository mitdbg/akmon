"""okg finds its source tree from OKG_FRAMEWORK_TREE when installed from a wheel.

In the runtime image okg is a wheel in /opt/okg/.venv, and a fixed
`Path(__file__).parents[N]` walk lands in the environment's lib/ directory.
The Nomos enforce-mode DSN scan and the environment descriptor root would
then silently lose okg's deployments/ and ops/environments/.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate import framework_tree
from okg.substrate.deployments import environment
from okg.substrate.framework_tree import (
    FrameworkTreeUnavailable,
    framework_tree_root,
)
from okg.substrate.nomos import rollout

REPO_ROOT = Path(__file__).resolve().parents[2]


def _tree(tmp_path: Path) -> Path:
    root = tmp_path / "opt-okg"
    (root / "src" / "okg").mkdir(parents=True)
    (root / "deployments").mkdir()
    (root / "ops" / "environments").mkdir(parents=True)
    return root


def test_the_declared_tree_wins(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _tree(tmp_path)
    monkeypatch.setenv("OKG_FRAMEWORK_TREE", str(root))
    assert framework_tree_root() == root.resolve()
    monkeypatch.delenv("OKG_ENVIRONMENTS_DIR", raising=False)
    assert environment.default_environments_dir() == root.resolve() / "ops" / "environments"


@pytest.mark.parametrize("make", ["missing", "not-a-tree"])
def test_a_declared_tree_that_is_not_there_refuses(
    make: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "nowhere"
    if make == "not-a-tree":
        target.mkdir()
    monkeypatch.setenv("OKG_FRAMEWORK_TREE", str(target))
    with pytest.raises(FrameworkTreeUnavailable):
        framework_tree_root()
    monkeypatch.delenv("OKG_ENVIRONMENTS_DIR", raising=False)
    with pytest.raises(FrameworkTreeUnavailable):
        environment.default_environments_dir()
    with pytest.raises(FrameworkTreeUnavailable):
        rollout.runtime_enforce_mode_dsn_keys(tmp_path / "dep" / "x")


def test_a_source_checkout_is_its_own_tree(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OKG_FRAMEWORK_TREE", raising=False)
    assert framework_tree_root() == REPO_ROOT


def test_a_wheel_install_without_a_declared_tree_has_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg

    site = tmp_path / ".venv" / "lib" / "python3.12" / "site-packages"
    (site / "okg").mkdir(parents=True)
    monkeypatch.delenv("OKG_FRAMEWORK_TREE", raising=False)
    monkeypatch.setattr(okg, "__file__", str(site / "okg" / "__init__.py"))
    assert framework_tree_root() is None


def test_the_enforce_mode_scan_includes_the_declared_trees_deployments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The image's baked /opt/okg/deployments must stay in the privacy scan."""
    root = _tree(tmp_path)
    scanned: list[Path] = []

    def scan(*roots: Path, strict: bool) -> tuple[dict[str, str], bool]:
        scanned.extend(roots)
        return {}, True

    monkeypatch.setattr(rollout, "_scan_enforce_mode_dsn_keys", scan)
    monkeypatch.setattr(framework_tree, "framework_tree_root", lambda: root)
    assert rollout.runtime_enforce_mode_dsn_keys(tmp_path / "dep" / "x") == {}
    assert root / "deployments" in scanned
