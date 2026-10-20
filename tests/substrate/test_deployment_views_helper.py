"""The deployment ``views`` import helper wins over a foreign cached package
and restores ``sys.path`` / ``sys.modules`` exactly, also on failure."""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from tests.substrate._deployment_views import isolated_deployment_views

REPO = Path(__file__).resolve().parents[2]
WORKSPACE_DIR = REPO / "deployments" / "okg-workspace"


def _scaffold_foreign_views(root: Path) -> Path:
    """Mimic a scaffolded test deployment: ``<root>/deployment/views``."""
    deployment = root / "deployment"
    (deployment / "views").mkdir(parents=True)
    (deployment / "views" / "__init__.py").write_text("", encoding="utf-8")
    (deployment / "views" / "dummy.py").write_text("X = 1\n", encoding="utf-8")
    return deployment


@pytest.fixture
def foreign_views_cached(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Leave a foreign ``views`` cached and its dir first on ``sys.path``,
    the state test_deployment_stack.py leaves behind on an xdist worker."""
    foreign = _scaffold_foreign_views(tmp_path)
    monkeypatch.syspath_prepend(str(foreign))
    for name in [n for n in sys.modules if n == "views" or n.startswith("views.")]:
        monkeypatch.delitem(sys.modules, name)
    importlib.import_module("views.dummy")
    yield foreign
    for name in [n for n in sys.modules if n == "views" or n.startswith("views.")]:
        sys.modules.pop(name, None)


def test_foreign_cached_views_shadows_a_bare_import(foreign_views_cached: Path) -> None:
    # The failure mode the helper exists for: the cached package wins.
    with pytest.raises(ModuleNotFoundError, match="views.source_performance_health"):
        importlib.import_module("views.source_performance_health")


def test_helper_imports_deployment_views_and_restores_state(
    foreign_views_cached: Path,
) -> None:
    path_before = list(sys.path)
    foreign_pkg = sys.modules["views"]
    foreign_dummy = sys.modules["views.dummy"]

    with isolated_deployment_views(WORKSPACE_DIR) as package:
        assert Path(package.__file__).parent == WORKSPACE_DIR.resolve() / "views"
        mod = importlib.import_module("views.source_performance_health")
        assert Path(mod.__file__).parent == WORKSPACE_DIR.resolve() / "views"
        assert sys.path[0] == str(WORKSPACE_DIR.resolve())

    assert sys.path == path_before
    assert sys.modules["views"] is foreign_pkg
    assert sys.modules["views.dummy"] is foreign_dummy
    assert "views.source_performance_health" not in sys.modules


def test_helper_fails_loudly_when_views_resolves_elsewhere(
    foreign_views_cached: Path,
    tmp_path: Path,
) -> None:
    # A deployment dir with no `views` package: the import falls through to
    # the foreign dir still on sys.path. The helper must refuse, not yield it.
    empty_deployment = tmp_path / "empty-deployment"
    empty_deployment.mkdir()
    path_before = list(sys.path)
    foreign_pkg = sys.modules["views"]

    with pytest.raises(ImportError, match="not .*empty-deployment"):
        with isolated_deployment_views(empty_deployment):
            pytest.fail("helper yielded a foreign views package")

    assert sys.path == path_before
    assert sys.modules["views"] is foreign_pkg
