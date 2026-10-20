"""The operator-ui audit seed needs the workspace deployment's rollup view.

The seed publishes its fixture with the okg-workspace ontology-graph rollup,
which it loads from that deployment's directory by path. The public copy does
not carry the deployment, so the seed must refuse by name, and before it wipes
the fixture database, rather than stop on a bare FileNotFoundError halfway
through (#2823).
"""

from __future__ import annotations

import importlib.util
import pathlib
from types import ModuleType

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SEED_SCRIPT = REPO_ROOT / "src" / "okg" / "operator_ui" / "web" / "audit" / "seed_fixture.py"


def _load_seed_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("audit_seed_rollup_view", SEED_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_a_checkout_without_the_rollup_view_is_refused_by_name(tmp_path: pathlib.Path) -> None:
    seed = _load_seed_module()

    with pytest.raises(RuntimeError) as raised:
        seed.rollup_view_path(tmp_path)

    assert str(seed.ROLLUP_VIEW_RELPATH) in str(raised.value)
    assert str(tmp_path / seed.ROLLUP_VIEW_RELPATH) in str(raised.value)

    view = tmp_path / seed.ROLLUP_VIEW_RELPATH
    view.parent.mkdir(parents=True)
    view.write_text("", encoding="utf-8")
    assert seed.rollup_view_path(tmp_path) == view


def test_seed_refuses_before_resetting_the_database(monkeypatch: pytest.MonkeyPatch) -> None:
    seed = _load_seed_module()
    monkeypatch.setattr(
        seed, "ROLLUP_VIEW_RELPATH", pathlib.PurePosixPath("deployments/absent/view.py")
    )
    monkeypatch.setattr(seed, "validate_destructive_fixture_authority", lambda **_kw: None)

    def no_reset(_dsn: str) -> None:
        raise AssertionError("the database must not be reset without the rollup view")

    monkeypatch.setattr(seed, "_reset_db", no_reset)

    with pytest.raises(RuntimeError, match="deployments/absent/view.py"):
        seed.seed("postgresql://unused/okg_audit_fixture", "audit-fixture", reset_authority="unused")
