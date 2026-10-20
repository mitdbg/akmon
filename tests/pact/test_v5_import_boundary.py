"""PACT v5 is a self-contained package: nothing under src/pact imports okg."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "src" / "pact"


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_package_exists_with_expected_modules() -> None:
    expected = {"schema", "digest", "store", "tier", "git", "approval", "check", "usage", "migrate", "cli"}
    present = {p.stem for p in PACKAGE.glob("*.py")}
    assert expected <= present


@pytest.mark.parametrize("path", sorted(PACKAGE.glob("*.py")), ids=lambda p: p.name)
def test_no_okg_import(path: Path) -> None:
    offending = {name for name in _imports(path) if name == "okg" or name.startswith("okg.")}
    assert not offending, f"{path.name} imports okg: {sorted(offending)}"


def test_boundary_check_refuses_a_planted_import(tmp_path: Path) -> None:
    planted = tmp_path / "planted.py"
    planted.write_text("import okg.substrate.pact\n", encoding="utf-8")
    assert {n for n in _imports(planted) if n.startswith("okg")}


def test_only_third_party_dependencies_are_yaml_and_pydantic() -> None:
    allowed_prefixes = ("pact", "yaml", "pydantic")
    stdlib = {"argparse", "collections", "dataclasses", "datetime", "hashlib", "json", "os", "pathlib", "re", "subprocess", "sys", "time", "typing", "__future__"}
    for path in PACKAGE.glob("*.py"):
        for name in _imports(path):
            top = name.split(".")[0]
            assert top in stdlib or top.startswith(allowed_prefixes), f"{path.name} imports {name}"
