"""Static guard for retire-legacy-transform-runtime."""

from __future__ import annotations

import ast
from collections.abc import Iterable
from pathlib import Path

from tests.pytest.py_ast_cache import parse_py


ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src" / "okg"
SUBSTRATE_TESTS = ROOT / "tests" / "substrate"
TRANSFORM_GATE = (
    ROOT / "src" / "okg" / "substrate" / "consistency" / "transform_gate.py"
)
COMMIT_MODULE = ROOT / "src" / "okg" / "substrate" / "transform" / "commit.py"


def _py_files() -> Iterable[Path]:
    yield from SRC.rglob("*.py")
    yield from SUBSTRATE_TESTS.rglob("*.py")


# Shared session cache, for the two single-module checks only.
_tree = parse_py


def _trees_mentioning(*tokens: str) -> Iterable[tuple[Path, ast.Module]]:
    """Parse, uncached, only the scanned files whose text names a token.

    Every node the whole-tree scans flag names one of its tokens as a single
    identifier (a called or an imported name), and an identifier is always
    literal source text, so a file without one cannot hold an offender. A
    dotted module path is not a token: `okg . substrate` is valid Python. Caching every tree of src/okg and
    tests/substrate in the session parse cache held about 3 GiB in one xdist
    worker, and the fast lane's 8 GiB pod killed that worker.
    """
    for path in _py_files():
        text = path.read_text(encoding="utf-8")
        if any(token in text for token in tokens):
            yield path, ast.parse(text, filename=str(path))


def test_transform_consistency_gate_has_no_legacy_backend() -> None:
    tree = _tree(TRANSFORM_GATE)
    class_defs = [n for n in tree.body if isinstance(n, ast.ClassDef)]
    gate = next(n for n in class_defs if n.name == "TransformConsistencyGate")

    assert not any(
        isinstance(n, ast.FunctionDef) and n.name == "_eval_legacy"
        for n in ast.walk(gate)
    )
    assert not any(
        isinstance(n, ast.arg) and n.arg == "backend" for n in ast.walk(gate)
    )
    assert not any(
        isinstance(n, ast.Attribute) and n.attr == "backend" for n in ast.walk(gate)
    )


def test_no_transform_gate_backend_callers_remain() -> None:
    offenders: list[str] = []
    for path, tree in _trees_mentioning("TransformConsistencyGate"):
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            calls_gate = (
                isinstance(func, ast.Name) and func.id == "TransformConsistencyGate"
            ) or (
                isinstance(func, ast.Attribute)
                and func.attr == "TransformConsistencyGate"
            )
            if calls_gate and any(kw.arg == "backend" for kw in node.keywords):
                offenders.append(str(path.relative_to(ROOT)))

    assert offenders == []


def test_python_commit_apply_boundary_is_not_exported() -> None:
    tree = _tree(COMMIT_MODULE)
    assert not any(
        isinstance(n, ast.FunctionDef)
        and n.name in {"commit_delta_set", "commit_delta_set_sql"}
        for n in ast.walk(tree)
    )
    assert not any(
        isinstance(n, ast.ClassDef) and n.name in {"CommitReceipt", "CommitRejected"}
        for n in ast.walk(tree)
    )


def test_no_python_commit_apply_callers_remain() -> None:
    offenders: list[str] = []
    for path, tree in _trees_mentioning(
        "commit_delta_set", "CommitReceipt", "CommitRejected"
    ):
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ImportFrom)
                and node.module == "okg.substrate.transform.commit"
            ):
                imported = {alias.name for alias in node.names}
                if imported & {
                    "commit_delta_set",
                    "commit_delta_set_sql",
                    "CommitReceipt",
                    "CommitRejected",
                }:
                    offenders.append(str(path.relative_to(ROOT)))
            elif isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name) and func.id == "commit_delta_set":
                    offenders.append(str(path.relative_to(ROOT)))
                elif (
                    isinstance(func, ast.Attribute) and func.attr == "commit_delta_set"
                ):
                    offenders.append(str(path.relative_to(ROOT)))
                elif isinstance(func, ast.Name) and func.id == "commit_delta_set_sql":
                    offenders.append(str(path.relative_to(ROOT)))
                elif (
                    isinstance(func, ast.Attribute)
                    and func.attr == "commit_delta_set_sql"
                ):
                    offenders.append(str(path.relative_to(ROOT)))

    assert offenders == []
