"""No test hands its own `sys.path` to a child process.

`sys.path` grows during a serial run (see `tests.pytest.child_pythonpath`),
so a child PYTHONPATH built from it eventually passes Linux's 128 KiB
per-string limit and the child fails to start with E2BIG. The scan below
holds that for every tracked test file; `child_pythonpath` is the
replacement.
"""
from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.pytest.child_pythonpath import REPO_ROOT, SRC_ROOT, child_pythonpath
from tests.pytest.tracked_files import tracked_files


def _is_sys_path(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "path"
        and isinstance(node.value, ast.Name)
        and node.value.id == "sys"
    )


def _reads_sys_path(expr: ast.AST | None, tainted: set[str]) -> bool:
    """True if `expr` reads `sys.path` or a name assigned from it."""
    return expr is not None and any(
        _is_sys_path(n) or (isinstance(n, ast.Name) and n.id in tainted)
        for n in ast.walk(expr)
    )


def _names_built_from_sys_path(tree: ast.AST) -> set[str]:
    """Names assigned anywhere in the module from an expression that reads
    `sys.path`, followed through chains such as `a = sys.path; b = a`."""
    assigns = [
        (target.id, node.value)
        for node in ast.walk(tree)
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign))
        for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
        if isinstance(target, ast.Name)
    ]
    tainted: set[str] = set()
    while True:
        grown = {
            name for name, value in assigns
            if name not in tainted and _reads_sys_path(value, tainted)
        }
        if not grown:
            return tainted
        tainted |= grown


def _is_pythonpath(node: ast.AST | None) -> bool:
    return isinstance(node, ast.Constant) and node.value == "PYTHONPATH"


def _pythonpath_values(tree: ast.AST):
    """Yield every expression this module assigns to PYTHONPATH."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Subscript) and _is_pythonpath(target.slice):
                    yield node, node.value
        elif isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values):
                if _is_pythonpath(key):
                    yield node, value
        elif isinstance(node, ast.Call):
            for keyword in node.keywords:
                if keyword.arg == "PYTHONPATH":
                    yield node, keyword.value
            # setenv("PYTHONPATH", v), env.setdefault("PYTHONPATH", v), ...
            if len(node.args) >= 2 and _is_pythonpath(node.args[0]):
                yield node, node.args[1]


def _offenders(source: str, name: str) -> list[str]:
    tree = ast.parse(source, filename=name)
    tainted = _names_built_from_sys_path(tree)
    return [
        f"{name}:{node.lineno}"
        for node, value in _pythonpath_values(tree)
        if _reads_sys_path(value, tainted)
    ]


@pytest.mark.parametrize(
    "snippet",
    [
        'env = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))',
        'env["PYTHONPATH"] = os.pathsep.join([root] + [p for p in sys.path if p])',
        'env = {"PYTHONPATH": ":".join(sys.path)}',
        'monkeypatch.setenv("PYTHONPATH", os.pathsep.join(sys.path))',
        'env["PYTHONPATH"] += os.pathsep + os.pathsep.join(sys.path)',
        'env.update({"PYTHONPATH": os.pathsep.join(sys.path)})',
        'parts = list(sys.path); joined = ":".join(parts); env["PYTHONPATH"] = joined',
    ],
)
def test_the_scan_catches_each_way_of_copying_sys_path(snippet: str) -> None:
    assert _offenders(snippet, "case.py") == ["case.py:1"]


def test_the_scan_allows_child_pythonpath() -> None:
    assert _offenders(
        'env["PYTHONPATH"] = child_pythonpath(tmp_path)\n'
        "sys.path.insert(0, str(tmp_path))\n",
        "case.py",
    ) == []


def test_no_test_copies_sys_path_into_a_child_pythonpath() -> None:
    offenders: list[str] = []
    for path in tracked_files(REPO_ROOT / "tests", "*.py"):
        source = path.read_bytes()
        if b"PYTHONPATH" not in source:
            continue
        offenders += _offenders(
            source.decode("utf-8"), str(path.relative_to(REPO_ROOT)),
        )
    assert offenders == [], (
        "These build a child PYTHONPATH from sys.path, which grows during a "
        "serial run until execve fails with E2BIG. Use "
        f"tests.pytest.child_pythonpath.child_pythonpath instead: {offenders}"
    )


def test_child_pythonpath_ignores_a_grown_sys_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    # About 1.2 MB of entries: past Linux's 128 KiB per-string limit and
    # macOS's 1 MiB total argument limit.
    leaked = [f"/okg-leaked/{i:06d}/" + "x" * 80 for i in range(12_000)]
    monkeypatch.setattr(sys, "path", leaked + list(sys.path))
    monkeypatch.setenv("PYTHONPATH", "/inherited")

    value = child_pythonpath(tmp_path)

    assert value.split(os.pathsep) == [
        str(tmp_path), str(SRC_ROOT), str(REPO_ROOT), "/inherited",
    ]
    subprocess.run(
        [sys.executable, "-c", "import okg.substrate.mcp.session_tool_surface"],
        env=dict(os.environ, PYTHONPATH=value),
        check=True,
        timeout=60,
    )
