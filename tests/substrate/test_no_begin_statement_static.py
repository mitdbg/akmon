"""No product code executes an unreviewed BEGIN statement (lane C 238a).

On a psycopg connection with autocommit off, psycopg already sends BEGIN before
the first statement of a transaction, so an explicit "BEGIN" there is a second
one that PostgreSQL ignores with "there is already a transaction in progress".
The live tests are in tests/substrate/test_no_nested_begin.py; this guard makes
a new explicit BEGIN state why it cannot nest.
"""
from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _begin_statement_calls(path: Path) -> list[tuple[str, int]]:
    """``<x>.execute("BEGIN ...")`` calls in ``path``, by enclosing function."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: list[tuple[str, int]] = []

    def visit(node: ast.AST, scope: tuple[str, ...]) -> None:
        for child in ast.iter_child_nodes(node):
            child_scope = scope
            if isinstance(
                child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
            ):
                child_scope = (*scope, child.name)
            if (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Attribute)
                and child.func.attr == "execute"
                and child.args
                and isinstance(child.args[0], ast.Constant)
                and isinstance(child.args[0].value, str)
            ):
                text = child.args[0].value.strip().upper()
                if text.startswith("BEGIN") or text.startswith("START TRANSACTION"):
                    found.append((".".join(scope), child.lineno))
            visit(child, child_scope)

    visit(tree, ())
    return found


# Each remaining BEGIN statement and why it cannot nest.
_ALLOWED_BEGIN_STATEMENTS: dict[tuple[str, str], str] = {
    (
        "src/chronos_core/branching/sql_adapters.py",
        "SQLiteDatabaseAdapter.begin",
    ): "sqlite3, not PostgreSQL",
    (
        "src/chronos_core/branching/sql_adapters.py",
        "PostgresDatabaseAdapter.begin",
    ): "autocommit connection only, after an idle-session check",
}


def test_no_unreviewed_begin_statement_in_product_code() -> None:
    """A new explicit BEGIN must say why it cannot run inside a transaction."""
    found: dict[tuple[str, str], int] = {}
    for root in ("src/okg", "src/chronos_core", "src/pact"):
        for path in sorted((REPO / root).rglob("*.py")):
            rel = path.relative_to(REPO).as_posix()
            for scope, _line in _begin_statement_calls(path):
                found[(rel, scope)] = found.get((rel, scope), 0) + 1
    unexpected = {
        key: count for key, count in found.items()
        if key not in _ALLOWED_BEGIN_STATEMENTS
    }
    assert unexpected == {}
    assert found == {key: 1 for key in _ALLOWED_BEGIN_STATEMENTS}
