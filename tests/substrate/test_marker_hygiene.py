from __future__ import annotations

import ast
from pathlib import Path


_REPO_TESTS = Path(__file__).resolve().parents[1]
# Scan every fast-lane (no-DB) path that pulls in DB fixtures. A DB-fixture test
# that is not postgres-marked leaks into the `fast`/`smoke` lanes (which run with
# `-m "not postgres ..."` and start NO Postgres) and fails-on-setup with a
# :5433 ConnectionTimeout instead of being routed to a DB lane. Keep this set in
# sync with the DB-capable paths in tests/qa_lanes.yaml's `fast` lane.
_SCAN_ROOTS = (
    _REPO_TESTS / "substrate",
    _REPO_TESTS / "views",
    _REPO_TESTS / "okg-workspace",
)
_DB_FIXTURES = {
    "clean_runtime",
    "clean_live_runtime",
    "clean_queue_runtime",
    "okg_dsn",
    "okg_migrated",
    "live_af_pubs_generation",
    "live_okg_dsn",
}


def _is_pytest_postgres_mark(node: ast.AST, aliases: set[str] | None = None) -> bool:
    aliases = aliases or set()
    if isinstance(node, ast.Call):
        return _is_pytest_postgres_mark(node.func, aliases)
    if isinstance(node, ast.Name):
        return node.id in aliases
    if not isinstance(node, ast.Attribute) or node.attr != "postgres":
        return False
    mark = node.value
    return (
        isinstance(mark, ast.Attribute)
        and mark.attr == "mark"
        and isinstance(mark.value, ast.Name)
        and mark.value.id == "pytest"
    )


def _assignment_has_postgres_mark(stmt: ast.stmt, aliases: set[str]) -> bool:
    if not isinstance(stmt, (ast.Assign, ast.AnnAssign)):
        return False
    value = stmt.value
    if value is None:
        return False
    if _is_pytest_postgres_mark(value, aliases):
        return True
    if isinstance(value, (ast.List, ast.Tuple)):
        return any(_is_pytest_postgres_mark(item, aliases) for item in value.elts)
    return False


def _assigns_pytestmark(stmt: ast.stmt) -> bool:
    targets: list[ast.expr]
    if isinstance(stmt, ast.Assign):
        targets = list(stmt.targets)
    elif isinstance(stmt, ast.AnnAssign):
        targets = [stmt.target]
    else:
        return False
    return any(isinstance(target, ast.Name) and target.id == "pytestmark"
               for target in targets)


def _body_has_postgres_pytestmark(body: list[ast.stmt], aliases: set[str]) -> bool:
    return any(
        _assigns_pytestmark(stmt) and _assignment_has_postgres_mark(stmt, aliases)
        for stmt in body
    )


def _node_has_postgres_mark(
    node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
    aliases: set[str],
) -> bool:
    if any(
        _is_pytest_postgres_mark(decorator, aliases)
        for decorator in node.decorator_list
    ):
        return True
    if isinstance(node, ast.ClassDef):
        return _body_has_postgres_pytestmark(node.body, aliases)
    return False


def _function_has_pytestmark_assignment(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    return any(_assigns_pytestmark(stmt) for stmt in ast.walk(node))


def _module_postgres_mark_aliases(body: list[ast.stmt]) -> set[str]:
    aliases: set[str] = set()
    for stmt in body:
        if not isinstance(stmt, ast.Assign):
            continue
        if not _is_pytest_postgres_mark(stmt.value, aliases):
            continue
        for target in stmt.targets:
            if isinstance(target, ast.Name) and target.id != "pytestmark":
                aliases.add(target.id)
    return aliases


def _iter_test_functions(
    tree: ast.Module,
) -> tuple[set[str], list[tuple[ast.FunctionDef | ast.AsyncFunctionDef, bool]]]:
    aliases = _module_postgres_mark_aliases(tree.body)
    module_marked = _body_has_postgres_pytestmark(tree.body, aliases)
    functions: list[tuple[ast.FunctionDef | ast.AsyncFunctionDef, bool]] = []

    for item in tree.body:
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if item.name.startswith("test_"):
                functions.append((item, module_marked))
        elif isinstance(item, ast.ClassDef):
            class_marked = module_marked or _node_has_postgres_mark(item, aliases)
            for member in item.body:
                if (
                    isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and member.name.startswith("test_")
                ):
                    functions.append((member, class_marked))
    return aliases, functions


def _fixture_arg_names(node: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    return {
        arg.arg
        for arg in (
            list(node.args.posonlyargs)
            + list(node.args.args)
            + list(node.args.kwonlyargs)
        )
    }


def _db_fixture_mark_failures(source: str, *, rel: Path) -> list[str]:
    failures: list[str] = []
    tree = ast.parse(source, filename=str(rel))
    aliases, test_functions = _iter_test_functions(tree)
    for node, inherited_marked in test_functions:
        function_marked = inherited_marked or _node_has_postgres_mark(
            node, aliases,
        )
        db_fixtures = sorted(_fixture_arg_names(node) & _DB_FIXTURES)
        if db_fixtures and not function_marked:
            failures.append(
                f"{rel}:{node.lineno} {node.name} uses "
                f"{', '.join(db_fixtures)} without @pytest.mark.postgres"
            )
        if _function_has_pytestmark_assignment(node):
            failures.append(
                f"{rel}:{node.lineno} {node.name} assigns pytestmark "
                "inside the function body; use a module, class, or "
                "decorator mark instead"
            )
    return failures


def test_db_fixture_hygiene_detects_unmarked_synthetic_test() -> None:
    failures = _db_fixture_mark_failures(
        "def test_needs_db(clean_runtime):\n    pass\n",
        rel=Path("synthetic/test_unmarked.py"),
    )

    assert failures == [
        "synthetic/test_unmarked.py:1 test_needs_db uses clean_runtime "
        "without @pytest.mark.postgres"
    ]


def test_db_fixture_hygiene_accepts_function_and_module_marks() -> None:
    function_marked = _db_fixture_mark_failures(
        "import pytest\n"
        "@pytest.mark.postgres\n"
        "def test_needs_db(clean_runtime):\n"
        "    pass\n",
        rel=Path("synthetic/test_function_marked.py"),
    )
    module_marked = _db_fixture_mark_failures(
        "import pytest\n"
        "pytestmark = pytest.mark.postgres\n"
        "def test_needs_db(okg_dsn):\n"
        "    pass\n",
        rel=Path("synthetic/test_module_marked.py"),
    )

    assert function_marked == []
    assert module_marked == []


def test_db_fixture_tests_are_marked_postgres() -> None:
    failures: list[str] = []

    for root in _SCAN_ROOTS:
        for path in sorted(root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            rel = path.relative_to(_REPO_TESTS)
            failures.extend(
                _db_fixture_mark_failures(
                    path.read_text(encoding="utf-8"),
                    rel=rel,
                )
            )

    assert failures == []
