"""No test may ``ALTER ROLE`` a shared role outside the cluster role lock.

The roles a migration or a runtime bootstrap creates (``okg_mcp``, ``app_rw``,
...) are each ONE ``pg_authid`` row shared by every database on the server,
and ``ALTER ROLE`` rewrites that row without locking it. Parallel test workers
each own a database on one server, so a test that alters such a role from its
own database collides with any other worker altering it: one of the two fails
with ``tuple concurrently updated``. The lock every writer must take is proven
against PostgreSQL in ``tests/substrate/test_cluster_role_ddl_lock.py``; this
file proves, by reading the test sources, that no test skips it.

Two fixtures skipped it until issue #2554: they ran ``ALTER ROLE okg_mcp``
around every test, and the nightly database job failed on that collision two
nights running, once in the fixture and once in the lock's own stress test.

The scan covers statements that NAME a shared role in a string literal, which
is how every such statement in ``tests/`` is written today. It cannot see a
role name passed in through a placeholder.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

TESTS_ROOT = Path(__file__).resolve().parents[1]

#: Roles created once per server and shared by every database on it.
SHARED_ROLES = (
    "app_ro",
    "app_rw",
    "catalog_admin",
    "okg_connection_reservation_owner",
    "okg_dbos",
    "okg_external_action_api",
    "okg_external_action_function_owner",
    "okg_mcp",
    "okg_substrate",
    "projection_remediator",
)

#: ``ALTER ROLE <shared role> ...``, except the ``SET`` / ``RESET`` /
#: ``IN DATABASE`` forms. Those do not write the role's ``pg_authid`` row;
#: they write ``pg_db_role_setting``, which the role lock does not cover.
_ALTER_SHARED_ROLE = re.compile(
    r"\bALTER\s+(?:ROLE|USER)\s+\"?(?:%s)\b(?!\"?\s+(?:SET|RESET|IN\s+DATABASE)\b)"
    % "|".join(SHARED_ROLES),
    re.IGNORECASE,
)

#: The only call a test may hand such a statement to.
LOCKED_HELPER = "alter_role_under_cluster_lock"

#: Files whose matching literal is never sent to a database as an owner:
#: expected message text, a pattern matched against a SQL file, or input the
#: read-only SQL tool must refuse.
TEXT_ONLY = {
    "substrate/mcp/test_run_sql_fuzz.py": (
        "seed input the read-only SQL tool refuses"
    ),
    "substrate/test_attestation_gates_fail_closed.py": (
        "expected next_command text"
    ),
    "substrate/test_local_database_roles.py": "expected operator hint text",
    "substrate/test_runtime_manifest_concurrency.py": (
        "pattern matched against role_split.sql"
    ),
}


def _docstrings(tree: ast.AST) -> set[int]:
    found: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(
            node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef
        ):
            continue
        body = node.body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            found.add(id(body[0].value))
    return found


def _called_name(call: ast.Call) -> str:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def unlocked_shared_role_alters(source: str) -> list[tuple[int, str]]:
    """``(line, text)`` of each ALTER of a shared role outside the helper."""
    tree = ast.parse(source)
    skipped = _docstrings(tree)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _called_name(node) == LOCKED_HELPER:
            skipped.update(id(inner) for inner in ast.walk(node))
    return sorted(
        (node.lineno, node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in skipped
        and _ALTER_SHARED_ROLE.search(node.value)
    )


def test_the_scan_sees_the_statement_the_old_fixtures_ran() -> None:
    """The exact shapes #2554 removed are reported; the locked shape is not."""
    old_fixture = '''
def fixture(dsn, password, was_login):
    """ALTER ROLE okg_mcp LOGIN in a docstring is prose, not a statement."""
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(
            psycopg_sql.SQL("ALTER ROLE okg_mcp LOGIN PASSWORD {}").format(
                psycopg_sql.Literal(password)
            )
        )
        admin.execute("ALTER ROLE okg_mcp " + ("LOGIN" if was_login else "NOLOGIN"))
        admin.execute(f'ALTER ROLE okg_mcp IN DATABASE "{dsn}" SET plan_cache_mode = auto')
        admin.execute("ALTER ROLE okg_mcp SET plan_cache_mode = force_custom_plan")
        admin.execute('ALTER ROLE "okg_mcp" SET plan_cache_mode = force_custom_plan')
'''
    assert [line for line, _text in unlocked_shared_role_alters(old_fixture)] == [
        6,
        10,
    ]

    locked = """
def fixture(dsn):
    alter_role_under_cluster_lock(
        dsn,
        psycopg.sql.SQL("ALTER ROLE okg_mcp SUPERUSER"),
        lock_role="okg_mcp",
    )
"""
    assert unlocked_shared_role_alters(locked) == []


def test_no_test_alters_a_shared_role_outside_the_cluster_role_lock() -> None:
    offenders: list[str] = []
    matched_text_only: set[str] = set()
    this_file = Path(__file__).resolve()
    for path in sorted(TESTS_ROOT.rglob("*.py")):
        if path == this_file:
            continue
        relative = path.relative_to(TESTS_ROOT).as_posix()
        source = path.read_text(encoding="utf-8")
        if "ALTER" not in source.upper():
            continue
        hits = unlocked_shared_role_alters(source)
        if not hits:
            continue
        if relative in TEXT_ONLY:
            matched_text_only.add(relative)
            continue
        offenders.extend(
            f"tests/{relative}:{line}: {text.strip()[:80]}" for line, text in hits
        )
    assert offenders == [], (
        "these statements alter a role every database on the server shares, "
        "without the cluster role lock; a parallel worker altering the same "
        "role fails one of the two with 'tuple concurrently updated'. Use the "
        "okg_mcp_can_log_in fixture, or run the statement through "
        f"{LOCKED_HELPER} (tests/substrate/conftest.py):\n" + "\n".join(offenders)
    )
    stale = sorted(set(TEXT_ONLY) - matched_text_only)
    assert stale == [], f"TEXT_ONLY entries that no longer match anything: {stale}"
