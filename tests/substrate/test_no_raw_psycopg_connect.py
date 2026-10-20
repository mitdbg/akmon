"""Refuse a new raw `psycopg.connect()` in the substrate.

Every substrate connection is supposed to come from the one factory in
`okg.substrate.db.connection`. That factory applies the TCP keepalives, the
finite `connect_timeout`, and the `idle_in_transaction_session_timeout` bound
(merging any `options` the DSN already carries), and it stamps an
`application_name` so `pg_stat_activity` names the owner. A raw
`psycopg.connect()` gets none of that: the connection can hang forever on a
TCP-level stall and can sit idle in a transaction holding locks, and it shows
up anonymously in `pg_stat_activity`.

This test is a ratchet, not a sweep. `ALLOWED_RAW_CONNECT_FILES` records every
file that still had a raw call when the factory lint landed (issue #1646), so
the lint can go green today. It fails two ways:

* a file NOT on the list makes a raw call -- a new violation, fix it now;
* a file ON the list makes no raw call -- it has been swept, so drop it from
  the list. That is what stops the list from being re-widened, and it is why
  each sweep pull request must edit this file.

A line-based check would not do: 47 of these call sites put the arguments on
following lines, so the walk is over the AST.

Scope is `src/okg/substrate` only. `bench/`, `chronos_core/`, and `scripts/`
are not walked and are not ratcheted, so a raw call added there passes. Only
calls that resolve to `psycopg.connect`, `psycopg.Connection.connect`, or
`psycopg.AsyncConnection.connect` count; building a `psycopg_pool.ConnectionPool`
is a different question about pool lifetime and is out of scope for this lint.
"""

from __future__ import annotations

import ast
import functools
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SUBSTRATE_ROOT = REPO_ROOT / "src" / "okg" / "substrate"

# The one legitimate raw call: the factory itself.
FACTORY_FILE = "src/okg/substrate/db/connection.py"

FIX_HINT = (
    "replace it with the substrate connection factory:\n"
    "    from okg.substrate.db import connection\n"
    "    conn = connection.connect(dsn, application_name=..., deployment=...)\n"
    "`connection.connect` takes `autocommit`, `application_name`, "
    "`connect_timeout`, `deployment`, `row_factory`, and `options` "
    "(appended to the settings the factory already applies, never "
    "replacing them)."
)

# Files that still carry a raw call, recorded when this lint landed. Shrink
# only -- never add a file here to make a new violation pass.
# Empty since issue #1968 swept `mcp/deployment_entry.py`, the last entry:
# every substrate connection now comes from the factory, which also carries the
# session `search_path` pin. Never add a file here to make a new violation pass.
ALLOWED_RAW_CONNECT_FILES: frozenset[str] = frozenset()


# The classmethod spellings that open a connection without going through
# `psycopg.connect`.
CONNECTION_CLASSES = frozenset({"Connection", "AsyncConnection"})


def _count_raw_connect_calls(source: str, filename: str = "<source>") -> int:
    """Count calls in `source` that open a psycopg connection directly.

    Four spellings resolve to the same thing and all four count:

    * `import psycopg` (with or without an alias) + `psycopg.connect(...)`;
    * `from psycopg import connect` + `connect(...)`;
    * `psycopg.Connection.connect(...)` / `psycopg.AsyncConnection.connect(...)`
      off that same module alias;
    * `from psycopg import Connection` (or `AsyncConnection`, with or without an
      alias) + `Connection.connect(...)`.
    """
    # Every spelling needs an import statement naming `psycopg`, so a file
    # without that word anywhere cannot hold a raw call. Skipping the parse
    # for those keeps this lint off the fast lane's critical path; it is a
    # shortcut in cost only, never in what counts as a violation.
    if "psycopg" not in source:
        return 0
    tree = ast.parse(source, filename=filename)

    module_aliases: set[str] = set()
    function_aliases: set[str] = set()
    class_aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "psycopg":
                    module_aliases.add(alias.asname or "psycopg")
        elif isinstance(node, ast.ImportFrom) and node.module == "psycopg":
            for alias in node.names:
                if alias.name == "connect":
                    function_aliases.add(alias.asname or "connect")
                elif alias.name in CONNECTION_CLASSES:
                    class_aliases.add(alias.asname or alias.name)

    def _is_raw_connect(func: ast.expr) -> bool:
        if isinstance(func, ast.Name):
            # `connect(...)` from `from psycopg import connect`.
            return func.id in function_aliases
        if not isinstance(func, ast.Attribute) or func.attr != "connect":
            return False
        value = func.value
        if isinstance(value, ast.Name):
            # `psycopg.connect(...)`, or `Connection.connect(...)` from
            # `from psycopg import Connection`.
            return value.id in module_aliases or value.id in class_aliases
        if isinstance(value, ast.Attribute) and value.attr in CONNECTION_CLASSES:
            # `psycopg.Connection.connect(...)`.
            return isinstance(value.value, ast.Name) and value.value.id in module_aliases
        return False

    return sum(
        1
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and _is_raw_connect(node.func)
    )


def _raw_connect_call_count(path: Path) -> int:
    """Count the raw psycopg connection calls in the file at `path`."""
    return _count_raw_connect_calls(
        path.read_text(encoding="utf-8"), filename=str(path)
    )


@functools.lru_cache(maxsize=1)
def _substrate_files() -> dict[str, int]:
    """Map every substrate module to its raw-connect count.

    Cached: all three tests below need the same whole-tree walk.
    """
    return {
        str(path.relative_to(REPO_ROOT)): _raw_connect_call_count(path)
        for path in sorted(SUBSTRATE_ROOT.rglob("*.py"))
    }


def test_no_new_raw_psycopg_connect() -> None:
    """No substrate file outside the factory and the allowlist may call
    `psycopg.connect` directly."""
    counts = _substrate_files()
    offenders = sorted(
        name
        for name, count in counts.items()
        if count and name != FACTORY_FILE
        and name not in ALLOWED_RAW_CONNECT_FILES
    )

    assert not offenders, (
        "raw psycopg.connect() in "
        + ", ".join(offenders)
        + ".\nThese connections get no keepalives, no connect timeout, and no "
        "idle-in-transaction bound.\n"
        + FIX_HINT
    )


def test_allowlist_ratchets_shut() -> None:
    """Every allowlisted file must still have a raw call.

    A swept file that stays on the list would leave room for the next raw call
    to slip back in unnoticed, so removing the calls and removing the entry are
    one change.
    """
    counts = _substrate_files()

    missing = sorted(
        name for name in ALLOWED_RAW_CONNECT_FILES if name not in counts
    )
    assert not missing, (
        "allowlisted file no longer exists: "
        + ", ".join(missing)
        + " -- drop it from ALLOWED_RAW_CONNECT_FILES."
    )

    swept = sorted(
        name for name in ALLOWED_RAW_CONNECT_FILES if counts[name] == 0
    )
    assert not swept, (
        "these files no longer call psycopg.connect directly: "
        + ", ".join(swept)
        + " -- delete them from ALLOWED_RAW_CONNECT_FILES so the allowlist "
        "keeps shrinking."
    )


def test_factory_holds_exactly_one_raw_connect() -> None:
    """The factory is the single place allowed to reach psycopg directly."""
    counts = _substrate_files()

    assert FACTORY_FILE in counts, (
        f"the connection factory {FACTORY_FILE} no longer exists -- point "
        "FACTORY_FILE at wherever the one raw psycopg.connect call now lives."
    )
    assert counts[FACTORY_FILE] == 1, (
        f"{FACTORY_FILE} makes {counts[FACTORY_FILE]} raw psycopg.connect "
        "calls; it is meant to make exactly one, inside connect()."
    )


MATCHED_SOURCES = {
    "module": "import psycopg\npsycopg.connect(dsn)\n",
    "module-alias": "import psycopg as pg\npg.connect(dsn)\n",
    "bare-function": "from psycopg import connect\nconnect(dsn)\n",
    "module-connection": "import psycopg\npsycopg.Connection.connect(dsn)\n",
    "module-async-connection": (
        "import psycopg\npsycopg.AsyncConnection.connect(dsn)\n"
    ),
    "alias-connection": "import psycopg as pg\npg.Connection.connect(dsn)\n",
    "imported-connection": (
        "from psycopg import Connection\nConnection.connect(dsn)\n"
    ),
    "imported-async-connection": (
        "from psycopg import AsyncConnection\nAsyncConnection.connect(dsn)\n"
    ),
    "imported-connection-alias": (
        "from psycopg import Connection as C\nC.connect(dsn)\n"
    ),
}

UNMATCHED_SOURCES = {
    # The factory itself is the sanctioned door; calling it is the fix, not a
    # violation.
    "factory": (
        "from okg.substrate.db import connection\nconnection.connect(dsn)\n"
    ),
    # A same-named method on something that is not psycopg.
    "unrelated-connect": "import psycopg\nself.pool.connect(dsn)\n",
    # A `Connection` that never came from psycopg.
    "foreign-connection-class": (
        "import psycopg\nfrom sqlalchemy import Connection\n"
        "Connection.connect(dsn)\n"
    ),
    # Pool construction is a different question and out of scope.
    "connection-pool": (
        "from psycopg_pool import ConnectionPool\nConnectionPool(dsn)\n"
    ),
    # No psycopg import anywhere: the fast-path skip must agree with the walk.
    "no-psycopg": "import sqlite3\nsqlite3.connect(path)\n",
}


@pytest.mark.parametrize("name", sorted(MATCHED_SOURCES))
def test_matcher_counts_every_raw_connect_spelling(name: str) -> None:
    """Each spelling that opens a psycopg connection directly is counted."""
    assert _count_raw_connect_calls(MATCHED_SOURCES[name]) == 1, (
        f"the {name} spelling slipped past the matcher; a raw connect written "
        "that way would not be caught."
    )


@pytest.mark.parametrize("name", sorted(UNMATCHED_SOURCES))
def test_matcher_ignores_calls_that_are_not_raw_connects(name: str) -> None:
    """Nothing but a direct psycopg connection counts."""
    assert _count_raw_connect_calls(UNMATCHED_SOURCES[name]) == 0, (
        f"the {name} source was counted as a raw connect; this lint would "
        "fail a file that does nothing wrong."
    )


# ---------------------------------------------------------------------------
# Issue #1968 -- a pool is a connection factory too.
# ---------------------------------------------------------------------------
#
# The raw-connect lint above says nothing about `ConnectionPool(...)`, and the
# comment at the top of this file said pool construction was "a different
# question". It is the same question for everything the factory applies: a
# hand-written `kwargs={...}` gets no session `search_path` pin, no TCP
# keepalives and no idle-in-transaction bound, exactly like a raw connect. Both
# MCP pools were built that way, and so was the viz pool. With those swept, an
# EMPTY `ALLOWED_RAW_CONNECT_FILES` would otherwise have promised more than it
# checked.
#
# The rule is deliberately coarse: a pool construction must sit in a scope that
# calls `connection_kwargs`. It does not try to prove the result reaches the
# `kwargs=` argument, because the factory's own pool builds the dict in a
# separate statement and passes it by name. A lint that demanded one spelling
# would push callers into contortions; this one catches the failure that
# actually happened -- a pool built with no reference to the factory at all.

#: Roots walked for pool construction. `okg/viz` is included because the viz
#: server builds its own pool and its sessions are okg's sessions.
POOL_ROOTS = (
    REPO_ROOT / "src" / "okg" / "substrate",
    REPO_ROOT / "src" / "okg" / "viz",
)

POOL_CLASSES = frozenset({"ConnectionPool", "AsyncConnectionPool"})

POOL_FIX_HINT = (
    "build the pool's kwargs from the substrate factory:\n"
    "    from okg.substrate.db.connection import connection_kwargs\n"
    "    ConnectionPool(conninfo=dsn, kwargs={\n"
    "        **connection_kwargs(dsn=dsn, connect_timeout=...),\n"
    "        'autocommit': True,\n"
    "    })\n"
    "`connection_kwargs` reads the DSN's own `options` first, so nothing an "
    "operator encoded in the connection string is dropped."
)


def _unguarded_pool_constructions(source: str) -> int:
    """Count pool constructions in a module that never calls the factory.

    The rule is deliberately coarse: a pool construction is accepted when
    `connection_kwargs` is called ANYWHERE in the same file. It does not try to
    prove the result reaches the `kwargs=` argument, because there are several
    correct spellings -- the factory's own pool builds the dict in a separate
    statement, and a caller may build it in a helper the pool call then uses.
    A stricter rule would flag correct code, which is worse than this one's
    imprecision: what it has to catch is a pool built with no reference to the
    factory at all, which is what had happened in both MCP pools and in viz.

    Two documented blind spots follow from that, neither of which was the
    failure this guard exists for:

    * a file that already calls the factory somewhere can add a second,
      hand-written pool without being flagged;
    * an aliased import (`from psycopg_pool import ConnectionPool as CP`, then
      `CP(...)`) is matched by name and so is missed.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return 0

    def _called_name(node: ast.AST) -> str | None:
        if not isinstance(node, ast.Call):
            return None
        func = node.func
        if isinstance(func, ast.Attribute):
            return func.attr
        if isinstance(func, ast.Name):
            return func.id
        return None

    calls = [
        name for name in (_called_name(node) for node in ast.walk(tree))
        if name
    ]
    if "connection_kwargs" in calls:
        return 0
    return sum(1 for name in calls if name in POOL_CLASSES)


def _pool_files() -> dict[str, int]:
    counts: dict[str, int] = {}
    for root in POOL_ROOTS:
        for path in sorted(root.rglob("*.py")):
            source = path.read_text(encoding="utf-8", errors="replace")
            if "Pool(" not in source:
                continue
            counts[str(path.relative_to(REPO_ROOT))] = (
                _unguarded_pool_constructions(source)
            )
    return counts


def test_every_connection_pool_takes_its_kwargs_from_the_factory() -> None:
    """A pool built outside the factory opens unpinned, unbounded sessions."""
    offenders = sorted(
        name for name, count in _pool_files().items() if count
    )

    assert not offenders, (
        "connection pool built without the substrate factory in "
        + ", ".join(offenders)
        + ".\nThose connections get no session search_path pin, no keepalives, "
        "and no idle-in-transaction bound.\n"
        + POOL_FIX_HINT
    )


def test_the_pool_walk_actually_finds_the_pools() -> None:
    """A walk that silently matched nothing would pass the test above."""
    counts = _pool_files()
    assert counts, (
        "no file with a pool construction was found under "
        + ", ".join(str(root) for root in POOL_ROOTS)
        + " -- the walk is broken, and the guard above is vacuous."
    )


MATCHED_POOL_SOURCES = {
    "hand-written-kwargs": (
        "from psycopg_pool import ConnectionPool\n"
        "def build(dsn):\n"
        "    return ConnectionPool(conninfo=dsn, kwargs={'autocommit': True})\n"
    ),
    "async-pool": (
        "from psycopg_pool import AsyncConnectionPool\n"
        "def build(dsn):\n"
        "    return AsyncConnectionPool(conninfo=dsn)\n"
    ),
    "module-level": (
        "from psycopg_pool import ConnectionPool\n"
        "POOL = ConnectionPool(conninfo='x')\n"
    ),
}

UNMATCHED_POOL_SOURCES = {
    "kwargs-from-factory": (
        "from psycopg_pool import ConnectionPool\n"
        "from okg.substrate.db.connection import connection_kwargs\n"
        "def build(dsn):\n"
        "    return ConnectionPool(\n"
        "        conninfo=dsn,\n"
        "        kwargs={**connection_kwargs(dsn=dsn), 'autocommit': True},\n"
        "    )\n"
    ),
    "kwargs-built-in-a-separate-statement": (
        "from psycopg_pool import ConnectionPool\n"
        "from okg.substrate.db.connection import connection_kwargs\n"
        "def build(dsn):\n"
        "    kw = {**connection_kwargs(dsn=dsn), 'autocommit': True}\n"
        "    return ConnectionPool(conninfo=dsn, kwargs=kw)\n"
    ),
    "kwargs-built-by-a-helper": (
        "from psycopg_pool import ConnectionPool\n"
        "from okg.substrate.db.connection import connection_kwargs\n"
        "def _kw(dsn):\n"
        "    return {**connection_kwargs(dsn=dsn), 'autocommit': True}\n"
        "def build(dsn):\n"
        "    return ConnectionPool(conninfo=dsn, kwargs=_kw(dsn))\n"
    ),
    "not-a-pool": (
        "from concurrent.futures import ThreadPoolExecutor\n"
        "def build():\n"
        "    return ThreadPoolExecutor(max_workers=2)\n"
    ),
}

#: Shapes this guard is known NOT to catch. They are recorded so the blind
#: spots are visible rather than discovered, and asserted so a future change
#: that closes one shows up as a failing test to update, not a silent shift.
KNOWN_POOL_MISSES = {
    "aliased-import": (
        "from psycopg_pool import ConnectionPool as CP\n"
        "def build(dsn):\n"
        "    return CP(conninfo=dsn)\n"
    ),
    "second-pool-in-a-file-that-already-calls-the-factory": (
        "from psycopg_pool import ConnectionPool\n"
        "from okg.substrate.db.connection import connection_kwargs\n"
        "def good(dsn):\n"
        "    return ConnectionPool(conninfo=dsn, kwargs=connection_kwargs(dsn=dsn))\n"
        "def bad(dsn):\n"
        "    return ConnectionPool(conninfo=dsn, kwargs={'autocommit': True})\n"
    ),
}


@pytest.mark.parametrize("name", sorted(KNOWN_POOL_MISSES))
def test_pool_matcher_documented_blind_spots(name: str) -> None:
    """These are not caught. Recorded deliberately, not by accident."""
    assert _unguarded_pool_constructions(KNOWN_POOL_MISSES[name]) == 0, (
        f"the {name} shape is now caught -- good. Move it out of "
        "KNOWN_POOL_MISSES and into MATCHED_POOL_SOURCES, and drop it from "
        "the docstring's list of blind spots."
    )


@pytest.mark.parametrize("name", sorted(MATCHED_POOL_SOURCES))
def test_pool_matcher_counts_an_unguarded_construction(name: str) -> None:
    assert _unguarded_pool_constructions(MATCHED_POOL_SOURCES[name]) == 1, (
        f"the {name} pool slipped past the matcher; a pool written that way "
        "would open unpinned sessions without being caught."
    )


@pytest.mark.parametrize("name", sorted(UNMATCHED_POOL_SOURCES))
def test_pool_matcher_accepts_a_factory_built_pool(name: str) -> None:
    assert _unguarded_pool_constructions(UNMATCHED_POOL_SOURCES[name]) == 0, (
        f"the {name} source was flagged; this lint would fail a caller that "
        "does the right thing."
    )
