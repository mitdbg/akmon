"""The pg_cron guard in ops/pg/initdb decides correctly on a real server.

``ops/pg/initdb/00-extensions.sql`` creates ``pg_cron`` only when
``shared_preload_libraries`` lists it. Getting that decision wrong either way
breaks something: a false "yes" brings back the okg-workspace first-boot
failure (``unrecognized configuration parameter "cron.database_name"``), and a
false "no" silently drops ``pg_cron`` from ``ops/pg/docker-compose.yaml``,
which does preload it.

A text check cannot see either mistake, so this test takes the guard's own IF
condition out of the script, replaces its one read of the server setting with
a literal preload string, and has PostgreSQL evaluate it. The container proof
in ``tests/live/postgres_launch`` runs the whole script on a fresh volume; it
needs a container engine and runs by hand, while this runs in the per-PR
substrate-db lane.
"""

from __future__ import annotations

import re
from pathlib import Path

import psycopg
import pytest

from tests.substrate.conftest import require_okg_test_dsn

pytestmark = pytest.mark.postgres

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "ops/pg/initdb/00-extensions.sql"
SETTING_READ = "current_setting('shared_preload_libraries')"
_DO_BODY = re.compile(r"\bDO\s+\$\$(?P<body>.*?)\$\$", re.DOTALL | re.IGNORECASE)
# The IF whose THEN branch is the pg_cron create. The condition may not span a
# THEN, so the match cannot start at an earlier, unrelated IF.
_GUARD = re.compile(
    r"\bIF\s+(?P<condition>(?:(?!\bTHEN\b).)*?)\s+THEN\s+"
    r"CREATE\s+EXTENSION\s+IF\s+NOT\s+EXISTS\s+pg_cron\s*;",
    re.DOTALL | re.IGNORECASE,
)

#: Spellings Postgres accepts for a list that loads pg_cron.
LOADS_PG_CRON = (
    "pg_cron",
    "timescaledb,pg_cron,pg_textsearch",
    "timescaledb, pg_cron, pg_textsearch",
    "timescaledb,\tpg_cron",
    "timescaledb,\n  pg_cron,\n  pg_textsearch",
    "$libdir/pg_cron",
    "pg_cron.so",
    "timescaledb,$libdir/pg_cron.so",
    '"pg_cron"',
    "/usr/lib/postgresql/17/lib/pg_cron.so",
)
#: Lists that do not load pg_cron, including near-miss names.
DOES_NOT_LOAD_PG_CRON = (
    "",
    "timescaledb,pg_textsearch",
    "timescaledb, pg_textsearch",
    "my_pg_cron",
    "pg_cron_extra",
    "pg_cronx.so",
    "$libdir/pg_cron2",
)


def _without_line_comments(sql: str) -> str:
    # The guard has no `--` inside a string literal; a comment in the body
    # must not be read as part of the condition.
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def guard_condition(sql: str) -> str:
    """The guard's IF condition, with the setting read as a query parameter."""
    matches = [
        match
        for body in _DO_BODY.finditer(_without_line_comments(sql))
        for match in _GUARD.finditer(body.group("body"))
    ]
    assert len(matches) == 1, "expected exactly one IF ... THEN CREATE EXTENSION pg_cron"
    condition = matches[0].group("condition")
    assert condition.count(SETTING_READ) == 1, (
        f"the guard must read {SETTING_READ} exactly once: {condition}"
    )
    assert "%" not in condition, "a literal % would be read as a query placeholder"
    return condition.replace(SETTING_READ, "%s::text")


def decide(conn: psycopg.Connection, condition: str, preload: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(f"SELECT ({condition})::boolean", (preload,))  # noqa: S608
        (value,) = cur.fetchone()
    assert value is not None, f"the guard evaluated to NULL for {preload!r}"
    return bool(value)


@pytest.fixture(scope="module")
def conn():
    with psycopg.connect(require_okg_test_dsn(), autocommit=True) as connection:
        yield connection


@pytest.fixture(scope="module")
def condition() -> str:
    return guard_condition(SCRIPT.read_text(encoding="utf-8"))


@pytest.mark.parametrize("preload", LOADS_PG_CRON)
def test_guard_creates_pg_cron_when_it_is_preloaded(conn, condition, preload) -> None:
    assert decide(conn, condition, preload) is True


@pytest.mark.parametrize("preload", DOES_NOT_LOAD_PG_CRON)
def test_guard_skips_pg_cron_when_it_is_not_preloaded(conn, condition, preload) -> None:
    assert decide(conn, condition, preload) is False


def test_the_server_agrees_with_the_guard_about_its_own_setting(conn, condition) -> None:
    """On the test server itself, the guard's answer matches what is loaded.

    pg_cron registers ``cron.database_name`` when it loads, and pg_settings
    lists only registered settings (an unregistered ``-c cron.x=...``
    placeholder is hidden), so its presence there is the server's own answer,
    independent of the string parsing.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT current_setting('shared_preload_libraries')")
        (preload,) = cur.fetchone()
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_settings WHERE name = 'cron.database_name')"
        )
        (loaded,) = cur.fetchone()
    assert decide(conn, condition, preload) is loaded
