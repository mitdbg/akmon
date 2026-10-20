"""Both schema-destructive DSN helpers must read a DSN the same way.

``_schema_destructive_test_dsn`` mints the disposable database and
``_drop_schema_destructive_test_db`` drops it. They once disagreed: the first
parsed with ``urlsplit`` and refused anything that was not a ``postgres://``
URL, while the second parsed with ``psycopg.conninfo.conninfo_to_dict``, which
also accepts libpq keyword/value form. A keyword ``OKG_TEST_DSN`` therefore
failed at mint time on a path the drop side would have handled.

These cases need no database: they parse strings and inspect the result.
"""
from __future__ import annotations

import psycopg
import pytest

from tests.substrate.conftest import (
    _SCHEMA_DESTRUCTIVE_DB_PREFIX,
    _drop_schema_destructive_test_db,
    _is_safe_test_dbname,
    _schema_destructive_db_name,
    _schema_destructive_test_dsn,
)

NODEID = "tests/substrate/test_mcp_tracing.py::test_trace_write_failure"
OTHER_NODEID = "tests/substrate/test_console_count_rollup_schema.py::test_cutover"

URL_DSN = "postgresql://okg:secret@127.0.0.1:5433/okg_test_gw1"
KEYWORD_DSN = "host=127.0.0.1 port=5433 user=okg password=secret dbname=okg_test_gw1"
BASE_DB = "okg_test_gw1"
OTHER_BASE_DB = "okg_test_gw2"
MULTI_HOST_DSN = "postgresql://u:p@h1:5433,h2:5434/okg_test"


@pytest.mark.parametrize("base_dsn", [URL_DSN, KEYWORD_DSN])
def test_both_dsn_forms_mint_the_same_database(base_dsn: str) -> None:
    """URL and keyword/value DSNs are accepted and name the same target.

    KEPT deliberately after issue #1971, which made ``require_okg_test_dsn``
    refuse a keyword-form ``OKG_TEST_DSN``. No environment variable can now
    deliver that arm, but ``base_dsn`` is a PARAMETER: this is the helper's
    own tolerance of both spellings, and the helper exists precisely because
    splitting the mint and drop sides across two parsers once made a keyword
    DSN raise on one side and parse on the other.
    """
    dsn = _schema_destructive_test_dsn(base_dsn, NODEID)

    info = psycopg.conninfo.conninfo_to_dict(dsn)
    assert info["dbname"] == _schema_destructive_db_name(NODEID, BASE_DB)
    assert info["host"] == "127.0.0.1"
    assert str(info["port"]) == "5433"
    assert info["user"] == "okg"
    assert info["password"] == "secret"


def test_minted_dsn_is_a_url_so_chronos_picks_the_postgres_backend() -> None:
    """Chronos selects its backend by URL scheme; a kv-string would be
    misrouted to the SQLite adapter."""
    assert _schema_destructive_test_dsn(KEYWORD_DSN, NODEID).startswith(
        "postgresql://"
    )


def test_mint_and_drop_agree_on_the_parsed_database_name() -> None:
    """The drop guard reads the minted DSN with the same parser, so it
    recognises the name instead of refusing it."""
    dsn = _schema_destructive_test_dsn(KEYWORD_DSN, NODEID)
    target = psycopg.conninfo.conninfo_to_dict(dsn)["dbname"]

    assert target.startswith(_SCHEMA_DESTRUCTIVE_DB_PREFIX)
    # The fixtures may only destroy databases the safety allowlist owns.
    assert _is_safe_test_dbname(target) is True


def test_each_test_gets_its_own_database() -> None:
    assert _schema_destructive_db_name(
        NODEID, BASE_DB
    ) != _schema_destructive_db_name(OTHER_NODEID, BASE_DB)


def test_each_base_database_gets_its_own_disposable_database() -> None:
    """Two sessions on one server must not mint the same disposable name.

    The fixture's first act is to drop this database by name, so a local run
    and a CI xdist worker sharing a name would destroy each other's database
    mid-test. The base DSN's dbname carries the worker id, so folding it into
    the hash separates them.
    """
    assert _schema_destructive_db_name(
        NODEID, BASE_DB
    ) != _schema_destructive_db_name(NODEID, OTHER_BASE_DB)


def test_minted_names_stay_inside_the_allowlist_shape() -> None:
    """The name must survive both guards: the destroy allowlist and the
    CREATE DATABASE identifier check in ``_ensure_test_db_exists``."""
    for base in (BASE_DB, OTHER_BASE_DB, None):
        name = _schema_destructive_db_name(NODEID, base)
        assert name.startswith(_SCHEMA_DESTRUCTIVE_DB_PREFIX)
        assert _is_safe_test_dbname(name) is True
        assert name.replace("_", "").isalnum()
        assert len(name) <= 63


def test_mint_refuses_a_multi_host_dsn() -> None:
    """libpq folds several hosts into one comma-joined value; packing that
    back into a URL authority yields a single host named ``h1%2Ch2``, which
    resolves nowhere. Refuse instead of minting a DSN for the wrong server."""
    with pytest.raises(RuntimeError, match="multi-host"):
        _schema_destructive_test_dsn(MULTI_HOST_DSN, NODEID)


def test_drop_refuses_a_database_it_did_not_mint() -> None:
    """The guard must refuse before it opens any connection."""
    with pytest.raises(RuntimeError, match="refusing to drop"):
        _drop_schema_destructive_test_db("postgresql://127.0.0.1:5433/okg_workspace")


# The database-free half of the isolation requirement: the two tests that drop a
# shared schema object must ask for the disposable fixture, not the shared
# session runtime. Whether the isolation holds against a live server is proved
# by the substrate-db lane in hosted CI; that a test silently slid back onto
# clean_runtime is catchable here, with no server.
DESTRUCTIVE_CASES = {
    "tests/substrate/test_mcp_tracing.py": (
        "test_trace_write_failure_does_not_break_tool_call"
    ),
    "tests/substrate/test_console_count_rollup_schema.py": (
        "test_interval_cutover_schema_migration_runs_on_legacy_registered_graph"
    ),
}


@pytest.mark.parametrize(("path", "func"), sorted(DESTRUCTIVE_CASES.items()))
def test_destructive_cases_request_the_disposable_fixture(
    path: str, func: str
) -> None:
    import ast
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
    tree = ast.parse((repo_root / path).read_text(encoding="utf-8"))
    matches = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == func
    ]
    assert matches, f"{path}::{func} is gone; update this guard"
    params = {arg.arg for arg in matches[0].args.args}

    assert "schema_destructive_runtime" in params, (
        f"{path}::{func} drops a shared schema object and must take "
        "schema_destructive_runtime"
    )
    assert "clean_runtime" not in params, (
        f"{path}::{func} must not also hold the shared session runtime"
    )


def test_the_disposable_fixture_exists() -> None:
    from tests.substrate import conftest

    assert hasattr(conftest, "schema_destructive_runtime")
