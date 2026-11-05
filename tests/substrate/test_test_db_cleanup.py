"""Witnesses for the end-of-process test-database cleanup (PACT test-db-cleanup).

Every test that touches Postgres creates its own uniquely named scratch
databases, derived from this worker's test database name, and passes them to
the cleanup explicitly; the rest check the cleanup registry and the root hook
without a database. None reads or drops the live ``_BUILT_TEMPLATES``.
"""
from __future__ import annotations

import re
import sys
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from tests.substrate import conftest as substrate_conftest
from tests.substrate.conftest import (
    _isolated_okg_workspace_dsn,
    _maintenance_dsn,
    _target_dbname,
    drop_test_databases,
    dsn_with_dbname,
    okg_test_template_dbname,
    require_okg_test_dsn,
)

pytestmark = [pytest.mark.postgres, pytest.mark.serial_db]


def _root_conftest():
    return sys.modules["tests.conftest"]


def _scratch_name(base_dsn: str, tag: str) -> str:
    return f"{_target_dbname(base_dsn)}_{tag}{uuid4().hex[:6]}"


def _existing(base_dsn: str, names: list[str]) -> set[str]:
    with psycopg.connect(_maintenance_dsn(base_dsn), autocommit=True) as conn:
        rows = conn.execute(
            "SELECT datname FROM pg_database WHERE datname = ANY(%s)", (names,)
        ).fetchall()
    return {row[0] for row in rows}


@pytest.fixture
def scratch():
    """Create scratch databases on the test server; drop leftovers after."""
    base_dsn = require_okg_test_dsn()
    created: list[str] = []

    def create(name: str) -> str:
        with psycopg.connect(_maintenance_dsn(base_dsn), autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        created.append(name)
        return name

    yield SimpleNamespace(base_dsn=base_dsn, create=create, created=created)
    with psycopg.connect(_maintenance_dsn(base_dsn), autocommit=True) as conn:
        for name in created:
            conn.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(name)
                )
            )


def _worker_with_template(scratch, dsn: str) -> list[str]:
    return [scratch.create(_target_dbname(dsn)), scratch.create(okg_test_template_dbname(dsn))]


def test_drops_exactly_the_given_worker_templates_and_isolated_clone(
    scratch, monkeypatch
) -> None:
    monkeypatch.delenv("OKG_TEST_KEEP_DBS", raising=False)
    worker_dsn = dsn_with_dbname(scratch.base_dsn, _scratch_name(scratch.base_dsn, "w"))
    clone_dsn = _isolated_okg_workspace_dsn(worker_dsn)
    targets = _worker_with_template(scratch, worker_dsn) + _worker_with_template(
        scratch, clone_dsn
    )
    bystander = scratch.create(_scratch_name(scratch.base_dsn, "u"))
    assert _existing(scratch.base_dsn, targets + [bystander]) == set(targets + [bystander])

    results = drop_test_databases([worker_dsn, clone_dsn])

    assert sorted(results) == sorted((name, True, None) for name in targets)
    assert _existing(scratch.base_dsn, targets + [bystander]) == {bystander}


def test_refuses_a_name_that_is_not_a_test_database(scratch, monkeypatch) -> None:
    monkeypatch.delenv("OKG_TEST_KEEP_DBS", raising=False)
    results = drop_test_databases([dsn_with_dbname(scratch.base_dsn, "postgres")])

    assert [(db, ok) for db, ok, _error in results] == [
        ("postgres_tpl", False),
        ("postgres", False),
    ]
    assert all(error.startswith("refused") for _db, _ok, error in results)
    assert _existing(scratch.base_dsn, ["postgres"]) == {"postgres"}


def test_keep_env_keeps_everything(scratch, monkeypatch) -> None:
    monkeypatch.setenv("OKG_TEST_KEEP_DBS", "1")
    worker_dsn = dsn_with_dbname(scratch.base_dsn, _scratch_name(scratch.base_dsn, "k"))
    targets = _worker_with_template(scratch, worker_dsn)

    assert drop_test_databases([worker_dsn]) == []
    assert _existing(scratch.base_dsn, targets) == set(targets)


def test_a_failed_drop_is_reported_not_raised(scratch, monkeypatch) -> None:
    monkeypatch.delenv("OKG_TEST_KEEP_DBS", raising=False)
    worker_dsn = dsn_with_dbname(scratch.base_dsn, _scratch_name(scratch.base_dsn, "f"))
    worker, template = _worker_with_template(scratch, worker_dsn)
    real_drop = substrate_conftest._drop_database_force

    def failing_drop(cur, dbname):
        if dbname == template:
            raise psycopg.errors.ObjectInUse(f"simulated: {dbname} is busy")
        real_drop(cur, dbname)

    monkeypatch.setattr(substrate_conftest, "_drop_database_force", failing_drop)
    # No password: the URL only has to be unreachable (port 1), and a password in it is a release-scan finding.
    unreachable = "postgres://nobody@127.0.0.1:1/okg_test_cleanup_unreachable"

    results = drop_test_databases([worker_dsn, unreachable])

    assert results[:2] == [
        (template, False, f"ObjectInUse: simulated: {template} is busy"),
        (worker, True, None),
    ]
    assert [(db, ok) for db, ok, _error in results[2:]] == [
        ("okg_test_cleanup_unreachable_tpl", False),
        ("okg_test_cleanup_unreachable", False),
    ]
    assert all("OperationalError" in error for _db, _ok, error in results[2:])
    assert _existing(scratch.base_dsn, [worker, template]) == {template}


class _Reporter:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def write_sep(self, sep: str, title: str) -> None:
        self.lines.append(f"{sep * 3} {title}")

    def write_line(self, line: str, **_markup) -> None:
        self.lines.append(line)


def test_worker_results_reach_the_controller_summary(scratch, monkeypatch) -> None:
    """The real hook drops a recorded database, hands the rows to the
    controller through workeroutput, and the summary names each failure;
    the session exit status is left alone."""
    monkeypatch.delenv("OKG_TEST_KEEP_DBS", raising=False)
    root = _root_conftest()
    worker_dsn = dsn_with_dbname(scratch.base_dsn, _scratch_name(scratch.base_dsn, "x"))
    targets = _worker_with_template(scratch, worker_dsn)
    unsafe_dsn = dsn_with_dbname(scratch.base_dsn, "postgres")
    # A stand-in record for this test only; the live one is not touched.
    monkeypatch.setattr(
        substrate_conftest,
        "_BUILT_TEMPLATES",
        {worker_dsn: {None}, unsafe_dsn: {None}},
    )
    worker_config = SimpleNamespace(
        workeroutput={}, workerinput={"workerid": "gw7"}, stash=pytest.Stash()
    )
    session = SimpleNamespace(config=worker_config, exitstatus=1)

    root._finish_test_db_cleanup(session)

    assert session.exitstatus == 1
    assert _existing(scratch.base_dsn, targets) == set()
    output = worker_config.workeroutput["test_db_cleanup"]
    assert output["worker"] == "gw7"

    controller_config = SimpleNamespace(stash=pytest.Stash())
    node = SimpleNamespace(workeroutput=worker_config.workeroutput, config=controller_config)
    root.pytest_testnodedown(node, None)
    reporter = _Reporter()
    root.pytest_terminal_summary(reporter, 1, controller_config)

    text = "\n".join(reporter.lines)
    assert "dropped 2 test databases; 2 drops failed" in text
    assert (
        "FAILED drop worker=gw7 database=postgres: "
        "refused: not a disposable test database name"
    ) in text
    assert "FAILED drop worker=gw7 database=postgres_tpl" in text


def test_without_xdist_the_summary_prints_in_process(scratch, monkeypatch) -> None:
    monkeypatch.delenv("OKG_TEST_KEEP_DBS", raising=False)
    root = _root_conftest()
    worker_dsn = dsn_with_dbname(scratch.base_dsn, _scratch_name(scratch.base_dsn, "m"))
    targets = _worker_with_template(scratch, worker_dsn)
    monkeypatch.setattr(substrate_conftest, "_BUILT_TEMPLATES", {worker_dsn: {None}})
    config = SimpleNamespace(stash=pytest.Stash())

    root._finish_test_db_cleanup(SimpleNamespace(config=config, exitstatus=0))
    reporter = _Reporter()
    root.pytest_terminal_summary(reporter, 0, config)

    assert _existing(scratch.base_dsn, targets) == set()
    assert "dropped 2 test databases; 0 drops failed" in "\n".join(reporter.lines)


def test_a_stand_in_session_drops_nothing(scratch, monkeypatch) -> None:
    """Tests that call pytest_sessionfinish with a fake session run
    mid-session; the hook must not drop the databases still in use."""
    monkeypatch.delenv("OKG_TEST_KEEP_DBS", raising=False)
    root = _root_conftest()
    worker_dsn = dsn_with_dbname(scratch.base_dsn, _scratch_name(scratch.base_dsn, "s"))
    targets = _worker_with_template(scratch, worker_dsn)
    monkeypatch.setattr(substrate_conftest, "_BUILT_TEMPLATES", {worker_dsn: {None}})
    config = SimpleNamespace(option=SimpleNamespace(), stash=pytest.Stash())

    root.pytest_sessionfinish(SimpleNamespace(config=config, exitstatus=0), 0)

    assert _existing(scratch.base_dsn, targets) == set(targets)
    assert config.stash.get(root._TEST_DB_CLEANUP_ROWS, None) is None


def test_the_substrate_cleanup_is_registered_once() -> None:
    """Importing the substrate fixtures registers their drop exactly once."""
    import tests

    registered = [
        cleanup
        for cleanup in tests.SESSION_CLEANUPS
        if cleanup is substrate_conftest.drop_built_test_databases
    ]
    assert len(registered) == 1


def test_the_root_conftest_does_not_name_the_substrate_fixtures() -> None:
    """The root hook reaches the substrate drop only through the registry.

    tests/ci/test_lane_scope_db_closure.py counts every dotted first-party
    string as an import; naming the substrate conftest in tests/conftest.py
    put the substrate fixtures into the install-path lane's closure.
    """
    import ast
    from pathlib import Path

    source = Path(_root_conftest().__file__).read_text(encoding="utf-8")
    strings = [
        node.value
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]
    dotted = re.compile(r"\btests(?:\.[A-Za-z_][A-Za-z0-9_]*)+")
    assert not [s for s in strings if dotted.search(s)]


def test_one_failing_cleanup_keeps_the_other_rows(monkeypatch) -> None:
    import tests

    def broken():
        raise RuntimeError("boom")

    monkeypatch.setattr(
        tests,
        "SESSION_CLEANUPS",
        [lambda: [("db_a", True, None)], broken, lambda: [("db_b", True, None)]],
    )
    rows = _root_conftest()._drop_this_process_test_databases()
    assert rows[0] == ("db_a", True, None)
    assert rows[2] == ("db_b", True, None)
    assert rows[1][1] is False and "RuntimeError: boom" in rows[1][2]
