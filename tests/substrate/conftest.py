"""Pytest fixtures for the substrate test suite.

Each xdist worker migrates its own test database once per session
and keeps the result as a template; every test that asks for a
clean database (``clean_runtime`` and friends) gets a fresh copy of
that template under the worker database's usual name (issue #2271).
See "Per-test database clones" below.

**Test-DB safety** (added 2026-04-26 after the post-bootstrap
audit caught `pytest tests/substrate/` blowing away the
okg-workspace graph): there is NO default DSN. `OKG_TEST_DSN` or
`OKG_TEST_DSN_TEMPLATE` names the database; with neither set,
:func:`require_okg_test_dsn` skips (issue #1913 -- the old
default was the shared `okg-pg` Compose service, which these
fixtures then truncated). Operators' `OKG_DSN` pointing at the
production `okg` DB is NEVER touched. The session also refuses
to start if the resolved test DSN points at a DB that contains
a published generation for any deployment other than `default`.

Every place under `tests/` that needs the test database calls
:func:`require_okg_test_dsn`; `tests/substrate/test_dsn_env_guard.py`
holds that rule for the whole tree.
"""
from __future__ import annotations

import hashlib
import json
import math
import contextlib
import os
import time
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

from okg.substrate.db.dsn import redact as _redact_dsn_for_display
from okg.substrate.db.role_lock import lock_roles_for_alter
from okg.substrate.dsn_form import (
    dsn_with_dbname as _dsn_with_dbname,
    is_keyword_form_dsn,
    keyword_form_refusal,
    postgresql_url,
)
from tests.pytest.chronos_reset import reset_chronos_graph


def dsn_with_dbname(base_dsn: str, dbname: str) -> str:
    """Compatibility wrapper for test modules that import this old fixture helper."""
    return _dsn_with_dbname(base_dsn, dbname)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--table-grid-report",
        action="store",
        default=None,
        help="Write the structured-table extraction publish evidence report.",
    )
    parser.addoption(
        "--embedding-report",
        action="store",
        default=None,
        help="Write the fail-closed embedding deployment evidence report.",
    )


@pytest.fixture(autouse=True)
def _clear_chronos_registry_cache():
    # The chronos registry cache (chronos_core.branching._common) plus the two
    # okg process-once guards (the schema-bootstrap guard in graph_store and the
    # MCP-grant guard in graph_store_internal.schema) are process-level for
    # production performance, where the schema is stable per process. Tests run
    # many schema states in one process, so clear all three around every test to
    # preserve the per-test isolation the rest of the harness already provides.
    # No-op dict/set clears when chronos was never touched.
    _clear_process_schema_caches()
    yield
    _clear_process_schema_caches()


def _clear_process_schema_caches() -> None:
    """Forget what this process has cached about the test database's schema.

    Called around every test, and by :func:`recreate_okg_test_database`: a
    clone keeps the DSN but replaces the database, so a cache keyed by DSN
    would otherwise describe tables the new database does not have (a
    mid-test reset then failed with ``relation okg.claim_effective does not
    exist``: Chronos still listed it as registered).

    The clone also restarts every sequence, so generation ids repeat across
    tests; caches keyed by generation id (the viz subtype-count cache) would
    hand one test's counts to the next, and are cleared too.
    """
    from chronos_core.branching._common import clear_all_registry_caches
    from okg.substrate import attributes as _attributes
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache
    from okg.substrate.graph_store_internal.schema import _clear_mcp_grant_guard
    from okg.substrate.publisher import native_view_stage as _native_view_stage
    from okg.viz import endpoints as _viz_endpoints

    from okg.substrate.nomos import reader_view as _reader_view

    clear_all_registry_caches()
    _reader_view.clear_reader_view_cache()
    clear_schema_ensured_cache()
    _clear_mcp_grant_guard()
    _attributes._TABLE_EXISTS_CACHE.clear()
    with _native_view_stage._INPUT_SIGNATURE_MATCH_CACHE_LOCK:
        _native_view_stage._INPUT_SIGNATURE_MATCH_CACHE.clear()
    _viz_endpoints._SUBTYPE_COUNTS_CACHE.clear()


# There is deliberately NO default DSN (issue #1913). The fixtures used to fall
# back to the shared `okg-pg` Compose service at 127.0.0.1:5433/okg_test, so a
# contributor who forgot the variable did not get a skip: on any machine where
# that service is up, the destructive fixtures truncated the database other
# sessions were using. An unset variable now skips instead.
#
# When you do set a DSN, prefer 127.0.0.1 over `localhost`: localhost resolves
# IPv6 ::1 first, and a Postgres bound to IPv4 only refuses that attempt, eating
# into connect_timeout. Under CI xdist+coverage load the IPv4 fallback can then
# exceed the budget -> a session-fixture ConnectionTimeout that cascades
# "failed on setup" to every DB test on that worker.
OKG_TEST_DSN_ENV = "OKG_TEST_DSN"
OKG_TEST_DSN_TEMPLATE_ENV = "OKG_TEST_DSN_TEMPLATE"
MISSING_TEST_DSN_SKIP_REASON = (
    "No test database configured: set OKG_TEST_DSN (or "
    "OKG_TEST_DSN_TEMPLATE with a {workerid} placeholder for xdist) to a "
    "database you own and may destroy. There is no default -- these "
    "fixtures truncate and drop, so they will not guess at a database. "
    "See the database section of docs/develop/index.md."
)
TRUST_EPHEMERAL_ENV = "OKG_TEST_DSN_TRUST_AS_EPHEMERAL"
QA_COST_REPORT_DIR_ENV = "OKG_QA_COST_REPORT_DIR"

# Connect timeout for the test-DB existence/safety/setup connects. Generous so a
# loaded CI runner (xdist + coverage instrumentation) does not spuriously time out
# during session setup; a genuinely-down Postgres still fails, just a few s slower.
_TEST_DB_CONNECT_TIMEOUT = 15

_QA_SESSION_STARTED: float | None = None
_QA_COST_EVENTS: list[dict[str, object]] = []
# The pytest phase ("setup", "call" or "teardown") running right now, or None
# outside any test. Every cost event records it as `during`, so a reset a
# fixture runs is told apart from one a test body runs mid-test.
_QA_CURRENT_PHASE: str | None = None


def _qa_cost_report_path() -> tuple[Path | None, str]:
    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    raw_dir = os.environ.get(QA_COST_REPORT_DIR_ENV)
    if not raw_dir:
        return None, worker
    path = Path(raw_dir)
    return path / f"worker-{worker}.jsonl", worker


def _record_qa_cost_event(
    phase: str,
    seconds: float,
    *,
    dsn: str | None = None,
    **details: object,
) -> None:
    _path, worker = _qa_cost_report_path()
    if _path is None:
        return
    if not math.isfinite(seconds) or seconds < 0:
        raise RuntimeError(
            f"cost decomposition seconds must be finite and non-negative: {seconds!r}"
        )
    event: dict[str, object] = {
        "phase": phase,
        "seconds": round(seconds, 6),
        "worker": worker,
    }
    if dsn:
        event["database"] = psycopg.conninfo.conninfo_to_dict(dsn).get("dbname")
    if _QA_CURRENT_PHASE is not None:
        event["during"] = _QA_CURRENT_PHASE
    event.update(details)
    _QA_COST_EVENTS.append(event)


def pytest_sessionstart(session: pytest.Session) -> None:
    global _QA_SESSION_STARTED
    _QA_COST_EVENTS.clear()
    _QA_SESSION_STARTED = time.perf_counter()


def pytest_collection_finish(session: pytest.Session) -> None:
    # Session start to the end of collection, per worker: what each worker
    # spends importing and collecting before its first test.
    if _QA_SESSION_STARTED is not None:
        _record_qa_cost_event(
            "collection",
            time.perf_counter() - _QA_SESSION_STARTED,
        )


def _timed_runtest_phase(phase: str, item: pytest.Item):
    """Hook-wrapper body shared by the setup, call and teardown timers.

    Setup and teardown are timed as well as the call because fixture work --
    the per-test database reset, module-scoped publishes, server start-up and
    shutdown -- runs there, and the lane's breakdown otherwise left about a
    fifth of the worker time unattributed.
    """
    global _QA_CURRENT_PHASE
    _QA_CURRENT_PHASE = phase
    started = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - started
        _QA_CURRENT_PHASE = None
        _record_qa_cost_event("pytest_" + phase, elapsed, nodeid=item.nodeid)


class _QaPhaseTimer:
    """The per-test phase timers, registered as a plugin, not as conftest hooks.

    A conftest's runtest hooks fire only for items under its own directory,
    so hooks defined here never timed the substrate-db lane's tests under
    tests/okg-workspace, tests/bench, tests/deployment or tests/sql, although
    they import these fixtures. A registered plugin's hooks fire for every
    item in the session.
    """

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_setup(self, item: pytest.Item):
        yield from _timed_runtest_phase("setup", item)

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_call(self, item: pytest.Item):
        yield from _timed_runtest_phase("call", item)

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_teardown(self, item: pytest.Item):
        yield from _timed_runtest_phase("teardown", item)


_QA_PHASE_TIMER_NAME = "okg-qa-phase-timer"


def pytest_configure(config: pytest.Config) -> None:
    if not config.pluginmanager.has_plugin(_QA_PHASE_TIMER_NAME):
        config.pluginmanager.register(_QaPhaseTimer(), _QA_PHASE_TIMER_NAME)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if _QA_SESSION_STARTED is not None:
        _record_qa_cost_event(
            "worker_wall",
            time.perf_counter() - _QA_SESSION_STARTED,
        )
    path, _worker = _qa_cost_report_path()
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "\n".join(json.dumps(event, sort_keys=True) for event in _QA_COST_EVENTS)
            + "\n",
            encoding="utf-8",
        )


def _refuse_keyword_form_dsn(env_var: str, dsn: str) -> None:
    """Refuse a keyword/value test DSN, naming the URL to set instead.

    libpq documents both spellings, so an operator setting the keyword form
    is doing nothing wrong. Chronos, however, selects its storage backend by
    URL SCHEME, and the keyword form has none: it was routed to the SQLite
    adapter, and the publish path then wrote a file named after the whole
    connection string, surfacing as a ``sqlite3.IntegrityError`` that never
    mentioned the DSN (issue #1971).

    :func:`chronos_core.branching.sql_adapters.connect_sql_database` now
    refuses the same string for every caller. This check is here as well so
    the refusal arrives at the variable the contributor set, before a
    session spends minutes reaching the publish path.

    The detection and the URL suggestion live in
    :mod:`okg.substrate.dsn_form`, shared with the production deployment-DSN
    resolvers so that both sides refuse exactly the same set of strings
    (issue #2001).
    """
    if not is_keyword_form_dsn(dsn):
        return

    raise RuntimeError(keyword_form_refusal(env_var, dsn))


def _resolve_okg_test_dsn() -> str | None:
    """The configured test DSN, or ``None`` when nothing is configured.

    ``None`` means skip. It never means "use the project's Compose service":
    see the comment on :data:`MISSING_TEST_DSN_SKIP_REASON`. A configured
    but keyword-form value is refused rather than returned; see
    :func:`_refuse_keyword_form_dsn`.
    """
    if explicit := os.environ.get(OKG_TEST_DSN_ENV):
        _refuse_keyword_form_dsn(OKG_TEST_DSN_ENV, explicit)
        return explicit

    if template := os.environ.get(OKG_TEST_DSN_TEMPLATE_ENV):
        workerid = os.environ.get("PYTEST_XDIST_WORKER", "master")
        resolved = template.format(workerid=workerid)
        _refuse_keyword_form_dsn(OKG_TEST_DSN_TEMPLATE_ENV, resolved)
        return resolved

    return None


def require_okg_test_dsn() -> str:
    """The configured test DSN, or skip this test.

    The single entry point for every caller under ``tests/`` that needs the
    test database: fixtures, module-scoped database builders, and the
    admin/maintenance DSNs derived from it. Callers must NOT reimplement the
    resolution with a default of their own -- that is the defect issue #1913
    documents, and `tests/substrate/test_dsn_env_guard.py` enforces it over
    the whole tree.

    Raises ``Skipped`` (never returns ``None``), so a caller can pass the
    result straight into ``make_conninfo`` or ``psycopg.connect`` without a
    None check that would otherwise resolve to the local socket.

    Raises ``RuntimeError`` when the configured value is a keyword/value
    connection string: that is a misconfiguration to fix, not a reason to
    skip. See :func:`_refuse_keyword_form_dsn`.
    """
    dsn = _resolve_okg_test_dsn()
    if dsn is None:
        pytest.skip(MISSING_TEST_DSN_SKIP_REASON)
    return dsn


def _target_dbname(dsn: str) -> str | None:
    return psycopg.conninfo.conninfo_to_dict(dsn).get("dbname")


#: Isolated okg-workspace clone prefix. Defined here rather than beside
#: `_isolated_okg_workspace_db_name` (which builds names with it) because
#: the safety allowlist below has to know it.
#:
#: The clone's name MUST begin with `okg_workspace_` so deployment-authority
#: resolves it as an isolated clone, which means it can never match the
#: `okg_test*` rule. Before this was allowlisted, every fixture touching the
#: clone refused on every local run, and the only way through was
#: `OKG_TEST_DSN_TRUST_AS_EPHEMERAL=1` — which disables the guard globally
#: and is far blunter than the case needs.
_ISOLATED_DB_PREFIX = "okg_workspace_test_isolated"


def _is_safe_test_dbname(dbname: str | None) -> bool:
    """Is this database one the test fixtures own and may destroy?

    Two families qualify: the `okg_test*` databases, and the isolated
    okg-workspace clone, whose name only `_isolated_okg_workspace_db_name`
    ever mints. Both are disposable by construction.
    """
    if dbname == "okg_test":
        return True
    if not dbname:
        return False
    return dbname.startswith("okg_test_") or dbname.startswith(
        _ISOLATED_DB_PREFIX
    )


def _assert_test_db_target_is_safe_for_mutation(
    dsn: str,
    *,
    operation: str,
) -> None:
    """Refuse destructive test-fixture operations on non-test databases."""
    if os.environ.get(TRUST_EPHEMERAL_ENV) == "1":
        return

    target_dbname = _target_dbname(dsn)
    if _is_safe_test_dbname(target_dbname):
        return

    pytest.exit(
        "\n\nREFUSING to run tests against "
        f"{_redact_dsn_for_display(dsn)!r}: {operation} would mutate database "
        f"{target_dbname!r}. Test fixtures only auto-create or clean "
        "databases named `okg_test`, `okg_test_*`, or "
        f"`{_ISOLATED_DB_PREFIX}*`.\n\n"
        "Fix: use worker-isolated test databases, for example "
        "`OKG_TEST_DSN_TEMPLATE=postgres://postgres:okg@127.0.0.1:5433/"
        "okg_test_{workerid}`. If this target really is disposable, set "
        f"`{TRUST_EPHEMERAL_ENV}=1`.\n",
        returncode=2,
    )


# ---------------------------------------------------------------------------
# Per-test database clones (issue #2271)
#
# Each xdist worker migrates its database once per session. The migrated
# database is then renamed to a TEMPLATE, and every test that asks for a clean
# database gets a fresh copy of it: the worker database is dropped and
# recreated with ``CREATE DATABASE <worker> TEMPLATE <template> STRATEGY
# FILE_COPY``. Nothing a test writes -- rows in any table, SQL functions,
# runtime-created tables such as ``okg.deployment_write_ownership``, DBOS
# schema, Chronos checkpoints, sequences -- can reach the next test, because
# the next test runs in a different physical database.
#
# The worker database keeps its NAME. Everything that addresses the test
# database -- the ``okg_migrated`` value, ``require_okg_test_dsn()``, a DSN a
# module computed at import -- still points at the right database after each
# clone. Only open connections are lost, which is why the pools are closed
# before the drop and the drop uses ``WITH (FORCE)``.
#
# The template refuses connections (``ALLOW_CONNECTIONS false``). PostgreSQL
# refuses to copy a database anyone is connected to, and TimescaleDB's
# background scheduler opens a session in every database that has the
# extension; refusing connections is what keeps it out.
# ---------------------------------------------------------------------------

#: Suffix of the per-worker template database. The result stays inside the
#: ``okg_test_`` (or isolated-clone) family that ``_is_safe_test_dbname``
#: allows, so every guard that protects the worker database protects its
#: template too.
_TEMPLATE_DB_SUFFIX = "_tpl"

#: PostgreSQL truncates identifiers longer than this, silently, so a template
#: name over the limit would collide with the worker database it came from.
_PG_MAX_IDENTIFIER_BYTES = 63

#: Bounded wait for the TimescaleDB scheduler (and any other session) to leave
#: the migrated database before it is renamed into the template.
_TEMPLATE_SESSION_WAIT_ATTEMPTS = 50
_TEMPLATE_SESSION_WAIT_SECONDS = 0.1


def _maintenance_dsn(dsn: str) -> str:
    """The ``postgres`` maintenance database on the same server as ``dsn``."""

    maint_info = dict(psycopg.conninfo.conninfo_to_dict(dsn))
    maint_info["dbname"] = "postgres"
    return psycopg.conninfo.make_conninfo(**maint_info)


def okg_test_template_dbname(dsn: str) -> str:
    """Name of the migrated template the worker database ``dsn`` is cloned from."""

    dbname = _target_dbname(dsn)
    if not dbname:
        raise RuntimeError(
            f"the test DSN names no database, so there is no template to clone: "
            f"{_redact_dsn_for_display(dsn)!r}"
        )
    template = f"{dbname}{_TEMPLATE_DB_SUFFIX}"
    if len(template.encode("utf-8")) > _PG_MAX_IDENTIFIER_BYTES:
        raise RuntimeError(
            f"test database name {dbname!r} is too long for a template "
            f"({template!r} exceeds {_PG_MAX_IDENTIFIER_BYTES} bytes). Use a "
            "shorter database name in OKG_TEST_DSN / OKG_TEST_DSN_TEMPLATE."
        )
    return template


def _refuse_shared_database_across_xdist_workers(dsn: str) -> None:
    """Refuse one explicit database shared by several xdist workers.

    Every worker drops and recreates its database once per test. With
    ``OKG_TEST_DSN`` naming a single database and more than one worker, the
    workers would drop that database out from under each other. The template
    variable gives each worker its own.
    """

    raw_count = os.environ.get("PYTEST_XDIST_WORKER_COUNT")
    if raw_count is None:
        # Not an xdist worker: one process, one database.
        return
    workers = int(raw_count)
    if workers <= 1:
        return
    template = os.environ.get(OKG_TEST_DSN_TEMPLATE_ENV)
    if (
        not os.environ.get(OKG_TEST_DSN_ENV)
        and template is not None
        and "{workerid}" not in template
    ):
        pytest.exit(
            "\n\nREFUSING to run: OKG_TEST_DSN_TEMPLATE has no {workerid} "
            f"placeholder, so all {workers} xdist workers would use one "
            f"database ({_target_dbname(dsn)!r}) and drop it out from under "
            "each other on every test. Add {workerid} to the database name.\n",
            returncode=2,
        )
    if os.environ.get(OKG_TEST_DSN_ENV):
        pytest.exit(
            "\n\nREFUSING to run: OKG_TEST_DSN names one database "
            f"({_target_dbname(dsn)!r}) but {workers} xdist workers are "
            "running. Each worker drops and recreates its test database for "
            "every test, so they would destroy each other's database. Set "
            "OKG_TEST_DSN_TEMPLATE with a {workerid} placeholder instead, or "
            "run without xdist (-p no:xdist).\n",
            returncode=2,
        )


def _refuse_unless_disposable_name(dbname: str | None, *, operation: str) -> None:
    """Refuse to drop, rename or clone over a database the fixtures do not own.

    Unlike :func:`_assert_test_db_target_is_safe_for_mutation`, this ignores
    ``OKG_TEST_DSN_TRUST_AS_EPHEMERAL``: that variable was written for the
    old truncate reset, and CI sets it on every job. A mistyped DSN under it
    used to empty some tables; with per-test clones it would drop the whole
    database. Only the names the fixtures mint (``okg_test*`` and the
    isolated okg-workspace clone) may be dropped.
    """
    if _is_safe_test_dbname(dbname):
        return
    pytest.exit(
        f"\n\nREFUSING to {operation}: database {dbname!r} is not a "
        "disposable test database. The per-test clone drops and recreates "
        "the test database, so it only acts on databases named `okg_test`, "
        f"`okg_test_*`, or `{_ISOLATED_DB_PREFIX}*`, whatever "
        f"{TRUST_EPHEMERAL_ENV} says. Point OKG_TEST_DSN / "
        "OKG_TEST_DSN_TEMPLATE at such a name.\n",
        returncode=2,
    )


def _drop_database_force(cur, dbname: str) -> None:
    _refuse_unless_disposable_name(dbname, operation="drop a database")
    cur.execute(
        sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
            sql.Identifier(dbname)
        )
    )


#: ``pg_stat_activity.backend_type`` of TimescaleDB's per-database scheduler.
TIMESCALEDB_SCHEDULER_BACKEND = "TimescaleDB Background Worker Scheduler"

#: ``pg_stat_activity.backend_type`` of an autovacuum worker.
AUTOVACUUM_WORKER_BACKEND = "autovacuum worker"


def template_source_sessions(cur, dbname: str) -> list[tuple]:
    """Every session on ``dbname`` except ``cur``'s own, oldest first.

    Each row is ``(pid, application_name, backend_type, state, query_start,
    backend_start, query)``, the query cut to 200 characters.
    """

    cur.execute(
        "SELECT pid, application_name, backend_type, state, query_start, "
        "backend_start, left(query, 200) FROM pg_stat_activity "
        "WHERE datname = %s AND pid <> pg_backend_pid() "
        "ORDER BY backend_start, pid",
        (dbname,),
    )
    return list(cur.fetchall())


def _describe_sessions(rows: list[tuple]) -> str:
    return "; ".join(
        f"pid={pid} application_name={app!r} backend_type={backend!r} "
        f"state={state!r} query_start={query_start} "
        f"backend_start={backend_start} query={query!r}"
        for pid, app, backend, state, query_start, backend_start, query in rows
    )


def create_database_from_template(cur, dbname: str, template: str) -> None:
    """``CREATE DATABASE dbname TEMPLATE template STRATEGY FILE_COPY``, safely.

    PostgreSQL refuses to copy a database another session is connected to
    (``ObjectInUse``, "source database ... is being accessed by other users").
    Two kinds of session can be there:

    * A client session: a connection this test process (a pool, a fixture,
      a test) left open. That is a leak, and this function never ends it. It
      waits as long as PostgreSQL itself would for a closing backend to
      exit, then raises with each session's application_name, state and
      query start.
    * TimescaleDB's background scheduler, which TimescaleDB starts in every
      database that has the extension and allows connections. It is not a
      leak; it is ended here, by pid and by backend type only.

    An autovacuum worker on the source is ignored: CREATE DATABASE signals
    it to exit on its own.

    While the copy runs, ``template`` refuses new connections, so the
    TimescaleDB launcher cannot start a new scheduler there and TimescaleDB's
    own CREATE DATABASE hook (which restarts the template's scheduler and
    races the copy) leaves it alone. The template's previous
    ``ALLOW_CONNECTIONS`` setting is restored afterwards. A database that
    allowed connections before is left without a scheduler until something
    starts one; test databases pause every TimescaleDB job anyway (see
    ``_pause_timescale_jobs``).
    """

    _refuse_unless_disposable_name(dbname, operation="create a database copy")
    _refuse_unless_disposable_name(template, operation="copy a database")
    cur.execute("SELECT datallowconn FROM pg_database WHERE datname = %s", (template,))
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"cannot copy {template!r}: it does not exist")
    allowed = bool(row[0])
    allow = sql.SQL("ALTER DATABASE {} WITH ALLOW_CONNECTIONS {}")
    if allowed:
        cur.execute(allow.format(sql.Identifier(template), sql.SQL("false")))
    try:
        def blocking() -> list[tuple]:
            # CREATE DATABASE signals autovacuum workers on the source itself,
            # so one is neither a leak nor a reason to wait.
            return [
                r for r in template_source_sessions(cur, template)
                if r[2] != AUTOVACUUM_WORKER_BACKEND
            ]

        remaining: list[tuple] = []
        for _attempt in range(_TEMPLATE_SESSION_WAIT_ATTEMPTS):
            remaining = blocking()
            if not remaining:
                break
            schedulers = [
                r[0] for r in remaining if r[2] == TIMESCALEDB_SCHEDULER_BACKEND
            ]
            if schedulers:
                cur.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE pid = ANY(%s) AND datname = %s AND backend_type = %s",
                    (schedulers, template, TIMESCALEDB_SCHEDULER_BACKEND),
                )
            time.sleep(_TEMPLATE_SESSION_WAIT_SECONDS)
        if remaining:
            # Read once more so the report names only sessions still there.
            remaining = blocking()
        if remaining:
            leaked = [r for r in remaining if r[2] != TIMESCALEDB_SCHEDULER_BACKEND]
            what = (
                "a session this test process or a fixture left open "
                "(close it before the copy)"
                if leaked
                else "the TimescaleDB scheduler, which did not exit when ended"
            )
            raise RuntimeError(
                f"cannot copy {template!r} into {dbname!r}: after "
                f"{_TEMPLATE_SESSION_WAIT_ATTEMPTS * _TEMPLATE_SESSION_WAIT_SECONDS:.0f}s "
                f"{what} is still connected: {_describe_sessions(remaining)}"
            )
        try:
            cur.execute(
                sql.SQL("CREATE DATABASE {} TEMPLATE {} STRATEGY FILE_COPY").format(
                    sql.Identifier(dbname), sql.Identifier(template)
                )
            )
        except psycopg.errors.ObjectInUse as exc:
            raise RuntimeError(
                f"cannot copy {template!r} into {dbname!r} "
                f"({exc.diag.message_primary}): a session joined "
                "after the check: "
                f"{_describe_sessions(template_source_sessions(cur, template))}"
            ) from exc
    finally:
        if allowed:
            cur.execute(allow.format(sql.Identifier(template), sql.SQL("true")))


def _drop_okg_test_database(dsn: str) -> None:
    """Drop the worker database ``dsn`` names (and its template), if present."""

    _assert_test_db_target_is_safe_for_mutation(
        dsn, operation="drop the test database before a fresh migrate"
    )
    dbname = _target_dbname(dsn)
    template = okg_test_template_dbname(dsn)
    _refuse_unless_disposable_name(dbname, operation="drop the test database")
    _refuse_unless_disposable_name(template, operation="drop the test template")
    with psycopg.connect(
        _maintenance_dsn(dsn),
        connect_timeout=_TEST_DB_CONNECT_TIMEOUT,
        autocommit=True,
    ) as conn, conn.cursor() as cur:
        _drop_database_force(cur, dbname)
        _drop_database_force(cur, template)


def _pause_timescale_jobs(dsn: str) -> int:
    """Unschedule every TimescaleDB background job in the migrated database.

    The migrate installs retention and compression policies on the fact and
    telemetry hypertables, plus TimescaleDB's own telemetry and job-history
    jobs. Their schedules live in the database, so every clone copies them,
    and the scheduler starts on each new clone and runs whatever is due at a
    moment no test chooses. A retention run that lands between a test's
    insert and its count deletes rows dated past the horizon, which is how
    test_fact_emitter_dedupe_scope.py saw ``assert 0 == 2`` on CI.
    ``scheduled => false`` is stored in the job row, so it survives the
    template-to-clone copy; the policies themselves (configuration, count)
    stay exactly as the migrate made them. A test that needs a job to run
    calls ``CALL run_job(<job_id>)`` itself.

    Returns the number of jobs paused. Raises if any job is still scheduled
    afterwards, or if anything but the ``scheduled`` flag changed (proc,
    hypertable, config, interval), instead of building a template that fires
    jobs anyway or carries different policies. ``lock_timeout`` bounds the
    wait on a job row a running job holds, so the session build fails with
    a lock error instead of hanging.
    """

    snapshot = (
        "SELECT job_id, proc_name, hypertable_schema, hypertable_name, "
        "config, schedule_interval FROM timescaledb_information.jobs "
        "ORDER BY job_id"
    )
    with psycopg.connect(
        dsn, connect_timeout=_TEST_DB_CONNECT_TIMEOUT, autocommit=True
    ) as conn, conn.cursor() as cur:
        cur.execute("SET lock_timeout = '30s'")
        cur.execute(snapshot)
        before = cur.fetchall()
        cur.execute(
            "SELECT count(*) FROM ("
            " SELECT alter_job(job_id, scheduled => false)"
            "   FROM timescaledb_information.jobs WHERE scheduled"
            ") AS paused"
        )
        paused = int(cur.fetchone()[0])
        cur.execute(
            "SELECT job_id, application_name FROM timescaledb_information.jobs "
            "WHERE scheduled ORDER BY job_id"
        )
        still_scheduled = cur.fetchall()
        cur.execute(snapshot)
        after = cur.fetchall()
    if still_scheduled:
        raise RuntimeError(
            "TimescaleDB jobs are still scheduled after pausing them, so every "
            f"clone of the test template would run them: {still_scheduled!r}"
        )
    if after != before:
        raise RuntimeError(
            "pausing the TimescaleDB jobs changed more than their schedule "
            f"flag: before={before!r} after={after!r}"
        )
    return paused


def build_okg_test_template(dsn: str) -> None:
    """Turn the freshly migrated worker database into its clone template.

    Every TimescaleDB background job is paused first (see
    ``_pause_timescale_jobs``). The migrated database is then closed to new
    connections, emptied of sessions, and renamed to ``<dbname>_tpl``; a
    first clone then takes back the worker name. A rename, not a copy, so
    building the template costs one clone.
    """

    from okg.substrate.db.pool import close_all

    _assert_test_db_target_is_safe_for_mutation(
        dsn, operation="turn the migrated test database into a template"
    )
    dbname = _target_dbname(dsn)
    template = okg_test_template_dbname(dsn)
    _refuse_unless_disposable_name(dbname, operation="rename a database into a template")
    close_all()
    _pause_timescale_jobs(dsn)
    with psycopg.connect(
        _maintenance_dsn(dsn),
        connect_timeout=_TEST_DB_CONNECT_TIMEOUT,
        autocommit=True,
    ) as conn, conn.cursor() as cur:
        _drop_database_force(cur, template)
        cur.execute(
            sql.SQL("ALTER DATABASE {} WITH ALLOW_CONNECTIONS false").format(
                sql.Identifier(dbname)
            )
        )
        remaining: list[tuple[int, str | None]] = []
        for _attempt in range(_TEMPLATE_SESSION_WAIT_ATTEMPTS):
            cur.execute(
                "SELECT pid, backend_type FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (dbname,),
            )
            remaining = cur.fetchall()
            if not remaining:
                break
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (dbname,),
            )
            time.sleep(_TEMPLATE_SESSION_WAIT_SECONDS)
        else:
            raise RuntimeError(
                f"sessions still open on {dbname!r} after "
                f"{_TEMPLATE_SESSION_WAIT_ATTEMPTS} attempts, so it cannot "
                f"become the clone template: {remaining!r}"
            )
        cur.execute(
            sql.SQL("ALTER DATABASE {} RENAME TO {}").format(
                sql.Identifier(dbname), sql.Identifier(template)
            )
        )
    recreate_okg_test_database(dsn, record_cost=False)


def recreate_okg_test_database(dsn: str, *, record_cost: bool = True) -> None:
    """Replace the worker database with a fresh clone of its migrated template.

    The per-test isolation step. Closes this process's pools, drops the worker
    database ``WITH (FORCE)`` (which also ends any session another thread or a
    subprocess left open), and recreates it from the template with
    ``STRATEGY FILE_COPY``. ``FILE_COPY`` copies the files and checkpoints;
    ``WAL_LOG`` would write the whole 26 MB schema into the write-ahead log on
    every test.
    """

    from okg.substrate.db.pool import close_all

    _assert_test_db_target_is_safe_for_mutation(
        dsn, operation="recreate the test database from its template"
    )
    dbname = _target_dbname(dsn)
    template = okg_test_template_dbname(dsn)
    _refuse_unless_disposable_name(dbname, operation="recreate the test database")
    started = time.perf_counter()
    close_all()
    with psycopg.connect(
        _maintenance_dsn(dsn),
        connect_timeout=_TEST_DB_CONNECT_TIMEOUT,
        autocommit=True,
    ) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (template,))
        if cur.fetchone() is None:
            raise RuntimeError(
                f"test database template {template!r} does not exist. It is "
                "built by the session fixture `okg_migrated` (or "
                "`okg_workspace_isolated_migrated`); request that fixture "
                "before recreating the database."
            )
        _drop_database_force(cur, dbname)
        dropped = time.perf_counter()
        create_database_from_template(cur, dbname, template)
    cloned = time.perf_counter()
    bm25_indexes = _reindex_bm25_indexes(dsn)
    _clear_process_schema_caches()
    _rewarm_chronos_bootstrap_guard(dsn)
    finished = time.perf_counter()
    if record_cost:
        # Recorded under the lane's existing ``reset`` phase: this IS the
        # per-test reset now. There is no Chronos-specific step any more; the
        # clone replaces the Chronos tables with everything else.
        _record_qa_cost_event(
            "reset",
            finished - started,
            dsn=dsn,
            relational_seconds=finished - started,
            chronos_seconds=0.0,
            reset_count=1,
            method="template_clone",
            drop_seconds=round(dropped - started, 6),
            clone_seconds=round(cloned - dropped, 6),
            bm25_reindex_seconds=round(finished - cloned, 6),
            bm25_reindexed=bm25_indexes,
        )


def _rewarm_chronos_bootstrap_guard(dsn: str) -> None:
    """Run the Chronos bootstrap on a fresh clone, as the old reset did.

    The table-emptying reset ended with ``reset_chronos_graph``, which runs
    ``ChronosGraphStore.from_connection(ensure_schema=True)`` on its own
    autocommit connection and so leaves this process's schema-bootstrap guard
    set for the worker database. A clone clears that guard (the database is
    new), and the first Chronos store a test then opens runs the bootstrap on
    the TEST's connection. ``ChronosBranchContext.ensure_metadata`` commits
    unconditionally, so a publish cycle that owns its caller's transaction
    (``Publisher.cycle(conn=..., atomic_precommit_verifier=...)``) had that
    transaction committed under it, and
    test_publisher_caller_verified_transaction.py failed its same-transaction
    check. Running the bootstrap here, on a separate autocommit connection,
    restores the state every test started from before the clones. The
    template always carries the Chronos tables (its migrate runs the
    bootstrap), so this finds them present; the bootstrap is idempotent.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(
        dsn, connect_timeout=_TEST_DB_CONNECT_TIMEOUT, autocommit=True
    ) as conn:
        present = conn.execute(
            "SELECT to_regclass('public._chronos_branch_interval_branches')"
        ).fetchone()[0]
        if present is None:
            return
        ChronosGraphStore.from_connection(conn, ensure_schema=True)


#: Every index built with pg_textsearch's ``bm25`` access method, found from
#: the catalog so an index an ``after_migrate`` hook added is covered too.
_BM25_INDEXES_SQL = """
    SELECT n.nspname, c.relname
      FROM pg_index i
      JOIN pg_class c ON c.oid = i.indexrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_am a ON a.oid = c.relam
     WHERE a.amname = 'bm25'
     ORDER BY n.nspname, c.relname
"""


_BM25_REINDEX_LOCK_TIMEOUT = "5s"


def _reindex_bm25_indexes(dsn: str) -> int:
    """Rebuild every BM25 index in a fresh clone; return how many.

    pg_textsearch keeps each BM25 index's in-memory part (its memtable) in
    server shared memory, keyed by the index OID alone. Every clone of one
    template has the same index OIDs, and ``DROP DATABASE`` does not evict the
    entry, so without a rebuild a new clone reads the dropped clone's
    memtable: ``DataCorrupted: pg_textsearch memtable record``, "could not
    read blocks", or empty lexical results (issue #2436). ``REINDEX`` resets
    the entry; it costs about 30 ms per clone.

    A session that still holds a lock on the table makes ``REINDEX`` fail
    after ``_BM25_REINDEX_LOCK_TIMEOUT`` instead of hanging until the test
    timeout.
    """

    with psycopg.connect(
        dsn, connect_timeout=_TEST_DB_CONNECT_TIMEOUT, autocommit=True
    ) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("SET lock_timeout = {}").format(
                sql.Literal(_BM25_REINDEX_LOCK_TIMEOUT)
            )
        )
        cur.execute(_BM25_INDEXES_SQL)
        indexes = cur.fetchall()
        for schema, name in indexes:
            cur.execute(
                sql.SQL("REINDEX INDEX {}").format(sql.Identifier(schema, name))
            )
    return len(indexes)


@pytest.fixture(scope="session")
def okg_dsn() -> str:
    """DSN for the test PG.

    Resolution order:
      1. `OKG_TEST_DSN` env (operator override; e.g. point
         at a separate test instance with its own pg_cron)
      2. `OKG_TEST_DSN_TEMPLATE` env with ``{workerid}``
         substitution for xdist worker-isolated DBs

    There is no third step. With neither variable set the
    fixture SKIPS (issue #1913); it does not fall back to the
    project's Compose service, because these fixtures truncate
    and drop and must never guess at a database someone else
    is using. A maintainer who wants the Compose service sets
    ``OKG_TEST_DSN`` to it explicitly.

    `OKG_DSN` is intentionally NOT consulted: it points at
    the deployment DB and trips ``_assert_dsn_is_safe`` on
    every dev machine that has a deployment installed.
    Operators who want a different test DSN set
    ``OKG_TEST_DSN`` explicitly.

    The test DB is auto-created on first use by the
    ``okg_migrated`` session fixture (see
    ``_ensure_test_db_exists``); operators do NOT need to
    run ``createdb okg_test`` manually.
    """
    return require_okg_test_dsn()


def _ensure_test_db_exists(dsn: str) -> None:
    """Create the test DB if it doesn't exist yet.

    Connects to ``dsn`` first; if that fails because the
    database doesn't exist, connects to the maintenance DB
    (``postgres``) on the same host/port/credentials and
    runs ``CREATE DATABASE``. Other connection errors
    surface unchanged so the migrate fixture's skip path
    catches them.
    """
    _assert_test_db_target_is_safe_for_mutation(
        dsn,
        operation="create or prepare the test database",
    )

    db_exists = False
    try:
        with psycopg.connect(dsn, connect_timeout=_TEST_DB_CONNECT_TIMEOUT):
            db_exists = True
    except psycopg.OperationalError as exc:
        msg = str(exc)
        if "does not exist" not in msg:
            # Not a missing-DB error — let it propagate so the
            # migrate fixture can decide whether to skip.
            raise

    # Parse the test DSN, swap dbname=postgres, run CREATE when
    # needed. We still continue to extension installation for an
    # existing DB so newly required extensions land on old okg_test
    # databases.
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    target_dbname = info.get("dbname")
    if not target_dbname:
        # No dbname to create; let the original error stand.
        return
    if not db_exists:
        maint_info = dict(info)
        maint_info["dbname"] = "postgres"
        maint_dsn = psycopg.conninfo.make_conninfo(**maint_info)
        try:
            with psycopg.connect(
                maint_dsn, connect_timeout=_TEST_DB_CONNECT_TIMEOUT, autocommit=True,
            ) as conn, conn.cursor() as cur:
                # quote_ident equivalent — dbname is operator-supplied
                # but constrained by the conninfo parser. Use a literal
                # check to defend.
                if not target_dbname.replace("_", "").isalnum():
                    raise RuntimeError(
                        f"refusing to CREATE DATABASE with non-"
                        f"alphanumeric name {target_dbname!r}"
                    )
                try:
                    cur.execute(f'CREATE DATABASE "{target_dbname}"')
                except psycopg.errors.DuplicateDatabase:
                    # Race: someone else created it between our two
                    # connects. Fine — fall through to extension install
                    # which is idempotent.
                    pass
        except psycopg.OperationalError as exc:
            # Maintenance DB unreachable. Operator-fix territory.
            raise RuntimeError(
                f"could not auto-create test DB {target_dbname!r}: "
                f"{exc}. Fix: set OKG_TEST_DSN to a DSN whose "
                f"database already exists, or ensure the "
                f"maintenance `postgres` DB is reachable on the "
                f"same host."
            ) from exc

    # Install required extensions in the new DB. The compose's
    # init script (`ops/pg/initdb/00-extensions.sql`) installs them
    # in the default `okg` DB at first boot, but new DBs on the
    # same instance need their own `CREATE EXTENSION`.
    #
    # `pg_cron` is single-DB-per-instance per the compose's
    # `cron.database_name=okg` setting and is intentionally
    # excluded from the test DB's required extensions (the
    # `okg_migrated` fixture also drops it from migrate's required
    # set).
    test_extensions = (
        "btree_gist",
        "timescaledb",
        "vector",
        "pg_textsearch",
        "pg_trgm",
        "fuzzystrmatch",
        "btree_gist",
    )
    try:
        with psycopg.connect(
            dsn, connect_timeout=_TEST_DB_CONNECT_TIMEOUT, autocommit=True,
        ) as conn, conn.cursor() as cur:
            for ext in test_extensions:
                cur.execute(f"CREATE EXTENSION IF NOT EXISTS {ext}")
    except psycopg.OperationalError as exc:
        raise RuntimeError(
            f"could not install extensions in test DB "
            f"{target_dbname!r}: {exc}"
        ) from exc

    # Register the DB as ephemeral (idempotent): the janitor's drop
    # authority is this in-database marker — a name prefix alone can never
    # get a database swept. Existing unstamped okg_test_* DBs become
    # registered the next time their suite prepares them.
    from okg.substrate.db.management import stamp_ephemeral_database

    with psycopg.connect(
        dsn, connect_timeout=_TEST_DB_CONNECT_TIMEOUT, autocommit=True,
    ) as conn:
        stamp_ephemeral_database(
            conn,
            registered_by="tests.substrate.conftest",
            purpose="pytest substrate test database",
        )


def _assert_dsn_is_safe(dsn: str) -> None:
    """Refuse to use a DSN whose graph_generations contains a
    published generation for any real deployment. The truncate
    fixture would silently blow it away.

    `bootstrap` is the substrate seed generation created by
    migration, not deployment data.

    The running process opts the DSN out of the deployment-name
    allowlist check by setting ``OKG_TEST_DSN_TRUST_AS_EPHEMERAL=1``
    in its environment — that explicit signal replaces the prior
    `dbname contains "_test"` substring heuristic, which silently
    exempted operator dev databases whose names happened to match
    (e.g. ``mydev_test_environment``) and blocked test runners
    that didn't follow the convention.
    """
    if os.environ.get(TRUST_EPHEMERAL_ENV) == "1":
        return
    try:
        with psycopg.connect(dsn, connect_timeout=_TEST_DB_CONNECT_TIMEOUT) as conn, \
             conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('okg.graph_generations')"
            )
            if cur.fetchone()[0] is None:
                return  # fresh DB, no schema yet
            cur.execute(
                "SELECT DISTINCT deployment_name "
                "  FROM okg.graph_generations "
                " WHERE status = 'published'"
            )
            deployments = {r[0] for r in cur.fetchall()}
            allowed = {
                "bootstrap",
                "concept-test",
                "default",
                "doctor-test",
                "integrity-test",
                "narrowing-test",
                "semantic-linker-test",
                "test",
                "view-test",
            }
            forbidden = deployments - allowed
            if forbidden:
                pytest.exit(
                    f"\n\nREFUSING to run tests against {_redact_dsn_for_display(dsn)!r} "
                    f"— it has published generations for "
                    f"deployments {sorted(forbidden)}. The "
                    f"test fixture's TRUNCATE would destroy "
                    f"that data.\n\n"
                    f"Fix: point OKG_TEST_DSN at a disposable database of "
                    f"your own — a throwaway PostgreSQL container on a free "
                    f"port, not the shared project stack. The fixtures create "
                    f"the database if it does not exist yet.\n\n"
                    f"If this DSN really is an ephemeral test DB "
                    f"and you accept TRUNCATE, set "
                    f"`OKG_TEST_DSN_TRUST_AS_EPHEMERAL=1` to "
                    f"opt out of this check.\n",
                    returncode=2,
                )
    except psycopg.OperationalError:
        # Not reachable — the migrate fixture will pytest.skip.
        return


_CLUSTER_RUNTIME_ROLE_LOCK_KEY = (0x4F4B47, 0x2020)


def _ensure_cluster_runtime_roles(
    dsn: str,
    *,
    role_name: str = "okg_substrate",
) -> None:
    """Create the cluster-level roles the migrations grant to, BEFORE migrating.

    ``okg_substrate`` is created by the runtime bootstrap
    (``okg/substrate/runtime/sql/role_split.sql``), never by ``migrate``. A test
    database that was only migrated therefore has no such role, and any test
    naming it in ``SET ROLE`` or in ``has_table_privilege`` /
    ``has_function_privilege`` fails with ``UndefinedObject`` -- ``role
    "okg_substrate" does not exist`` (issue #2020).

    The ORDER is the whole point, and it is why this cannot be a fixture in the
    test module that needs the role. Every grant migrations 0014 and 0015 make
    to ``okg_substrate`` is guarded by ``IF EXISTS (SELECT 1 FROM pg_roles
    ...)``, and the migrations are ledgered: a role created after the session's
    migrate never receives those grants, and a later ``migrate`` does not
    replay them. Creating the role here, before ``migrate`` runs, is what makes
    the grants land -- so a module that asserts the role HOLDS the finalizer
    EXECUTE privilege gets the real migration's answer rather than a
    wrong-privilege failure.

    The check and create run under a transaction-scoped advisory lock in the
    maintenance database. Without that lock, two fresh xdist workers can both
    observe the cluster-wide role as absent and race ``CREATE ROLE``. The role
    is NOT dropped afterwards: it is shared cluster state that a real
    deployment carries, and a teardown drop would race any concurrent xdist
    worker mid-migration against the same cluster.

    Connects to the maintenance database rather than ``dsn`` because roles are
    cluster-scoped and the test database may not exist yet -- creating it is
    the very next step in ``okg_migrated``.
    """
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    info["dbname"] = "postgres"
    maintenance_dsn = psycopg.conninfo.make_conninfo(**info)
    with psycopg.connect(
        maintenance_dsn,
        connect_timeout=_TEST_DB_CONNECT_TIMEOUT,
    ) as conn:
        conn.execute(
            "SELECT pg_advisory_xact_lock(%s, %s)",
            _CLUSTER_RUNTIME_ROLE_LOCK_KEY,
        )
        present = conn.execute(
            "SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s",
            (role_name,),
        ).fetchone()
        if present is None:
            conn.execute(
                psycopg.sql.SQL("CREATE ROLE {}").format(
                    psycopg.sql.Identifier(role_name)
                )
            )


#: How long a worker waits for the cluster role lock before failing loudly. A
#: holder runs one ALTER ROLE or one CREATE ROLE, which takes milliseconds, so
#: reaching this bound means a stuck session, not a slow one.
_CLUSTER_ROLE_LOCK_TIMEOUT = "60s"


def alter_role_under_cluster_lock(
    dsn: str,
    statement: psycopg.sql.Composable,
    *,
    lock_role: str | None,
) -> None:
    """Run one ``ALTER ROLE`` on the test server, serialized cluster-wide.

    Roles are cluster-global: each is ONE ``pg_authid`` row shared by every
    database on the server. ``ALTER ROLE`` rewrites that row without first
    locking it, so two sessions altering the same role at the same moment do
    not queue -- the loser fails with ``tuple concurrently updated``. xdist
    workers each own a database on one server, so a per-test fixture that
    altered ``okg_mcp`` in every worker made that race a routine CI failure
    (Tests runs 35893854631 and 35817734765).

    The statement runs in the maintenance database, in the same transaction
    as ``pg_advisory_xact_lock`` on the key ``_ensure_cluster_runtime_roles``
    already takes for ``CREATE ROLE``. The ALTER commits before the lock is
    released, so the next worker to take the lock sees it. Advisory locks are
    keyed per database, so a lock in ``postgres`` cannot collide with any
    lock the product code takes inside a test database.

    The advisory lock only serializes this helper's own callers. Product code
    alters the same roles from inside test databases (``okg runtime
    bootstrap`` reconciling ``okg_mcp``), where this key does not reach, so
    ``lock_role`` names the role the statement alters and the helper also
    holds that role's cluster-wide lock (``lock_roles_for_alter``, issue
    #1985), the one the product writers take. Pass ``None`` only for a
    statement that alters no existing role (``CREATE ROLE``, ``GRANT``).
    """
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    info["dbname"] = "postgres"
    maintenance_dsn = psycopg.conninfo.make_conninfo(**info)
    with psycopg.connect(
        maintenance_dsn,
        connect_timeout=_TEST_DB_CONNECT_TIMEOUT,
    ) as conn:
        conn.execute(
            psycopg.sql.SQL("SET LOCAL lock_timeout = {}").format(
                psycopg.sql.Literal(_CLUSTER_ROLE_LOCK_TIMEOUT)
            )
        )
        conn.execute(
            "SELECT pg_advisory_xact_lock(%s, %s)",
            _CLUSTER_RUNTIME_ROLE_LOCK_KEY,
        )
        if lock_role is not None:
            lock_roles_for_alter(conn, [lock_role])
        conn.execute(statement)


def enable_okg_mcp_login(dsn: str) -> None:
    """Give ``okg_mcp`` LOGIN and the local password, under the cluster lock.

    This is the state ``okg runtime bootstrap`` leaves on a local deployment
    (``LOCAL_DATABASE_ROLE_PASSWORDS``), so tests that open the real MCP read
    pool as ``okg_mcp`` exercise the least-privilege role as deployed.
    """
    from okg.substrate.local_database_roles import LOCAL_DATABASE_ROLE_PASSWORDS

    alter_role_under_cluster_lock(
        dsn,
        psycopg.sql.SQL("ALTER ROLE okg_mcp LOGIN PASSWORD {}").format(
            psycopg.sql.Literal(LOCAL_DATABASE_ROLE_PASSWORDS["okg_mcp"])
        ),
        lock_role="okg_mcp",
    )


def ensure_builtin_dev_logins(dsn: str) -> None:
    """Give ``okg_mcp`` and ``app_rw`` LOGIN with their built-in passwords.

    ``ops/pg/initdb`` used to create both roles that way, and the MCP pool
    tests connect as them. Since issue #3273 it creates them NOLOGIN and
    ``okg db role-passwords --apply`` gives them random passwords. A
    disposable test server is the single-user case the built-in passwords
    remain for (the root conftest sets ``OKG_ALLOW_BUILTIN_ROLE_PASSWORDS=1``),
    so a fresh server is brought to the state the old first-boot script left.
    Read first; ALTER only a role that cannot log in, under the cluster role
    lock, and never revert (see ``okg_mcp_login_enabled``).
    """
    from okg.substrate.local_database_roles import LOCAL_DATABASE_ROLE_PASSWORDS

    with psycopg.connect(dsn, connect_timeout=_TEST_DB_CONNECT_TIMEOUT) as conn:
        rows = conn.execute(
            "SELECT rolname, rolcanlogin FROM pg_catalog.pg_roles "
            "WHERE rolname IN ('okg_mcp', 'app_rw')"
        ).fetchall()
    for role, can_login in rows:
        if can_login:
            continue
        alter_role_under_cluster_lock(
            dsn,
            psycopg.sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}").format(
                psycopg.sql.Identifier(role),
                psycopg.sql.Literal(LOCAL_DATABASE_ROLE_PASSWORDS[role]),
            ),
            lock_role=role,
        )


def _okg_mcp_can_login(dsn: str) -> bool:
    with psycopg.connect(dsn, connect_timeout=_TEST_DB_CONNECT_TIMEOUT) as conn:
        row = conn.execute(
            "SELECT rolcanlogin FROM pg_catalog.pg_roles WHERE rolname = 'okg_mcp'"
        ).fetchone()
    if row is None:
        pytest.fail(
            "migration did not create the okg_mcp role; the MCP read-pool "
            "tests have no least-privilege role to log in as"
        )
    return bool(row[0])


@pytest.fixture(scope="session")
def okg_mcp_login_enabled(okg_migrated: str) -> str:
    """Session-scoped: ``okg_mcp`` can log in with the local password.

    Set ONCE per worker session, under the cluster role lock, and never
    reverted. Reverting is what raced: the old per-test fixtures read
    ``rolcanlogin``, set LOGIN, and restored the value they had read, so a
    worker could switch the role back to NOLOGIN while another worker's test
    was still opening connections as it, besides colliding on the row. The
    role is shared cluster state on a test server, left as a local deployment
    leaves it -- the same reasoning ``_ensure_cluster_runtime_roles`` gives
    for never dropping ``okg_substrate``.
    """
    _okg_mcp_can_login(okg_migrated)  # fails loudly when the role is absent
    enable_okg_mcp_login(okg_migrated)
    return okg_migrated


@pytest.fixture
def okg_mcp_can_log_in(okg_mcp_login_enabled: str) -> str:
    """Per-test: ``okg_mcp`` can log in, checked by a READ only.

    A read of ``pg_roles`` cannot collide. Only when something else left the
    role NOLOGIN is LOGIN set again, and then through the same cluster lock.
    A fresh migration creates the role NOLOGIN. One test still restores the
    value it read (test_mcp_actionable_bounds_disposable.py, under the same
    lock, against a disposable server named by an environment variable) and
    can leave it NOLOGIN there. No other test may alter the role outside the
    lock: tests/substrate/test_shared_role_alter_is_locked.py reads every
    test source for it (issue #2554).
    """
    if not _okg_mcp_can_login(okg_mcp_login_enabled):
        enable_okg_mcp_login(okg_mcp_login_enabled)
    return okg_mcp_login_enabled


@pytest.fixture(scope="session")
def okg_migrated(okg_dsn: str) -> Iterator[str]:
    """Session-scoped: a freshly migrated worker database and its template.

    Runs once per test session. The worker database is dropped, created,
    migrated, and turned into the clone template (``<dbname>_tpl``); the
    worker name then holds a first clone. Tests that use this fixture
    WITHOUT ``clean_runtime`` share whatever the previous test on the worker
    left in that clone.

    ``pg_cron`` is dropped from the migrate's required-extensions list for
    the session, so substrate tests can run against a test DB that doesn't
    have pg_cron available (single-instance pg_cron constraint per
    `one-db-per-deployment`). The migrate still creates the same schema.
    Tests that actually exercise pg_cron-driven scheduling skip themselves
    when cron isn't installed.
    """
    yield from _migrated_okg_test_template(okg_dsn)


#: Templates this process has built, by worker DSN, with the ``after_migrate``
#: hooks each carries (``None`` = the plain migrate). Several conftests
#: re-export ``okg_migrated`` (tests/deployments, tests/okg-workspace, ...),
#: and pytest runs each re-export as its own session fixture. Without this
#: record every one of them would drop and re-migrate the worker database.
_BUILT_TEMPLATES: dict[str, set[object]] = {}


def _okg_test_template_exists(dsn: str) -> bool:
    with psycopg.connect(
        _maintenance_dsn(dsn),
        connect_timeout=_TEST_DB_CONNECT_TIMEOUT,
        autocommit=True,
    ) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (okg_test_template_dbname(dsn),),
        )
        return cur.fetchone() is not None


def _migrated_okg_test_template(
    okg_dsn: str,
    *,
    after_migrate=None,
    refuse_published_deployments: bool = True,
    record_cost: bool = True,
) -> Iterator[str]:
    """Build a fresh migrated database for this worker and make it the template.

    Shared by the substrate and views ``okg_migrated`` fixtures. The first
    time a process asks for ``okg_dsn``, the worker database is DROPPED and
    created anew, so nothing an earlier session left behind (rows, functions;
    cluster roles aside) reaches the template. ``after_migrate(dsn)`` runs on
    the migrated database before it becomes the template, for suite-specific
    schema such as deployment views.

    A later request in the same process for a template it already built (a
    re-exported ``okg_migrated``) only takes a fresh clone. A request for a
    hook the template does not carry rebuilds it with that hook.

    ``pg_cron`` stays out of the migrate's required extensions for the whole
    session, because tests migrate again mid-session (after a DBOS reset, for
    instance) against a server where pg_cron serves another database.
    """
    from okg.substrate.db import migrate as _migrate_mod

    original = dict(_migrate_mod.REQUIRED_EXTENSIONS)
    _migrate_mod.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        hooks = _BUILT_TEMPLATES.get(okg_dsn)
        if (
            hooks is not None
            and after_migrate in hooks
            and _okg_test_template_exists(okg_dsn)
        ):
            recreate_okg_test_database(okg_dsn, record_cost=False)
        else:
            migration_started = time.perf_counter()
            _build_fresh_okg_test_template(
                okg_dsn,
                after_migrate=after_migrate,
                refuse_published_deployments=refuse_published_deployments,
            )
            _BUILT_TEMPLATES[okg_dsn] = {None, after_migrate}
            if record_cost:
                _record_qa_cost_event(
                    "migration_setup",
                    time.perf_counter() - migration_started,
                    dsn=okg_dsn,
                    migration_count=1,
                )
        ensure_builtin_dev_logins(okg_dsn)
        yield okg_dsn
    finally:
        _migrate_mod.REQUIRED_EXTENSIONS.clear()
        _migrate_mod.REQUIRED_EXTENSIONS.update(original)


def _build_fresh_okg_test_template(
    okg_dsn: str,
    *,
    after_migrate,
    refuse_published_deployments: bool,
) -> None:
    _refuse_shared_database_across_xdist_workers(okg_dsn)
    # Refuse to run against a DB that hosts a real deployment -- before the
    # drop below, which would otherwise destroy it.
    if refuse_published_deployments:
        _assert_dsn_is_safe(okg_dsn)
    try:
        _drop_okg_test_database(okg_dsn)
    except psycopg.OperationalError as e:
        # Skip only when the server cannot be reached. An error the server
        # itself returned carries a SQLSTATE -- e.g. ObjectInUse when a
        # prepared transaction pins the database even against WITH (FORCE)
        # -- and must fail the session instead of skipping every test.
        if e.sqlstate is not None:
            raise
        pytest.skip(f"Postgres not reachable at {okg_dsn}: {e}")

    # Create the test DB. If the maintenance DB is also unreachable, skip
    # gracefully — the suite can't run without Postgres.
    try:
        _ensure_test_db_exists(okg_dsn)
    except RuntimeError as e:
        pytest.skip(f"Test DB unavailable: {e}")

    # Final reachability check. After auto-create succeeds, the
    # test DSN should connect cleanly.
    try:
        with psycopg.connect(okg_dsn, connect_timeout=_TEST_DB_CONNECT_TIMEOUT):
            pass
    except Exception as e:
        pytest.skip(f"Postgres not reachable at {okg_dsn}: {e}")

    _ensure_cluster_runtime_roles(okg_dsn)

    from okg.substrate.db import migrate as _migrate_mod
    from okg.substrate.db.management import apply_remove_job_runtime_subsystem

    # A second session fixture (the views ``okg_migrated``, with its own
    # after_migrate hook) can rebuild the template in a process that already
    # migrated once. The database was just dropped, so forget what this
    # process cached about the old one; otherwise the migrate trusts a stale
    # Chronos registry and fails on ``_chronos_branch_tables``.
    _clear_process_schema_caches()
    _migrate_mod.migrate(okg_dsn)
    apply_remove_job_runtime_subsystem(okg_dsn, confirm=True)
    if after_migrate is not None:
        after_migrate(okg_dsn)
    build_okg_test_template(okg_dsn)


# ---------------------------------------------------------------------------
# Legacy table-emptying reset, kept ONLY for four files (issue #2271).
#
# Every clean_* fixture now hands a test a fresh clone of the migrated
# template, so nothing outside a fixed table list can leak between tests. These
# four files still empty tables in place, and are deliberately left unchanged
# by the pull request that introduced the clones:
#
#   tests/bench/synth/test_governed_binding.py   -- resets MID-TEST between
#   tests/bench/synth/test_parity_replay.py      -- two runs that must share
#                                                    the installed catalog
#   tests/substrate/exchange/test_poros_http.py  -- resets the receiver's
#                                                    second database
#   tests/substrate/viz/test_regressions_2026_04_25.py -- module-scoped seed
#
# None of them has a blocking pull-request test lane yet, so editing them
# fails the lane-scope check; the per-PR step for nightly-only database files
# (issue #2324) will give them one. Migrate them then and delete this whole
# section. Do not add a new caller: use clean_runtime or
# reset_okg_test_runtime (a fresh clone) instead.
# ---------------------------------------------------------------------------

#: The four files above import these two lists; nothing in this module reads
#: them. Naming them here tells static analysis (CodeQL's unused-global rule)
#: that they are exported on purpose. Nothing star-imports this conftest.
__all__ = ["_DERIVED_RUNTIME_TABLES", "_RUNTIME_TABLES"]

_RUNTIME_TABLES = (
    # Per-DB owner stamp (P0 #419): reset between tests so the shared test DB —
    # into which many tests publish under DIFFERENT random deployment names —
    # starts unstamped each test and the first publish stamps its own deployment.
    "database_owner",
    "node_facts",
    "edge_facts",
    "nodes_live",
    "edges_live",
    "edge_fact_strandings",
    "link_cache",
    # Unresolved-route retry table + alias-collision records
    # (fix-projection-alias-identity #685/#686): runtime/derived identity state,
    # reset per test so recovery + collision assertions start clean.
    "link_retry",
    "extractor_retry_queue",
    "projection_remediation_batches",
    "projection_remediation_runs",
    "alias_collisions",
    "entity_identity_claims",
    "entity_resolution_rejections",
    "entity_merge_receipts",
    "source_retirement_receipts",
    # Branch-aware generation-ordinal registry (ivm-effective-attribute-view).
    # Per-generation runtime bookkeeping; reset per test so each test's
    # branch/ordinal assignments start clean in the shared test DB.
    "generation_ordinals",
    # Overlay range markers and ranges (migration 0044, storage redesign S2) are
    # keyed by the same per-branch ordinal. Reset with generation_ordinals: if the
    # ordinals restart at 1 while old markers stay ranges_synced, the next publish
    # is refused as out of order (issue #4063). The two tables have no foreign keys.
    "attribute_overlay_generations",
    "attribute_overlay_ranges",
    "history_generation_pins",
    "compacted_generations",
    "history_compaction_plans",
    "source_watermarks",
    "source_progress",
    "enricher_progress",
    "enricher_input_quarantines",
    "enricher_backfill_progress",
    # Figure-caption staging + retry-quarantine state (visual-caption-enricher):
    # crop bytes staged at discovery and the per-(visual, signature) VLM-failure
    # counter. Reset per test so caption/quarantine row-count assertions in the
    # shared test DB don't see figures leaked from a prior test.
    "visual_crop_blobs",
    "visual_caption_failures",
    "post_publish_projection_status",
    "projection_pending",
    "lease_audit",
    "mcp_trace_events",
    "telemetry_spans",
    "telemetry_events",
    "telemetry_metrics",
    "telemetry_artifacts",
    "telemetry_traces",
    "node_series",
    "external_probe_cache",
    "node_series_watermark",
    # Operator call samples (issue #2011), one table per retention class.
    "call_sample_hot",
    "call_sample_warm",
    "call_sample_cold",
    "call_sample_watermark",
    "mcp_call_log",
    "lens_overlay_log",
    "lens_putback_requests",
    "admission_record_refs",
    "admission_events",
    "admission_envelopes",
    "coordination_native_events",
    "coordination_native_approvals",
    "coordination_native_messages",
    "coordination_native_state",
    "coordination_native_target_enrollments",
    "coordination_native_control",
    "producer_profiles",
    "poros_delivery_attempts",
    "federation_route_events",
    "federation_peer_trust_pins",
    "federation_connection_requests",
    "federation_discovered_candidates",
    "federation_mailbox_events",
    "federation_mailbox_messages",
    "poros_incorporation_events",
    "poros_detached_object_manifests",
    "poros_replay_cache",
    "poros_inbox_messages",
    "poros_outbox_messages",
    "nomos_external_claims",
    "nomos_decisions",
    "nomos_permission_grants",
    "nomos_subject_bindings",
    "nomos_identity_providers",
    "nomos_config_activations",
    "nomos_config_sets",
    "nomos_policy_activations",
    "nomos_policy_bundles",
    # Opt-in indexed record authority (issue 1325, migration 0031). These are
    # per-run staging and sealed-authority rows keyed by source name, and the
    # payload tables are content-addressed and globally shared, so a row left
    # behind by one test is read as real authority by the next one. TRUNCATE
    # does not fire the row-level immutability triggers, so the reset is safe.
    "source_record_authority_pending_admissions",
    "source_record_authority_refs",
    "source_record_authority_identities",
    "source_record_authority_component_fields",
    "source_record_authority_components",
    "source_record_authority_records",
    "source_record_authority_operations",
    "source_record_authorities",
    "source_record_authority_payload_chunks",
    "source_record_authority_payloads",
)

_DERIVED_RUNTIME_TABLES = (
    "observation_current",
    "consistency_gate_results",
    "observation_events",
    "consistency_candidate_coverage",
    "consistency_candidates",
    "consistency_check_definitions",
    # Per-update equivalence-harness verdicts (PACT
    # consistency-complete-equivalence): reset per test so a series' stored
    # verdicts start clean in the shared test DB (generation ids reset too).
    "equivalence_verdicts",
    # Per-skip reuse certificates (PACT consistency-resolution-aware-reuse):
    # reset per test so a run's recorded skips start clean in the shared DB.
    "reuse_certificates",
    # Per-(deployment, generation, mode, category) charged-work ledger (PACT
    # consistency-maintenance-accounting): reset per test so a series' recorded
    # ledger rows start clean in the shared DB (generation ids reset too).
    "work_ledger",
    "derived_run_dependency_bindings",
    "derived_schedule_nodes",
    "derived_schedule_plans",
    "derived_artifact_dependencies",
    "derived_dirty_rule_audits",
    "derived_dirty_scopes",
    "derived_dirty_rule_runs",
    "derived_output_dependency_index",
    "derived_output_lineage_envelopes",
    "derived_output_slots",
    "derived_output_manifests",
    "derived_artifact_runs",
    "derived_relation_registry",
    "derived_artifact_definitions",
)

_EXISTING_OKG_TABLES_CACHE: dict[tuple[str, int, str, tuple[str, ...]], tuple[str, ...]] = {}


def _table_cache_key(
    cur,
    tables: tuple[str, ...],
) -> tuple[str, int, str, tuple[str, ...]]:
    info = cur.connection.info
    return (
        info.host or "",
        int(info.port or 0),
        info.dbname or "",
        tuple(tables),
    )


def _truncate_existing_okg_tables(
    cur,
    tables: tuple[str, ...],
    *,
    use_cache: bool = False,
) -> None:
    """Truncate the requested okg tables that exist in this DB.

    Session cleanup runs before migrations so stale local test DBs may
    not have tables introduced by newer migrations yet.
    """
    cache_key = _table_cache_key(cur, tables)
    existing = _EXISTING_OKG_TABLES_CACHE.get(cache_key) if use_cache else None
    if existing is None:
        cur.execute(
            """
            SELECT tablename
              FROM pg_tables
             WHERE schemaname = 'okg'
               AND tablename = ANY(%s)
            """,
            (list(tables),),
        )
        found = {row[0] for row in cur.fetchall()}
        existing = tuple(table for table in tables if table in found)
        if use_cache:
            _EXISTING_OKG_TABLES_CACHE[cache_key] = existing
    if not existing:
        return
    cur.execute(
        sql.SQL("TRUNCATE {} RESTART IDENTITY CASCADE").format(
            sql.SQL(", ").join(
                sql.Identifier("okg", table) for table in existing
            )
        )
    )


def _restore_bootstrap_generation(cur) -> None:
    cur.execute(
        """
        INSERT INTO okg.graph_generations
          (generation_id, deployment_name, status, catalog_version_id,
           published_at, correlation_id, invariant_summary, excluded_sources)
        VALUES
          (0, 'bootstrap', 'published', 1, now(), 'bootstrap:generation:0',
           '{"reason":"bootstrap generation zero"}'::jsonb, '[]'::jsonb)
        ON CONFLICT (generation_id) DO NOTHING
        """
    )


def _truncate_okg_derived_schema(cur) -> None:
    # Issue #974: TRUNCATE only the tables that hold rows. Same reasoning as
    # _dirty_okg_tables -- TRUNCATE costs per relation, and this sweep runs on
    # every per-test reset over a schema that is usually entirely empty. The
    # EXISTS probe reads zero heap pages on an empty table.
    cur.execute(
        """
        DO $clean_okg_derived$
        DECLARE
          rec RECORD;
          has_rows boolean;
        BEGIN
          FOR rec IN
            SELECT schemaname, tablename
              FROM pg_tables
             WHERE schemaname = 'okg_derived'
          LOOP
            EXECUTE format(
              'SELECT EXISTS (SELECT 1 FROM %I.%I)',
              rec.schemaname,
              rec.tablename
            ) INTO has_rows;
            IF has_rows THEN
              EXECUTE format(
                'TRUNCATE TABLE %I.%I',
                rec.schemaname,
                rec.tablename
              );
            END IF;
          END LOOP;
        END
        $clean_okg_derived$;
        """
    )


def _delete_dbos_workflow_status_if_present(cur) -> None:
    """Reset DBOS runtime queue rows when the shared test DB has DBOS tables."""
    cur.execute("SELECT to_regclass('dbos.workflow_status')")
    if cur.fetchone()[0] is not None:
        cur.execute("DELETE FROM dbos.workflow_status")


def _clean_runtime_subset(
    dsn: str,
    tables: tuple[str, ...],
    *,
    operation: str,
    reset_generations: bool = False,
    clean_derived_schema: bool = False,
    close_pools: bool = True,
) -> None:
    """Reset a declared runtime table family without a full substrate clean.

    ``reset_generations=True`` erases the published-generation history, and
    that history lives in two places: ``okg.graph_generations`` with the
    per-branch ``generation_ordinals`` registry, and the Chronos checkpoint
    tables (``public._chronos_*``) that ``latest_generation`` reads. Resetting
    only the first leaves a published Chronos checkpoint whose ordinal row is
    gone, so the next publish on that deployment finds a predecessor it cannot
    place and refused (the retired interval precondition,
    ``predecessor_ordinal_unavailable``). That is what made
    ``test_console_view_performance.py`` error whenever an earlier module had
    published under the same deployment in the same database (issue #2238).
    So a generation reset always resets Chronos too. (``clean_runtime`` and
    ``reset_okg_test_runtime`` now take a fresh clone instead, which has no
    Chronos checkpoints at all.)
    """
    if close_pools:
        from okg.substrate.db.pool import close_all

        close_all()
    _assert_test_db_target_is_safe_for_mutation(dsn, operation=operation)
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock_all();")
            _truncate_existing_okg_tables(cur, tables, use_cache=True)
            _delete_dbos_workflow_status_if_present(cur)
            if clean_derived_schema:
                _truncate_okg_derived_schema(cur)
            if reset_generations:
                cur.execute(
                    "DELETE FROM okg.graph_generations WHERE generation_id > 0;"
                )
                _restore_bootstrap_generation(cur)
    if reset_generations:
        reset_chronos_graph(dsn)
    if close_pools:
        from okg.substrate.db.pool import close_all

        close_all()


def reset_okg_test_runtime(dsn: str) -> None:
    """Give ``dsn`` a fresh clone of the migrated template, mid-test.

    The body of the ``clean_runtime`` fixture, exposed as a callable so a test
    that must compare TWO otherwise-identical publishes (e.g. the same corpus
    with and without a constraint-family annotation) can reset between them
    inside one test rather than smuggling state across test boundaries.

    It used to truncate a fixed list of runtime tables. It now replaces the
    whole database (issue #2271), so any connection the caller still holds to
    ``dsn`` is closed by the reset; open a new one afterwards.
    """
    recreate_okg_test_database(dsn)


@pytest.fixture
def clean_runtime(okg_migrated: str) -> Iterator[str]:
    """Per-test: a fresh clone of the migrated template database.

    The catalog the migration seeds is there; nothing an earlier test wrote
    is. The DSN is the worker database's usual address.
    """
    from okg.substrate.db.pool import close_all

    recreate_okg_test_database(okg_migrated)
    try:
        yield okg_migrated
    finally:
        close_all()


@pytest.fixture
def clean_live_runtime(okg_migrated: str) -> Iterator[str]:
    """Per-test fresh database; kept as a name for the live-graph tests."""
    from okg.substrate.db.pool import close_all

    recreate_okg_test_database(okg_migrated)
    try:
        yield okg_migrated
    finally:
        close_all()


@pytest.fixture
def clean_queue_runtime(okg_migrated: str) -> Iterator[str]:
    """Per-test fresh database; kept as a name for the queue/lease tests."""
    from okg.substrate.db.pool import close_all

    recreate_okg_test_database(okg_migrated)
    try:
        yield okg_migrated
    finally:
        close_all()


# ---------------------------------------------------------------------------
# okg-workspace isolated-clone runtime
#
# A handful of end-to-end tests (provision, okg-self e2e) drive the REAL
# okg-workspace deployment through migrate / publish / MCP. okg-workspace is a
# self-hosted-authority deployment: its manifest pins `database: okg_workspace`
# and `sync_root: <repo>`, so the deployment-authority guard
# (`resolved_target` / `publish_provenance_verdict`) refuses a generic test DB
# (`okg_test`, etc.) as an `identity: mismatch` target and refuses a
# file-derived migrate from a non-sync-root checkout (CI / worktree) as
# `foreign_checkout`.
#
# An ISOLATED CLONE — a DB whose name is `okg_workspace_<suffix>` — resolves to
# `identity: isolated`, which the guard explicitly allows as `isolated_clone`.
# That is the supported "run okg-workspace off to the side" path, so the test
# fixture provisions exactly that instead of relaxing the (intentionally
# fail-closed) guard. `_ISOLATED_DB_PREFIX` is defined next to
# `_is_safe_test_dbname`, which has to allowlist it.
# ---------------------------------------------------------------------------


def _isolated_okg_workspace_db_name(base_dsn: str) -> str:
    """Worker-unique isolated-clone DB name in the okg-workspace namespace.

    Must start with ``okg_workspace_`` so deployment-authority resolves it as
    an isolated clone (``identity: isolated``). Under xdist each worker's base
    DSN already carries a distinct dbname (the ``okg_test_{workerid}`` template
    the QA postgres lane sets); fold a sanitized form of it in so concurrent
    workers don't collide on one shared clone DB.
    """
    base_db = psycopg.conninfo.conninfo_to_dict(base_dsn).get("dbname") or "x"
    suffix = "".join(c if (c.isalnum() or c == "_") else "_" for c in base_db)
    return f"{_ISOLATED_DB_PREFIX}_{suffix}"


def _isolated_okg_workspace_dsn(base_dsn: str) -> str:
    """Derive an isolated-clone DSN (`okg_workspace_<suffix>`) on the same
    host/port/credentials as ``base_dsn`` (the ephemeral test server).

    Returns a ``postgres://`` URL (not a libpq keyword/value string): Chronos'
    ``connect_sql_database`` selects its backend by URL scheme, so a
    ``make_conninfo`` kv-string (empty urlparse scheme) would be misrouted to
    the SQLite adapter. Swap only the path component of the URL.
    """
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(base_dsn)
    return urlunsplit(
        parts._replace(path="/" + _isolated_okg_workspace_db_name(base_dsn)),
    )


_SCHEMA_DESTRUCTIVE_DB_PREFIX = "okg_test_schema_"

#: Bounded retry for the disposable-database drop. ``WITH (FORCE)`` already
#: terminates the backends inside the DROP itself, so a retry is only needed
#: for a client that reconnected in the window between two attempts.
_SCHEMA_DESTRUCTIVE_DROP_ATTEMPTS = 5
_SCHEMA_DESTRUCTIVE_DROP_RETRY_SECONDS = 0.5


def _schema_destructive_db_name(nodeid: str, base_dbname: str | None) -> str:
    """Name the disposable database that belongs to exactly one test.

    The hash folds in the BASE DSN's database name as well as the node id.
    Two pytest sessions against one server -- a local run beside a CI xdist
    worker, or two workers, whose base dbname carries the worker id (the
    ``okg_test_{workerid}`` template the QA postgres lane sets) -- would
    otherwise mint the SAME name for the same test. The fixture's first act
    is to terminate that database's backends and drop it by name, so each
    session would destroy the other's database mid-test.

    ``_isolated_okg_workspace_db_name`` folds the base dbname in for the same
    reason. The result stays inside the ``okg_test_`` allowlist family
    (:func:`_is_safe_test_dbname`) and inside the underscore-or-alphanumeric
    shape :func:`_ensure_test_db_exists` insists on, at 32 characters.
    """

    digest = hashlib.sha256(
        f"{base_dbname or ''}\x00{nodeid}".encode("utf-8")
    ).hexdigest()[:16]
    return f"{_SCHEMA_DESTRUCTIVE_DB_PREFIX}{digest}"


def _schema_destructive_test_dsn(base_dsn: str, nodeid: str) -> str:
    """Return a test-owned database URL unique to one destructive test.

    Parses ``base_dsn`` with ``conninfo_to_dict`` -- the same parser
    :func:`_drop_schema_destructive_test_db` uses -- so a keyword/value
    ``OKG_TEST_DSN`` (``host=... dbname=okg_test``) mints and drops the same
    database instead of minting nothing and refusing. Splitting the two
    helpers across two parsers meant a keyword DSN raised here while the drop
    side accepted it.

    Returns a ``postgres://`` URL rather than a ``make_conninfo`` kv-string
    for the reason :func:`_isolated_okg_workspace_dsn` documents: Chronos'
    ``connect_sql_database`` selects its backend by URL scheme, and a
    scheme-less kv-string is misrouted to the SQLite adapter.

    A multi-host DSN is REFUSED rather than rebuilt. libpq folds
    ``host=h1,h2 port=5433,5434`` into one comma-joined value, and packing
    that back into a URL authority yields ``h1%2Ch2:5433,5434`` -- a single
    host whose name contains a comma, which resolves nowhere. Failing loudly
    beats minting a DSN that silently points at the wrong server.
    """

    from urllib.parse import quote, urlencode

    info = psycopg.conninfo.conninfo_to_dict(base_dsn)
    user = info.pop("user", None)
    password = info.pop("password", None)
    host = info.pop("host", None)
    port = info.pop("port", None)
    base_dbname = info.pop("dbname", None)
    dbname = _schema_destructive_db_name(nodeid, base_dbname)

    for label, value in (("host", host), ("port", port)):
        if value is not None and "," in str(value):
            raise RuntimeError(
                "refusing to derive a schema-destructive test DSN from a "
                f"multi-host DSN ({label}={value!r}): the rebuilt URL would "
                "name one host called "
                f"{str(host)!r}. Point the test DSN at a single host:port."
            )

    netloc = ""
    if user:
        netloc += quote(str(user), safe="")
        if password:
            netloc += ":" + quote(str(password), safe="")
        netloc += "@"
    if host:
        # A socket directory (``/tmp``) is legal here once percent-encoded.
        netloc += quote(str(host), safe="")
    if port:
        netloc += f":{port}"

    query = urlencode(
        {key: str(value) for key, value in sorted(info.items()) if value is not None}
    )
    return postgresql_url(netloc, dbname, query)


def _schema_destructive_maintenance_dsn(dsn: str) -> str:
    """The ``postgres`` maintenance DSN on the same server as ``dsn``."""

    maint_info = dict(psycopg.conninfo.conninfo_to_dict(dsn))
    maint_info["dbname"] = "postgres"
    return psycopg.conninfo.make_conninfo(**maint_info)


def _drop_schema_destructive_test_db(dsn: str) -> None:
    """Drop only a database minted by :func:`_schema_destructive_test_dsn`.

    ``DROP DATABASE ... WITH (FORCE)`` (PostgreSQL 13+) terminates the other
    backends inside the drop itself. A separate ``pg_terminate_backend`` sweep
    followed by a plain DROP leaves a window in which a pooled client
    reconnects, the DROP fails with ``database is being accessed by other
    users``, and the disposable database leaks. The terminate sweep is kept
    only as the retry body for that narrow race.
    """

    info = psycopg.conninfo.conninfo_to_dict(dsn)
    target_dbname = info.get("dbname")
    if not (target_dbname or "").startswith(_SCHEMA_DESTRUCTIVE_DB_PREFIX):
        raise RuntimeError(
            f"refusing to drop unexpected schema-test database {target_dbname!r}"
        )
    maint_dsn = _schema_destructive_maintenance_dsn(dsn)
    drop = sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
        sql.Identifier(target_dbname)
    )
    last_exc: Exception | None = None
    with psycopg.connect(
        maint_dsn, connect_timeout=_TEST_DB_CONNECT_TIMEOUT, autocommit=True
    ) as conn:
        for attempt in range(_SCHEMA_DESTRUCTIVE_DROP_ATTEMPTS):
            try:
                conn.execute(drop)
                return
            except psycopg.errors.ObjectInUse as exc:
                last_exc = exc
            if attempt + 1 < _SCHEMA_DESTRUCTIVE_DROP_ATTEMPTS:
                conn.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()",
                    (target_dbname,),
                )
                time.sleep(_SCHEMA_DESTRUCTIVE_DROP_RETRY_SECONDS)
    raise RuntimeError(
        f"could not drop schema-test database {target_dbname!r} after "
        f"{_SCHEMA_DESTRUCTIVE_DROP_ATTEMPTS} attempts: {last_exc}"
    )


@pytest.fixture
def schema_destructive_runtime(
    okg_dsn: str, request: pytest.FixtureRequest
) -> Iterator[str]:
    """A disposable migrated database for a test that drops schema objects."""

    dsn = _schema_destructive_test_dsn(okg_dsn, request.node.nodeid)

    # Mirror ``okg_migrated``: without Postgres the suite cannot run, so skip
    # rather than error out of every destructive test's setup.
    try:
        with psycopg.connect(
            _schema_destructive_maintenance_dsn(dsn),
            connect_timeout=_TEST_DB_CONNECT_TIMEOUT,
        ):
            pass
    except Exception as exc:  # noqa: BLE001 - any connect failure means skip
        pytest.skip(f"Postgres not reachable at {okg_dsn}: {exc}")

    # Start pristine: a previous run killed mid-test can leave this name behind.
    _drop_schema_destructive_test_db(dsn)
    from okg.substrate.db import migrate as _migrate_mod
    original = dict(_migrate_mod.REQUIRED_EXTENSIONS)
    _migrate_mod.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        # Inside the try: a failure after CREATE DATABASE (extension install,
        # migrate) must still reach the drop in ``finally``, or an unstamped
        # database leaks on the server.
        _ensure_test_db_exists(dsn)
        _migrate_mod.migrate(dsn)
        from okg.substrate.db.management import apply_remove_job_runtime_subsystem

        apply_remove_job_runtime_subsystem(dsn, confirm=True)
        yield dsn
    finally:
        from okg.substrate.db.pool import close_all

        close_all()
        _migrate_mod.REQUIRED_EXTENSIONS.clear()
        _migrate_mod.REQUIRED_EXTENSIONS.update(original)
        _drop_schema_destructive_test_db(dsn)


@pytest.fixture(scope="session")
def okg_workspace_isolated_migrated(okg_dsn: str) -> Iterator[str]:
    """Session-scoped: an isolated clone of okg-workspace's DB, migrated.

    Same fresh-migrate-then-template path as ``okg_migrated`` (pg_cron dropped
    from the required extensions for the test server) but against a DB named
    ``okg_workspace_<suffix>`` so deployment-authority resolution treats it as
    an isolated clone rather than a mismatch. The database is dropped and
    rebuilt each session: catalog ownership is keyed to the checkout's
    fingerprints and git-dirty state, so a database reused across local runs
    would trip ``catalog_ownership_mismatch``.

    The published-deployment guard is not applied: this database exists to
    hold okg-workspace publishes, and its name is minted only by
    ``_isolated_okg_workspace_db_name``.
    """
    iso_dsn = _isolated_okg_workspace_dsn(okg_dsn)
    yield from _migrated_okg_test_template(
        iso_dsn,
        refuse_published_deployments=False,
        record_cost=False,
    )


@pytest.fixture
def okg_workspace_isolated_runtime(
    okg_workspace_isolated_migrated: str,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[str]:
    """Per-test isolated-clone okg-workspace runtime.

    A fresh clone of the migrated template (like ``clean_runtime``): the
    migration's pristine catalog, no write-ownership claim, no rows. For the
    duration of the test ``OKG_DSN`` points at the isolated clone and Nomos
    runtime enforcement is ``off``.

    okg-workspace's manifest declares ``nomos.runtime_enforcement: enforce``;
    a freshly-migrated DB has no active Nomos policy bundle, so the publisher /
    source-ingest paths default-deny (``nomos.default_deny.no_active_bundle``).
    These harness tests exercise the publish/MCP pipeline, not Nomos policy, so
    they run under the explicit permissive posture via the canonical
    ``OKG_NOMOS_RUNTIME_ENFORCEMENT`` override (the same single source of truth
    the runtime reads); Nomos enforcement itself is covered by the dedicated
    ``tests/substrate/nomos/`` suites.
    """
    iso_dsn = okg_workspace_isolated_migrated
    from okg.substrate.db.pool import close_all

    recreate_okg_test_database(iso_dsn, record_cost=False)
    monkeypatch.setenv("OKG_DSN", iso_dsn)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    try:
        yield iso_dsn
    finally:
        close_all()


@pytest.fixture
def dbos_test_runtime():
    """Pytest fixture for tests that need a clean DBOS singleton.

    Performs the documented init cycle on entry and tears down on exit:
        DBOS.destroy()
        DBOS(DBOSConfig(system_database_url=test_dsn,
                        database_url=test_dsn))
        DBOS.reset_system_database()
        DBOS.launch()
        yield
        DBOS.destroy()

    Per Decision 11 of `rebuild-substrate-on-dbos`, `system_database_url`
    and `database_url` MUST point to the same Postgres database; the
    DBOS workflow-state schema and the OKG fact tables coexist in one
    database. The fixture honors this by passing the same DSN as both
    URLs.

    Resolves the DSN with ``require_okg_test_dsn()``, the resolver the
    `clean_runtime` / `okg_migrated` fixtures use: ``OKG_TEST_DSN``, else
    this worker's database from ``OKG_TEST_DSN_TEMPLATE``. It SKIPS when
    neither is set rather than guessing at a database (issue #1913):
    `DBOS.reset_system_database()` drops the whole database the DSN names
    (DBOS 2.26 runs DROP DATABASE ... WITH (FORCE)), so the target must
    also pass the okg_test_* name check. CI sets only
    the template, so reading ``OKG_TEST_DSN`` alone skipped every
    dbos_integration test in the nightly coverage lane. Tests that
    use both `clean_runtime`
    AND `dbos_test_runtime` therefore share one Postgres database —
    which matches the production contract; if it surfaces concurrency
    issues those issues exist in production too.

    Tests using this fixture should be marked
    ``@pytest.mark.dbos_integration`` since the DBOS init cycle is
    slow; CI should run them on a separate (e.g., nightly) job rather
    than on every PR. Per `sdk-design.md` §2.6.

    The fixture is a Phase 1 §2.6.3 deliverable. Phase 2's
    ``build-deployment-sdk`` proposal extends this with a richer
    ``mock_dbos`` shim for fast unit tests that don't need real
    workflow durability.
    """
    # Issue #1913: this used to default to the shared `okg-pg` Compose
    # service, and `DBOS.reset_system_database()` below wipes whatever it is
    # pointed at. The resolver skips when no test database is configured.
    # Resolved BEFORE importorskip so the skip reason names the variables
    # rather than the optional dependency.
    test_dsn = require_okg_test_dsn()
    # The reset runs DROP DATABASE on the database the DSN names, so hold it
    # to the same test-database name rule as recreate_okg_test_database.
    _assert_test_db_target_is_safe_for_mutation(
        test_dsn, operation="DBOS.reset_system_database drops the database"
    )

    pytest.importorskip("dbos")

    from dbos import DBOS, DBOSConfig

    # Defensive destroy in case a previous fixture left state.
    with contextlib.suppress(Exception):
        DBOS.destroy()

    cfg = DBOSConfig(
        name="okg-test-runtime",
        system_database_url=test_dsn,
        database_url=test_dsn,
    )
    DBOS(config=cfg)
    DBOS.reset_system_database()
    DBOS.launch()
    try:
        yield DBOS
    finally:
        DBOS.destroy()
