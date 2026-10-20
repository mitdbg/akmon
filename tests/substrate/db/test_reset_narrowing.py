"""Each database test gets a fresh clone of the migrated template (#2271).

This file used to pin the per-test TRUNCATE narrowing of issue #974. That
reset cleaned a fixed list of runtime tables, and everything outside the list
-- runtime-created tables such as ``okg.deployment_write_ownership``, SQL
functions, DBOS tables, Chronos checkpoints -- survived into the next test.
Those survivors were the mechanism behind #2247, #2250, #2238 and #1667.

The reset now replaces the whole database: the worker database is dropped and
recreated from a template that was migrated once per session. The file keeps
its name because a lane row names it; the tests pin the new contract:

  * everything a test writes is gone after the reset, including objects no
    list names, while the migration's own seed rows are back;
  * the template refuses connections, so nothing can dirty it or block a clone;
  * the template's TimescaleDB jobs are paused, so no retention or compression
    run fires in a clone at a moment no test chose;
  * a reset without a template, and one explicit database shared by several
    xdist workers, are refused loudly instead of guessing.
"""
from __future__ import annotations

import os
import time

import psycopg
import pytest
from psycopg import sql

from tests.substrate import conftest as substrate_conftest
from tests.substrate.conftest import (
    okg_test_template_dbname,
    recreate_okg_test_database,
)


def _probe_state(dsn: str) -> dict[str, object]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.node_facts")
        node_facts = cur.fetchone()[0]
        cur.execute(
            "SELECT to_regprocedure('okg.pytest_probe_2271()') IS NOT NULL"
        )
        has_function = cur.fetchone()[0]
        cur.execute("SELECT to_regclass('okg.pytest_probe_2271_table') IS NOT NULL")
        has_table = cur.fetchone()[0]
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM okg.graph_generations "
            "WHERE generation_id = 0)"
        )
        has_bootstrap_generation = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM okg.catalog_versions")
        catalog_versions = cur.fetchone()[0]
        cur.execute("SELECT nextval('okg.edge_facts_fact_id_seq')")
        next_edge_fact_id = cur.fetchone()[0]
    return {
        "node_facts": node_facts,
        "function": has_function,
        "table": has_table,
        "bootstrap_generation": has_bootstrap_generation,
        "catalog_versions": catalog_versions,
        "next_edge_fact_id": next_edge_fact_id,
    }


@pytest.mark.postgres
def test_the_reset_removes_what_no_table_list_names(clean_runtime: str) -> None:
    """Rows, a function, a runtime-created table and a sequence all reset.

    The old reset left the function (#1667) and runtime-created tables such as
    the catalog ownership table (#2247) behind. A clone cannot: the next test
    runs in a different physical database.
    """
    fresh = _probe_state(clean_runtime)
    assert fresh["node_facts"] == 0
    assert fresh["function"] is False
    assert fresh["table"] is False
    assert fresh["bootstrap_generation"] is True
    assert fresh["catalog_versions"] > 0
    # _probe_state drew one value from the sequence; reset to measure again.
    recreate_okg_test_database(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES ('probe:2271', 'Probe', 'probe',
                    '{}'::jsonb, '{}'::jsonb, now(),
                    'probe:2271', gen_random_uuid(), 'I')
            """
        )
        cur.execute(
            "CREATE FUNCTION okg.pytest_probe_2271() RETURNS int "
            "LANGUAGE sql AS 'SELECT 1'"
        )
        cur.execute("CREATE TABLE okg.pytest_probe_2271_table (id int)")
        cur.execute(
            "SELECT nextval('okg.edge_facts_fact_id_seq') "
            "FROM generate_series(1, 5)"
        )
        cur.execute("DELETE FROM okg.graph_generations WHERE generation_id = 0")

    dirty = _probe_state(clean_runtime)
    assert dirty["node_facts"] == 1
    assert dirty["function"] is True
    assert dirty["table"] is True
    assert dirty["bootstrap_generation"] is False
    assert dirty["next_edge_fact_id"] > fresh["next_edge_fact_id"]

    recreate_okg_test_database(clean_runtime)

    assert _probe_state(clean_runtime) == fresh


@pytest.mark.postgres
def test_the_template_refuses_connections(clean_runtime: str) -> None:
    """Nobody -- a test, the TimescaleDB scheduler -- can open the template.

    PostgreSQL refuses to copy a database with open sessions, and a session in
    the template could also change what every later clone starts from.
    """
    template_dsn = substrate_conftest.dsn_with_dbname(
        clean_runtime, okg_test_template_dbname(clean_runtime)
    )
    with pytest.raises(psycopg.OperationalError, match="not currently accepting"):
        psycopg.connect(template_dsn, connect_timeout=5).close()


@pytest.mark.postgres
def test_a_clone_keeps_every_timescale_policy_but_schedules_none(
    clean_runtime: str,
) -> None:
    """The template pauses every TimescaleDB job, and the pause survives
    the copy into each clone.

    Before, each clone inherited the migrate's due retention and compression
    jobs, and the scheduler ran them on the new clone at a moment no test
    chose: a retention run between an insert and its count deleted rows
    dated past the horizon (``assert 0 == 2`` in
    test_fact_emitter_dedupe_scope.py). The policies themselves must still
    be there, unchanged, for the tests that read them.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT job_id, proc_name, hypertable_name, scheduled "
            "FROM timescaledb_information.jobs ORDER BY job_id"
        )
        jobs = cur.fetchall()
    policies = {
        (proc, table) for _job, proc, table, _scheduled in jobs if table
    }
    for table in ("node_facts", "edge_facts"):
        assert ("policy_retention", table) in policies, jobs
        assert ("policy_compression", table) in policies, jobs
    assert [job for job in jobs if job[3]] == [], jobs


@pytest.mark.postgres
def test_a_test_that_needs_a_paused_job_can_still_run_it(
    clean_runtime: str,
) -> None:
    """Pausing unschedules a job; it does not disable it. A test that needs
    retention runs it explicitly, when it chooses."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES ('probe:paused-job', 'Probe', 'probe',
                    '{}'::jsonb, '{}'::jsonb, now() - interval '400 days',
                    'probe:paused-job', gen_random_uuid(), 'I')
            """
        )
        cur.execute(
            "SELECT job_id FROM timescaledb_information.jobs "
            "WHERE proc_name = 'policy_retention' "
            "AND hypertable_schema = 'okg' AND hypertable_name = 'node_facts'"
        )
        (job_id,) = cur.fetchone()
        cur.execute(
            "SELECT count(*) FROM okg.node_facts WHERE dedupe_key = 'probe:paused-job'"
        )
        assert cur.fetchone()[0] == 1
        cur.execute("CALL run_job(%s)", (job_id,))
        cur.execute(
            "SELECT count(*) FROM okg.node_facts WHERE dedupe_key = 'probe:paused-job'"
        )
        assert cur.fetchone()[0] == 0


_JOB = (1000, "policy_retention", "okg", "node_facts", {"drop_after": "180 days"}, "1 day")


@pytest.mark.parametrize(
    ("snapshot_after", "still_scheduled", "refusal"),
    [
        pytest.param(
            [_JOB],
            [(1000, "Retention Policy [1000]")],
            r"still scheduled.*Retention Policy \[1000\]",
            id="job-still-scheduled",
        ),
        pytest.param(
            [(*_JOB[:4], {"drop_after": "7 days"}, _JOB[5])],
            [],
            r"changed more than their schedule flag",
            id="config-changed",
        ),
    ],
)
def test_the_template_build_refuses_a_pause_it_cannot_verify(
    monkeypatch: pytest.MonkeyPatch,
    snapshot_after: list[tuple[object, ...]],
    still_scheduled: list[tuple[int, str]],
    refusal: str,
) -> None:
    """The build stops, instead of producing a template whose clones still
    run a job or carry a different policy. A fake connection feeds the
    post-pause checks; it proves the checks refuse, not that alter_job
    behaves (the postgres tests above prove that)."""

    results = [[_JOB], still_scheduled, snapshot_after]

    class _Cursor:
        def execute(self, *_args: object) -> None:
            return None

        def fetchone(self) -> tuple[int]:
            return (1,)

        def fetchall(self) -> list[tuple[object, ...]]:
            return results.pop(0)

        def __enter__(self) -> "_Cursor":
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

    class _Conn:
        def cursor(self) -> _Cursor:
            return _Cursor()

        def __enter__(self) -> "_Conn":
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

    monkeypatch.setattr(
        substrate_conftest.psycopg, "connect", lambda *_a, **_k: _Conn()
    )
    with pytest.raises(RuntimeError, match=refusal):
        substrate_conftest._pause_timescale_jobs(
            "postgres://postgres:okg@127.0.0.1:1/okg_test_gw0"
        )
    assert results == []


@pytest.mark.postgres
def test_a_reset_without_a_template_is_refused(okg_migrated: str) -> None:
    """A database whose template was never built is an error, not a guess."""
    missing = substrate_conftest.dsn_with_dbname(
        okg_migrated, "okg_test_2271_no_template"
    )
    with pytest.raises(RuntimeError, match="does not exist"):
        recreate_okg_test_database(missing)


_TEMPLATE_ROWS = 20


def _bm25_match_count(dsn: str, term: str) -> int:
    """How many rows an index-only BM25 search for ``term`` returns."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SET enable_seqscan = off")
        cur.execute(
            """
            SELECT count(*) FROM (
              SELECT id FROM bm25_probe_2436
               WHERE deleted = FALSE AND attrs ? 'text'
               ORDER BY (attrs->>'text') <@> to_bm25query(%s, 'bm25_probe_2436_idx')
               LIMIT 1000
            ) hits
            """,
            (term,),
        )
        return cur.fetchone()[0]


@pytest.mark.postgres
def test_a_fresh_clone_does_not_read_the_dropped_clones_bm25_index(
    okg_migrated: str,
) -> None:
    """Each clone's lexical search sees only its own rows (issue #2436).

    pg_textsearch caches a BM25 index's in-memory part in server shared memory
    under the index OID. Every clone of one template has the same OID, and
    dropping a clone does not evict the entry, so without the rebuild in
    ``recreate_okg_test_database`` a later clone with fewer rows reads the
    earlier clone's entries: a corrupt-record error, a block-read error, or
    the wrong hit count.
    """
    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    dbname = f"okg_test_2436_{worker}"
    dsn = substrate_conftest.dsn_with_dbname(okg_migrated, dbname)
    template = okg_test_template_dbname(dsn)
    maintenance = substrate_conftest.dsn_with_dbname(okg_migrated, "postgres")

    def drop_both() -> None:
        with psycopg.connect(maintenance, autocommit=True) as conn:
            for name in (dbname, template):
                conn.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                        sql.Identifier(name)
                    )
                )

    drop_both()
    try:
        with psycopg.connect(maintenance, autocommit=True) as conn:
            conn.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(template))
            )
        template_dsn = substrate_conftest.dsn_with_dbname(okg_migrated, template)
        with psycopg.connect(template_dsn, autocommit=True) as conn:
            conn.execute("CREATE EXTENSION IF NOT EXISTS pg_textsearch")
            conn.execute(
                "CREATE TABLE bm25_probe_2436 ("
                "id text PRIMARY KEY, attrs jsonb, deleted bool DEFAULT false)"
            )
            conn.execute(
                "CREATE INDEX bm25_probe_2436_idx ON bm25_probe_2436 "
                "USING bm25 ((attrs->>'text') text_bm25_ops) "
                "WITH (text_config = 'english') "
                "WHERE deleted = FALSE AND attrs ? 'text'"
            )
            # Rows in the template are what put the index into the shared
            # cache: with an empty template index the bug does not show.
            conn.execute(
                "INSERT INTO bm25_probe_2436 (id, attrs) "
                "SELECT 'tpl' || g, "
                "jsonb_build_object('text', 'template row alpha beta ' || g) "
                "FROM generate_series(1, %s) g",
                (_TEMPLATE_ROWS,),
            )
        with psycopg.connect(maintenance, autocommit=True) as conn:
            conn.execute(
                sql.SQL("ALTER DATABASE {} WITH ALLOW_CONNECTIONS false").format(
                    sql.Identifier(template)
                )
            )

        # More rows first, then fewer: a later clone that reads an earlier
        # clone's cached entries finds rows that are not in its own table.
        for round_number, rows in enumerate((40, 30, 20, 10), start=1):
            recreate_okg_test_database(dsn, record_cost=False)
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute(
                    "INSERT INTO bm25_probe_2436 (id, attrs) "
                    "SELECT 'r' || %s || '_' || g, "
                    "jsonb_build_object('text', "
                    "'round gamma delta row ' || g || ' ' || repeat('word ', g)) "
                    "FROM generate_series(1, %s) g",
                    (round_number, rows),
                )
            assert _bm25_match_count(dsn, "gamma") == rows, (
                f"clone {round_number} holds {rows} rows matching 'gamma'"
            )
            assert _bm25_match_count(dsn, "alpha") == _TEMPLATE_ROWS, (
                f"clone {round_number} holds {_TEMPLATE_ROWS} template rows"
            )
    finally:
        drop_both()


def test_one_explicit_database_shared_by_xdist_workers_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = "postgres://postgres:okg@127.0.0.1:5433/okg_test_shared"
    monkeypatch.setenv("OKG_TEST_DSN", dsn)
    monkeypatch.setenv("PYTEST_XDIST_WORKER_COUNT", "2")

    with pytest.raises(pytest.exit.Exception) as excinfo:
        substrate_conftest._refuse_shared_database_across_xdist_workers(dsn)

    assert "OKG_TEST_DSN_TEMPLATE" in str(excinfo.value)


def test_one_explicit_database_without_xdist_is_allowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = "postgres://postgres:okg@127.0.0.1:5433/okg_test_shared"
    monkeypatch.setenv("OKG_TEST_DSN", dsn)
    monkeypatch.delenv("PYTEST_XDIST_WORKER_COUNT", raising=False)

    substrate_conftest._refuse_shared_database_across_xdist_workers(dsn)


def test_a_template_name_over_the_identifier_limit_is_refused() -> None:
    """PostgreSQL would silently truncate it onto the worker database's name."""
    long_name = "okg_test_" + "x" * 54
    assert len(long_name) == 63
    with pytest.raises(RuntimeError, match="too long for a template"):
        okg_test_template_dbname(
            f"postgres://postgres:okg@127.0.0.1:5433/{long_name}"
        )


def test_a_template_without_a_worker_placeholder_is_refused_under_xdist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every worker would resolve to the same database and drop it for the others."""
    dsn = "postgres://postgres:okg@127.0.0.1:5433/okg_test_shared"
    monkeypatch.delenv("OKG_TEST_DSN", raising=False)
    monkeypatch.setenv("OKG_TEST_DSN_TEMPLATE", dsn)
    monkeypatch.setenv("PYTEST_XDIST_WORKER_COUNT", "2")

    with pytest.raises(pytest.exit.Exception) as excinfo:
        substrate_conftest._refuse_shared_database_across_xdist_workers(dsn)

    assert "{workerid}" in str(excinfo.value)


def test_trust_env_does_not_let_the_clone_drop_a_non_test_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TRUST_AS_EPHEMERAL relaxed the old truncate; it must not unlock a DROP.

    CI sets the variable on every job. The name check fires before any
    connection, so a mistyped DSN cannot drop a real database.
    """
    monkeypatch.setenv("OKG_TEST_DSN_TRUST_AS_EPHEMERAL", "1")

    def fail_connect(*args, **kwargs):
        raise AssertionError("connected before the name check")

    monkeypatch.setattr(substrate_conftest.psycopg, "connect", fail_connect)

    for call in (
        substrate_conftest._drop_okg_test_database,
        substrate_conftest.build_okg_test_template,
        substrate_conftest.recreate_okg_test_database,
    ):
        with pytest.raises(pytest.exit.Exception) as excinfo:
            call("postgres://postgres:okg@127.0.0.1:5433/okg")
        assert "not a disposable test database" in str(excinfo.value)


def test_a_drop_the_server_refuses_fails_instead_of_skipping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A database the server will not drop is an error, not "unreachable".

    ``ObjectInUse`` subclasses ``OperationalError``; the session fixture used to
    turn it into a skip of every database test on the worker.
    """
    dsn = "postgres://postgres:okg@127.0.0.1:5433/okg_test_gw0"
    monkeypatch.delenv("PYTEST_XDIST_WORKER_COUNT", raising=False)

    def refuse(_dsn: str) -> None:
        raise psycopg.errors.ObjectInUse("database is being accessed")

    monkeypatch.setattr(substrate_conftest, "_drop_okg_test_database", refuse)

    with pytest.raises(psycopg.errors.ObjectInUse):
        substrate_conftest._build_fresh_okg_test_template(
            dsn, after_migrate=None, refuse_published_deployments=False
        )


def test_an_unreachable_server_still_skips(monkeypatch: pytest.MonkeyPatch) -> None:
    dsn = "postgres://postgres:okg@127.0.0.1:5433/okg_test_gw0"
    monkeypatch.delenv("PYTEST_XDIST_WORKER_COUNT", raising=False)

    def unreachable(_dsn: str) -> None:
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(substrate_conftest, "_drop_okg_test_database", unreachable)

    with pytest.raises(pytest.skip.Exception, match="not reachable"):
        substrate_conftest._build_fresh_okg_test_template(
            dsn, after_migrate=None, refuse_published_deployments=False
        )


def _copy_names(okg_migrated: str) -> tuple[str, str, str]:
    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    return (
        f"okg_test_copysrc_{worker}",
        f"okg_test_copydst_{worker}",
        substrate_conftest.dsn_with_dbname(okg_migrated, "postgres"),
    )


def _drop_all(maintenance: str, *names: str) -> None:
    with psycopg.connect(maintenance, autocommit=True) as conn:
        for name in names:
            conn.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(name)
                )
            )


def _allows_connections(cur, dbname: str) -> bool:
    cur.execute("SELECT datallowconn FROM pg_database WHERE datname = %s", (dbname,))
    return bool(cur.fetchone()[0])


@pytest.mark.postgres
def test_a_copy_refuses_and_names_a_session_left_open_on_the_source(
    okg_migrated: str,
) -> None:
    """An open client session on the copy source is reported, never ended."""
    source, copy, maintenance = _copy_names(okg_migrated)
    _drop_all(maintenance, copy, source)
    try:
        with psycopg.connect(maintenance, autocommit=True) as admin:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(source)))
        leak = psycopg.connect(
            substrate_conftest.dsn_with_dbname(okg_migrated, source),
            autocommit=True,
            application_name="okg-test-left-open",
        )
        try:
            with psycopg.connect(maintenance, autocommit=True) as admin, \
                    admin.cursor() as cur:
                with pytest.raises(RuntimeError) as excinfo:
                    substrate_conftest.create_database_from_template(
                        cur, copy, source
                    )
                assert _allows_connections(cur, source), (
                    "the source's ALLOW_CONNECTIONS is restored"
                )
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (copy,))
                assert cur.fetchone() is None, "no copy was made"
            message = str(excinfo.value)
            assert "left open" in message, message
            assert "application_name='okg-test-left-open'" in message, message
            assert "query_start=" in message, message
            # The leaked session was reported, not terminated.
            assert leak.execute("SELECT 1").fetchone() == (1,)
        finally:
            leak.close()
    finally:
        _drop_all(maintenance, copy, source)


# The launcher can notice a new database only on its next poll
# (timescaledb.bgw_launcher_poll_time), and a busy CI host starts the worker
# later still: 10 s failed on hosted runners with free worker slots. Wait one
# full poll period plus a margin, and never less than a minute.
_SCHEDULER_WAIT_FLOOR_SECONDS = 60.0
_SCHEDULER_WAIT_MARGIN_SECONDS = 15.0


def _scheduler_wait_seconds(cur) -> float:
    cur.execute(
        "SELECT setting::float, unit FROM pg_settings "
        "WHERE name = 'timescaledb.bgw_launcher_poll_time'"
    )
    row = cur.fetchone()
    assert row is not None, "the test server has TimescaleDB (ops/pg image)"
    setting, unit = row
    # TimescaleDB declares this setting in milliseconds but without a GUC unit,
    # so pg_settings reports an empty unit (60000 by default).
    assert unit in ("", "ms", None), f"bgw_launcher_poll_time is in {unit!r}, expected ms"
    return max(_SCHEDULER_WAIT_FLOOR_SECONDS, setting / 1000 + _SCHEDULER_WAIT_MARGIN_SECONDS)


def _wait_for_scheduler(cur, dbname: str) -> None:
    """Wait until TimescaleDB runs a scheduler in ``dbname``.

    Skips when every TimescaleDB worker slot is taken, since the launcher
    then starts no scheduler for a new database; otherwise fails loudly with
    the scheduler state, so a scheduler that never starts is not hidden.
    """
    backend = substrate_conftest.TIMESCALEDB_SCHEDULER_BACKEND
    wait_seconds = _scheduler_wait_seconds(cur)
    started = time.monotonic()
    deadline = started + wait_seconds
    while time.monotonic() < deadline:
        cur.execute(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE datname = %s AND backend_type = %s",
            (dbname, backend),
        )
        if cur.fetchone()[0]:
            return
        time.sleep(0.2)
    cur.execute(
        "SELECT current_setting('timescaledb.max_background_workers')::int, "
        "(SELECT count(*) FROM pg_stat_activity WHERE backend_type = %s)",
        (backend,),
    )
    slots, running = cur.fetchone()
    # The launcher takes one slot too (the same count as
    # test_timescaledb_workers_pg.py uses).
    in_use = running + 1
    if in_use >= slots:
        pytest.skip(
            f"no free TimescaleDB worker slot: the launcher and {running} "
            f"schedulers use {in_use} of timescaledb.max_background_workers="
            f"{slots}, so no scheduler starts in {dbname!r} and the copy "
            "cannot show it is ended"
        )
    cur.execute(
        "SELECT coalesce(string_agg(datname, ', ' ORDER BY datname), '') "
        "FROM pg_stat_activity WHERE backend_type = %s",
        (backend,),
    )
    scheduled = cur.fetchone()[0]
    pytest.fail(
        f"no TimescaleDB scheduler appeared in {dbname!r} within "
        f"{time.monotonic() - started:.0f}s (limit {wait_seconds:.0f}s) although "
        f"only {in_use} of {slots} worker slots (launcher included) are in use; "
        f"schedulers run in: [{scheduled}]"
    )


@pytest.mark.postgres
def test_a_copy_of_a_database_that_allows_connections_ends_its_scheduler(
    okg_migrated: str,
) -> None:
    """The source of the pre-lock clone allows connections, so TimescaleDB
    runs a scheduler in it; the copy ends only that worker and succeeds."""
    source, copy, maintenance = _copy_names(okg_migrated)
    _drop_all(maintenance, copy, source)
    try:
        with psycopg.connect(maintenance, autocommit=True) as admin, \
                admin.cursor() as cur:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(source)))
            with psycopg.connect(
                substrate_conftest.dsn_with_dbname(okg_migrated, source)
            ) as conn:
                assert conn.execute(
                    "SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb'"
                ).fetchone() == (1,), "the source has TimescaleDB (ops/pg image)"
            _wait_for_scheduler(cur, source)
            substrate_conftest.create_database_from_template(cur, copy, source)
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (copy,))
            assert cur.fetchone() == (1,)
            assert _allows_connections(cur, source)
            leftover = substrate_conftest.template_source_sessions(cur, source)
            assert leftover == [], leftover
    finally:
        _drop_all(maintenance, copy, source)
