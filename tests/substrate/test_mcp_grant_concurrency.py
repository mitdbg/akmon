"""Concurrent MCP/remediator grants must neither fail a source nor be lost.

The ingest layer runs several sources in one layer (compute_env_local,
git_files, git_history, openspec, pact), as separate PROCESSES. Each runs the
graph-store grant catch-up, whose DO block GRANTs on ``okg.graph_nodes`` /
``okg.graph_edges``. Two sessions updating that same pg_class ACL tuple make
PostgreSQL raise ``tuple concurrently updated`` (XX000) in the loser.

That error used to escape the first, unguarded GRANT in the block and fail the
whole source. Observed in CI as ``pact`` FAIL -> ingest FAIL -> ``okg init``
FAIL -> documented-golden-path FAIL (run 29635924151, job 88058470387). The
in-process ``_MCP_GRANTED_AT_EPOCH`` memo does not prevent it: it is
per-process, and the competing sources are separate processes.

Two properties are covered, because the obvious fix only gets the first:

* not raising -- ``test_concurrent_grant_survives_identical_writer``;
* not silently losing the grant -- ``test_grant_survives_non_identical_writer``.
  Merely swallowing XX000 passes the first and FAILS the second: the waiter
  blocks on the competing TRANSACTION, not on a matching statement, so the
  competitor need not be the same GRANT. A concurrent ``ALTER TABLE ... ADD
  COLUMN`` from ``ensure_schema`` is enough, and then swallowing drops the
  privilege silently. Hence the retry in the handler.

Marked ``serial_db``: the deterministic tests hold an uncommitted ACL-tuple
update on the shared graph tables, which would block sibling workers sharing
one test database.
"""
from __future__ import annotations

import inspect
import re
import threading
from collections.abc import Iterator

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.graph_store_internal.schema import (
    ChronosGraphSchemaMixin,
    _clear_mcp_grant_guard,
)

pytestmark = [pytest.mark.postgres, pytest.mark.serial_db]

REMEDIATOR = "projection_remediator"
GRAPH_TABLES = ("okg.graph_nodes", "okg.graph_edges")
TARGET_PRIVS = ("SELECT", "INSERT", "UPDATE", "DELETE")

IDENTICAL_GRANT = (
    "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE "
    "okg.graph_nodes, okg.graph_edges TO " + REMEDIATOR
)
# A DIFFERENT ACL change to the SAME pg_class tuples: contends identically, but
# confers none of the privileges the catch-up is responsible for -- so if the
# handler swallows instead of retrying, the loss is observable.
NON_IDENTICAL_GRANT = (
    "GRANT SELECT ON TABLE okg.graph_nodes, okg.graph_edges TO app_ro"
)

# Generous: these bound a hang, they do not pace the race, which resolves as
# soon as the blocker commits.
JOIN_TIMEOUT_S = 60.0
BLOCK_POLL_TIMEOUT_S = 30.0


def _assert_race_is_reachable(conn: psycopg.Connection) -> None:
    """Fail loudly if the grant would be skipped -- a skipped GRANT races nothing.

    The DO block guards its grants with ``IF EXISTS (SELECT 1 FROM pg_roles
    ...)``. Without the roles, or without the tables, every assertion in this
    module would pass while proving nothing.
    """
    for role in (REMEDIATOR, "app_ro"):
        exists = conn.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s)", (role,)
        ).fetchone()[0]
        assert exists, (
            f"role {role} absent -- the DO block would skip the GRANT and these "
            "tests would pass vacuously"
        )
    for table in GRAPH_TABLES:
        present = conn.execute(
            "SELECT to_regclass(%s) IS NOT NULL", (table,)
        ).fetchone()[0]
        assert present, f"{table} absent -- the GRANT would not reach the ACL tuple"


def _bootstrap(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=True)
        _assert_race_is_reachable(conn)


def _execute_contended(conn: psycopg.Connection, sql: str, attempts: int = 5) -> None:
    """Run the harness's own contended catalog DDL, retrying the same race.

    The scaffolding issues GRANT/REVOKE against the very ACL tuples these tests
    contend for, so it can lose the race exactly like production did -- a
    blocker's own GRANT raising XX000 is a flake in the test, not a finding
    about the code. Retrying here keeps the harness honest while leaving every
    assertion about the code under test strict.
    """
    for attempt in range(attempts):
        try:
            conn.execute(sql)
            return
        except psycopg.Error as exc:
            if getattr(exc, "sqlstate", None) != "XX000" or attempt == attempts - 1:
                raise
            # The failed statement aborted this transaction; clear it and retry.
            conn.rollback()


def _direct_privileges(
    conn: psycopg.Connection, table: str, role: str = REMEDIATOR
) -> set[str]:
    """Privileges granted DIRECTLY to ``role`` on ``table``.

    Deliberately not ``has_table_privilege``: migrate makes
    ``projection_remediator`` a member of ``app_rw``, and grants ``app_rw`` the
    same four privileges on these tables. The effective check is therefore true
    by inheritance no matter what this code path does -- it cannot tell a landed
    grant from a swallowed one. The direct ACL entry is what the catch-up
    creates, and what a swallowed grant loses.
    """
    rows = conn.execute(
        """
        SELECT a.privilege_type
          FROM pg_class c
          CROSS JOIN LATERAL aclexplode(c.relacl) AS a
          JOIN pg_roles g ON g.oid = a.grantee
         WHERE c.oid = %s::regclass
           AND g.rolname = %s
        """,
        (table, role),
    ).fetchall()
    return {row[0] for row in rows}


def _revoke_target_privileges(dsn: str) -> None:
    """Clear the privileges under test so 'present afterwards' means something."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        _execute_contended(
            conn, f"REVOKE ALL ON {', '.join(GRAPH_TABLES)} FROM {REMEDIATOR}"
        )
        for table in GRAPH_TABLES:
            remaining = _direct_privileges(conn, table) & set(TARGET_PRIVS)
            assert not remaining, (
                f"{REMEDIATOR} still directly holds {sorted(remaining)} on "
                f"{table} after REVOKE -- the post-race assertion would be "
                "tautological"
            )


def _assert_target_privileges_present(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        for table in GRAPH_TABLES:
            missing = set(TARGET_PRIVS) - _direct_privileges(conn, table)
            assert not missing, (
                f"{REMEDIATOR} is missing {sorted(missing)} on {table}: the "
                "concurrent-update handler swallowed the grant instead of "
                "retrying it"
            )


@pytest.fixture
def graph_grants_isolated(clean_runtime: str) -> Iterator[str]:
    """Provision the graph, clear the privileges under test, always restore them.

    Clearing them is what makes "present afterwards" evidence of anything. The
    restore is unconditional: a test that fails before the catch-up re-grants
    would otherwise leave this database's ``projection_remediator`` stripped,
    breaking sibling tests that share it (``clean_runtime`` truncates rows, it
    does not restore grants).

    The REVOKE happens INSIDE the try. It commits before
    ``_revoke_target_privileges`` runs its own assertion, so raising there --
    which is exactly what that assertion is for -- would otherwise strip the
    database with no restore.
    """
    _bootstrap(clean_runtime)
    # Recorded before anything is revoked: the non-identical blocker grants
    # SELECT to app_ro, and teardown must leave this database's ACLs as found.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        app_ro_had_select = {
            table: "SELECT" in _direct_privileges(conn, table, "app_ro")
            for table in GRAPH_TABLES
        }
    try:
        _revoke_target_privileges(clean_runtime)
        yield clean_runtime
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            _execute_contended(conn, IDENTICAL_GRANT)
            for table, had_select in app_ro_had_select.items():
                if not had_select:
                    _execute_contended(
                        conn, f"REVOKE SELECT ON {table} FROM app_ro"
                    )


def _backend_pid(conn: psycopg.Connection) -> int:
    return int(conn.execute("SELECT pg_backend_pid()").fetchone()[0])


def _wait_until_blocked(dsn: str, victim_pid: int, blocker_pid: int) -> bool:
    """True once ``victim_pid`` is waiting specifically on ``blocker_pid``.

    Checking *who* blocks matters: on a shared test database an unrelated
    backend (a sibling worker's TRUNCATE) can block the victim, and treating
    that as "the race is set up" would release the blocker early and let the
    test pass without ever exercising the race.
    """
    deadline = threading.Event()
    timer = threading.Timer(BLOCK_POLL_TIMEOUT_S, deadline.set)
    timer.daemon = True
    timer.start()
    try:
        with psycopg.connect(dsn, autocommit=True) as probe:
            while not deadline.is_set():
                blocking = probe.execute(
                    "SELECT pg_blocking_pids(%s)", (victim_pid,)
                ).fetchone()[0]
                if blocker_pid in (blocking or []):
                    return True
                deadline.wait(0.05)
        return False
    finally:
        timer.cancel()


def _run_grant_catchup(store: ChronosGraphStore) -> None:
    # The epoch memo would otherwise skip the DO block outright, so a warm
    # process would exercise nothing.
    _clear_mcp_grant_guard()
    store._grant_chronos_public_tables_to_mcp_if_present()


def _race_against(dsn: str, blocker_sql: str) -> list[Exception]:
    """Force the interleaving, and return whatever the catch-up raised.

    A blocker session holds ``blocker_sql`` uncommitted against the graph
    tables' ACL tuples; the victim runs the real catch-up and blocks on them;
    the blocker then commits, which is what turns the victim's wait into XX000.
    """
    blocker = psycopg.connect(dsn, autocommit=False)
    victim = psycopg.connect(dsn, autocommit=True)
    raised: list[Exception] = []
    try:
        store = ChronosGraphStore.from_connection(victim, ensure_schema=False)
        victim_pid = _backend_pid(victim)
        blocker_pid = _backend_pid(blocker)

        # Uncommitted: holds the ACL tuples. Retried because the blocker is
        # itself a contender and can lose the race to lingering catalog writes.
        _execute_contended(blocker, blocker_sql)

        def _victim() -> None:
            try:
                _run_grant_catchup(store)
            except Exception as exc:
                raised.append(exc)

        thread = threading.Thread(target=_victim, name="grant-victim", daemon=True)
        thread.start()
        try:
            blocked = _wait_until_blocked(dsn, victim_pid, blocker_pid)
        finally:
            # Release the victim no matter how the wait went, so a failed setup
            # reports itself instead of stalling for the full join timeout.
            blocker.commit()
            thread.join(timeout=JOIN_TIMEOUT_S)

        assert not thread.is_alive(), "grant catch-up hung after the blocker committed"
        assert blocked, (
            "victim never blocked on the blocker's ACL tuple -- the race was not "
            "exercised, so a pass here would be meaningless"
        )
        return raised
    finally:
        blocker.close()
        victim.close()


def _assert_did_not_raise(raised: list[Exception]) -> None:
    assert not raised, (
        "concurrent grant escaped the DO block's handler: "
        f"{type(raised[0]).__name__} "
        f"[{getattr(raised[0], 'sqlstate', None)}]: {raised[0]}"
    )


def test_concurrent_grant_survives_identical_writer(
    graph_grants_isolated: str,
) -> None:
    """The reported CI failure: a sibling source running the same catch-up.

    Against the pre-fix schema.py this fails with
    ``InternalError_: tuple concurrently updated``.
    """
    dsn = graph_grants_isolated
    _assert_did_not_raise(_race_against(dsn, IDENTICAL_GRANT))
    _assert_target_privileges_present(dsn)


def test_grant_survives_non_identical_writer(graph_grants_isolated: str) -> None:
    """The competing writer is NOT the same GRANT, so the privilege can be lost.

    This is the case a bare ``EXCEPTION ... THEN NULL`` handler gets wrong: it
    does not raise, so it looks fixed, while the catch-up's privilege is
    silently dropped. Only the retry makes the grant actually land.
    """
    dsn = graph_grants_isolated
    _assert_did_not_raise(_race_against(dsn, NON_IDENTICAL_GRANT))
    _assert_target_privileges_present(dsn)


def test_corruption_errors_are_not_swallowed(okg_migrated: str) -> None:
    """The handler must not mask XX001/XX002 while tolerating XX000.

    ``internal_error`` is the XX000 *category* code: PL/pgSQL matches a
    condition name ending in ``000`` against the whole class, so the obvious
    ``WHEN ... OR internal_error THEN NULL`` also swallows ``data_corrupted``
    and ``index_corrupted``. That Postgres rule is the non-obvious thing this
    test pins, since it is the entire reason the handler needs a re-raise.
    """
    handler = """
    DO $probe$ BEGIN
      BEGIN
        RAISE EXCEPTION 'synthetic' USING ERRCODE = '{code}';
      EXCEPTION
        WHEN insufficient_privilege OR undefined_table THEN
          NULL;
        WHEN internal_error THEN
          IF SQLSTATE <> 'XX000' THEN RAISE; END IF;
      END;
    END $probe$
    """
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(handler.format(code="XX000"))  # tolerated

        for code in ("XX001", "XX002"):
            with pytest.raises(psycopg.Error) as caught:
                conn.execute(handler.format(code=code))
            assert caught.value.sqlstate == code, (
                f"expected {code} to escape the handler, got "
                f"{caught.value.sqlstate}"
            )

    # The check above pins the PL/pgSQL rule, not this repo's code. Tie it to the
    # real handler so widening it back to a blanket swallow fails here.
    #
    # Counted, not just "is the narrowing present somewhere": an earlier revision
    # of this fix had a correctly-narrowed outer handler AND a second, wide one
    # on the retry path. A substring check passed while the retry silently
    # swallowed corruption. Every `internal_error` must carry its own guard.
    source = inspect.getsource(
        ChronosGraphSchemaMixin._grant_chronos_public_tables_to_mcp_if_present
    )
    # Strip `#` and `--` comments: both discuss `internal_error` by name, and
    # would otherwise be counted as handlers. Counting tokens rather than
    # matching a shape keeps this robust to reformatting the WHEN clause across
    # lines (though not to reformatting the guard literal itself).
    sql = re.sub(r"(--|#)[^\n]*", "", source)
    catches = sql.count("internal_error")
    guards = sql.count("IF SQLSTATE <> 'XX000' THEN")
    assert catches >= 1, "the grant handler no longer catches `internal_error`"
    assert guards == catches, (
        f"{catches} `internal_error` handler(s) but {guards} XX000 narrowing "
        "guard(s): an unguarded one swallows XX001 data_corrupted / XX002 "
        "index_corrupted"
    )


def test_catchup_grants_tables_and_sequences_but_not_decoys(
    graph_grants_isolated: str,
) -> None:
    """The catch-up's full grant set, asserted directly.

    Nothing else in the repo covers the ``_chronos*`` SEQUENCE loop -- a grep for
    ``ON SEQUENCE`` / ``has_sequence_privilege`` across tests/ finds nothing --
    so an edit dropping it would leave every other test green while
    projection_remediator silently lost USAGE/SELECT/UPDATE on chronos
    sequences. This states the contract rather than diffing against the previous
    implementation, so it keeps its meaning once that implementation is history.
    """
    dsn = graph_grants_isolated
    tables = [f"_chronos_grantprobe_t{i}" for i in range(3)]
    sequences = [f"_chronos_grantprobe_s{i}" for i in range(3)]
    decoys_t, decoys_s = "not_chronos_grantprobe_t", "not_chronos_grantprobe_s"

    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            for name in tables:
                conn.execute(f"CREATE TABLE public.{name} (id int)")
            for name in sequences:
                conn.execute(f"CREATE SEQUENCE public.{name}")
            conn.execute(f"CREATE TABLE public.{decoys_t} (id int)")
            conn.execute(f"CREATE SEQUENCE public.{decoys_s}")

            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            _run_grant_catchup(store)

            mcp_exists = conn.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp')"
            ).fetchone()[0]

            for name in tables:
                granted = _direct_privileges(conn, f"public.{name}")
                assert set(TARGET_PRIVS) <= granted, (
                    f"{REMEDIATOR} missing {sorted(set(TARGET_PRIVS) - granted)} "
                    f"on chronos table {name}"
                )
                if mcp_exists:
                    mcp = _direct_privileges(conn, f"public.{name}", "okg_mcp")
                    assert "SELECT" in mcp, f"okg_mcp cannot read chronos table {name}"

            for name in sequences:
                held = {
                    priv
                    for priv in ("USAGE", "SELECT", "UPDATE")
                    if conn.execute(
                        "SELECT has_sequence_privilege(%s, %s, %s)",
                        (REMEDIATOR, f"public.{name}", priv),
                    ).fetchone()[0]
                }
                assert held == {"USAGE", "SELECT", "UPDATE"}, (
                    f"{REMEDIATOR} has {sorted(held)} on chronos sequence {name}, "
                    "expected USAGE/SELECT/UPDATE -- the sequence grant loop "
                    "stopped covering it"
                )

            # The scan is anchored on the `_chronos` prefix; nothing else is
            # swept in. Checked on the direct ACL entry, since app_rw membership
            # makes the effective check true for unrelated reasons.
            assert not _direct_privileges(conn, f"public.{decoys_t}"), (
                f"{REMEDIATOR} was granted on non-chronos table {decoys_t}"
            )
        finally:
            for name in tables + [decoys_t]:
                conn.execute(f"DROP TABLE IF EXISTS public.{name}")
            for name in sequences + [decoys_s]:
                conn.execute(f"DROP SEQUENCE IF EXISTS public.{name}")


def test_tolerated_error_does_not_abort_remaining_grants(okg_migrated: str) -> None:
    """A tolerated failure must skip ONE statement, not the rest of the batch.

    The apply loop is ``FOREACH stmt ... FOR attempt IN 1..3 ... EXIT``. If that
    ``EXIT`` left the FOREACH rather than the retry loop, a single missing
    relation would silently skip every remaining grant -- and nothing else in
    this suite would notice, because in a healthy database no statement fails.
    Pins the PL/pgSQL scoping rule the loop shape depends on.

    The batch covers both tolerated conditions: a missing relation
    (``undefined_table``) and a missing role (``undefined_object``, the one added
    because collect-then-apply widened the window for a role to disappear
    between its ``IF EXISTS`` check and its GRANT).
    """
    role = "probe_exit_scope_role"
    block = f"""
    DO $probe$
    DECLARE
      stmt  text;
      stmts text[] := ARRAY[
        'GRANT SELECT ON TABLE okg.probe_missing TO {role}',
        'GRANT SELECT ON TABLE okg.probe_a TO probe_role_that_does_not_exist',
        'GRANT SELECT ON TABLE okg.probe_a TO {role}',
        'GRANT SELECT ON TABLE okg.probe_b TO {role}'
      ];
    BEGIN
      FOREACH stmt IN ARRAY stmts LOOP
        FOR attempt IN 1..3 LOOP
          BEGIN
            EXECUTE stmt;
            EXIT;
          EXCEPTION
            WHEN insufficient_privilege OR undefined_table
                 OR undefined_object THEN
              EXIT;
            WHEN internal_error THEN
              IF SQLSTATE <> 'XX000' THEN RAISE; END IF;
          END;
        END LOOP;
      END LOOP;
    END $probe$
    """
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        conn.execute(
            f"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE "
            f"rolname='{role}') THEN CREATE ROLE {role} NOLOGIN; END IF; END $$"
        )
        conn.execute("CREATE TABLE IF NOT EXISTS okg.probe_a (id int)")
        conn.execute("CREATE TABLE IF NOT EXISTS okg.probe_b (id int)")
        conn.execute("DROP TABLE IF EXISTS okg.probe_missing")
        try:
            conn.execute(block)  # first statement hits undefined_table
            for table in ("okg.probe_a", "okg.probe_b"):
                granted = conn.execute(
                    "SELECT has_table_privilege(%s, %s, 'SELECT')", (role, table)
                ).fetchone()[0]
                assert granted, (
                    f"{table} was not granted after an earlier statement hit a "
                    "tolerated error -- EXIT is leaving the statement loop, so "
                    "one missing relation silently drops every later grant"
                )
        finally:
            conn.execute("DROP TABLE IF EXISTS okg.probe_a")
            conn.execute("DROP TABLE IF EXISTS okg.probe_b")
            conn.execute(f"DROP ROLE IF EXISTS {role}")


def test_parallel_grant_catchups_do_not_raise(graph_grants_isolated: str) -> None:
    """Production-shaped check: sibling sources run the catch-up simultaneously.

    Supplements -- does not replace -- the deterministic tests above: whether
    any given run actually collides is up to the scheduler, so this would not by
    itself be a reliable witness.
    """
    workers = 8
    dsn = graph_grants_isolated

    conns = [psycopg.connect(dsn, autocommit=True) for _ in range(workers)]
    raised: list[Exception] = []
    lock = threading.Lock()
    start = threading.Barrier(workers)

    def _worker(conn: psycopg.Connection) -> None:
        try:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            start.wait(timeout=JOIN_TIMEOUT_S)  # fire together to maximize overlap
            _run_grant_catchup(store)
        except Exception as exc:
            # Break the barrier so siblings fail fast on the real cause instead
            # of all reporting a timeout.
            start.abort()
            with lock:
                raised.append(exc)

    threads = [
        threading.Thread(target=_worker, args=(conn,), name=f"grant-{i}", daemon=True)
        for i, conn in enumerate(conns)
    ]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=JOIN_TIMEOUT_S)
        assert all(not t.is_alive() for t in threads), "a grant catch-up hung"
        assert not raised, (
            "concurrent grant catch-ups raised: "
            + "; ".join(
                f"{type(e).__name__} [{getattr(e, 'sqlstate', None)}]: {e}"
                for e in raised
            )
        )
    finally:
        for conn in conns:
            conn.close()

    _assert_target_privileges_present(dsn)
