"""Issue #960: shared runtime pools must reject severed idle connections."""
from __future__ import annotations

import contextlib
import time
import uuid
from typing import Iterator

import psycopg
import pytest
from psycopg_pool import ConnectionPool

from okg.substrate.db import pool as pool_module
from okg.substrate.db.pool import close_all, get_pool


def test_get_pool_configures_checkout_health_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakePool:
        check_connection = ConnectionPool.check_connection

        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)
            self.closed = False

        def wait(self, *, timeout: float) -> None:
            del timeout

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(pool_module, "ConnectionPool", FakePool)
    monkeypatch.setattr(pool_module, "_POOLS", {})

    result = pool_module.get_pool(
        "host=localhost dbname=issue960",
        max_size=2,
        application_name="issue-960-test",
    )

    assert result is pool_module._POOLS["host=localhost dbname=issue960"]
    assert captured["check"] is ConnectionPool.check_connection
    assert captured["min_size"] == 2
    assert captured["max_size"] == 2


def _terminate_backend_and_wait(dsn: str, pid: int) -> None:
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute("SELECT pg_terminate_backend(%s)", (pid,))
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            row = admin.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE pid = %s",
                (pid,),
            ).fetchone()
            if row is not None and row[0] == 0:
                return
            time.sleep(0.05)
    raise AssertionError(f"backend {pid} still alive after terminate")


@pytest.mark.postgres
def test_get_pool_survives_backend_termination(okg_migrated: str) -> None:
    pool = get_pool(
        okg_migrated,
        max_size=2,
        application_name="okg-issue-960-health-test",
    )
    try:
        # get_pool pre-opens two connections. Borrow both before terminating
        # them so a no-check pool cannot satisfy the next checkout with a
        # different healthy idle connection and make the recovery assertion
        # vacuous.
        with pool.connection() as first:
            with pool.connection() as second:
                old_pids = {first.info.backend_pid, second.info.backend_pid}
        for old_pid in old_pids:
            _terminate_backend_and_wait(okg_migrated, old_pid)

        with pool.connection() as conn:
            row = conn.execute("SELECT 1").fetchone()
            new_pid = conn.info.backend_pid

        assert row == (1,)
        assert new_pid not in old_pids
    finally:
        close_all()


# --- Issue #1997 -------------------------------------------------------------
# Chronos's lazy metadata bootstrap takes a TRANSACTION-scoped advisory lock
# and used to leave the transaction open behind it. An `idle in transaction`
# backend that has written anything holds an in-progress transaction id, and
# that pins the CLUSTER-wide vacuum horizon: VACUUM in every database on the
# instance stops marking pages all-visible and stops removing newer dead rows
# for as long as the session sits there. It also parks the metadata lock, so
# no other session can bootstrap.
#
# Both tests drive the real production helper. They run it against a throwaway
# schema rather than `public`, because the registry table already exists in
# `public` of the shared migrated test database and the bootstrap branch is
# only reached when it does not.

_METADATA_LOCK_KEYS = (1720812901, 19840717)


@contextlib.contextmanager
def _chronos_bootstrap_session(dsn: str) -> "Iterator[tuple[psycopg.Connection, object]]":
    """A non-autocommit chronos adapter whose registry table does not exist yet."""
    from chronos_core.branching import _common
    from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter

    schema = f"issue1997_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    # Cleared before AND after: the registry cache is keyed on database, host
    # and port, so a warm entry would skip the bootstrap this test is about,
    # and the entry this test warms describes a schema that is about to be
    # dropped.
    _common.clear_all_registry_caches()
    conn = psycopg.connect(dsn)  # autocommit=False, as okg's pools hand out
    try:
        conn.execute(f'SET search_path = "{schema}"')
        conn.commit()
        yield conn, PostgresDatabaseAdapter(conn)
    finally:
        conn.close()
        _common.clear_all_registry_caches()
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.mark.postgres
def test_chronos_metadata_bootstrap_leaves_no_idle_in_transaction_lock_holder(
    okg_migrated: str,
) -> None:
    """The bootstrap must not return with its own transaction still open.

    Before the fix this failed on every assertion below: the session came back
    `idle in transaction`, still holding advisory lock (1720812901, 19840717)
    and an in-progress `backend_xid` that held the cluster vacuum horizon at
    its own value.
    """
    from chronos_core.branching import _common

    with _chronos_bootstrap_session(okg_migrated) as (conn, adapter):
        pid = conn.execute("SELECT pg_backend_pid()").fetchone()[0]
        conn.commit()

        _common._parse_table_registry(adapter, "interval")

        with psycopg.connect(okg_migrated, autocommit=True) as observer:
            row = observer.execute(
                "SELECT state, backend_xid, backend_xmin "
                "  FROM pg_stat_activity WHERE pid = %s",
                (pid,),
            ).fetchone()
            assert row is not None, "the bootstrap session disappeared"
            state, backend_xid, backend_xmin = row
            advisory_locks = observer.execute(
                "SELECT count(*) FROM pg_locks "
                " WHERE locktype = 'advisory' AND pid = %s",
                (pid,),
            ).fetchone()[0]
            horizon_pinned = observer.execute(
                "SELECT pg_snapshot_xmin(pg_current_snapshot()) <= %s::text::xid8",
                (str(backend_xid),),
            ).fetchone()[0] if backend_xid is not None else False

        assert advisory_locks == 0, (
            "the bootstrap returned still holding "
            f"{advisory_locks} advisory lock(s); no other session can "
            "bootstrap chronos metadata until this one commits"
        )
        assert state != "idle in transaction", (
            f"the bootstrap left its session {state!r}"
        )
        assert backend_xid is None, (
            f"the bootstrap left in-progress transaction id {backend_xid} "
            "open, which holds back VACUUM in every database on this "
            f"instance (horizon pinned: {horizon_pinned})"
        )
        assert backend_xmin is None, (
            f"the bootstrap left snapshot xmin {backend_xmin} held"
        )

        # The guarded DDL is durable, not rolled back: a separate session
        # sees the table the bootstrap created.
        with psycopg.connect(okg_migrated, autocommit=True) as observer:
            search_path = conn.execute("SHOW search_path").fetchone()[0]
            observer.execute(f"SET search_path = {search_path}")
            assert observer.execute(
                "SELECT to_regclass('_chronos_branch_tables')"
            ).fetchone()[0] is not None, (
                "the bootstrap rolled back the registry table it created"
            )


@pytest.mark.postgres
def test_chronos_metadata_bootstrap_still_excludes_a_concurrent_session(
    okg_migrated: str,
) -> None:
    """Releasing the transaction must not weaken the mutual exclusion.

    The lock stays transaction-scoped and stays held across the whole guarded
    region, so while the bootstrap is inside it no other session can take the
    same key; once it returns, another session can take it at once.
    """
    from chronos_core.branching import _common

    probes: list[bool] = []
    calls = {"n": 0}
    real_relation_exists = _common._relation_exists

    def probing_relation_exists(db: object, name: str) -> bool:
        result = real_relation_exists(db, name)
        if name == "_chronos_branch_tables":
            calls["n"] += 1
            # Call 1 runs before the lock; call 2 is the re-check INSIDE it.
            if calls["n"] == 2:
                with psycopg.connect(okg_migrated) as other:
                    probes.append(
                        other.execute(
                            "SELECT pg_try_advisory_xact_lock(%s, %s)",
                            _METADATA_LOCK_KEYS,
                        ).fetchone()[0]
                    )
        return result

    with _chronos_bootstrap_session(okg_migrated) as (conn, adapter):
        _common._relation_exists = probing_relation_exists  # type: ignore[assignment]
        try:
            _common._parse_table_registry(adapter, "interval")
        finally:
            _common._relation_exists = real_relation_exists  # type: ignore[assignment]

        assert calls["n"] == 2, (
            "the bootstrap branch did not run, so this test proved nothing "
            f"(saw {calls['n']} existence check(s), expected 2)"
        )
        assert probes == [False], (
            "another session took the chronos metadata lock while the "
            "bootstrap was inside the guarded region, so mutual exclusion "
            f"is gone (probe results: {probes})"
        )

        # Released on the way out, so the next bootstrapper is not blocked.
        with psycopg.connect(okg_migrated) as other:
            assert other.execute(
                "SELECT pg_try_advisory_xact_lock(%s, %s)", _METADATA_LOCK_KEYS
            ).fetchone()[0] is True, (
                "the metadata lock was still held after the bootstrap returned"
            )


@pytest.mark.postgres
def test_chronos_owned_sessions_carry_the_idle_in_transaction_bound(
    okg_migrated: str,
) -> None:
    """Issue #1997, ask 2: Chronos's own factory must bound idle transactions.

    okg reaches `PostgresDatabaseAdapter.connect` from ~18 production call
    sites through `ChronosGraphStore.connect`, and those sessions carried no
    `idle_in_transaction_session_timeout` at all while okg's own pools have
    had one since #1310. Both factories now read the same constant, so a
    session that does leak a transaction is terminated with SQLSTATE 25P03
    instead of pinning the cluster vacuum horizon indefinitely.
    """
    from chronos_core.pg_session import DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS
    from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter
    from okg.substrate.db.connection import (
        DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS as OKG_DEFAULT,
    )

    # One literal, not two that can drift apart.
    assert OKG_DEFAULT == DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT_MS == 300_000

    adapter = PostgresDatabaseAdapter.connect(okg_migrated)
    try:
        applied = adapter.execute(
            "SHOW idle_in_transaction_session_timeout"
        ).fetchone()
        value = applied["idle_in_transaction_session_timeout"]
        assert value == "5min", (
            "a Chronos-owned session opened no idle-in-transaction bound "
            f"(SHOW returned {value!r})"
        )
        # The search_path pin from #1968 is still first and still in force.
        assert adapter.execute("SHOW search_path").fetchone()[
            "search_path"
        ] == "public"
    finally:
        adapter.close()

    # An explicit caller override still wins: libpq takes the LAST `-c` for a
    # setting, and this is the documented way to ask for a deliberately long
    # external transaction (the enricher backfill uses exactly this form).
    override = PostgresDatabaseAdapter.connect(
        okg_migrated, options="-c idle_in_transaction_session_timeout=0",
    )
    try:
        assert override.execute(
            "SHOW idle_in_transaction_session_timeout"
        ).fetchone()["idle_in_transaction_session_timeout"] == "0", (
            "an explicit caller override did not win over the factory default"
        )
    finally:
        override.close()


def _backend_application_name(adapter: object) -> str:
    """The name the SERVER recorded for this session in pg_stat_activity."""
    row = adapter.execute(  # type: ignore[attr-defined]
        "SELECT application_name FROM pg_stat_activity "
        "WHERE pid = pg_backend_pid()"
    ).fetchone()
    return str(row["application_name"])


@pytest.mark.postgres
def test_chronos_store_sessions_report_the_okg_store_name(
    okg_migrated: str,
) -> None:
    """Issue #2027: a store session is attributed to okg, not to Chronos.

    `stamp_store_application_name` adds `application_name=okg-chronos-store`
    to every `ChronosGraphStore.connect` DSN, and Chronos's adapter used to
    pass `application_name="chronos-branching"` as a keyword, which overrides
    the DSN's value. Every store session then showed `chronos-branching`.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    assert "application_name=" not in okg_migrated, (
        "precondition: the test DSN must carry no name of its own"
    )
    store = ChronosGraphStore.connect(okg_migrated, ensure_schema=False)
    try:
        assert _backend_application_name(store.context.db) == (
            "okg-chronos-store"
        )
    finally:
        store.close()


@pytest.mark.postgres
def test_chronos_adapter_names_a_session_whose_dsn_names_none(
    okg_migrated: str,
) -> None:
    """Issue #2027: the adapter's own name is the fallback, not the override."""
    from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter

    unnamed = PostgresDatabaseAdapter.connect(okg_migrated)
    try:
        assert _backend_application_name(unnamed) == "chronos-branching"
    finally:
        unnamed.close()

    separator = "&" if "?" in okg_migrated else "?"
    named = PostgresDatabaseAdapter.connect(
        f"{okg_migrated}{separator}application_name=issue-2027-caller"
    )
    try:
        assert _backend_application_name(named) == "issue-2027-caller"
    finally:
        named.close()


@pytest.mark.parametrize(
    ("dsn", "expected"),
    [
        ("postgresql://u@h/d", "chronos-branching"),
        ("postgresql://u@h/d?application_name=", "chronos-branching"),
        ("postgresql://u@h/d?application_name=okg-chronos-store", None),
        ("host=h dbname=d application_name=okg-chronos-store", None),
    ],
)
def test_chronos_adapter_passes_its_name_only_when_the_dsn_has_none(
    monkeypatch: pytest.MonkeyPatch, dsn: str, expected: str | None,
) -> None:
    """The keyword reaches psycopg only when the DSN names no session."""
    from chronos_core.branching import sql_adapters

    captured: dict[str, object] = {}

    def fake_connect(conninfo: str, **kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(sql_adapters.psycopg, "connect", fake_connect)
    sql_adapters.PostgresDatabaseAdapter.connect(dsn)

    assert captured.get("application_name") == expected


@pytest.mark.postgres
def test_chronos_adapter_keeps_the_server_message_on_a_connection_failure(
    okg_dsn: str,
) -> None:
    """Only PARSE failures are redacted; a server refusal keeps its message.

    The DSN parses, so the adapter must hand it to psycopg and let the
    server's own rejection through unchanged. The role does not exist, so
    the server refuses the login whatever its authentication method: with
    password authentication it reports a password failure, with trust
    authentication it reports the missing role. Both come from the server's
    authentication step, and a redacted parse message says neither.
    """
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter

    fields = conninfo_to_dict(okg_dsn)
    fields.update(user="issue_2311_no_such_role", password="wrong-password")
    fields["connect_timeout"] = "5"
    bad_login = make_conninfo(**fields)

    with pytest.raises(psycopg.OperationalError) as excinfo:
        PostgresDatabaseAdapter.connect(bad_login)

    message = str(excinfo.value)
    assert (
        'password authentication failed for user "issue_2311_no_such_role"'
        in message
        or 'role "issue_2311_no_such_role" does not exist' in message
    ), message
