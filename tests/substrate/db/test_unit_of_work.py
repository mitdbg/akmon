"""The shared unit of work: lock, transaction, rollback and retry (issue #2574).

The fake-connection rows drive every branch of ``UnitOfWork`` and
``retry_after_rollback`` deterministically. The postgres rows repeat the three
claims that depend on real driver behavior: a connection left inside a
transaction is refused, a committed refusal's row is visible from a second
connection, and a rollback on a dead connection keeps the original error.
"""
from __future__ import annotations

import threading
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from okg.substrate.db.unit_of_work import (
    CommitNotConfirmed,
    NestedTransactionRefused,
    UnitOfWork,
    committed_refusal,
    is_committed_refusal,
    retry_after_rollback,
    rollback_confirmed,
    rollback_failed,
)

IDLE = psycopg.pq.TransactionStatus.IDLE
INTRANS = psycopg.pq.TransactionStatus.INTRANS
INERROR = psycopg.pq.TransactionStatus.INERROR
UNKNOWN = psycopg.pq.TransactionStatus.UNKNOWN


class _Boom(Exception):
    """The original failure every path must surface unchanged."""


class _FakeTransaction:
    def __init__(self, conn: "_FakeConnection") -> None:
        self.conn = conn

    def __enter__(self) -> "_FakeTransaction":
        self.conn.calls.append("begin")
        self.conn.status = INTRANS
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        if exc_type is None:
            self.conn.calls.append("commit")
            self.conn.status = self.conn.commit_leaves or IDLE
            if self.conn.fail_commit:
                raise psycopg.OperationalError("commit failed")
            return False
        self.conn.calls.append("rollback")
        if self.conn.rollback == "raises":
            self.conn.status = UNKNOWN
            raise psycopg.OperationalError("rollback failed: server closed the connection")
        if self.conn.rollback == "swallowed":
            # psycopg's own Transaction.__exit__ logs a failing ROLLBACK and
            # returns; the connection is left unusable.
            self.conn.status = UNKNOWN
            return False
        self.conn.status = IDLE
        return False


class _FakeConnection:
    def __init__(
        self,
        *,
        status: psycopg.pq.TransactionStatus = IDLE,
        fail_commit: bool = False,
        rollback: str = "ok",
        fail_begin: bool = False,
        commit_leaves: psycopg.pq.TransactionStatus | None = None,
    ) -> None:
        self.commit_leaves = commit_leaves
        self.status = status
        self.fail_commit = fail_commit
        self.rollback = rollback
        self.fail_begin = fail_begin
        self.calls: list[str] = []

    @property
    def info(self) -> SimpleNamespace:
        return SimpleNamespace(transaction_status=self.status)

    def transaction(self) -> _FakeTransaction:
        if self.fail_begin:
            raise psycopg.OperationalError("begin failed")
        return _FakeTransaction(self)


def _released(lock: Any) -> bool:
    """True when another thread can take ``lock`` within 2 seconds.

    The lock is re-entrant, so only a different thread proves this thread let
    go of it.
    """

    acquired: list[bool] = []

    def other_thread() -> None:
        got = lock.acquire(timeout=2)
        acquired.append(got)
        if got:
            lock.release()

    worker = threading.Thread(target=other_thread)
    worker.start()
    worker.join(timeout=5)
    return acquired == [True]


def _raise_inside(unit: UnitOfWork, error: BaseException) -> None:
    # Kept in a helper so a static analyzer, which does not know that
    # ``pytest.raises`` swallows the error, sees the checks after it.
    with unit:
        raise error


def _unit(conn: _FakeConnection, **kwargs: Any) -> tuple[UnitOfWork, Any]:
    lock = threading.RLock()
    return UnitOfWork(conn, label="test unit", lock=lock, **kwargs), lock


# (a) ------------------------------------------------------------------------


@pytest.mark.parametrize("status", [INTRANS, INERROR, UNKNOWN])
def test_open_refuses_a_connection_that_is_not_idle(status: psycopg.pq.TransactionStatus) -> None:
    conn = _FakeConnection(status=status)
    unit, lock = _unit(conn)
    with pytest.raises(NestedTransactionRefused) as refused:
        with unit:
            pass
    assert refused.value.status == status.name
    assert status.name in str(refused.value)
    assert conn.calls == []
    assert _released(lock)
    assert not unit.active


# (b) ------------------------------------------------------------------------


def test_clean_exit_commits() -> None:
    conn = _FakeConnection()
    unit, lock = _unit(conn)
    with unit as entered:
        assert entered is unit
        assert unit.active
    assert conn.calls == ["begin", "commit"]
    assert _released(lock)


# (c) ------------------------------------------------------------------------


def test_committed_refusal_commits_then_propagates_unchanged() -> None:
    conn = _FakeConnection()
    unit, lock = _unit(conn)
    refusal = committed_refusal(_Boom("deadline reached"))
    assert is_committed_refusal(refusal)
    with pytest.raises(_Boom) as raised:
        _raise_inside(unit, refusal)
    assert raised.value is refusal
    assert conn.calls == ["begin", "commit"]
    assert not rollback_failed(refusal)
    assert _released(lock)


def test_failed_commit_of_a_committed_refusal_raises_in_its_place() -> None:
    conn = _FakeConnection(fail_commit=True)
    unit, lock = _unit(conn)
    refusal = committed_refusal(_Boom("deadline reached"))
    with pytest.raises(psycopg.OperationalError, match="commit failed") as raised:
        _raise_inside(unit, refusal)
    assert raised.value.__context__ is refusal
    assert conn.calls == ["begin", "commit"]
    assert _released(lock)


def test_failed_commit_on_a_clean_exit_raises_and_releases() -> None:
    conn = _FakeConnection(fail_commit=True)
    unit, lock = _unit(conn)
    with pytest.raises(psycopg.OperationalError, match="commit failed"):
        with unit:
            pass
    assert _released(lock)


def test_commit_on_a_failed_transaction_is_refused_and_rolled_back() -> None:
    """PostgreSQL answers COMMIT on a failed transaction with a silent ROLLBACK."""

    conn = _FakeConnection()
    unit, lock = _unit(conn)
    refusal = committed_refusal(_Boom("late"))

    def swallowed_sql_error_then_refuse() -> None:
        with unit:
            conn.status = INERROR  # a statement failed and the caller caught it
            raise refusal

    with pytest.raises(CommitNotConfirmed) as raised:
        swallowed_sql_error_then_refuse()
    assert raised.value.status == "INERROR"
    assert "before the commit" in str(raised.value)
    assert conn.calls == ["begin", "rollback"]
    assert _released(lock)


def test_commit_that_does_not_leave_the_connection_idle_is_not_confirmed() -> None:
    """psycopg's block can return without committing on a bad connection."""

    conn = _FakeConnection(commit_leaves=UNKNOWN)
    unit, lock = _unit(conn)
    with pytest.raises(CommitNotConfirmed) as raised:
        with unit:
            pass
    assert raised.value.status == "UNKNOWN"
    assert "after the commit" in str(raised.value)
    assert _released(lock)


# (d) ------------------------------------------------------------------------


def test_other_errors_roll_back_and_propagate_unchanged() -> None:
    conn = _FakeConnection()
    unit, lock = _unit(conn)
    error = _Boom("write failed")
    with pytest.raises(_Boom) as raised:
        _raise_inside(unit, error)
    assert raised.value is error
    assert conn.calls == ["begin", "rollback"]
    assert not rollback_failed(error)
    assert not getattr(error, "__notes__", [])
    assert _released(lock)


@pytest.mark.parametrize("rollback", ["raises", "swallowed"])
def test_failed_rollback_keeps_the_original_error(rollback: str) -> None:
    conn = _FakeConnection(rollback=rollback)
    unit, lock = _unit(conn)
    error = _Boom("write failed")
    with pytest.raises(_Boom) as raised:
        _raise_inside(unit, error)
    assert raised.value is error
    assert str(error) == "write failed"
    assert rollback_failed(error)
    notes = getattr(error, "__notes__", [])
    assert len(notes) == 1
    assert notes[0].startswith("test unit: the rollback after this error also failed:")
    if rollback == "raises":
        assert "OperationalError: rollback failed" in notes[0]
    else:
        assert "transaction status is UNKNOWN after the rollback" in notes[0]
    assert conn.calls == ["begin", "rollback"]
    assert _released(lock)
    assert not unit.active


# (e) ------------------------------------------------------------------------


def test_lock_released_when_begin_raises() -> None:
    conn = _FakeConnection(fail_begin=True)
    closed: list[bool] = []
    unit, lock = _unit(conn, on_close=lambda: closed.append(True))
    with pytest.raises(psycopg.OperationalError, match="begin failed"):
        with unit:
            pass
    assert _released(lock)
    assert not unit.active
    assert closed == []  # nothing was opened, so nothing closes


@pytest.mark.parametrize("rollback", ["ok", "raises"])
def test_lock_released_when_on_open_raises(rollback: str) -> None:
    conn = _FakeConnection(rollback=rollback)
    closed: list[bool] = []
    error = _Boom("snapshot failed")

    def on_open() -> None:
        raise error

    unit, lock = _unit(conn, on_open=on_open, on_close=lambda: closed.append(True))
    with pytest.raises(_Boom) as raised:
        with unit:
            pass
    assert raised.value is error
    assert conn.calls == ["begin", "rollback"]
    assert rollback_failed(error) is (rollback == "raises")
    assert closed == [True]
    assert _released(lock)
    assert not unit.active


def test_on_close_runs_on_every_exit() -> None:
    for conn, error in (
        (_FakeConnection(), None),
        (_FakeConnection(), _Boom("x")),
        (_FakeConnection(rollback="raises"), _Boom("x")),
        (_FakeConnection(fail_commit=True), None),
    ):
        closed: list[bool] = []
        unit, lock = _unit(conn, on_close=lambda: closed.append(True))
        expect = pytest.raises(Exception) if (error or conn.fail_commit) else nullcontext()
        with expect:
            with unit:
                if error is not None:
                    raise error
        assert closed == [True]
        assert _released(lock)


# (f) ------------------------------------------------------------------------


def test_nested_entry_joins_the_outer_transaction() -> None:
    conn = _FakeConnection()
    unit, lock = _unit(conn)
    with unit:
        with unit:
            assert conn.calls == ["begin"]
        assert conn.calls == ["begin"]
        assert unit.active
    assert conn.calls == ["begin", "commit"]
    assert _released(lock)


def test_error_in_a_nested_entry_rolls_back_once_at_the_outermost_exit() -> None:
    conn = _FakeConnection()
    unit, lock = _unit(conn)
    error = _Boom("inner failed")
    with pytest.raises(_Boom):
        with unit:
            _raise_inside(unit, error)
    assert conn.calls == ["begin", "rollback"]
    assert _released(lock)


def test_committed_refusal_in_a_nested_entry_commits_at_the_outermost_exit() -> None:
    conn = _FakeConnection()
    unit, lock = _unit(conn)
    refusal = committed_refusal(_Boom("late"))
    with pytest.raises(_Boom):
        with unit:
            _raise_inside(unit, refusal)
    assert conn.calls == ["begin", "commit"]
    assert _released(lock)


# (g) ------------------------------------------------------------------------


class _Attempts:
    def __init__(self, errors: list[BaseException | None]) -> None:
        self.errors = errors
        self.count = 0

    def __call__(self) -> str:
        error = self.errors[self.count]
        self.count += 1
        if error is not None:
            raise error
        return "done"


def _retryable(error: BaseException) -> bool:
    return isinstance(error, _Boom)


def test_retry_runs_again_after_a_confirmed_rollback() -> None:
    attempts = _Attempts([_Boom("unique"), None])
    result = retry_after_rollback(
        attempts, retryable=_retryable, connection=_FakeConnection(), limit=3, label="t"
    )
    assert result == "done"
    assert attempts.count == 2


def test_retry_refused_when_the_rollback_failed() -> None:
    error = _Boom("unique")
    setattr(error, "rollback_failed", True)
    attempts = _Attempts([error, None])
    with pytest.raises(_Boom) as raised:
        retry_after_rollback(
            attempts, retryable=_retryable, connection=_FakeConnection(), limit=3, label="t"
        )
    assert raised.value is error
    assert attempts.count == 1
    assert any("not retried" in note for note in error.__notes__)


@pytest.mark.parametrize("status", [INTRANS, INERROR, UNKNOWN])
def test_retry_refused_when_the_connection_is_not_idle(status: psycopg.pq.TransactionStatus) -> None:
    error = _Boom("unique")
    attempts = _Attempts([error, None])
    with pytest.raises(_Boom) as raised:
        retry_after_rollback(
            attempts,
            retryable=_retryable,
            connection=_FakeConnection(status=status),
            limit=3,
            label="t",
        )
    assert raised.value is error
    assert attempts.count == 1
    assert not rollback_confirmed(error, _FakeConnection(status=status))


def test_retry_does_not_retry_a_non_retryable_error() -> None:
    error = ValueError("other")
    attempts = _Attempts([error, None])
    with pytest.raises(ValueError):
        retry_after_rollback(
            attempts, retryable=_retryable, connection=_FakeConnection(), limit=3, label="t"
        )
    assert attempts.count == 1
    assert not getattr(error, "__notes__", [])


def test_retry_raises_the_last_error_when_the_limit_is_reached() -> None:
    errors: list[BaseException | None] = [_Boom("1"), _Boom("2"), _Boom("3")]
    attempts = _Attempts(errors)
    with pytest.raises(_Boom) as raised:
        retry_after_rollback(
            attempts, retryable=_retryable, connection=_FakeConnection(), limit=3, label="t"
        )
    assert raised.value is errors[2]
    assert attempts.count == 3


def test_retry_limit_must_be_positive() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        retry_after_rollback(
            lambda: None, retryable=_retryable, connection=_FakeConnection(), limit=0, label="t"
        )


# Chronos parity -------------------------------------------------------------


def test_chronos_failed_rollback_reads_as_rollback_failed() -> None:
    """chronos_core marks a failed rollback the way this module reads it."""

    from chronos_core.branching._runtime import ChronosBranchContext

    context = ChronosBranchContext.__new__(ChronosBranchContext)
    error = _Boom("write failed")

    def failing_rollback() -> None:
        raise psycopg.OperationalError("rollback failed")

    assert context._rollback_after_error(error, failing_rollback) is False
    assert rollback_failed(error)

    clean = _Boom("write failed")
    assert context._rollback_after_error(clean, lambda: None) is True
    assert not rollback_failed(clean)


# Real PostgreSQL --------------------------------------------------------------


@pytest.fixture
def probe_table(okg_migrated: str):
    from okg.substrate.db.connection import connect

    name = "unit_of_work_probe_" + uuid4().hex[:12]
    with connect(okg_migrated, autocommit=True) as admin:
        admin.execute(f"CREATE TABLE public.{name} (id text PRIMARY KEY)")
    try:
        yield okg_migrated, name
    finally:
        with connect(okg_migrated, autocommit=True) as admin:
            admin.execute(f"DROP TABLE IF EXISTS public.{name}")


def _committed_ids(dsn: str, table: str) -> list[str]:
    from okg.substrate.db.connection import connect

    with connect(dsn, autocommit=True) as observer:
        rows = observer.execute(f"SELECT id FROM public.{table} ORDER BY id").fetchall()
    return [str(row[0]) for row in rows]


@pytest.mark.postgres
def test_postgres_refuses_a_connection_left_inside_a_transaction(probe_table) -> None:
    from okg.substrate.db.connection import connect

    dsn, table = probe_table
    with connect(dsn) as conn:
        conn.execute("SELECT 1")  # psycopg opens a transaction implicitly
        assert conn.info.transaction_status == INTRANS
        unit = UnitOfWork(conn, label="probe", lock=threading.RLock())
        with pytest.raises(NestedTransactionRefused) as refused:
            with unit:
                conn.execute(f"INSERT INTO public.{table} VALUES ('nested')")
        assert refused.value.status == "INTRANS"
        conn.rollback()
    assert _committed_ids(dsn, table) == []


@pytest.mark.postgres
def test_postgres_committed_refusal_keeps_its_row_and_other_errors_do_not(
    probe_table,
) -> None:
    from okg.substrate.db.connection import connect

    dsn, table = probe_table
    with connect(dsn) as conn:
        unit = UnitOfWork(conn, label="probe", lock=threading.RLock())

        def write_then_raise(row_id: str, error: BaseException) -> None:
            with unit:
                conn.execute(f"INSERT INTO public.{table} VALUES (%s)", (row_id,))
                raise error

        refusal = committed_refusal(_Boom("late"))
        with pytest.raises(_Boom) as raised:
            write_then_raise("kept", refusal)
        assert raised.value is refusal
        with pytest.raises(_Boom):
            write_then_raise("dropped", _Boom("plain"))
        assert conn.info.transaction_status == IDLE
    assert _committed_ids(dsn, table) == ["kept"]


@pytest.mark.postgres
def test_postgres_rollback_on_a_dead_connection_keeps_the_original_error(
    probe_table,
) -> None:
    from okg.substrate.db.connection import connect

    dsn, table = probe_table
    lock = threading.RLock()
    with connect(dsn) as conn:
        pid = conn.info.backend_pid
        unit = UnitOfWork(conn, label="probe", lock=lock)
        error = _Boom("write failed")


        def write_kill_then_raise() -> None:
            with unit:
                conn.execute(f"INSERT INTO public.{table} VALUES ('lost')")
                with connect(dsn, autocommit=True) as killer:
                    # Wait up to 5 s for the backend to exit, so the client's
                    # ROLLBACK cannot reach it first on a loaded runner.
                    assert killer.execute(
                        "SELECT pg_terminate_backend(%s, 5000)", (pid,)
                    ).fetchone()[0] is True
                raise error

        with pytest.raises(_Boom) as raised:
            write_kill_then_raise()
        assert raised.value is error
        assert rollback_failed(error)
        assert any(
            "the rollback after this error also failed" in note
            for note in error.__notes__
        )
        assert conn.info.transaction_status != IDLE
    assert _released(lock)
    assert _committed_ids(dsn, table) == []
