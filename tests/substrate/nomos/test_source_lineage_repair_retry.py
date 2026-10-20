"""Lineage-repair retries run only after a confirmed rollback (issue #2302).

``_upsert_logical_batch`` retries a batch and ``_upsert_logical_rows_with_retry``
splits one whenever the write fails with a retryable interval unique-key
violation. Since #2159 a failed Chronos rollback surfaces that original
(retryable) error, so both loops used to retry, and split, on a connection
whose rollback had just failed. Now each retry and each split first needs a
confirmed rollback: no ``rollback_failed`` mark and an idle connection.

Deterministic: fake sessions and a fake connection status, plus one real
``BranchSession`` whose database rollback fails. No Postgres.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import psycopg
import pytest

from chronos_core.branching._common import _PreparedBranchRef
from chronos_core.branching._runtime import BranchSession, ChronosBranchContext
from okg.substrate.db.unit_of_work import rollback_failed
from okg.substrate.nomos.source_lineage_repair import (
    _upsert_logical_batch,
    _upsert_logical_rows_with_retry,
)

_RETRYABLE_MESSAGE = (
    'duplicate key value violates unique constraint "_chronos_b_interval_t_pk"'
)


class _IntervalUniqueViolation(Exception):
    """Name and message match the retryable interval unique-violation check."""


def _connection(status: psycopg.pq.TransactionStatus = psycopg.pq.TransactionStatus.IDLE) -> Any:
    return SimpleNamespace(info=SimpleNamespace(transaction_status=status))


class _Session:
    def __init__(self, store: "_Store") -> None:
        self.store = store

    def upsert_rows(self, table: str, rows: list[dict[str, Any]]) -> None:
        self.store.upserts.append([row["edge_id"] for row in rows])
        error = self.store.fail(rows)
        if error is not None:
            raise error

    def execute(self, sql: str, params: dict[str, Any]) -> None:
        self.store.single_updates.append(params["edge_id"])


class _Store:
    """``store.checkout(branch)`` stand-in that records every write."""

    # No class catalog: the read-class stamp skips its catalog write, which
    # these retry tests do not exercise.
    context = SimpleNamespace(db=None)

    def __init__(self, fail: Any) -> None:
        self.fail = fail
        self.upserts: list[list[int]] = []
        self.single_updates: list[int] = []

    def checkout(self, branch: str) -> _Session:
        return _Session(self)


def _violation(*, rollback_failed_mark: bool) -> _IntervalUniqueViolation:
    error = _IntervalUniqueViolation(_RETRYABLE_MESSAGE)
    if rollback_failed_mark:
        error.rollback_failed = True  # type: ignore[attr-defined]
    return error


def _rows(count: int) -> list[dict[str, Any]]:
    return [{"edge_id": index, "attrs": {}} for index in range(count)]


# Refused: failed rollback ---------------------------------------------------


def test_batch_is_not_retried_after_a_failed_rollback() -> None:
    raised: list[BaseException] = []

    def fail(rows: list[dict[str, Any]]) -> BaseException:
        raised.append(_violation(rollback_failed_mark=True))
        return raised[-1]

    store = _Store(fail)
    with pytest.raises(_IntervalUniqueViolation) as caught:
        _upsert_logical_batch(
            store,
            connection=_connection(),
            branch="default",
            table="okg.graph_edges",
            rows=_rows(4),
            retry_limit=3,
        )
    assert caught.value is raised[0]
    assert store.upserts == [[0, 1, 2, 3]]
    assert any("not retried" in note for note in caught.value.__notes__)


def test_rows_are_not_split_after_a_failed_rollback() -> None:
    raised: list[BaseException] = []

    def fail(rows: list[dict[str, Any]]) -> BaseException:
        raised.append(_violation(rollback_failed_mark=True))
        return raised[-1]

    store = _Store(fail)
    with pytest.raises(_IntervalUniqueViolation) as caught:
        _upsert_logical_rows_with_retry(
            store,
            connection=_connection(),
            branch="default",
            table="okg.graph_edges",
            key_column="edge_id",
            rows=_rows(4),
        )
    assert caught.value is raised[0]
    # One write attempt: no retry of the batch, no split, no single-row fallback.
    assert store.upserts == [[0, 1, 2, 3]]
    assert store.single_updates == []
    notes = [note for note in caught.value.__notes__ if "not retried" in note]
    assert len(notes) == 1


@pytest.mark.parametrize(
    "status",
    [psycopg.pq.TransactionStatus.INERROR, psycopg.pq.TransactionStatus.INTRANS, psycopg.pq.TransactionStatus.UNKNOWN],
)
def test_rows_are_not_split_when_the_connection_is_not_idle(
    status: psycopg.pq.TransactionStatus,
) -> None:
    store = _Store(lambda rows: _violation(rollback_failed_mark=False))
    with pytest.raises(_IntervalUniqueViolation):
        _upsert_logical_rows_with_retry(
            store,
            connection=_connection(status),
            branch="default",
            table="okg.graph_edges",
            key_column="edge_id",
            rows=_rows(2),
        )
    assert store.upserts == [[0, 1]]
    assert store.single_updates == []


def test_a_real_chronos_rollback_failure_stops_the_retry() -> None:
    """End to end through ``BranchSession.upsert_rows``.

    The write raises a retryable interval unique violation and the database
    rollback after it fails. Chronos keeps the original error and marks it;
    the repair loop reads the mark and makes no second attempt.
    """

    class DB:
        dialect = "sqlite"  # keeps `_chronos_metadata_lock` from issuing SQL
        in_transaction = True
        rollbacks = 0

        def begin(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            DB.rollbacks += 1
            raise psycopg.OperationalError("server closed the connection unexpectedly")

    class Backend:
        name = "fake"
        prepare_calls = 0

        def prepare_mutation(self, _branch_id: str) -> None:
            Backend.prepare_calls += 1
            raise _IntervalUniqueViolation(_RETRYABLE_MESSAGE)

        def after_commit(self) -> None:
            pass

        def after_rollback(self) -> None:
            pass

    context = ChronosBranchContext(DB(), Backend(), autocommit=True)

    class Store:
        # Class attribute only; checkout() still uses the enclosing branch context.
        context = SimpleNamespace(db=None)

        def checkout(self, branch: str) -> BranchSession:
            ref = _PreparedBranchRef("main", "ref-1", metadata={"_context_epoch": 0})
            return BranchSession(context, ref)

    with pytest.raises(_IntervalUniqueViolation) as caught:
        _upsert_logical_rows_with_retry(
            Store(),
            connection=_connection(),
            branch="default",
            table="okg.graph_edges",
            key_column="edge_id",
            rows=_rows(4),
        )
    assert rollback_failed(caught.value)
    assert Backend.prepare_calls == 1
    assert DB.rollbacks == 1


# Unchanged: confirmed rollback ------------------------------------------------


def test_batch_is_retried_after_a_confirmed_rollback() -> None:
    attempts: list[int] = []

    def fail(rows: list[dict[str, Any]]) -> BaseException | None:
        attempts.append(1)
        return _violation(rollback_failed_mark=False) if len(attempts) == 1 else None

    store = _Store(fail)
    _upsert_logical_batch(
        store,
        connection=_connection(),
        branch="default",
        table="okg.graph_edges",
        rows=_rows(2),
        retry_limit=3,
    )
    assert store.upserts == [[0, 1], [0, 1]]


def test_rows_are_retried_then_split_after_confirmed_rollbacks() -> None:
    """A multi-row batch that keeps failing is retried, then split to singles."""

    def fail(rows: list[dict[str, Any]]) -> BaseException | None:
        return _violation(rollback_failed_mark=False) if len(rows) > 1 else None

    store = _Store(fail)
    _upsert_logical_rows_with_retry(
        store,
        connection=_connection(),
        branch="default",
        table="okg.graph_edges",
        key_column="edge_id",
        rows=_rows(2),
    )
    assert store.upserts == [[0, 1], [0, 1], [0, 1], [0], [1]]
    assert store.single_updates == []


def test_a_single_row_falls_back_to_an_attrs_update_after_confirmed_rollbacks() -> None:
    store = _Store(lambda rows: _violation(rollback_failed_mark=False))
    _upsert_logical_rows_with_retry(
        store,
        connection=_connection(),
        branch="default",
        table="okg.graph_edges",
        key_column="edge_id",
        rows=_rows(1),
    )
    assert store.upserts == [[0], [0], [0]]
    assert store.single_updates == [0]


def test_a_non_retryable_error_propagates_on_the_first_attempt() -> None:
    store = _Store(lambda rows: ValueError("not a unique violation"))
    with pytest.raises(ValueError):
        _upsert_logical_rows_with_retry(
            store,
            connection=_connection(),
            branch="default",
            table="okg.graph_edges",
            key_column="edge_id",
            rows=_rows(2),
        )
    assert store.upserts == [[0, 1]]
