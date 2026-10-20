"""One unit of work over a psycopg connection (issue #2574).

Database callers used to write their own lock, transaction, rollback and
retry handling, and each one failed differently: a rollback that raised could
strand a thread lock (#2347), a refusal whose record had to survive was rolled
back with it (#2342), and a retry loop could retry on a connection whose
rollback had just failed (#2302). This module owns those rules once.

* :class:`UnitOfWork` holds an optional thread lock and one transaction. It
  refuses to open on a connection that is already inside a transaction,
  because psycopg would silently nest a savepoint there and the outermost
  exit would commit nothing. A clean exit commits, and the commit is
  confirmed from the connection's transaction status. An error marked with
  :func:`committed_refusal` commits the unit's writes and then propagates. Any
  other error rolls back. The lock is released on every path.
* A failed rollback never replaces the error that caused it. The original
  error propagates with its type and message unchanged, carries a note naming
  the rollback failure, and reads as failed through :func:`rollback_failed`.
  psycopg's own transaction block swallows a failing ROLLBACK and only logs
  it, so the unit also treats a connection that is not idle after the
  rollback as a failed rollback.
* :func:`retry_after_rollback` re-runs a retryable failure only after a
  confirmed rollback: no failed-rollback mark and an idle connection.

``chronos_core`` cannot import okg, so its own rollback guard
(``ChronosBranchContext._rollback_after_error``) sets the same
``rollback_failed`` attribute; ``tests/substrate/db/test_unit_of_work.py``
pins that the two agree.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, TypeVar

_LOG = logging.getLogger(__name__)

_E = TypeVar("_E", bound=BaseException)
_T = TypeVar("_T")

# The attribute names are part of the contract: the late-claim refusal set
# ``commits_transaction`` before this module existed (issue #1925), and
# chronos_core sets ``rollback_failed`` without importing this module.
COMMITTED_REFUSAL_ATTRIBUTE = "commits_transaction"
ROLLBACK_FAILED_ATTRIBUTE = "rollback_failed"


class NestedTransactionRefused(RuntimeError):
    """A unit of work was asked to open on a connection that is not idle."""

    def __init__(self, label: str, status: str) -> None:
        if status == "UNKNOWN":
            reason = "the connection is lost (transaction status UNKNOWN)"
        elif status == "INERROR":
            reason = (
                "the connection is inside a failed transaction (INERROR); roll "
                "it back first"
            )
        else:
            reason = (
                f"the connection is already inside a transaction ({status}); a "
                "transaction opened here would nest as a savepoint and commit "
                "nothing. Commit or roll back the caller's transaction first"
            )
        super().__init__(f"{label}: refusing to open a transaction: {reason}.")
        self.label = label
        self.status = status


class CommitNotConfirmed(RuntimeError):
    """A unit of work could not confirm that its commit took effect."""

    def __init__(self, label: str, status: str, when: str) -> None:
        super().__init__(
            f"{label}: the commit is not confirmed: the connection's transaction "
            f"status is {status} {when}; the unit's writes may be lost."
        )
        self.label = label
        self.status = status


def committed_refusal(error: _E) -> _E:
    """Mark ``error`` so an enclosing unit of work commits before raising it.

    Use it for a refusal whose durable record must survive the refusal, such
    as the expiry event written by a late claim or a late review decision.
    """

    setattr(error, COMMITTED_REFUSAL_ATTRIBUTE, True)
    return error


def is_committed_refusal(error: BaseException | None) -> bool:
    return getattr(error, COMMITTED_REFUSAL_ATTRIBUTE, False) is True


def rollback_failed(error: BaseException) -> bool:
    """True when the rollback after ``error`` failed (here or in Chronos)."""

    return getattr(error, ROLLBACK_FAILED_ATTRIBUTE, False) is True


def mark_rollback_failed(
    error: BaseException, rollback_error: BaseException | str, *, label: str
) -> None:
    """Record on ``error`` that the rollback after it failed.

    The caller keeps propagating ``error``; this only notes the second
    failure so the operator sees both and a retry loop can refuse to retry.
    """

    if isinstance(rollback_error, BaseException):
        described = f"{type(rollback_error).__name__}: {rollback_error}"
    else:
        described = rollback_error
    setattr(error, ROLLBACK_FAILED_ATTRIBUTE, True)
    error.add_note(f"{label}: the rollback after this error also failed: {described}")
    _LOG.warning(
        "%s: rollback after %s failed; re-raising the original error: %s",
        label,
        type(error).__name__,
        described,
    )


def transaction_status(connection: Any) -> str:
    """Name of the psycopg connection's transaction status (``IDLE`` etc.)."""

    status = connection.info.transaction_status
    return str(getattr(status, "name", status))


def connection_idle(connection: Any) -> bool:
    return transaction_status(connection) == "IDLE"


def rollback_keeping_error(
    error: BaseException,
    rollback: Callable[[], Any],
    *,
    label: str,
    connection: Any = None,
) -> bool:
    """Run ``rollback`` after ``error`` without letting a failure replace it.

    Returns True only for a confirmed rollback: ``rollback`` returned and,
    when ``connection`` is given, the connection is idle afterwards. On any
    other outcome ``error`` is marked with :func:`mark_rollback_failed` and
    the caller re-raises it unchanged.
    """

    try:
        rollback()
    except Exception as rollback_error:
        mark_rollback_failed(error, rollback_error, label=label)
        return False
    if connection is not None:
        status = transaction_status(connection)
        if status != "IDLE":
            mark_rollback_failed(
                error,
                f"the connection's transaction status is {status} after the rollback",
                label=label,
            )
            return False
    return True


def rollback_confirmed(error: BaseException, connection: Any) -> bool:
    """True when the rollback after ``error`` is known to have succeeded."""

    return not rollback_failed(error) and connection_idle(connection)


def retry_after_rollback(
    attempt: Callable[[], _T],
    *,
    retryable: Callable[[BaseException], bool],
    connection: Any,
    limit: int,
    label: str,
) -> _T:
    """Run ``attempt`` up to ``limit`` times, retrying only safe failures.

    A failure is retried only when ``retryable`` accepts it and its rollback
    is confirmed (:func:`rollback_confirmed`). Every other failure, and the
    last one, propagates unchanged; a retryable one that was not retried
    because its rollback is unconfirmed carries a note saying so.
    """

    if limit < 1:
        raise ValueError(f"{label}: retry limit must be at least 1, got {limit}")
    for number in range(1, limit + 1):
        try:
            return attempt()
        except Exception as error:
            if number >= limit or not retryable(error):
                raise
            if not rollback_confirmed(error, connection):
                note_unconfirmed_rollback(error, connection, label=label)
                raise
    raise AssertionError("unreachable")


def note_unconfirmed_rollback(
    error: BaseException, connection: Any, *, label: str
) -> None:
    """Note on ``error`` that it was not retried; nested retry loops note once."""

    note = (
        f"{label}: not retried because the rollback after this error is not "
        f"confirmed (rollback_failed={rollback_failed(error)}, connection "
        f"transaction status {transaction_status(connection)})"
    )
    if note not in getattr(error, "__notes__", ()):
        error.add_note(note)


class UnitOfWork:
    """An optional thread lock plus one transaction on one connection.

    Re-entrant on one thread when ``lock`` is re-entrant: a nested entry joins
    the outer transaction, and only the outermost exit commits or rolls back.
    ``connection`` is the psycopg connection or a zero-argument callable that
    returns it. ``on_open`` runs inside the new transaction, after it begins;
    ``on_close`` runs after the transaction ends, on every path. Without a
    lock, one instance must not be shared between threads.

    The context manager never swallows an exception.
    """

    def __init__(
        self,
        connection: Any,
        *,
        label: str,
        lock: Any = None,
        on_open: Callable[[], Any] | None = None,
        on_close: Callable[[], Any] | None = None,
    ) -> None:
        self._connection = connection
        self._label = label
        self._lock = lock
        self._on_open = on_open
        self._on_close = on_close
        self._depth = 0
        self._transaction: Any = None

    @property
    def connection(self) -> Any:
        connection = self._connection
        return connection() if callable(connection) else connection

    @property
    def active(self) -> bool:
        return self._depth > 0

    def __enter__(self) -> Any:
        if self._lock is not None:
            self._lock.acquire()
        try:
            if self._depth == 0:
                self._open()
            self._depth += 1
        except BaseException:
            if self._lock is not None:
                self._lock.release()
            raise
        return self

    def __exit__(self, exc_type: Any, exc: BaseException | None, tb: Any) -> None:
        try:
            self._depth -= 1
            if self._depth == 0:
                transaction, self._transaction = self._transaction, None
                try:
                    self._finish(transaction, exc)
                finally:
                    if self._on_close is not None:
                        self._on_close()
        finally:
            if self._lock is not None:
                self._lock.release()

    def _open(self) -> None:
        connection = self.connection
        status = transaction_status(connection)
        if status != "IDLE":
            raise NestedTransactionRefused(self._label, status)
        transaction = connection.transaction()
        transaction.__enter__()
        if self._on_open is None:
            self._transaction = transaction
            return
        try:
            self._on_open()
        except BaseException as error:
            try:
                self._finish(transaction, error)
            finally:
                if self._on_close is not None:
                    self._on_close()
            raise
        self._transaction = transaction

    def _finish(self, transaction: Any, error: BaseException | None) -> None:
        if error is None or is_committed_refusal(error):
            # Commit. A committed refusal propagates after this returns; a
            # failed or unconfirmed commit raises in its place, so a caller
            # never sees a refusal whose record was lost.
            self._commit(transaction)
            return
        rollback_keeping_error(
            error,
            lambda: transaction.__exit__(type(error), error, error.__traceback__),
            label=self._label,
            connection=self.connection,
        )

    def _commit(self, transaction: Any) -> None:
        """Commit and confirm it from the connection's transaction status.

        psycopg's transaction block returns without committing on a bad
        connection, and PostgreSQL answers COMMIT on a failed transaction
        with a silent ROLLBACK, so a returned ``__exit__`` is not proof.
        """

        connection = self.connection
        before = transaction_status(connection)
        if before != "INTRANS":
            refused = CommitNotConfirmed(self._label, before, "before the commit")
            rollback_keeping_error(
                refused,
                lambda: transaction.__exit__(CommitNotConfirmed, refused, None),
                label=self._label,
                connection=connection,
            )
            raise refused
        transaction.__exit__(None, None, None)
        after = transaction_status(connection)
        if after != "IDLE":
            raise CommitNotConfirmed(self._label, after, "after the commit")


__all__ = [
    "CommitNotConfirmed",
    "COMMITTED_REFUSAL_ATTRIBUTE",
    "NestedTransactionRefused",
    "ROLLBACK_FAILED_ATTRIBUTE",
    "UnitOfWork",
    "committed_refusal",
    "connection_idle",
    "is_committed_refusal",
    "mark_rollback_failed",
    "note_unconfirmed_rollback",
    "retry_after_rollback",
    "rollback_confirmed",
    "rollback_failed",
    "rollback_keeping_error",
    "transaction_status",
]
