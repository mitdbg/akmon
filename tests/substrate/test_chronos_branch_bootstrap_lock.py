"""The Chronos branch-metadata bootstrap must run under the session advisory
lock, so concurrent sessions can't race the `CREATE TABLE IF NOT EXISTS`
(which is not atomic against the composite-type create and otherwise fails the
loser with a duplicate pg_type_typname_nsp_index).

Deterministic: a recording fake adapter that reports the postgres dialect (so
the lock engages) and captures executed SQL. No Postgres, threads, or timing.
"""
from __future__ import annotations

from chronos_core.branching._common import (
    _ensure_index_registry,
    _parse_table_registry,
)

_LOCK = "pg_advisory_xact_lock(1720812901, 19840717)"


class _RecordingDB:
    """Minimal SQLDatabaseAdapter stand-in: records executed SQL and reports
    the postgres dialect so `_chronos_metadata_lock` takes the advisory lock."""

    dialect = "postgres"

    def __init__(self, *, existing: set[str] | None = None) -> None:
        self.sql: list[str] = []
        self.existing = existing or set()
        # `in_transaction` is an abstract member of SQLDatabaseAdapter, so the
        # stand-in has to model it: on a non-autocommit connection the FIRST
        # statement opens the transaction, and only commit/rollback ends it.
        # Issue #1997 turns on exactly that state, so a fake that skipped it
        # would let the leak back in unseen.
        self._in_transaction = False
        self.ended: list[str] = []

    @property
    def in_transaction(self) -> bool:
        return self._in_transaction

    def execute(self, sql, params=None):  # noqa: ANN001
        normalized = " ".join(str(sql).split())
        self.sql.append(normalized)
        self._in_transaction = True
        if "SELECT to_regclass" in normalized:
            name = params[0]
            return [{"reg": name if name in self.existing else None}]
        return []  # fresh schema: no registry rows

    def commit(self) -> None:
        self._in_transaction = False
        self.ended.append("commit")

    def rollback(self) -> None:
        self._in_transaction = False
        self.ended.append("rollback")


def _idx(db: _RecordingDB, needle: str) -> int:
    return next(i for i, s in enumerate(db.sql) if needle in s)


def test_branch_table_bootstrap_is_serialized_by_metadata_lock() -> None:
    db = _RecordingDB()
    _parse_table_registry(db, "demo")
    # CREATE is wrapped: the transaction-scoped lock is acquired before it.
    assert (
        _idx(db, _LOCK)
        < _idx(db, "CREATE TABLE IF NOT EXISTS _chronos_branch_tables")
    )
    # Issue #1997: the transaction this bootstrap opened for itself must not
    # outlive it. A transaction-scoped advisory lock is released only at
    # transaction end, and an unended one also pins the cluster vacuum
    # horizon.
    assert db.ended == ["commit"], db.ended
    assert not db.in_transaction


def test_existing_branch_table_registry_skips_bootstrap_ddl() -> None:
    db = _RecordingDB(existing={"_chronos_branch_tables"})
    _parse_table_registry(db, "demo")

    assert not any(_LOCK in sql for sql in db.sql)
    assert not any(
        "CREATE TABLE IF NOT EXISTS _chronos_branch_tables" in sql for sql in db.sql
    )


def test_branch_index_bootstrap_is_serialized_by_metadata_lock() -> None:
    db = _RecordingDB()
    _ensure_index_registry(db)
    assert (
        _idx(db, _LOCK)
        < _idx(db, "CREATE TABLE IF NOT EXISTS _chronos_branch_indexes")
    )
    # Issue #1997, as above.
    assert db.ended == ["commit"], db.ended
    assert not db.in_transaction


def test_existing_branch_index_registry_skips_bootstrap_ddl() -> None:
    db = _RecordingDB(existing={"_chronos_branch_indexes"})
    _ensure_index_registry(db)

    assert not any(_LOCK in sql for sql in db.sql)
    assert not any(
        "CREATE TABLE IF NOT EXISTS _chronos_branch_indexes" in sql for sql in db.sql
    )


def test_bootstrap_never_ends_a_transaction_its_caller_owns() -> None:
    """Issue #1997: only the transaction this helper opened is its to end.

    `_runtime.py` wraps `refresh_registries()` in its own
    `_chronos_metadata_lock` and decides commit-versus-rollback itself, and a
    store built with `autocommit=False` hands that decision to the caller
    through `finalize_external_transaction`. Committing there would break both
    contracts, so a session that is ALREADY in a transaction on entry must be
    handed back exactly as it was found.
    """
    db = _RecordingDB()
    db.execute("SELECT 1")  # the caller's own transaction is already open
    assert db.in_transaction

    _parse_table_registry(db, "demo")

    assert db.ended == [], db.ended
    assert db.in_transaction
    # The guarded region still ran under the lock.
    assert any(_LOCK in sql for sql in db.sql)


def test_bootstrap_rolls_back_its_own_transaction_when_the_ddl_fails() -> None:
    """A failure must not leave the lock and the transaction parked either."""

    class _FailingDB(_RecordingDB):
        def execute(self, sql, params=None):  # noqa: ANN001
            result = super().execute(sql, params)
            if "CREATE TABLE IF NOT EXISTS" in " ".join(str(sql).split()):
                raise RuntimeError("bootstrap DDL failed")
            return result

    db = _FailingDB()
    try:
        _parse_table_registry(db, "demo")
    except RuntimeError:
        pass
    else:  # pragma: no cover - the fake always raises
        raise AssertionError("the failing DDL did not propagate")

    assert db.ended == ["rollback"], db.ended
    assert not db.in_transaction
