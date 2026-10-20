"""Stable compacted-generation refusal and renewable historical read leases."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import math
import time
import uuid
from collections.abc import Iterator
from typing import Any, Protocol

from okg.substrate.errors import GenerationCompacted, SubstrateError


_LEASE_SECONDS = 300

# Renew a held lease only once this much of its life has elapsed.
#
# A pin protects its generation from compaction until `expires_at`, so
# re-writing the lease before *every* query spends 7 round trips -- a write,
# a tombstone read, a checkpoint probe, two catalog probes and a COMMIT -- to
# restate something already true for the next `_LEASE_SECONDS`.  That per-query
# cost showed up as a cold-read latency regression in the operator UI SLO.
# Renewing at a third of the lease leaves a 200s margin, far longer than any
# single query, so the compaction-protection invariant is unchanged.
_LEASE_REFRESH_SECONDS = _LEASE_SECONDS / 3


@dataclass(frozen=True, slots=True)
class GenerationReadLeaseState:
    """Database-observed state for one scoped history-read lease.

    The timestamps must come from the database transaction that observed the
    pin.  Comparing two application clocks cannot prove that a database lease
    exists or remains live.
    """

    pin_name: str
    deployment_name: str
    branch: str
    checkpoint_id: str
    observed_at_epoch: float
    expires_at_epoch: float


class GenerationReadLeaseBackend(Protocol):
    """Optional scoped lease authority used by supported generation checkout.

    The default checkout path continues to call OKG's generic lease functions.
    A deployment that cannot safely grant those generic functions may provide
    a backend whose database functions constrain the caller to a narrower
    checkpoint scope.  The backend only replaces lease persistence; OKG still
    validates the checkpoint and owns the checkout/session lifecycle.
    """

    def acquire(
        self,
        context: Any,
        *,
        checkpoint_id: str,
        deployment_name: str,
        branch: str,
    ) -> str:
        pass

    def refresh(
        self,
        context: Any,
        *,
        checkpoint_id: str,
        deployment_name: str,
        branch: str,
        pin_name: str,
    ) -> None:
        pass

    def release(
        self,
        context: Any,
        *,
        deployment_name: str,
        branch: str,
        pin_name: str,
    ) -> None:
        pass

    def inspect(
        self,
        context: Any,
        *,
        pin_name: str,
    ) -> GenerationReadLeaseState | None:
        pass


def _value(row: Any, key: str, index: int) -> Any:
    if row is None:
        return None
    if hasattr(row, "get"):
        return row.get(key)
    return row[index]


def compacted_record(
    db: Any,
    generation_id: int | str,
    *,
    deployment_name: str | None = None,
) -> dict[str, Any] | None:
    """Read a tombstone through the Chronos SQL adapter (``?`` placeholders)."""
    relation = db.execute(
        "SELECT to_regclass('okg.compacted_generations') AS relation"
    ).fetchone()
    if relation is None or _value(relation, "relation", 0) is None:
        return None
    text = str(generation_id)
    function = db.execute(
        "SELECT to_regprocedure("
        "'okg.compacted_generation_record(text,text)') AS procedure"
    ).fetchone()
    if function is not None and _value(function, "procedure", 0) is not None:
        row = db.execute(
            "SELECT okg.compacted_generation_record(?, ?) AS record",
            (text, deployment_name),
        ).fetchone()
        record = _value(row, "record", 0)
        return dict(record) if record is not None else None
    numeric = int(text) if text.isdigit() else None
    query = (
        "SELECT checkpoint_id, target_generation_id, deployment_name, branch_id, "
        "plan_id::text AS plan_id, compacted_at, reason "
        "FROM okg.compacted_generations WHERE (checkpoint_id = ?"
    )
    params: list[Any] = [text]
    if numeric is not None and deployment_name is not None:
        query += " OR target_generation_id = ?"
        params.append(numeric)
    query += ")"
    if deployment_name is not None:
        query += " AND deployment_name = ?"
        params.append(deployment_name)
    row = db.execute(query, tuple(params)).fetchone()
    if row is None:
        return None
    compacted_at = _value(row, "compacted_at", 5)
    return {
        "checkpoint_id": str(_value(row, "checkpoint_id", 0)),
        "target_generation_id": _value(row, "target_generation_id", 1),
        "deployment_name": str(_value(row, "deployment_name", 2)),
        "branch": str(_value(row, "branch_id", 3)),
        "plan_id": str(_value(row, "plan_id", 4)),
        "compacted_at": compacted_at.isoformat() if compacted_at else None,
        "reason": str(_value(row, "reason", 6)),
    }


def raise_if_compacted(
    db: Any,
    generation_id: int | str,
    *,
    deployment_name: str | None = None,
) -> None:
    record = compacted_record(
        db, generation_id, deployment_name=deployment_name
    )
    if record is not None:
        raise GenerationCompacted(
            f"generation {generation_id!r} was retired by history compaction",
            data=record,
            fix_hint="choose a retained generation from `okg generation list`",
        )


class LeasedGenerationSession:
    """Proxy that renews a DB-backed lease before every historical query."""

    def __init__(
        self,
        *,
        context: Any,
        session: Any,
        checkpoint_id: str,
        deployment_name: str,
        branch: str,
        pin_name: str,
        lease_backend: GenerationReadLeaseBackend | None = None,
    ) -> None:
        self._context = context
        self._session = session
        self._checkpoint_id = checkpoint_id
        self._deployment_name = deployment_name
        self._branch = branch
        self._pin_name = pin_name
        self._lease_backend = lease_backend
        # The caller acquires the lease immediately before constructing this
        # proxy, so the pin is fresh as of now.  Both clocks are stamped: see
        # `_lease_age`.
        self._leased_at = time.monotonic()
        self._leased_at_wall = time.time()

    def _lease_age(self) -> float:
        """Seconds since the pin was written, robust to system suspend.

        `time.monotonic()` does not advance while the host is suspended on
        either macOS or Linux, but `expires_at` is database real time.  A
        laptop lid, VM pause, or container freeze could therefore let a
        long-expired pin look fresh, and the read would silently return rows
        from a generation compaction had already retired.  Taking the larger
        of the two elapsed values renews on wake instead.  The wall clock can
        step backwards under NTP, which only makes its term smaller, so the
        monotonic term still bounds the age from below.
        """
        return max(
            time.monotonic() - self._leased_at,
            time.time() - self._leased_at_wall,
        )

    def _refresh(self) -> None:
        if self._lease_age() < _LEASE_REFRESH_SECONDS:
            return
        _write_lease(
            self._context,
            checkpoint_id=self._checkpoint_id,
            deployment_name=self._deployment_name,
            branch=self._branch,
            pin_name=self._pin_name,
            lease_backend=self._lease_backend,
        )
        # Only advance on success: a failed renewal must be retried by the
        # next query rather than silently extending the assumed lease.
        self._leased_at = time.monotonic()
        self._leased_at_wall = time.time()

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self._refresh()
        return self._session.query(sql, params)

    def session_for_read_only_query(self) -> Any:
        """Renew the lease if due, then return the inner checkout session.

        For a caller that runs its next query in a read-only transaction: the
        renewal is a write, so it must happen first, and the read must then
        go to the inner session, which does not renew. Calling `query()`
        instead could cross the renewal age between the two steps and write
        under read-only. The lease is 300 seconds and renews at 100, so one
        bounded query (at most 60 seconds) cannot outlive it.

        This is the only renewal ahead of a read-only read. run_sql's graph
        path (issue #2899) calls the returned inner session's
        `read_relations()` and splices those subqueries into the caller's
        text; see `read_relations` below for why the proxy refuses it.
        """
        self._refresh()
        return self._session

    def read_relations(self) -> Any:
        """Refuse: read relations only after `session_for_read_only_query`.

        Relations are read to splice them into SQL that runs read-only. A
        renewal here would be a write under read-only, and without this method
        `__getattr__` would pass the call through to the inner session with no
        renewal at all, reading on an unrenewed lease. Both are wrong, so the
        one supported order is renew (`session_for_read_only_query`), enter
        read-only, then read relations from the inner session.
        """
        raise SubstrateError(
            "read relations from session_for_read_only_query(), not the leased proxy",
            data={"pin_name": self._pin_name},
            fix_hint=(
                "call session_for_read_only_query() before the read-only "
                "boundary and read relations from the session it returns"
            ),
        )

    def close(self) -> None:
        release_generation_read_lease(
            self._context,
            checkpoint_id=self._checkpoint_id,
            deployment_name=self._deployment_name,
            branch=self._branch,
            pin_name=self._pin_name,
            lease_backend=self._lease_backend,
        )
        # The pin is gone. Expire the local stamps so that a query issued
        # after close re-acquires and re-validates instead of trusting a
        # lease that no longer exists.
        self._leased_at -= _LEASE_SECONDS
        self._leased_at_wall -= _LEASE_SECONDS

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)


def acquire_generation_read_lease(
    context: Any,
    *,
    checkpoint_id: str,
    deployment_name: str,
    branch: str,
    lease_backend: GenerationReadLeaseBackend | None = None,
) -> str | None:
    """Acquire a lease before checkout, atomically coordinated with tombstoning."""
    if lease_backend is not None:
        managed_transaction = False
        try:
            with _scoped_lease_transaction(context) as managed_transaction:
                pin_name = lease_backend.acquire(
                    context,
                    checkpoint_id=checkpoint_id,
                    deployment_name=deployment_name,
                    branch=branch,
                )
                if not isinstance(pin_name, str) or not pin_name.startswith("read:"):
                    raise SubstrateError(
                        "scoped history read lease backend returned an invalid pin name",
                        data={"checkpoint_id": checkpoint_id},
                    )
                _attest_scoped_lease(
                    lease_backend,
                    context,
                    pin_name=pin_name,
                    checkpoint_id=checkpoint_id,
                    deployment_name=deployment_name,
                    branch=branch,
                    require_active=True,
                )
                _validate_lease_target(
                    context,
                    checkpoint_id=checkpoint_id,
                    deployment_name=deployment_name,
                )
                if not managed_transaction:
                    context._commit_autocommit()
                return pin_name
        except Exception:
            if not managed_transaction:
                context._rollback_autocommit()
            raise

    db = context.db
    relation = db.execute(
        "SELECT to_regclass('okg.history_generation_pins') AS relation"
    ).fetchone()
    if relation is None or _value(relation, "relation", 0) is None:
        return None
    pin_name = f"read:{uuid.uuid4()}"
    _write_lease(
        context,
        checkpoint_id=checkpoint_id,
        deployment_name=deployment_name,
        branch=branch,
        pin_name=pin_name,
    )
    return pin_name


def _write_lease(
    context: Any,
    *,
    checkpoint_id: str,
    deployment_name: str,
    branch: str,
    pin_name: str,
    lease_backend: GenerationReadLeaseBackend | None = None,
) -> None:
    db = context.db
    if lease_backend is not None:
        managed_transaction = False
        try:
            with _scoped_lease_transaction(context) as managed_transaction:
                previous = _attest_scoped_lease(
                    lease_backend,
                    context,
                    pin_name=pin_name,
                    checkpoint_id=checkpoint_id,
                    deployment_name=deployment_name,
                    branch=branch,
                    require_active=True,
                )
                lease_backend.refresh(
                    context,
                    checkpoint_id=checkpoint_id,
                    deployment_name=deployment_name,
                    branch=branch,
                    pin_name=pin_name,
                )
                _attest_scoped_lease(
                    lease_backend,
                    context,
                    pin_name=pin_name,
                    checkpoint_id=checkpoint_id,
                    deployment_name=deployment_name,
                    branch=branch,
                    require_active=True,
                    expires_after=previous.expires_at_epoch,
                )
                _validate_lease_target(
                    context,
                    checkpoint_id=checkpoint_id,
                    deployment_name=deployment_name,
                )
                if not managed_transaction:
                    context._commit_autocommit()
            return
        except Exception:
            if not managed_transaction:
                context._rollback_autocommit()
            raise

    try:
        # The table trigger obtains the same lock held by destructive apply.
        # Validate after the write while that transaction lock is still held.
        acquire = db.execute(
            "SELECT to_regprocedure("
            "'okg.acquire_history_generation_read_lease(text,text,text,text)') AS procedure"
        ).fetchone()
        if acquire is not None and _value(acquire, "procedure", 0) is not None:
            row = db.execute(
                "SELECT okg.acquire_history_generation_read_lease(?, ?, ?, ?) AS acquired",
                (deployment_name, branch, pin_name, checkpoint_id),
            ).fetchone()
            if row is None or not bool(_value(row, "acquired", 0)):
                raise_if_compacted(
                    db, checkpoint_id, deployment_name=deployment_name
                )
                raise SubstrateError(
                    "historical generation disappeared while acquiring its read lease",
                    data={"checkpoint_id": checkpoint_id},
                )
        else:
            db.execute(
                "INSERT INTO okg.history_generation_pins "
                "(deployment_name, branch_id, pin_name, checkpoint_id, expires_at) "
                "VALUES (?, ?, ?, ?, now() + (? * interval '1 second')) "
                "ON CONFLICT (deployment_name, branch_id, pin_name) DO UPDATE "
                "SET checkpoint_id = EXCLUDED.checkpoint_id, "
                "expires_at = EXCLUDED.expires_at",
                (deployment_name, branch, pin_name, checkpoint_id, _LEASE_SECONDS),
            )
        _validate_lease_target(
            context,
            checkpoint_id=checkpoint_id,
            deployment_name=deployment_name,
        )
        context._commit_autocommit()
    except Exception:
        context._rollback_autocommit()
        raise


def release_generation_read_lease(
    context: Any,
    *,
    checkpoint_id: str,
    deployment_name: str,
    branch: str,
    pin_name: str,
    lease_backend: GenerationReadLeaseBackend | None = None,
) -> None:
    """Release a held lease and verify that a scoped backend removed it."""
    db = context.db
    if lease_backend is not None:
        managed_transaction = False
        try:
            with _scoped_lease_transaction(context) as managed_transaction:
                # An absent or wrong-scope pin must not make a no-op release
                # look successful. Expired rows remain removable so cleanup
                # does not strand database debris after a long-idle reader.
                _attest_scoped_lease(
                    lease_backend,
                    context,
                    pin_name=pin_name,
                    checkpoint_id=checkpoint_id,
                    deployment_name=deployment_name,
                    branch=branch,
                    require_active=False,
                )
                lease_backend.release(
                    context,
                    deployment_name=deployment_name,
                    branch=branch,
                    pin_name=pin_name,
                )
                remaining = lease_backend.inspect(context, pin_name=pin_name)
                if remaining is not None:
                    raise SubstrateError(
                        "scoped history read lease backend did not release its pin",
                        data={
                            "checkpoint_id": checkpoint_id,
                            "pin_name": pin_name,
                        },
                    )
                if not managed_transaction:
                    context._commit_autocommit()
            return
        except Exception:
            if not managed_transaction:
                context._rollback_autocommit()
            raise

    try:
        release = db.execute(
            "SELECT to_regprocedure("
            "'okg.release_history_generation_read_lease(text,text,text)') AS procedure"
        ).fetchone()
        if release is not None and _value(release, "procedure", 0) is not None:
            db.execute(
                "SELECT okg.release_history_generation_read_lease(?, ?, ?) AS released",
                (deployment_name, branch, pin_name),
            )
        else:
            db.execute(
                "DELETE FROM okg.history_generation_pins "
                "WHERE deployment_name = ? AND branch_id = ? AND pin_name = ?",
                (deployment_name, branch, pin_name),
            )
        context._commit_autocommit()
    except Exception:
        context._rollback_autocommit()
        raise


@contextmanager
def _scoped_lease_transaction(context: Any) -> Iterator[bool]:
    """Make backend mutation plus attestation atomic on autocommit drivers."""
    raw = getattr(context, "conn", None)
    transaction = getattr(raw, "transaction", None)
    if (
        bool(getattr(raw, "autocommit", False))
        and not bool(getattr(context.db, "in_transaction", False))
        and callable(transaction)
    ):
        with transaction():
            yield True
        return
    yield False


def _attest_scoped_lease(
    lease_backend: GenerationReadLeaseBackend,
    context: Any,
    *,
    pin_name: str,
    checkpoint_id: str,
    deployment_name: str,
    branch: str,
    require_active: bool,
    expires_after: float | None = None,
) -> GenerationReadLeaseState:
    """Refuse unless the backend proves the exact database lease state."""
    state = lease_backend.inspect(context, pin_name=pin_name)
    if state is None:
        raise SubstrateError(
            "scoped history read lease backend did not persist its pin",
            data={"checkpoint_id": checkpoint_id, "pin_name": pin_name},
        )
    if not isinstance(state, GenerationReadLeaseState):
        raise SubstrateError(
            "scoped history read lease backend returned invalid lease state",
            data={"checkpoint_id": checkpoint_id, "pin_name": pin_name},
        )
    expected = {
        "pin_name": pin_name,
        "deployment_name": deployment_name,
        "branch": branch,
        "checkpoint_id": checkpoint_id,
    }
    actual = {
        "pin_name": state.pin_name,
        "deployment_name": state.deployment_name,
        "branch": state.branch,
        "checkpoint_id": state.checkpoint_id,
    }
    if actual != expected:
        raise SubstrateError(
            "scoped history read lease backend persisted the wrong lease target",
            data={"expected": expected, "actual": actual},
        )
    if not (
        math.isfinite(state.observed_at_epoch)
        and math.isfinite(state.expires_at_epoch)
    ):
        raise SubstrateError(
            "scoped history read lease backend returned invalid lease timestamps",
            data={"checkpoint_id": checkpoint_id, "pin_name": pin_name},
        )
    if require_active and state.expires_at_epoch <= state.observed_at_epoch:
        raise SubstrateError(
            "scoped history read lease backend returned an expired lease",
            data={"checkpoint_id": checkpoint_id, "pin_name": pin_name},
        )
    if expires_after is not None and state.expires_at_epoch <= expires_after:
        raise SubstrateError(
            "scoped history read lease backend did not extend its pin",
            data={"checkpoint_id": checkpoint_id, "pin_name": pin_name},
        )
    return state


def _validate_lease_target(
    context: Any,
    *,
    checkpoint_id: str,
    deployment_name: str,
) -> None:
    """Retain OKG's post-write validation for default and scoped backends."""
    db = context.db
    raise_if_compacted(db, checkpoint_id, deployment_name=deployment_name)
    checkpoint = db.execute(
        "SELECT 1 AS present FROM _chronos_branch_interval_checkpoints "
        "WHERE checkpoint_id = ?",
        (checkpoint_id,),
    ).fetchone()
    if checkpoint is None:
        raise SubstrateError(
            "historical generation disappeared while acquiring its read lease",
            data={"checkpoint_id": checkpoint_id},
        )
