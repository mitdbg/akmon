from __future__ import annotations

import functools
import logging
import os
import time
from collections.abc import Callable

from chronos_core.branching._common import *
from chronos_core.branching._copy_backend import _CopyBackend
from chronos_core.branching._interval_backend import _IntervalBackend
from chronos_core.branching._litetree_backend import _LiteTreeBackend
from chronos_core.branching._log_backend import _LogBackend
from chronos_core.branching._orpheus_backend import _OrpheusBackend
from chronos_core.branching.sql_adapters import connect_sql_database

_LOG = logging.getLogger(__name__)

_INTERVAL_UNIQUE_RETRY_LIMIT = 3
_INTERVAL_CONTINUATION_PERCENT_ENV = "CHRONOS_INTERVAL_CONTINUATION_PERCENT"


def _resolve_interval_continuation_percent(value: int | None) -> int:
    if value is not None:
        return _validate_interval_continuation_percent(value)
    raw = os.environ.get(_INTERVAL_CONTINUATION_PERCENT_ENV)
    if raw is None or raw == "":
        return _INTERVAL_CONTINUATION_PERCENT
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"{_INTERVAL_CONTINUATION_PERCENT_ENV} must be an integer "
            "between 1 and 99"
        ) from exc
    return _validate_interval_continuation_percent(parsed)


def _is_retryable_interval_unique_violation(exc: Exception) -> bool:
    message = str(exc)
    return (
        "UniqueViolation" in type(exc).__name__
        or "duplicate key value violates unique constraint" in message
    ) and "_chronos_b_interval_" in message


class BranchSession:
    """Checked-out branch handle used by agents and applications.

    A session owns a prepared branch reference. It is cheap to reuse for many
    statements, and its transaction context groups several SQL mutations into
    one database transaction.
    """

    def __init__(self, context: ChronosBranchContext, ref: _PreparedBranchRef):
        self._context = context
        self._ref = ref
        self._transaction_depth = 0

    @property
    def branch_id(self) -> str:
        return self._ref.branch_id

    @property
    def current_ref(self) -> str:
        """Stable backend reference captured by this checkout."""

        return self._ref.ref

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self._ensure_fresh()
        try:
            rows = self._context._backend.query(self._ref, sql, _ensure_params(params))
        except Exception as exc:
            if self._transaction_depth == 0:
                self._context._rollback_after_error(exc)
            raise
        if self._transaction_depth == 0:
            self._context._commit_autocommit()
        return rows

    def read_relations(self) -> tuple[dict[str, str], dict[str, Any]]:
        """Return ``(subquery per registered table, parameters they bind)``.

        This is the read half of `query` without the SQL re-render: a caller
        that must run exactly the text it checked splices these subqueries in
        place of its table references (okg issue #2899).
        """

        self._ensure_fresh()
        return self._context._backend.read_relations(self._ref)

    def execute(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> ExecuteResult:
        self._ensure_fresh()
        if (
            self._transaction_depth == 0
            and self._context.autocommit
            and self._context._db.in_transaction
        ):
            self._context._db.commit()
        retry_limit = _INTERVAL_UNIQUE_RETRY_LIMIT if self._transaction_depth == 0 else 1
        for attempt in range(retry_limit):
            try:
                self._context._backend.prepare_mutation(self.branch_id)
                result = self._context._backend.execute(self._ref, sql, _ensure_params(params))
                # Some backends mutate the branch head on write. Refresh the prepared
                # metadata so later reads in the same session see their own writes.
                self._ref = self._context._backend.refresh_ref_after_execute(self._ref)
                self._context._stamp_prepared_ref(self._ref)
            except Exception as exc:
                if self._transaction_depth == 0:
                    rolled_back = self._context._rollback_after_error(exc)
                    if (
                        rolled_back
                        and attempt + 1 < retry_limit
                        and _is_retryable_interval_unique_violation(exc)
                    ):
                        self._ensure_fresh()
                        continue
                raise
            if self._transaction_depth == 0:
                self._context._commit_autocommit()
            return result
        raise AssertionError("unreachable")

    @contextlib.contextmanager
    def transaction(self, *, external_transaction: bool = False) -> Iterator[None]:
        """Group several session writes in one underlying SQL transaction.

        ``external_transaction=True`` is the additive caller-owned mode.  It
        requires an already-active non-autocommit transaction and nests this
        session in a driver savepoint.  A failure rolls back only that
        savepoint; this method never commits or rolls back the caller's outer
        transaction.  The default behavior is unchanged.
        """

        root = self._transaction_depth == 0
        def record_transaction_timing(label: str, started: float) -> None:
            recorder = getattr(
                self._context._backend,
                "record_transaction_timing_ms",
                None,
            )
            if callable(recorder):
                recorder(label, (time.perf_counter() - started) * 1000)

        external_savepoint = None
        if external_transaction and not root:
            raise BranchingError(
                "external_transaction is valid only for a root session transaction"
            )
        if root:
            pop_stale_timings = getattr(
                self._context._backend,
                "pop_last_transaction_timings_ms",
                None,
            )
            if callable(pop_stale_timings):
                pop_stale_timings()
            self._ensure_fresh()
            if self._context.autocommit and self._context._db.in_transaction:
                step_started = time.perf_counter()
                self._context._db.commit()
                record_transaction_timing("pre_begin_commit", step_started)
            step_started = time.perf_counter()
            self._context._backend.prepare_transaction(self._ref)
            record_transaction_timing("prepare_transaction", step_started)
            if external_transaction:
                if self._context.autocommit:
                    raise BranchingError(
                        "external_transaction requires Chronos autocommit=False"
                    )
                if not self._context._db.in_transaction:
                    raise BranchingError(
                        "external_transaction requires an active caller transaction"
                    )
                raw_connection = self._context._db.raw_connection
                transaction = getattr(raw_connection, "transaction", None)
                if not callable(transaction):
                    raise BranchingError(
                        "external_transaction requires driver savepoint support"
                    )
                external_savepoint = transaction()
                external_savepoint.__enter__()
            elif self._context.autocommit or not self._context._db.in_transaction:
                step_started = time.perf_counter()
                self._context._db.begin()
                record_transaction_timing("begin", step_started)
        self._transaction_depth += 1
        try:
            yield
        except Exception as exc:
            self._transaction_depth -= 1
            if root:
                self._context._rollback_after_error(
                    exc,
                    functools.partial(self._rollback_root_transaction, exc, external_savepoint),
                )
            raise
        else:
            self._transaction_depth -= 1
            if root:
                try:
                    commit = getattr(self._context._backend, "commit_transaction", None)
                    if callable(commit):
                        step_started = time.perf_counter()
                        self._ref = commit(self._ref)
                        record_transaction_timing("commit_transaction", step_started)
                        step_started = time.perf_counter()
                        self._context._stamp_prepared_ref(self._ref)
                        record_transaction_timing("stamp_prepared_ref", step_started)
                    if external_savepoint is not None:
                        external_savepoint.__exit__(None, None, None)
                    elif self._context.autocommit:
                        step_started = time.perf_counter()
                        self._context._db.commit()
                        record_transaction_timing("db_commit", step_started)
                        step_started = time.perf_counter()
                        self._context._backend.after_commit()
                        record_transaction_timing("after_commit", step_started)
                except Exception as exc:
                    self._context._rollback_after_error(
                        exc,
                        functools.partial(self._rollback_root_transaction, exc, external_savepoint),
                    )
                    raise

    def _rollback_root_transaction(
        self, exc: BaseException, external_savepoint: Any = None
    ) -> None:
        """Undo a root ``transaction()`` block; only ``_rollback_after_error`` calls it.

        With an external savepoint, the savepoint is always exited (rolled
        back) after the backend rollback, as before. Otherwise a failure here
        skips the remaining steps, including ``after_rollback``, exactly as the
        unguarded code did; the guard only stops that failure from replacing
        the error that triggered the rollback.
        """
        rollback = getattr(self._context._backend, "rollback_transaction", None)
        if external_savepoint is not None:
            try:
                if callable(rollback):
                    rollback(self._ref)
            finally:
                external_savepoint.__exit__(type(exc), exc, exc.__traceback__)
            return
        if callable(rollback):
            rollback(self._ref)
        self._context._db.rollback()
        self._context._backend.after_rollback()

    def branch_info(self) -> BranchInfo:
        return self._context.get_branch(self.branch_id)

    def upsert_rows(self, table: str, rows: list[dict[str, Any]]) -> ExecuteResult:
        self._ensure_fresh()
        if self._ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
        if (
            self._transaction_depth == 0
            and self._context.autocommit
            and self._context._db.in_transaction
        ):
            self._context._db.commit()
        try:
            self._context._backend.prepare_mutation(self.branch_id)
            self._context._backend.upsert_rows(self.branch_id, table, rows)
            self._ref = self._context._backend.refresh_ref_after_execute(self._ref)
            self._context._stamp_prepared_ref(self._ref)
        except Exception as exc:
            if self._transaction_depth == 0:
                self._context._rollback_after_error(exc)
            raise
        if self._transaction_depth == 0:
            self._context._commit_autocommit()
        return ExecuteResult(len(rows))

    def delete_keys(self, table: str, keys: list[dict[str, Any]]) -> ExecuteResult:
        self._ensure_fresh()
        if self._ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
        if (
            self._transaction_depth == 0
            and self._context.autocommit
            and self._context._db.in_transaction
        ):
            self._context._db.commit()
        try:
            self._context._backend.prepare_mutation(self.branch_id)
            self._context._backend.delete_keys(self.branch_id, table, keys)
            self._ref = self._context._backend.refresh_ref_after_execute(self._ref)
            self._context._stamp_prepared_ref(self._ref)
        except Exception as exc:
            if self._transaction_depth == 0:
                self._context._rollback_after_error(exc)
            raise
        if self._transaction_depth == 0:
            self._context._commit_autocommit()
        return ExecuteResult(len(keys))

    def apply_table_edits(
        self,
        table: str,
        *,
        upserts: list[dict[str, Any]] | None = None,
        deletes: list[dict[str, Any]] | None = None,
    ) -> ExecuteResult:
        upsert_rows = list(upserts or [])
        delete_keys = list(deletes or [])
        self._ensure_fresh()
        if self._ref.readonly:
            raise BranchingError("checkpoint sessions are read-only")
        if (
            self._transaction_depth == 0
            and self._context.autocommit
            and self._context._db.in_transaction
        ):
            self._context._db.commit()
        try:
            self._context._backend.prepare_mutation(self.branch_id)
            self._context._backend.apply_table_edits(
                self.branch_id,
                table,
                upserts=upsert_rows,
                deletes=delete_keys,
            )
            self._ref = self._context._backend.refresh_ref_after_execute(self._ref)
            self._context._stamp_prepared_ref(self._ref)
        except Exception as exc:
            if self._transaction_depth == 0:
                self._context._rollback_after_error(exc)
            raise
        if self._transaction_depth == 0:
            self._context._commit_autocommit()
        return ExecuteResult(len(upsert_rows) + len(delete_keys))

    def _ensure_fresh(self) -> None:
        if self._ref.readonly:
            return
        # Branch creation/checkpointing can move interval segments. Existing
        # sessions refresh lazily when the context metadata epoch changes.
        if self._ref.metadata.get("_context_epoch") != self._context._metadata_epoch:
            current_ref = self._context.get_branch(self.branch_id).current_ref
            self._ref = self._context._prepare_ref(
                _BranchRef(self._ref.branch_id, current_ref, self._ref.readonly)
            )


class ChronosBranchContext:
    """Branch manager for SQL-backed relational data."""

    def __init__(
        self,
        db: SQLDatabaseAdapter,
        backend: _SQLBranchBackend,
        autocommit: bool = True,
    ):
        self._db = db
        self._backend = backend
        self._autocommit = autocommit
        # Incremented whenever branch/table metadata changes. Checked-out
        # sessions compare against this to invalidate prepared metadata.
        self._metadata_epoch = 0

    @classmethod
    def connect(
        cls,
        database_url: str,
        backend: BranchBackendName = "interval",
        autocommit: bool = True,
        interval_continuation_percent: int | None = None,
        interval_child_width: int | None = None,
        interval_allocation_strategy: IntervalAllocationStrategy = "adaptive",
        ensure_metadata: bool = True,
        enable_schema_branching: bool = False,
        enable_diff_merge_tracking: bool = False,
    ) -> ChronosBranchContext:
        db = connect_sql_database(database_url)
        return cls.from_database_adapter(
            db,
            backend=backend,
            autocommit=autocommit,
            interval_continuation_percent=interval_continuation_percent,
            interval_child_width=interval_child_width,
            interval_allocation_strategy=interval_allocation_strategy,
            ensure_metadata=ensure_metadata,
            enable_schema_branching=enable_schema_branching,
            enable_diff_merge_tracking=enable_diff_merge_tracking,
        )

    @classmethod
    def from_database_adapter(
        cls,
        db: SQLDatabaseAdapter,
        backend: BranchBackendName = "interval",
        autocommit: bool = True,
        interval_continuation_percent: int | None = None,
        interval_child_width: int | None = None,
        interval_allocation_strategy: IntervalAllocationStrategy = "adaptive",
        ensure_metadata: bool = True,
        enable_schema_branching: bool = False,
        enable_diff_merge_tracking: bool = False,
    ) -> ChronosBranchContext:
        resolved_interval_continuation_percent = (
            _resolve_interval_continuation_percent(interval_continuation_percent)
        )

        def build_backend() -> _SQLBranchBackend:
            if backend == "interval":
                return _IntervalBackend(
                    db,
                    continuation_percent=resolved_interval_continuation_percent,
                    child_width=interval_child_width,
                    allocation_strategy=interval_allocation_strategy,
                    enable_schema_branching=enable_schema_branching,
                )
            if enable_schema_branching and backend not in {"copy", "orpheus"}:
                raise ValueError("schema branching is currently supported only by the interval, copy, and orpheus backends")
            if backend == "log":
                return _LogBackend(db)
            if backend == "copy":
                return _CopyBackend(db, enable_schema_branching=enable_schema_branching)
            if backend == "orpheus":
                return _OrpheusBackend(
                    db,
                    enable_schema_branching=enable_schema_branching,
                    enable_diff_merge_tracking=enable_diff_merge_tracking,
                )
            if backend == "litetree":
                return _LiteTreeBackend(db)
            raise ValueError(f"unknown branch backend: {backend}")

        if ensure_metadata:
            with _chronos_metadata_lock(db):
                impl = build_backend()
                impl.ensure()
        else:
            impl = build_backend()
        return cls(db, impl, autocommit=autocommit)

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, enabled: bool) -> None:
        self._autocommit = bool(enabled)

    @property
    def backend_name(self) -> str:
        return self._backend.name

    @property
    def db(self) -> SQLDatabaseAdapter:
        return self._db

    @property
    def conn(self) -> Any:
        """Return the underlying driver connection for low-level tests/tools."""
        return self._db.raw_connection

    def close(self) -> None:
        self._db.close()

    def ensure_metadata(self) -> None:
        try:
            with _chronos_metadata_lock(self._db):
                self._backend.ensure()
                self._backend.refresh_registries()
                self._commit_autocommit()
        except Exception as exc:
            self._rollback_after_error(exc)
            raise
        self._metadata_epoch += 1

    def wait_for_background_work(self) -> None:
        if self._db.in_transaction:
            self._db.commit()
        wait = getattr(self._backend, "wait_for_async_schema_indexes", None)
        if callable(wait):
            wait()
        wait_gc = getattr(self._backend, "wait_for_interval_gc", None)
        if callable(wait_gc):
            wait_gc()

    def register_table(self, table: str, primary_key: list[str]) -> None:
        try:
            with _chronos_metadata_lock(self._db):
                self._backend.refresh_registries()
                self._backend.register_table(table, primary_key)
                self._commit_autocommit()
        except Exception as exc:
            self._rollback_after_error(exc)
            raise
        self._metadata_epoch += 1

    def create_index(
        self, table: str, columns: list[str], name: str | None = None
    ) -> IndexInfo:
        try:
            with _chronos_metadata_lock(self._db):
                self._backend.refresh_registries()
                info = self._backend.create_index(table, columns, name)
                self._commit_autocommit()
        except Exception as exc:
            self._rollback_after_error(exc)
            raise
        self._metadata_epoch += 1
        return info

    def finalize_external_transaction(self, *, committed: bool) -> None:
        """Run backend lifecycle hooks when a caller owns checkpoint commit."""
        if committed:
            self._backend.after_commit()
        else:
            self._backend.after_rollback()

    def list_indexes(self, table: str | None = None) -> list[IndexInfo]:
        return self._backend.list_indexes(table)

    def create_branch(
        self,
        branch_id: str,
        from_branch: str = "main",
        metadata: dict[str, Any] | None = None,
        *,
        terminal: bool = False,
    ) -> None:
        try:
            self._backend.create_branch(
                branch_id, from_branch, metadata, terminal=terminal
            )
        except Exception as exc:
            self._rollback_after_error(exc)
            raise
        self._commit_autocommit()
        self._metadata_epoch += 1

    def update_branch_metadata(
        self, branch_id: str, metadata: dict[str, Any]
    ) -> BranchInfo:
        try:
            info = self._backend.update_branch_metadata(branch_id, metadata)
        except Exception as exc:
            self._rollback_after_error(exc)
            raise
        self._commit_autocommit()
        self._metadata_epoch += 1
        return info

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None:
        try:
            self._backend.create_branch_from_checkpoint(branch_id, checkpoint)
        except Exception as exc:
            self._rollback_after_error(exc)
            raise
        self._commit_autocommit()
        self._metadata_epoch += 1

    def delete_branch(self, branch_id: str) -> None:
        try:
            self._backend.delete_branch(branch_id)
        except Exception as exc:
            self._rollback_after_error(exc)
            raise
        self._commit_autocommit()
        self._metadata_epoch += 1

    def delete_checkpoint(self, checkpoint: str) -> None:
        try:
            self._backend.delete_checkpoint(checkpoint)
        except Exception as exc:
            self._rollback_after_error(exc)
            raise
        self._commit_autocommit()
        self._metadata_epoch += 1

    def list_branches(self) -> list[BranchInfo]:
        return self._backend.list_branches()

    def get_branch(self, branch_id: str) -> BranchInfo:
        return self._backend.get_branch(branch_id)

    def checkout(self, branch_id: str) -> BranchSession:
        try:
            info = self._backend.get_branch(branch_id)
            prepared = self._prepare_ref(_BranchRef(branch_id, info.current_ref))
        except Exception as exc:
            self._rollback_after_error(exc)
            raise
        self._commit_autocommit()
        return BranchSession(self, prepared)

    def checkout_checkpoint(self, checkpoint: str) -> BranchSession:
        try:
            prepared = self._prepare_ref(self._backend.checkout_checkpoint(checkpoint))
        except Exception as exc:
            self._rollback_after_error(exc)
            raise
        self._commit_autocommit()
        return BranchSession(self, prepared)

    def create_checkpoint(
        self,
        checkpoint: str,
        branch: str = "main",
        metadata: dict[str, Any] | None = None,
        *,
        commit: bool | None = None,
    ) -> CheckpointInfo:
        pop_stale_timings = getattr(
            self._backend,
            "pop_last_transaction_timings_ms",
            None,
        )
        if callable(pop_stale_timings):
            pop_stale_timings()

        def record_checkpoint_timing(label: str, started: float) -> None:
            recorder = getattr(
                self._backend,
                "record_transaction_timing_ms",
                None,
            )
            if callable(recorder):
                recorder(
                    f"create_checkpoint.{label}",
                    (time.perf_counter() - started) * 1000,
                )

        try:
            step_started = time.perf_counter()
            info = self._backend.create_checkpoint(checkpoint, branch, metadata)
            record_checkpoint_timing("backend", step_started)
        except Exception as exc:
            self._rollback_after_error(exc)
            raise
        should_commit = self._autocommit if commit is None else commit
        if should_commit:
            step_started = time.perf_counter()
            self._db.commit()
            record_checkpoint_timing("commit", step_started)
            step_started = time.perf_counter()
            self._backend.after_commit()
            record_checkpoint_timing("after_commit", step_started)
        self._metadata_epoch += 1
        return info

    def fast_forward_branch(
        self,
        checkpoint: str,
        target_branch: str,
        onto_segment: int,
        expected_current_segment: int,
        metadata: dict[str, Any] | None = None,
    ) -> CheckpointInfo:
        """Advance ``target_branch`` onto a gated staging segment and write the published
        checkpoint atomically (one commit). The sound segment-lineage fast-forward
        (chronos-fast-forward-branch); interval backend only -- other backends raise."""
        try:
            info = self._backend.fast_forward_branch(
                checkpoint,
                target_branch,
                onto_segment,
                expected_current_segment,
                metadata,
            )
        except Exception as exc:
            self._rollback_after_error(exc)
            raise
        self._commit_autocommit()
        self._metadata_epoch += 1
        return info

    def get_checkpoint(self, checkpoint: str) -> CheckpointInfo:
        return self._backend.get_checkpoint(checkpoint)

    def list_checkpoints(
        self,
        branch: str | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> list[CheckpointInfo]:
        return self._backend.list_checkpoints(branch, metadata_filter)

    def _commit_autocommit(self) -> None:
        if self._autocommit:
            self._db.commit()
            self._backend.after_commit()

    def _rollback_autocommit(self) -> None:
        if self._autocommit and self._db.in_transaction:
            self._db.rollback()
            self._backend.after_rollback()

    def _rollback_after_error(
        self,
        error: BaseException,
        rollback: Callable[[], None] | None = None,
    ) -> bool:
        """Roll back after ``error`` without letting a rollback failure replace it.

        Callers run this inside an ``except`` block and then re-raise. If the
        rollback itself raised there, Python would propagate the rollback error
        and keep ``error`` only as ``__context__``, so the operator would see
        the rollback failure instead of the cause (issue #2159). Here a failed
        rollback is logged and noted on ``error``; the caller's bare ``raise``
        still propagates ``error`` with its type and message unchanged.

        ``rollback`` defaults to the autocommit rollback; ``BranchSession``
        passes its root-transaction rollback.

        A failed rollback also sets ``error.rollback_failed = True``, so a
        caller outside this module can refuse to retry on a connection whose
        rollback failed (issue #2302). okg reads it through
        ``okg.substrate.db.unit_of_work.rollback_failed``; this package cannot
        import okg, so the attribute name is the shared contract and a test on
        the okg side pins it.

        Returns True when the rollback succeeded (or had nothing to undo).
        """
        try:
            (rollback or self._rollback_autocommit)()
        except Exception as rollback_error:
            _LOG.warning(
                "chronos rollback after %s failed; re-raising the original error",
                type(error).__name__,
                exc_info=rollback_error,
            )
            error.add_note(
                "chronos: the rollback after this error also failed: "
                f"{type(rollback_error).__name__}: {rollback_error}"
            )
            error.rollback_failed = True  # type: ignore[attr-defined]
            return False
        return True

    def _prepare_ref(self, ref: _BranchRef) -> _PreparedBranchRef:
        prepared = self._backend.prepare_ref(ref)
        return self._stamp_prepared_ref(prepared)

    def _stamp_prepared_ref(self, ref: _PreparedBranchRef) -> _PreparedBranchRef:
        ref.metadata["_context_epoch"] = self._metadata_epoch
        return ref

    def diff(self, left: str, right: str) -> BranchDiff:
        changes: list[RowDiff] = []
        for table in self._backend.diff_tables():
            changes.extend(self.diff_rows(left, right, table))
        return BranchDiff(left=left, right=right, changes=changes)

    def diff_rows(self, left: str, right: str, table: str) -> list[RowDiff]:
        backend_diffs = self._backend.diff_rows(left, right, table)
        if backend_diffs is not None:
            return backend_diffs
        try:
            left_meta = self._backend.table_meta_for_branch(left, table)
        except TableNotRegisteredError:
            left_meta = None
        try:
            right_meta = self._backend.table_meta_for_branch(right, table)
        except TableNotRegisteredError:
            right_meta = None
        meta = left_meta or right_meta
        if meta is None:
            return []
        left_rows = self._rows_by_key(left, table, meta)
        right_rows = self._rows_by_key(right, table, meta)
        diffs: list[RowDiff] = []
        for key in sorted(set(left_rows) | set(right_rows), key=repr):
            before = left_rows.get(key)
            after = right_rows.get(key)
            key_dict = dict(zip(meta.pk_columns, key))
            if before is None and after is not None:
                diffs.append(RowDiff(table, key_dict, "added", None, after))
            elif before is not None and after is None:
                diffs.append(RowDiff(table, key_dict, "deleted", before, None))
            elif before != after:
                diffs.append(RowDiff(table, key_dict, "modified", before, after))
        return diffs

    def merge_preview(
        self, source: str, target: str, *, policy: MergePolicyInput = None
    ) -> MergePreview:
        backend_preview = self._backend.merge_preview(source, target)
        if backend_preview is not None:
            return _preview_with_merge_policy(
                backend_preview, policy, backend=self._backend.name
            )
        changes = self.diff(target, source).changes
        return _preview_with_merge_policy(
            MergePreview(source=source, target=target, changes=changes, conflicts=[]),
            policy,
            backend=self._backend.name,
        )

    def merge_apply(
        self,
        source: str,
        target: str,
        resolution: MergeResolution | None = None,
        *,
        policy: MergePolicyInput = None,
    ) -> MergeResult:
        normalized_policy = _normalize_merge_policy(policy)
        if self._backend.name == "orpheus" and (
            normalized_policy.mode != "abort_on_conflict"
            or (resolution is not None and resolution.conflict_choices)
        ):
            raise BranchingError(
                "custom merge policies are not yet supported by the Orpheus backend"
            )
        try:
            backend_result = self._backend.merge_apply(source, target, resolution)
        except Exception as exc:
            self._rollback_after_error(exc)
            raise
        if backend_result is not None:
            self._commit_autocommit()
            self._metadata_epoch += 1
            return backend_result
        applied = 0
        target_session = self.checkout(target)
        with target_session.transaction():
            preview = self.merge_preview(source, target)
            changes = _resolve_merge_changes(
                preview, normalized_policy, resolution, backend=self._backend.name
            )
            applied = self._apply_merge_changes(target_session, changes)
        return MergeResult(source=source, target=target, applied=applied)

    def _apply_merge_changes(
        self, target_session: BranchSession, changes: list[RowDiff]
    ) -> int:
        pending_deletes: dict[str, list[dict[str, Any]]] = {}
        pending_upserts: dict[str, list[dict[str, Any]]] = {}
        table_order: list[str] = []
        seen_tables: set[str] = set()
        for change in changes:
            if change.table not in seen_tables:
                seen_tables.add(change.table)
                table_order.append(change.table)
            if change.change == "deleted":
                pending_deletes.setdefault(change.table, []).append(change.key)
            else:
                assert change.after is not None
                pending_upserts.setdefault(change.table, []).append(change.after)

        applied = 0
        for table in table_order:
            deletes = pending_deletes.get(table, [])
            if deletes:
                target_session.delete_keys(table, deletes)
                applied += len(deletes)
            upserts = pending_upserts.get(table, [])
            if upserts:
                target_session.upsert_rows(table, upserts)
                applied += len(upserts)
        return applied

    def _rows_by_key(
        self, branch_id: str, table: str, meta: _TableMeta
    ) -> dict[tuple[Any, ...], dict[str, Any]]:
        try:
            rows = self._backend.visible_rows(branch_id, table)
        except TableNotRegisteredError:
            rows = []
        return {
            tuple(row[column] for column in meta.pk_columns): row
            for row in rows
        }
