"""A failed rollback must not replace the error that caused it (issue #2159).

Every Chronos branch-context and branch-session write path rolls back inside
its ``except`` block and then re-raises. Before the fix, if that rollback
raised -- for example psycopg refusing an explicit ``rollback()`` inside an
open ``Transaction`` block -- Python propagated the rollback error and kept the
real cause only as ``__context__``, so callers and operators saw the wrong
exception type and message.

Deterministic: a fake adapter whose ``rollback()`` raises psycopg's
``ProgrammingError`` with its real message, and a fake backend whose every
operation raises a distinct ``_BackendBoom``. No Postgres, threads, or timing.
The site table covers all 18 call sites in
``src/chronos_core/branching/_runtime.py``; separate tests cover the two
except blocks in ``BranchSession.transaction()``. An AST scan fails on any
rollback call in the module that does not go through the guarded helper.
"""
from __future__ import annotations

import ast
import inspect
import logging
from typing import Any, Callable

import psycopg
import pytest

from chronos_core.branching import _runtime as runtime
from chronos_core.branching._common import _PreparedBranchRef
from chronos_core.branching._runtime import BranchSession, ChronosBranchContext

_ROLLBACK_MESSAGE = (
    "Explicit rollback() forbidden within a Transaction context. "
    "(Transaction will be automatically rolled back if an exception is raised.)"
)
_RETRYABLE_MESSAGE = (
    'duplicate key value violates unique constraint "_chronos_b_interval_t_pk"'
)


class _BackendBoom(Exception):
    """The original failure every site must surface unchanged."""


class _IntervalUniqueViolation(Exception):
    """Name and message match the retryable interval unique-violation check."""


class _FakeDB:
    """Adapter stand-in that is always inside a transaction and whose
    ``rollback()`` fails the way psycopg does inside a ``Transaction`` block."""

    dialect = "sqlite"  # keeps `_chronos_metadata_lock` from issuing SQL

    def __init__(self, *, rollback_fails: bool) -> None:
        self.rollback_fails = rollback_fails
        self.rollbacks = 0

    @property
    def in_transaction(self) -> bool:
        return True

    def begin(self) -> None:
        pass

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        self.rollbacks += 1
        if self.rollback_fails:
            raise psycopg.ProgrammingError(_ROLLBACK_MESSAGE)


def _boom(name: str) -> Callable[..., Any]:
    def fail(*_args: Any, **_kwargs: Any) -> Any:
        raise _BackendBoom(f"backend {name} failed")

    return fail


class _FakeBackend:
    """Backend whose operations raise; lifecycle hooks succeed."""

    name = "fake"

    def __init__(self) -> None:
        self.after_rollback_calls = 0
        self.rollback_transaction_calls = 0
        self.prepare_mutation_calls = 0
        self.prepare_mutation_error: Exception = _BackendBoom(
            "backend prepare_mutation failed"
        )
        for op in (
            "ensure",
            "refresh_registries",
            "register_table",
            "create_index",
            "create_branch",
            "update_branch_metadata",
            "create_branch_from_checkpoint",
            "delete_branch",
            "delete_checkpoint",
            "get_branch",
            "checkout_checkpoint",
            "create_checkpoint",
            "fast_forward_branch",
            "merge_apply",
            "query",
        ):
            setattr(self, op, _boom(op))

    def prepare_mutation(self, _branch_id: str) -> None:
        self.prepare_mutation_calls += 1
        raise self.prepare_mutation_error

    # BranchSession.transaction() lifecycle; commit_transaction is set per test.
    commit_transaction: Callable[..., Any] | None = None

    def prepare_transaction(self, _ref: Any) -> None:
        pass

    def rollback_transaction(self, _ref: Any) -> None:
        self.rollback_transaction_calls += 1

    def after_commit(self) -> None:
        pass

    def after_rollback(self) -> None:
        self.after_rollback_calls += 1


def _context(*, rollback_fails: bool = True) -> ChronosBranchContext:
    return ChronosBranchContext(
        _FakeDB(rollback_fails=rollback_fails), _FakeBackend(), autocommit=True
    )


def _session(ctx: ChronosBranchContext) -> BranchSession:
    ref = _PreparedBranchRef("main", "ref-1", metadata={"_context_epoch": 0})
    return BranchSession(ctx, ref)


# (site, call, expected original-error message): one row per call site.
_SITES: list[tuple[str, Callable[[ChronosBranchContext], Any], str]] = [
    ("ensure_metadata", lambda c: c.ensure_metadata(), "backend ensure failed"),
    (
        "register_table",
        lambda c: c.register_table("t", ["id"]),
        "backend refresh_registries failed",
    ),
    (
        "create_index",
        lambda c: c.create_index("t", ["id"]),
        "backend refresh_registries failed",
    ),
    ("create_branch", lambda c: c.create_branch("b"), "backend create_branch failed"),
    (
        "update_branch_metadata",
        lambda c: c.update_branch_metadata("b", {}),
        "backend update_branch_metadata failed",
    ),
    (
        "create_branch_from_checkpoint",
        lambda c: c.create_branch_from_checkpoint("b", "cp"),
        "backend create_branch_from_checkpoint failed",
    ),
    ("delete_branch", lambda c: c.delete_branch("b"), "backend delete_branch failed"),
    (
        "delete_checkpoint",
        lambda c: c.delete_checkpoint("cp"),
        "backend delete_checkpoint failed",
    ),
    ("checkout", lambda c: c.checkout("b"), "backend get_branch failed"),
    (
        "checkout_checkpoint",
        lambda c: c.checkout_checkpoint("cp"),
        "backend checkout_checkpoint failed",
    ),
    (
        "create_checkpoint",
        lambda c: c.create_checkpoint("cp"),
        "backend create_checkpoint failed",
    ),
    (
        "fast_forward_branch",
        lambda c: c.fast_forward_branch("cp", "main", 2, 1),
        "backend fast_forward_branch failed",
    ),
    ("merge_apply", lambda c: c.merge_apply("b", "main"), "backend merge_apply failed"),
    ("session.query", lambda c: _session(c).query("SELECT 1"), "backend query failed"),
    (
        "session.execute",
        lambda c: _session(c).execute("UPDATE t SET x = 1"),
        "backend prepare_mutation failed",
    ),
    (
        "session.upsert_rows",
        lambda c: _session(c).upsert_rows("t", [{"id": 1}]),
        "backend prepare_mutation failed",
    ),
    (
        "session.delete_keys",
        lambda c: _session(c).delete_keys("t", [{"id": 1}]),
        "backend prepare_mutation failed",
    ),
    (
        "session.apply_table_edits",
        lambda c: _session(c).apply_table_edits("t", upserts=[{"id": 1}]),
        "backend prepare_mutation failed",
    ),
]


# The only functions allowed to call a rollback directly. Each runs solely
# through `_rollback_after_error`, which keeps the original error.
_ROLLBACK_HELPERS = frozenset(
    {"_rollback_after_error", "_rollback_autocommit", "_rollback_root_transaction"}
)
# `finalize_external_transaction` runs the lifecycle hook for a caller that
# owns the commit; it is not inside an except block and has no error to mask.
_ROLLBACK_HOOK_ONLY = frozenset({"finalize_external_transaction"})
# BranchSession.transaction() has two guarded blocks (body error, commit error).
_TRANSACTION_SITES = 2


def _called_name(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return None


def _rollback_calls() -> list[tuple[str, str, int]]:
    """Every call whose callee name mentions rollback, with its enclosing function."""
    tree = ast.parse(inspect.getsource(runtime))
    found: list[tuple[str, str, int]] = []

    def visit(node: ast.AST, function: str) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            function = node.name
        if isinstance(node, ast.Call):
            name = _called_name(node)
            if name is not None and "rollback" in name.lower():
                found.append((function, name, node.lineno))
        for child in ast.iter_child_nodes(node):
            visit(child, function)

    visit(tree, "<module>")
    return found


def test_site_table_covers_every_rollback_call_site() -> None:
    calls = _rollback_calls()
    # Every guarded call site is in the site table or is a transaction() block.
    guarded = [c for c in calls if c[1] == "_rollback_after_error"]
    assert len(guarded) == len(_SITES) + _TRANSACTION_SITES, guarded
    # Any other rollback call -- `.rollback()`, `_rollback_autocommit()`,
    # `self._context._rollback_autocommit()`, `rollback_transaction`, a local
    # `rollback(...)` -- must live inside a guarded helper.
    unguarded = [
        c
        for c in calls
        if c[1] != "_rollback_after_error"
        and c[0] not in _ROLLBACK_HELPERS
        and not (c[0] in _ROLLBACK_HOOK_ONLY and c[1] == "after_rollback")
    ]
    assert not unguarded, f"unguarded rollback calls (function, callee, line): {unguarded}"
    # The helpers themselves are reached only through `_rollback_after_error`:
    # no code outside it calls them, and nothing but it references the
    # autocommit one (the root-transaction one is passed to it by name).
    tree = ast.parse(inspect.getsource(runtime))
    references: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in (
            "_rollback_autocommit",
            "_rollback_root_transaction",
        ):
            references.append((node.attr, node.lineno))
    helper_calls = [c for c in calls if c[1] in _ROLLBACK_HELPERS - {"_rollback_after_error"}]
    assert all(c[0] == "_rollback_after_error" for c in helper_calls), helper_calls
    assert sorted(name for name, _ in references) == [
        "_rollback_autocommit",
        *["_rollback_root_transaction"] * _TRANSACTION_SITES,
    ], references


@pytest.mark.parametrize(
    ("site", "call", "message"), _SITES, ids=[row[0] for row in _SITES]
)
def test_failed_rollback_keeps_original_error(
    site: str,
    call: Callable[[ChronosBranchContext], Any],
    message: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    ctx = _context()
    with caplog.at_level(logging.WARNING, logger=runtime.__name__):
        with pytest.raises(Exception) as raised:
            call(ctx)

    # The caller sees the ORIGINAL error: its exact type and message.
    assert type(raised.value) is _BackendBoom, (
        f"{site} surfaced {type(raised.value).__name__}: {raised.value}"
    )
    assert str(raised.value) == message
    # The rollback was attempted, and its failure is still reported.
    assert ctx._db.rollbacks == 1
    assert any(
        "rollback after this error also failed: ProgrammingError" in note
        for note in getattr(raised.value, "__notes__", [])
    )
    assert any(
        "chronos rollback after _BackendBoom failed" in record.getMessage()
        and record.exc_info is not None
        and isinstance(record.exc_info[1], psycopg.ProgrammingError)
        for record in caplog.records
    )


def test_successful_rollback_still_runs_and_raises_original() -> None:
    ctx = _context(rollback_fails=False)
    with pytest.raises(_BackendBoom, match="backend ensure failed") as raised:
        ctx.ensure_metadata()
    assert ctx._db.rollbacks == 1
    assert ctx._backend.after_rollback_calls == 1
    assert not getattr(raised.value, "__notes__", [])


def test_execute_does_not_retry_after_a_failed_rollback() -> None:
    ctx = _context()
    ctx._backend.prepare_mutation_error = _IntervalUniqueViolation(_RETRYABLE_MESSAGE)
    with pytest.raises(_IntervalUniqueViolation):
        _session(ctx).execute("INSERT INTO t VALUES (1)")
    # Once the rollback failed the connection state is unknown: no retry.
    assert ctx._backend.prepare_mutation_calls == 1


def test_execute_still_retries_after_a_successful_rollback() -> None:
    ctx = _context(rollback_fails=False)
    ctx._backend.prepare_mutation_error = _IntervalUniqueViolation(_RETRYABLE_MESSAGE)
    with pytest.raises(_IntervalUniqueViolation):
        _session(ctx).execute("INSERT INTO t VALUES (1)")
    assert ctx._backend.prepare_mutation_calls == 3


def _fail_inside_transaction(session: BranchSession, message: str) -> None:
    """Raise a backend failure from inside the session's transaction body."""
    with session.transaction():
        raise _BackendBoom(message)


def _assert_rollback_failure_reported(
    error: BaseException, caplog: pytest.LogCaptureFixture
) -> None:
    assert any(
        "rollback after this error also failed: ProgrammingError" in note
        for note in getattr(error, "__notes__", [])
    )
    assert any(
        f"chronos rollback after {type(error).__name__} failed" in record.getMessage()
        and record.exc_info is not None
        and isinstance(record.exc_info[1], psycopg.ProgrammingError)
        for record in caplog.records
    )


def test_transaction_body_error_survives_a_failed_rollback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The root ``transaction()`` except block (source-retirement shape)."""
    ctx = _context()
    session = _session(ctx)
    with caplog.at_level(logging.WARNING, logger=runtime.__name__):
        with pytest.raises(Exception) as raised:
            _fail_inside_transaction(
                session, "write inside the session transaction failed"
            )

    assert type(raised.value) is _BackendBoom, (
        f"surfaced {type(raised.value).__name__}: {raised.value}"
    )
    assert str(raised.value) == "write inside the session transaction failed"
    assert ctx._backend.rollback_transaction_calls == 1
    assert ctx._db.rollbacks == 1
    # A failed db rollback skips after_rollback, as it did before the guard.
    assert ctx._backend.after_rollback_calls == 0
    _assert_rollback_failure_reported(raised.value, caplog)
    # The session is usable again: the depth counter was restored.
    assert session._transaction_depth == 0


def test_transaction_commit_error_survives_a_failed_rollback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The commit-path except block keeps the commit failure, not the rollback's."""
    ctx = _context()
    ctx._backend.commit_transaction = _boom("commit_transaction")
    with caplog.at_level(logging.WARNING, logger=runtime.__name__):
        with pytest.raises(Exception) as raised:
            with _session(ctx).transaction():
                pass

    assert type(raised.value) is _BackendBoom, (
        f"surfaced {type(raised.value).__name__}: {raised.value}"
    )
    assert str(raised.value) == "backend commit_transaction failed"
    assert ctx._backend.rollback_transaction_calls == 1
    assert ctx._db.rollbacks == 1
    _assert_rollback_failure_reported(raised.value, caplog)
