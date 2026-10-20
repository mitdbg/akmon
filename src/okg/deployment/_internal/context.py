"""Per-workflow / per-transaction context for SDK helpers.

The SDK's mutation helpers (Phase 2 §3) and read helpers (§4) need
three pieces of context that DBOS does not provide directly:

  * **Deployment name.** The substrate role split is per-deployment;
    the worker-write generation allocator is per-deployment.
  * **Connection.** Inside a ``@deployment_transaction``, DBOS exposes
    a SQLAlchemy ``sql_session``. The SDK's helpers operate at the
    raw-psycopg level (matching the substrate's existing data-access
    patterns), so we adapt by retrieving the underlying ``psycopg``
    connection from the active SQLAlchemy session.
  * **Worker-write generation.** Multiple mutation calls inside one
    ``@deployment_transaction`` must land at the **same** worker-write
    generation_id (per Phase 0 C1, dual-mutation correctness). The
    first call allocates; subsequent calls cache.

The three are managed through Python ``contextvars`` so concurrent
workflows in the same process see independent context. Callers outside
a DBOS workflow bind the context manually via :func:`bind_sdk_context`
(production) or :func:`bind_test_context` (tests; pins a stable
workflow id).

## DBOS integration boundary

The default lookup path reads:

  * Deployment from ``okg.deployment._internal.context._DEPLOYMENT``
    contextvar. ``@deployment_workflow`` sets this on entry.
  * Workflow id from ``DBOS.workflow_id``.
  * Step id from ``DBOS.step_id`` (when inside a step).
  * Connection by extracting the raw ``psycopg.Connection`` from
    ``DBOS.sql_session.connection().connection.connection`` (the
    SQLAlchemy → psycopg adapter unwrap).

Failures at any layer raise :class:`MissingContextError` with a
specific actionable message so authors know where to add a decorator.
"""
from __future__ import annotations

import contextvars
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

import psycopg


__all__ = [
    "MissingContextError",
    "WorkflowContext",
    "bind_sdk_context",
    "bind_test_context",
    "current_context",
    "current_worker_write_generation",
    "set_worker_write_generation",
]


class MissingContextError(RuntimeError):
    """Raised when an SDK helper is called outside the context it needs.

    Examples that surface this error:

      * ``emit_node_fact()`` called outside a ``@deployment_workflow``
        (no deployment name in scope).
      * ``worker_write_generation()`` called outside a
        ``@deployment_transaction`` (no active connection).
      * Read-helper called without a pin and outside a workflow.
    """


@dataclass(frozen=True)
class WorkflowContext:
    """Snapshot of the SDK context for one workflow / transaction invocation."""

    deployment: str
    workflow_id: str
    step_id: str | None
    conn: psycopg.Connection


# ---------------------------------------------------------------------------
# Contextvars
# ---------------------------------------------------------------------------


_DEPLOYMENT: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "okg_deployment_name", default=None,
)
_CONN_OVERRIDE: contextvars.ContextVar[psycopg.Connection | None] = (
    contextvars.ContextVar("okg_conn_override", default=None)
)
_WORKER_WRITE_GEN: contextvars.ContextVar[int | None] = (
    contextvars.ContextVar("okg_worker_write_gen", default=None)
)
# Test override for workflow_id: when set, current_context returns
# this value instead of probing DBOS. Real production paths never
# touch it.
_WORKFLOW_ID_OVERRIDE: contextvars.ContextVar[str | None] = (
    contextvars.ContextVar("okg_workflow_id_override", default=None)
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def current_context() -> WorkflowContext:
    """Resolve the SDK context.

    Lookup order:

      1. Deployment: ``_DEPLOYMENT`` contextvar set by
         ``@deployment_workflow``. Falls back to the env var
         ``OKG_DEPLOYMENT`` for ergonomics (e.g. running deployment
         code outside a workflow for ad-hoc scripts).
      2. Workflow id: ``DBOS.workflow_id`` if available; otherwise
         ``"_local_$pid"`` so out-of-DBOS test code still has a
         stable identifier.
      3. Step id: ``DBOS.step_id`` if available; otherwise None.
      4. Connection: ``_CONN_OVERRIDE`` contextvar (set by
         :func:`bind_sdk_context` / :func:`bind_test_context`).
         Otherwise unwrapped from ``DBOS.sql_session``.

    Raises
    ------
    MissingContextError
        When neither the deployment nor the connection can be
        resolved. The exception message names which piece is missing
        so the author can decorate the calling code correctly.
    """
    deployment = _DEPLOYMENT.get()
    if deployment is None:
        import os

        deployment = os.environ.get("OKG_DEPLOYMENT")
    if not deployment:
        raise MissingContextError(
            "no deployment context: SDK mutation/read helpers must be "
            "called inside a @deployment_workflow / @deployment_step / "
            "@deployment_transaction, or with OKG_DEPLOYMENT set in the "
            "environment for ad-hoc scripts"
        )

    workflow_id: str = _WORKFLOW_ID_OVERRIDE.get() or ""
    step_id: str | None = None
    if not workflow_id:
        try:
            from dbos import DBOS

            workflow_id = getattr(DBOS, "workflow_id", None) or ""
            step_id = getattr(DBOS, "step_id", None)
        except Exception:  # noqa: BLE001 — DBOS may be optional
            workflow_id = ""

    if not workflow_id:
        import os
        workflow_id = f"_local_{os.getpid()}"

    conn = _CONN_OVERRIDE.get()
    if conn is None:
        conn = _resolve_dbos_connection()

    if conn is None:
        raise MissingContextError(
            "no connection in scope: SDK mutation helpers must be "
            "called inside a @deployment_transaction, OR the caller "
            "must bind a connection via bind_sdk_context() "
            "(bind_test_context() in tests)"
        )

    return WorkflowContext(
        deployment=deployment,
        workflow_id=workflow_id,
        step_id=step_id,
        conn=conn,
    )


def current_worker_write_generation(
    *,
    allocate_if_missing: bool = True,
) -> int:
    """Return the worker-write generation id for the active transaction.

    On first call within a transaction, allocates a new generation via
    :func:`okg.substrate.runtime.generation.allocate_worker_write_generation`
    and caches it in :data:`_WORKER_WRITE_GEN`. Subsequent calls return
    the cached id so dual-mutation lands at the same generation.

    Parameters
    ----------
    allocate_if_missing : bool, default True
        Whether to allocate when the contextvar is unset. Set False to
        do a read-only probe (e.g. for diagnostics).

    Raises
    ------
    MissingContextError
        If ``allocate_if_missing=False`` and no generation is cached.
    """
    cached = _WORKER_WRITE_GEN.get()
    if cached is not None:
        return cached
    if not allocate_if_missing:
        raise MissingContextError(
            "no worker-write generation is active; the SDK helper that "
            "needs one must be called within a @deployment_transaction "
            "whose body has already called a mutation"
        )

    from okg.substrate.runtime.generation import (
        allocate_worker_write_generation,
    )

    ctx = current_context()
    gen = allocate_worker_write_generation(
        ctx.conn, deployment=ctx.deployment,
    )
    _WORKER_WRITE_GEN.set(gen.generation_id)
    return gen.generation_id


def set_worker_write_generation(generation_id: int | None) -> None:
    """Override or clear the cached worker-write generation.

    Useful for tests that want to pin a known generation, and for the
    ``@deployment_transaction`` enter/exit hooks once Phase 2 §2.2.3
    wires them in.
    """
    _WORKER_WRITE_GEN.set(generation_id)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_dbos_connection() -> psycopg.Connection | None:
    """Pull a raw psycopg connection out of DBOS's SQLAlchemy session.

    DBOS exposes ``DBOS.sql_session`` inside ``@DBOS.transaction``-
    decorated code (per Decision 9). The session wraps a SQLAlchemy
    engine which wraps a DB-API connection. We unwrap one level past
    the SQLAlchemy layer to access the raw ``psycopg.Connection``.

    Returns None if no DBOS context is active.
    """
    try:
        from dbos import DBOS
    except ImportError:
        return None

    try:
        session = getattr(DBOS, "sql_session", None)
        if session is None:
            return None
        # SQLAlchemy session → connection → raw DB-API conn.
        sa_conn = session.connection()
        raw = sa_conn.connection.dbapi_connection  # psycopg3
        # SQLAlchemy-2 returns the DB-API connection here; psycopg's
        # ``Connection`` is the concrete type for our DSNs.
        return raw  # type: ignore[return-value]
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# Context binders
# ---------------------------------------------------------------------------


@contextmanager
def bind_sdk_context(
    *,
    deployment: str,
    conn: psycopg.Connection,
    workflow_id: str | None = None,
) -> Iterator[None]:
    """Bind SDK context (deployment + connection) outside a DBOS workflow.

    This is the one seam for supplying :func:`current_context`'s inputs
    when the caller is not running under a ``@deployment_workflow`` /
    DBOS singleton — e.g. the substrate's own spend settlement inside a
    CLI publish cycle.

    ``workflow_id=None`` (the default) leaves workflow-id resolution to
    :func:`current_context`: ``DBOS.workflow_id`` when a workflow is
    active, else the ``_local_$pid`` fallback. The override contextvar is
    explicitly CLEARED in that case — leaving it untouched would let a
    stale id pinned by an enclosing bind leak into this scope's
    provenance. Pass an explicit id only when the caller genuinely owns
    one; a made-up id ends up verbatim in provenance columns such as
    ``okg.deployment_cost_ledger.workflow_id``.

    On exit, clears all bound contextvars so the next caller starts
    fresh.
    """
    dep_tok = _DEPLOYMENT.set(deployment)
    conn_tok = _CONN_OVERRIDE.set(conn)
    gen_tok = _WORKER_WRITE_GEN.set(None)
    wf_tok = _WORKFLOW_ID_OVERRIDE.set(workflow_id)

    try:
        yield
    finally:
        _DEPLOYMENT.reset(dep_tok)
        _CONN_OVERRIDE.reset(conn_tok)
        _WORKER_WRITE_GEN.reset(gen_tok)
        _WORKFLOW_ID_OVERRIDE.reset(wf_tok)


@contextmanager
def bind_test_context(
    *,
    deployment: str,
    conn: psycopg.Connection,
    workflow_id: str = "test-wf",
) -> Iterator[None]:
    """Bind SDK context for tests that don't run a DBOS singleton.

    Test-flavored wrapper over :func:`bind_sdk_context` that pins a
    stable ``workflow_id`` (``"test-wf"``) so test assertions on
    provenance columns are deterministic. Production code must use
    :func:`bind_sdk_context` instead — this wrapper's pinned id would
    otherwise leak into ledger rows.

    Use::

        with bind_test_context(deployment="test", conn=conn):
            emit_node_fact("n", "Subtype", {"k": "v"}, dedupe_key="d")
    """
    with bind_sdk_context(
        deployment=deployment, conn=conn, workflow_id=workflow_id,
    ):
        yield


def _set_deployment_for_workflow(deployment: str) -> Any:
    """Set the deployment contextvar; return the reset token.

    Used by ``@deployment_workflow``'s wrapper.
    """
    return _DEPLOYMENT.set(deployment)


def _reset_deployment(token: Any) -> None:
    _DEPLOYMENT.reset(token)
