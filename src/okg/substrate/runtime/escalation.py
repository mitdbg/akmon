"""Substrate-side escalation lifecycle (Phase 2 §1.4.1).

The `request_escalation` SDK primitive (Phase 2 §3.2) is built on a
two-step pattern per Phase 0 C5:

  1. Inside a ``@DBOS.transaction``: ``_create_escalation`` writes
     the escalation row and a wake-key the operator's response uses.
  2. As a ``@DBOS.workflow_step(retries_allowed=False)``: ``_await_escalation``
     blocks on ``DBOS.recv(topic=f"escalation:{escalation_id}")``,
     idempotent on retry because DBOS step caching returns the same
     value the first call returned.

The SDK helpers in the deployment package implement (1) and (2); they
delegate the substrate-side state mutations to the four functions
exported by this module:

  * :func:`create_escalation` — idempotent insert keyed on
    ``(workflow_id, step_id)`` so the SDK's `_create_escalation` step
    can be retried without duplicating the escalation node.
  * :func:`transition_to_decided` — closes the live row, writes a
    new live row with ``status='decided'`` and the operator's
    decision payload. Called by the CLI/FastAPI response surface
    after validating the JSON against ``decision_schema_ref``.
  * :func:`transition_to_applied` — closes the ``decided`` row,
    writes ``status='applied'``. Called by the SDK helper once the
    deployment workflow has consumed the decision via ``DBOS.recv``.
  * :func:`transition_to_timed_out` — closes the ``pending`` row,
    writes ``status='timed_out'``. Called by the SDK when the
    deployment workflow's ``request_escalation`` timeout expires.

## State machine

::

    pending  ──operator responds──>  decided  ──workflow consumes──>  applied
       │
       ┕──timeout expires──>  timed_out

Each transition writes a new bitemporal row to ``okg.escalation_live``
at a worker-write generation, and a fact row to ``okg.escalation_facts``.
The previous live row's ``valid_to_gen`` is set to the new generation
so a pinned reader at any past generation sees the correct historical
state.

## Worker-write generation

Each transition allocates its own worker-write generation by calling
:func:`okg.substrate.runtime.generation.allocate_worker_write_generation`.
This means a single escalation's lifecycle (pending → decided →
applied) typically lands in two or three distinct worker-write
generations — not one — since the operator's response and the
workflow's consume happen in separate transactions and would otherwise
need cross-transaction coordination.

A reader who wants the *current* state of an escalation reads the row
with ``valid_to_gen IS NULL``.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg

from .generation import allocate_worker_write_generation


__all__ = [
    "ESCALATION_STATUSES",
    "Escalation",
    "EscalationNotFoundError",
    "EscalationStateError",
    "create_escalation",
    "get_escalation",
    "sweep_timed_out_escalations",
    "transition_to_applied",
    "transition_to_decided",
    "transition_to_timed_out",
]


log = logging.getLogger(__name__)


# Status lifecycle. SDK helpers use these literals; CLI/FastAPI use
# them; the audit can iterate them. Order is suggestive only — the
# allowed transitions are encoded in the transition functions, not in
# this tuple.
ESCALATION_STATUSES = (
    "pending",
    "routed",
    "decided",
    "applied",
    "timed_out",
)


# Source identifier stamped into escalation_facts. Per the SDK boundary
# spec, deployment-write provenance carries deployment + workflow info;
# the substrate's own writes carry "_substrate:runtime:escalation".
_SOURCE_PREFIX = "_substrate:runtime:escalation"


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Escalation:
    """Current view of an escalation (the row with ``valid_to_gen IS NULL``).

    Mirrors the column shape of ``okg.escalation_live`` plus the flat
    attrs fields the lifecycle is keyed on. Construct via
    :func:`get_escalation` or as a return value from a transition.
    """

    escalation_id: str
    subject_node_id: str
    status: str
    requested_by_workflow: str
    reason: str
    view_ref: str | None
    decision_schema_ref: str
    timeout_at: datetime | None
    decided_by: str | None
    decided_at: datetime | None
    decision: dict[str, Any] | None
    valid_from_gen: int


class EscalationNotFoundError(KeyError):
    """Raised when an escalation_id has no current row in escalation_live."""


class EscalationStateError(ValueError):
    """Raised when a transition is illegal from the current status
    (e.g. trying to mark an already-decided escalation as decided)."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compute_escalation_id(*, workflow_id: str, step_id: str) -> str:
    """Stable escalation_id from (workflow_id, step_id).

    This is the idempotency key used by :func:`create_escalation`: the
    same workflow step retried twice produces the same escalation_id,
    so the INSERT collides on the unique dedupe_key constraint and the
    cached step output is returned.
    """
    raw = f"{workflow_id}|{step_id}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]
    return f"esc-{digest}"


def _compute_dedupe_key(*, escalation_id: str, status: str) -> str:
    """Per-(escalation_id, status) dedupe key for escalation_facts.

    Same hash convention as node_facts/edge_facts: this dedupe key
    collides when the same lifecycle transition is re-attempted within
    the same observed_at bucket, which is the documented behavior of
    bitemporal facts. Different observed_at values land separate rows
    even with the same key.
    """
    raw = f"escalation|{escalation_id}|{status}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _close_current_live_row(
    cur: psycopg.Cursor,
    *,
    escalation_id: str,
    new_generation_id: int,
) -> int | None:
    """Set ``valid_to_gen = new_generation_id`` on the current live row.

    Returns the closed row's ``valid_from_gen`` so the caller can
    record provenance. Returns None when no live row exists (caller
    must decide if that's legal — :func:`create_escalation` permits
    it; transitions don't).
    """
    cur.execute(
        """
        UPDATE okg.escalation_live
           SET valid_to_gen = %s
         WHERE escalation_id = %s
           AND valid_to_gen IS NULL
        RETURNING valid_from_gen
        """,
        (new_generation_id, escalation_id),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _insert_fact(
    cur: psycopg.Cursor,
    *,
    escalation_id: str,
    subject_node_id: str,
    attrs: dict[str, Any],
    status: str,
    workflow_id: str,
    run_id: str,
    observed_at: datetime,
) -> int:
    """Insert a row into ``okg.escalation_facts``. Returns ``fact_id``."""
    cur.execute(
        """
        INSERT INTO okg.escalation_facts
          (escalation_id, subject_node_id, attrs, op, source,
           observed_at, dedupe_key, run_id)
        VALUES (%s, %s, %s::jsonb, 'I', %s, %s, %s, %s)
        RETURNING fact_id
        """,
        (
            escalation_id,
            subject_node_id,
            json.dumps(attrs),
            f"{_SOURCE_PREFIX}:{workflow_id}",
            observed_at,
            _compute_dedupe_key(
                escalation_id=escalation_id, status=status,
            ),
            run_id,
        ),
    )
    return cur.fetchone()[0]


def _insert_live(
    cur: psycopg.Cursor,
    *,
    escalation_id: str,
    subject_node_id: str,
    attrs: dict[str, Any],
    valid_from_gen: int,
    last_fact_id: int,
) -> None:
    """Insert a new ``okg.escalation_live`` row (no valid_to_gen)."""
    cur.execute(
        """
        INSERT INTO okg.escalation_live
          (escalation_id, subject_node_id, attrs,
           valid_from_gen, valid_to_gen, last_fact_id)
        VALUES (%s, %s, %s::jsonb, %s, NULL, %s)
        """,
        (
            escalation_id,
            subject_node_id,
            json.dumps(attrs),
            valid_from_gen,
            last_fact_id,
        ),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_escalation(
    conn: psycopg.Connection,
    *,
    deployment: str,
    workflow_id: str,
    step_id: str,
    subject_node_id: str,
    reason: str,
    view_ref: str | None,
    decision_schema_ref: str,
    timeout_at: datetime | None = None,
) -> Escalation:
    """Create a new escalation in ``status='pending'`` (idempotent).

    The escalation_id is derived from ``(workflow_id, step_id)`` so the
    SDK's ``_create_escalation`` step is idempotent under DBOS retry:
    the same step computes the same id, the INSERT collides on the
    dedupe_key UNIQUE constraint, and the cached step output is
    returned by DBOS.

    On retry detected (UNIQUE violation on dedupe_key), this function
    returns the existing escalation row without raising.

    Returns
    -------
    Escalation
        The new (or already-existing) escalation, in ``pending`` status.
    """
    escalation_id = _compute_escalation_id(
        workflow_id=workflow_id, step_id=step_id,
    )

    # First, check if it already exists (retry path). Cheap fast path.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM okg.escalation_live "
            "WHERE escalation_id = %s AND valid_to_gen IS NULL",
            (escalation_id,),
        )
        if cur.fetchone() is not None:
            existing = get_escalation(conn, escalation_id=escalation_id)
            log.info(
                "create_escalation: returning existing escalation %s "
                "(idempotent retry from workflow=%s step=%s)",
                escalation_id, workflow_id, step_id,
            )
            return existing

    # New escalation: allocate a worker-write generation, insert fact +
    # live row in one transaction.
    gen = allocate_worker_write_generation(conn, deployment=deployment)
    now = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())

    attrs = {
        "status": "pending",
        "requested_by_workflow": workflow_id,
        "reason": reason,
        "view_ref": view_ref,
        "decision_schema_ref": decision_schema_ref,
        "timeout_at": timeout_at.isoformat() if timeout_at else None,
        "decided_by": None,
        "decided_at": None,
        "decision": None,
    }

    try:
        with conn.transaction():
            with conn.cursor() as cur:
                fact_id = _insert_fact(
                    cur,
                    escalation_id=escalation_id,
                    subject_node_id=subject_node_id,
                    attrs=attrs,
                    status="pending",
                    workflow_id=workflow_id,
                    run_id=run_id,
                    observed_at=now,
                )
                _insert_live(
                    cur,
                    escalation_id=escalation_id,
                    subject_node_id=subject_node_id,
                    attrs=attrs,
                    valid_from_gen=gen.generation_id,
                    last_fact_id=fact_id,
                )
    except psycopg.errors.UniqueViolation:
        # Lost the race to a concurrent retry; return the existing row.
        return get_escalation(conn, escalation_id=escalation_id)

    log.info(
        "create_escalation: escalation_id=%s workflow=%s step=%s "
        "generation=%s",
        escalation_id, workflow_id, step_id, gen.generation_id,
    )
    return get_escalation(conn, escalation_id=escalation_id)


def get_escalation(
    conn: psycopg.Connection,
    *,
    escalation_id: str,
) -> Escalation:
    """Read the current (``valid_to_gen IS NULL``) row for an escalation.

    Raises
    ------
    EscalationNotFoundError
        If no escalation with the given id exists or its lifecycle
        has been retired (no current row).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT escalation_id, subject_node_id, attrs, valid_from_gen
              FROM okg.escalation_live
             WHERE escalation_id = %s
               AND valid_to_gen IS NULL
            """,
            (escalation_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise EscalationNotFoundError(escalation_id)
    esc_id, subject, attrs, valid_from_gen = row
    return Escalation(
        escalation_id=esc_id,
        subject_node_id=subject,
        status=attrs["status"],
        requested_by_workflow=attrs["requested_by_workflow"],
        reason=attrs["reason"],
        view_ref=attrs.get("view_ref"),
        decision_schema_ref=attrs["decision_schema_ref"],
        timeout_at=(
            datetime.fromisoformat(attrs["timeout_at"])
            if attrs.get("timeout_at") else None
        ),
        decided_by=attrs.get("decided_by"),
        decided_at=(
            datetime.fromisoformat(attrs["decided_at"])
            if attrs.get("decided_at") else None
        ),
        decision=attrs.get("decision"),
        valid_from_gen=valid_from_gen,
    )


def _transition(
    conn: psycopg.Connection,
    *,
    deployment: str,
    escalation_id: str,
    new_status: str,
    expected_from_statuses: tuple[str, ...],
    attrs_updates: dict[str, Any],
) -> Escalation:
    """Generic state-transition helper.

    Validates the from-status, allocates a worker-write generation,
    closes the current live row, writes new fact + live rows. All
    inside one transaction.
    """
    current = get_escalation(conn, escalation_id=escalation_id)
    if current.status not in expected_from_statuses:
        raise EscalationStateError(
            f"cannot transition escalation {escalation_id} from "
            f"{current.status!r} to {new_status!r}; expected current "
            f"status in {expected_from_statuses}"
        )

    gen = allocate_worker_write_generation(conn, deployment=deployment)
    now = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())

    # Build new attrs by overlaying updates on the current row's attrs.
    new_attrs = {
        "status": new_status,
        "requested_by_workflow": current.requested_by_workflow,
        "reason": current.reason,
        "view_ref": current.view_ref,
        "decision_schema_ref": current.decision_schema_ref,
        "timeout_at": (
            current.timeout_at.isoformat() if current.timeout_at else None
        ),
        "decided_by": current.decided_by,
        "decided_at": (
            current.decided_at.isoformat() if current.decided_at else None
        ),
        "decision": current.decision,
    }
    new_attrs.update(attrs_updates)
    new_attrs["status"] = new_status  # ensure attrs_updates didn't lose it

    with conn.transaction():
        with conn.cursor() as cur:
            closed_from = _close_current_live_row(
                cur,
                escalation_id=escalation_id,
                new_generation_id=gen.generation_id,
            )
            if closed_from is None:
                # Concurrent transition closed it first.
                raise EscalationStateError(
                    f"escalation {escalation_id} has no current live "
                    f"row to close (concurrent transition?)"
                )
            fact_id = _insert_fact(
                cur,
                escalation_id=escalation_id,
                subject_node_id=current.subject_node_id,
                attrs=new_attrs,
                status=new_status,
                workflow_id=current.requested_by_workflow,
                run_id=run_id,
                observed_at=now,
            )
            _insert_live(
                cur,
                escalation_id=escalation_id,
                subject_node_id=current.subject_node_id,
                attrs=new_attrs,
                valid_from_gen=gen.generation_id,
                last_fact_id=fact_id,
            )

    log.info(
        "escalation transition: id=%s %s -> %s generation=%s",
        escalation_id, current.status, new_status, gen.generation_id,
    )
    return get_escalation(conn, escalation_id=escalation_id)


def transition_to_decided(
    conn: psycopg.Connection,
    *,
    deployment: str,
    escalation_id: str,
    decision: dict[str, Any],
    decided_by: str,
) -> Escalation:
    """Operator response landed: ``pending`` → ``decided``.

    The CLI (``okg runtime escalation respond``) and the FastAPI
    endpoint both validate the ``decision`` payload against the
    escalation's ``decision_schema_ref`` *before* calling this
    function. By the time we get here, the payload is presumed valid.
    """
    return _transition(
        conn,
        deployment=deployment,
        escalation_id=escalation_id,
        new_status="decided",
        expected_from_statuses=("pending", "routed"),
        attrs_updates={
            "decision": decision,
            "decided_by": decided_by,
            "decided_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def transition_to_applied(
    conn: psycopg.Connection,
    *,
    deployment: str,
    escalation_id: str,
) -> Escalation:
    """Workflow consumed the decision: ``decided`` → ``applied``.

    Called by the SDK helper after the deployment workflow's
    ``request_escalation`` returned successfully and the workflow has
    presumably acted on the decision.
    """
    return _transition(
        conn,
        deployment=deployment,
        escalation_id=escalation_id,
        new_status="applied",
        expected_from_statuses=("decided",),
        attrs_updates={},
    )


def transition_to_timed_out(
    conn: psycopg.Connection,
    *,
    deployment: str,
    escalation_id: str,
) -> Escalation:
    """Timeout fired before operator response: ``pending`` → ``timed_out``."""
    return _transition(
        conn,
        deployment=deployment,
        escalation_id=escalation_id,
        new_status="timed_out",
        expected_from_statuses=("pending", "routed"),
        attrs_updates={},
    )


def sweep_timed_out_escalations(
    conn: psycopg.Connection,
    *,
    deployment: str,
    now: datetime,
) -> dict[str, int]:
    """Transition all open escalations whose ``timeout_at`` is past.

    For each ``okg.escalation_live`` row currently open
    (``valid_to_gen IS NULL``) under ``deployment`` with
    ``attrs->>'status' IN ('pending', 'routed')`` and
    ``(attrs->>'timeout_at')::timestamptz < now``, call
    :func:`transition_to_timed_out`. Each transition runs in its own
    bitemporal-correct unit (close current live row + write fact +
    write new live row, in one transaction), so concurrent operator
    responses race cleanly.

    The ``now`` parameter is required (rather than reading ``now()``
    from the database) so a wrapping DBOS workflow can capture it once
    in a ``@DBOS.step`` for replay-determinism: a recovered workflow
    sees the same timestamp the original attempt did and produces the
    same sweep set, not an expanded one drawn from the wall clock
    during downtime.

    Deployment scoping joins ``escalation_live`` to
    ``graph_generations`` via ``valid_from_gen`` because
    ``escalation_live`` has no ``deployment_name`` column — escalations
    inherit deployment identity from the worker-write generation that
    created them.

    Returns
    -------
    dict
        ``{"considered": N, "transitioned": M, "skipped": K}`` where
        ``considered`` is the size of the eligibility set as of the
        SELECT, ``transitioned`` is how many ``transition_to_timed_out``
        calls succeeded, and ``skipped`` is how many raised
        :class:`EscalationStateError` (concurrent transition closed
        them between SELECT and UPDATE).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT el.escalation_id
              FROM okg.escalation_live el
              JOIN okg.graph_generations gg
                ON gg.generation_id = el.valid_from_gen
             WHERE el.valid_to_gen IS NULL
               AND gg.deployment_name = %(deployment)s
               AND (el.attrs->>'status') IN ('pending', 'routed')
               AND (el.attrs->>'timeout_at') IS NOT NULL
               AND (el.attrs->>'timeout_at')::timestamptz < %(now)s
             ORDER BY el.valid_from_gen
            """,
            {"deployment": deployment, "now": now},
        )
        candidate_ids = [row[0] for row in cur.fetchall()]

    transitioned = 0
    skipped = 0
    for escalation_id in candidate_ids:
        try:
            transition_to_timed_out(
                conn,
                deployment=deployment,
                escalation_id=escalation_id,
            )
            transitioned += 1
        except EscalationStateError:
            # Concurrent transition (operator response or another
            # sweeper) closed the row first. Not a failure — the
            # escalation reached a terminal state, just not the one
            # we'd have written. Count as skipped.
            skipped += 1

    return {
        "considered": len(candidate_ids),
        "transitioned": transitioned,
        "skipped": skipped,
    }
