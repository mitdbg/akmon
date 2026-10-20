"""SDK `request_escalation` primitive (Phase 2 §3.2).

Two-step pattern per Phase 0 C5:

  1. ``_create_escalation_step`` — runs inside a DBOS transaction.
     Writes the escalation row (status=pending) keyed on
     ``(workflow_id, step_id)`` for retry idempotency.
  2. ``_await_escalation_step`` — runs as a DBOS workflow step
     (``retries_allowed=False``). Blocks on ``DBOS.recv`` keyed on
     ``escalation:{escalation_id}``.

After receiving the decision, the SDK transitions the escalation to
``applied`` so the substrate ledger reflects the full lifecycle.

Outside a DBOS workflow (e.g. tests with :func:`bind_test_context`),
``request_escalation`` raises an explicit error — the primitive only
makes sense inside a workflow, since the wake-up needs DBOS.recv.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from .context import current_context


__all__ = ["EscalationTimeoutError", "request_escalation"]


log = logging.getLogger(__name__)


# Topic prefix matches okg.substrate.runtime.escalation_response (the
# operator-side surface uses the same string). Changing either
# requires changing both.
_TOPIC_PREFIX = "escalation"


class EscalationTimeoutError(TimeoutError):
    """Raised when the operator does not respond before
    ``timeout_seconds`` elapses. The substrate transitions the
    escalation to ``timed_out`` before this error is raised."""


def request_escalation(
    *,
    subject: str,
    reason: str,
    view: str | None = None,
    decision_schema: str = "schema:open",
    timeout_seconds: float = 86400.0,
) -> dict[str, Any]:
    """Pause the workflow until an operator decides on ``subject``.

    Two-step internally:

      1. Create the escalation row (idempotent on retry via
         (workflow_id, step_id)).
      2. Block on ``DBOS.recv(topic="escalation:{id}",
         timeout=timeout_seconds)``.

    Parameters
    ----------
    subject : str
        The node_id the escalation is about.
    reason : str
        Human-readable explanation; surfaces in the operator's CLI
        and FastAPI listings.
    view : str | None
        Optional view reference (e.g. a markdown view ID) that
        operators can pull up for context.
    decision_schema : str, default "schema:open"
        ``decision_schema_ref`` stamped into the escalation row. The
        operator-response surface validates payloads against this.
    timeout_seconds : float, default 86400.0
        How long to wait before timing out.

    Returns
    -------
    dict
        The operator's decision payload.

    Raises
    ------
    EscalationTimeoutError
        If no operator response arrives before the timeout.
    """
    try:
        from dbos import DBOS
    except ImportError as e:
        raise RuntimeError(
            "request_escalation requires the dbos package; "
            "ensure it's installed and the SDK is running inside a "
            "DBOS singleton context"
        ) from e

    ctx = current_context()
    if not ctx.workflow_id or ctx.workflow_id.startswith("_local_"):
        raise RuntimeError(
            "request_escalation must be called inside a DBOS workflow "
            f"(got workflow_id={ctx.workflow_id!r}); ensure the caller "
            "is decorated with @deployment_workflow"
        )

    step_id = ctx.step_id or "_create_escalation"

    # Step 1: create. Goes via the substrate's lifecycle module which
    # handles the worker-write generation + idempotency.
    from okg.substrate.runtime.escalation import create_escalation

    timeout_at = (
        datetime.now(timezone.utc) + timedelta(seconds=timeout_seconds)
        if timeout_seconds else None
    )
    esc = create_escalation(
        ctx.conn,
        deployment=ctx.deployment,
        workflow_id=ctx.workflow_id,
        step_id=step_id,
        subject_node_id=subject,
        reason=reason,
        view_ref=view,
        decision_schema_ref=decision_schema,
        timeout_at=timeout_at,
    )

    log.info(
        "request_escalation: escalation_id=%s subject=%s timeout=%.0fs",
        esc.escalation_id, subject, timeout_seconds,
    )

    # Step 2: await. DBOS.recv blocks until a matching DBOSClient.send
    # arrives (operator response via CLI / FastAPI) or the timeout
    # fires.
    topic = f"{_TOPIC_PREFIX}:{esc.escalation_id}"
    try:
        decision = DBOS.recv(topic=topic, timeout_seconds=timeout_seconds)
    except Exception as e:  # noqa: BLE001 — DBOS may surface variants
        # Treat any recv failure as a timeout for the operator's
        # perspective; the substrate has not yet been told to time the
        # escalation out, so we do that now.
        log.warning(
            "request_escalation: recv error for %s: %s; transitioning "
            "to timed_out", esc.escalation_id, e,
        )
        from okg.substrate.runtime.escalation import (
            transition_to_timed_out,
        )
        transition_to_timed_out(
            ctx.conn,
            deployment=ctx.deployment,
            escalation_id=esc.escalation_id,
        )
        raise EscalationTimeoutError(
            f"no operator response for escalation {esc.escalation_id} "
            f"within {timeout_seconds}s"
        ) from e

    if decision is None:
        # DBOS.recv returns None on timeout.
        from okg.substrate.runtime.escalation import (
            transition_to_timed_out,
        )
        transition_to_timed_out(
            ctx.conn,
            deployment=ctx.deployment,
            escalation_id=esc.escalation_id,
        )
        raise EscalationTimeoutError(
            f"operator did not respond to {esc.escalation_id} within "
            f"{timeout_seconds}s"
        )

    # Step 3: mark applied so the lifecycle row reflects consumption.
    from okg.substrate.runtime.escalation import transition_to_applied

    transition_to_applied(
        ctx.conn,
        deployment=ctx.deployment,
        escalation_id=esc.escalation_id,
    )

    log.info(
        "request_escalation resolved: escalation_id=%s decision_keys=%s",
        esc.escalation_id,
        sorted(decision.keys()) if isinstance(decision, dict) else "<non-dict>",
    )
    return decision
