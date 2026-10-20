"""Operator response surface for escalations (Phase 2 §1.4.2 / §1.4.3).

The substrate exposes two equivalent ways for an operator to respond
to an escalation:

  * CLI: ``okg runtime escalation respond <escalation_id> --decision <json>``
  * FastAPI: ``POST /escalations/{escalation_id}/respond``

Both paths flow through this module's :func:`respond_to_escalation`:

  1. Read the escalation row by id (raises if not found / not pending).
  2. Validate the decision JSON against the escalation's
     ``decision_schema_ref`` (raises :class:`InvalidDecisionPayload` on
     mismatch).
  3. Transition the escalation to ``decided`` (writes new fact + live
     rows at a worker-write generation).
  4. Call ``DBOSClient.send(workflow_id, OperatorDecision, topic=...)``
     to wake the requesting workflow.

The state mutation (step 3) and the wake-up (step 4) are sequenced
deliberately: the durable status transition happens first, so a
crash between steps 3 and 4 leaves the escalation in ``decided``
status; the deployment workflow's ``request_escalation`` step, on
recovery, sees the decided status and can pull the decision payload
directly from ``okg.escalation_live`` rather than waiting on
``DBOS.recv``. This is the "send-after-commit" pattern from the
DBOS docs.

## decision_schema_ref formats

Phase 2 supports three lightweight schema references:

  * ``schema:open`` — any JSON object is accepted. Useful for a
    deployment that hasn't yet pinned a verdict schema.
  * ``schema:literal:<urlencoded-json-schema>`` — the schema is
    embedded in the ref string itself. Compact for simple verdict
    shapes (e.g. ``{"type": "object", "required": ["label"]}``).
  * ``schema:registry:<key>`` — looked up in an in-process registry
    populated by SDK schema-declaration helpers. Not implemented in
    Phase 2.1; treated as ``schema:open`` with a warning.

Richer schema-registry integration (e.g. catalog-version-pinned
schemas) lands in a follow-on proposal.
"""
from __future__ import annotations

import json
import logging
import urllib.parse
from dataclasses import dataclass
from typing import Any

import psycopg

from .escalation import (
    EscalationStateError,
    get_escalation,
    transition_to_decided,
)


__all__ = [
    "InvalidDecisionPayload",
    "OperatorResponseResult",
    "respond_to_escalation",
    "validate_decision_payload",
]


log = logging.getLogger(__name__)


# Topic prefix used for DBOSClient.send wake-up. The deployment SDK's
# `request_escalation` calls `DBOS.recv(topic=f"escalation:{esc_id}")`
# inside the await step, so the two must agree on this format.
_TOPIC_PREFIX = "escalation"


class InvalidDecisionPayload(ValueError):
    """Raised when the operator's decision JSON does not validate
    against the escalation's ``decision_schema_ref``.

    The exception message is operator-facing — the CLI prints it
    verbatim and exits non-zero. The FastAPI endpoint surfaces it as
    HTTP 400 with the message in the response body.
    """


@dataclass(frozen=True)
class OperatorResponseResult:
    """Outcome of a successful operator response.

    Attributes
    ----------
    escalation_id : str
        The escalation that transitioned.
    workflow_id : str
        The workflow that was woken via ``DBOSClient.send``.
    new_status : str
        Always ``"decided"`` on success.
    decided_at : str
        ISO-8601 timestamp of the transition.
    decided_by : str
        Operator identifier (CLI: ``--decided-by`` flag default
        ``"cli:$USER"``; FastAPI: bearer-token subject or
        ``"http:anonymous"``).
    wake_sent : bool
        True if ``DBOSClient.send`` was invoked. False only when the
        ``send_wake`` flag is False (test paths).
    """

    escalation_id: str
    workflow_id: str
    new_status: str
    decided_at: str
    decided_by: str
    wake_sent: bool


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


def validate_decision_payload(
    *,
    decision: Any,
    decision_schema_ref: str,
) -> None:
    """Validate ``decision`` against ``decision_schema_ref``.

    Raises
    ------
    InvalidDecisionPayload
        On any validation failure. Message names the problem.
    """
    if decision_schema_ref == "schema:open":
        return  # no constraint

    if decision_schema_ref.startswith("schema:literal:"):
        encoded = decision_schema_ref[len("schema:literal:"):]
        try:
            schema = json.loads(urllib.parse.unquote(encoded))
        except json.JSONDecodeError as e:
            raise InvalidDecisionPayload(
                f"decision_schema_ref embeds malformed JSON Schema: {e}"
            ) from e
        try:
            import jsonschema
        except ImportError as e:  # pragma: no cover — dep declared
            raise InvalidDecisionPayload(
                "jsonschema package not available; cannot validate "
                f"decision against {decision_schema_ref!r}"
            ) from e
        try:
            jsonschema.validate(instance=decision, schema=schema)
        except jsonschema.ValidationError as e:
            raise InvalidDecisionPayload(
                f"decision does not validate against schema: "
                f"{e.message} (path: {list(e.absolute_path)})"
            ) from e
        return

    if decision_schema_ref.startswith("schema:registry:"):
        log.warning(
            "schema:registry references are not implemented in Phase 2; "
            "accepting %s as schema:open",
            decision_schema_ref,
        )
        return

    raise InvalidDecisionPayload(
        f"unsupported decision_schema_ref scheme: {decision_schema_ref!r}; "
        "expected one of: schema:open, schema:literal:<json>, "
        "schema:registry:<key>"
    )


# ---------------------------------------------------------------------------
# Response orchestration
# ---------------------------------------------------------------------------


def respond_to_escalation(
    conn: psycopg.Connection,
    *,
    deployment: str,
    escalation_id: str,
    decision: dict[str, Any],
    decided_by: str,
    send_wake: bool = True,
    dbos_client: Any | None = None,
) -> OperatorResponseResult:
    """Execute the operator-response flow.

    Parameters
    ----------
    conn : psycopg.Connection
        Connection used to read the escalation and write the transition.
        Caller manages the connection lifetime.
    deployment : str
        Deployment slug; used to allocate the worker-write generation.
    escalation_id : str
        The escalation to respond to. Looked up in
        ``okg.escalation_live`` at ``valid_to_gen IS NULL``.
    decision : dict
        The operator's verdict payload. Validated against the
        escalation's ``decision_schema_ref``.
    decided_by : str
        Operator identifier; stamped into the escalation's attrs.
    send_wake : bool, default True
        Whether to call ``DBOSClient.send`` after the transition. Test
        paths set False to avoid spinning up a DBOS client.
    dbos_client : DBOSClient | None
        Optional pre-built client (lets tests inject a mock). When
        None and ``send_wake=True``, this function builds one against
        the connection's DSN.

    Raises
    ------
    EscalationNotFoundError
        Unknown escalation_id.
    EscalationStateError
        Escalation is already decided / applied / timed_out.
    InvalidDecisionPayload
        Decision JSON does not validate against the schema_ref.
    """
    esc = get_escalation(conn, escalation_id=escalation_id)
    if esc.status not in ("pending", "routed"):
        raise EscalationStateError(
            f"escalation {escalation_id} is in status {esc.status!r}; "
            "expected pending or routed"
        )

    validate_decision_payload(
        decision=decision,
        decision_schema_ref=esc.decision_schema_ref,
    )

    after = transition_to_decided(
        conn,
        deployment=deployment,
        escalation_id=escalation_id,
        decision=decision,
        decided_by=decided_by,
    )

    wake_sent = False
    if send_wake:
        wake_sent = _send_wake(
            workflow_id=after.requested_by_workflow,
            escalation_id=escalation_id,
            decision=decision,
            dbos_client=dbos_client,
            conninfo=conn.info,
        )

    return OperatorResponseResult(
        escalation_id=escalation_id,
        workflow_id=after.requested_by_workflow,
        new_status=after.status,
        decided_at=after.decided_at.isoformat() if after.decided_at else "",
        decided_by=after.decided_by or decided_by,
        wake_sent=wake_sent,
    )


def _send_wake(
    *,
    workflow_id: str,
    escalation_id: str,
    decision: dict[str, Any],
    dbos_client: Any | None,
    conninfo: Any,
) -> bool:
    """Send the decision via DBOSClient.send. Best-effort: failures
    are logged and surface as ``wake_sent=False`` but do NOT roll back
    the escalation transition — by the time we get here, the
    transition is already durable.

    Returns True on success, False on any error.
    """
    try:
        client = dbos_client
        if client is None:
            from dbos import DBOSClient
            # Reconstruct DSN from psycopg conninfo. Per Decision 11
            # the substrate DSN serves all three DBOSClient URLs.
            dsn = _conninfo_to_dsn(conninfo)
            client = DBOSClient(
                database_url=dsn,
                system_database_url=dsn,
                application_database_url=dsn,
            )
        topic = f"{_TOPIC_PREFIX}:{escalation_id}"
        client.send(
            destination_id=workflow_id,
            message=decision,
            topic=topic,
        )
        log.info(
            "send wake: workflow=%s topic=%s decision_keys=%s",
            workflow_id, topic, sorted(decision.keys()),
        )
        return True
    except Exception as e:  # noqa: BLE001 — best-effort
        log.error(
            "send wake failed (transition is still durable): "
            "workflow=%s escalation=%s error=%s",
            workflow_id, escalation_id, e,
        )
        return False


def _conninfo_to_dsn(conninfo: Any) -> str:
    """Build a libpq DSN string from psycopg's ConnectionInfo.

    psycopg's ``conn.info`` exposes user / dbname / host / port; we
    reconstruct the URL form. Password is intentionally NOT included —
    the DBOSClient inherits the running process's credentials via
    PGPASSWORD env or .pgpass.
    """
    user = conninfo.user or "postgres"
    host = conninfo.host or "localhost"
    port = conninfo.port or 5432
    dbname = conninfo.dbname or "postgres"
    password = conninfo.password
    auth = f"{user}:{urllib.parse.quote(password, safe='')}@" if password else f"{user}@"
    return f"postgresql://{auth}{host}:{port}/{dbname}"
