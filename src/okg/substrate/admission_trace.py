"""Admission trace reconstruction and model-refinement checks.

The TLA+ model in ``formal/admission`` is intentionally abstract. This
module provides the implementation-side bridge: load admission envelope,
event, ref, generation, and optional DBOS workflow rows, map them to a
small action vocabulary, and verify the observed order is accepted by
the protocol relation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

import psycopg

from okg.substrate.admission import admission_mode_awaits_decision


TERMINAL_STATUSES = frozenset({
    "published",
    "blocked",
    "rejected",
    "failed",
    "quarantined",
})


EVENT_TO_ACTION = {
    "created": "Submit",
    "submitted": "SubmitReview",
    "duplicate_coalesced": "DuplicateSubmit",
    "refs_attached": "RecoverRefs",
    "accepted": "Accept",
    "rejected": "Reject",
    "quarantined": "Quarantine",
    "published": "PublishFeedback",
    "blocked": "BlockFeedback",
    "failed": "Fail",
}


@dataclass(frozen=True)
class AdmissionTraceEvent:
    event_id: int
    event_type: str
    action: str
    workflow_name: str | None = None
    workflow_id: str | None = None
    # D2: the generation reference carried in an admission event payload is
    # the TEXT checkpoint ref, not a numeric compat token.
    generation_id: str | None = None
    reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "action": self.action,
            "workflow_name": self.workflow_name,
            "workflow_id": self.workflow_id,
            "generation_id": self.generation_id,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class AdmissionTrace:
    admission_id: str
    deployment_name: str
    mode: str
    final_status: str
    producer_id: str
    target_generation_ref: str | None
    events: tuple[AdmissionTraceEvent, ...]
    refs: tuple[dict[str, Any], ...]
    workflow_rows: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, Any]:
        return {
            "admission_id": self.admission_id,
            "deployment_name": self.deployment_name,
            "mode": self.mode,
            "final_status": self.final_status,
            "producer_id": self.producer_id,
            "target_generation_ref": self.target_generation_ref,
            "events": [event.as_dict() for event in self.events],
            "refs": list(self.refs),
            "workflow_rows": list(self.workflow_rows),
        }


@dataclass(frozen=True)
class TraceValidationResult:
    ok: bool
    errors: tuple[dict[str, Any], ...]
    actions: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "errors": list(self.errors),
            "actions": list(self.actions),
        }


def load_admission_traces(
    conn: psycopg.Connection,
    *,
    deployment_name: str | None = None,
    admission_id: str | None = None,
) -> list[AdmissionTrace]:
    """Load concrete admission rows and map events to abstract actions."""
    clauses = []
    params: list[Any] = []
    if deployment_name is not None:
        clauses.append("deployment_name = %s")
        params.append(deployment_name)
    if admission_id is not None:
        clauses.append("admission_id = %s::uuid")
        params.append(admission_id)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT admission_id::text, deployment_name, producer_id,
                   admission_mode, status, target_generation_ref
              FROM okg.admission_envelopes
              {where}
             ORDER BY created_at, admission_id
            """,
            params,
        )
        envelopes = [dict(row) for row in cur.fetchall()]

    workflow_refs: set[str] = set()
    traces: list[AdmissionTrace] = []
    for envelope in envelopes:
        events = _load_trace_events(
            conn,
            admission_id=envelope["admission_id"],
            workflow_refs=workflow_refs,
        )
        refs = _load_trace_refs(conn, admission_id=envelope["admission_id"])
        traces.append(
            AdmissionTrace(
                admission_id=str(envelope["admission_id"]),
                deployment_name=str(envelope["deployment_name"]),
                mode=str(envelope["admission_mode"]),
                final_status=str(envelope["status"]),
                producer_id=str(envelope["producer_id"]),
                target_generation_ref=envelope["target_generation_ref"],
                events=tuple(events),
                refs=tuple(refs),
            )
        )

    workflow_rows = _load_dbos_workflow_rows(conn, workflow_refs)
    if not workflow_rows:
        return traces
    rows_by_id = {
        str(row["workflow_id"]): row for row in workflow_rows
        if row.get("workflow_id")
    }
    enriched: list[AdmissionTrace] = []
    for trace in traces:
        trace_rows = []
        seen = set()
        for event in trace.events:
            if event.workflow_id and event.workflow_id not in seen:
                row = rows_by_id.get(event.workflow_id)
                if row:
                    trace_rows.append(row)
                    seen.add(event.workflow_id)
        enriched.append(
            AdmissionTrace(
                admission_id=trace.admission_id,
                deployment_name=trace.deployment_name,
                mode=trace.mode,
                final_status=trace.final_status,
                producer_id=trace.producer_id,
                target_generation_ref=trace.target_generation_ref,
                events=trace.events,
                refs=trace.refs,
                workflow_rows=tuple(trace_rows),
            )
        )
    return enriched


def validate_admission_trace(trace: AdmissionTrace) -> TraceValidationResult:
    """Validate one concrete trace against the abstract admission model."""
    mode = trace.mode
    state = "absent"
    refs_attached = False
    errors: list[dict[str, Any]] = []
    actions: list[str] = []

    for event in trace.events:
        action = event.action
        actions.append(action)
        before = state

        if event.event_type == "created":
            if state != "absent":
                errors.append(_transition_error(event, before, state))
            state = "pending"
        elif event.event_type == "submitted":
            # A held-mode (review/quarantine) source run finalizes
            # `pending` and records `submitted` (issue #1752).
            if not admission_mode_awaits_decision(mode) or state != "pending":
                errors.append(_transition_error(event, before, state))
        elif event.event_type == "duplicate_coalesced":
            if state == "absent":
                errors.append(_transition_error(event, before, state))
        elif event.event_type == "refs_attached":
            if state == "absent":
                errors.append(_transition_error(event, before, state))
            refs_attached = True
        elif event.event_type == "accepted":
            if state != "pending" or not refs_attached:
                errors.append(_transition_error(event, before, state))
            state = "accepted"
        elif event.event_type == "rejected":
            if state != "pending":
                errors.append(_transition_error(event, before, state))
            state = "rejected"
        elif event.event_type == "quarantined":
            if state != "pending":
                errors.append(_transition_error(event, before, state))
            state = "quarantined"
        elif event.event_type == "published":
            if state != "accepted" or not refs_attached:
                errors.append(_transition_error(event, before, state))
            state = "published"
        elif event.event_type == "blocked":
            if state not in {"pending", "accepted"} or not refs_attached:
                errors.append(_transition_error(event, before, state))
            state = "blocked"
        elif event.event_type == "failed":
            if state in TERMINAL_STATUSES or state == "absent":
                errors.append(_transition_error(event, before, state))
            state = "failed"
        else:
            errors.append({
                "code": "unknown_admission_event",
                "event_id": event.event_id,
                "event_type": event.event_type,
            })

    if state != trace.final_status:
        # Duplicate/replay-only traces may have a coalesced event after
        # terminal state. The state machine still needs to land on the
        # envelope's durable final status.
        errors.append({
            "code": "final_status_mismatch",
            "admission_id": trace.admission_id,
            "observed_state": state,
            "envelope_status": trace.final_status,
        })
    return TraceValidationResult(
        ok=not errors,
        errors=tuple(errors),
        actions=tuple(actions),
    )


def validate_admission_traces(
    traces: list[AdmissionTrace],
) -> TraceValidationResult:
    errors: list[dict[str, Any]] = []
    actions: list[str] = []
    for trace in traces:
        result = validate_admission_trace(trace)
        actions.extend(result.actions)
        errors.extend(result.errors)
    return TraceValidationResult(
        ok=not errors,
        errors=tuple(errors),
        actions=tuple(actions),
    )


def validate_event_sequence(
    *,
    mode: str,
    final_status: str,
    event_types: list[str],
) -> TraceValidationResult:
    """Pure helper for property tests over the abstract relation."""
    trace = AdmissionTrace(
        admission_id="property",
        deployment_name="property",
        mode=mode,
        final_status=final_status,
        producer_id="property",
        target_generation_ref=None,
        events=tuple(
            AdmissionTraceEvent(
                event_id=index + 1,
                event_type=event_type,
                action=EVENT_TO_ACTION.get(event_type, "<unknown>"),
            )
            for index, event_type in enumerate(event_types)
        ),
        refs=(),
    )
    return validate_admission_trace(trace)


def _load_trace_events(
    conn: psycopg.Connection,
    *,
    admission_id: str,
    workflow_refs: set[str],
) -> list[AdmissionTraceEvent]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT event_id, event_type, reason, payload, workflow_ref
              FROM okg.admission_events
             WHERE admission_id = %s::uuid
             ORDER BY event_id
            """,
            (admission_id,),
        )
        rows = [dict(row) for row in cur.fetchall()]
    events = []
    for row in rows:
        payload = _as_mapping(row.get("payload"))
        workflow_ref = _as_mapping(row.get("workflow_ref"))
        workflow_id = _optional_str(workflow_ref.get("workflow_id"))
        if workflow_id:
            workflow_refs.add(workflow_id)
        events.append(
            AdmissionTraceEvent(
                event_id=int(row["event_id"]),
                event_type=str(row["event_type"]),
                action=EVENT_TO_ACTION.get(str(row["event_type"]), "<unknown>"),
                workflow_name=_optional_str(
                    workflow_ref.get("workflow_name")
                ),
                workflow_id=workflow_id,
                generation_id=_optional_str(
                    payload.get("generation_id")
                    or payload.get("target_generation_ref")
                ),
                reason=_optional_str(row.get("reason")),
            )
        )
    return events


def _load_trace_refs(
    conn: psycopg.Connection,
    *,
    admission_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT ref_kind, source, run_id::text, fact_id_min,
                   fact_id_max, record_count
              FROM okg.admission_record_refs
             WHERE admission_id = %s::uuid
             ORDER BY ref_id
            """,
            (admission_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def _load_dbos_workflow_rows(
    conn: psycopg.Connection,
    workflow_ids: set[str],
) -> list[dict[str, Any]]:
    if not workflow_ids:
        return []
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('dbos.workflow_status')")
        if cur.fetchone()[0] is None:
            return []
        cur.execute(
            """
            SELECT workflow_uuid::text, name, status, queue_name,
                   created_at, updated_at
              FROM dbos.workflow_status
             WHERE workflow_uuid::text = ANY(%s)
             ORDER BY created_at, workflow_uuid
            """,
            (sorted(workflow_ids),),
        )
        rows = cur.fetchall()
    return [
        {
            "workflow_id": str(row[0]),
            "name": row[1],
            "status": row[2],
            "queue_name": row[3],
            "created_at": row[4],
            "updated_at": row[5],
        }
        for row in rows
    ]


def _transition_error(
    event: AdmissionTraceEvent,
    before: str,
    state: str,
) -> dict[str, Any]:
    return {
        "code": "unmodeled_admission_transition",
        "event_id": event.event_id,
        "event_type": event.event_type,
        "action": event.action,
        "before": before,
        "state": state,
    }


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _optional_str(value: Any) -> str | None:
    return str(value) if value not in (None, "") else None
