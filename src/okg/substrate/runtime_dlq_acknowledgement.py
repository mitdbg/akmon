"""Plan and apply exact, append-only runtime DLQ acknowledgements."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from okg.substrate.errors import SubstrateError
from okg.substrate.runtime_dlq import (
    _FAILURE_STATUSES,
    _digest,
    _responsibility,
    _workflow_row,
    failure_snapshot_digest,
)


ACKNOWLEDGEMENT_REQUEST_SCHEMA = "okg.runtime_dlq_acknowledgement_request.v1"
ACKNOWLEDGEMENT_PLAN_SCHEMA = "okg.runtime_dlq_acknowledgement_plan.v1"
ACKNOWLEDGEMENT_RESULT_SCHEMA = "okg.runtime_dlq_acknowledgement_result.v1"
ACKNOWLEDGEMENT_TABLE = "okg.runtime_dlq_acknowledgement_events"

_WORKFLOW_COLUMNS = (
    "workflow_uuid",
    "name",
    "status",
    "application_version",
    "queue_name",
    "inputs",
    "serialization",
    "error",
    "output",
    "updated_at",
)


def load_acknowledgement_request(path: str | Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SubstrateError(
            "runtime DLQ acknowledgement request could not be loaded",
            data={"path": str(path), "error": type(exc).__name__},
        ) from exc
    if not isinstance(value, dict):
        raise SubstrateError("runtime DLQ acknowledgement request must be an object")
    return value


def plan_runtime_dlq_acknowledgements(
    conn: Any,
    *,
    deployment: str,
    request: Mapping[str, Any],
    lock: bool = False,
) -> dict[str, Any]:
    """Validate a request and bind it to exact live failure snapshots."""
    _require_acknowledgement_schema(conn)
    if request.get("schema") != ACKNOWLEDGEMENT_REQUEST_SCHEMA:
        raise SubstrateError(
            f"runtime DLQ request must use {ACKNOWLEDGEMENT_REQUEST_SCHEMA}"
        )
    actor = _required_text(request, "actor")
    reason = _required_text(request, "reason")
    candidate_revision = _required_text(request, "candidate_revision")
    receipt = request.get("operation_receipt")
    if not isinstance(receipt, Mapping):
        raise SubstrateError("runtime DLQ request requires operation_receipt")
    if str(receipt.get("candidate_revision") or "") != candidate_revision:
        raise SubstrateError(
            "operation receipt candidate revision does not match request"
        )
    receipt_digest = _digest(dict(receipt))
    fence = _runtime_fence_binding(
        conn,
        deployment=deployment,
        candidate_revision=candidate_revision,
        receipt=receipt,
        lock=lock,
    )
    readiness = request.get("readiness")
    if not isinstance(readiness, Mapping):
        raise SubstrateError("runtime DLQ request requires readiness evidence")
    witnesses = request.get("witnesses")
    if not isinstance(witnesses, Sequence) or isinstance(witnesses, (str, bytes)):
        raise SubstrateError("runtime DLQ request requires a witness list")
    witness_rows = [dict(row) for row in witnesses if isinstance(row, Mapping)]
    if len(witness_rows) != len(witnesses):
        raise SubstrateError("every runtime DLQ witness must be an object")
    workflow_ids = request.get("workflow_ids")
    if not isinstance(workflow_ids, Sequence) or isinstance(workflow_ids, (str, bytes)):
        raise SubstrateError("runtime DLQ request requires workflow_ids")
    ids = [str(value).strip() for value in workflow_ids]
    if not ids or any(not value for value in ids) or len(set(ids)) != len(ids):
        raise SubstrateError("workflow_ids must be a non-empty unique list")

    current_version = _current_application_version(conn, lock=lock)
    rows = _workflow_rows(conn, ids, lock=lock)
    missing = sorted(set(ids) - set(rows))
    if missing:
        raise SubstrateError(
            "runtime DLQ acknowledgement target is absent",
            data={"missing_workflow_ids": missing},
        )
    already_acknowledged = _acknowledged_workflow_ids(
        conn,
        workflow_ids=ids,
        lock=lock,
    )
    if already_acknowledged:
        raise SubstrateError(
            "runtime DLQ acknowledgement target is already acknowledged",
            data={"workflow_ids": sorted(already_acknowledged)},
        )

    entries: list[dict[str, Any]] = []
    for workflow_id in sorted(ids):
        raw = rows[workflow_id]
        failure = _workflow_row(raw)
        if failure["status"] not in _FAILURE_STATUSES:
            raise SubstrateError(
                "runtime DLQ acknowledgement target is not a terminal failure",
                data={"workflow_id": workflow_id, "status": failure["status"]},
            )
        if not failure.get("application_version"):
            raise SubstrateError(
                "runtime DLQ acknowledgement target has no application version",
                data={"workflow_id": workflow_id},
            )
        if failure["application_version"] != current_version:
            raise SubstrateError(
                "runtime DLQ acknowledgement target is not current-version",
                data={
                    "workflow_id": workflow_id,
                    "application_version": failure["application_version"],
                    "current_application_version": current_version,
                },
            )
        responsibility = _responsibility(failure)
        if (
            failure["name"] == "okg_source_sync"
            and responsibility.get("deployment") != deployment
        ):
            raise SubstrateError(
                "runtime DLQ acknowledgement target belongs to another deployment",
                data={"workflow_id": workflow_id, "responsibility": responsibility},
            )
        witness = _validated_witness(
            conn,
            deployment=deployment,
            failure=failure,
            failure_updated_at=raw.get("updated_at"),
            witnesses=witness_rows,
            readiness=readiness,
            lock=lock,
        )
        entries.append(
            {
                "workflow_id": workflow_id,
                "workflow_name": failure["name"],
                "application_version": failure["application_version"],
                "failure_snapshot_digest": failure_snapshot_digest(failure),
                "error_digest": _digest(failure.get("error")),
                "responsibility": responsibility,
                "witness": witness,
            }
        )

    plan_body: dict[str, Any] = {
        "schema": ACKNOWLEDGEMENT_PLAN_SCHEMA,
        "deployment": deployment,
        "actor": actor,
        "reason": reason,
        "candidate_revision": candidate_revision,
        "operation_receipt_digest": receipt_digest,
        "runtime_fence": fence,
        "current_application_version": current_version,
        "entries": entries,
    }
    return {**plan_body, "plan_id": _digest(plan_body), "applied": False}


def apply_runtime_dlq_acknowledgements(
    conn: Any,
    *,
    deployment: str,
    request: Mapping[str, Any],
    confirm_plan_id: str,
) -> dict[str, Any]:
    """Re-plan under row locks and append the exact confirmed batch."""
    if not confirm_plan_id.startswith("sha256:"):
        raise SubstrateError("--confirm-plan-id must be a sha256 digest")
    with conn.transaction():
        plan = plan_runtime_dlq_acknowledgements(
            conn,
            deployment=deployment,
            request=request,
            lock=True,
        )
        if plan["plan_id"] != confirm_plan_id:
            raise SubstrateError(
                "runtime DLQ acknowledgement plan drifted",
                data={
                    "confirmed_plan_id": confirm_plan_id,
                    "observed_plan_id": plan["plan_id"],
                },
            )
        plan_payload = {
            key: value
            for key, value in plan.items()
            if key not in {"plan_id", "applied"}
        }
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO okg.runtime_dlq_acknowledgement_events "
                "(deployment_name, workflow_id, workflow_name, "
                "application_version, failure_snapshot_digest, error_digest, "
                "responsibility, actor, reason, candidate_revision, "
                "operation_receipt_digest, witness, plan_payload, plan_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, "
                "%s, %s::jsonb, %s::jsonb, %s)",
                [
                    (
                        deployment,
                        entry["workflow_id"],
                        entry["workflow_name"],
                        entry["application_version"],
                        entry["failure_snapshot_digest"],
                        entry["error_digest"],
                        json.dumps(entry["responsibility"], sort_keys=True),
                        plan["actor"],
                        plan["reason"],
                        plan["candidate_revision"],
                        plan["operation_receipt_digest"],
                        json.dumps(entry["witness"], sort_keys=True),
                        json.dumps(plan_payload, sort_keys=True),
                        plan["plan_id"],
                    )
                    for entry in plan["entries"]
                ],
            )
    return {
        **plan,
        "schema": ACKNOWLEDGEMENT_RESULT_SCHEMA,
        "applied": True,
        "acknowledged_count": len(plan["entries"]),
    }


def _require_acknowledgement_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (ACKNOWLEDGEMENT_TABLE,))
        row = cur.fetchone()
    if row is None or row[0] is None:
        raise SubstrateError(
            "runtime DLQ acknowledgement schema is unavailable",
            data={"table": ACKNOWLEDGEMENT_TABLE},
        )


def _current_application_version(conn: Any, *, lock: bool) -> str:
    with conn.cursor() as cur:
        if lock:
            cur.execute("LOCK TABLE dbos.application_versions IN SHARE MODE")
        suffix = " FOR SHARE" if lock else ""
        cur.execute(
            "SELECT version_name FROM dbos.application_versions "
            "ORDER BY version_timestamp DESC NULLS LAST, "
            f"created_at DESC NULLS LAST LIMIT 1{suffix}"
        )
        row = cur.fetchone()
    value = str(row[0] or "").strip() if row else ""
    if not value:
        raise SubstrateError("current DBOS application version is unavailable")
    return value


def _workflow_rows(
    conn: Any,
    workflow_ids: Sequence[str],
    *,
    lock: bool,
) -> dict[str, dict[str, Any]]:
    suffix = " FOR UPDATE" if lock else ""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT workflow_uuid, name, status, application_version, "
            "queue_name, inputs, serialization, error, output, updated_at "
            "FROM dbos.workflow_status WHERE workflow_uuid = ANY(%s)" + suffix,
            (list(workflow_ids),),
        )
        rows = cur.fetchall()
    values = [
        dict(row) if isinstance(row, Mapping) else dict(zip(_WORKFLOW_COLUMNS, row))
        for row in rows
    ]
    return {str(row["workflow_uuid"]): row for row in values}


def _acknowledged_workflow_ids(
    conn: Any,
    *,
    workflow_ids: Sequence[str],
    lock: bool,
) -> set[str]:
    suffix = " FOR SHARE" if lock else ""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT workflow_id FROM okg.runtime_dlq_acknowledgement_events "
            "WHERE workflow_id = ANY(%s)" + suffix,
            (list(workflow_ids),),
        )
        return {str(row[0]) for row in cur.fetchall()}


def _runtime_fence_binding(
    conn: Any,
    *,
    deployment: str,
    candidate_revision: str,
    receipt: Mapping[str, Any],
    lock: bool,
) -> dict[str, Any]:
    suffix = " FOR SHARE" if lock else ""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT promotion_id, status, metadata "
            "FROM okg.deployment_promotion_fences WHERE deployment_name = %s" + suffix,
            (deployment,),
        )
        row = cur.fetchone()
    if row is None:
        raise SubstrateError(
            "runtime DLQ acknowledgement requires an active promotion fence"
        )
    metadata = row[2] if isinstance(row[2], Mapping) else {}
    fence_candidate = str(metadata.get("candidate_revision") or "")
    if row[1] == "released":
        raise SubstrateError("runtime DLQ acknowledgement promotion fence is released")
    if fence_candidate != candidate_revision:
        raise SubstrateError(
            "runtime DLQ acknowledgement candidate does not own the fence",
            data={"expected": fence_candidate, "observed": candidate_revision},
        )
    receipt_owner = str(
        receipt.get("runtime_fence_owner") or receipt.get("promotion_id") or ""
    )
    if receipt_owner != str(row[0]):
        raise SubstrateError(
            "operation receipt is not bound to the active promotion fence"
        )
    return {
        "promotion_id": str(row[0]),
        "status": str(row[1]),
        "candidate_revision": fence_candidate,
    }


def _validated_witness(
    conn: Any,
    *,
    deployment: str,
    failure: Mapping[str, Any],
    failure_updated_at: Any,
    witnesses: Sequence[Mapping[str, Any]],
    readiness: Mapping[str, Any],
    lock: bool,
) -> dict[str, Any]:
    workflow_name = str(failure.get("name") or "")
    if workflow_name == "okg_source_sync":
        return _source_sync_witness(
            conn,
            deployment=deployment,
            failure=failure,
            failure_updated_at=failure_updated_at,
            witnesses=witnesses,
            readiness=readiness,
            lock=lock,
        )
    if workflow_name == "doctor_workflow":
        return _doctor_witness(
            conn,
            deployment=deployment,
            failure=failure,
            failure_updated_at=failure_updated_at,
            witnesses=witnesses,
            readiness=readiness,
            lock=lock,
        )
    if workflow_name == "okg_ci_resource_monitor":
        return _resource_monitor_witness(
            conn,
            failure=failure,
            deployment=deployment,
            failure_updated_at=failure_updated_at,
            witnesses=witnesses,
            readiness=readiness,
            lock=lock,
        )
    if workflow_name == "okg_ci_git_history_reconcile":
        return _git_reconcile_witness(
            conn,
            deployment=deployment,
            failure=failure,
            failure_updated_at=failure_updated_at,
            witnesses=witnesses,
            readiness=readiness,
            lock=lock,
        )
    raise SubstrateError(
        "runtime DLQ acknowledgement refuses unknown workflow type",
        data={"workflow_name": workflow_name},
    )


def _source_sync_witness(
    conn: Any,
    *,
    deployment: str,
    failure: Mapping[str, Any],
    failure_updated_at: Any,
    witnesses: Sequence[Mapping[str, Any]],
    readiness: Mapping[str, Any],
    lock: bool,
) -> dict[str, Any]:
    responsibility = _responsibility(failure)
    matching = [
        witness
        for witness in witnesses
        if witness.get("kind") == "source_sync"
        and witness.get("responsibility") == responsibility
    ]
    if len(matching) != 1:
        raise SubstrateError(
            "source-sync acknowledgement requires one matching responsibility witness",
            data={"responsibility": responsibility, "match_count": len(matching)},
        )
    witness = matching[0]
    replacement_id = _required_text(witness, "replacement_workflow_id")
    replacement = _one_workflow(conn, replacement_id, lock=lock)
    replacement_view = _workflow_row(replacement)
    if (
        replacement_view["name"] != "okg_source_sync"
        or replacement_view["status"] != "SUCCESS"
    ):
        raise SubstrateError(
            "source-sync recovery witness is not a successful source workflow"
        )
    if _responsibility(replacement_view) != responsibility:
        raise SubstrateError(
            "source-sync recovery witness responsibility does not match"
        )
    _require_matching_recovery_version(
        failure, replacement_view, label="source-sync"
    )
    _require_newer(
        failure_updated_at, replacement.get("updated_at"), label="source-sync"
    )
    watermark = _source_watermark(
        conn,
        deployment=deployment,
        responsibility=responsibility,
        failure_updated_at=failure_updated_at,
        lock=lock,
    )
    generation = _published_generation_for_recovery(
        conn,
        deployment=deployment,
        expected_source=str(responsibility["sources"][0]),
        failure_updated_at=failure_updated_at,
        recovery=replacement,
        lock=lock,
    )
    live_convergence = _live_source_convergence(
        workflow_id=replacement_id,
        watermark=watermark,
        generation=generation,
    )
    return {
        "kind": "source_sync",
        "replacement_workflow_id": replacement_id,
        "responsibility": responsibility,
        "watermark": watermark,
        "published_generation": generation,
        "live_convergence": live_convergence,
        "request_readiness_digest": _digest(dict(readiness)),
    }


def _doctor_witness(
    conn: Any,
    *,
    deployment: str,
    failure: Mapping[str, Any],
    failure_updated_at: Any,
    witnesses: Sequence[Mapping[str, Any]],
    readiness: Mapping[str, Any],
    lock: bool,
) -> dict[str, Any]:
    matching = [row for row in witnesses if row.get("kind") == "doctor"]
    if len(matching) != 1:
        raise SubstrateError("doctor acknowledgement requires exactly one witness")
    witness = matching[0]
    replacement_id = _required_text(witness, "replacement_workflow_id")
    replacement = _one_workflow(conn, replacement_id, lock=lock)
    replacement_view = _workflow_row(replacement)
    if (
        replacement_view["name"] != failure.get("name")
        or replacement_view["status"] != "SUCCESS"
    ):
        raise SubstrateError("doctor witness is not a successful doctor workflow")
    _require_matching_recovery_version(failure, replacement_view, label="doctor")
    _require_newer(failure_updated_at, replacement.get("updated_at"), label="doctor")
    verdict = _workflow_event(
        conn,
        replacement_id,
        "doctor_verdict",
        lock=lock,
    )
    if (
        not isinstance(verdict, Mapping)
        or verdict.get("deployment") != deployment
        or verdict.get("ok") is not True
    ):
        raise SubstrateError("doctor verdict is unhealthy or for another deployment")
    return {
        "kind": "doctor",
        "replacement_workflow_id": replacement_id,
        "verdict_digest": _digest(dict(verdict)),
        "request_readiness_digest": _digest(dict(readiness)),
    }


def _resource_monitor_witness(
    conn: Any,
    *,
    deployment: str,
    failure: Mapping[str, Any],
    failure_updated_at: Any,
    witnesses: Sequence[Mapping[str, Any]],
    readiness: Mapping[str, Any],
    lock: bool,
) -> dict[str, Any]:
    matching = [
        row for row in witnesses if row.get("kind") == "resource_monitor"
    ]
    if len(matching) != 1:
        raise SubstrateError(
            "resource monitor acknowledgement requires exactly one witness"
        )
    replacement_id = _required_text(matching[0], "replacement_workflow_id")
    replacement = _one_workflow(conn, replacement_id, lock=lock)
    replacement_view = _workflow_row(replacement)
    if (
        replacement_view["name"] != failure.get("name")
        or replacement_view["status"] != "SUCCESS"
    ):
        raise SubstrateError(
            "resource monitor witness is not a successful matching workflow"
        )
    _require_matching_recovery_version(
        failure, replacement_view, label="resource monitor"
    )
    _require_newer(
        failure_updated_at,
        replacement.get("updated_at"),
        label="resource monitor",
    )
    output = _decoded_dbos_value(
        replacement.get("output"),
        replacement.get("serialization"),
        label="resource monitor output",
    )
    if (
        isinstance(output, Mapping)
        and output.get("status") == "skipped_previous_run_active"
    ):
        # A tick the overlap guard skipped succeeds quickly and produces no
        # samples. On a monitor that outlasts its own cron most SUCCESS rows
        # are skips, so name this case rather than letting it read as "no
        # healthy fresh samples" — the operator needs a run that executed.
        raise SubstrateError(
            "resource monitor witness was skipped, not run",
            data={
                "reason": "resource_monitor_witness_skipped",
                "workflow_id": replacement.get("workflow_uuid"),
                "skipped_runs": output.get("skipped_runs"),
            },
            fix_hint=(
                "that tick was skipped because the previous run was still "
                "active; pick a run that actually executed, or widen the "
                "monitor cron so runs stop overlapping"
            ),
        )
    reachable = output.get("reachable") if isinstance(output, Mapping) else None
    node_ids = output.get("node_ids") if isinstance(output, Mapping) else None
    if (
        not isinstance(output, Mapping)
        or output.get("ok") is not True
        or int(output.get("host_count") or 0) <= 0
        or not isinstance(reachable, Mapping)
        or not reachable
        or any(value is not True for value in reachable.values())
        or not isinstance(node_ids, Mapping)
        or set(node_ids) != set(reachable)
        or int(output.get("host_count") or 0) != len(node_ids)
        or any(not str(value or "").strip() for value in node_ids.values())
        or len(set(node_ids.values())) != len(node_ids)
    ):
        raise SubstrateError("resource monitor output has no healthy fresh samples")
    samples = _resource_monitor_samples(
        conn,
        deployment=deployment,
        failure_updated_at=failure_updated_at,
        recovery_updated_at=replacement.get("updated_at"),
        expected_node_ids={str(value) for value in node_ids.values()},
        lock=lock,
    )
    return {
        "kind": "resource_monitor",
        "replacement_workflow_id": replacement_id,
        "output_digest": _digest(dict(output)),
        "samples": samples,
        "deployment": deployment,
        "request_readiness_digest": _digest(dict(readiness)),
    }


def _git_reconcile_witness(
    conn: Any,
    *,
    deployment: str,
    failure: Mapping[str, Any],
    failure_updated_at: Any,
    witnesses: Sequence[Mapping[str, Any]],
    readiness: Mapping[str, Any],
    lock: bool,
) -> dict[str, Any]:
    responsibility = {
        "deployment": deployment,
        "sources": ["git-history"],
        "workflow_type": "okg_source_sync",
        "application_version": _current_application_version(conn, lock=lock),
        "run_mode": "scope_complete",
        "scope": [],
    }
    matching = [row for row in witnesses if row.get("kind") == "git_reconcile"]
    if len(matching) != 1:
        raise SubstrateError(
            "git reconcile acknowledgement requires exactly one witness"
        )
    witness = matching[0]
    wrapper_id = _required_text(witness, "replacement_workflow_id")
    wrapper = _one_workflow(conn, wrapper_id, lock=lock)
    wrapper_view = _workflow_row(wrapper)
    if (
        wrapper_view["name"] != failure.get("name")
        or wrapper_view["status"] != "SUCCESS"
    ):
        raise SubstrateError("git reconcile witness is not a successful wrapper")
    _require_matching_recovery_version(
        failure, wrapper_view, label="git reconcile wrapper"
    )
    _require_newer(
        failure_updated_at,
        wrapper.get("updated_at"),
        label="git reconcile wrapper",
    )
    output = _decoded_dbos_value(
        wrapper.get("output"),
        wrapper.get("serialization"),
        label="git reconcile output",
    )
    if (
        not isinstance(output, Mapping)
        or output.get("ok") is not True
        or output.get("source") != "git-history"
        or output.get("run_mode") != "scope_complete"
    ):
        raise SubstrateError("git reconcile wrapper output is not healthy")
    child_id = str(output.get("workflow_id") or "").strip()
    if not child_id:
        raise SubstrateError("git reconcile wrapper output has no child workflow")
    requested_child = str(witness.get("child_workflow_id") or "").strip()
    if requested_child and requested_child != child_id:
        raise SubstrateError("git reconcile child workflow does not match wrapper output")
    child = _one_workflow(conn, child_id, lock=lock)
    child_view = _workflow_row(child)
    if (
        child_view["status"] != "SUCCESS"
        or _responsibility(child_view) != responsibility
    ):
        raise SubstrateError(
            "git reconcile child source workflow is not a matching success"
        )
    _require_matching_recovery_version(
        failure, child_view, label="git reconcile child"
    )
    _require_newer(failure_updated_at, child.get("updated_at"), label="git reconcile")
    watermark = _source_watermark(
        conn,
        deployment=deployment,
        responsibility=responsibility,
        failure_updated_at=failure_updated_at,
        lock=lock,
    )
    generation = _published_generation_for_recovery(
        conn,
        deployment=deployment,
        expected_source="git-history",
        failure_updated_at=failure_updated_at,
        recovery=child,
        lock=lock,
    )
    live_convergence = _live_source_convergence(
        workflow_id=child_id,
        watermark=watermark,
        generation=generation,
    )
    return {
        "kind": "git_reconcile",
        "replacement_workflow_id": wrapper_id,
        "child_workflow_id": child_id,
        "wrapper_output_digest": _digest(dict(output)),
        "watermark": watermark,
        "published_generation": generation,
        "live_convergence": live_convergence,
        "request_readiness_digest": _digest(dict(readiness)),
    }


def _one_workflow(
    conn: Any,
    workflow_id: str,
    *,
    lock: bool,
) -> dict[str, Any]:
    rows = _workflow_rows(conn, [workflow_id], lock=lock)
    if workflow_id not in rows:
        raise SubstrateError(
            "runtime DLQ recovery witness workflow is absent",
            data={"workflow_id": workflow_id},
        )
    return rows[workflow_id]


def _workflow_event(
    conn: Any,
    workflow_id: str,
    key: str,
    *,
    lock: bool,
) -> Any:
    suffix = " FOR SHARE" if lock else ""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('dbos.workflow_events')")
        row = cur.fetchone()
        if row is None or row[0] is None:
            raise SubstrateError("DBOS workflow event authority is unavailable")
        cur.execute(
            "SELECT value, serialization FROM dbos.workflow_events "
            "WHERE workflow_uuid = %s AND key = %s" + suffix,
            (workflow_id, key),
        )
        row = cur.fetchone()
    if row is None:
        raise SubstrateError(
            "required DBOS workflow event is absent",
            data={"workflow_id": workflow_id, "key": key},
        )
    return _decoded_dbos_value(row[0], row[1], label=f"DBOS event {key}")


def _decoded_dbos_value(value: Any, serialization: Any, *, label: str) -> Any:
    from dbos._serialization import DBOSDefaultSerializer, deserialize_value

    try:
        return deserialize_value(
            value,
            str(serialization) if serialization is not None else None,
            DBOSDefaultSerializer,
        )
    except Exception as exc:  # noqa: BLE001
        raise SubstrateError(f"{label} could not be decoded") from exc


def _source_watermark(
    conn: Any,
    *,
    deployment: str,
    responsibility: Mapping[str, Any],
    failure_updated_at: Any,
    lock: bool,
) -> dict[str, Any]:
    sources = responsibility.get("sources")
    if not isinstance(sources, list) or len(sources) != 1:
        raise SubstrateError("source recovery witness requires one source")
    source = str(sources[0])
    run_mode = str(responsibility.get("run_mode") or "")
    suffix = " FOR SHARE" if lock else ""
    with conn.cursor() as cur:
        if run_mode in {"scope_complete", "reconcile"}:
            cur.execute(
                "SELECT last_reconcile_at FROM okg.source_watermarks "
                "WHERE source = %s" + suffix,
                (source,),
            )
            row = cur.fetchone()
            observed_at = row[0] if row else None
            field = "last_reconcile_at"
        else:
            cur.execute(
                "SELECT updated_at FROM okg.source_progress "
                "WHERE deployment = %s AND source_name = %s "
                "ORDER BY updated_at DESC LIMIT 1" + suffix,
                (deployment, source),
            )
            row = cur.fetchone()
            observed_at = row[0] if row else None
            field = "source_progress.updated_at"
    if observed_at is None or _as_datetime(observed_at) <= _epoch_datetime(
        failure_updated_at
    ):
        raise SubstrateError(
            "source recovery watermark did not move after the failure",
            data={"source": source, "field": field, "observed_at": observed_at},
        )
    return {
        "source": source,
        "field": field,
        "observed_at": _as_datetime(observed_at).isoformat(),
    }


def _published_generation_for_recovery(
    conn: Any,
    *,
    deployment: str,
    expected_source: str,
    failure_updated_at: Any,
    recovery: Mapping[str, Any],
    lock: bool,
) -> dict[str, Any]:
    output = _decoded_dbos_value(
        recovery.get("output"),
        recovery.get("serialization"),
        label="source recovery output",
    )
    if not isinstance(output, Mapping) or output.get("any_failed") is not False:
        raise SubstrateError("source recovery output is not fully successful")
    report = output.get("report")
    if not isinstance(report, Mapping):
        raise SubstrateError("source recovery output has no ingest report")
    results = report.get("results")
    if (
        not isinstance(results, list)
        or not results
        or any(
            not isinstance(result, Mapping)
            or str(result.get("status") or "").upper()
            not in {"OK", "SUCCESS", "PUBLISHED"}
            for result in results
        )
        or {
            str(result.get("source") or "")
            for result in results
            if isinstance(result, Mapping)
        }
        != {expected_source}
    ):
        raise SubstrateError("source recovery output has unsuccessful source results")
    publishes = report.get("publishes")
    if not isinstance(publishes, list):
        raise SubstrateError("source recovery output has no publish report")
    real_publishes = [
        publish
        for publish in publishes
        if isinstance(publish, Mapping)
        and publish.get("status") != "skipped"
        and publish.get("publish_skipped") is not True
    ]
    if (
        not real_publishes
        or any(publish.get("status") != "published" for publish in real_publishes)
    ):
        raise SubstrateError("source recovery output has no successful publish")
    generation_id = real_publishes[-1].get("generation_id") or real_publishes[
        -1
    ].get("target_generation_id")
    if generation_id is None:
        raise SubstrateError("source recovery publish has no generation id")
    lower = _epoch_datetime(failure_updated_at)
    upper = _epoch_datetime(recovery.get("updated_at"))
    suffix = " FOR SHARE" if lock else ""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT generation_id, published_at FROM okg.graph_generations "
            "WHERE generation_id::text = %s AND deployment_name = %s "
            "AND status = 'published' "
            "AND published_at > %s AND published_at <= %s "
            "LIMIT 1" + suffix,
            (str(generation_id), deployment, lower, upper),
        )
        row = cur.fetchone()
    if row is None:
        raise SubstrateError(
            "source recovery output generation is not a bound published generation"
        )
    return {
        "generation_id": str(row[0]),
        "published_at": _as_datetime(row[1]).isoformat(),
    }


def _resource_monitor_samples(
    conn: Any,
    *,
    deployment: str,
    failure_updated_at: Any,
    recovery_updated_at: Any,
    expected_node_ids: set[str],
    lock: bool,
) -> dict[str, Any]:
    lower = _epoch_datetime(failure_updated_at)
    upper = _epoch_datetime(recovery_updated_at)
    suffix = " FOR SHARE" if lock else ""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT node_id, value, status, observed_at "
            "FROM okg.node_series WHERE deployment_name = %s "
            "AND node_id = ANY(%s) "
            "AND metric = 'reachable' AND observed_at > %s "
            "AND observed_at <= %s ORDER BY node_id, observed_at DESC" + suffix,
            (deployment, sorted(expected_node_ids), lower, upper),
        )
        rows = cur.fetchall()
    latest: dict[str, tuple[Any, Any, Any]] = {}
    for row in rows:
        node_id = str(row[0])
        latest.setdefault(node_id, (row[1], row[2], row[3]))
    if (
        set(latest) != expected_node_ids
        or not latest
        or any(
            status != "ok" or value is None or float(value) < 1.0
            for value, status, _observed_at in latest.values()
        )
    ):
        raise SubstrateError(
            "resource monitor output is not backed by healthy fresh samples",
            data={
                "expected_node_ids": sorted(expected_node_ids),
                "observed_host_count": len(latest),
            },
        )
    observed = [_as_datetime(row[2]) for row in latest.values()]
    return {
        "host_count": len(latest),
        "node_ids": sorted(latest),
        "latest_observed_at": max(observed).isoformat(),
    }


def _live_source_convergence(
    *,
    workflow_id: str,
    watermark: Mapping[str, Any],
    generation: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema": "okg.runtime_dlq.live_source_convergence.v1",
        "state": "ready",
        "authority": [
            "dbos.workflow_status.output",
            "okg.source_progress_or_watermark",
            "okg.graph_generations",
        ],
        "workflow_id": workflow_id,
        "watermark_digest": _digest(dict(watermark)),
        "generation_id": str(generation["generation_id"]),
    }


def _require_matching_recovery_version(
    failure: Mapping[str, Any],
    recovery: Mapping[str, Any],
    *,
    label: str,
) -> None:
    if (
        not failure.get("application_version")
        or recovery.get("application_version") != failure.get("application_version")
    ):
        raise SubstrateError(
            f"{label} recovery witness application version does not match"
        )


def _require_newer(before: Any, after: Any, *, label: str) -> None:
    if _epoch_datetime(after) <= _epoch_datetime(before):
        raise SubstrateError(f"{label} recovery witness is not newer than the failure")


def _epoch_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _as_datetime(value)
    try:
        raw = float(value)
    except (TypeError, ValueError) as exc:
        raise SubstrateError("workflow timestamp is unavailable") from exc
    if raw > 10_000_000_000:
        raw /= 1000.0
    return datetime.fromtimestamp(raw, tz=timezone.utc)


def _as_datetime(value: datetime) -> datetime:
    return (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )


def _required_text(value: Mapping[str, Any], key: str) -> str:
    result = str(value.get(key) or "").strip()
    if not result:
        raise SubstrateError(f"runtime DLQ acknowledgement requires {key}")
    return result


__all__ = [
    "ACKNOWLEDGEMENT_PLAN_SCHEMA",
    "ACKNOWLEDGEMENT_REQUEST_SCHEMA",
    "ACKNOWLEDGEMENT_RESULT_SCHEMA",
    "apply_runtime_dlq_acknowledgements",
    "load_acknowledgement_request",
    "plan_runtime_dlq_acknowledgements",
]
