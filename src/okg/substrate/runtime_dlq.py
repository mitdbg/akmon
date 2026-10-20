"""Current DBOS failure authority with explicit source-sync supersession."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from okg.substrate.source_sync_enqueue import (
    source_sync_payload_from_dbos_inputs,
)

_FAILURE_STATUSES = {
    "ERROR",
    "FAILED",
    "MAX_RECOVERY_ATTEMPTS",
    "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
}
_MAX_SUPERSESSION_ROWS = 20_000
_MAX_FAILURE_ROWS = 20_000
# The SQL form of `_FAILURE_STATUSES`, built from it so the filter the census
# counts with and the set the acknowledgement path checks cannot drift apart.
_FAILURE_STATUS_SQL = "status IN ({})".format(
    ", ".join(f"'{status}'" for status in sorted(_FAILURE_STATUSES))
)


class RuntimeDLQAuthorityUnavailable(RuntimeError):
    """The retained DBOS history cannot be reduced within the read-side bound."""


def _is_statement_timeout(exc: BaseException) -> bool:
    """True when PostgreSQL cancelled the statement at its timeout.

    Matched on SQLSTATE so it holds whether the driver raises
    ``QueryCanceled`` or a bare ``OperationalError``.
    """
    sqlstate = getattr(exc, "sqlstate", None) or getattr(
        getattr(exc, "diag", None), "sqlstate", None
    )
    return sqlstate in {"57014", "57P01"}


def probe_current_runtime_dlq(
    cur: Any,
    *,
    ignored_workflow_names: set[str] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Read the current DLQ, or say plainly that it was not read.

    Returns ``(result, unknown)`` with exactly one side populated. The scans
    in :func:`current_runtime_dlq` cap how many ROWS they reduce; a row cap
    does not bound execution TIME. Under a caller-set ``statement_timeout``
    an over-large retained history is cancelled, and that cancellation is a
    measurement gap, not a dead-letter finding: on 2026-08-15 it took the
    whole dogfood status surface down with it (issue #1290).

    The caller sets the bound (session ``statement_timeout``); this function
    only classifies the outcome, so there is one place that decides what a
    cancelled DLQ scan MEANS.
    """
    try:
        return current_runtime_dlq(
            cur, ignored_workflow_names=ignored_workflow_names,
        ), None
    except Exception as exc:  # noqa: BLE001 - classification, then re-raise
        if not _is_statement_timeout(exc):
            raise
        return None, {
            "schema": "okg.runtime_dlq_readiness.v1",
            "ok": None,
            "status": "unknown",
            "reason": "readiness_probe_timeout",
            "probe_id": "current_runtime_dlq",
            "error": type(exc).__name__,
            "message": str(exc),
            "detail": (
                "the retained DBOS failure history exceeded the probe's "
                "statement timeout; the current dead-letter count was not "
                "measured. A row-count bound does not bound execution time."
            ),
        }


def current_runtime_dlq(
    cur: Any,
    *,
    ignored_workflow_names: set[str] | None = None,
) -> dict[str, Any]:
    """Return current, recoverable DBOS failures without deleting history.

    DBOS retains terminal rows indefinitely. A failure is current when its
    application version is current (or cannot be classified) and, for
    ``okg_source_sync``, no later successful workflow explicitly supersedes the
    exact failed responsibility. ``CANCELLED`` rows are worker hygiene, not
    dead letters. Workflows delegated to another health gate are reported but
    excluded from this gate's failure count.
    """
    ignored = ignored_workflow_names or set()
    cur.execute("SELECT to_regclass('dbos.workflow_status')")
    row = cur.fetchone()
    if row is None or row[0] is None:
        return {
            "current_count": 0,
            "raw_terminal_count": 0,
            "terminal_current_version_count": 0,
            "historical_count": 0,
            "superseded_count": 0,
            "ignored_count": 0,
            "ignored_workflow_names": sorted(ignored),
            "current_application_version": None,
            "current_failures": [],
            "acknowledged_count": 0,
            "acknowledged_failures": [],
            "invalid_acknowledgements": [],
            "acknowledgement_authority": {
                "available": False,
                "mode": "not_applicable",
                "reason": "DBOS workflow history is unavailable",
            },
        }

    columns = _table_columns(cur, "workflow_status")
    failure_order_by = (
        "updated_at DESC NULLS LAST, workflow_uuid DESC"
        if "updated_at" in columns
        else "workflow_uuid DESC"
    )
    current_version = _current_application_version(cur)
    # Issue #2202. Failures of an ignored workflow and failures from another
    # application version reach the result only as COUNTS, so SQL counts
    # them: a GROUP BY over (name, version) inside the same 20,001-row bound
    # the full read used. Only the rows whose content IS served -- current
    # version, not ignored -- are fetched with their inputs and decoded.
    # Readiness ignores `okg_source_sync`; on okg-workspace this took its
    # dead-letter check from 12,605 decoded rows to none.
    cur.execute(
        "SELECT name, application_version, count(*) AS row_count "
        "FROM (SELECT name, "
        f"{_optional_column(columns, 'application_version')} AS application_version "
        "FROM dbos.workflow_status "
        f"WHERE {_FAILURE_STATUS_SQL} "
        f"LIMIT {_MAX_FAILURE_ROWS + 1}) AS bounded "
        "GROUP BY name, application_version"
    )
    census = [_census_row(raw) for raw in cur.fetchall()]
    raw_terminal_count = sum(count for _name, _version, count in census)
    if raw_terminal_count > _MAX_FAILURE_ROWS:
        raise RuntimeDLQAuthorityUnavailable(
            "terminal failure history exceeds the read-side bound"
        )
    counted_historical = 0
    counted_ignored = 0
    historical_versions: set[str] = set()
    fetch_needed = False
    for name, raw_version, count in census:
        version = _normalized_version(raw_version)
        if _is_other_version(version, current_version):
            counted_historical += count
            historical_versions.add(str(raw_version))
        elif name in ignored:
            counted_ignored += count
        else:
            fetch_needed = True
    failure_rows: list[Any] = []
    if fetch_needed:
        predicates = [_FAILURE_STATUS_SQL, "COALESCE(name, '') <> ALL(%s)"]
        params: list[Any] = [sorted(ignored)]
        if historical_versions:
            predicates.append(
                "(application_version IS NULL "
                "OR application_version <> ALL(%s))"
            )
            params.append(sorted(historical_versions))
        cur.execute(
            "SELECT workflow_uuid, name, status, "
            f"{_optional_column(columns, 'application_version')} AS application_version, "
            f"{_optional_column(columns, 'queue_name')} AS queue_name, "
            f"{_optional_column(columns, 'inputs')} AS inputs, "
            f"{_optional_column(columns, 'serialization')} AS serialization, "
            f"{_optional_column(columns, 'error')} AS error, "
            f"{_optional_column(columns, 'output')} AS output, "
            f"{_optional_column(columns, 'updated_at')} AS updated_at "
            "FROM dbos.workflow_status "
            f"WHERE {' AND '.join(predicates)} "
            f"ORDER BY {failure_order_by} "
            f"LIMIT {_MAX_FAILURE_ROWS + 1}",
            tuple(params),
        )
        failure_rows = cur.fetchall()
        if len(failure_rows) > _MAX_FAILURE_ROWS:
            raise RuntimeDLQAuthorityUnavailable(
                "terminal failure history exceeds the read-side bound"
            )
    failures = [_workflow_row(raw) for raw in failure_rows]
    source_failure_ids = {
        failure["workflow_id"]
        for failure in failures
        if failure["name"] == "okg_source_sync"
        and failure["name"] not in ignored
        and (
            not current_version
            or failure["application_version"] is None
            or failure["application_version"] == current_version
        )
    }
    successes_by_failure: dict[str, list[dict[str, Any]]] = {}
    if source_failure_ids:
        order_by = (
            "updated_at DESC NULLS LAST, workflow_uuid DESC"
            if "updated_at" in columns
            else "workflow_uuid DESC"
        )
        cur.execute(
            "SELECT workflow_uuid, name, status, "
            f"{_optional_column(columns, 'application_version')} AS application_version, "
            f"{_optional_column(columns, 'queue_name')} AS queue_name, "
            f"{_optional_column(columns, 'inputs')} AS inputs, "
            f"{_optional_column(columns, 'serialization')} AS serialization, "
            "NULL::text AS error, NULL::text AS output, "
            f"{_optional_column(columns, 'updated_at')} AS updated_at "
            "FROM dbos.workflow_status "
            "WHERE status = 'SUCCESS' AND name = 'okg_source_sync' "
            f"ORDER BY {order_by} "
            f"LIMIT {_MAX_SUPERSESSION_ROWS + 1}"
        )
        success_rows = cur.fetchall()
        if len(success_rows) > _MAX_SUPERSESSION_ROWS:
            raise RuntimeDLQAuthorityUnavailable(
                "source-sync supersession history exceeds the read-side bound"
            )
        for raw in success_rows:
            success = _workflow_row(raw)
            superseded_id = _supersedes_workflow_id(success)
            if superseded_id in source_failure_ids:
                successes_by_failure.setdefault(superseded_id, []).append(success)

    acknowledgements, acknowledgement_authority = _acknowledgements(
        cur,
        workflow_ids={
            failure["workflow_id"]
            for failure in failures
            if not current_version
            or failure["application_version"] is None
            or failure["application_version"] == current_version
        },
    )
    current: list[dict[str, Any]] = []
    # The census already counted every row that was not fetched.
    historical = counted_historical
    superseded = 0
    terminal_current_version = counted_ignored
    ignored_count = counted_ignored
    acknowledged: list[dict[str, Any]] = []
    invalid_acknowledgements: list[dict[str, Any]] = []
    for failure in failures:
        if (
            current_version
            and failure["application_version"] is not None
            and failure["application_version"] != current_version
        ):
            historical += 1
            continue
        terminal_current_version += 1
        if failure["name"] in ignored:
            ignored_count += 1
            continue
        if failure["name"] == "okg_source_sync" and any(
            _explicitly_supersedes(success, failure)
            for success in successes_by_failure.get(failure["workflow_id"], ())
        ):
            historical += 1
            superseded += 1
            continue
        failure_view = {
            "workflow_id": failure["workflow_id"],
            "workflow_name": failure["name"],
            "status": failure["status"],
            "application_version": failure["application_version"],
            "responsibility": _responsibility(failure),
        }
        matching = acknowledgements.get(failure["workflow_id"], [])
        valid_acknowledgement = _valid_acknowledgement(failure, matching)
        if valid_acknowledgement is not None:
            acknowledged.append({
                **failure_view,
                "acknowledgement": valid_acknowledgement,
            })
            continue
        if matching:
            invalid_acknowledgements.append({
                **failure_view,
                "reason": (
                    "ambiguous acknowledgement authority"
                    if len(matching) != 1
                    else "acknowledgement does not match the live failure snapshot"
                ),
                "acknowledgement_count": len(matching),
            })
        current.append(failure_view)
    return {
        "current_count": len(current),
        # Every row the census counted carries a failure status: the SQL
        # filter is exactly `_FAILURE_STATUSES`.
        "raw_terminal_count": raw_terminal_count,
        "terminal_current_version_count": terminal_current_version,
        "historical_count": historical,
        "superseded_count": superseded,
        "ignored_count": ignored_count,
        "ignored_workflow_names": sorted(ignored),
        "current_application_version": current_version,
        "current_failures": current,
        "acknowledged_count": len(acknowledged),
        "acknowledged_failures": acknowledged,
        "invalid_acknowledgements": invalid_acknowledgements,
        "acknowledgement_authority": acknowledgement_authority,
    }


def _census_row(raw: Any) -> tuple[str, Any, int]:
    """One (name, raw application_version, count) group from the census."""
    if isinstance(raw, Mapping):
        name, version, count = (
            raw.get("name"), raw.get("application_version"), raw.get("row_count")
        )
    else:
        name, version, count = raw
    return str(name or ""), version, int(count or 0)


def _normalized_version(raw: Any) -> str | None:
    """The version exactly as `_workflow_row` normalizes it: blank is None."""
    return (str(raw).strip() or None) if raw is not None else None


def _is_other_version(version: str | None, current_version: str | None) -> bool:
    """True for a failure from a known, different application version.

    Unknown (NULL or blank) versions fail closed as current, as before.
    """
    return bool(current_version) and version is not None and version != current_version


def _workflow_row(raw: Any) -> dict[str, Any]:
    names = (
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
    values = (
        dict(raw)
        if isinstance(raw, Mapping)
        else dict(zip(names, raw, strict=True))
    )
    queue_name = str(values.get("queue_name") or "")
    deployment = _deployment_from_queue(queue_name)
    inputs = values.get("inputs")
    payload = None
    if (
        str(values.get("name") or "") == "okg_source_sync"
        and isinstance(inputs, str)
    ):
        payload = source_sync_payload_from_dbos_inputs(
            deployment=deployment,
            inputs=inputs,
            serialization=(
                str(values["serialization"])
                if values.get("serialization") is not None
                else None
            ),
        )
    application_version = (
        str(values["application_version"]).strip()
        if values.get("application_version") is not None
        else ""
    )
    return {
        "workflow_id": str(values.get("workflow_uuid") or ""),
        "name": str(values.get("name") or ""),
        "status": str(values.get("status") or "").upper(),
        "application_version": application_version or None,
        "queue_name": queue_name,
        "inputs": inputs,
        "serialization": values.get("serialization"),
        "args": _workflow_args(inputs),
        "payload": payload,
        "error": values.get("error"),
        "output": values.get("output"),
        "updated_at": values.get("updated_at"),
    }


def failure_snapshot_digest(row: Mapping[str, Any]) -> str:
    """Bind an acknowledgement to the exact durable DBOS failure state."""
    return _digest({
        "workflow_id": row.get("workflow_id"),
        "workflow_name": row.get("name"),
        "status": row.get("status"),
        "application_version": row.get("application_version"),
        "queue_name": row.get("queue_name"),
        "inputs_digest": _digest(row.get("inputs")),
        "serialization": row.get("serialization"),
        "responsibility": _responsibility(row),
        "error_digest": _digest(row.get("error")),
        "output_digest": _digest(row.get("output")),
        "updated_at": row.get("updated_at"),
    })


def _digest(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _acknowledgements(
    cur: Any,
    *,
    workflow_ids: set[str],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    cur.execute("SELECT to_regclass('okg.runtime_dlq_acknowledgement_events')")
    row = cur.fetchone()
    if row is None or row[0] is None:
        return {}, {
            "available": False,
            "mode": "dbos_history_only",
            "reason": "acknowledgement table is unavailable",
        }
    if not workflow_ids:
        return {}, {
            "available": True,
            "mode": "append_only_exact_failure_snapshot",
        }
    cur.execute(
        "SELECT deployment_name, workflow_id, workflow_name, "
        "application_version, failure_snapshot_digest, error_digest, "
        "responsibility, actor, reason, candidate_revision, "
        "operation_receipt_digest, witness, plan_payload, plan_id, acknowledged_at "
        "FROM okg.runtime_dlq_acknowledgement_events "
        "WHERE workflow_id = ANY(%s)",
        (sorted(workflow_ids),),
    )
    names = (
        "deployment_name",
        "workflow_id",
        "workflow_name",
        "application_version",
        "failure_snapshot_digest",
        "error_digest",
        "responsibility",
        "actor",
        "reason",
        "candidate_revision",
        "operation_receipt_digest",
        "witness",
        "plan_payload",
        "plan_id",
        "acknowledged_at",
    )
    result: dict[str, list[dict[str, Any]]] = {}
    for raw in cur.fetchall():
        value = dict(raw) if isinstance(raw, Mapping) else dict(zip(names, raw, strict=True))
        result.setdefault(str(value["workflow_id"]), []).append(value)
    return result, {
        "available": True,
        "mode": "append_only_exact_failure_snapshot",
    }


def _valid_acknowledgement(
    failure: Mapping[str, Any],
    acknowledgements: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if len(acknowledgements) != 1:
        return None
    acknowledgement = acknowledgements[0]
    responsibility = _json_mapping(acknowledgement.get("responsibility"))
    witness = _json_mapping(acknowledgement.get("witness"))
    plan = _json_mapping(acknowledgement.get("plan_payload"))
    if responsibility is None or witness is None or plan is None:
        return None
    deployment = str(acknowledgement.get("deployment_name") or "").strip()
    failure_deployment = str(_responsibility(failure).get("deployment") or "").strip()
    candidate_revision = str(
        acknowledgement.get("candidate_revision") or ""
    ).strip()
    operation_receipt_digest = acknowledgement.get("operation_receipt_digest")
    actor = str(acknowledgement.get("actor") or "").strip()
    reason = str(acknowledgement.get("reason") or "").strip()
    plan_id = acknowledgement.get("plan_id")
    if (
        set(plan) != {
            "schema",
            "deployment",
            "actor",
            "reason",
            "candidate_revision",
            "operation_receipt_digest",
            "runtime_fence",
            "current_application_version",
            "entries",
        }
        or plan.get("schema") != "okg.runtime_dlq_acknowledgement_plan.v1"
        or plan_id != _digest(plan)
        or not _sha256_digest(operation_receipt_digest)
        or plan.get("deployment") != deployment
        or deployment != failure_deployment
        or plan.get("actor") != actor
        or plan.get("reason") != reason
        or plan.get("candidate_revision") != candidate_revision
        or plan.get("operation_receipt_digest") != operation_receipt_digest
        or plan.get("current_application_version")
        != failure.get("application_version")
        or not deployment
        or not candidate_revision
        or not actor
        or not reason
    ):
        return None
    fence = _json_mapping(plan.get("runtime_fence"))
    if (
        fence is None
        or set(fence) != {"promotion_id", "status", "candidate_revision"}
        or not str(fence.get("promotion_id") or "").strip()
        or fence.get("status") not in {"fenced", "recovery_blocked"}
        or fence.get("candidate_revision") != candidate_revision
    ):
        return None
    entries = plan.get("entries")
    if not isinstance(entries, list) or not entries:
        return None
    if any(
        not isinstance(entry, Mapping)
        or set(entry) != {
            "workflow_id",
            "workflow_name",
            "application_version",
            "failure_snapshot_digest",
            "error_digest",
            "responsibility",
            "witness",
        }
        or not str(entry.get("workflow_id") or "").strip()
        for entry in entries
    ) or len({str(entry["workflow_id"]) for entry in entries}) != len(entries):
        return None
    workflow_id = str(failure.get("workflow_id") or "")
    matching_entries = [
        entry for entry in entries if entry.get("workflow_id") == workflow_id
    ]
    if len(matching_entries) != 1:
        return None
    entry = matching_entries[0]
    expected = (
        acknowledgement.get("workflow_id") == workflow_id
        and acknowledgement.get("workflow_name") == failure.get("name")
        and acknowledgement.get("application_version")
        == failure.get("application_version")
        and acknowledgement.get("failure_snapshot_digest")
        == failure_snapshot_digest(failure)
        and acknowledgement.get("error_digest") == _digest(failure.get("error"))
        and responsibility == _responsibility(failure)
        and entry.get("workflow_name") == acknowledgement.get("workflow_name")
        and entry.get("application_version")
        == acknowledgement.get("application_version")
        and entry.get("failure_snapshot_digest")
        == acknowledgement.get("failure_snapshot_digest")
        and entry.get("error_digest") == acknowledgement.get("error_digest")
        and entry.get("responsibility") == responsibility
        and entry.get("witness") == witness
        and _valid_witness_shape(
            witness,
            workflow_name=str(failure.get("name") or ""),
            responsibility=responsibility,
            deployment=deployment,
        )
    )
    if not expected:
        return None
    return {
        "deployment": acknowledgement.get("deployment_name"),
        "actor": acknowledgement.get("actor"),
        "reason": acknowledgement.get("reason"),
        "candidate_revision": acknowledgement.get("candidate_revision"),
        "operation_receipt_digest": operation_receipt_digest,
        "witness": witness,
        "plan_id": acknowledgement.get("plan_id"),
        "acknowledged_at": acknowledgement.get("acknowledged_at"),
    }


def _json_mapping(value: Any) -> dict[str, Any] | None:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return None
    return dict(value) if isinstance(value, Mapping) else None


def _sha256_digest(value: Any) -> bool:
    raw = str(value or "")
    return (
        len(raw) == 71
        and raw.startswith("sha256:")
        and all(char in "0123456789abcdef" for char in raw[7:])
    )


def _valid_witness_shape(
    witness: Mapping[str, Any],
    *,
    workflow_name: str,
    responsibility: Mapping[str, Any],
    deployment: str,
) -> bool:
    digest_keys = {"request_readiness_digest"}
    if workflow_name == "okg_source_sync":
        expected_keys = {
            "kind",
            "replacement_workflow_id",
            "responsibility",
            "watermark",
            "published_generation",
            "live_convergence",
            "request_readiness_digest",
        }
        if (
            witness.get("kind") != "source_sync"
            or witness.get("responsibility") != responsibility
            or not _valid_source_witness_parts(
                witness,
                responsibility,
                expected_workflow_id=str(
                    witness.get("replacement_workflow_id") or ""
                ),
            )
        ):
            return False
    elif workflow_name == "doctor_workflow":
        expected_keys = {
            "kind",
            "replacement_workflow_id",
            "verdict_digest",
            "request_readiness_digest",
        }
        digest_keys.add("verdict_digest")
        if witness.get("kind") != "doctor":
            return False
    elif workflow_name == "okg_ci_resource_monitor":
        expected_keys = {
            "kind",
            "replacement_workflow_id",
            "output_digest",
            "samples",
            "deployment",
            "request_readiness_digest",
        }
        digest_keys.add("output_digest")
        if (
            witness.get("kind") != "resource_monitor"
            or witness.get("deployment") != deployment
            or not _valid_resource_samples(witness.get("samples"))
        ):
            return False
    elif workflow_name == "okg_ci_git_history_reconcile":
        expected_keys = {
            "kind",
            "replacement_workflow_id",
            "child_workflow_id",
            "wrapper_output_digest",
            "watermark",
            "published_generation",
            "live_convergence",
            "request_readiness_digest",
        }
        digest_keys.add("wrapper_output_digest")
        git_responsibility = {
            "deployment": deployment,
            "sources": ["git-history"],
            "workflow_type": "okg_source_sync",
            "application_version": responsibility.get("application_version"),
            "run_mode": "scope_complete",
            "scope": [],
        }
        if (
            witness.get("kind") != "git_reconcile"
            or not str(witness.get("child_workflow_id") or "").strip()
            or not _valid_source_witness_parts(
                witness,
                git_responsibility,
                expected_workflow_id=str(
                    witness.get("child_workflow_id") or ""
                ),
            )
        ):
            return False
    else:
        return False
    return (
        set(witness) == expected_keys
        and bool(str(witness.get("replacement_workflow_id") or "").strip())
        and all(_sha256_digest(witness.get(key)) for key in digest_keys)
    )


def _valid_resource_samples(value: Any) -> bool:
    samples = _json_mapping(value)
    if samples is None or set(samples) != {
        "host_count",
        "node_ids",
        "latest_observed_at",
    }:
        return False
    node_ids = samples.get("node_ids")
    return (
        isinstance(samples.get("host_count"), int)
        and samples["host_count"] > 0
        and isinstance(node_ids, list)
        and len(node_ids) == samples["host_count"]
        and len(set(node_ids)) == len(node_ids)
        and all(isinstance(node_id, str) and node_id for node_id in node_ids)
        and bool(str(samples.get("latest_observed_at") or "").strip())
    )


def _valid_source_witness_parts(
    witness: Mapping[str, Any],
    responsibility: Mapping[str, Any],
    *,
    expected_workflow_id: str,
) -> bool:
    watermark = _json_mapping(witness.get("watermark"))
    generation = _json_mapping(witness.get("published_generation"))
    convergence = _json_mapping(witness.get("live_convergence"))
    sources = responsibility.get("sources")
    return (
        isinstance(sources, list)
        and len(sources) == 1
        and watermark is not None
        and set(watermark) == {"source", "field", "observed_at"}
        and watermark.get("source") == sources[0]
        and watermark.get("field")
        in {"last_reconcile_at", "source_progress.updated_at"}
        and bool(str(watermark.get("observed_at") or "").strip())
        and generation is not None
        and set(generation) == {"generation_id", "published_at"}
        and bool(str(generation.get("generation_id") or "").strip())
        and bool(str(generation.get("published_at") or "").strip())
        and convergence is not None
        and set(convergence)
        == {
            "schema",
            "state",
            "authority",
            "workflow_id",
            "watermark_digest",
            "generation_id",
        }
        and convergence.get("schema")
        == "okg.runtime_dlq.live_source_convergence.v1"
        and convergence.get("state") == "ready"
        and convergence.get("authority")
        == [
            "dbos.workflow_status.output",
            "okg.source_progress_or_watermark",
            "okg.graph_generations",
        ]
        and convergence.get("workflow_id") == expected_workflow_id
        and convergence.get("watermark_digest") == _digest(watermark)
        and convergence.get("generation_id")
        == str(generation.get("generation_id"))
    )


def _workflow_args(raw: Any) -> list[Any]:
    value = raw
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return []
    if isinstance(value, Mapping):
        value = value.get("args", value.get("inputs", []))
    return list(value) if isinstance(value, (list, tuple)) else []


def _responsibility(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = row.get("payload")
    if isinstance(payload, Mapping):
        sources = payload.get("sources")
        events = payload.get("events")
        run_mode = payload.get("run_mode")
        deployment = str(
            payload.get("deployment")
            or _deployment_from_queue(str(row.get("queue_name") or ""))
        )
    else:
        args = list(row.get("args") or ())
        sources = args[0] if len(args) > 0 else ()
        run_mode = args[1] if len(args) > 1 else ""
        events = args[4] if len(args) > 4 else ()
        deployment = _deployment_from_queue(str(row.get("queue_name") or ""))

    source_names = (
        sources if isinstance(sources, (list, tuple)) else ()
    )
    event_rows = events if isinstance(events, (list, tuple)) else ()
    scopes = sorted({
        str(event.get("scope_key"))
        for event in event_rows
        if isinstance(event, Mapping) and event.get("scope_key")
    })
    return {
        "deployment": deployment,
        "sources": sorted(str(source) for source in source_names),
        "workflow_type": str(row.get("name") or ""),
        "application_version": row.get("application_version"),
        "run_mode": str(run_mode or ""),
        "scope": scopes,
    }


def _explicitly_supersedes(
    success: Mapping[str, Any],
    failure: Mapping[str, Any],
) -> bool:
    supersedes = _supersedes_workflow_id(success)
    return (
        supersedes == failure.get("workflow_id")
        and _responsibility(success) == _responsibility(failure)
    )


def _supersedes_workflow_id(success: Mapping[str, Any]) -> str:
    payload = success.get("payload")
    if isinstance(payload, Mapping):
        return str(payload.get("supersedes_workflow_id") or "")
    args = list(success.get("args") or ())
    return str(args[9] or "") if len(args) > 9 else ""


def _current_application_version(cur: Any) -> str | None:
    cur.execute("SELECT to_regclass('dbos.application_versions')")
    row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    columns = _table_columns(cur, "application_versions")
    if "version_name" not in columns:
        return None
    order_columns = [
        column
        for column in ("version_timestamp", "created_at")
        if column in columns
    ]
    order_by = (
        ", ".join(f"{column} DESC NULLS LAST" for column in order_columns)
        if order_columns
        else "version_name DESC"
    )
    cur.execute(
        "SELECT version_name FROM dbos.application_versions "
        f"ORDER BY {order_by} LIMIT 1"
    )
    row = cur.fetchone()
    return str(row[0]) if row and row[0] is not None else None


def _table_columns(cur: Any, table_name: str) -> set[str]:
    if table_name not in {"workflow_status", "application_versions"}:
        raise ValueError(f"unsupported DBOS table: {table_name}")
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'dbos' "
        f"AND table_name = '{table_name}'"
    )
    return {
        str(row.get("column_name") if isinstance(row, Mapping) else row[0])
        for row in cur.fetchall()
    }


def _optional_column(columns: set[str], name: str) -> str:
    return name if name in columns else "NULL::text"


def _deployment_from_queue(queue_name: str) -> str:
    return queue_name.removeprefix("okg-").removesuffix("-source-sync")
