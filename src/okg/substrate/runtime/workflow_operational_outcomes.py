"""Typed, queryable outcomes for participant DBOS workflows.

DBOS intentionally serializes ``workflow_status.output`` with its pickle
serializer.  Operational health therefore must not inspect that column.  The
embed and source-sync participants use the transactions defined here to write
an attributable registration and one replay-stable outcome row instead.

The reader is deliberately fail-closed: once the outcome migration is applied
every successful participant execution needs a valid registration and outcome
with fresh timestamps.  A malformed, stale, or unattributable row is unknown,
which is unhealthy evidence.
"""
from __future__ import annotations

import json
import math
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from okg.substrate.db.connection import connect as db_connect
from psycopg.rows import dict_row
from sqlalchemy import text

from okg.substrate.runtime.partition_queue import (
    partition_queue_name,
    source_sync_queue_name,
)
from okg.substrate.runtime.source_sync_constants import SOURCE_SYNC_WORKFLOW_NAME

CONTRACT_VERSION = 1
MIGRATION_ID = "0029_workflow_operational_outcomes"
DEFAULT_LOOKBACK_SECONDS = 24 * 60 * 60
OUTCOMES = frozenset({"clean", "partial_failure", "retained_failure", "skipped"})
EMBED_WORKFLOW_NAME = "okg_embed_batch"


class WorkflowOperationalEvidenceError(RuntimeError):
    """The operational-evidence protocol was violated."""


class WorkflowOperationalEvidenceConflict(WorkflowOperationalEvidenceError):
    """A replay attempted to alter immutable semantic evidence."""


@dataclass(frozen=True)
class OperationalParticipant:
    deployment_name: str
    workflow_name: str
    queue_name: str


@dataclass(frozen=True)
class OperationalTransactions:
    register_execution_tx: Callable[[], dict[str, Any]]
    record_outcome_tx: Callable[[Mapping[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class WorkflowOperationalOutcomeVerdict:
    deployment: str
    ok: bool
    code: str
    message: str
    cutover_epoch_ms: int | None
    checked_count: int
    partial_count: int
    retained_count: int
    unknown_count: int
    global_unknown_count: int
    legacy_count: int
    samples: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "deployment": self.deployment,
            "cutover_epoch_ms": self.cutover_epoch_ms,
            "checked_count": self.checked_count,
            "partial_count": self.partial_count,
            "retained_count": self.retained_count,
            "unknown_count": self.unknown_count,
            "global_unknown_count": self.global_unknown_count,
            "legacy_count": self.legacy_count,
            "samples": [dict(sample) for sample in self.samples],
        }


def participant_for(*, deployment: str, workflow_name: str) -> OperationalParticipant:
    if workflow_name == EMBED_WORKFLOW_NAME:
        queue_name = partition_queue_name(deployment)
    elif workflow_name == SOURCE_SYNC_WORKFLOW_NAME:
        queue_name = source_sync_queue_name(deployment)
    else:
        raise WorkflowOperationalEvidenceError(
            f"workflow {workflow_name!r} is not an operational participant"
        )
    return OperationalParticipant(deployment, workflow_name, queue_name)


def operational_participants(deployment: str) -> tuple[OperationalParticipant, ...]:
    return tuple(
        participant_for(deployment=deployment, workflow_name=name)
        for name in (EMBED_WORKFLOW_NAME, SOURCE_SYNC_WORKFLOW_NAME)
    )


def registration_transaction_name(workflow_name: str) -> str:
    return f"{workflow_name}_register_operational_execution"


def outcome_transaction_name(workflow_name: str) -> str:
    return f"{workflow_name}_record_operational_outcome"


def define_operational_transactions(
    *, participant: OperationalParticipant,
) -> OperationalTransactions:
    """Define the two application transactions for one participant.

    DBOS uses the same application database as the system database in OKG;
    consequently each insert is committed with its transaction checkpoint and
    is replayed from that checkpoint rather than executed twice.
    """
    from dbos import DBOS

    @DBOS.transaction(name=registration_transaction_name(participant.workflow_name))
    def register_execution_tx() -> dict[str, Any]:
        workflow_uuid = _current_workflow_uuid()
        session = DBOS.sql_session
        status = (
            session.execute(
                text(
                    "SELECT name, queue_name, created_at "
                    "FROM dbos.workflow_status WHERE workflow_uuid = :id"
                ),
                {"id": workflow_uuid},
            )
            .mappings()
            .one_or_none()
        )
        if status is None:
            raise WorkflowOperationalEvidenceError(
                f"DBOS workflow row missing for {workflow_uuid!r}"
            )
        if status["name"] != participant.workflow_name:
            raise WorkflowOperationalEvidenceError("workflow-name registration mismatch")
        if status["queue_name"] != participant.queue_name:
            raise WorkflowOperationalEvidenceError("queue registration mismatch")
        expected = {
            "workflow_uuid": workflow_uuid,
            "deployment_name": participant.deployment_name,
            "workflow_name": participant.workflow_name,
            "queue_name": participant.queue_name,
            "contract_version": CONTRACT_VERSION,
        }
        session.execute(
            text(
                "INSERT INTO okg.workflow_operational_registrations "
                "(workflow_uuid, deployment_name, workflow_name, queue_name, contract_version) "
                "VALUES (:workflow_uuid, :deployment_name, :workflow_name, :queue_name, :contract_version) "
                "ON CONFLICT (workflow_uuid) DO NOTHING"
            ),
            expected,
        )
        actual = dict(
            session.execute(
                text(
                    "SELECT workflow_uuid, deployment_name, workflow_name, queue_name, contract_version "
                    "FROM okg.workflow_operational_registrations WHERE workflow_uuid = :id"
                ),
                {"id": workflow_uuid},
            )
            .mappings()
            .one()
        )
        if actual != expected:
            raise WorkflowOperationalEvidenceConflict(
                f"registration replay conflict for {workflow_uuid!r}"
            )
        return expected

    @DBOS.transaction(name=outcome_transaction_name(participant.workflow_name))
    def record_outcome_tx(raw_outcome: Mapping[str, Any]) -> dict[str, Any]:
        workflow_uuid = _current_workflow_uuid()
        canonical = normalize_outcome(raw_outcome)
        session = DBOS.sql_session
        expected_registration = {
            "deployment_name": participant.deployment_name,
            "workflow_name": participant.workflow_name,
            "queue_name": participant.queue_name,
            "contract_version": CONTRACT_VERSION,
        }
        registration = (
            session.execute(
                text(
                    "SELECT deployment_name, workflow_name, queue_name, contract_version "
                    "FROM okg.workflow_operational_registrations WHERE workflow_uuid = :id"
                ),
                {"id": workflow_uuid},
            )
            .mappings()
            .one_or_none()
        )
        if registration is None or dict(registration) != expected_registration:
            raise WorkflowOperationalEvidenceError(
                f"outcome has no matching registration for {workflow_uuid!r}"
            )
        values = {
            "workflow_uuid": workflow_uuid,
            "contract_version": CONTRACT_VERSION,
            "outcome": canonical["outcome"],
            "succeeded_count": canonical["succeeded_count"],
            "failed_count": canonical["failed_count"],
            "semantic_units": json.dumps(
                canonical["semantic_units"], sort_keys=True, separators=(",", ":")
            ),
            "details": json.dumps(
                canonical["details"], sort_keys=True, separators=(",", ":")
            ),
        }
        session.execute(
            text(
                "INSERT INTO okg.workflow_operational_outcomes "
                "(workflow_uuid, contract_version, outcome, succeeded_count, failed_count, semantic_units, details) "
                "VALUES (:workflow_uuid, :contract_version, :outcome, :succeeded_count, :failed_count, "
                "CAST(:semantic_units AS jsonb), CAST(:details AS jsonb)) "
                "ON CONFLICT (workflow_uuid) DO NOTHING"
            ),
            values,
        )
        actual = dict(
            session.execute(
                text(
                    "SELECT contract_version, outcome, succeeded_count, failed_count, semantic_units, details "
                    "FROM okg.workflow_operational_outcomes WHERE workflow_uuid = :id"
                ),
                {"id": workflow_uuid},
            )
            .mappings()
            .one()
        )
        expected_semantic = {
            "contract_version": CONTRACT_VERSION,
            "outcome": canonical["outcome"],
            "succeeded_count": canonical["succeeded_count"],
            "failed_count": canonical["failed_count"],
            "semantic_units": canonical["semantic_units"],
        }
        if {key: actual[key] for key in expected_semantic} != expected_semantic:
            raise WorkflowOperationalEvidenceConflict(
                f"outcome replay conflict for {workflow_uuid!r}"
            )
        return {"workflow_uuid": workflow_uuid, **expected_semantic, "details": actual["details"]}

    return OperationalTransactions(register_execution_tx, record_outcome_tx)


def normalize_outcome(raw: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise WorkflowOperationalEvidenceError("outcome must be a mapping")
    outcome = raw.get("outcome")
    if not isinstance(outcome, str) or outcome not in OUTCOMES:
        raise WorkflowOperationalEvidenceError(f"unknown outcome {outcome!r}")
    succeeded = _nonnegative_int(raw.get("succeeded_count"), "succeeded_count")
    failed = _nonnegative_int(raw.get("failed_count"), "failed_count")
    if outcome == "clean" and failed != 0:
        raise WorkflowOperationalEvidenceError("clean outcome has failures")
    if outcome == "partial_failure" and not (succeeded > 0 and failed > 0):
        raise WorkflowOperationalEvidenceError("partial_failure requires both counts")
    if outcome == "retained_failure" and failed == 0:
        raise WorkflowOperationalEvidenceError("retained_failure requires failures")
    if outcome == "skipped" and (succeeded != 0 or failed != 0):
        raise WorkflowOperationalEvidenceError("skipped outcome must be 0/0")
    raw_units = raw.get("semantic_units")
    if not isinstance(raw_units, Mapping):
        raise WorkflowOperationalEvidenceError("semantic_units must be an object")
    units: dict[str, list[str]] = {}
    for key, values in raw_units.items():
        if not isinstance(key, str) or not key:
            raise WorkflowOperationalEvidenceError("semantic unit keys must be non-empty")
        if not isinstance(values, (list, tuple, set, frozenset)):
            raise WorkflowOperationalEvidenceError(f"semantic_units[{key!r}] must be a collection")
        if any(not isinstance(value, str) or not value for value in values):
            raise WorkflowOperationalEvidenceError(f"semantic_units[{key!r}] has an invalid unit")
        units[key] = sorted(set(values))
    details = raw.get("details", {})
    if not isinstance(details, Mapping):
        raise WorkflowOperationalEvidenceError("details must be an object")
    return {
        "outcome": outcome,
        "succeeded_count": succeeded,
        "failed_count": failed,
        "semantic_units": dict(sorted(units.items())),
        "details": _bounded_details(details),
    }


def check_workflow_operational_outcomes(
    *, deployment: str, dsn: str, lookback_seconds: int = DEFAULT_LOOKBACK_SECONDS,
    sample_limit: int = 10,
) -> WorkflowOperationalOutcomeVerdict:
    """Read participant outcomes from the typed side channel only."""
    if not isinstance(deployment, str) or not deployment:
        raise ValueError("deployment must be a non-empty string")
    if isinstance(lookback_seconds, bool) or not isinstance(lookback_seconds, int) or lookback_seconds <= 0:
        raise ValueError("lookback_seconds must be a positive integer")
    if isinstance(sample_limit, bool) or not isinstance(sample_limit, int) or sample_limit <= 0:
        raise ValueError("sample_limit must be a positive integer")
    targets = {(p.workflow_name, p.queue_name) for p in operational_participants(deployment)}
    names = sorted({name for name, _queue in targets})
    window_start = math.floor((time.time() - lookback_seconds) * 1000)
    try:
        with db_connect(dsn, row_factory=dict_row) as conn:
            migrations = conn.execute(
                "SELECT applied_at, status FROM okg.schema_migrations WHERE migration_id = %s",
                (MIGRATION_ID,),
            ).fetchall()
            if len(migrations) != 1 or migrations[0]["status"] != "applied" or not isinstance(migrations[0]["applied_at"], datetime):
                return _unknown(deployment, "migration_not_applied", None)
            cutover = _epoch_ms(migrations[0]["applied_at"])
    except Exception as exc:  # noqa: BLE001
        return _unknown(deployment, f"migration_unreadable:{type(exc).__name__}", None)
    try:
        with db_connect(dsn, row_factory=dict_row) as conn:
            rows = conn.execute(
                """
                SELECT s.workflow_uuid, s.name AS status_workflow_name,
                       s.queue_name AS status_queue_name, s.created_at,
                       s.completed_at, r.deployment_name AS registration_deployment_name,
                       r.workflow_name AS registration_workflow_name,
                       r.queue_name AS registration_queue_name,
                       r.contract_version AS registration_contract_version,
                       r.registered_at, o.contract_version AS outcome_contract_version,
                       o.outcome, o.succeeded_count, o.failed_count,
                       o.semantic_units, o.details, o.recorded_at
                  FROM dbos.workflow_status s
                  LEFT JOIN okg.workflow_operational_registrations r USING (workflow_uuid)
                  LEFT JOIN okg.workflow_operational_outcomes o USING (workflow_uuid)
                 WHERE s.status = 'SUCCESS' AND s.name = ANY(%s)
                   AND (s.completed_at IS NULL OR s.completed_at >= %s)
                 ORDER BY s.completed_at NULLS FIRST, s.workflow_uuid
                """,
                (names, window_start),
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        return _unknown(deployment, f"reader_error:{type(exc).__name__}", cutover)

    counts = {"checked": 0, "partial": 0, "retained": 0, "unknown": 0, "global": 0, "legacy": 0}
    samples: list[dict[str, Any]] = []

    def sample(row: Mapping[str, Any], category: str, reason: str) -> None:
        if len(samples) < sample_limit:
            samples.append({
                "workflow_uuid": str(row.get("workflow_uuid") or ""),
                "workflow_name": row.get("status_workflow_name"),
                "queue_name": row.get("status_queue_name"),
                "completed_at": row.get("completed_at"),
                "category": category,
                "reason": reason,
            })

    for row in rows:
        pair = (row.get("status_workflow_name"), row.get("status_queue_name"))
        completed = _epoch_ms(row.get("completed_at"))
        if completed is not None and completed < cutover:
            if pair in targets:
                counts["legacy"] += 1
            continue
        registration = _registration_deployment(row)
        if registration is None:
            counts["global"] += 1
            sample(row, "global_unknown", "missing_or_mismatched_registration")
            continue
        if registration != deployment:
            continue
        reason = _outcome_reason(row)
        if reason is not None:
            counts["unknown"] += 1
            sample(row, "unknown", reason)
            continue
        counts["checked"] += 1
        if row["outcome"] == "partial_failure":
            counts["partial"] += 1
            sample(row, "partial_failure", "typed_partial_failure")
        elif row["outcome"] == "retained_failure":
            counts["retained"] += 1
            sample(row, "retained_failure", "typed_retained_failure")

    if counts["unknown"] or counts["global"]:
        code, ok, message = "workflow_outcome_unknown", False, "successful participant workflows have missing, malformed, stale, or unattributable typed outcome evidence"
    elif counts["partial"] or counts["retained"]:
        code, ok, message = "workflow_outcome_degraded", False, "successful participant workflows retained typed failures"
    elif counts["checked"]:
        code, ok, message = "workflow_outcome_clean", True, "all checked participant successes have clean typed outcomes"
    else:
        code, ok, message = "workflow_outcome_not_applicable", True, "no post-cutover participant successes in the health window"
    return WorkflowOperationalOutcomeVerdict(
        deployment=deployment, ok=ok, code=code, message=message,
        cutover_epoch_ms=cutover, checked_count=counts["checked"],
        partial_count=counts["partial"], retained_count=counts["retained"],
        unknown_count=counts["unknown"], global_unknown_count=counts["global"],
        legacy_count=counts["legacy"], samples=tuple(samples),
    )


def _registration_deployment(row: Mapping[str, Any]) -> str | None:
    deployment = row.get("registration_deployment_name")
    workflow = row.get("registration_workflow_name")
    queue = row.get("registration_queue_name")
    if not isinstance(deployment, str) or not deployment or not isinstance(workflow, str) or not isinstance(queue, str):
        return None
    if row.get("registration_contract_version") != CONTRACT_VERSION or workflow != row.get("status_workflow_name") or queue != row.get("status_queue_name"):
        return None
    try:
        expected = participant_for(deployment=deployment, workflow_name=workflow)
    except WorkflowOperationalEvidenceError:
        return None
    if expected.queue_name != queue:
        return None
    registered = _epoch_ms(row.get("registered_at"))
    created = _epoch_ms(row.get("created_at"))
    completed = _epoch_ms(row.get("completed_at"))
    if registered is None or (created is not None and registered < created):
        return None
    if completed is not None and registered > completed + 5_000:
        return None
    return deployment


def _outcome_reason(row: Mapping[str, Any]) -> str | None:
    if row.get("outcome_contract_version") != CONTRACT_VERSION:
        return "missing_or_wrong_outcome_contract_version"
    recorded = _epoch_ms(row.get("recorded_at"))
    registered = _epoch_ms(row.get("registered_at"))
    completed = _epoch_ms(row.get("completed_at"))
    if recorded is None or registered is None or completed is None:
        return "stale_outcome"
    if recorded < registered:
        return "stale_outcome"
    # The application transaction is committed during the workflow and must
    # be tied to that terminal execution. A missing terminal timestamp or a
    # record outside a bounded five-minute recovery window is unknown rather
    # than healthy. This prevents a copied/old outcome row from certifying a
    # later SUCCESS row with the same UUID.
    # Registration/outcome transactions commit before DBOS writes terminal
    # status. Permit only a small clock/commit tolerance into the future;
    # evidence materially newer than the terminal row is copied or forged.
    if recorded > completed + 5_000 or completed - recorded > 5 * 60 * 1000:
        return "stale_outcome"
    try:
        canonical = normalize_outcome({
            "outcome": row.get("outcome"),
            "succeeded_count": row.get("succeeded_count"),
            "failed_count": row.get("failed_count"),
            "semantic_units": row.get("semantic_units"),
            "details": row.get("details"),
        })
    except WorkflowOperationalEvidenceError as exc:
        return f"malformed_outcome:{type(exc).__name__}"
    if canonical["semantic_units"] != row.get("semantic_units") or canonical["details"] != row.get("details"):
        return "noncanonical_outcome"
    return None


def _unknown(deployment: str, reason: str, cutover: int | None) -> WorkflowOperationalOutcomeVerdict:
    return WorkflowOperationalOutcomeVerdict(
        deployment=deployment, ok=False, code="workflow_outcome_unknown",
        message="workflow operational outcome evidence is unreadable",
        cutover_epoch_ms=cutover, checked_count=0, partial_count=0,
        retained_count=0, unknown_count=1, global_unknown_count=0,
        legacy_count=0, samples=({"workflow_uuid": "", "category": "unknown", "reason": reason},),
    )


def _current_workflow_uuid() -> str:
    from dbos import DBOS
    workflow_uuid = DBOS.workflow_id
    if not isinstance(workflow_uuid, str) or not workflow_uuid:
        raise WorkflowOperationalEvidenceError("DBOS workflow id is unavailable")
    return workflow_uuid


def _nonnegative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise WorkflowOperationalEvidenceError(f"{field} must be a non-negative integer")
    return value


def _bounded_details(raw: Mapping[str, Any]) -> dict[str, Any]:
    normalized = json.loads(json.dumps(dict(raw), default=str))
    kept: dict[str, Any] = {}
    truncated = len(normalized) > 10
    for key, value in sorted(normalized.items(), key=lambda item: str(item[0]))[:10]:
        candidate = {**kept, str(key): value}
        if len(json.dumps(candidate, sort_keys=True, separators=(",", ":")).encode()) > 8192:
            truncated = True
            continue
        kept[str(key)] = value
    if truncated:
        kept["_truncated"] = True
    return kept


def _epoch_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return math.floor(value.timestamp() * 1000)
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


__all__ = [
    "CONTRACT_VERSION", "DEFAULT_LOOKBACK_SECONDS", "EMBED_WORKFLOW_NAME",
    "MIGRATION_ID", "OUTCOMES", "OperationalParticipant", "OperationalTransactions",
    "WorkflowOperationalEvidenceConflict", "WorkflowOperationalEvidenceError",
    "WorkflowOperationalOutcomeVerdict", "check_workflow_operational_outcomes",
    "define_operational_transactions", "normalize_outcome", "operational_participants",
    "outcome_transaction_name", "participant_for", "registration_transaction_name",
]
