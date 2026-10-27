"""Policy-driven, tombstone-first Chronos interval-history compaction.

The ordinary publisher never imports this module.  Compaction is an explicit
operator action with a persisted dry-run plan.  Apply revalidates every binding,
retires affected checkpoints before deleting any interval rows, and records
progress in the same transaction as each bounded batch.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Mapping, Sequence

import psycopg
from psycopg import sql

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store_internal.common import (
    DEFAULT_BRANCH,
    EDGE_TABLE,
    NODE_TABLE,
)
from okg.substrate.runtime.source_sync_priority import (
    publisher_publish_lock_on_connection,
)


_DURATION = re.compile(r"^([1-9][0-9]*)([smhdw])$")
_DURATION_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
_POLICY_KEYS = frozenset({"default", "audit_tail_generations", "nodes", "edges"})
_RULE_KEYS = frozenset({"horizon"})
_DATA_BATCHES = ("nodes", "edges")
_log = logging.getLogger(__name__)

_COMPACTION_LOCK_NAMESPACE = 0x4F4B4743  # "OKGC"
_PROTECTION_LOCK_NAMESPACE = 1326131011
_PROTECTION_LOCK_KEY = 1


@dataclass(frozen=True)
class RetentionRule:
    mode: str
    horizon_seconds: int | None = None

    def to_json(self) -> str | dict[str, str]:
        if self.mode != "horizon":
            return self.mode
        seconds = int(self.horizon_seconds or 0)
        return {"horizon_seconds": str(seconds)}


@dataclass(frozen=True)
class HistoryRetentionPolicy:
    default: RetentionRule
    audit_tail_generations: int
    nodes: Mapping[str, RetentionRule]
    edges: Mapping[str, RetentionRule]

    def rule_for(self, kind: str, classifier: str) -> RetentionRule:
        overrides = self.nodes if kind == "node" else self.edges
        return overrides.get(classifier, self.default)

    def canonical(self) -> dict[str, Any]:
        return {
            "default": self.default.to_json(),
            "audit_tail_generations": self.audit_tail_generations,
            "nodes": {key: self.nodes[key].to_json() for key in sorted(self.nodes)},
            "edges": {key: self.edges[key].to_json() for key in sorted(self.edges)},
        }

    @property
    def digest(self) -> str:
        encoded = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":"))
        return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class _Checkpoint:
    checkpoint_id: str
    target_generation_id: int | None
    point: int
    ordinal: int | None
    created_at: datetime
    catalog_version_id: int | None


DEFAULT_HISTORY_RETENTION: Mapping[str, Any] = {
    "default": {"horizon": "14d"},
    "audit_tail_generations": 50,
}

HISTORY_RETENTION_YAML_SNIPPET = (
    "chronos:\n"
    "  history_retention:\n"
    "    default:\n"
    "      horizon: 14d\n"
    "    audit_tail_generations: 50\n"
)

@dataclass(frozen=True)
class ResolvedHistoryRetention:
    """Which retention policy a compaction run is about to use, and from where.

    ``source`` is ``deployment`` when ``chronos.history_retention`` is declared
    and ``built_in_default`` when it is not.  The built-in is never applied
    quietly: the plan carries both fields, the CLI prints them, and the
    unattended okg-workspace compaction schedule REFUSES when the source is
    not ``deployment`` -- a default nobody read must not be the authority for
    a delete no human is watching (#1309).
    """

    policy_raw: Mapping[str, Any]
    source: str
    notice: str | None


def resolve_history_retention(
    policy_raw: Mapping[str, Any] | None,
) -> ResolvedHistoryRetention:
    """Resolve the retention rules, falling back to a documented default.

    Compaction used to refuse outright when ``chronos.history_retention`` was
    absent, so the only lever that reclaims interval-history storage was
    unavailable exactly when a deployment had grown large enough to need it
    (#1309).  An absent policy now resolves to :data:`DEFAULT_HISTORY_RETENTION`
    and says so.  A policy that is PRESENT but not a mapping is still an error:
    that is a broken declaration, not an absent one, and guessing at the
    operator's intent there could delete history they meant to keep.
    """
    if policy_raw is None:
        return ResolvedHistoryRetention(
            policy_raw=dict(DEFAULT_HISTORY_RETENTION),
            source="built_in_default",
            notice=(
                "chronos.history_retention is not declared for this "
                "deployment; compaction used the built-in default "
                "(default.horizon 14d, audit_tail_generations 50). Declare it "
                "in deployment.yaml to choose your own rules:\n"
                + HISTORY_RETENTION_YAML_SNIPPET
            ),
        )
    if not isinstance(policy_raw, Mapping):
        raise SubstrateError(
            "chronos.history_retention must be a mapping",
            data={"value_type": type(policy_raw).__name__},
            fix_hint=(
                "declare it as a mapping, or remove it to use the built-in "
                "default:\n" + HISTORY_RETENTION_YAML_SNIPPET
            ),
        )
    return ResolvedHistoryRetention(
        policy_raw=dict(policy_raw), source="deployment", notice=None
    )


def _catalog_subtypes_for_probe(conn: psycopg.Connection[Any]) -> set[str]:
    """Every subtype any applied catalog knows, for validating a declared policy.

    The probe is a health report, not a compaction: it must not fail because a
    policy names a subtype that the CURRENT head catalog dropped. Accepting the
    union across applied catalogs keeps the probe reporting on shape errors --
    a non-mapping, a bad duration, an unknown key -- which is what it is for.
    """
    rows = conn.execute(
        "SELECT DISTINCT name FROM okg.subtypes"
    ).fetchall()
    return {str(row[0]) for row in rows}


def unattended_apply_refusal(plan: Mapping[str, Any]) -> str | None:
    """Why an unattended caller must not apply this plan, or None if it may.

    The CLI can apply a plan built from the built-in retention default because
    an operator read the dry run and typed the plan id back. A scheduled
    workflow has neither: it confirms its own plan and nobody sees the notice.
    So for those callers an undeclared policy is a refusal, not a default
    (#1309).
    """
    source = str(plan.get("policy_source") or "")
    if source == "deployment":
        return None
    return (
        "refusing to apply unattended: chronos.history_retention is not "
        "declared for this deployment, so the plan used a built-in default no "
        f"operator has reviewed (policy_source={source or 'unknown'!s})"
    )


def check_history_retention(
    conn: psycopg.Connection[Any],
    *,
    store: Any,
    deployment_name: str,
    branch: str = DEFAULT_BRANCH,
    policy_raw: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Report interval-history growth and whether retention is declared.

    A deployment that never declares ``chronos.history_retention`` accumulates
    superseded interval rows forever, and nothing said so until an operator
    tried to compact during an incident (#1309).  This probe is the standing
    statement: how big the interval tables are, what share of their rows are no
    longer visible at the published head, whether a policy is declared, and
    whether a compaction has ever actually completed.
    """
    # Three states, not two. A policy that is PRESENT but malformed stops both
    # the publisher ("must be a mapping before publisher startup") and
    # compaction, so reporting it as configured would call a deployment that
    # cannot publish healthy.
    declared_invalid: str | None = None
    if policy_raw is not None:
        try:
            parse_history_retention(
                policy_raw,
                catalog_node_subtypes=_catalog_subtypes_for_probe(conn),
                observed_edge_types=set(),
            )
        except SubstrateError as err:
            declared_invalid = str(err)
    report: dict[str, Any] = {
        "ok": True,
        "status": "ok",
        "code": "history_retention_ok",
        "configured": policy_raw is not None and declared_invalid is None,
        "declared": policy_raw is not None,
        "deployment_name": deployment_name,
        "branch": branch,
    }
    if declared_invalid is not None:
        report["declared_invalid_reason"] = declared_invalid
    try:
        checkpoints = _checkpoints(
            conn, store, deployment_name=deployment_name, branch=branch
        )
    except SubstrateError as err:
        report.update(
            status="skipped",
            code="history_retention_probe_skipped",
            message=f"cannot read published generations: {err}",
        )
        return report
    if not checkpoints:
        report.update(
            status="skipped",
            code="history_retention_probe_skipped",
            message="no published generations on this branch yet",
        )
        return report

    head_point = checkpoints[-1].point
    physical = _physical_tables(conn)
    tables: dict[str, Any] = {}
    branch_rows = 0
    branch_superseded = 0
    total_bytes = 0
    for logical, name in ((NODE_TABLE, "nodes"), (EDGE_TABLE, "edges")):
        table = physical[logical]
        # The physical interval tables are shared by every Chronos branch, and
        # compaction only ever deletes rows this branch's segments own. A
        # whole-relation ratio would tell an operator to run compact and then
        # reclaim none of it (the in-place-upgrade case, where pre-existing
        # rows belong to `main`). Report both populations, and drive the
        # verdict off the branch-scoped one.
        row = conn.execute(
            sql.SQL(
                "SELECT count(*),"
                " count(*) FILTER (WHERE t.live_hi <= %s),"
                " count(*) FILTER (WHERE s.segment_id IS NOT NULL),"
                " count(*) FILTER (WHERE s.segment_id IS NOT NULL"
                "                    AND t.live_hi <= %s)"
                " FROM {} AS t"
                " LEFT JOIN _chronos_branch_interval_segments AS s"
                "   ON s.segment_id = t.writer_segment_id"
                "  AND s.owner_branch_id = %s"
            ).format(_qualified_identifier(table)),
            (head_point, head_point, branch),
        ).fetchone()
        rows, superseded, rows_branch, superseded_branch = (int(v) for v in row)
        table_bytes = int(
            conn.execute(
                "SELECT pg_total_relation_size(%s::regclass)", (table,)
            ).fetchone()[0]
        )
        tables[name] = {
            "table": table,
            "rows_total": rows,
            "rows_superseded": superseded,
            "rows_branch_total": rows_branch,
            "rows_branch_superseded": superseded_branch,
            "rows_branch_live": rows_branch - superseded_branch,
            "superseded_ratio": (
                round(superseded_branch / rows_branch, 4) if rows_branch else 0.0
            ),
            "relation_bytes": table_bytes,
        }
        branch_rows += rows_branch
        branch_superseded += superseded_branch
        total_bytes += table_bytes
    ratio = round(branch_superseded / branch_rows, 4) if branch_rows else 0.0
    report["tables"] = tables
    report["interval_relation_bytes"] = total_bytes
    report["superseded_row_ratio"] = ratio

    completed_compactions = _completed_compaction_count(
        conn, deployment_name=deployment_name, branch=branch
    )
    report["completed_compactions"] = completed_compactions
    # Issue #3578: a new graph has no history to retire yet, so the warnings
    # below would only alarm a newcomer. They fire from the first superseded
    # version: from then on that history stays until something retires it.
    dead_rows = branch_superseded
    report["dead_history_rows"] = dead_rows
    size_text = (
        f"interval tables hold {total_bytes} bytes; of the {branch_rows} rows "
        f"this branch owns, {branch_superseded} ({ratio:.0%}) are no longer "
        f"visible at the published head"
    )

    if declared_invalid is not None:
        report.update(
            ok=True,
            status="warn",
            code="history_retention_declared_invalid",
            message=(
                "chronos.history_retention is declared but does not parse "
                f"({declared_invalid}), so both the publisher and compaction "
                f"refuse it: {size_text}. Fix or remove the block."
            ),
        )
        return report

    if policy_raw is None and branch_superseded == 0:
        report.update(
            message=(
                "chronos.history_retention is not declared; this branch owns "
                "no superseded node or edge version yet, so nothing needs "
                f"retiring: "
                f"{size_text}. A warning starts with the first superseded "
                "version."
            ),
        )
        return report
    if policy_raw is None:
        # Once any version is superseded, an undeclared policy is worth
        # saying: the node and edge interval tables reached 88% dead on
        # okg-workspace.
        report.update(
            ok=True,
            status="warn",
            code="history_retention_unset",
            message=(
                "chronos.history_retention is not declared, so node and edge "
                f"history is never retired: {size_text}. `okg generation "
                "compact` now plans with a built-in default and prints it; "
                "declare your own rules to choose them deliberately:\n"
                + HISTORY_RETENTION_YAML_SNIPPET
            ),
        )
        return report

    report["deletion_authority"] = "confirmed_compaction_plan"
    # No ratio gate here: until one compaction has completed NOTHING is
    # deleting history. A scaffolded deployment that never schedules
    # compaction is exactly this state, and it must not read as healthy once
    # a single row of dead history exists. Before that there is nothing to
    # delete.
    if completed_compactions == 0 and dead_rows == 0:
        report["message"] = (
            "chronos.history_retention is declared and no compaction has "
            "run, but no history is dead yet, so nothing needs retiring: "
            f"{size_text}. A warning starts with the first dead row; then run "
            f"`okg generation compact --deployment {deployment_name}` or "
            "schedule the history_compaction workflow."
        )
        return report
    if completed_compactions == 0:
        report.update(
            ok=True,
            status="warn",
            code="history_retention_declared_but_never_applied",
            message=(
                "chronos.history_retention is declared, which makes a "
                "confirmed compaction plan the only thing that deletes "
                "history -- and no compaction has completed for this "
                f"deployment: {size_text}. Run `okg generation compact "
                f"--deployment {deployment_name}` or schedule the "
                "history_compaction workflow."
            ),
        )
        return report
    report["message"] = (
        "chronos.history_retention is declared; a confirmed compaction plan "
        f"is the sole deletion authority ({completed_compactions} completed "
        f"so far). {size_text}."
    )
    return report


def _completed_compaction_count(
    conn: psycopg.Connection[Any], *, deployment_name: str, branch: str
) -> int:
    """Count completed plans without creating the table if it is absent."""
    present = conn.execute(
        "SELECT to_regclass('okg.history_compaction_plans')"
    ).fetchone()[0]
    if present is None:
        return 0
    return int(
        conn.execute(
            "SELECT count(*) FROM okg.history_compaction_plans "
            "WHERE deployment_name = %s AND chronos_branch_id = %s "
            "AND status = 'complete'",
            (deployment_name, branch),
        ).fetchone()[0]
    )


def parse_history_retention(
    raw: Mapping[str, Any] | None,
    *,
    catalog_node_subtypes: set[str],
    observed_edge_types: set[str],
) -> HistoryRetentionPolicy:
    """Parse the exact ``chronos.history_retention`` contract.

    Unknown fields and unknown classifiers are errors; there is no fallback to
    a permissive default because that could delete the wrong history.
    """
    if not isinstance(raw, Mapping):
        raise SubstrateError(
            "chronos.history_retention must be a mapping",
            data={"value_type": type(raw).__name__},
        )
    unknown = sorted(set(raw) - _POLICY_KEYS)
    missing = sorted({"default", "audit_tail_generations"} - set(raw))
    if unknown or missing:
        raise SubstrateError(
            "invalid chronos.history_retention fields",
            data={"unknown": unknown, "missing": missing},
            fix_hint="use only default, audit_tail_generations, nodes, and edges",
        )
    audit_tail = raw.get("audit_tail_generations")
    if isinstance(audit_tail, bool) or not isinstance(audit_tail, int) or audit_tail <= 0:
        raise SubstrateError("audit_tail_generations must be a positive integer")
    nodes_raw = raw.get("nodes", {})
    edges_raw = raw.get("edges", {})
    if not isinstance(nodes_raw, Mapping) or not isinstance(edges_raw, Mapping):
        raise SubstrateError("history_retention nodes and edges must be mappings")
    unknown_nodes = sorted(set(map(str, nodes_raw)) - catalog_node_subtypes)
    unknown_edges = sorted(set(map(str, edges_raw)) - observed_edge_types)
    if unknown_nodes or unknown_edges:
        raise SubstrateError(
            "history retention contains unknown classifiers",
            data={
                "unknown_node_subtypes": unknown_nodes,
                "unknown_edge_types": unknown_edges,
            },
            fix_hint="refresh the catalog/observed edge inventory, then correct the policy",
        )
    return HistoryRetentionPolicy(
        default=_parse_rule(raw["default"], path="default"),
        audit_tail_generations=audit_tail,
        nodes={str(key): _parse_rule(value, path=f"nodes.{key}") for key, value in nodes_raw.items()},
        edges={str(key): _parse_rule(value, path=f"edges.{key}") for key, value in edges_raw.items()},
    )


def _parse_rule(value: Any, *, path: str) -> RetentionRule:
    if isinstance(value, str) and value in {"full", "latest-only"}:
        return RetentionRule(str(value))
    if not isinstance(value, Mapping) or set(value) != _RULE_KEYS:
        raise SubstrateError(
            f"{path} must be full, latest-only, or {{horizon: <duration>}}"
        )
    duration = value.get("horizon")
    if not isinstance(duration, str) or (match := _DURATION.fullmatch(duration)) is None:
        raise SubstrateError(f"{path}.horizon must be a positive duration ending in s, m, h, d, or w")
    seconds = int(match.group(1)) * _DURATION_SECONDS[match.group(2)]
    return RetentionRule("horizon", seconds)


def _json_digest(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


class PlannerProgress:
    """Phase timing for the compaction planner.

    The planner holds the publisher lease for its whole run, so every other
    connection queues behind it.  Without progress an operator cannot tell a
    slow scan from a wedged one -- on okg-workspace that ambiguity cost an
    hour of misdiagnosis (#1308).  Each phase is logged at INFO with its own
    elapsed seconds and the elapsed seconds since the planner started, and the
    same records are persisted in the plan under ``planner_progress`` so the
    timings survive the run.
    """

    def __init__(self, *, deployment_name: str, branch: str) -> None:
        self.deployment_name = deployment_name
        self.branch = branch
        self._started = time.monotonic()
        self.phases: list[dict[str, Any]] = []

    @contextmanager
    def phase(self, name: str, **context: Any) -> Iterator[None]:
        began = time.monotonic()
        _log.info(
            "history compaction planner: %s started (deployment=%s branch=%s "
            "elapsed=%.1fs)%s",
            name,
            self.deployment_name,
            self.branch,
            began - self._started,
            _format_context(context),
        )
        outcome = "finished"
        try:
            yield
        except BaseException as exc:
            # "finished" in front of a traceback is the wrong word for a
            # diagnostic whose whole job is telling a slow phase from a broken
            # one. Name the failure, then re-raise unchanged.
            outcome = f"failed ({type(exc).__name__})"
            raise
        finally:
            ended = time.monotonic()
            record = {
                "phase": name,
                "outcome": outcome,
                "elapsed_seconds": round(ended - began, 3),
                "since_start_seconds": round(ended - self._started, 3),
                **context,
            }
            self.phases.append(record)
            _log.info(
                "history compaction planner: %s %s in %.1fs (total %.1fs)%s",
                name,
                outcome,
                ended - began,
                ended - self._started,
                _format_context(context),
            )


def _format_context(context: Mapping[str, Any]) -> str:
    if not context:
        return ""
    return " " + " ".join(f"{key}={context[key]}" for key in sorted(context))


@contextmanager
def _phase(progress: PlannerProgress | None, name: str, **context: Any) -> Iterator[None]:
    """Time one planner phase; a no-op when no progress sink was supplied."""
    if progress is None:
        yield
        return
    with progress.phase(name, **context):
        yield


def ensure_history_compaction_schema(conn: psycopg.Connection[Any]) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS okg")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.history_compaction_plans (
              plan_id UUID PRIMARY KEY,
              deployment_name TEXT NOT NULL,
              chronos_branch_id TEXT NOT NULL,
              status TEXT NOT NULL CHECK (status IN ('planned', 'applying', 'complete', 'failed')),
              plan JSONB NOT NULL,
              progress JSONB NOT NULL DEFAULT '{}'::jsonb,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.compacted_generations (
              checkpoint_id TEXT PRIMARY KEY,
              target_generation_id BIGINT,
              deployment_name TEXT NOT NULL,
              branch_id TEXT NOT NULL,
              plan_id UUID NOT NULL REFERENCES okg.history_compaction_plans(plan_id),
              compacted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              reason TEXT NOT NULL DEFAULT 'history_retention'
            )
            """
        )
        cur.execute(
            "ALTER TABLE okg.compacted_generations "
            "ADD COLUMN IF NOT EXISTS target_generation_id BIGINT"
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.history_generation_pins (
              deployment_name TEXT NOT NULL,
              branch_id TEXT NOT NULL,
              pin_name TEXT NOT NULL,
              checkpoint_id TEXT NOT NULL,
              expires_at TIMESTAMPTZ,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              PRIMARY KEY (deployment_name, branch_id, pin_name)
            )
            """
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS compacted_generations_target_id_idx "
            "ON okg.compacted_generations (deployment_name, target_generation_id) "
            "WHERE target_generation_id IS NOT NULL"
        )
        _install_protection_coordination(cur)
    conn.commit()


def create_history_compaction_plan(
    conn: psycopg.Connection[Any],
    *,
    store: Any,
    deployment_name: str,
    branch: str = DEFAULT_BRANCH,
    policy_raw: Mapping[str, Any] | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Persist and return a dry-run plan.  This performs no destructive write."""
    ensure_history_compaction_schema(conn)
    with publisher_publish_lock_on_connection(
        conn=conn,
        deployment=deployment_name,
        wait_seconds=30.0,
        operation="Chronos history compaction dry-run",
    ), _history_protection_lock(conn):
        progress = PlannerProgress(deployment_name=deployment_name, branch=branch)
        snapshot = _snapshot(
            conn,
            store=store,
            deployment_name=deployment_name,
            branch=branch,
            policy_raw=policy_raw,
            now=now,
            progress=progress,
        )
        plan_id = str(uuid.uuid4())
        plan = {
            "plan_id": plan_id,
            "dry_run": True,
            "applied": False,
            **snapshot,
            "lock_posture": {
                "publisher_branch_lock": "held_during_plan_and_required_for_apply",
                "deployment_compaction_lock": "required_for_apply",
                "protection_lock": "held_during_plan_and_required_for_apply",
            },
            "physical_reclaim": {
                "mode": "offline-only",
                "workers_must_be_stopped": True,
                "live_vacuum_full": "forbidden",
            },
        }
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.history_compaction_plans
                  (plan_id, deployment_name, chronos_branch_id, status, plan)
                VALUES (%s, %s, %s, 'planned', %s::jsonb)
                """,
                (plan_id, deployment_name, branch, json.dumps(plan, default=str)),
            )
        conn.commit()
    return plan


def apply_history_compaction_plan(
    conn: psycopg.Connection[Any],
    *,
    store: Any,
    plan_id: str,
    confirm: str | None,
    apply: bool,
    policy_raw: Mapping[str, Any] | None,
    expected_deployment_name: str | None = None,
    expected_branch: str | None = None,
    batch_size: int = 5000,
    crash_after_batch: str | None = None,
    crash_after_committed_batch: str | None = None,
    publisher_lock_wait_seconds: float = 30.0,
) -> dict[str, Any]:
    """Apply one persisted plan under both coordination locks.

    ``crash_after_batch`` is a deterministic test hook.  Every completed batch
    has already committed its progress, so a retry resumes safely.
    """
    if not apply or confirm != plan_id:
        raise SubstrateError(
            "history compaction apply requires --apply and the exact --confirm <plan-id>",
            data={"plan_id": plan_id, "confirm": confirm, "apply": apply},
        )
    if batch_size <= 0:
        raise SubstrateError("history compaction batch_size must be positive")
    ensure_history_compaction_schema(conn)
    plan, status, progress = _load_plan(conn, plan_id)
    deployment = str(plan["deployment_name"])
    branch = str(plan["branch"])
    if expected_deployment_name is not None and expected_deployment_name != deployment:
        raise SubstrateError(
            "history compaction plan belongs to a different deployment",
            data={
                "plan_id": plan_id,
                "expected_deployment_name": expected_deployment_name,
                "plan_deployment_name": deployment,
            },
            fix_hint="select the plan's deployment or create a new dry-run plan",
        )
    if expected_branch is not None and expected_branch != branch:
        raise SubstrateError(
            "history compaction plan belongs to a different branch",
            data={
                "plan_id": plan_id,
                "expected_branch": expected_branch,
                "plan_branch": branch,
            },
            fix_hint="select the plan's branch or create a new dry-run plan",
        )
    if status == "complete":
        return {**plan, "dry_run": False, "applied": True, "replayed": True, "progress": progress}

    with publisher_publish_lock_on_connection(
        conn=conn,
        deployment=deployment,
        wait_seconds=publisher_lock_wait_seconds,
        operation="Chronos history compaction",
    ), _deployment_compaction_lock(conn, deployment), _history_protection_lock(conn):
        current = _snapshot(
            conn,
            store=store,
            deployment_name=deployment,
            branch=branch,
            policy_raw=policy_raw,
            now=_parse_time(plan["planned_at"]),
            include_estimates=False,
        )
        drift = _binding_drift(plan, current, progress=progress)
        if drift:
            raise SubstrateError(
                "history compaction plan drifted; refusing to apply",
                data={"plan_id": plan_id, "drift": drift},
                fix_hint="run a fresh dry-run and confirm the new plan id",
            )
        _set_plan_status(conn, plan_id, "applying")

        # The refusal barrier is first.  Once any interval deletion can commit,
        # every affected historical checkpoint already resolves to
        # generation_compacted rather than a partial snapshot.
        if not progress.get("tombstones"):
            _commit_tombstone_barrier(conn, plan)
            progress["tombstones"] = {"complete": True, "retired": len(plan["retired_checkpoints"])}
            _persist_progress(conn, plan_id, progress)
            if crash_after_batch == "tombstones":
                raise RuntimeError("injected crash after tombstones")

        for batch_name in _DATA_BATCHES:
            if progress.get(batch_name, {}).get("complete"):
                continue
            injected = False

            def record_progress(count: int, name: str = batch_name) -> None:
                nonlocal injected
                _batch_progress(conn, plan_id, progress, name, count)
                if crash_after_committed_batch == name and not injected:
                    injected = True
                    raise RuntimeError(f"injected crash after committed {name} batch")

            deleted = _delete_batches(
                conn,
                plan=plan,
                batch_name=batch_name,
                batch_size=batch_size,
                already_deleted=int(progress.get(batch_name, {}).get("deleted", 0)),
                on_progress=record_progress,
            )
            progress[batch_name] = {"complete": True, "deleted": deleted}
            _persist_progress(conn, plan_id, progress)
            if crash_after_batch == batch_name:
                raise RuntimeError(f"injected crash after {batch_name}")

        _set_plan_status(conn, plan_id, "complete", progress=progress)
    conn.commit()
    return {
        **plan,
        # Report the source this APPLY resolved, not the one the plan was
        # written with. `policy_digest` pins the VALUES, so deleting a
        # declaration whose values equal the built-in default passes drift --
        # and the run really did use the default. Saying "deployment" there
        # would hide it.
        "policy_source": current.get("policy_source", plan.get("policy_source")),
        "policy_notice": current.get("policy_notice", plan.get("policy_notice")),
        "dry_run": False,
        "applied": True,
        "replayed": False,
        "progress": progress,
    }


def offline_reclaim_history_storage(
    conn: psycopg.Connection[Any],
    *,
    plan_id: str,
    workers_stopped: bool,
) -> dict[str, Any]:
    """Physically shrink planned tables in an explicit maintenance window."""
    if not workers_stopped:
        raise SubstrateError(
            "offline history reclaim requires --workers-stopped",
            fix_hint="stop publisher/source-sync workers and retry in a maintenance window",
        )
    plan, status, _progress = _load_plan(conn, plan_id)
    if status != "complete":
        raise SubstrateError("offline reclaim requires a completed compaction plan")
    tables = [str(value) for value in plan["physical_tables"].values()]
    before = _relation_sizes(conn, tables)
    # Callers commonly inspect logical results before reclaim, which opens an
    # implicit read transaction. VACUUM FULL requires autocommit and cannot run
    # until that read boundary is closed.
    conn.commit()
    old_autocommit = conn.autocommit
    conn.autocommit = True
    try:
        with publisher_publish_lock_on_connection(
            conn=conn,
            deployment=str(plan["deployment_name"]),
            operation="offline Chronos history reclaim",
        ), _deployment_compaction_lock(conn, str(plan["deployment_name"])):
            with conn.cursor() as cur:
                for table in tables:
                    cur.execute(sql.SQL("VACUUM (FULL, ANALYZE) {}").format(_qualified_identifier(table)))
    finally:
        conn.autocommit = old_autocommit
    after = _relation_sizes(conn, tables)
    return {
        "plan_id": plan_id,
        "workers_stopped": True,
        "before_bytes": before,
        "after_bytes": after,
        "physically_reclaimed_bytes": sum(before.values()) - sum(after.values()),
    }


def compacted_generation_record(
    conn: psycopg.Connection[Any],
    checkpoint_id: str | int,
    *,
    deployment_name: str | None = None,
) -> dict[str, Any] | None:
    """Return the stable tombstone for a retired checkpoint when installed."""
    present = conn.execute(
        "SELECT to_regclass('okg.compacted_generations')"
    ).fetchone()
    if present is None or present[0] is None:
        return None
    function = conn.execute(
        "SELECT to_regprocedure('okg.compacted_generation_record(text,text)')"
    ).fetchone()
    if function is not None and function[0] is not None:
        row = conn.execute(
            "SELECT okg.compacted_generation_record(%s, %s)",
            (str(checkpoint_id), deployment_name),
        ).fetchone()
        return dict(row[0]) if row is not None and row[0] is not None else None
    text = str(checkpoint_id)
    numeric = int(text) if text.isdigit() else None
    clauses = ["checkpoint_id = %s"]
    params: list[Any] = [text]
    if numeric is not None:
        clauses.append("target_generation_id = %s")
        params.append(numeric)
    if deployment_name is not None:
        clauses.append("deployment_name = %s")
        params.append(deployment_name)
    row = conn.execute(
        "SELECT checkpoint_id, target_generation_id, deployment_name, branch_id, "
        "plan_id::text, compacted_at, reason FROM okg.compacted_generations WHERE ("
        + " OR ".join(clauses[: 2 if numeric is not None else 1])
        + ")"
        + (" AND deployment_name = %s" if deployment_name is not None else ""),
        tuple(params),
    ).fetchone()
    if row is None:
        return None
    return {
        "checkpoint_id": row[0],
        "target_generation_id": row[1],
        "deployment_name": row[2],
        "branch": row[3],
        "plan_id": row[4],
        "compacted_at": row[5].isoformat() if row[5] else None,
        "reason": row[6],
    }


def _snapshot(
    conn: psycopg.Connection[Any],
    *,
    store: Any,
    deployment_name: str,
    branch: str,
    policy_raw: Mapping[str, Any] | None,
    now: datetime | None,
    include_estimates: bool = True,
    progress: PlannerProgress | None = None,
) -> dict[str, Any]:
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    with _phase(progress, "checkpoints"):
        checkpoints = _checkpoints(conn, store, deployment_name=deployment_name, branch=branch)
    if not checkpoints:
        raise SubstrateError("cannot compact history: no published checkpoints")
    head = checkpoints[-1]
    physical = _physical_tables(conn)
    with _phase(progress, "classifier_inventory"):
        observed_nodes = _distinct_classifier(conn, physical[NODE_TABLE], "subtype")
        observed_edges = _distinct_classifier(conn, physical[EDGE_TABLE], "edge_type")
    active_catalog_version = _assert_head_catalog_active(
        conn,
        catalog_version_id=head.catalog_version_id,
        deployment_name=deployment_name,
    )
    catalog_nodes = _catalog_subtypes(conn, catalog_version_id=active_catalog_version)
    missing_catalog = sorted(observed_nodes - catalog_nodes)
    if missing_catalog:
        raise SubstrateError(
            "observed node history contains subtypes absent from the active catalog",
            data={"missing_subtypes": missing_catalog},
        )
    resolved = resolve_history_retention(policy_raw)
    policy = parse_history_retention(
        resolved.policy_raw,
        catalog_node_subtypes=catalog_nodes,
        observed_edge_types=observed_edges,
    )
    with _phase(progress, "protection_set"):
        protection, ignored_abandoned_branches = _protection_set(
            conn,
            store,
            deployment_name=deployment_name,
            branch=branch,
            checkpoints=checkpoints,
            audit_tail=policy.audit_tail_generations,
        )
    floors = {
        "nodes": _selector_floors(
            policy=policy,
            kind="node",
            classifiers=observed_nodes,
            checkpoints=checkpoints,
            now=now_utc,
        ),
        "edges": _selector_floors(
            policy=policy,
            kind="edge",
            classifiers=observed_edges,
            checkpoints=checkpoints,
            now=now_utc,
        ),
    }
    non_full = [
        floor["point"]
        for selectors in floors.values()
        for floor in selectors.values()
        if floor["point"] is not None
    ]
    retirement_point = max(non_full) if non_full else None
    retired = [cp.checkpoint_id for cp in checkpoints if retirement_point is not None and cp.point < retirement_point]
    # Refuse to retire a generation whose ordinal is unknown. `_checkpoints`
    # ranks un-ordinaled checkpoints last on the assumption that they are the
    # un-minted tail; that assumption is fine for ORDERING but must never be
    # the basis for a DELETE. An un-ordinaled checkpoint down here means the
    # assumption did not hold -- the mint failed for an older generation (it is
    # swallowed by design in `_maybe_mint_generation_ordinal`) -- so its true
    # position, and therefore whether it really falls below the floor, is not
    # established. Delete nothing and say why.
    retired_set = set(retired)
    unordered_retired = sorted(
        cp.checkpoint_id
        for cp in checkpoints
        if cp.checkpoint_id in retired_set and cp.ordinal is None
    )
    if unordered_retired:
        raise SubstrateError(
            "cannot retire generations with no recorded ordinal",
            data={
                "deployment_name": deployment_name,
                "branch": branch,
                "checkpoints_without_ordinal": unordered_retired,
            },
            fix_hint=(
                "backfill okg.generation_ordinals for this deployment "
                "(backfill_generation_ordinals) so generation order is "
                "established before any history is deleted"
            ),
        )
    crossed = sorted(set(retired) & protection)
    if crossed:
        raise SubstrateError(
            "history retention floor crosses protected generations",
            data={"protected_generations": crossed},
        )
    _refuse_run_bases_below(
        conn, branch=branch, retirement_point=retirement_point,
    )
    database_identity = {
        "dbname": conn.info.dbname,
        "host": conn.info.host,
        "port": conn.info.port,
    }
    binding = {
        "database_identity": database_identity,
        "deployment_name": deployment_name,
        "branch": branch,
        "policy_digest": policy.digest,
        "head_checkpoint": head.checkpoint_id,
        "checkpoint_vector": [cp.checkpoint_id for cp in checkpoints],
        "checkpoint_targets": {
            cp.checkpoint_id: cp.target_generation_id
            for cp in checkpoints
            if cp.target_generation_id is not None
        },
        "physical_tables": physical,
        "catalog_version_id": active_catalog_version,
        "protection_set": sorted(protection),
    }
    binding["binding_digest"] = _json_digest(binding)
    estimates = (
        _estimate_rows_and_bytes(
            conn, physical, floors, branch=branch, progress=progress
        )
        if include_estimates
        else {}
    )
    return {
        **binding,
        "planned_at": now_utc.isoformat(),
        "policy": policy.canonical(),
        # Where the rules came from. `policy_digest` (inside `binding`) already
        # pins the VALUES apply must re-verify; these two say out loud that the
        # operator never wrote them, so a printed default can never pass for a
        # declared one.
        "policy_source": resolved.source,
        "policy_notice": resolved.notice,
        "floors": floors,
        "retired_checkpoints": retired,
        # Reported, not part of `binding`/`binding_digest`: this is a
        # diagnostic ("compaction ran past these tombstoned branches"), not a
        # value apply must re-verify. Abandoning another branch between plan
        # and apply must not invalidate the plan.
        "ignored_abandoned_branches": ignored_abandoned_branches,
        "logical_estimates": estimates,
        # Reported, not part of `binding`/`binding_digest`: planner phase
        # timings are a diagnostic, and apply must never treat "the plan took
        # a different amount of time" as drift.
        "planner_progress": list(progress.phases) if progress is not None else [],
        "expected_index_impact": "dead tuples become reusable; physical bytes change only after offline reclaim",
    }


def _refuse_run_bases_below(
    conn: psycopg.Connection[Any],
    *,
    branch: str,
    retirement_point: int | None,
) -> None:
    """Refuse to delete history a source run's run base still reads.

    ``_checkpoints`` lists generations only, so a run-base checkpoint (issue
    #2993) is never retired or protected there. But while its run is in
    flight, or its undo unfinished, the undo reads rows as of that
    checkpoint; a retirement point above it would delete rows the undo
    must restore. Such a plan is refused until the run ends.
    """
    if retirement_point is None:
        return
    rows = conn.execute(
        """
        SELECT c.checkpoint_id, s.branch_point
          FROM _chronos_branch_interval_checkpoints AS c
          JOIN _chronos_branch_interval_segments AS s
            ON s.segment_id = c.segment_id
         WHERE c.branch_id = %s
           AND c.checkpoint_id LIKE 'okg-run-base:%%'
        """,
        (branch,),
    ).fetchall()
    blocking = sorted(
        str(row[0]) for row in rows if int(row[1]) < int(retirement_point)
    )
    if blocking:
        raise SubstrateError(
            "cannot compact history: a source run's run-base checkpoint "
            "still needs rows this plan would delete",
            data={"branch": branch, "run_base_checkpoints": blocking},
            fix_hint=(
                "a run base belongs to a source run that is still going, or "
                "to one that ended without its undo. Run `okg runtime "
                "finish-undo --deployment <deployment>` to see which: it lists "
                "every ended run's unfinished undo and every run base no undo "
                "needs, and with --apply finishes those undos and deletes "
                "those run bases. Wait for a run that is still going to end, "
                "then plan the compaction again"
            ),
        )


def _checkpoints(
    conn: psycopg.Connection[Any],
    store: Any,
    *,
    deployment_name: str,
    branch: str,
) -> list[_Checkpoint]:
    result: list[_Checkpoint] = []
    foreign: list[dict[str, str | None]] = []
    for cp in store.context.list_checkpoints(branch=branch):
        metadata = dict(cp.metadata or {})
        if metadata.get("kind") != "okg_generation":
            continue
        if metadata.get("deployment_name") != deployment_name:
            foreign.append(
                {
                    "checkpoint_id": str(cp.checkpoint_id),
                    "deployment_name": metadata.get("deployment_name"),
                }
            )
            continue
        if metadata.get("status") != "published":
            continue
        row = conn.execute(
            "SELECT branch_point FROM _chronos_branch_interval_segments WHERE segment_id = %s",
            (int(cp.ref),),
        ).fetchone()
        if row is None:
            raise SubstrateError(
                "generation checkpoint has an unresolvable interval segment",
                data={"checkpoint_id": cp.checkpoint_id, "segment_id": cp.ref},
            )
        ordinal_row = conn.execute(
            "SELECT ordinal FROM okg.generation_ordinals WHERE checkpoint_id = %s",
            (str(cp.checkpoint_id),),
        ).fetchone()
        result.append(
            _Checkpoint(
                checkpoint_id=str(cp.checkpoint_id),
                target_generation_id=_optional_int(
                    metadata.get("target_generation_id")
                ),
                point=int(row[0]),
                ordinal=(
                    int(ordinal_row[0])
                    if ordinal_row is not None and ordinal_row[0] is not None
                    else None
                ),
                created_at=_parse_time(cp.created_at),
                catalog_version_id=_optional_int(metadata.get("catalog_version_id")),
            )
        )
    if foreign:
        raise SubstrateError(
            "cannot compact a Chronos branch shared by another deployment",
            data={"deployment_name": deployment_name, "foreign_checkpoints": foreign},
        )
    # Never compare an ordinal against a `point`. Ordinals are small 1..N
    # integers; points are large Chronos interval coordinates. Substituting one
    # for the other puts two unrelated coordinate spaces in a single sort key,
    # where they interleave and silently reorder the list that head selection
    # (`checkpoints[-1]`), audit-tail protection (`checkpoints[-audit_tail:]`)
    # and floor selection are all positionally indexed off.
    #
    # A missing ordinal is NOT exceptional: `_maybe_mint_generation_ordinal`
    # mints at checkpoint creation but swallows failures by design, and the
    # newest checkpoint routinely has no ordinal row yet. So rank in two tiers
    # -- ordinaled checkpoints first, ordered among themselves; un-ordinaled
    # ones strictly after, ordered by point among themselves. Un-ordinaled
    # checkpoints can only be the un-minted tail, so "last" is the correct
    # position, and no comparison ever crosses the two spaces.
    #
    # This orders the list safely but does not license DELETING anything whose
    # position rests on that assumption. `_snapshot` carries a backstop refusal
    # for a retired checkpoint with no ordinal. In practice `_selector_floors`
    # reaches the same conclusion first -- once any checkpoint lacks an ordinal
    # it sorts last, becomes the floor candidate, and that function raises
    # "retention floor checkpoint has no generation ordinal". The backstop is
    # kept because it is the check that is stated in terms of what is actually
    # being deleted, and both are fail-closed.
    result.sort(
        key=lambda item: (
            (0, item.ordinal) if item.ordinal is not None else (1, item.point),
            item.created_at,
            item.checkpoint_id,
        )
    )
    return result


def _physical_tables(conn: psycopg.Connection[Any]) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT table_name, physical_table
          FROM _chronos_branch_tables
         WHERE backend = 'interval' AND table_name IN (%s, %s)
        """,
        (NODE_TABLE, EDGE_TABLE),
    ).fetchall()
    mapping = {str(row[0]): str(row[1]) for row in rows}
    missing = sorted({NODE_TABLE, EDGE_TABLE} - set(mapping))
    if missing:
        raise SubstrateError("Chronos interval physical table mapping is incomplete", data={"missing": missing})
    return mapping


def _assert_head_catalog_active(
    conn: psycopg.Connection[Any],
    *,
    catalog_version_id: int | None,
    deployment_name: str,
) -> int:
    if catalog_version_id is None:
        raise SubstrateError("published head is missing its active catalog version")
    row = conn.execute(
        "SELECT status, deployment_name FROM okg.catalog_versions WHERE catalog_version_id = %s",
        (catalog_version_id,),
    ).fetchone()
    if row is None or row[0] != "applied":
        raise SubstrateError(
            "published head catalog is not applied",
            data={"catalog_version_id": catalog_version_id},
        )
    catalog_deployment = row[1]
    if catalog_deployment is not None and str(catalog_deployment) != deployment_name:
        raise SubstrateError(
            "published head catalog belongs to another deployment",
            data={
                "catalog_version_id": catalog_version_id,
                "catalog_deployment": catalog_deployment,
                "deployment_name": deployment_name,
            },
        )
    return catalog_version_id


def _catalog_subtypes(
    conn: psycopg.Connection[Any], *, catalog_version_id: int
) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM okg.subtypes WHERE catalog_version_id = %s",
        (catalog_version_id,),
    ).fetchall()
    return {str(row[0]) for row in rows if row[0] is not None}


def _distinct_classifier(conn: psycopg.Connection[Any], table: str, column: str) -> set[str]:
    missing_query = sql.SQL("SELECT count(*) FROM {} WHERE {} IS NULL").format(
        _qualified_identifier(table), sql.Identifier(column)
    )
    missing = int(conn.execute(missing_query).fetchone()[0])
    if missing:
        raise SubstrateError(
            "physical history is missing its stored classifier",
            data={"table": table, "classifier_column": column, "rows": missing},
        )
    query = sql.SQL("SELECT DISTINCT {} FROM {} WHERE {} IS NOT NULL").format(
        sql.Identifier(column), _qualified_identifier(table), sql.Identifier(column)
    )
    return {str(row[0]) for row in conn.execute(query).fetchall()}


def _selector_floors(
    *,
    policy: HistoryRetentionPolicy,
    kind: str,
    classifiers: set[str],
    checkpoints: Sequence[_Checkpoint],
    now: datetime,
) -> dict[str, dict[str, Any]]:
    tail_index = max(0, len(checkpoints) - policy.audit_tail_generations)
    tail = checkpoints[tail_index]
    result: dict[str, dict[str, Any]] = {}
    for classifier in sorted(classifiers):
        rule = policy.rule_for(kind, classifier)
        selected: _Checkpoint | None
        if rule.mode == "full":
            selected = None
        elif rule.mode == "latest-only":
            selected = checkpoints[-1]
        else:
            cutoff = now - timedelta(seconds=int(rule.horizon_seconds or 0))
            selected = next((cp for cp in checkpoints if cp.created_at >= cutoff), checkpoints[-1])
        if selected is not None and tail.point < selected.point:
            selected = tail
        if selected is not None and selected.ordinal is None:
            raise SubstrateError(
                "retention floor checkpoint has no generation ordinal",
                data={"checkpoint_id": selected.checkpoint_id, "classifier": classifier},
            )
        result[classifier] = {
            "mode": rule.mode,
            "horizon_seconds": rule.horizon_seconds,
            "checkpoint_id": selected.checkpoint_id if selected else None,
            "point": selected.point if selected else None,
            "ordinal": selected.ordinal if selected else None,
        }
    return result


def _protection_set(
    conn: psycopg.Connection[Any],
    store: Any,
    *,
    deployment_name: str,
    branch: str,
    checkpoints: Sequence[_Checkpoint],
    audit_tail: int,
) -> tuple[set[str], list[str]]:
    """Return (protected checkpoint ids, abandoned branch ids that were ignored).

    A live child/fork branch still reads the parent's history, so compaction
    refuses while one exists. An **abandoned** branch is different: it is a
    tombstone, not a readable lineage.

    Before #1306 this loop rejected every non-`main`, non-target branch
    regardless of status, and `okg branch abandon` only stamps
    ``status: abandoned`` into metadata -- the row survives in
    ``_chronos_branch_interval_branches``. One `okg deployment fork-worktree`
    therefore disabled history compaction for the deployment permanently, with
    no CLI path to undo it. Abandoned branches are now skipped (and reported),
    so the documented remedy in the `fix_hint` below actually works.
    """
    protected = {checkpoints[-1].checkpoint_id}
    protected.update(cp.checkpoint_id for cp in checkpoints[-audit_tail:])
    branches = store.context.list_branches()
    ignored_abandoned: list[str] = []
    for info in branches:
        branch_id = str(getattr(info, "branch_id", ""))
        metadata = dict(getattr(info, "metadata", None) or {})
        if branch_id in {"main", branch}:
            continue
        if str(metadata.get("status") or "") == "abandoned":
            # Ignored, not protected: rows this branch can still see may be
            # deleted. That is the point -- an abandoned branch is not a
            # lineage anyone is entitled to read. `okg branch delete` removes
            # the row outright and returns its interval space to GC.
            ignored_abandoned.append(branch_id)
            continue
        raise SubstrateError(
            "cannot compact history while a child/fork branch exists",
            data={
                "branch": branch_id,
                "base_generation_id": metadata.get("base_generation_id"),
            },
            fix_hint=(
                "merge, `okg branch abandon`, or `okg branch delete` child "
                "branches before creating a compaction plan"
            ),
        )
    rows = conn.execute(
        """
        SELECT checkpoint_id, pin_name, expires_at
          FROM okg.history_generation_pins
         WHERE deployment_name = %s AND branch_id = %s
           AND (expires_at IS NULL OR expires_at > now())
        """,
        (deployment_name, branch),
    ).fetchall()
    checkpoint_ids = {cp.checkpoint_id for cp in checkpoints}
    for checkpoint_id, pin_name, _expires_at in rows:
        if str(checkpoint_id) not in checkpoint_ids:
            raise SubstrateError(
                "history generation pin references an unknown checkpoint",
                data={"pin_name": pin_name, "checkpoint_id": checkpoint_id},
            )
        protected.add(str(checkpoint_id))
    return protected, sorted(ignored_abandoned)


def _grouped_table_counts(
    conn: psycopg.Connection[Any],
    *,
    table: str,
    classifier: str,
    floors: Mapping[str, int],
    branch: str,
) -> dict[str, int]:
    """Every row population the plan reports, in ONE pass over the table.

    Two things were wrong with the shape this replaces (#1308).  The planner
    ran one ``count(*)`` per classifier value, so the work grew with the number
    of subtypes and edge types and never finished on a 61 GB interval table.
    And the four populations it reports -- whole relation, this branch, retirable
    in this branch, retirable in any branch -- were four more full scans that
    differ only by predicate.  Both collapse here: the per-classifier floors ride
    in as a two-column ``unnest`` that is LEFT JOINed (so non-retirable rows are
    still counted), branch ownership is a LEFT JOIN on the segment registry
    (whose ``segment_id`` is the primary key, so neither join can duplicate a
    row), and the four numbers come out as ``FILTER`` clauses over a single
    aggregate.

    ``pt`` is cast to ``numeric[]``, not ``bigint[]``: Chronos interval
    coordinates are ``NUMERIC(32,0)`` and the ``main`` branch's continuation
    segments carry points around 5e30, which overflow ``bigint`` outright.
    """
    values = sorted(floors)
    points = [str(int(floors[value])) for value in values]
    query = sql.SQL(
        "SELECT count(*) AS rows_total,"
        " count(*) FILTER (WHERE s.segment_id IS NOT NULL) AS rows_branch_total,"
        " count(*) FILTER (WHERE f.cls IS NOT NULL AND s.segment_id IS NOT NULL)"
        "   AS rows_eligible,"
        " count(*) FILTER (WHERE f.cls IS NOT NULL) AS rows_eligible_any_branch"
        " FROM {} AS t"
        " LEFT JOIN unnest(%s::text[], %s::numeric[]) AS f(cls, pt)"
        "   ON t.{} = f.cls AND t.live_hi <= f.pt"
        " LEFT JOIN _chronos_branch_interval_segments AS s"
        "   ON s.segment_id = t.writer_segment_id AND s.owner_branch_id = %s"
    ).format(_qualified_identifier(table), sql.Identifier(classifier))
    row = conn.execute(query, (values, points, branch)).fetchone()
    return {
        "rows_total": int(row[0]),
        "rows_branch_total": int(row[1]),
        "rows_eligible": int(row[2]),
        "rows_eligible_any_branch": int(row[3]),
    }


def _estimate_rows_and_bytes(
    conn: psycopg.Connection[Any],
    physical: Mapping[str, str],
    floors: Mapping[str, Mapping[str, Mapping[str, Any]]],
    *,
    branch: str,
    progress: PlannerProgress | None = None,
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for logical, name, classifier, selectors in (
        (NODE_TABLE, "nodes", "subtype", floors["nodes"]),
        (EDGE_TABLE, "edges", "edge_type", floors["edges"]),
    ):
        # The physical tables are shared by every Chronos branch, so there are
        # two different populations here and the plan reports both rather than
        # quietly mixing them:
        #   rows_total          -- the whole relation, every branch.
        #   rows_branch_total   -- rows this branch wrote.
        #   rows_eligible       -- what apply will actually delete (branch-scoped,
        #                          same predicate as `_delete_batches`).
        #   rows_eligible_ignoring_branch_scope
        #                       -- rows old enough to retire but owned by another
        #                          branch, which apply deliberately will NOT touch.
        # That last number matters: when a deployment was upgraded in place,
        # every pre-existing row was stamped with the bootstrap segment, which
        # branch `main` owns. Compacting `default` therefore reclaims none of
        # that history. That is the safe direction, but an operator expecting a
        # large reclaim must be able to SEE the shortfall instead of inferring
        # it from a disappointing result -- silently under-delivering on a
        # delete is its own kind of surprise.
        #
        # `logical_eligible_bytes_estimate` divides by `rows_total` on purpose:
        # `relation_bytes / rows_total` is the average bytes-per-row for the
        # whole relation, which is the right scale factor for any subset of it.
        bounded = {
            value: int(floor["point"])
            for value, floor in selectors.items()
            if floor["point"] is not None
        }
        with _phase(
            progress,
            f"estimate_{name}",
            table=physical[logical],
            classifiers=len(selectors),
            retiring_classifiers=len(bounded),
        ):
            counts = _grouped_table_counts(
                conn,
                table=physical[logical],
                classifier=classifier,
                floors=bounded,
                branch=branch,
            )
            relation_bytes = int(conn.execute("SELECT pg_total_relation_size(%s::regclass)", (physical[logical],)).fetchone()[0])
        total = counts["rows_total"]
        eligible = counts["rows_eligible"]
        output[name] = {
            "rows_total": total,
            "rows_branch_total": counts["rows_branch_total"],
            "rows_eligible": eligible,
            "rows_eligible_ignoring_branch_scope": (
                counts["rows_eligible_any_branch"] - eligible
            ),
            "logical_eligible_bytes_estimate": int(relation_bytes * eligible / max(1, total)),
            "physical_relation_bytes": relation_bytes,
        }
    return output


def _load_plan(conn: psycopg.Connection[Any], plan_id: str) -> tuple[dict[str, Any], str, dict[str, Any]]:
    try:
        parsed = str(uuid.UUID(plan_id))
    except ValueError as exc:
        raise SubstrateError("invalid history compaction plan id") from exc
    row = conn.execute(
        "SELECT plan, status, progress FROM okg.history_compaction_plans WHERE plan_id = %s",
        (parsed,),
    ).fetchone()
    if row is None:
        raise SubstrateError("history compaction plan not found", data={"plan_id": plan_id})
    return dict(row[0]), str(row[1]), dict(row[2] or {})


def _binding_drift(
    plan: Mapping[str, Any],
    current: Mapping[str, Any],
    *,
    progress: Mapping[str, Any],
) -> dict[str, Any]:
    keys = (
        "database_identity", "deployment_name", "branch", "policy_digest",
        "head_checkpoint", "checkpoint_vector", "checkpoint_targets", "physical_tables",
        "catalog_version_id", "protection_set",
    )
    drift = {
        key: {"planned": plan.get(key), "current": current.get(key)}
        for key in keys
        if plan.get(key) != current.get(key)
    }
    if progress.get("tombstones"):
        retired = set(plan.get("retired_checkpoints", []))
        expected_vector = [
            checkpoint
            for checkpoint in plan.get("checkpoint_vector", [])
            if checkpoint not in retired
        ]
        if expected_vector == current.get("checkpoint_vector"):
            drift.pop("checkpoint_vector", None)
        expected_targets = {
            checkpoint: target
            for checkpoint, target in plan.get("checkpoint_targets", {}).items()
            if checkpoint not in retired
        }
        if expected_targets == current.get("checkpoint_targets"):
            drift.pop("checkpoint_targets", None)
    elif plan.get("binding_digest") != current.get("binding_digest"):
        drift["binding_digest"] = {
            "planned": plan.get("binding_digest"),
            "current": current.get("binding_digest"),
        }
    return drift


def _commit_tombstone_barrier(conn: psycopg.Connection[Any], plan: Mapping[str, Any]) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "SELECT pg_advisory_xact_lock(%s, %s)",
            (_PROTECTION_LOCK_NAMESPACE, _PROTECTION_LOCK_KEY),
        )
        for checkpoint_id in plan["retired_checkpoints"]:
            target_generation_id = plan.get("checkpoint_targets", {}).get(checkpoint_id)
            cur.execute(
                """
                INSERT INTO okg.compacted_generations
                  (checkpoint_id, target_generation_id, deployment_name, branch_id, plan_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (checkpoint_id) DO UPDATE
                  SET target_generation_id = EXCLUDED.target_generation_id,
                      deployment_name = EXCLUDED.deployment_name,
                      branch_id = EXCLUDED.branch_id,
                      plan_id = EXCLUDED.plan_id,
                      compacted_at = now(),
                      reason = EXCLUDED.reason
                """,
                (
                    checkpoint_id,
                    target_generation_id,
                    plan["deployment_name"],
                    plan["branch"],
                    plan["plan_id"],
                ),
            )
            cur.execute(
                "DELETE FROM _chronos_branch_interval_checkpoints WHERE checkpoint_id = %s",
                (checkpoint_id,),
            )


def _delete_batches(
    conn: psycopg.Connection[Any],
    *,
    plan: Mapping[str, Any],
    batch_name: str,
    batch_size: int,
    already_deleted: int,
    on_progress: Any,
) -> int:
    total_deleted = already_deleted
    if batch_name in {"nodes", "edges"}:
        logical = NODE_TABLE if batch_name == "nodes" else EDGE_TABLE
        classifier = "subtype" if batch_name == "nodes" else "edge_type"
        table = str(plan["physical_tables"][logical])
        selectors = plan["floors"][batch_name]
        for value, floor in selectors.items():
            point = floor.get("point")
            if point is None:
                continue
            while True:
                with conn.transaction(), conn.cursor() as cur:
                    # Scope every delete to segments this branch owns. The
                    # physical node/edge interval tables are SHARED across all
                    # Chronos branches and carry no branch column, so
                    # `live_hi <= point` alone is not a branch-local predicate:
                    # `point` is derived from the target branch's checkpoints,
                    # but a row matching it may still be visible from another
                    # branch (notably the `main` root, which `_protection_set`
                    # permits to coexist and whose checkpoints `_checkpoints`
                    # never enumerates, so they are neither protected nor
                    # tombstoned -- a read there would silently come back
                    # short). Restricting to
                    # `writer_segment_id` in the branch's own segment lineage
                    # is conservative by construction: rows inherited from a
                    # parent branch are simply never reclaimed, which is the
                    # safe direction to err.
                    cur.execute(
                        sql.SQL(
                            "DELETE FROM {} WHERE ctid IN (SELECT ctid FROM {} "
                            "WHERE {} = %s AND live_hi <= %s "
                            "AND writer_segment_id IN ("
                            "  SELECT segment_id FROM _chronos_branch_interval_segments "
                            "   WHERE owner_branch_id = %s"
                            ") LIMIT %s)"
                        ).format(
                            _qualified_identifier(table),
                            _qualified_identifier(table),
                            sql.Identifier(classifier),
                        ),
                        (value, int(point), str(plan["branch"]), batch_size),
                    )
                    deleted = int(cur.rowcount)
                    total_deleted += deleted
                    _write_progress(cur, str(plan["plan_id"]), batch_name, total_deleted)
                on_progress(total_deleted)
                if deleted < batch_size:
                    break
    else:
        raise SubstrateError(
            "unknown history compaction batch",
            data={"batch": batch_name, "batches": list(_DATA_BATCHES)},
        )
    return total_deleted


def _write_progress(cur: Any, plan_id: str, batch_name: str, deleted: int) -> None:
    cur.execute(
        """
        UPDATE okg.history_compaction_plans
           SET progress = jsonb_set(progress, %s, %s::jsonb, true), updated_at = now()
         WHERE plan_id = %s
        """,
        ([batch_name], json.dumps({"complete": False, "deleted": deleted}), plan_id),
    )


def _batch_progress(
    conn: psycopg.Connection[Any],
    plan_id: str,
    progress: dict[str, Any],
    batch_name: str,
    deleted: int,
) -> None:
    progress[batch_name] = {"complete": False, "deleted": deleted}


def _persist_progress(
    conn: psycopg.Connection[Any],
    plan_id: str,
    progress: Mapping[str, Any],
) -> None:
    conn.execute(
        "UPDATE okg.history_compaction_plans SET progress = %s::jsonb, updated_at = now() WHERE plan_id = %s",
        (json.dumps(progress), plan_id),
    )
    conn.commit()


def _set_plan_status(
    conn: psycopg.Connection[Any],
    plan_id: str,
    status: str,
    *,
    progress: Mapping[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        UPDATE okg.history_compaction_plans
           SET status = %s,
               progress = COALESCE(%s::jsonb, progress),
               updated_at = now()
         WHERE plan_id = %s
        """,
        (status, json.dumps(progress) if progress is not None else None, plan_id),
    )
    conn.commit()


def _first_column(row: Any) -> Any:
    """Read the first column of a fetched row on any row factory.

    These lock helpers run on whatever connection the caller already holds, and
    those do not agree on a row factory: the compaction paths use the default
    tuple rows, while `create_branch` reaches here holding a dict-row
    connection. Indexing `row[0]` on a dict row raises `KeyError: 0` -- which
    crashed branch creation outright instead of reporting lock contention.
    """
    if row is None:
        return None
    if isinstance(row, Mapping):
        return next(iter(row.values()), None)
    return row[0]


@contextmanager
def _deployment_compaction_lock(
    conn: psycopg.Connection[Any], deployment: str
) -> Iterator[None]:
    key = int.from_bytes(
        hashlib.sha256(f"okg.history_compaction.{deployment}".encode()).digest()[:4],
        "big",
        signed=True,
    )
    row = conn.execute("SELECT pg_try_advisory_lock(%s, %s)", (_COMPACTION_LOCK_NAMESPACE, key)).fetchone()
    if not bool(_first_column(row)):
        raise SubstrateError("another history compaction holds the deployment lock")
    try:
        yield
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s, %s)", (_COMPACTION_LOCK_NAMESPACE, key))


@contextmanager
def _history_protection_lock(conn: psycopg.Connection[Any]) -> Iterator[None]:
    """Serialize destructive apply with branch/pin/checkpoint protection writes."""
    row = conn.execute(
        "SELECT pg_try_advisory_lock(%s, %s)",
        (_PROTECTION_LOCK_NAMESPACE, _PROTECTION_LOCK_KEY),
    ).fetchone()
    if not bool(_first_column(row)):
        raise SubstrateError(
            "history protection state is being changed; refusing compaction",
            fix_hint="wait for branch/pin/read-lease acquisition to finish, then retry",
        )
    try:
        yield
    finally:
        conn.execute(
            "SELECT pg_advisory_unlock(%s, %s)",
            (_PROTECTION_LOCK_NAMESPACE, _PROTECTION_LOCK_KEY),
        )


def _install_protection_coordination(cur: Any) -> None:
    cur.execute(
        """
        CREATE OR REPLACE FUNCTION okg.lock_history_compaction_protection()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          PERFORM pg_advisory_xact_lock(1326131011, 1);
          RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
        END;
        $$
        """
    )
    cur.execute(
        "DROP TRIGGER IF EXISTS history_generation_pins_coordination "
        "ON okg.history_generation_pins"
    )
    cur.execute(
        "CREATE TRIGGER history_generation_pins_coordination "
        "BEFORE INSERT OR UPDATE OR DELETE ON okg.history_generation_pins "
        "FOR EACH STATEMENT EXECUTE FUNCTION okg.lock_history_compaction_protection()"
    )
    for table, trigger in (
        ("_chronos_branch_interval_branches", "chronos_branch_history_coordination"),
        ("_chronos_branch_interval_checkpoints", "chronos_checkpoint_history_coordination"),
    ):
        present = cur.execute("SELECT to_regclass(%s)", (f"public.{table}",)).fetchone()
        if present is None or present[0] is None:
            continue
        cur.execute(
            sql.SQL("DROP TRIGGER IF EXISTS {} ON {}").format(
                sql.Identifier(trigger), sql.Identifier("public", table)
            )
        )
        cur.execute(
            sql.SQL(
                "CREATE TRIGGER {} BEFORE INSERT OR UPDATE OR DELETE ON {} "
                "FOR EACH STATEMENT EXECUTE FUNCTION okg.lock_history_compaction_protection()"
            ).format(sql.Identifier(trigger), sql.Identifier("public", table))
        )


def _relation_sizes(conn: psycopg.Connection[Any], tables: Sequence[str]) -> dict[str, int]:
    return {
        table: int(conn.execute("SELECT pg_total_relation_size(%s::regclass)", (table,)).fetchone()[0])
        for table in tables
    }


def _qualified_identifier(value: str) -> sql.Composed | sql.Identifier:
    parts = value.split(".")
    if len(parts) == 1:
        return sql.Identifier(parts[0])
    if len(parts) == 2:
        return sql.Identifier(parts[0], parts[1])
    raise SubstrateError("invalid physical table identifier", data={"table": value})


def _parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
