"""Deployment readiness checks (`_ready_*`).

Extracted from ``cli/deployment.py`` by ``decompose-deployment-cli-godmodules``
as a PURE re-export-shim split. Every reference to a shared helper, constant, or
sibling function is routed through ``_dep`` (the live ``cli.deployment`` module
object) so that tests which ``monkeypatch.setattr`` names on
``okg.substrate.cli.deployment`` still affect calls made here. ``cli.deployment``
remains the canonical home of the lazy helper globals + ``_load_main_helpers``
and re-exports these functions at its bottom.
"""
from __future__ import annotations

from okg.substrate import fastyaml  # noqa: F401 - used by moved function bodies
from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.unknown import Unknown, read_ok
from okg.substrate.runtime_dlq import (
    current_runtime_dlq,  # noqa: F401 - re-exported for existing callers
    probe_current_runtime_dlq,
)
from okg.substrate.runtime_queue_health import (
    DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES,
    _ignored_runtime_workflow_names,
    summarize_runtime_queue_health,
)

import contextlib
import json  # noqa: F401
import os
import shlex  # noqa: F401
import sys
import tempfile  # noqa: F401
import time
from collections.abc import Iterator, Mapping  # noqa: F401
from datetime import datetime, timezone  # noqa: F401
from pathlib import Path  # noqa: F401
from typing import Any  # noqa: F401
from okg.substrate.support.user_hints import okg_prefix

# NOTE: `import okg.substrate.cli.deployment as _dep` is done at the BOTTOM of
# this module (after all defs) — see the end of the file. The moved functions
# below reference `_dep` only at CALL time, so a forward module-global is fine,
# and deferring the import keeps the circular `deployment <-> readiness` cycle
# safe regardless of which module is imported first.

__all__ = [
    "_ready_add_check",
    "_ready_add_generation_count_integrity_check",
    "_ready_add_source_sync_liveness_check",
    "_ready_as_float",
    "_ready_build_source_sync_status_contract",
    "_ready_database_status",
    "_ready_postgres_capacity_status",
    "_ready_declared_baseline_source_groups",
    "_ready_doctor_status",
    "_ready_embeddings_enabled",
    "_ready_generation_status",
    "_ready_is_workspace_graph_deployment",
    "_ready_load_yaml_mapping",
    "_ready_mcp_status",
    "_READY_NOMOS_ACTIVE_STATE_PRESENT_MESSAGE",
    "_ready_nomos_active_state_message",
    "_ready_nomos_active_state_status",
    "_ready_nomos_blocking_required",
    "_ready_nomos_audit_status",
    "_ready_nomos_read_probe_status",
    "_ready_nomos_source_lineage_status",
    "_ready_parse_datetime",
    "_ready_parse_json",
    "_ready_progress_freshness_sources",
    "_ready_no_change_expected_delta_is_all_zero",
    "_ready_queue_status",
    "_ready_required_dependency",
    "_ready_rollout_project_command",
    "_ready_rollout_repo_root",
    "_ready_rollout_source_registry",
    "_ready_rollout_source_sync_summary",
    "_ready_rollout_status",
    "_ready_runtime_config_warning",
    "_ready_runtime_source_sync",
    "_ready_slug_for_dsn",
    "_ready_source_freshness_status",
    "_ready_source_aliases",
    "_ready_source_groups_for_profile",
    "_ready_source_optimization_full_pipeline_proof_failed",
    "_ready_source_optimization_full_pipeline_proof_passed",
    "_ready_source_optimization_has_full_pipeline_proof",
    "_ready_source_optimization_latest_sample_over_budget",
    "_ready_source_optimization_no_change_dominant",
    "_ready_source_optimization_observability_gap",
    "_ready_source_optimization_observability_gaps",
    "_ready_source_optimization_proof_age_seconds",
    "_ready_publisher_lock_headroom_status",
    "_ready_add_publisher_lock_headroom_check",
    "_ready_source_optimization_publisher_lock_contention",
    "_ready_source_optimization_publisher_path_blocker",
    "_ready_source_optimization_status",
    "_ready_source_optimization_status_from_ledger",
    "_ready_source_optimization_violation_reason",
    "_ready_source_progress_freshness_row",
    "_ready_source_sync_liveness_status",
    "_ready_source_sync_recovery_commands",
]


def _ready_source_groups_for_profile(
    readiness_profile: str,
    *,
    deployment: str | None = None,
    deployment_dir: Path | None = None,
) -> dict[str, tuple[str, ...]]:
    if readiness_profile == "ci":
        if deployment_dir is not None:
            from okg.substrate.source_sync_convergence import (
                readiness_source_groups_for_deployment_dir,
            )

            return readiness_source_groups_for_deployment_dir(
                deployment_dir,
                (readiness_profile,),
            )
        return _dep._READY_CI_SOURCE_GROUPS
    if readiness_profile == "dogfood":
        if deployment_dir is not None and not _dep._ready_is_workspace_graph_deployment(
            deployment=deployment,
            deployment_dir=deployment_dir,
        ):
            return _dep._ready_declared_baseline_source_groups(deployment_dir)
        return _dep._READY_AGENT_CONTEXT_SOURCE_GROUPS
    return {}


def _ready_is_workspace_graph_deployment(
    *,
    deployment: str | None,
    deployment_dir: Path,
) -> bool:
    # Detection is manifest-based only (profile name or module set) — the
    # deployment-qa capability forbids branching on a deployment NAME, and
    # okg-workspace's manifest satisfies both detectors anyway.
    del deployment
    manifest = _dep._ready_load_yaml_mapping(deployment_dir / "deployment.yaml")
    profile = manifest.get("profile") if isinstance(manifest, dict) else {}
    profile = profile if isinstance(profile, dict) else {}
    profile_name = str(profile.get("name") or "").strip()
    if profile_name == "workspace-graph":
        return True
    modules = {
        str(module)
        for module in (manifest.get("modules") or [])
        if isinstance(module, str)
    }
    return _dep._READY_WORKSPACE_GRAPH_MODULES.issubset(modules)


def _ready_declared_baseline_source_groups(
    deployment_dir: Path,
) -> dict[str, tuple[str, ...]]:
    manifest = _dep._ready_load_yaml_mapping(deployment_dir / "deployment.yaml")
    registry_path = _dep._ready_rollout_source_registry(
        deployment_dir=deployment_dir,
        manifest=manifest,
    )
    if not registry_path.is_file():
        return {}
    from okg.substrate.sources.registry import load_source_registry

    registry = load_source_registry(registry_path)
    baseline_sources = tuple(
        sorted(
            str(source_name)
            for source_name, entry in registry.items()
            if bool(getattr(entry, "required_for_baseline", False))
        )
    )
    if not baseline_sources:
        return {}
    return {"declared_baseline": baseline_sources}


def _ready_progress_freshness_sources(
    deployment_dir: Path,
    readiness_profile: str,
) -> set[str]:
    """Sources whose readiness freshness is source progress, not completed scope."""
    if readiness_profile != "ci":
        return set()
    try:
        from okg.substrate.source_sync import (
            SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES,
        )
        from okg.substrate.sources.registry import load_source_registry

        manifest = _dep._ready_load_yaml_mapping(deployment_dir / "deployment.yaml")
        registry_path = _dep._ready_rollout_source_registry(
            deployment_dir=deployment_dir,
            manifest=manifest,
        )
        registry = load_source_registry(registry_path)
    except Exception:  # noqa: BLE001 - fail closed to completed-scope freshness.
        return set()

    sources: set[str] = set()
    for source_name, entry in registry.items():
        if str(source_name) not in _dep.CI_READY_SOURCES:
            continue
        raw = getattr(entry, "raw", None)
        raw = raw if isinstance(raw, Mapping) else {}
        params = raw.get("params") if isinstance(raw.get("params"), Mapping) else {}
        sync = getattr(entry, "sync", None)
        default_mode = str(getattr(sync, "default_event_mode", "") or "")
        reconcile_mode = str(getattr(sync, "reconcile_mode", "") or "")
        append_only = bool(params.get("append_only")) or (
            str(raw.get("deletion_semantics") or "") == "append_only"
        )
        completed_scope_configured = (
            default_mode in SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES
            or reconcile_mode in SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES
        )
        if append_only or (sync is not None and not completed_scope_configured):
            sources.add(str(source_name))
    return sources


def _ready_nomos_source_lineage_status(
    conn: Any,
    *,
    deployment_name: str,
    readiness_profile: str,
    generation_id: Any,
) -> dict[str, Any]:
    from okg.substrate.nomos.doctor import source_lineage_readiness_report

    return source_lineage_readiness_report(
        conn,
        deployment_name=deployment_name,
        readiness_profile=readiness_profile,
        generation_id=generation_id,
    )


def _ready_nomos_read_probe_status(
    conn: Any,
    *,
    deployment_name: str,
    deployment_dir: Path | None = None,
    readiness_profile: str = "general",
    manifest_override: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Probe: can the standard read subjects actually read at the active bundle?

    Per ``fix-nomos-read-path-ux`` (req.readux.readiness-read-probe, GitHub
    #640): a deployment can look healthy while every read surface is 100%
    deny, because readiness never evaluated the subjects the read paths
    actually present. This check evaluates the standard read subjects — the
    manifest's ``read_principals`` subjects at the CLI search boundary and
    ``subject:mcp_client`` at the MCP boundaries — against the ACTIVE bundle
    for the primary read surfaces, and reports any denied ones.

    Read-only: probes run through ``evaluate_runtime_boundary`` (the same
    evaluator the live boundaries use, with the same boundary resource
    shapes) WITHOUT persisting decisions, so readiness never pollutes the
    ``nomos_decisions`` audit trail. Extra probe facts cannot change any
    decision — rule matching is subset-based on ``rule.match`` keys.

    Skipped when the deployment's effective enforcement mode is ``off``
    (no boundary is evaluated at runtime). In ``audit`` mode the probes use
    log-and-allow semantics, matching what a live caller experiences.
    """
    from okg.substrate.nomos import (
        MCP_CLIENT_SUBJECT as _MCP_CLIENT_SUBJECT,
        NomosRuntimeRequest,
        deployment_nomos_enforcement_mode,
        evaluate_runtime_boundary,
        load_active_policy_bundle,
        manifest_nomos_enforcement_mode,
        manifest_read_principal_subjects,
    )

    mode = (
        manifest_nomos_enforcement_mode(manifest_override)
        if manifest_override is not None
        else deployment_nomos_enforcement_mode(deployment_name)
    )
    scope = f"deployment:{deployment_name}"
    base = {
        "schema": "okg.nomos.read_path_probe.v1",
        "deployment": deployment_name,
        "scope": scope,
        "effective_mode": mode,
        "readiness_profile": readiness_profile,
    }
    if mode == "off":
        return {
            **base,
            "ok": None,
            "status": "skipped",
            "reason": "nomos.runtime_enforcement is off",
        }

    manifest: Mapping[str, Any] = manifest_override or {}
    if manifest_override is None and deployment_dir is not None:
        manifest = _dep._ready_load_yaml_mapping(
            Path(deployment_dir) / "deployment.yaml"
        )
    read_subjects = manifest_read_principal_subjects(manifest)

    # (subject, surface, boundary resource_kind) — the same shapes the live
    # call sites present: cli/_read_side._authorize_search_with_nomos and
    # mcp/server._authorize_mcp_runtime_boundary.
    probe_specs: list[tuple[str, str, str]] = [
        (subject, "graph.read.cli_search", "cli_search_boundary")
        for subject in read_subjects
    ]
    probe_specs.append((_MCP_CLIENT_SUBJECT, "mcp.tool.call", "mcp_tool"))
    probe_specs.append(
        (_MCP_CLIENT_SUBJECT, "graph.read.mcp", "mcp_graph_read_boundary"),
    )

    bundle = load_active_policy_bundle(
        conn,
        deployment_name=deployment_name,
        scope=scope,
    )
    probes: list[dict[str, Any]] = []
    denied: list[dict[str, Any]] = []
    for subject, surface, resource_kind in probe_specs:
        outcome = evaluate_runtime_boundary(
            bundle,
            NomosRuntimeRequest(
                surface=surface,
                subject=subject,
                resource={
                    "deployment": deployment_name,
                    "scope": scope,
                    "resource_kind": resource_kind,
                    "probe": "deployment_ready.read_probe",
                },
                context={"deployment": deployment_name, "scope": scope},
            ),
            active_policy_fingerprint=(
                bundle.policy_fingerprint if bundle is not None else None
            ),
            performed_obligations=("audit",),
            enforcement_mode=mode,
        )
        row = {
            "subject": subject,
            "surface": surface,
            "resource_kind": resource_kind,
            "allowed": outcome.allowed,
            "effect": outcome.decision.effect,
            "final_effect": outcome.final_effect,
            "reason_code": outcome.decision.reason_code,
            "matched_rule_ids": list(outcome.decision.matched_rule_ids),
        }
        probes.append(row)
        if not outcome.allowed:
            denied.append({
                "subject": subject,
                "surface": surface,
                "reason_code": outcome.decision.reason_code,
            })

    findings = [
        {
            "code": "nomos.runtime.read_subject_denied",
            "reason_code": row["reason_code"],
            "severity": "error",
            "message": (
                f"standard read subject {row['subject']} is denied "
                f"{row['surface']} at the active bundle"
            ),
            "subject": row["subject"],
            "surface": row["surface"],
            "path": "okg.nomos_policy_activations",
        }
        for row in denied
    ]
    return {
        **base,
        "ok": not denied,
        "status": "pass" if not denied else "failed",
        "active_policy_bundle_id": (
            bundle.policy_bundle_id if bundle is not None else None
        ),
        "read_subjects": list(read_subjects),
        "probes": probes,
        "denied": denied,
        "findings": findings,
    }


def _ready_build_source_sync_status_contract(
    source_sync_liveness: dict[str, Any],
    *,
    source_freshness: Any,
    generation: Any,
    mcp: Any,
) -> dict[str, Any]:
    from okg.substrate.source_sync_status import (
        build_source_sync_status_contract,
    )

    return build_source_sync_status_contract(
        source_sync_liveness,
        source_freshness=source_freshness,
        generation=generation,
        mcp=mcp,
    )


# ---------------------------------------------------------------------------
# Bounded readiness probes (issue #1290)
#
# Readiness ran its live probes serially with no execution bound. On
# 2026-08-15 one DLQ query in the dogfood status path exceeded 90 seconds and
# the operator got nothing back: worker, generation, source, policy and
# database all read as unavailable although several were independently
# readable. A row-count limit does not bound execution time -- only a
# `statement_timeout` does -- and a probe that never returns suppresses every
# probe queued behind it.
#
# Three rules follow, and they are the whole contract:
#
#   1. Every database probe runs under an explicit statement timeout.
#   2. A probe that exceeds its bound reads `unknown`. Not a pass, and not a
#      downstream failure -- the defect being fixed is precisely
#      unknown-rendered-as-known, so rendering it as a different wrong answer
#      would not be a fix.
#   3. Independent probes still run, and the whole run is bounded by a total
#      deadline. What could be measured is reported.
# ---------------------------------------------------------------------------

READINESS_PROBE_TIMEOUT_ENV = "OKG_READINESS_PROBE_TIMEOUT_SECONDS"
READINESS_TOTAL_DEADLINE_ENV = "OKG_READINESS_TOTAL_DEADLINE_SECONDS"
#: Per-probe bound. The incident's DLQ query ran past 90s; 30s is well above
#: any healthy probe here and well below an operator's patience.
DEFAULT_READINESS_PROBE_TIMEOUT_SECONDS = 30.0
#: Whole-run bound, including the doctor subprocess on dogfood profiles.
DEFAULT_READINESS_TOTAL_DEADLINE_SECONDS = 180.0
READINESS_PROBE_TIMEOUT = "readiness_probe_timeout"
READINESS_DEADLINE_EXCEEDED = "readiness_deadline_exceeded"
READINESS_UNKNOWN_STATUS = "unknown"


def _ready_positive_float_env(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


class ReadinessDeadline:
    """Wall-clock budget for one readiness run.

    Holds the total budget and the per-probe budget. The per-probe budget is
    clamped to whatever remains of the total, so the last probe cannot
    overrun the run.
    """

    def __init__(
        self,
        *,
        total_seconds: float | None = None,
        per_probe_seconds: float | None = None,
        clock: Any = None,
    ) -> None:
        self._clock = clock or time.monotonic
        self.total_seconds = float(
            total_seconds
            if total_seconds is not None
            else _ready_positive_float_env(
                READINESS_TOTAL_DEADLINE_ENV,
                DEFAULT_READINESS_TOTAL_DEADLINE_SECONDS,
            )
        )
        self.per_probe_seconds = float(
            per_probe_seconds
            if per_probe_seconds is not None
            else _ready_positive_float_env(
                READINESS_PROBE_TIMEOUT_ENV,
                DEFAULT_READINESS_PROBE_TIMEOUT_SECONDS,
            )
        )
        self.started_at = self._clock()

    def elapsed(self) -> float:
        return max(0.0, self._clock() - self.started_at)

    def remaining(self) -> float:
        return max(0.0, self.total_seconds - self.elapsed())

    def expired(self) -> bool:
        return self.remaining() <= 0.0

    def probe_budget(self) -> float:
        """Seconds this probe may take: its own bound, capped by the run's."""
        return max(0.0, min(self.per_probe_seconds, self.remaining()))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "okg.readiness_deadline.v1",
            "total_seconds": self.total_seconds,
            "per_probe_seconds": self.per_probe_seconds,
            "elapsed_seconds": round(self.elapsed(), 3),
            "remaining_seconds": round(self.remaining(), 3),
        }


def _ready_is_query_timeout(exc: BaseException) -> bool:
    """True when `exc` is PostgreSQL cancelling a statement at its timeout.

    Matches on the SQLSTATE rather than the exception class so it holds for
    both `psycopg.errors.QueryCanceled` and drivers that surface the same
    condition as a plain `OperationalError`.
    """
    sqlstate = getattr(exc, "sqlstate", None) or getattr(
        getattr(exc, "diag", None), "sqlstate", None
    )
    if sqlstate in {"57014", "57P01"}:
        return True
    try:
        import psycopg
    except Exception:  # noqa: BLE001 - classification must never raise
        return False
    return isinstance(exc, psycopg.errors.QueryCanceled)


@contextlib.contextmanager
def ready_statement_timeout(conn: Any, seconds: float) -> Iterator[None]:
    """Bound every statement on `conn` for the duration of the block.

    Readiness owns this connection exclusively, so a session-level
    `statement_timeout` is the right instrument: it bounds each statement a
    probe issues, including ones inside helpers that never see this call.
    """
    milliseconds = max(1, int(round(seconds * 1000)))
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {milliseconds}")
    except Exception:  # noqa: BLE001 - an unbounded probe is still better than no probe
        yield
        return
    try:
        yield
    finally:
        with contextlib.suppress(Exception):
            with conn.cursor() as cur:
                cur.execute("RESET statement_timeout")


def _ready_record_probe(
    payload: dict[str, Any],
    *,
    probe_id: str,
    outcome: str,
    elapsed_seconds: float,
    timeout_seconds: float | None,
) -> None:
    """Append this probe's execution record. Every probe, every outcome."""
    payload.setdefault("probes", []).append({
        "probe_id": probe_id,
        "outcome": outcome,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "timeout_seconds": (
            None if timeout_seconds is None else round(timeout_seconds, 3)
        ),
    })


def _ready_skip_unknown_dependent(
    payload: dict[str, Any],
    *,
    check_id: str,
    message: str,
    depends_on: str,
) -> dict[str, Any]:
    """Record a probe skipped because its INPUT was never measured.

    Not a blocker: the dependency was not proven broken, it was not read.
    Running the probe anyway on a null input is what turns one unresolved
    value into a page of downstream policy blockers.
    """
    details = {
        "ok": None,
        "status": READINESS_UNKNOWN_STATUS,
        "reason": "readiness_dependency_unknown",
        "depends_on": depends_on,
        "state_semantics": _dep.READY_STATE_SEMANTICS,
    }
    _dep._ready_add_check(
        payload, check_id, READINESS_UNKNOWN_STATUS, message, details,
    )
    return details


def _ready_probe_timed_out(details: Any) -> bool:
    """True when `details` describe a probe that ran out of time.

    A timed-out probe is not a failed dependency: its state was never
    measured, so nothing downstream may be inferred from it.
    """
    if not isinstance(details, Mapping):
        return False
    return str(details.get("reason") or "") in {
        READINESS_PROBE_TIMEOUT,
        READINESS_DEADLINE_EXCEEDED,
    }


def _ready_add_check(
    payload: dict[str, Any],
    check_id: str,
    status: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> None:
    check = {
        "id": check_id,
        "status": status,
        "ok": True if status == "pass" else False if status == "blocker" else None,
        "message": message,
    }
    if details:
        check["details"] = details
    payload.setdefault("checks", []).append(check)
    if status == "blocker":
        payload.setdefault("blockers", []).append(check)
    elif status == "warning":
        payload.setdefault("warnings", []).append(check)
    elif status == READINESS_UNKNOWN_STATUS:
        # Issue #1290. An unproven probe is collected separately so it can
        # never be counted as a pass and never be reported as a blocker the
        # operator would try to repair.
        payload.setdefault("unknowns", []).append(check)


def _ready_finalize_payload_state(payload: dict[str, Any]) -> None:
    # An unknown probe is not a pass (issue #1290): readiness cannot be
    # asserted from evidence that was never collected.
    payload["ok"] = not payload.get("blockers") and not payload.get("unknowns")
    payload["status"] = _dep._ready_payload_status(payload)
    payload["state_semantics"] = _dep.READY_STATE_SEMANTICS


def _ready_payload_status(payload: Mapping[str, Any]) -> str:
    blockers = [
        row for row in payload.get("blockers") or []
        if isinstance(row, Mapping)
    ]
    unknowns = [
        row for row in payload.get("unknowns") or []
        if isinstance(row, Mapping)
    ]
    if not blockers:
        return READINESS_UNKNOWN_STATUS if unknowns else "clean"
    if any(_dep._ready_check_is_command_failed(row) for row in blockers):
        return "command_failed"
    if any(_dep._ready_check_is_unavailable(row) for row in blockers):
        return "unavailable"
    return "stale"


def _ready_check_is_command_failed(row: Mapping[str, Any]) -> bool:
    details = _dep._ready_mapping(row.get("details"))
    return str(details.get("status") or "") == "command_failed"


def _ready_check_is_unavailable(row: Mapping[str, Any]) -> bool:
    check_id = str(row.get("id") or "")
    if check_id.startswith("env.") or check_id.endswith(".unavailable"):
        return True
    if check_id in _dep._ready_unavailable_blocker_ids():
        return True
    details = _dep._ready_mapping(row.get("details"))
    status = str(details.get("status") or "")
    reason = str(details.get("reason") or "")
    if (
        check_id == "deployment.code_drift"
        and status in {"error", "misconfigured"}
    ):
        return True
    return status == "unavailable" or reason == "readiness_dependency_failed"


def _ready_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _ready_required_dependency(
    payload: dict[str, Any],
    *,
    check_id: str,
    dependency: str,
    message: str,
    call: Any,
    deadline: "ReadinessDeadline | None" = None,
) -> tuple[bool, Any]:
    """Run one readiness probe under the run's deadline (issue #1290).

    Returns ``(ok, result)``. On a failure the result is the blocker detail
    dict, as before. On a TIMEOUT the result is an ``unknown`` detail dict —
    callers must consult :func:`_ready_probe_timed_out` before concluding
    anything downstream, because nothing about this dependency was measured.
    """
    budget = deadline.probe_budget() if deadline is not None else None
    if deadline is not None and budget is not None and budget <= 0.0:
        details = {
            "ok": None,
            "status": READINESS_UNKNOWN_STATUS,
            "reason": READINESS_DEADLINE_EXCEEDED,
            "dependency": dependency,
            "probe_id": dependency,
            "deadline": deadline.to_dict(),
            "state_semantics": _dep.READY_STATE_SEMANTICS,
        }
        _dep._ready_add_check(
            payload,
            check_id,
            READINESS_UNKNOWN_STATUS,
            f"{message}: readiness deadline exceeded before this probe ran",
            details,
        )
        _ready_record_probe(
            payload,
            probe_id=dependency,
            outcome="deadline_exceeded",
            elapsed_seconds=0.0,
            timeout_seconds=budget,
        )
        return False, details
    started = time.monotonic()
    try:
        result = call()
    except Exception as exc:  # noqa: BLE001 - readiness must fail loudly.
        elapsed = time.monotonic() - started
        if _ready_is_query_timeout(exc):
            details = {
                "ok": None,
                "status": READINESS_UNKNOWN_STATUS,
                "reason": READINESS_PROBE_TIMEOUT,
                "dependency": dependency,
                "probe_id": dependency,
                "timeout_seconds": budget,
                "elapsed_seconds": round(elapsed, 3),
                "error": type(exc).__name__,
                "message": str(exc),
                "state_semantics": _dep.READY_STATE_SEMANTICS,
            }
            _dep._ready_add_check(
                payload,
                check_id,
                READINESS_UNKNOWN_STATUS,
                f"{message}: probe exceeded its deadline and was not measured",
                details,
            )
            _ready_record_probe(
                payload,
                probe_id=dependency,
                outcome="timeout",
                elapsed_seconds=elapsed,
                timeout_seconds=budget,
            )
            return False, details
        details = {
            "ok": False,
            "status": "unavailable",
            "reason": "readiness_dependency_failed",
            "dependency": dependency,
            "error": type(exc).__name__,
            "message": str(exc),
            "state_semantics": _dep.READY_STATE_SEMANTICS,
        }
        _dep._ready_add_check(
            payload,
            check_id,
            "blocker",
            message,
            details,
        )
        _ready_record_probe(
            payload,
            probe_id=dependency,
            outcome="failed",
            elapsed_seconds=elapsed,
            timeout_seconds=budget,
        )
        return False, details
    _ready_record_probe(
        payload,
        probe_id=dependency,
        outcome="ok",
        elapsed_seconds=time.monotonic() - started,
        timeout_seconds=budget,
    )
    return True, result


def _ready_add_source_sync_liveness_check(
    payload: dict[str, Any],
    source_sync_status: dict[str, Any],
) -> None:
    if not source_sync_status.get("ok"):
        _dep._ready_add_check(
            payload,
            "sources.sync_liveness",
            "blocker",
            "required source-sync status is blocked",
            source_sync_status,
        )
        return
    _dep._ready_add_check(
        payload,
        "sources.sync_liveness",
        "pass",
        "required source-sync status is healthy",
        source_sync_status,
    )


def _ready_database_status(conn: Any) -> dict[str, Any]:
    present: list[str] = []
    missing: list[str] = []
    with conn.cursor() as cur:
        for relation in _dep._ready_required_relations():
            cur.execute("SELECT to_regclass(%s)", (relation,))
            if cur.fetchone()[0] is None:
                missing.append(relation)
            else:
                present.append(relation)
    return {
        "required_relations": list(_dep._ready_required_relations()),
        "present_relations": present,
        "missing_relations": missing,
    }


def _ready_postgres_capacity_status(
    conn: Any,
    *,
    deployment: str,
    deployment_dir: Path,
    connection_dsn: str | None = None,
) -> dict[str, Any]:
    """Compare live worker capacity with sealed environment authority."""
    from okg.substrate.deployments.environment import (
        resolve_postgres_capacity_authority,
    )

    authority = resolve_postgres_capacity_authority(
        deployment_name=deployment,
        deployment_dir=deployment_dir,
    )
    if authority is None:
        return {
            "schema": "okg.postgres_capacity_readiness/v1",
            "ok": True,
            "applicable": False,
            "status": "not_applicable",
            "deployment": deployment,
            "reason": "deployment has no sealed production environment",
        }
    observed: dict[str, int] = {}
    with conn.cursor() as cur:
        for key, setting in (
            ("max_worker_processes", "max_worker_processes"),
            ("max_parallel_workers", "max_parallel_workers"),
            (
                "timescaledb_max_background_workers",
                "timescaledb.max_background_workers",
            ),
        ):
            cur.execute(f"SHOW {setting}")  # noqa: S608 - fixed setting names only.
            observed[key] = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT current_database(),
                   count(*) FILTER (
                       WHERE backend_type =
                             'TimescaleDB Background Worker Launcher'
                   )
              FROM pg_stat_activity
            """
        )
        launcher_row = cur.fetchone()

    current_database = str(launcher_row[0])
    launcher_count = int(launcher_row[1])
    demand = _ready_timescaledb_database_demand(
        conn,
        connection_dsn=connection_dsn,
    )
    configured_headroom = (
        observed["max_worker_processes"]
        - observed["max_parallel_workers"]
        - observed["timescaledb_max_background_workers"]
    )
    required_timescaledb_workers = (
        int(demand["loaded_database_count"])
        + int(authority["timescaledb_job_worker_reserve"])
    )
    timescaledb_slot_headroom = (
        observed["timescaledb_max_background_workers"]
        - required_timescaledb_workers
    )
    mismatches = {
        key: {
            "expected": int(authority[key]),
            "observed": observed[key],
        }
        for key in observed
        if observed[key] != int(authority[key])
    }
    problems: list[str] = []
    if mismatches:
        problems.append("live PostgreSQL settings differ from sealed authority")
    if configured_headroom < int(authority["minimum_headroom"]):
        problems.append("configured worker budget has insufficient reserved headroom")
    if timescaledb_slot_headroom < 1:
        problems.append(
            "TimescaleDB scheduler and job-worker demand has no positive headroom"
        )
    if launcher_count != 1:
        problems.append("TimescaleDB background-worker launcher is not singular")
    if int(demand["probe_error_count"]) > 0:
        problems.append("TimescaleDB database demand could not be fully measured")
    if int(demand["missing_scheduler_count"]) > 0:
        problems.append("one or more TimescaleDB database schedulers are not running")
    if not bool(demand["current_database_timescaledb_loaded"]):
        problems.append("deployment database has not loaded TimescaleDB")
    if not bool(demand["current_database_scheduler_running"]):
        problems.append("deployment database TimescaleDB scheduler is not running")

    return {
        "schema": "okg.postgres_capacity_readiness/v1",
        "ok": not problems,
        "applicable": True,
        "status": "ready" if not problems else "unavailable",
        "deployment": deployment,
        "database_identity": _ready_identity_digest(current_database),
        "authority": authority,
        "observed": observed,
        "mismatches": mismatches,
        "configured_headroom": configured_headroom,
        "timescaledb_launcher_count": launcher_count,
        "timescaledb_demand": demand,
        "required_timescaledb_workers": required_timescaledb_workers,
        "timescaledb_slot_headroom": timescaledb_slot_headroom,
        "problems": problems,
        "repair_commands": _ready_postgres_capacity_repair_commands(
            str(authority["environment_id"])
        ),
    }


def _ready_postgres_capacity_repair_commands(
    environment_id: str,
) -> list[str]:
    if sys.platform == "darwin":
        adapter = "launchd"
    elif sys.platform.startswith("linux"):
        adapter = "systemd"
    else:
        return []
    quoted_environment = shlex.quote(environment_id)
    return [
        (
            f"{okg_prefix()} environment service install "
            f"{quoted_environment} --adapter {adapter} --apply --json"
        ),
        (
            f"{okg_prefix()} environment service restart "
            f"{quoted_environment} --adapter {adapter} --apply --json"
        ),
    ]


def _ready_identity_digest(value: str) -> str:
    import hashlib

    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _ready_timescaledb_database_demand(
    conn: Any,
    *,
    connection_dsn: str | None = None,
) -> dict[str, Any]:
    """Inventory every connectable database, including missing schedulers."""
    from okg.substrate.db.connection import connect as db_connect
    from psycopg.conninfo import make_conninfo

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT datname
              FROM pg_database
             WHERE datallowconn
               AND NOT datistemplate
             ORDER BY datname
            """
        )
        database_names = [str(row[0]) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT datname
              FROM pg_stat_activity
             WHERE backend_type =
                   'TimescaleDB Background Worker Scheduler'
               AND datname IS NOT NULL
             ORDER BY datname
            """
        )
        active_schedulers = {str(row[0]) for row in cur.fetchall()}
        cur.execute("SELECT current_database()")
        current_database = str(cur.fetchone()[0])

    loaded: set[str] = set()
    probe_errors = 0
    for database_name in database_names:
        try:
            probe_dsn = make_conninfo(
                connection_dsn or conn.info.dsn,
                dbname=database_name,
                connect_timeout=2,
            )
            with db_connect(probe_dsn, autocommit=True) as probe_conn:
                with probe_conn.cursor() as probe_cur:
                    probe_cur.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1
                              FROM pg_extension
                             WHERE extname = 'timescaledb'
                        )
                        """
                    )
                    if bool(probe_cur.fetchone()[0]):
                        loaded.add(database_name)
        except Exception:  # noqa: BLE001 - incomplete inventory fails readiness.
            probe_errors += 1

    missing = loaded - active_schedulers
    extra = active_schedulers - loaded
    identity_material = "\n".join(sorted(loaded))
    return {
        "schema": "okg.timescaledb_database_demand/v1",
        "database_count_probed": len(database_names),
        "loaded_database_count": len(loaded),
        "active_scheduler_count": len(active_schedulers),
        "missing_scheduler_count": len(missing),
        "extra_scheduler_count": len(extra),
        "probe_error_count": probe_errors,
        "current_database_timescaledb_loaded": current_database in loaded,
        "current_database_scheduler_running": current_database in active_schedulers,
        "loaded_database_identity_digest": _ready_identity_digest(
            identity_material
        ),
    }


def _ready_writer_driver_status(
    conn: Any,
    *,
    deployment_name: str,
    deployment_dir: Any,
) -> dict[str, Any]:
    """Compare the recorded ownership writer against the running release.

    Reported as a WARNING, not a blocker: a version skew is a real
    condition an operator must see, but it does not by itself make the
    deployment unreadable, and turning it into a blocker would strand
    every deployment whose maintenance is legitimately done from a
    sync-root checkout. The point is that the cause is NAMED rather than
    surfacing only as an opaque fingerprint mismatch elsewhere.
    """
    from pathlib import Path

    from okg.substrate.catalog.foreign_driver import (
        compare_writers,
        recorded_writer_provenance,
        running_writer_provenance,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT deployment_name, metadata
                  FROM okg.deployment_write_ownership
                 WHERE deployment_name = %s
                """,
                (deployment_name,),
            )
            row = cur.fetchone()
    except Exception as exc:  # noqa: BLE001 - a missing table is not drift
        return {
            "schema": "okg.writer_driver_comparison.v1",
            "ok": True,
            "status": "unavailable",
            "reason_code": None,
            "message": f"{type(exc).__name__}: {exc}",
        }

    owner = (
        {"deployment_name": row[0], "metadata": row[1] or {}} if row else None
    )
    root = Path(deployment_dir).expanduser().resolve()
    from okg.substrate.catalog.ownership import _git_output

    running = running_writer_provenance(
        deployment_root=str(root),
        code_revision=_git_output(["rev-parse", "HEAD"], root),
    )
    return compare_writers(
        recorded=recorded_writer_provenance(owner),
        running=running,
    ).to_dict()


def _ready_generation_status(conn: Any, deployment: str) -> dict[str, Any]:
    from okg.substrate.generation_resolver import resolve_okg_generation
    from okg.substrate.graph_store import ChronosGraphStore

    resolved = resolve_okg_generation(
        conn,
        deployment_name=deployment,
        requested=None,
        status="published",
        # The checkpoint stamp is the expected value. Query the immutable pinned
        # checkout separately below; asking the resolver for counts would replace
        # the stamp with the same live query and make the comparison vacuous.
        include_counts=False,
    )
    if resolved is None:
        return {
            "status": "missing",
            "deployment_name": deployment,
            "generation_id": None,
        }
    checkpoint_counts = {
        "node_count": resolved.metadata.get("node_count"),
        "edge_count": resolved.metadata.get("edge_count"),
    }
    if (
        checkpoint_counts["node_count"] is None
        or checkpoint_counts["edge_count"] is None
    ):
        count_integrity = {
            "schema": "okg.generation_count_integrity.v1",
            "ok": False,
            "status": "unavailable",
            "generation_id": resolved.generation_id,
            "checkpoint": checkpoint_counts,
            "pinned": None,
            "mismatches": ["checkpoint_counts_missing"],
        }
    else:
        try:
            session = ChronosGraphStore.from_connection(
                conn,
                ensure_schema=False,
            ).checkout_generation(resolved.generation_id)
            pinned_counts = {
                "node_count": int(session.query(
                    "SELECT count(*) AS n FROM okg.graph_nodes",
                )[0]["n"]),
                "edge_count": int(session.query(
                    "SELECT count(*) AS n FROM okg.graph_edges",
                )[0]["n"]),
            }
        except Exception as exc:  # noqa: BLE001 - readiness must fail closed
            count_integrity = {
                "schema": "okg.generation_count_integrity.v1",
                "ok": False,
                "status": "unavailable",
                "generation_id": resolved.generation_id,
                "checkpoint": checkpoint_counts,
                "pinned": None,
                "mismatches": ["pinned_count_query_failed"],
                "error": f"{type(exc).__name__}: {exc}",
            }
        else:
            mismatches = [
                dimension
                for dimension in ("node_count", "edge_count")
                if int(checkpoint_counts[dimension]) != int(pinned_counts[dimension])
            ]
            count_integrity = {
                "schema": "okg.generation_count_integrity.v1",
                "ok": not mismatches,
                "status": "clean" if not mismatches else "mismatch",
                "generation_id": resolved.generation_id,
                "checkpoint": checkpoint_counts,
                "pinned": pinned_counts,
                "mismatches": mismatches,
            }
    return {
        "generation_id": resolved.generation_id,
        "deployment_name": resolved.deployment_name,
        "status": resolved.status,
        "catalog_version_id": resolved.catalog_version_id,
        "target_generation_id": resolved.target_generation_id,
        "created_at": resolved.created_at,
        "node_count": checkpoint_counts["node_count"],
        "edge_count": checkpoint_counts["edge_count"],
        "count_integrity": count_integrity,
        "publisher_configuration": resolved.metadata.get("publisher_configuration"),
        "first_publish_budget_warning": resolved.metadata.get(
            "first_publish_budget_warning",
        ),
        # Decision 284a: the lock hold this publish measured, so readiness
        # has publish-capacity evidence from the deployment's own publishes.
        "publisher_lock": _ready_generation_publisher_lock(
            resolved.generation_id, resolved.metadata,
        ),
    }


def _ready_generation_publisher_lock(
    generation_id: Any, metadata: Mapping[str, Any],
) -> dict[str, Any] | None:
    """The lock receipt a published generation stamped (``publisher_lock``).

    The publisher stamps the hold measured up to the checkpoint write
    (decision 284a); the full post-commit hold exists only in the publish
    result. Older checkpoints carry no receipt.
    """
    lock = metadata.get("publisher_lock")
    if not isinstance(lock, Mapping):
        return None
    return {
        "generation_id": generation_id,
        "hold_ms": lock.get("hold_ms"),
        "measured_until": lock.get("measured_until"),
        "hard_timeout_ms": lock.get("hard_timeout_ms"),
        "hard_timeout_source": lock.get("hard_timeout_source"),
    }


def _ready_add_generation_count_integrity_check(
    payload: dict[str, Any],
    generation: Mapping[str, Any],
) -> None:
    integrity = generation.get("count_integrity")
    if isinstance(integrity, Mapping) and integrity.get("ok") is True:
        _dep._ready_add_check(
            payload,
            "generation.count_integrity",
            "pass",
            "checkpoint counts match the pinned published generation",
            dict(integrity),
        )
        return
    details = (
        dict(integrity)
        if isinstance(integrity, Mapping)
        else {
            "schema": "okg.generation_count_integrity.v1",
            "ok": False,
            "status": "unavailable",
            "generation_id": generation.get("generation_id"),
            "checkpoint": {
                "node_count": generation.get("node_count"),
                "edge_count": generation.get("edge_count"),
            },
            "pinned": None,
            "mismatches": ["count_integrity_evidence_missing"],
        }
    )
    _dep._ready_add_check(
        payload,
        "generation.count_integrity",
        "blocker",
        "checkpoint counts do not match the pinned published generation",
        details,
    )


def _ready_add_publish_block_check(
    payload: dict[str, Any],
    conn: Any,
    deployment: str,
    deadline: "ReadinessDeadline | None",
) -> None:
    """Issue #2993: a failed source run's unfinished undo, or its kept partial
    rows awaiting a rerun, refuses every publish of the deployment. Readers
    keep the last good generation, so the other checks can read clean; this
    one names the source and the commands out."""
    from okg.substrate.ingest.run_base import publish_block

    ok, block = _dep._ready_required_dependency(
        payload,
        check_id="publish.source_undo.unavailable",
        dependency="publish_block",
        message="source-undo publish block check failed",
        call=lambda: publish_block(
            conn, deployment=deployment, include_in_progress=False,
        ),
        deadline=deadline,
    )
    if not ok:
        return
    _ready_add_source_run_warnings(payload, conn, deployment, deadline)
    if block is None:
        _dep._ready_add_check(
            payload,
            "publish.source_undo",
            "pass",
            "no failed source run blocks publishing",
        )
        return
    _dep._ready_add_check(
        payload,
        "publish.source_undo",
        "blocker",
        block["message"],
        {
            "code": block["code"],
            "sources": block["sources"],
            "fix_hint": block["fix_hint"],
            **block["data"],
        },
    )


def _ready_add_source_run_warnings(
    payload: dict[str, Any],
    conn: Any,
    deployment: str,
    deadline: "ReadinessDeadline | None",
) -> None:
    """Issue #2993 round 6: a source run in progress holds every publish back
    (an ingest's publish waits for it, any other publish is refused), and a
    stale run base pins history. Both are warnings: the first ends when the
    run does, the second blocks nothing. Each names the run, its source,
    whether its owner session is alive, and the command that clears it."""
    from okg.substrate.ingest.run_base import (
        in_progress_hint,
        runs_in_progress,
        stale_run_bases,
    )

    ok, running = _dep._ready_required_dependency(
        payload,
        check_id="publish.source_runs.unavailable",
        dependency="runs_in_progress",
        message="source-run liveness check failed",
        call=lambda: {
            "in_progress": runs_in_progress(conn, deployment=deployment),
            "stale_run_bases": stale_run_bases(conn, deployment=deployment),
        },
        deadline=deadline,
    )
    if not ok:
        return
    if running["in_progress"]:
        _dep._ready_add_check(
            payload,
            "publish.source_run_in_progress",
            "warning",
            "a source run is still writing; publishes wait or are refused "
            "until it ends",
            {
                "code": "source_run_in_progress",
                "in_progress": running["in_progress"],
                "fix_hint": in_progress_hint(deployment, running["in_progress"]),
            },
        )
    if running["stale_run_bases"]:
        _dep._ready_add_check(
            payload,
            "publish.stale_run_bases",
            "warning",
            "run-base checkpoints of ended runs are left over",
            {
                "stale_run_bases": running["stale_run_bases"],
                "fix_hint": (
                    f"`okg runtime finish-undo --deployment {deployment} "
                    "--apply` deletes them"
                ),
            },
        )


def _ready_queue_status(
    conn: Any,
    *,
    deployment_dir: Path,
    max_active_queue_depth: int,
) -> dict[str, Any]:
    """Queue health and the current DLQ, as TWO independently bounded reads.

    They used to share one cursor block, so the DLQ scan — the expensive one,
    over retained DBOS history — took queue health down with it when it ran
    long (issue #1290). Reading them apart means a slow DLQ costs the DLQ
    answer only: queue depth, staleness and the rest survive, and the DLQ
    reads `unknown` instead of an invented count.
    """
    ignored = {"embed"} if not _dep._ready_embeddings_enabled(deployment_dir) else set()
    # Read the dead-letter authority FIRST and ONCE, under the connection's
    # statement timeout. Queue health used to call it internally, so a
    # cancelled scan raised straight through and erased every queue
    # measurement with it -- and the readiness path scanned the retained
    # history twice per pass.
    # Derive the same ignored set queue health would derive, so the single
    # DLQ read is filtered exactly as both consumers filtered it before.
    ignored_workflows = _ignored_runtime_workflow_names(ignored) | set(
        DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES
    )
    with conn.cursor() as cur:
        current_dlq, dlq_unknown = probe_current_runtime_dlq(
            cur, ignored_workflow_names=ignored_workflows,
        )
    with conn.cursor() as cur:
        status = summarize_runtime_queue_health(
            cur,
            ignored_job_kinds=ignored,
            ignored_workflow_names=set(
                DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES
            ),
            current_dlq=current_dlq,
            dlq_unknown=dlq_unknown,
        )
    terminal_history_dlq_count = int(
        status.get("terminal_history_dlq_count") or 0
    )
    if dlq_unknown is not None:
        # Keep the key set identical to the measured shape: a consumer must
        # not read a MISSING `current_version_dlq` as "no dead letters".
        status["queue_health_measured"] = True
        status["current_version_dlq"] = dict(dlq_unknown)
        return {
            **status,
            "max_active_queue_depth": max_active_queue_depth,
        }
    current_dlq_count = int(status.get("dlq_count") or 0)
    base_status = str(status.get("status") or "unavailable")
    if current_dlq_count:
        status["ok"] = False
        status["status"] = "dead_letter"
        status["reason"] = (
            "runtime_queue_dead_letter "
            f"({current_dlq_count} current-version failure-class rows)"
        )
    elif base_status == "dead_letter":
        stale_history_rows = [
            row for row in status.get("runtime_queue_health") or []
            if row.get("stale_active_work_present")
            or str(row.get("status") or "").lower() == "stale"
        ]
        status["ok"] = not stale_history_rows
        status["status"] = "stale" if stale_history_rows else "available"
        if stale_history_rows:
            status["reason"] = (
                stale_history_rows[0].get("reason")
                or "runtime_queue_health_stale"
            )
        else:
            status.pop("reason", None)
    status.update({
        "dlq_count": current_dlq_count,
        "runtime_failed_count": current_dlq_count,
        "terminal_history_dlq_count": terminal_history_dlq_count,
        "current_version_dlq": status.get("current_runtime_dlq") or {},
        "queue_health_measured": True,
    })
    return {
        **status,
        "max_active_queue_depth": max_active_queue_depth,
    }


def _ready_embeddings_enabled(deployment_dir: Path) -> bool:
    from okg.substrate.publisher.deployment_stack import embeddings_enabled

    return embeddings_enabled(deployment_dir)


def _ready_source_freshness_status(
    conn: Any,
    *,
    deployment: str,
    source_namespace: str | None = None,
    max_source_age_seconds: int,
    source_groups: Mapping[str, tuple[str, ...]] | None = None,
    progress_freshness_sources: set[str] | None = None,
    source_aliases: Mapping[str, tuple[str, ...]] | None = None,
) -> dict[str, Any]:
    source_groups = source_groups or _dep._READY_AGENT_CONTEXT_SOURCE_GROUPS
    progress_freshness_sources = progress_freshness_sources or set()
    groups: dict[str, Any] = {}
    missing: list[str] = []
    stale: list[str] = []
    with conn.cursor() as cur:
        for group, sources in source_groups.items():
            rows: list[dict[str, Any]] = []
            group_ok = True
            for source in sources:
                fq_source = f"{deployment}.{source}"
                # MERGE NOTE (dev <- codex/okg-workspace-cutover-followup):
                # both resolution mechanisms are kept because they answer
                # different questions and each has a witness.
                #   * source_namespace (cutover branch): the watermark lives
                #     under an explicitly declared DEFINITION namespace, which
                #     must NOT fall back to the runtime deployment's own name —
                #     that fallback is what let a stale runtime row satisfy
                #     readiness. Strict, fail-closed, so the identity reported
                #     stays the deployment's logical fq name.
                #   * source_aliases (dev): the registry's ownership_id IS the
                #     source's durable identity, so a match reports that.
                if source_namespace:
                    candidates = (f"{source_namespace}.{source}",)
                else:
                    candidates = tuple(dict.fromkeys((
                        *(source_aliases or {}).get(source, ()),
                        fq_source,
                        source,
                    )))
                cur.execute(
                    """
                    SELECT source, last_reconcile_at,
                           EXTRACT(EPOCH FROM now() - last_reconcile_at)
                      FROM okg.source_watermarks
                     WHERE source = ANY(%s)
                     ORDER BY array_position(%s::text[], source)
                     LIMIT 1
                    """,
                    (list(candidates), list(candidates)),
                )
                row = cur.fetchone()
                if row is None or row[1] is None:
                    progress_row = (
                        _dep._ready_source_progress_freshness_row(
                            cur,
                            deployment=deployment,
                            source_namespace=source_namespace,
                            source=source,
                            candidates=candidates,
                            max_source_age_seconds=max_source_age_seconds,
                        )
                        if source in progress_freshness_sources
                        else None
                    )
                    if (
                        progress_row is not None
                        and progress_row["status"] == "fresh"
                    ):
                        rows.append(progress_row)
                        continue
                    source_row = {
                        # namespace mode reports the deployment's own logical
                        # name; alias mode reports the durable identity looked for
                        "source": fq_source if source_namespace else candidates[0],
                        "status": "missing",
                        "last_reconcile_at": None,
                        "age_seconds": None,
                    }
                    if progress_row is not None:
                        source_row["progress"] = progress_row
                    missing.append(fq_source)
                    group_ok = False
                else:
                    age_seconds = float(row[2])
                    status = (
                        "fresh"
                        if age_seconds <= max_source_age_seconds
                        else "stale"
                    )
                    source_row = {
                        # see the MERGE NOTE above: namespace mode keeps the
                        # deployment's logical name (the watermark merely lives
                        # elsewhere); alias mode reports the matched ownership_id
                        # because that IS the source's durable identity.
                        "source": fq_source if source_namespace else row[0],
                        "observed_source": row[0],
                        "status": status,
                        "last_reconcile_at": row[1],
                        "age_seconds": age_seconds,
                    }
                    if status == "stale":
                        progress_row = (
                            _dep._ready_source_progress_freshness_row(
                                cur,
                                deployment=deployment,
                                source_namespace=source_namespace,
                                source=source,
                                candidates=candidates,
                                max_source_age_seconds=max_source_age_seconds,
                                watermark_source=(
                                    fq_source if source_namespace else row[0]
                                ),
                                last_reconcile_at=row[1],
                                watermark_age_seconds=age_seconds,
                            )
                            if source in progress_freshness_sources
                            else None
                        )
                        if (
                            progress_row is not None
                            and progress_row["status"] == "fresh"
                        ):
                            rows.append(progress_row)
                            continue
                        if progress_row is not None:
                            source_row["progress"] = progress_row
                        stale.append(fq_source)
                        group_ok = False
                rows.append(source_row)
            groups[group] = {"ok": group_ok, "sources": rows}
    return {
        "ok": not missing and not stale,
        "source_namespace": source_namespace or deployment,
        "max_source_age_seconds": max_source_age_seconds,
        "groups": groups,
        "missing": missing,
        "stale": stale,
    }


def _ready_source_progress_freshness_row(
    cur: Any,
    *,
    deployment: str,
    source_namespace: str | None = None,
    source: str,
    candidates: tuple[str, ...] | None = None,
    max_source_age_seconds: int,
    watermark_source: str | None = None,
    last_reconcile_at: Any | None = None,
    watermark_age_seconds: float | None = None,
) -> dict[str, Any] | None:
    # Same two-mechanism rule as the caller (see its MERGE NOTE): an explicit
    # definition namespace is strict and must not fall back to the runtime
    # deployment's own name.
    if candidates is None:
        candidates = (
            (f"{source_namespace}.{source}",)
            if source_namespace
            else (f"{deployment}.{source}", source)
        )
    cur.execute(
        """
        WITH progress AS (
            SELECT source_name,
                   key,
                   COALESCE(
                       NULLIF(value::jsonb->>'finished_at', '')::timestamptz,
                       NULLIF(value::jsonb->>'last_run_at', '')::timestamptz,
                       updated_at
                   ) AS observed_at
              FROM okg.source_progress
             WHERE deployment = %s
               AND source_name = ANY(%s)
               AND key IN ('sync.last_run', 'sync.producer.poll')
        )
        SELECT source_name,
               key,
               observed_at,
               EXTRACT(EPOCH FROM now() - observed_at)
          FROM progress
         WHERE observed_at IS NOT NULL
         ORDER BY CASE WHEN source_name = %s THEN 0 ELSE 1 END,
                  CASE WHEN key = 'sync.last_run' THEN 0 ELSE 1 END,
                  observed_at DESC
         LIMIT 1
        """,
        (deployment, list(candidates), candidates[0]),
    )
    row = cur.fetchone()
    if row is None:
        return None
    age_seconds = float(row[3])
    status = "fresh" if age_seconds <= max_source_age_seconds else "stale"
    return {
        "source": watermark_source or candidates[0],
        "status": status,
        "freshness_kind": "source_progress",
        "last_reconcile_at": last_reconcile_at,
        "watermark_age_seconds": watermark_age_seconds,
        "last_progress_at": row[2],
        "progress_source": row[0],
        "progress_key": row[1],
        "age_seconds": age_seconds,
    }


def _ready_source_aliases(deployment_dir: Path) -> dict[str, tuple[str, ...]]:
    """Map logical registry names to their durable ownership identities."""
    manifest = _dep._ready_load_yaml_mapping(deployment_dir / "deployment.yaml")
    registry_path = _dep._ready_rollout_source_registry(
        deployment_dir=deployment_dir,
        manifest=manifest,
    )
    if not registry_path.is_file():
        return {}
    from okg.substrate.sources.registry import load_source_registry

    registry = load_source_registry(registry_path)
    aliases: dict[str, tuple[str, ...]] = {}
    for source_name, entry in registry.items():
        ownership_id = str(getattr(entry, "ownership_id", "") or "").strip()
        if ownership_id:
            aliases[str(source_name)] = (ownership_id,)
    return aliases


def _ready_source_optimization_status(
    *,
    deployment: str,
    deployment_dir: Path,
    dsn: str,
    max_producer_age_seconds: int,
) -> dict[str, Any]:
    from okg.substrate.deployments import _publisher_config_from_manifest

    manifest = _dep._ready_load_yaml_mapping(deployment_dir / "deployment.yaml")
    registry_path = _dep._ready_rollout_source_registry(
        deployment_dir=deployment_dir,
        manifest=manifest,
    )
    if not registry_path.is_file():
        return {
            "ok": False,
            "status": "blocked",
            "reason": "source_registry_missing",
            "source_registry": str(registry_path),
            "violations": [{
                "source_name": None,
                "reason": "source_registry_missing",
                "next_action": "restore_source_registry",
            }],
            "warnings": [],
        }

    from okg.substrate.source_sync_optimization_ledger import (
        SUBSECOND_LATENCY_BUDGET_MS,
        build_source_optimization_ledger,
    )
    from okg.substrate.source_sync_microbench import build_microbenchmark_inventory
    from okg.substrate.sources.registry import load_source_registry

    registry = load_source_registry(registry_path)
    runtime_source_sync, runtime_config_warnings = _dep._ready_runtime_source_sync(
        manifest,
        deployment=deployment,
    )
    runtime_block = (
        manifest.get("runtime") if isinstance(manifest, Mapping) else {}
    )
    runtime_enabled = bool(
        runtime_block.get("enabled")
        if isinstance(runtime_block, Mapping) else False
    )
    inventory = build_microbenchmark_inventory(
        deployment=deployment,
        deployment_dir=deployment_dir,
        registry=registry,
        runtime_source_sync=runtime_source_sync,
        runtime_enabled=runtime_enabled,
        dsn=dsn,
        include_live=True,
        max_producer_age_seconds=max_producer_age_seconds,
    )
    ledger = build_source_optimization_ledger(
        inventory,
        latency_budget_ms=SUBSECOND_LATENCY_BUDGET_MS,
    )
    return _dep._ready_source_optimization_status_from_ledger(
        ledger,
        latency_budget_ms=SUBSECOND_LATENCY_BUDGET_MS,
        warnings=runtime_config_warnings,
        enforcement=_ready_source_optimization_enforcement(manifest),
        latency_budget_enforcement=(
            _ready_source_optimization_latency_budget_enforcement(manifest)
        ),
        publisher_config=_publisher_config_from_manifest(
            dict(manifest), manifest_path=deployment_dir / "deployment.yaml",
        ),
    )


def _ready_source_optimization_enforcement(
    manifest: Mapping[str, Any],
) -> str:
    """Resolve explicit performance enforcement without a permissive default."""
    readiness = manifest.get("readiness")
    readiness = readiness if isinstance(readiness, Mapping) else {}
    source_optimization = readiness.get("source_optimization")
    source_optimization = (
        source_optimization
        if isinstance(source_optimization, Mapping)
        else {}
    )
    enforcement = str(
        source_optimization.get("enforcement", "block")
    ).strip().lower()
    if enforcement not in {"block", "warn"}:
        raise ValueError(
            "readiness.source_optimization.enforcement must be "
            "'block' or 'warn'"
        )
    return enforcement


def _ready_source_optimization_latency_budget_enforcement(
    manifest: Mapping[str, Any],
) -> str:
    """Resolve the latency-only policy; unset means block, like ``enforcement``.

    ``readiness.source_optimization.latency_budget_enforcement: warn`` makes a
    row advisory only when its sole finding is a measured full-pipeline latency
    over the budget. Every other reason keeps blocking.
    """
    readiness = manifest.get("readiness")
    readiness = readiness if isinstance(readiness, Mapping) else {}
    source_optimization = readiness.get("source_optimization")
    source_optimization = (
        source_optimization
        if isinstance(source_optimization, Mapping)
        else {}
    )
    value = str(
        source_optimization.get("latency_budget_enforcement", "block")
    ).strip().lower()
    if value not in {"block", "warn"}:
        raise ValueError(
            "readiness.source_optimization.latency_budget_enforcement must be "
            "'block' or 'warn'"
        )
    return value


# Amplification signals the violation-reason ladder only inspects AFTER the
# latency verdict, so a row over budget never reports them. A latency-only
# advisory must not wave them through behind the latency reason.
_READY_NON_DELTA_AMPLIFICATION_SIGNALS = frozenset({
    "dedupe_drops_after_source_work",
    "full_emission_zero_append",
    "zero_delta_fixed_cost",
})


def _ready_source_optimization_latency_only(
    row: Mapping[str, Any],
    reason: str | None,
    *,
    latency_budget_ms: float,
) -> bool:
    """Is a measured over-budget latency this row's one and only finding?

    Reasons ranked above latency in the ladder are returned instead of it, so
    they already block. ``latency_budget_exceeded`` is also the reason for
    every failed full-pipeline sample (``latest_full_pipeline_ok`` false: a
    sample error, a failed publish, a failed outcome, a non-OK source status),
    whatever its latency, and it hides the non-delta work check, which ranks
    below it. So a row is latency-only when all of these hold: the sample
    reports ``ok`` exactly true (false or missing blocks), its measured p95 is
    over the budget, and no non-delta work signal is present.
    """
    if reason != "latency_budget_exceeded":
        return False
    if row.get("latest_full_pipeline_ok") is not True:
        return False
    if row.get("amplification_signal") in _READY_NON_DELTA_AMPLIFICATION_SIGNALS:
        return False
    measured_ms = _dep._ready_as_float(
        row.get("latest_full_pipeline_latency_p95_ms"),
    )
    return measured_ms is not None and measured_ms > latency_budget_ms


def _ready_source_optimization_status_from_ledger(
    ledger: Mapping[str, Any],
    *,
    latency_budget_ms: float,
    warnings: list[dict[str, Any]] | None = None,
    enforcement: str = "block",
    latency_budget_enforcement: str = "block",
    publisher_config: Any = None,
) -> dict[str, Any]:
    if enforcement not in {"block", "warn"}:
        raise ValueError(
            "source optimization enforcement must be 'block' or 'warn'"
        )
    if latency_budget_enforcement not in {"block", "warn"}:
        raise ValueError(
            "source optimization latency_budget_enforcement must be "
            "'block' or 'warn'"
        )
    latency_advisory_count = 0
    violations: list[dict[str, Any]] = []
    warnings = list(warnings or [])
    source_rows = [
        row for row in (ledger.get("rows") or ())
        if isinstance(row, Mapping)
        and row.get("row_kind") == "source"
    ]
    readiness_rows = [
        row for row in source_rows
        if row.get("readiness_required") is True
    ]
    # A profile that ships sources but marks NONE readiness_required yields an
    # empty `readiness_rows`, so the loop below records zero violations and the
    # gate certifies `ok=True` on the strength of having measured nothing (issue
    # #690). Zero required sources on a profile that has sources is a blocker,
    # not a blanket pass: absence of a required-source measurement is a failure.
    if source_rows and not readiness_rows:
        violations.append({
            "source_name": None,
            "reason": "no_readiness_required_sources",
            "next_action": "declare_readiness_required_sources",
            "source_count": len(source_rows),
        })
    for row in readiness_rows:
        reason = _dep._ready_source_optimization_violation_reason(
            row,
            latency_budget_ms=latency_budget_ms,
        )
        item = {
            "source_name": row.get("name"),
            "reason": reason,
            "optimization_status": row.get("optimization_status"),
            "latency_status": row.get("latency_status"),
            "amplification_signal": row.get("amplification_signal"),
            "proof_status": row.get("proof_status"),
            "latency_budget_ms": latency_budget_ms,
            "latest_source_runner_ms": row.get("latest_source_runner_ms"),
            "latest_stage_total_ms": row.get("latest_stage_total_ms"),
            "latest_full_pipeline_scenario": row.get(
                "latest_full_pipeline_scenario",
            ),
            "latest_full_pipeline_latency_p95_ms": row.get(
                "latest_full_pipeline_latency_p95_ms",
            ),
            "latest_full_pipeline_rows_appended": row.get(
                "latest_full_pipeline_rows_appended",
            ),
            "latest_full_pipeline_changed_graph_rows": row.get(
                "latest_full_pipeline_changed_graph_rows",
            ),
            "latest_full_pipeline_expected_delta_source": row.get(
                "latest_full_pipeline_expected_delta_source",
            ),
            "latest_full_pipeline_expected_delta_units": row.get(
                "latest_full_pipeline_expected_delta_units",
            ),
            "latest_full_pipeline_observed_delta_units": row.get(
                "latest_full_pipeline_observed_delta_units",
            ),
            "latest_full_pipeline_source_work_units": row.get(
                "latest_full_pipeline_source_work_units",
            ),
            "latest_full_pipeline_work_amplification_ratio": row.get(
                "latest_full_pipeline_work_amplification_ratio",
            ),
            "latest_full_pipeline_work_amplification_budget": (
                row.get("latest_full_pipeline_work_amplification_budget")
                or _dep._ready_max_work_amplification_ratio()
            ),
            "latest_full_pipeline_observability_status": row.get(
                "latest_full_pipeline_observability_status",
            ),
            "latest_full_pipeline_observability_gaps": row.get(
                "latest_full_pipeline_observability_gaps",
            ),
            "latest_full_pipeline_top_widening_stage": row.get(
                "latest_full_pipeline_top_widening_stage",
            ),
            "latest_full_pipeline_top_widening_owner": row.get(
                "latest_full_pipeline_top_widening_owner",
            ),
            "latest_full_pipeline_top_widening_reason": row.get(
                "latest_full_pipeline_top_widening_reason",
            ),
            "latest_full_pipeline_top_widening_ms": row.get(
                "latest_full_pipeline_top_widening_ms",
            ),
            "latest_full_pipeline_readiness_impact": row.get(
                "latest_full_pipeline_readiness_impact",
            ),
            "latest_full_pipeline_artifact": row.get(
                "latest_full_pipeline_artifact",
            ),
            "latest_full_pipeline_generated_at": row.get(
                "latest_full_pipeline_generated_at",
            ),
            "latest_full_pipeline_proof_age_seconds": (
                _dep._ready_source_optimization_proof_age_seconds(
                    row,
                    ledger_generated_at=ledger.get("generated_at"),
                )
            ),
            "latest_nodes_appended": row.get("latest_nodes_appended"),
            "latest_edges_appended": row.get("latest_edges_appended"),
            "bottleneck_stage": row.get("bottleneck_stage"),
            "next_action": row.get("next_action"),
            "next_command": row.get("next_command"),
        }
        environment_fields = {
            "latest_full_pipeline_lock_wait_p95_ms": row.get(
                "latest_full_pipeline_lock_wait_p95_ms",
            ),
            "latest_full_pipeline_lock_hold_p95_ms": row.get(
                "latest_full_pipeline_lock_hold_p95_ms",
            ),
            "latest_full_pipeline_environment_contention": row.get(
                "latest_full_pipeline_environment_contention",
            ),
            "latest_full_pipeline_environment_stage": row.get(
                "latest_full_pipeline_environment_stage",
            ),
            "latest_full_pipeline_environment_reason": row.get(
                "latest_full_pipeline_environment_reason",
            ),
        }
        if (
            bool(environment_fields[
                "latest_full_pipeline_environment_contention"
            ])
            or environment_fields["latest_full_pipeline_lock_wait_p95_ms"]
            is not None
            or environment_fields["latest_full_pipeline_lock_hold_p95_ms"]
            is not None
            or environment_fields["latest_full_pipeline_environment_stage"]
            is not None
            or environment_fields["latest_full_pipeline_environment_reason"]
            is not None
        ):
            item.update(environment_fields)
        if row.get("latest_full_pipeline_no_change_dominant") is not None:
            item["latest_full_pipeline_no_change_dominant"] = bool(
                row.get("latest_full_pipeline_no_change_dominant"),
            )
        if reason is not None:
            if (
                latency_budget_enforcement == "warn"
                and _ready_source_optimization_latency_only(
                    row, reason, latency_budget_ms=latency_budget_ms,
                )
            ):
                item["advisory"] = "latency_budget_enforcement"
                latency_advisory_count += 1
            violations.append(item)
            continue
        if (
            _dep._ready_source_optimization_full_pipeline_proof_passed(
                row,
                latency_budget_ms=latency_budget_ms,
            )
            and _dep._ready_source_optimization_latest_sample_over_budget(
                row,
                latency_budget_ms=latency_budget_ms,
            )
        ):
            warning = dict(item)
            warning["reason"] = "live_sample_over_budget_benchmark_passed"
            warnings.append(warning)
            continue

    # The wide ``enforcement: warn`` downgrades every violation. The latency-only
    # policy downgrades the result only when EVERY violation is a latency-only
    # finding; one other reason anywhere keeps the whole check blocked.
    advisory = bool(violations) and (
        enforcement == "warn"
        or latency_advisory_count == len(violations)
    )
    return {
        "ok": not violations or advisory,
        "status": (
            "pass"
            if not violations
            else ("warning" if advisory else "blocked")
        ),
        "schema": "okg.source_optimization.readiness.v1",
        "deployment": ledger.get("deployment"),
        "generated_at": ledger.get("generated_at"),
        "enforcement": enforcement,
        "latency_budget_enforcement": latency_budget_enforcement,
        "latency_advisory_count": latency_advisory_count,
        "latency_budget_ms": latency_budget_ms,
        "readiness_required_count": len(readiness_rows),
        "violation_count": len(violations),
        "warning_count": len(warnings),
        "publish_path_blocker_count": sum(
            1 for item in violations
            if item.get("reason") == "publisher_path_budget_exceeded"
        ),
        "publish_path_blockers": [
            item for item in violations
            if item.get("reason") == "publisher_path_budget_exceeded"
        ],
        "environment_blocker_count": sum(
            1 for item in violations
            if item.get("reason") == "publisher_lock_contention"
        ),
        "environment_blockers": [
            item for item in violations
            if item.get("reason") == "publisher_lock_contention"
        ],
        "violations": violations,
        "warnings": warnings,
        "publisher_lock_headroom": (
            _dep._ready_publisher_lock_headroom_status(
                ledger, publisher_config=publisher_config,
            )
        ),
        "optimization_status_counts": (
            ledger.get("optimization_status_counts") or {}
        ),
        "optimization_wave_counts": ledger.get("optimization_wave_counts") or {},
    }


def _ready_publisher_lock_headroom_status(
    ledger: Mapping[str, Any],
    *, publisher_config: Any = None,
) -> dict[str, Any]:
    """Publish capacity: does the measured lock hold keep its safety factor?

    Issue #1291. The publisher runs a whole cycle under one advisory lock
    bounded by a hard ``statement_timeout``. A staging publish held it 881.6s
    against the 900s bound and still reported success, because the only
    consequence of a long hold was a log warning.

    This reads the representative measured hold — the WORST readiness-required
    source's full-pipeline p95 lock hold — and applies the shared headroom
    policy. Three outcomes, and only one of them is a pass:

    ``pass``              every measured hold fits the configured bound.
    ``capacity_warning``  a measured hold lacks the required safety factor.
                          Decision 35a: readiness reports this as a warning,
                          not a blocker, with the same status string the
                          publish proof uses (``CAPACITY_WARNING_STATUS``).
    ``unmeasured``        no readiness-required source measured a lock hold
                          at all, so the bar is unproven. Absence is not
                          agreement: this is reported as unknown, never as a
                          pass. ``unbounded`` (no statement timeout) is
                          reported the same way.

    Scope: the measured hold lives in the source-optimization ledger, which
    only the ``dogfood`` readiness profile builds and enforces, so only that
    profile records this check. Other profiles neither pass nor fail it -- they
    have no measurement authority to read.
    """
    from okg.substrate.publish_lock_headroom import (
        _as_float as _headroom_float,
        headroom_policy,
    )

    from okg.substrate.deployments import PublisherConfig

    config = publisher_config if publisher_config is not None else PublisherConfig()
    policy = headroom_policy(
        hard_timeout_ms=config.hard_statement_timeout_ms,
        hard_timeout_source=config.hard_statement_timeout_source,
        required_factor=config.lock_hold_headroom_factor,
        factor_source=config.lock_hold_headroom_factor_source,
        allow_raised_timeout=config.allow_raised_statement_timeout,
        allow_raised_timeout_source=config.allow_raised_statement_timeout_source,
    )
    rows = [
        row for row in (ledger.get("rows") or ())
        if isinstance(row, Mapping)
        and row.get("row_kind") == "source"
        and row.get("readiness_required") is True
    ]
    measured: list[dict[str, Any]] = []
    for row in rows:
        # The shared coercion rejects bools and non-finite values: a NaN or a
        # negative p95 is corrupt instrumentation, and counting it as a
        # measurement would let it pass every comparison.
        hold_ms = _headroom_float(
            row.get("latest_full_pipeline_lock_hold_p95_ms"),
        )
        if hold_ms is None or hold_ms < 0:
            continue
        measured.append({
            "source_name": row.get("name"),
            "lock_hold_p95_ms": hold_ms,
            "lock_wait_p95_ms": _headroom_float(
                row.get("latest_full_pipeline_lock_wait_p95_ms"),
            ),
            "max_hold_ms": policy["max_hold_ms"],
            "hard_timeout_ms": policy["hard_timeout_ms"],
            "observed_factor": (
                round(policy["hard_timeout_ms"] / hold_ms, 3)
                if policy["hard_timeout_ms"] and hold_ms > 0
                else None
            ),
            "scenario": row.get("latest_full_pipeline_scenario"),
            "artifact": row.get("latest_full_pipeline_artifact"),
        })
    payload: dict[str, Any] = {
        "schema": "okg.publisher.lock_headroom.readiness.v1",
        "policy": policy,
        "required_factor": policy["required_factor"],
        "hard_timeout_ms": policy["hard_timeout_ms"],
        "max_hold_ms": policy["max_hold_ms"],
        "readiness_required_count": len(rows),
        "measured_count": len(measured),
        # The hold was measured on a benchmark run elsewhere; the bound comes
        # from the deployment manifest or its explicit compatibility defaults.
        # These settings were not necessarily observed with that measurement.
        "hard_timeout_source": policy["hard_timeout_source"],
        "publisher_configuration": config.authority_receipt(),
        "measurements": measured,
        "violations": [],
        "representative": None,
    }
    return _ready_lock_headroom_verdict(payload, policy, measured)


def _ready_lock_headroom_verdict(
    payload: dict[str, Any],
    policy: Mapping[str, Any],
    measured: list[dict[str, Any]],
) -> dict[str, Any]:
    """Judge the measured holds against the policy (shared by both evidence
    sources: the source-optimization ledger and the latest publish)."""
    payload["measured_count"] = len(measured)
    payload["measurements"] = measured
    payload["violations"] = []
    payload["representative"] = None
    if policy["max_hold_ms"] is None:
        payload.update({
            "ok": False,
            "status": "unbounded",
            "message": (
                "the publisher hard statement timeout is disabled, so a publish "
                "lock hold has no bound to demonstrate headroom against"
            ),
        })
        return payload
    if not measured:
        payload.update({
            "ok": False,
            "status": "unmeasured",
            "message": (
                "no readiness-required source measured a publisher lock hold "
                "and no published generation recorded one, so the required "
                f"{policy['required_factor']}x timeout headroom is unproven"
            ),
        })
        return payload
    representative = max(
        measured, key=lambda item: item["lock_hold_p95_ms"],
    )
    payload["representative"] = representative
    violations = [
        item for item in measured
        if item["lock_hold_p95_ms"] > policy["max_hold_ms"]
    ]
    payload["violations"] = violations
    if violations:
        from okg.substrate.publish_proof import CAPACITY_WARNING_STATUS

        payload.update({
            "ok": False,
            "status": CAPACITY_WARNING_STATUS,
            "message": (
                "publisher lock hold "
                f"{representative['lock_hold_p95_ms'] / 1000.0:.1f}s "
                f"({_ready_lock_hold_origin(representative)}) exceeds the "
                f"{policy['required_factor']}x headroom bound "
                f"{policy['max_hold_ms'] / 1000.0:.1f}s against the "
                f"{policy['hard_timeout_ms'] / 1000.0:.0f}s hard statement "
                "timeout"
            ),
            "next_action": "reduce_publish_lock_hold",
        })
        return payload
    payload.update({
        "ok": True,
        "status": "pass",
        "message": (
            "the worst measured publisher lock hold "
            f"{representative['lock_hold_p95_ms'] / 1000.0:.1f}s keeps the "
            f"{policy['required_factor']}x headroom bound "
            f"{policy['max_hold_ms'] / 1000.0:.1f}s"
        ),
    })
    return payload


def _ready_lock_hold_origin(item: Mapping[str, Any]) -> str:
    if item.get("origin") == "latest_publish":
        return f"latest publish {item.get('generation_id')}"
    return f"source {item.get('source_name')}"


def _ready_lock_headroom_with_latest_publish(
    status: Mapping[str, Any],
    latest_publish: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Add the lock hold the latest successful publish measured (decision
    284a).

    A fresh install's readiness-required sources have no full-pipeline
    benchmark, so the ledger alone leaves the check ``unmeasured`` forever,
    although provision's own publish measured a hold under the same bound.
    That publish is evidence too: it joins the ledger's measurements and the
    worst hold decides, so a fast publish cannot hide a slow benchmark. Only a
    deployment that never published a measured hold stays unknown.
    """
    from okg.substrate.publish_lock_headroom import _as_float as _headroom_float

    out = dict(status)
    policy = out.get("policy")
    if not isinstance(latest_publish, Mapping) or not isinstance(policy, Mapping):
        return out
    hold_ms = _headroom_float(latest_publish.get("hold_ms"))
    if hold_ms is None or hold_ms < 0:
        return out
    hard_timeout_ms = policy.get("hard_timeout_ms")
    measured = [dict(item) for item in out.get("measurements") or ()]
    measured.append({
        "source_name": None,
        "origin": "latest_publish",
        "generation_id": latest_publish.get("generation_id"),
        "lock_hold_p95_ms": hold_ms,
        "lock_wait_p95_ms": None,
        "measured_until": latest_publish.get("measured_until"),
        # The bound the publish ran under; the verdict uses the bound the
        # deployment declares now, and a difference is shown, not hidden.
        "publish_hard_timeout_ms": latest_publish.get("hard_timeout_ms"),
        "publish_hard_timeout_source": latest_publish.get("hard_timeout_source"),
        "max_hold_ms": policy.get("max_hold_ms"),
        "hard_timeout_ms": hard_timeout_ms,
        "observed_factor": (
            round(hard_timeout_ms / hold_ms, 3)
            if hard_timeout_ms and hold_ms > 0
            else None
        ),
        "scenario": "latest_published_generation",
        "artifact": None,
    })
    for key in ("ok", "status", "message", "next_action"):
        out.pop(key, None)
    return _ready_lock_headroom_verdict(out, policy, measured)


def _ready_add_publisher_lock_headroom_check(
    payload: dict[str, Any],
    source_optimization: Mapping[str, Any] | None,
    latest_publish: Mapping[str, Any] | None = None,
) -> None:
    """Record the publish-capacity verdict as a first-class readiness check.

    ``latest_publish`` is the lock receipt of the latest published generation
    (``_ready_generation_status``'s ``publisher_lock``); its measured hold is
    evidence alongside the ledger's (decision 284a).
    """
    from okg.substrate.publish_proof import CAPACITY_WARNING_STATUS

    status = None
    if isinstance(source_optimization, Mapping):
        candidate = source_optimization.get("publisher_lock_headroom")
        if isinstance(candidate, Mapping):
            status = _ready_lock_headroom_with_latest_publish(
                candidate, latest_publish,
            )
    if status is None:
        _dep._ready_skip_unknown_dependent(
            payload,
            check_id="publisher.lock_headroom",
            message=(
                "publisher lock-hold headroom was not evaluated: the source "
                "optimization ledger, which carries the measured hold, was "
                "not read"
            ),
            depends_on="_ready_source_optimization_status",
        )
        return
    payload["publisher_lock_headroom"] = status
    message = str(status.get("message") or "publisher lock-hold headroom")
    if status.get("ok"):
        _dep._ready_add_check(
            payload, "publisher.lock_headroom", "pass", message, status,
        )
    elif status.get("status") == CAPACITY_WARNING_STATUS:
        # Decision 35a: a measured hold over the bar is a capacity warning,
        # not a blocker. It stays in the payload's warnings with its message,
        # violations and next_action; it does not fail readiness.
        _dep._ready_add_check(
            payload, "publisher.lock_headroom", "warning", message, status,
        )
    else:
        # Unmeasured or unbounded: not a pass, and not a repairable blocker
        # either. #1290's unknown state keeps it out of both buckets.
        _dep._ready_add_check(
            payload,
            "publisher.lock_headroom",
            READINESS_UNKNOWN_STATUS,
            message,
            status,
        )


def _ready_source_optimization_violation_reason(
    row: Mapping[str, Any],
    *,
    latency_budget_ms: float,
) -> str | None:
    if row.get("blocker"):
        return "inventory_blocker"
    if not _dep._ready_source_optimization_has_full_pipeline_proof(row):
        if row.get("proof_status") in {
            "missing_source_runner_sample",
            "blocked_before_benchmark",
        } or row.get("optimization_status") == "needs_benchmark":
            return "missing_source_optimization_evidence"
        return "full_pipeline_benchmark_missing"
    observability_gap = _dep._ready_source_optimization_observability_gap(row)
    if observability_gap is not None:
        return observability_gap
    if _dep._ready_source_optimization_publisher_path_blocker(
        row,
        latency_budget_ms=latency_budget_ms,
    ):
        return "publisher_path_budget_exceeded"
    if _dep._ready_source_optimization_publisher_lock_contention(row):
        return "publisher_lock_contention"
    if _dep._ready_source_optimization_full_pipeline_proof_failed(
        row,
        latency_budget_ms=latency_budget_ms,
    ):
        return "latency_budget_exceeded"
    if _dep._ready_source_optimization_full_pipeline_proof_passed(
        row,
        latency_budget_ms=latency_budget_ms,
    ):
        return None
    if row.get("amplification_signal") in {
        "dedupe_drops_after_source_work",
        "full_emission_zero_append",
        "zero_delta_fixed_cost",
    }:
        return "non_delta_proportional_work"
    # Reached here with a full-pipeline proof present but NEITHER a passing nor a
    # failing latency verdict: the sample carries no usable latency measurement
    # (e.g. a scenario recorded with no `latest_full_pipeline_latency_p95_ms`),
    # so the budget cannot be certified. The prior terminal `return None` folded
    # this "no measurement" state to a pass (issue #690); an absent measurement
    # is not-passing, with a typed reason. Reaching here already implies the
    # full-pipeline proof did NOT pass (the only pass exit is the `return None`
    # at the proof_passed branch above), so this is unconditional.
    return "missing_latency_measurement"


def _ready_source_optimization_publisher_path_blocker(
    row: Mapping[str, Any],
    *,
    latency_budget_ms: float,
) -> bool:
    if _dep._ready_source_optimization_publisher_lock_contention(row):
        return False
    full_pipeline_ms = _dep._ready_as_float(
        row.get("latest_full_pipeline_latency_p95_ms"),
    )
    if full_pipeline_ms is None or full_pipeline_ms <= latency_budget_ms:
        return False
    if not row.get("latest_full_pipeline_top_widening_stage"):
        return False
    source_runner_ms = _dep._ready_as_float(row.get("latest_source_runner_ms"))
    if source_runner_ms is None or source_runner_ms > latency_budget_ms:
        return False
    stage_total_ms = _dep._ready_as_float(row.get("latest_stage_total_ms"))
    if stage_total_ms is not None and stage_total_ms > latency_budget_ms:
        return False
    return True


def _ready_source_optimization_publisher_lock_contention(
    row: Mapping[str, Any],
) -> bool:
    return (
        bool(row.get("latest_full_pipeline_environment_contention"))
        and row.get("latest_full_pipeline_environment_stage")
        == "publisher.lock_acquire"
    )


def _ready_as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ready_source_optimization_has_full_pipeline_proof(
    row: Mapping[str, Any],
) -> bool:
    proof_status = row.get("proof_status")
    return (
        proof_status in {
            "full_pipeline_benchmark_passed",
            "full_pipeline_benchmark_failed",
        }
        or bool(row.get("latest_full_pipeline_scenario"))
        or _dep._ready_as_float(row.get("latest_full_pipeline_latency_p95_ms"))
        is not None
    )


def _ready_source_optimization_observability_gap(
    row: Mapping[str, Any],
) -> str | None:
    declared_status = str(
        row.get("latest_full_pipeline_observability_status") or "",
    )
    if declared_status == "fail":
        for gap in _dep._ready_source_optimization_observability_gaps(row):
            if gap in {
                "missing_expected_delta",
                "missing_observed_delta",
                "expected_delta_mismatch",
                "missing_measurement_rows",
                "missing_latency_measurement",
                "missing_source_work_units",
                "missing_work_amplification_ratio",
                "work_amplification_exceeded",
            }:
                return gap
        return "full_pipeline_observability_failed"
    expected_source = str(
        row.get("latest_full_pipeline_expected_delta_source") or "",
    )
    expected_units = _dep._ready_as_float(
        row.get("latest_full_pipeline_expected_delta_units"),
    )
    observed_units = _dep._ready_as_float(
        row.get("latest_full_pipeline_observed_delta_units"),
    )
    source_work_units = _dep._ready_as_float(
        row.get("latest_full_pipeline_source_work_units"),
    )
    amplification_ratio = _dep._ready_as_float(
        row.get("latest_full_pipeline_work_amplification_ratio"),
    )
    amplification_budget = (
        _dep._ready_as_float(row.get("latest_full_pipeline_work_amplification_budget"))
        or _dep._ready_max_work_amplification_ratio()
    )
    if _dep._ready_source_optimization_no_change_dominant(row):
        full_pipeline_ms = _dep._ready_as_float(
            row.get("latest_full_pipeline_latency_p95_ms"),
        )
        if full_pipeline_ms is None:
            return "missing_latency_measurement"
        if (
            expected_source not in _dep._ready_accepted_expected_delta_sources()
            or expected_units is None
        ):
            return "missing_expected_delta"
        if expected_units != 0:
            return "expected_delta_mismatch"
        if not _dep._ready_no_change_expected_delta_is_all_zero(row):
            return "expected_delta_mismatch"
        if observed_units is None:
            return "missing_observed_delta"
        if observed_units > 0:
            return "expected_delta_mismatch"
        if source_work_units is None:
            return "missing_source_work_units"
        return None
    if (
        expected_source not in _dep._ready_accepted_expected_delta_sources()
        or expected_units is None
        or expected_units <= 0
    ):
        return "missing_expected_delta"
    if observed_units is None or observed_units <= 0:
        return "missing_observed_delta"
    if expected_units != observed_units:
        return "expected_delta_mismatch"
    if source_work_units is None:
        return "missing_source_work_units"
    if amplification_ratio is None:
        return "missing_work_amplification_ratio"
    if amplification_ratio > amplification_budget:
        return "work_amplification_exceeded"
    return None


def _ready_source_optimization_observability_gaps(
    row: Mapping[str, Any],
) -> list[str]:
    gaps = row.get("latest_full_pipeline_observability_gaps")
    if isinstance(gaps, str):
        try:
            decoded = json.loads(gaps)
        except json.JSONDecodeError:
            return [gaps]
        gaps = decoded
    if isinstance(gaps, (list, tuple, set)):
        return [str(gap) for gap in gaps if gap]
    return []


def _ready_no_change_expected_delta_is_all_zero(row: Mapping[str, Any]) -> bool:
    expected_delta = row.get("latest_full_pipeline_expected_delta")
    if not isinstance(expected_delta, Mapping):
        return True
    units = [_dep._ready_as_float(value) for value in expected_delta.values()]
    return bool(units) and all(value == 0.0 for value in units)


def _ready_source_optimization_no_change_dominant(
    row: Mapping[str, Any],
) -> bool:
    if bool(row.get("latest_full_pipeline_no_change_dominant")):
        return True
    expected_source = str(
        row.get("latest_full_pipeline_expected_delta_source") or "",
    )
    expected_units = _dep._ready_as_float(
        row.get("latest_full_pipeline_expected_delta_units"),
    )
    observed_units = _dep._ready_as_float(
        row.get("latest_full_pipeline_observed_delta_units"),
    )
    source_work_units = _dep._ready_as_float(
        row.get("latest_full_pipeline_source_work_units"),
    )
    if expected_source not in _dep._ready_accepted_expected_delta_sources():
        return False
    return (
        expected_units == 0.0
        and observed_units == 0.0
        and source_work_units == 0.0
        and _dep._ready_no_change_expected_delta_is_all_zero(row)
    )


def _ready_source_optimization_full_pipeline_proof_passed(
    row: Mapping[str, Any],
    *,
    latency_budget_ms: float,
) -> bool:
    if not _dep._ready_source_optimization_has_full_pipeline_proof(row):
        return False
    full_pipeline_ms = _dep._ready_as_float(
        row.get("latest_full_pipeline_latency_p95_ms"),
    )
    if row.get("proof_status") == "full_pipeline_benchmark_passed":
        return full_pipeline_ms is not None and full_pipeline_ms <= latency_budget_ms
    return bool(row.get("latest_full_pipeline_ok")) and (
        full_pipeline_ms is not None and full_pipeline_ms <= latency_budget_ms
    )


def _ready_source_optimization_full_pipeline_proof_failed(
    row: Mapping[str, Any],
    *,
    latency_budget_ms: float,
) -> bool:
    if not _dep._ready_source_optimization_has_full_pipeline_proof(row):
        return False
    full_pipeline_ms = _dep._ready_as_float(
        row.get("latest_full_pipeline_latency_p95_ms"),
    )
    if row.get("proof_status") == "full_pipeline_benchmark_failed":
        return True
    if full_pipeline_ms is not None and full_pipeline_ms > latency_budget_ms:
        return True
    if row.get("latest_full_pipeline_ok") is False:
        return True
    return False


def _ready_source_optimization_latest_sample_over_budget(
    row: Mapping[str, Any],
    *,
    latency_budget_ms: float,
) -> bool:
    return (
        row.get("latency_status") == "over_budget"
        or row.get("optimization_status") == "failing_budget"
        or (
            (latest_ms := _dep._ready_as_float(row.get("latest_source_runner_ms")))
            is not None
            and latest_ms > latency_budget_ms
        )
        or (
            (stage_ms := _dep._ready_as_float(row.get("latest_stage_total_ms")))
            is not None
            and stage_ms > latency_budget_ms
        )
    )


def _ready_source_optimization_proof_age_seconds(
    row: Mapping[str, Any],
    *,
    ledger_generated_at: Any,
) -> float | None:
    proof_generated_at = _dep._ready_parse_datetime(
        row.get("latest_full_pipeline_generated_at"),
    )
    ledger_generated = _dep._ready_parse_datetime(ledger_generated_at)
    if proof_generated_at is None or ledger_generated is None:
        return None
    return max(0.0, (ledger_generated - proof_generated_at).total_seconds())


def _ready_parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _ready_rollout_status(
    *,
    deployment: str,
    deployment_dir: Path,
    env_payload: dict[str, Any],
    readiness_profile: str,
) -> dict[str, Any]:
    manifest = _dep._ready_load_yaml_mapping(deployment_dir / "deployment.yaml")
    profile = manifest.get("profile") if isinstance(manifest, dict) else {}
    profile = profile if isinstance(profile, dict) else {}
    answers = profile.get("init_answers")
    answers = answers if isinstance(answers, dict) else {}
    modules = [
        str(module)
        for module in (manifest.get("modules") or [])
        if isinstance(module, str)
    ]
    inferred_workspace_graph = _dep._ready_workspace_graph_modules().issubset(
        set(modules)
    )
    profile_name = str(profile.get("name") or "").strip()
    profile_source = "manifest"
    if not profile_name and inferred_workspace_graph:
        profile_name = "workspace-graph"
        profile_source = "inferred_from_modules"
    elif not profile_name:
        profile_name = "custom"
        profile_source = "unspecified"
    is_okg_ci_profile = profile_name == "okg-ci"

    repo_root = _dep._ready_rollout_repo_root(
        deployment_dir=deployment_dir,
        answers=answers,
    )
    viz = manifest.get("viz") if isinstance(manifest, dict) else {}
    viz = viz if isinstance(viz, dict) else {}
    mcp_port = answers.get("mcp_port") or viz.get("port") or 18770
    source_registry = _dep._ready_rollout_source_registry(
        deployment_dir=deployment_dir,
        manifest=manifest,
    )
    runtime_source_sync, runtime_config_warnings = _dep._ready_runtime_source_sync(
        manifest,
        deployment=deployment,
    )
    source_sync = _dep._ready_rollout_source_sync_summary(
        source_registry,
        deployment=deployment,
        runtime_source_sync=runtime_source_sync,
        config_warnings=runtime_config_warnings,
    )
    dsn_boundary = {
        "kind": "one_postgres_dsn_per_deployment",
        "resolved": bool(
            (env_payload.get("postgres") or {}).get("resolved")
        ),
        "required_env": list(
            (env_payload.get("postgres") or {}).get("required_env") or []
        ),
    }
    project_ok = source_sync.get("source_registry_exists") and (
        profile_name == "workspace-graph"
        or inferred_workspace_graph
        or is_okg_ci_profile
    )
    readiness_command = (
        f"{okg_prefix()} deployment ready "
        f"{shlex.quote(deployment)} "
        f"--checks {shlex.quote(readiness_profile)} --json"
    )
    return {
        "ok": bool(project_ok),
        "deployment": deployment,
        "deployment_dir": str(deployment_dir),
        "concept": "OKG-CI" if is_okg_ci_profile else "Workspace Graph",
        "project_scope": {
            "kind": (
                "ci_authority_graph"
                if is_okg_ci_profile
                else "single_workspace_graph"
            ),
            "status": "configured" if project_ok else "needs_profile_review",
            "profile_name": profile_name,
            "profile_source": profile_source,
            "repo_root": str(repo_root),
            "dsn_boundary": dsn_boundary,
            "mcp_target": {
                "deployment": deployment,
                "port": int(mcp_port),
                "readiness_command": readiness_command,
            },
            "source_sync": source_sync,
            "next_setup_command": (
                readiness_command
                if is_okg_ci_profile
                else _dep._ready_rollout_project_command(
                    deployment=deployment,
                    repo_root=repo_root,
                    mcp_port=mcp_port,
                )
            ),
        },
        "portfolio_scope": {
            "kind": "multi_deployment_portfolio",
            "status": "requires_one_workspace_graph_per_project",
            "partition_boundary": [
                "deployment_name",
                "postgres_dsn",
                "repo_root",
                "mcp_port",
            ],
            "dsn_boundary": "one Postgres database per deployment",
            "mcp_target": (
                "select a deployment-specific MCP/readiness target; "
                "do not merge all projects into okg-workspace"
            ),
            "discovery": (
                "set OKG_DEPLOYMENTS_DIR to the directory containing "
                "project deployment directories"
            ),
            "next_setup_command": (
                "export OKG_DEPLOYMENTS_DIR=/path/to/okg-deployments && "
                # The documented door, per define-newcomer-entry-path.
                # `okg init` is an alias of it; a command the CLI HANDS an
                # operator must name the door, not the alias.
                f"{okg_prefix()} install --profile workspace-graph "
                "--deployment-name <project-slug> "
                "--repo-root /path/to/project "
                "--postgres-dsn postgresql://postgres:okg@localhost:5433/"
                "okg_<project_slug> "
                "--mcp-port <project-port>"
            ),
        },
    }


def _ready_nomos_audit_status(
    *,
    deployment: str,
    deployment_dir: Path,
    readiness_profile: str,
) -> dict[str, Any]:
    manifest_path = deployment_dir / "deployment.yaml"
    manifest = _dep._ready_load_yaml_mapping(manifest_path)
    registry_path = _dep._ready_rollout_source_registry(
        deployment_dir=deployment_dir,
        manifest=manifest,
    )
    registry_exists = registry_path.is_file()
    nomos_block = manifest.get("nomos") if isinstance(manifest, dict) else None
    has_nomos_block = isinstance(nomos_block, Mapping)
    protected_profile = _dep._ready_nomos_blocking_required(
        manifest,
        readiness_profile=readiness_profile,
    )
    audit_required = has_nomos_block or protected_profile
    if not audit_required:
        return {
            "schema": "okg.nomos.deployment_policy_audit.v1",
            "ok": None,
            "status": "skipped",
            "deployment": deployment,
            "manifest_path": str(manifest_path),
            "source_registry": str(registry_path),
            "source_registry_exists": registry_exists,
            "reason": (
                "manifest has no nomos block and no production-profile "
                "source registry policy surface"
            ),
        }
    if protected_profile and not registry_exists:
        return {
            "schema": "okg.nomos.deployment_policy_audit.v1",
            "ok": False,
            "status": "failed",
            "deployment": deployment,
            "manifest_path": str(manifest_path),
            "source_registry": str(registry_path),
            "source_registry_exists": False,
            "required_by_profile": True,
            "deployment_posture": {},
            "source_policies": {},
            "source_policy_count": 0,
            "findings": [{
                "code": "nomos.audit.source_registry_missing",
                "reason_code": "nomos.audit.source_registry_missing",
                "severity": "error",
                "message": (
                    "protected Nomos readiness requires a source registry "
                    "to audit source policies"
                ),
                "path": "source_registry",
            }],
            "warnings": [],
        }

    from okg.substrate.nomos.deployment_audit import audit_deployment_policy

    registry = _dep._ready_load_yaml_mapping(registry_path)
    audit = audit_deployment_policy(
        manifest,
        source_registry=registry,
        deployment=deployment,
    )
    audit = dict(audit)
    audit.update({
        "status": "pass" if audit.get("ok") else "failed",
        "manifest_path": str(manifest_path),
        "source_registry": str(registry_path),
        "source_registry_exists": registry_exists,
        "required_by_profile": protected_profile,
    })
    return audit


def _ready_nomos_readiness_profiles(manifest: Mapping[str, Any]) -> tuple[str, ...]:
    nomos = manifest.get("nomos") if isinstance(manifest, Mapping) else None
    if not isinstance(nomos, Mapping):
        return ()
    posture = nomos.get("deployment_posture")
    if not isinstance(posture, Mapping):
        return ()
    raw_profiles = posture.get("readiness_profiles")
    if not isinstance(raw_profiles, (list, tuple)):
        return ()
    return tuple(
        str(profile).strip()
        for profile in raw_profiles
        if str(profile).strip()
    )


def _ready_nomos_blocking_required(
    manifest: Mapping[str, Any],
    *,
    readiness_profile: str,
) -> bool:
    """Return whether this readiness profile must prove blocking Nomos mode."""
    profile = str(readiness_profile or "").strip()
    if profile in {"ci", "dogfood", "prod", "production"}:
        return True
    nomos = manifest.get("nomos") if isinstance(manifest, Mapping) else None
    if not isinstance(nomos, Mapping):
        return False
    return profile in _ready_nomos_readiness_profiles(manifest)


def _ready_nomos_active_state_status(
    *,
    deployment: str,
    dsn: str | None,
    manifest: Mapping[str, Any],
    deployment_dir: Path,
    readiness_profile: str = "general",
) -> dict[str, Any]:
    from okg.substrate.nomos.runtime_enforcement import (
        manifest_nomos_audit_readiness_exception,
        manifest_nomos_blocks_actions,
        manifest_nomos_enforcement_mode,
        manifest_nomos_enforcement_mode_source,
    )

    mode = manifest_nomos_enforcement_mode(manifest)
    mode_source = manifest_nomos_enforcement_mode_source(manifest)
    blocks_actions = manifest_nomos_blocks_actions(manifest)
    blocking_required = _dep._ready_nomos_blocking_required(
        manifest,
        readiness_profile=readiness_profile,
    )
    rollout_exception = manifest_nomos_audit_readiness_exception(
        manifest,
        readiness_profile=readiness_profile,
    )
    mode_details = {
        "effective_mode": mode,
        "blocks_actions": blocks_actions,
        "blocking_required": blocking_required,
        "readiness_profile": readiness_profile,
        "mode_source": mode_source,
        "rollout_exception": rollout_exception,
    }
    if mode_source == "environment":
        mode_details["mode_source_detail"] = "OKG_NOMOS_RUNTIME_ENFORCEMENT"
    elif mode_source == "manifest":
        mode_details["mode_source_detail"] = "deployment.yaml"
    else:
        mode_details["mode_source_detail"] = "default_off"
    if mode == "off":
        if blocking_required:
            return {
                "schema": "okg.nomos.runtime_active_state.v1",
                "ok": False,
                "status": "failed",
                "deployment": deployment,
                **mode_details,
                "findings": [{
                    "code": "nomos.runtime.mode_not_blocking",
                    "reason_code": "nomos.runtime.mode_not_blocking",
                    "severity": "error",
                    "message": (
                        "production readiness requires Nomos enforce mode; "
                        "effective mode is off"
                    ),
                    "path": "nomos.runtime_enforcement",
                    **mode_details,
                }],
            }
        return {
            "schema": "okg.nomos.runtime_active_state.v1",
            "ok": None,
            "status": "skipped",
            "deployment": deployment,
            "reason": "nomos.runtime_enforcement is off",
            **mode_details,
        }
    if mode == "audit" and blocking_required and rollout_exception is None:
        return {
            "schema": "okg.nomos.runtime_active_state.v1",
            "ok": False,
            "status": "failed",
            "deployment": deployment,
            **mode_details,
            "findings": [{
                "code": "nomos.runtime.mode_not_blocking",
                "reason_code": "nomos.runtime.mode_not_blocking",
                "severity": "error",
                "message": (
                    "production readiness requires Nomos enforce mode; audit "
                    "mode needs an explicit rollout exception"
                ),
                "path": "nomos.runtime_enforcement",
                **mode_details,
            }],
        }
    if not _dep._manifest_requires_nomos_runtime(manifest):
        return {
            "schema": "okg.nomos.runtime_active_state.v1",
            "ok": None,
            "status": "skipped",
            "deployment": deployment,
            "reason": "nomos.runtime_enforcement does not evaluate runtime gates",
            **mode_details,
        }
    scope = f"deployment:{deployment}"
    if not dsn:
        return {
            "schema": "okg.nomos.runtime_active_state.v1",
            "ok": False,
            "status": "failed",
            "deployment": deployment,
            "scope": scope,
            **mode_details,
            "findings": [{
                "code": "nomos.runtime.dsn_unresolved",
                "reason_code": "nomos.runtime.dsn_unresolved",
                "severity": "error",
                "message": "Nomos runtime active-state check needs a resolved DSN",
                "path": "postgres.dsn",
            }],
        }
    try:
        from okg.substrate.db.connection import connect as db_connect
        from okg.substrate.nomos import (
            active_nomos_summary,
            compile_config_graph,
            deployment_baseline_config_graph,
        )
        from okg.substrate.nomos.baseline import deployment_baseline_spec

        with db_connect(dsn) as conn:
            baseline_spec = deployment_baseline_spec(
                deployment=deployment,
                deployment_dir=deployment_dir,
            )
            if not baseline_spec.get("required"):
                raise RuntimeError(
                    "Nomos active-state readiness requires a deployment baseline"
                )
            expected_policy_graph = baseline_spec["policy_graph"]
            expected_policy_fingerprint = baseline_spec["policy_fingerprint"]
            expected_config = compile_config_graph(
                deployment_baseline_config_graph(
                    manifest,
                    deployment_name=deployment,
                    scope=scope,
                ),
            )
            summary = active_nomos_summary(
                conn,
                deployment_name=deployment,
                scope=scope,
            )
    except Exception as exc:  # noqa: BLE001 - readiness reports the blocker.
        from okg.substrate.db.dsn import redact_text

        # The probe failure is usually a connection error whose text embeds the
        # DSN, password included. This message becomes the operator-facing
        # blocker summary and lands in `--json` output pasted into PRs and PACT
        # evidence, so strip the password before it travels. Name the probe too:
        # "could not connect to server" alone reads as absent Nomos state.
        return {
            "schema": "okg.nomos.runtime_active_state.v1",
            "ok": False,
            "status": "failed",
            "deployment": deployment,
            "scope": scope,
            **mode_details,
            "findings": [{
                "code": "nomos.runtime.active_state_unavailable",
                "reason_code": "nomos.runtime.active_state_unavailable",
                "severity": "error",
                "message": (
                    "could not read Nomos active state: "
                    f"{redact_text(str(exc), dsn_flags=())}"
                ),
                "path": "nomos.active_state",
            }],
        }
    findings: list[dict[str, Any]] = []
    if not summary.get("active_policy"):
        findings.append({
            "code": "nomos.runtime.no_active_policy_bundle",
            "reason_code": "nomos.runtime.no_active_policy_bundle",
            "severity": "error",
            "message": "enforce-mode deployment has no active Nomos policy bundle",
            "path": "okg.nomos_policy_activations",
        })
    if not summary.get("active_config"):
        findings.append({
            "code": "nomos.runtime.no_active_config_set",
            "reason_code": "nomos.runtime.no_active_config_set",
            "severity": "error",
            "message": "enforce-mode deployment has no active Nomos config set",
            "path": "okg.nomos_config_activations",
        })
    active_policy = summary.get("active_policy") or {}
    active_config = summary.get("active_config") or {}
    if (
        active_policy
        and active_policy.get("policy_fingerprint")
        != expected_policy_fingerprint
    ):
        findings.append({
            "code": "nomos.runtime.active_policy_mismatch",
            "reason_code": "nomos.runtime.active_policy_mismatch",
            "severity": "error",
            "message": (
                "active Nomos policy does not match the deployment rollout "
                "baseline derived from this manifest"
            ),
            "path": "okg.nomos_policy_activations.policy_fingerprint",
            "expected": expected_policy_fingerprint,
            "actual": active_policy.get("policy_fingerprint"),
        })
    if (
        active_config
        and active_config.get("config_fingerprint")
        != expected_config.config_fingerprint
    ):
        findings.append({
            "code": "nomos.runtime.active_config_mismatch",
            "reason_code": "nomos.runtime.active_config_mismatch",
            "severity": "error",
            "message": (
                "active Nomos config does not match the deployment rollout "
                "baseline derived from this manifest"
            ),
            "path": "okg.nomos_config_activations.config_fingerprint",
            "expected": expected_config.config_fingerprint,
            "actual": active_config.get("config_fingerprint"),
        })
    return {
        "schema": "okg.nomos.runtime_active_state.v1",
        "ok": not findings,
        "status": "pass" if not findings else "failed",
        "deployment": deployment,
        "scope": scope,
        **mode_details,
        "expected_policy": {
            "policy_bundle_id": expected_policy_graph["policy_bundle_id"],
            "policy_version": expected_policy_graph["policy_version"],
            "policy_fingerprint": expected_policy_fingerprint,
        },
        "expected_config": {
            "config_set_id": expected_config.config_set_id,
            "config_version": expected_config.config_version,
            "config_fingerprint": expected_config.config_fingerprint,
        },
        "active_policy": summary.get("active_policy"),
        "active_config": summary.get("active_config"),
        "recent_decision_count": summary.get("recent_decision_count"),
        "findings": findings,
    }


_READY_NOMOS_ACTIVE_STATE_PRESENT_MESSAGE = (
    "Nomos runtime active policy/config present"
)
_READY_NOMOS_ACTIVE_STATE_NO_FINDING_MESSAGE = (
    "Nomos runtime active state check failed without reporting a finding"
)
# Keep one finding readable in a terminal blocker line; the full text stays in
# the payload's `findings` array.
_READY_NOMOS_ACTIVE_STATE_MAX_FINDING_CHARS = 240
_READY_NOMOS_ACTIVE_STATE_MAX_FINDINGS_SHOWN = 2


def _ready_nomos_active_state_message(status: Mapping[str, Any]) -> str:
    """Summarize a `nomos.active_state` payload by naming its findings.

    The summary must not be a binary present/missing string: this check fails
    for seven distinct codes, and only two of them
    (`no_active_policy_bundle`, `no_active_config_set`) mean state is absent.
    Reporting a stale fingerprint, a non-blocking mode, or an unreachable
    database as "missing" sends the reader looking for state that is present
    (issue #1053), and the remedies differ. Every finding already carries an
    accurate message, so derive the summary from them rather than restating a
    classification here — that also keeps this free of a code list that could
    drift from `nomos_rollout_audit`'s.
    """
    if status.get("ok"):
        return _READY_NOMOS_ACTIVE_STATE_PRESENT_MESSAGE
    raw_findings = status.get("findings")
    parts: list[str] = []
    if isinstance(raw_findings, (list, tuple)):
        for finding in raw_findings:
            if not isinstance(finding, Mapping):
                continue
            # Fall back to the code so a finding built without a message names
            # itself rather than contributing empty text.
            text = str(
                finding.get("message")
                or finding.get("code")
                or finding.get("reason_code")
                or ""
            )
            text = " ".join(text.split())
            if len(text) > _READY_NOMOS_ACTIVE_STATE_MAX_FINDING_CHARS:
                text = (
                    text[: _READY_NOMOS_ACTIVE_STATE_MAX_FINDING_CHARS - 1]
                    + "…"
                )
            if text and text not in parts:
                parts.append(text)
    if not parts:
        return _READY_NOMOS_ACTIVE_STATE_NO_FINDING_MESSAGE
    shown = parts[:_READY_NOMOS_ACTIVE_STATE_MAX_FINDINGS_SHOWN]
    summary = "; ".join(shown)
    hidden = len(parts) - len(shown)
    if hidden:
        summary = f"{summary} (+{hidden} more finding"
        summary += "s)" if hidden > 1 else ")"
    # No "Nomos runtime active state:" prefix — every surface that renders this
    # message already prints the check id next to it (`  - {id}: {message}` in
    # `dogfood.py`, the `id` field in `deployment ready --json`), and the
    # finding messages are self-describing.
    return summary


def _ready_rollout_repo_root(
    *,
    deployment_dir: Path,
    answers: dict[str, Any],
) -> Path:
    raw = answers.get("repo_root")
    if isinstance(raw, str) and raw.strip():
        return Path(raw).expanduser().resolve()
    if deployment_dir.parent.name == "deployments":
        return deployment_dir.parent.parent.resolve()
    return deployment_dir.parent.resolve()


def _ready_rollout_project_command(
    *,
    deployment: str,
    repo_root: Path,
    mcp_port: Any,
) -> str:
    return " ".join([
        # The declared door (define-newcomer-entry-path); `okg init` aliases
        # it, but a command the CLI hands an operator names the door.
        f"{okg_prefix()} install",
        "--profile workspace-graph",
        f"--deployment-name {shlex.quote(deployment)}",
        f"--repo-root {shlex.quote(str(repo_root))}",
        "--postgres-dsn postgresql://postgres:okg@localhost:5433/"
        f"okg_{_dep._ready_slug_for_dsn(deployment)}",
        f"--mcp-port {int(mcp_port)}",
    ])


def _ready_slug_for_dsn(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in value).strip("_")


def _ready_rollout_source_registry(
    *,
    deployment_dir: Path,
    manifest: dict[str, Any],
) -> Path:
    raw = str(manifest.get("source_registry") or "source_registry.yaml")
    path = Path(raw)
    return path if path.is_absolute() else deployment_dir / path


def _ready_rollout_source_sync_summary(
    registry_path: Path,
    *,
    deployment: str,
    runtime_source_sync: dict[str, Any] | None,
    config_warnings: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    health_command = (
        f"{okg_prefix()} sync health --deployment "
        f"{shlex.quote(deployment)} --json"
    )
    if not registry_path.is_file():
        return {
            "source_registry": str(registry_path),
            "source_registry_exists": False,
            "source_count": 0,
            "expected_producer_count": 0,
            "producer_kinds": [],
            "health_command": health_command,
        }
    from okg.substrate.source_sync_status import (
        source_sync_producer_expectations,
    )
    from okg.substrate.sources.registry import load_source_registry

    registry = load_source_registry(registry_path)
    expected, disabled, warnings = source_sync_producer_expectations(
        registry,
        runtime_source_sync=runtime_source_sync,
    )
    warnings = [*list(config_warnings or []), *warnings]
    producer_rows = [*expected, *disabled]
    return {
        "source_registry": str(registry_path),
        "source_registry_exists": True,
        "source_count": len(registry),
        "expected_producer_count": len(expected),
        "disabled_producer_count": len(disabled),
        "producer_kinds": sorted({row["producer"] for row in producer_rows}),
        "expected_producers": expected,
        "disabled_producers": disabled,
        "config_warnings": warnings,
        "health_command": health_command,
    }


def _ready_runtime_source_sync(
    manifest: dict[str, Any],
    *,
    deployment: str | None = None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    from okg.substrate.runtime.config import (
        RuntimeConfig,
        resolve_source_sync,
    )

    try:
        raw_runtime = manifest.get("runtime") if isinstance(manifest, dict) else None
        return resolve_source_sync(RuntimeConfig.from_manifest_block(raw_runtime)), []
    except Exception as exc:  # noqa: BLE001
        return None, [
            _dep._ready_runtime_config_warning(deployment=deployment, error=exc)
        ]


def _ready_runtime_config_warning(
    *,
    deployment: str | None,
    error: Exception,
) -> dict[str, Any]:
    warning = {
        "code": "runtime_source_sync_config_unresolved",
        "message": (
            "deployment runtime source-sync configuration could not be resolved"
        ),
        "error": f"{type(error).__name__}: {error}",
        "owner": "deployment.runtime.source_sync",
        "recovery_command": (
            "fix deployment.yaml or run "
            f"`{okg_prefix()} deployment ready <deployment> --json`"
        ),
    }
    if deployment:
        warning["deployment"] = deployment
    return warning


def _ready_load_yaml_mapping(path: Path) -> dict[str, Any]:
    if os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR"):
        from okg.substrate.deployments import active_effective_manifest_for_path

        effective = active_effective_manifest_for_path(path)
        if effective is not None:
            return effective

    try:
        loaded = fastyaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(loaded, dict):
        return {}
    return loaded


def _ready_source_sync_liveness_status(
    *,
    dsn: str,
    deployment: str,
    deployment_dir: Path,
    max_producer_age_seconds: int,
    readiness_profile: str = "general",
    readiness_source_groups: Mapping[str, tuple[str, ...]] | None = None,
) -> dict[str, Any]:
    recovery_commands = _dep._ready_source_sync_recovery_commands(
        deployment=deployment,
        max_producer_age_seconds=max_producer_age_seconds,
        readiness_profile=readiness_profile,
    )
    manifest = _dep._ready_load_yaml_mapping(deployment_dir / "deployment.yaml")
    registry_path = _dep._ready_rollout_source_registry(
        deployment_dir=deployment_dir,
        manifest=manifest,
    )
    if not registry_path.is_file():
        out = {
            "ok": False,
            "error": "source_registry_missing",
            "message": f"source_registry.yaml not found at {registry_path}",
            "expected_producer_count": 0,
            "producer_count": 0,
            "missing_producers": [],
            "stale_producers": [],
            "config_warnings": [{
                "code": "source_registry_missing",
                "message": f"source_registry.yaml not found at {registry_path}",
            }],
            "queue_depth": [],
            "dbos_queue_depth": {},
            "dlq_count": None,
            "dlq_status": "unknown",
            "recovery_commands": recovery_commands,
        }
        from okg.substrate.source_sync_status import (
            build_source_sync_status_contract,
        )

        out["source_sync_status"] = build_source_sync_status_contract(out)
        return out
    from okg.substrate.source_sync_status import build_source_sync_status_contract
    from okg.substrate.source_sync_producers import source_sync_producer_health
    from okg.substrate.sources.registry import load_source_registry

    registry = load_source_registry(registry_path)
    runtime_source_sync, runtime_config_warnings = _dep._ready_runtime_source_sync(
        manifest,
        deployment=deployment,
    )
    runtime_block = (
        manifest.get("runtime") if isinstance(manifest, Mapping) else {}
    )
    runtime_enabled = bool(
        runtime_block.get("enabled")
        if isinstance(runtime_block, Mapping) else False
    )
    out = source_sync_producer_health(
        dsn,
        deployment=deployment,
        registry=registry,
        max_producer_age_seconds=max_producer_age_seconds,
        runtime_source_sync=runtime_source_sync,
        runtime_enabled=runtime_enabled,
        readiness_profiles=(readiness_profile,),
        readiness_source_groups=readiness_source_groups,
    )
    if runtime_config_warnings:
        out["config_warnings"] = [
            *list(out.get("config_warnings") or []),
            *runtime_config_warnings,
        ]
        out["source_sync_status"] = build_source_sync_status_contract(out)
        out["ok"] = bool(out["source_sync_status"].get("ok"))
    out["recovery_commands"] = recovery_commands
    if isinstance(out.get("source_sync_status"), dict):
        out["source_sync_status"]["recovery_commands"] = recovery_commands
    return out


def _ready_source_sync_recovery_commands(
    *,
    deployment: str,
    max_producer_age_seconds: int,
    readiness_profile: str = "dogfood",
) -> dict[str, str]:
    quoted = shlex.quote(deployment)
    prefix = (
        _dep._dogfood_local_profile_command_prefix()
        if readiness_profile == "dogfood"
        else ""
    )
    ready_command = (
        f"{prefix}{okg_prefix()} deployment ready {quoted} "
        f"--checks {shlex.quote(readiness_profile)} --json"
    )
    commands = {
        "start_supervised_worker": (
            f"{prefix}{okg_prefix()} runtime worker --deployment {quoted}"
        ),
        "refresh_poll_backstop": (
            f"{prefix}{okg_prefix()} sync poll --deployment {quoted} --json"
        ),
        "check_health": (
            f"{prefix}{okg_prefix()} sync health --deployment {quoted} --json "
            f"--max-producer-age-seconds {int(max_producer_age_seconds)}"
        ),
        "check_ready": ready_command,
    }
    if readiness_profile == "dogfood":
        commands["check_dogfood_ready"] = ready_command
    return commands


def _ready_mcp_principal_login_status(conn: Any) -> dict[str, Any] | None:
    """Probe the EFFECTIVE MCP principal (``okg_mcp``) for login authority.

    ``_ready_mcp_status`` certifies ``mcp.graph_read`` by reading through the
    OWNER connection, so a constrained MCP principal that cannot even log in
    (``rolcanlogin = false``) was silently certified as ready (issue #670). This
    probe queries ``pg_roles`` for the effective principal and refuses fail-loud,
    naming ``rolcanlogin`` and the remediation, when the role exists but cannot
    log in. A genuinely absent role or a probe error returns ``None`` (proceed):
    the owner-connection read below still runs, and role provisioning is a
    separate concern handled by the role-split path.
    """
    role = "okg_mcp"
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT rolcanlogin FROM pg_roles WHERE rolname = %s",
                (role,),
            )
            row = cur.fetchone()
    except Exception:  # noqa: BLE001 - a probe failure is not a NOLOGIN finding.
        return None
    if row is None:
        return None
    if not bool(row[0]):
        return {
            "ok": False,
            "status": "blocker",
            "reason": "required_authority_unavailable",
            "role": role,
            "rolcanlogin": False,
            "message": (
                f"MCP effective principal {role!r} has rolcanlogin=false; MCP "
                "clients cannot connect as this least-privilege role, so "
                "graph-read readiness cannot be certified through the owner "
                "connection alone"
            ),
            "next_command": f"ALTER ROLE {role} LOGIN",
        }
    return None


def _ready_mcp_status(
    conn: Any,
    deployment: str,
    *,
    session: Any = None,
    sample_verdicts: McpSampleVerdicts | None = None,
) -> dict[str, Any]:
    from okg.substrate.mcp.principal_readiness import offline_principal_readiness

    # This must run even when the graph probe returns early on a DB-role fault.
    principal_readiness = offline_principal_readiness(conn, deployment)
    # The store is passed only when a caller holds one (the console), so every
    # other caller -- and every test double of the graph probe -- sees the
    # probe's original call shape.
    extra = {} if sample_verdicts is None else {"sample_verdicts": sample_verdicts}
    result = _ready_mcp_graph_status(conn, deployment, session=session, **extra)
    result["principal_readiness"] = principal_readiness
    result["probe_scope"] = "database_graph_sample_only"
    return result


def _ready_add_mcp_principal_check(payload: dict[str, Any], mcp: dict[str, Any]) -> None:
    """Make configuration faults visible without changing the graph-read verdict."""
    report = mcp.get("principal_readiness")
    if not isinstance(report, dict):
        return
    mapping = report.get("mapping", {})
    provider = report.get("provider_validation", {})
    if mapping.get("status") == "absent":
        status, reason = "pass", "mapping_absent"
    elif mapping.get("status") == "blocked":
        status, reason = "blocker", "mapping_invalid"
    elif report.get("runtime_enforcement") in {"disabled", "invalid"}:
        status = "blocker"
        reason = f"runtime_enforcement_{report['runtime_enforcement']}"
    elif provider.get("status") == "blocked":
        status, reason = "blocker", "provider_validation_failed"
    elif mapping.get("status") == "configured" and provider.get("status") == "validated":
        status, reason = "pass", "configuration_validated"
    else:
        status, reason = READINESS_UNKNOWN_STATUS, "configuration_unmeasured"
    _ready_add_check(
        payload, "mcp.principal_configuration", status,
        f"MCP principal configuration: {reason}; live HTTP authentication unmeasured",
        {"reason": reason, **report},
    )


#: The subject the MCP graph-read probe evaluates its sample as.
_READY_MCP_PROBE_SUBJECT = "subject:mcp_client"


def _ready_mcp_sample_payload(
    session: Any, visible_sample_count: int, sample_size: int
) -> dict[str, Any]:
    return {
        "ok": visible_sample_count > 0,
        "pinned_generation_id": session.generation_id,
        "catalog_version_id": session.catalog_version_id,
        "compat_generation_id": session.compat_generation_id,
        "visible_sample_count": visible_sample_count,
        "sample_size": sample_size,
        "inspect_graph": {
            "status": "skipped",
            "reason": (
                "Nomos runtime readiness uses a bounded "
                "source-lineage-filtered sample"
            ),
        },
        "nomos": {
            "runtime_enforcement": True,
            "probe": "source_lineage_filtered_mcp_sample",
            "subject": _READY_MCP_PROBE_SUBJECT,
        },
    }


class McpSampleVerdicts:
    """Passing MCP graph-read sample verdicts, kept while their inputs hold.

    The Nomos-filtered sample in `_ready_mcp_graph_status` evaluates 300
    policy decisions per call (about 2 s on okg-workspace, issue #2547), and
    its answer is a function of a short list of inputs: the rows of the pinned
    generation, the catalog version, the fixed probe subject, the session's
    read action and surface, the active policy bundle, the identity
    providers, subject bindings and permission grants, and the enforcement
    mode. `key` names all of them, reading the policy bundle, the identity
    rows and the mode live on every call, so a policy activation, a revoked
    binding or grant, or a mode change is seen on the next request exactly as
    it is without this store.

    Only a passing verdict is kept -- a blocker is re-measured every time --
    and a kept one expires after `max_age_seconds` as a bound on any input
    this key does not name. A session that carries an identity claim or an
    access context, or runs multi-user, is never served from here: its
    decisions depend on request facts the key does not read.

    The cost of the bound: while a verdict is kept, a read path that has just
    broken -- the sample checkout, the catalog pii_classes load, the audit
    decision insert -- is not exercised, so the page can show a pass for up
    to `max_age_seconds` after such a break. `okg deployment ready` never
    reuses and always exercises the path.

    One instance serves one process (the console holds it). Nothing else
    passes one, so the `okg deployment ready` probe is unchanged.
    """

    MAX_ENTRIES = 16

    def __init__(
        self,
        *,
        max_age_seconds: float = 300.0,
        clock: Any = time.monotonic,
    ) -> None:
        import threading

        self.max_age_seconds = float(max_age_seconds)
        self._clock = clock
        self._lock = threading.Lock()
        self._kept: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}

    @staticmethod
    def reusable(session: Any) -> bool:
        # The same sessions `visible_ids_by_decision_class` refuses to decide
        # by class (`nomos/read_filter.py`): a claim or an access context turns
        # into per-request policy facts this key does not name.
        return (
            getattr(session, "nomos_identity_claim", None) is None
            and not getattr(session, "nomos_access_context", None)
            and not getattr(session, "nomos_multi_user", False)
        )

    @staticmethod
    def key(conn: Any, session: Any) -> tuple[Any, ...]:
        """Every input the sample verdict depends on, read now."""
        from okg.substrate.nomos.read_filter import session_read_identity
        from okg.substrate.nomos.runtime_enforcement import (
            deployment_nomos_enforcement_mode,
        )
        from okg.substrate.nomos.store import load_active_policy_bundle

        deployment_name = session.deployment_name
        # The scope the read filter evaluates under (`read_filter.py`).
        bundle = load_active_policy_bundle(
            conn,
            deployment_name=deployment_name,
            scope=f"deployment:{deployment_name}",
        )
        info = conn.info
        return (
            info.host,
            info.port,
            info.dbname,
            deployment_name,
            session.generation_id,
            session.branch_name,
            session.catalog_version_id,
            session.compat_generation_id,
            _READY_MCP_PROBE_SUBJECT,
            session_read_identity(session, action=None, surface=None),
            None if bundle is None else bundle.policy_fingerprint,
            McpSampleVerdicts.identity_state(conn, deployment_name),
            deployment_nomos_enforcement_mode(deployment_name),
        )

    @staticmethod
    def identity_state(conn: Any, deployment_name: str) -> tuple[Any, ...]:
        """A digest of every identity provider, subject binding and permission
        grant row of the deployment scope, read now.

        The policy fingerprint does not cover these: they live in their own
        tables and change without a policy activation. The probe's own
        evaluation reads them only for a claim-bearing session, which is never
        served a kept verdict, but a revoked binding or grant must still end a
        kept "pass" on the next read. Each digest is over whole rows -- every
        status, window and timestamp -- so any insert, update or delete changes
        it; the tables hold tens of rows, not graph-sized data.
        """
        scope = f"deployment:{deployment_name}"
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  (SELECT count(*) || ':' || md5(coalesce(
                            string_agg(p::text, '|' ORDER BY p.provider_row_id), ''))
                     FROM okg.nomos_identity_providers p
                    WHERE p.deployment_name = %(deployment)s),
                  (SELECT count(*) || ':' || md5(coalesce(
                            string_agg(b::text, '|' ORDER BY b.binding_row_id), ''))
                     FROM okg.nomos_subject_bindings b
                    WHERE b.deployment_name = %(deployment)s
                      AND b.scope = %(scope)s),
                  (SELECT count(*) || ':' || md5(coalesce(
                            string_agg(g::text, '|' ORDER BY g.grant_row_id), ''))
                     FROM okg.nomos_permission_grants g
                    WHERE g.deployment_name = %(deployment)s
                      AND g.scope = %(scope)s)
                """,
                {"deployment": deployment_name, "scope": scope},
            )
            row = cur.fetchone()
        if row is None:
            raise RuntimeError("Nomos identity state read returned no row")
        return tuple(row)

    def lookup(self, key: tuple[Any, ...]) -> dict[str, Any] | None:
        """The kept verdict for `key`, marked as reused, or None."""
        import copy

        with self._lock:
            kept = self._kept.get(key)
        if kept is None:
            return None
        computed_at, verdict = kept
        age = self._clock() - computed_at
        if age > self.max_age_seconds:
            return None
        out = copy.deepcopy(verdict)
        out["verdict_reuse"] = {
            "status": "reused",
            "age_seconds": round(age, 3),
            "max_age_seconds": self.max_age_seconds,
        }
        return out

    def keep(self, key: tuple[Any, ...], verdict: dict[str, Any]) -> None:
        """Keep a passing verdict; a blocker is never kept."""
        import copy

        if verdict.get("ok") is not True:
            return
        with self._lock:
            self._kept.pop(key, None)
            self._kept[key] = (self._clock(), copy.deepcopy(verdict))
            while len(self._kept) > self.MAX_ENTRIES:
                self._kept.pop(next(iter(self._kept)))


def _ready_mcp_graph_status(
    conn: Any,
    deployment: str,
    *,
    session: Any = None,
    sample_verdicts: McpSampleVerdicts | None = None,
) -> dict[str, Any]:
    # #670: certify graph-read authority as the effective least-privilege
    # principal, not merely as the owner connection. A NOLOGIN okg_mcp role
    # means MCP clients cannot connect at all, so readiness refuses even though
    # the owner-connection read below would otherwise succeed.
    principal = _ready_mcp_principal_login_status(conn)
    if principal is not None:
        principal_ok = read_ok(principal, source="mcp_principal_login")
        if isinstance(principal_ok, Unknown):
            # A probe result with no verdict is unknown, never a pass (#2573).
            return {
                "ok": False,
                "status": "blocker",
                "reason": "mcp_principal_login_unknown",
                "unknown_reason": principal_ok.reason,
                "message": (
                    "the MCP principal login probe returned no ok verdict "
                    f"({principal_ok.reason}); graph-read readiness cannot "
                    "be certified"
                ),
            }
        if not principal_ok:
            return principal
    try:
        from okg.substrate.mcp.session import open_session
        from okg.substrate.nomos import deployment_requires_nomos_runtime

        # A caller already holding a pinned session (the console) passes it,
        # so the latest generation is not searched for a second time.
        if session is None:
            session = open_session(conn, deployment_name=deployment)
        if deployment_requires_nomos_runtime(deployment):
            from okg.substrate.graph_store import ChronosGraphStore
            from okg.substrate.nomos.read_filter import filter_node_rows_for_session

            reuse_key: tuple[Any, ...] | None = None
            reuse_status: str | None = None
            if sample_verdicts is not None:
                if sample_verdicts.reusable(session):
                    reuse_key = sample_verdicts.key(conn, session)
                    kept = sample_verdicts.lookup(reuse_key)
                    if kept is not None:
                        return kept
                    reuse_status = "computed"
                else:
                    reuse_status = "not_reusable_session"
            checkout = ChronosGraphStore.from_connection(
                conn,
                ensure_schema=False,
            ).checkout_generation(
                session.generation_id,
                branch=session.branch_name,
            )
            sample_rows = checkout.query(
                """
                SELECT node_id, subtype, attrs, source, source_record_id, source_revision
                  FROM okg.graph_nodes
                 ORDER BY node_id
                 LIMIT 100
                """
            )
            visible_rows = filter_node_rows_for_session(
                conn,
                session,
                sample_rows,
                subject=_READY_MCP_PROBE_SUBJECT,
                audit=True,
            )
            verdict = _ready_mcp_sample_payload(
                session, len(visible_rows), len(sample_rows)
            )
            if reuse_key is not None:
                sample_verdicts.keep(reuse_key, verdict)
            if reuse_status is not None:
                verdict["verdict_reuse"] = {"status": reuse_status}
            return verdict
        from okg.substrate.mcp.tools import inspect_graph

        graph = inspect_graph(conn, session)
        from okg.substrate.mcp.run_sql import run_sql

        sql_result = run_sql(
            conn,
            session,
            "SELECT count(*) AS node_count FROM okg.v_nodes",
            row_cap=1,
            timeout_ms=5_000,
        )
        node_count = None
        if sql_result.get("rows"):
            node_count = sql_result["rows"][0][0]
        return {
            "ok": bool(node_count and int(node_count) > 0),
            "pinned_generation_id": session.generation_id,
            "catalog_version_id": session.catalog_version_id,
            "compat_generation_id": session.compat_generation_id,
            "node_count": int(node_count or 0),
            "inspect_graph": {
                "node_count": graph.get("node_count"),
                "edge_count": graph.get("edge_count"),
                "subtype_count": graph.get("subtype_count"),
                "edge_type_count": graph.get("edge_type_count"),
            },
            "run_sql": {
                "columns": sql_result.get("columns"),
                # run_sql reports the rows in its payload, not a set total
                # (issue #1440); the readiness probe only needs "did the
                # query come back with rows".
                "returned_count": sql_result.get("returned_count"),
                "query_plan_summary": sql_result.get("query_plan_summary"),
            },
        }
    except Exception as exc:  # noqa: BLE001 - convert to readiness blocker.
        return {
            "ok": False,
            "error": type(exc).__name__,
            "message": str(exc),
        }


def _ready_doctor_status(
    deployment: str,
    *,
    dsn: str,
    skip: bool,
) -> dict[str, Any]:
    if skip:
        return {"status": "skipped", "ok": None}
    import subprocess  # nosec B404

    try:
        from okg.substrate.doctor import load_deployment_doctor_checks

        custom_check_names = {
            check.name for check in load_deployment_doctor_checks(deployment)
        }
    except Exception as exc:  # noqa: BLE001 - readiness should surface load errors
        return {
            "ok": False,
            "status": "check_load_error",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }

    prefix = running_okg_argv()
    command = [
        *prefix,
        "doctor",
        "--deployment",
        deployment,
        "--json",
    ]
    if deployment in custom_check_names:
        # Right after `doctor`, whatever the helper's prefix length.
        after_doctor = len(prefix) + 1
        command[after_doctor:after_doctor] = ["--check", deployment]
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            prefix="okg-ready-doctor-",
        ) as dsn_file:
            os.chmod(dsn_file.name, 0o600)
            dsn_file.write(dsn)
            dsn_file.flush()
            child_env = os.environ.copy()
            child_env["OKG_READY_DOCTOR_DSN_FILE"] = dsn_file.name
            result = subprocess.run(  # nosec B603
                command,
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
                env=child_env,
            )
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "status": "command_failed",
            "reason": "doctor_command_timeout",
            "command": command,
            "timeout_seconds": exc.timeout,
            "state_semantics": _dep.READY_STATE_SEMANTICS,
        }
    except Exception as exc:  # noqa: BLE001 - readiness must fail loudly.
        return {
            "ok": False,
            "status": "command_failed",
            "reason": "doctor_command_failed",
            "error": type(exc).__name__,
            "message": _ready_redact_doctor_value(str(exc), dsn),
            "command": command,
            "state_semantics": _dep.READY_STATE_SEMANTICS,
        }
    # MERGE NOTE (dev <- codex/okg-workspace-cutover-followup): the two sides
    # are complementary, so both are kept. dev contributed the fail-closed
    # verdict checks below (issue #690: a zero exit alone is NOT positive
    # proof); the cutover branch contributed DSN redaction of doctor output.
    # Redaction is applied at every return point, including dev's new
    # early-return branches.
    parsed = _ready_redact_doctor_value(
        _dep._ready_parse_json(result.stdout),
        dsn,
    )
    if result.returncode != 0:
        return {
            "ok": False,
            "status": "command_failed",
            "reason": "doctor_command_nonzero",
            "returncode": result.returncode,
            "command": command,
            "json": parsed,
            "stderr": _ready_redact_doctor_value(result.stderr.strip(), dsn),
            "state_semantics": _dep.READY_STATE_SEMANTICS,
        }
    # returncode 0 but the doctor JSON is missing/unparseable or omits the `ok`
    # verdict: a zero exit alone is NOT positive proof. The prior code folded
    # this to `(parsed or {}).get("ok", True)`, so an empty or truncated doctor
    # payload passed readiness (issue #690). Absence of a parsed verdict fails
    # closed with a typed reason.
    if not isinstance(parsed, dict) or "ok" not in parsed:
        return {
            "ok": False,
            "status": "command_failed",
            "reason": "doctor_json_unparseable",
            "returncode": result.returncode,
            "command": command,
            "json": parsed,
            "stderr": _ready_redact_doctor_value(result.stderr.strip(), dsn),
            "state_semantics": _dep.READY_STATE_SEMANTICS,
        }
    doctor_ok = bool(parsed["ok"])
    return {
        "ok": doctor_ok,
        "status": "pass" if doctor_ok else "command_failed",
        **({} if doctor_ok else {"reason": "doctor_reported_not_ok"}),
        "returncode": result.returncode,
        "command": command,
        "json": parsed,
        "stderr": _ready_redact_doctor_value(result.stderr.strip(), dsn),
        # dev's condition, not the cutover branch's `returncode != 0`: a zero
        # exit can now still be not-ok, and state_semantics belongs on every
        # not-ok verdict.
        **({} if doctor_ok else {"state_semantics": _dep.READY_STATE_SEMANTICS}),
    }


def _ready_redact_doctor_value(value: Any, dsn: str) -> Any:
    from okg.substrate.sources.redaction import redact_text

    if isinstance(value, dict):
        return {
            key: _ready_redact_doctor_value(item, dsn)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_ready_redact_doctor_value(item, dsn) for item in value]
    if isinstance(value, tuple):
        return tuple(_ready_redact_doctor_value(item, dsn) for item in value)
    if isinstance(value, str):
        return redact_text(value.replace(dsn, "<redacted-dsn>"))
    return value


def _ready_parse_json(text: str) -> Any:
    stripped = text.strip()
    if not stripped:
        return None
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(stripped[start : end + 1])
            except json.JSONDecodeError:
                return None
    return None


# Bound at module bottom (after all defs) to keep the circular
# `deployment <-> deployment_readiness` import safe in either import order. The
# functions above reference `_dep` only at call time.
import okg.substrate.cli.deployment as _dep  # noqa: E402
