"""Source-sync readiness convergence model.

This module is intentionally deployment-generic. It reads declared
deployment/source policy and combines it with live source-sync status to
answer a narrower question than "is the worker alive?": can readiness-required
sources converge to a published, completed-scope-fresh generation?
"""
from __future__ import annotations

import os
import shlex
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from okg.substrate import fastyaml
from okg.substrate.runtime.partition_queue import source_sync_queue_name
from okg.substrate.source_run_modes import COMPLETED_SCOPE_RUN_MODES
from okg.substrate.support.user_hints import okg_prefix


SOURCE_SYNC_CONVERGENCE_SCHEMA = "okg.source_sync.convergence.v1"
SOURCE_SYNC_INVENTORY_SCHEMA = "okg.source_sync.convergence.inventory.v1"

DOGFOOD_READY_SOURCE_GROUPS: dict[str, tuple[str, ...]] = {
    "codex_sessions": ("codex-sessions",),
    "claude_sessions": ("agent-sessions",),
    "pact": ("pact",),
    "openspec_history": ("openspec",),
    "coordination": ("coordination-config", "coordination-native"),
    "code": ("git-files", "code-structure"),
}
CI_READY_SOURCE_GROUPS: dict[str, tuple[str, ...]] = {
    "repo": ("git-history", "github-issues", "pact"),
    "test_artifacts": ("test-reports",),
    "dependencies": ("package-manifests",),
}
READINESS_SOURCE_GROUPS_BY_PROFILE: dict[str, dict[str, tuple[str, ...]]] = {
    "dogfood": DOGFOOD_READY_SOURCE_GROUPS,
    "ci": CI_READY_SOURCE_GROUPS,
}

READY_SOURCE_GROUPS = DOGFOOD_READY_SOURCE_GROUPS
READY_SOURCES = frozenset(
    source for sources in READY_SOURCE_GROUPS.values() for source in sources
)


def readiness_source_groups_for_profiles(
    readiness_profiles: Sequence[str] | None,
) -> dict[str, tuple[str, ...]]:
    """Return merged readiness groups for one or more deployment profiles."""
    profiles = _normalized_readiness_profiles(readiness_profiles)
    merged: dict[str, tuple[str, ...]] = {}
    for profile in profiles:
        groups = READINESS_SOURCE_GROUPS_BY_PROFILE.get(profile, {})
        for group, sources in groups.items():
            key = group if group not in merged else f"{profile}.{group}"
            merged[key] = tuple(sources)
    return merged


def readiness_source_groups_for_manifest(
    manifest: Mapping[str, Any],
    readiness_profiles: Sequence[str] | None,
) -> dict[str, tuple[str, ...]]:
    """Resolve profile groups, honoring explicit manifest overrides."""
    profiles = _normalized_readiness_profiles(readiness_profiles)
    merged = readiness_source_groups_for_profiles(profiles)
    readiness = _mapping(manifest.get("readiness"))
    configured = _mapping(readiness.get("source_groups"))
    for profile in profiles:
        if profile not in configured:
            continue
        raw_groups = configured[profile]
        if not isinstance(raw_groups, Mapping) or not raw_groups:
            raise ValueError(
                f"readiness.source_groups.{profile} must be a non-empty mapping"
            )
        profile_defaults = READINESS_SOURCE_GROUPS_BY_PROFILE.get(profile, {})
        for group in profile_defaults:
            merged.pop(group, None)
        for group, raw_sources in raw_groups.items():
            if (
                not isinstance(raw_sources, Sequence)
                or isinstance(raw_sources, str)
                or not raw_sources
                or not all(
                    isinstance(source, str) and source.strip()
                    for source in raw_sources
                )
            ):
                raise ValueError(
                    f"readiness.source_groups.{profile}.{group} must be a "
                    "non-empty source-name list"
                )
            key = str(group) if str(group) not in merged else f"{profile}.{group}"
            merged[key] = tuple(str(source).strip() for source in raw_sources)
    return merged


def readiness_source_groups_for_deployment_dir(
    deployment_dir: Path,
    readiness_profiles: Sequence[str] | None,
) -> dict[str, tuple[str, ...]]:
    manifest_path = deployment_dir / "deployment.yaml"
    if os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR"):
        from okg.substrate.deployments import active_effective_manifest_for_path

        manifest = active_effective_manifest_for_path(manifest_path)
        if manifest is None:
            manifest = _read_yaml_mapping(manifest_path)
    else:
        manifest = _read_yaml_mapping(manifest_path)
    resolved = readiness_source_groups_for_manifest(manifest, readiness_profiles)
    defaults = readiness_source_groups_for_profiles(readiness_profiles)
    default_sources = {source for sources in defaults.values() for source in sources}
    resolved_sources = {source for sources in resolved.values() for source in sources}
    omitted = default_sources - resolved_sources
    if not omitted:
        return resolved

    authority = str(
        _mapping(manifest.get("readiness")).get("override_authority") or ""
    )
    if authority != "staging_fail_closed":
        raise ValueError(
            "readiness source overrides may omit profile defaults only with "
            "readiness.override_authority=staging_fail_closed"
        )
    registry_path = _source_registry_path(
        deployment_dir=deployment_dir,
        manifest=manifest,
    )
    registry_doc = _read_yaml_mapping(registry_path)
    raw_sources = _mapping(registry_doc.get("sources"))
    invalid_omissions = sorted(
        source
        for source in omitted
        if str(_mapping(raw_sources.get(source)).get("source_class") or "")
        != "mutable_api"
    )
    mutable_defaults = _mapping(
        _mapping(
            _mapping(manifest.get("nomos")).get("source_policy_defaults")
        ).get("by_source_class")
    )
    mutable_api_policy = _mapping(mutable_defaults.get("mutable_api"))
    if invalid_omissions or mutable_api_policy.get("live_call_allowed") is not False:
        details = ", ".join(invalid_omissions or sorted(omitted))
        raise ValueError(
            "readiness source overrides may omit only mutable_api sources when "
            "Nomos sets mutable_api.live_call_allowed=false; invalid omissions: "
            f"{details}"
        )
    return resolved


def readiness_sources_for_profiles(
    readiness_profiles: Sequence[str] | None,
) -> frozenset[str]:
    groups = readiness_source_groups_for_profiles(readiness_profiles)
    return frozenset(source for sources in groups.values() for source in sources)


def readiness_profiles_for_manifest(manifest: Mapping[str, Any]) -> tuple[str, ...]:
    """Read the deployment's declared readiness profiles, defaulting to dogfood."""
    nomos = _mapping(manifest.get("nomos"))
    posture = _mapping(nomos.get("deployment_posture"))
    raw_profiles = posture.get("readiness_profiles")
    profiles = (
        tuple(str(item) for item in raw_profiles if str(item).strip())
        if isinstance(raw_profiles, Sequence) and not isinstance(raw_profiles, str)
        else ()
    )
    known = _normalized_readiness_profiles(profiles, default=())
    if known:
        return known
    if str(posture.get("deployment_class") or "") == "ci_authority":
        return ("ci",)
    return ("dogfood",)


def readiness_profiles_for_deployment_dir(deployment_dir: Path) -> tuple[str, ...]:
    return readiness_profiles_for_manifest(
        _read_yaml_mapping(deployment_dir / "deployment.yaml")
    )


def _normalized_readiness_profiles(
    readiness_profiles: Sequence[str] | None,
    *,
    default: Sequence[str] = ("dogfood",),
) -> tuple[str, ...]:
    profiles: list[str] = []
    for profile in readiness_profiles or default:
        name = str(profile or "").strip()
        if name in READINESS_SOURCE_GROUPS_BY_PROFILE and name not in profiles:
            profiles.append(name)
    return tuple(profiles)


def build_convergence_inventory(
    *,
    repo_root: Path | str | None = None,
    deployments_root: Path | str | None = None,
    deployment: str | None = None,
    all_deployments: bool = False,
) -> dict[str, Any]:
    """Return a checked-in deployment/source convergence inventory."""
    root = Path(repo_root or Path.cwd()).resolve()
    dep_root = (
        Path(deployments_root).expanduser().resolve()
        if deployments_root is not None else root / "deployments"
    )
    deployment_dirs = _deployment_dirs(
        dep_root,
        deployment=deployment,
        all_deployments=all_deployments,
    )
    reports = [
        deployment_convergence_inventory(dep_dir)
        for dep_dir in deployment_dirs
    ]
    exposed = [
        dep["deployment"]
        for dep in reports
        if dep.get("exposure", {}).get("exposed")
    ]
    return {
        "schema": SOURCE_SYNC_INVENTORY_SCHEMA,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(root),
        "deployments_root": str(dep_root),
        "deployment_count": len(reports),
        "source_count": sum(int(dep.get("source_count") or 0) for dep in reports),
        "exposed_deployments": sorted(exposed),
        "deployments": reports,
    }


def deployment_convergence_inventory(deployment_dir: Path) -> dict[str, Any]:
    manifest_path = deployment_dir / "deployment.yaml"
    manifest = _read_yaml_mapping(manifest_path)
    deployment = str(manifest.get("name") or deployment_dir.name)
    readiness_profiles = readiness_profiles_for_manifest(manifest)
    readiness_source_groups = readiness_source_groups_for_deployment_dir(
        deployment_dir,
        readiness_profiles,
    )
    registry_path = _source_registry_path(
        deployment_dir=deployment_dir,
        manifest=manifest,
    )
    runtime = _runtime_summary(manifest)
    if not registry_path.is_file():
        return {
            "deployment": deployment,
            "deployment_dir": str(deployment_dir),
            "manifest_path": str(manifest_path),
            "source_registry_path": str(registry_path),
            "source_registry_exists": False,
            "runtime": runtime,
            "readiness_profiles": list(readiness_profiles),
            "source_count": 0,
            "sources": [],
            "readiness": _readiness_summary(
                deployment,
                [],
                readiness_profiles=readiness_profiles,
                readiness_source_groups=readiness_source_groups,
            ),
            "exposure": {
                "exposed": False,
                "reason": "source_registry_missing",
                "source_count": 0,
                "sources": [],
            },
        }

    try:
        from okg.substrate.sources.registry import load_source_registry

        registry = load_source_registry(registry_path)
        sources = source_inventory_for_registry(
            deployment=deployment,
            registry=registry,
            runtime_source_sync=runtime.get("source_sync"),
            runtime_enabled=bool(runtime.get("enabled")),
            readiness_profiles=readiness_profiles,
            readiness_source_groups=readiness_source_groups,
        )
        registry_error = None
    except Exception as exc:  # noqa: BLE001 - inventory must classify failure.
        sources = []
        registry_error = {
            "error": type(exc).__name__,
            "message": str(exc),
        }

    exposure = _deployment_exposure(sources, runtime=runtime)
    out = {
        "deployment": deployment,
        "deployment_dir": str(deployment_dir),
        "manifest_path": str(manifest_path),
        "source_registry_path": str(registry_path),
        "source_registry_exists": True,
        "runtime": runtime,
        "readiness_profiles": list(readiness_profiles),
        "source_count": len(sources),
        "sources": sources,
        "readiness": _readiness_summary(
            deployment,
            sources,
            readiness_profiles=readiness_profiles,
            readiness_source_groups=readiness_source_groups,
        ),
        "exposure": exposure,
    }
    if registry_error:
        out["registry_error"] = registry_error
    return out


def source_inventory_for_registry(
    *,
    deployment: str,
    registry: Mapping[str, Any],
    runtime_source_sync: Mapping[str, Any] | None = None,
    runtime_enabled: bool = False,
    readiness_profiles: Sequence[str] | None = None,
    readiness_source_groups: Mapping[str, Sequence[str]] | None = None,
) -> list[dict[str, Any]]:
    """Classify source-sync/readiness policy for registry entries."""
    runtime_source_sync = (
        runtime_source_sync if isinstance(runtime_source_sync, Mapping) else {}
    )
    rows: list[dict[str, Any]] = []
    for source_name, entry in sorted(registry.items()):
        rows.append(source_inventory_row(
            deployment=deployment,
            source_name=str(source_name),
            entry=entry,
            runtime_source_sync=runtime_source_sync,
            runtime_enabled=runtime_enabled,
            readiness_profiles=readiness_profiles,
            readiness_source_groups=readiness_source_groups,
        ))
    return rows


def source_inventory_row(
    *,
    deployment: str,
    source_name: str,
    entry: Any,
    runtime_source_sync: Mapping[str, Any] | None = None,
    runtime_enabled: bool = False,
    readiness_profiles: Sequence[str] | None = None,
    readiness_source_groups: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    sync = getattr(entry, "sync", None)
    raw = getattr(entry, "raw", None)
    raw = raw if isinstance(raw, Mapping) else {}
    triggers = tuple(str(item) for item in getattr(sync, "triggers", ()) or ())
    default_event_mode = str(getattr(sync, "default_event_mode", "") or "")
    reconcile_mode = getattr(sync, "reconcile_mode", None)
    if reconcile_mode is not None:
        reconcile_mode = str(reconcile_mode)
    ready_sources = (
        frozenset(
            str(source)
            for sources in readiness_source_groups.values()
            for source in sources
        )
        if readiness_source_groups is not None
        else readiness_sources_for_profiles(readiness_profiles)
    )
    readiness_required = source_name in ready_sources
    credential_refs = [
        str(item) for item in (getattr(entry, "credential_refs", None) or ())
    ]
    manual_or_credential = _manual_or_credential_constraint(
        raw=raw,
        credential_refs=credential_refs,
        sync_declared=sync is not None,
        source_class=getattr(entry, "source_class", None),
    )
    progress_required = bool(
        readiness_required
        and _source_progress_freshness_capable(raw=raw, sync=sync)
    )
    completed_scope_required = bool(readiness_required and not progress_required)
    completed_scope_capable = _completed_scope_capable(
        sync=sync,
        source_class=getattr(entry, "source_class", None),
    )
    runtime_source_sync = (
        runtime_source_sync if isinstance(runtime_source_sync, Mapping) else {}
    )
    trigger_runtime = {
        "watch": _runtime_trigger_enabled(runtime_source_sync, "watch"),
        "poll": _runtime_trigger_enabled(runtime_source_sync, "poll"),
        "webhook": _runtime_trigger_enabled(runtime_source_sync, "webhook"),
    }
    return {
        "deployment": deployment,
        "source_name": source_name,
        "ownership_id": str(getattr(entry, "ownership_id", "") or source_name),
        "source_class": getattr(entry, "source_class", None),
        "enabled_by_default": bool(getattr(entry, "enabled_by_default", False)),
        "runtime": {
            "enabled": bool(runtime_enabled),
            "source_sync_enabled": bool(runtime_enabled and sync is not None),
            "trigger_runtime": trigger_runtime,
        },
        "sync": {
            "declared": sync is not None,
            "triggers": list(triggers),
            "trigger_kinds": list(triggers),
            "default_event_mode": default_event_mode or None,
            "reconcile_mode": reconcile_mode,
            "reconcile_run_mode": reconcile_mode or "scope_complete",
            "event_mode_is_completed_scope": (
                default_event_mode in COMPLETED_SCOPE_RUN_MODES
            ),
            "deletes_require_reconcile": bool(
                getattr(sync, "deletes_require_reconcile", False)
            ),
        },
        "freshness_policy": {
            "policy": (
                "source_progress"
                if progress_required else (
                    "completed_scope_watermark"
                    if readiness_required else str(
                        raw.get("freshness") or "not_readiness_gated"
                    )
                )
            ),
            "readiness_role": (
                "agent_context_required"
                if readiness_required else "not_readiness_required"
            ),
            "readiness_profiles": list(
                _normalized_readiness_profiles(readiness_profiles)
            ),
            "readiness_required": readiness_required,
            "completed_scope_required_for_readiness": completed_scope_required,
            "progress_freshness_required_for_readiness": progress_required,
            "completed_scope_capable": completed_scope_capable,
        },
        "constraints": manual_or_credential,
        "exposed_to_stale_watermark_gap": bool(
            runtime_enabled
            and readiness_required
            and sync is not None
            and completed_scope_required
            and default_event_mode not in COMPLETED_SCOPE_RUN_MODES
        ),
    }


def build_convergence_status(
    *,
    deployment: str,
    source_inventory: Sequence[Mapping[str, Any]] | None = None,
    source_sync_status: Mapping[str, Any] | None = None,
    source_freshness: Mapping[str, Any] | None = None,
    active_work_by_source: Mapping[str, Any] | None = None,
    generation: Mapping[str, Any] | None = None,
    mcp: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a single source-readiness convergence status object."""
    source_inventory = tuple(source_inventory or ())
    source_sync_status = (
        source_sync_status if isinstance(source_sync_status, Mapping) else {}
    )
    source_freshness = (
        source_freshness if isinstance(source_freshness, Mapping) else {}
    )
    active_work_by_source = (
        active_work_by_source if isinstance(active_work_by_source, Mapping) else {}
    )
    runtime_sources = _runtime_sources(source_sync_status)
    freshness_by_source = _freshness_by_source(
        deployment=deployment,
        source_freshness=source_freshness,
    )
    inventory_by_name = {
        str(row.get("source_name") or ""): row
        for row in source_inventory
        if row.get("source_name")
    }
    ownership_aliases = {
        str(row.get("ownership_id") or ""): str(row.get("source_name") or "")
        for row in source_inventory
        if row.get("ownership_id") and row.get("source_name")
    }
    for source in freshness_by_source:
        if source in ownership_aliases:
            continue
        inventory_by_name.setdefault(source, {
            "deployment": deployment,
            "source_name": source,
            "runtime": {"enabled": None, "source_sync_enabled": None},
            "sync": {
                "declared": None,
                "triggers": [],
                "default_event_mode": None,
                "reconcile_mode": None,
                "reconcile_run_mode": "scope_complete",
            },
            "freshness_policy": {
                "readiness_required": source in READY_SOURCES,
                "readiness_role": (
                    "agent_context_required"
                    if source in READY_SOURCES else "not_readiness_required"
                ),
                "completed_scope_required_for_readiness": source in READY_SOURCES,
                "completed_scope_capable": None,
            },
            "constraints": {},
        })

    source_rows = []
    for source_name in sorted(inventory_by_name):
        row = inventory_by_name[source_name]
        ownership_id = str(row.get("ownership_id") or "")
        freshness = freshness_by_source.get(
            ownership_id,
            freshness_by_source.get(source_name, {}),
        )
        runtime = runtime_sources.get(
            ownership_id,
            runtime_sources.get(source_name, {}),
        )
        active_work = _active_work_for_source(
            active_work_by_source,
            deployment=deployment,
            source_name=ownership_id or source_name,
        )
        source_rows.append(_source_convergence_row(
            deployment=deployment,
            inventory=row,
            freshness=freshness,
            runtime=runtime,
            active_work=active_work,
            generation=generation,
            mcp=mcp,
        ))

    readiness_rows = [
        row for row in source_rows
        if row.get("readiness_required") is True
    ]
    state = _rollup_convergence_state(readiness_rows)
    blockers = [
        {
            "source_name": row["source_name"],
            "state": row["state"],
            "reason": row.get("reason"),
            "blocking_stage": row.get("blocking_stage"),
            "recovery_action": row.get("recovery_action"),
        }
        for row in readiness_rows
        if row.get("state") in {"blocked", "stuck", "manual_required", "unsupported"}
    ]
    recovery_actions = [
        dict(row["recovery_action"])
        for row in readiness_rows
        if isinstance(row.get("recovery_action"), Mapping)
        and row["recovery_action"].get("action_kind") != "no_op"
    ]
    return {
        "schema": SOURCE_SYNC_CONVERGENCE_SCHEMA,
        "deployment": deployment,
        "state": state,
        "ok": state == "ready",
        "source_count": len(source_rows),
        "readiness_required_source_count": len(readiness_rows),
        "state_counts": _state_counts(readiness_rows),
        "sources": source_rows,
        "blockers": blockers,
        "recovery_plan": {
            "action_count": len(recovery_actions),
            "actions": recovery_actions,
        },
        "stage_tracking": {
            "stages": [
                "planned_action",
                "dbos_workflow",
                "source_execution",
                "admission_publish",
                "completed_scope_watermark",
                "mcp_visible_generation",
            ],
            "generation": dict(generation) if isinstance(generation, Mapping) else {},
            "mcp": dict(mcp) if isinstance(mcp, Mapping) else {},
        },
        "next_action": _next_action(state, recovery_actions, blockers),
    }


def source_convergence_for_source(
    convergence: Mapping[str, Any],
    source_name: str,
) -> dict[str, Any]:
    for row in convergence.get("sources") or ():
        if isinstance(row, Mapping) and row.get("source_name") == source_name:
            return dict(row)
    return {
        "schema": SOURCE_SYNC_CONVERGENCE_SCHEMA,
        "deployment": convergence.get("deployment"),
        "source_name": source_name,
        "state": "unsupported",
        "reason": "source_not_in_convergence_status",
    }


def _source_convergence_row(
    *,
    deployment: str,
    inventory: Mapping[str, Any],
    freshness: Mapping[str, Any],
    runtime: Mapping[str, Any],
    active_work: Mapping[str, Any],
    generation: Mapping[str, Any] | None,
    mcp: Mapping[str, Any] | None,
) -> dict[str, Any]:
    source_name = str(inventory.get("source_name") or "")
    freshness_policy = _mapping(inventory.get("freshness_policy"))
    readiness_required = bool(freshness_policy.get("readiness_required"))
    completed_required = bool(
        freshness_policy.get("completed_scope_required_for_readiness")
    )
    progress_required = bool(
        freshness_policy.get("progress_freshness_required_for_readiness")
    )
    sync = _mapping(inventory.get("sync"))
    constraints = _mapping(inventory.get("constraints"))
    fresh_status = str(freshness.get("status") or "not_evaluated")
    active_run_mode = str(active_work.get("run_mode") or "")
    active_completed = active_run_mode in COMPLETED_SCOPE_RUN_MODES
    active_stale = bool(active_work.get("stale"))
    if not readiness_required:
        state = "fresh"
        reason = "source_not_required_for_readiness"
        blocking_stage = None
    elif (
        freshness.get("status") == "fresh"
        and not _published_generation_observed(generation)
    ):
        state = "blocked"
        reason = "published_generation_not_observed"
        blocking_stage = "admission_publish"
    elif freshness.get("status") == "fresh" and not _mcp_generation_visible(mcp):
        state = "blocked"
        reason = "published_generation_not_visible_to_read_side"
        blocking_stage = "mcp_visible_generation"
    elif freshness.get("status") == "fresh":
        state = "fresh"
        reason = (
            "source_progress_freshness_fresh"
            if progress_required else "completed_scope_watermark_fresh"
        )
        blocking_stage = None
    elif constraints.get("manual_required") or constraints.get("credential_required"):
        state = "manual_required"
        reason = "manual_or_credential_constraint"
        blocking_stage = "operator_authorization"
    elif sync.get("declared") is False or not sync.get("declared"):
        state = "unsupported"
        reason = "source_sync_not_declared_for_readiness_source"
        blocking_stage = "runtime_orchestration"
    elif completed_required and active_work and active_completed and not active_stale:
        state = "converging"
        reason = "completed_scope_recovery_active"
        blocking_stage = None
    elif completed_required and active_work and active_completed and active_stale:
        state = "stuck"
        reason = "completed_scope_recovery_stale"
        blocking_stage = "dbos_workflow"
    elif completed_required and active_work and not active_completed:
        state = "blocked"
        reason = "active_incomplete_work_does_not_satisfy_completed_freshness"
        blocking_stage = "completed_scope_watermark"
    elif progress_required and active_work and not active_stale:
        state = "converging"
        reason = "source_progress_recovery_active"
        blocking_stage = None
    elif progress_required and active_work and active_stale:
        state = "stuck"
        reason = "source_progress_recovery_stale"
        blocking_stage = "dbos_workflow"
    elif progress_required and fresh_status in {"stale", "missing", "not_evaluated"}:
        state = "blocked"
        reason = f"source_progress_freshness_{fresh_status}"
        blocking_stage = "planned_action"
    elif fresh_status in {"stale", "missing", "not_evaluated"}:
        state = "blocked"
        reason = f"completed_scope_watermark_{fresh_status}"
        blocking_stage = "planned_action"
    else:
        state = "blocked"
        reason = "unknown_readiness_gap"
        blocking_stage = "planned_action"

    recovery_action = _recovery_action(
        deployment=deployment,
        source_name=source_name,
        state=state,
        inventory=inventory,
        freshness=freshness,
        active_work=active_work,
    )
    return {
        "deployment": deployment,
        "source_name": source_name,
        "readiness_required": readiness_required,
        "state": state,
        "reason": reason,
        "blocking_stage": blocking_stage,
        "freshness": dict(freshness),
        "runtime_liveness": dict(runtime),
        "active_work": dict(active_work),
        "recovery_action": recovery_action,
        "stages": _stage_statuses(
            state=state,
            freshness=freshness,
            active_work=active_work,
            generation=generation,
            mcp=mcp,
        ),
    }


def _published_generation_observed(generation: Mapping[str, Any] | None) -> bool:
    return (
        isinstance(generation, Mapping)
        and generation.get("status") == "published"
        and bool(generation.get("generation_id"))
    )


def _mcp_generation_visible(mcp: Mapping[str, Any] | None) -> bool:
    return (
        isinstance(mcp, Mapping)
        and bool(mcp.get("ok"))
        and bool(mcp.get("generation_id") or mcp.get("pinned_generation_id"))
    )


def _recovery_action(
    *,
    deployment: str,
    source_name: str,
    state: str,
    inventory: Mapping[str, Any],
    freshness: Mapping[str, Any],
    active_work: Mapping[str, Any],
) -> dict[str, Any]:
    sync = _mapping(inventory.get("sync"))
    constraints = _mapping(inventory.get("constraints"))
    freshness_policy = _mapping(inventory.get("freshness_policy"))
    progress_required = bool(
        freshness_policy.get("progress_freshness_required_for_readiness")
    )
    if state == "fresh" or not _mapping(inventory.get("freshness_policy")).get(
        "readiness_required"
    ):
        return {
            "action_kind": "no_op",
            "reason": "source_fresh_or_not_readiness_required",
        }
    if state == "manual_required":
        return {
            "action_kind": "manual_or_credential_action",
            "source_name": source_name,
            "reason": "credential_or_manual_source_cannot_be_auto_recovered",
            "constraints": dict(constraints),
        }
    if state == "unsupported":
        return {
            "action_kind": "unsupported",
            "source_name": source_name,
            "reason": "source has no runtime source-sync policy",
        }
    if freshness.get("status") == "fresh":
        # Source work cannot repair a later generation/read-side readiness
        # blocker. Re-enqueueing the same completed-scope workflow here used
        # to turn a real MCP/generation problem into the false diagnosis
        # "watermark did not move", even though the action's own trigger
        # reported the watermark as fresh.
        return {
            "action_kind": "no_op",
            "source_name": source_name,
            "reason": "source_fresh_downstream_readiness_blocked",
        }
    if active_work and str(active_work.get("run_mode") or "") in COMPLETED_SCOPE_RUN_MODES:
        if active_work.get("stale"):
            return _stale_active_work_recovery_action(
                deployment=deployment,
                source_name=source_name,
                active_work=active_work,
                progress_required=progress_required,
            )
        return {
            "action_kind": "observe_active_completed_scope_recovery",
            "source_name": source_name,
            "workflow_id": active_work.get("workflow_id"),
            "run_mode": active_work.get("run_mode"),
            "expected_watermark_movement": "last_reconcile_at_moves",
            "expected_publish_visibility": "new_published_generation_visible_to_mcp",
        }
    if progress_required and active_work:
        if active_work.get("stale"):
            return _stale_active_work_recovery_action(
                deployment=deployment,
                source_name=source_name,
                active_work=active_work,
                progress_required=True,
            )
        return {
            "action_kind": "observe_active_source_progress_recovery",
            "source_name": source_name,
            "workflow_id": active_work.get("workflow_id"),
            "run_mode": active_work.get("run_mode"),
            "expected_watermark_movement": "source_progress.last_run_moves",
            "expected_publish_visibility": "new_published_generation_visible_to_mcp",
        }
    run_mode = str(sync.get("reconcile_run_mode") or "scope_complete")
    quoted_deployment = shlex.quote(deployment)
    quoted_source = shlex.quote(source_name)
    action_kind = (
        "source_progress_reconcile"
        if progress_required else "completed_scope_reconcile"
    )
    expected_watermark = (
        "source_progress.last_run_moves"
        if progress_required else "last_reconcile_at_moves"
    )
    return {
        "action_kind": action_kind,
        "source_name": source_name,
        "run_mode": run_mode,
        "trigger_reason": str(freshness.get("status") or "stale"),
        "deduplication_key": (
            f"sync:{deployment}:convergence:{source_name}:{run_mode}"
        ),
        "expected_watermark_movement": expected_watermark,
        "expected_publish_visibility": "new_published_generation_visible_to_mcp",
        "command": (
            f"env -u OKG_DSN {okg_prefix()} ingest "
            f"--deployment {quoted_deployment} "
            f"--include {quoted_source} "
            f"--mode {shlex.quote(run_mode)} --enqueue --json"
        ),
    }


def _stale_active_work_recovery_action(
    *,
    deployment: str,
    source_name: str,
    active_work: Mapping[str, Any],
    progress_required: bool = False,
) -> dict[str, Any]:
    status = str(active_work.get("status") or "").upper()
    run_mode = str(active_work.get("run_mode") or "scope_complete")
    stale_after = int(active_work.get("stale_after_seconds") or 600)
    base: dict[str, Any] = {
        "source_name": source_name,
        "workflow_id": active_work.get("workflow_id"),
        "workflow_status": status or None,
        "run_mode": run_mode,
        "priority": active_work.get("priority"),
        "age_seconds": active_work.get("age_seconds"),
        "stale_after_seconds": stale_after,
        "expected_watermark_movement": (
            "source_progress.last_run_moves"
            if progress_required else "last_reconcile_at_moves"
        ),
        "expected_publish_visibility": "new_published_generation_visible_to_mcp",
    }
    quoted_deployment = shlex.quote(deployment)
    if status == "PENDING":
        return {
            **base,
            "action_kind": "restart_started_workflow",
            "reason": "stale_started_pending_workflow_blocks_completed_scope_recovery",
            "command": (
                f"env -u OKG_DSN {okg_prefix()} dogfood restart "
                f"--deployment {quoted_deployment} --apply --json"
            ),
        }
    if _source_sync_workflow_priority(active_work.get("priority")) < 100:
        quoted_source = shlex.quote(source_name)
        return {
            **base,
            "action_kind": "requeue_protected_workflow",
            "reason": "stale_protected_waiting_workflow_blocks_completed_scope_recovery",
            "command": (
                f"env -u OKG_DSN {okg_prefix()} ingest "
                f"--deployment {quoted_deployment} "
                f"--include {quoted_source} "
                f"--mode {shlex.quote(run_mode)} --enqueue --json"
            ),
        }
    queue_name = source_sync_queue_name(deployment)
    return {
        **base,
        "action_kind": "cancel_replaceable_workflow",
        "reason": "stale_replaceable_waiting_workflow_blocks_completed_scope_recovery",
        "command": (
            f"env -u OKG_DSN {okg_prefix()} db dbos-gc "
            f"--deployment {quoted_deployment} "
            f"--stale-after-seconds {stale_after} "
            "--status ENQUEUED "
            f"--queue-name {shlex.quote(queue_name)} "
            "--workflow-name okg_source_sync --confirm --apply --json"
        ),
    }


def _source_sync_workflow_priority(raw: Any) -> int:
    try:
        priority = int(raw)
    except (TypeError, ValueError):
        return 100
    return priority if priority > 0 else 100


def _stage_statuses(
    *,
    state: str,
    freshness: Mapping[str, Any],
    active_work: Mapping[str, Any],
    generation: Mapping[str, Any] | None,
    mcp: Mapping[str, Any] | None,
) -> dict[str, Any]:
    generation = generation if isinstance(generation, Mapping) else {}
    mcp = mcp if isinstance(mcp, Mapping) else {}
    return {
        "planned_action": {
            "status": "satisfied" if state in {"fresh", "converging", "stuck"} else "required",
        },
        "dbos_workflow": {
            "status": (
                "active"
                if active_work and not active_work.get("stale") else (
                    "stale" if active_work else "not_started"
                )
            ),
            "workflow_id": active_work.get("workflow_id"),
            "workflow_status": active_work.get("status"),
            "run_mode": active_work.get("run_mode"),
            "priority": active_work.get("priority"),
            "age_seconds": active_work.get("age_seconds"),
            "stale_after_seconds": active_work.get("stale_after_seconds"),
        },
        "source_execution": {
            "status": "not_observed",
        },
        "admission_publish": {
            "status": (
                "published"
                if generation.get("status") == "published" else "not_observed"
            ),
            "generation_id": generation.get("generation_id"),
        },
        "completed_scope_watermark": {
            "status": freshness.get("status") or "not_evaluated",
            "last_reconcile_at": freshness.get("last_reconcile_at"),
            "age_seconds": freshness.get("age_seconds"),
        },
        "mcp_visible_generation": {
            "status": "ok" if mcp.get("ok") else "not_observed",
            "generation_id": mcp.get("generation_id")
            or mcp.get("pinned_generation_id"),
        },
    }


def _deployment_dirs(
    deployments_root: Path,
    *,
    deployment: str | None,
    all_deployments: bool,
) -> list[Path]:
    if deployment and not all_deployments:
        return [deployments_root / deployment]
    return sorted(
        path.parent
        for path in deployments_root.glob("*/deployment.yaml")
        if path.is_file()
    )


def _read_yaml_mapping(path: Path) -> dict[str, Any]:
    try:
        payload = fastyaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _source_registry_path(
    *,
    deployment_dir: Path,
    manifest: Mapping[str, Any],
) -> Path:
    raw = str(manifest.get("source_registry") or "source_registry.yaml")
    path = Path(raw)
    return path.resolve() if path.is_absolute() else (deployment_dir / path).resolve()


def _runtime_summary(manifest: Mapping[str, Any]) -> dict[str, Any]:
    from okg.substrate.runtime.config import RuntimeConfig, resolve_source_sync

    runtime_block = manifest.get("runtime") if isinstance(manifest, Mapping) else None
    try:
        runtime = RuntimeConfig.from_manifest_block(
            runtime_block if isinstance(runtime_block, dict) else None
        )
        source_sync = resolve_source_sync(runtime)
        return {
            "enabled": bool(runtime.enabled),
            "source_sync": source_sync,
            "source_sync_services_enabled": {
                kind: bool(_mapping(source_sync.get(kind)).get("enabled"))
                for kind in ("watch", "poll", "webhook")
            },
        }
    except Exception as exc:  # noqa: BLE001 - inventory classifies bad manifests.
        return {
            "enabled": False,
            "source_sync": {},
            "source_sync_services_enabled": {},
            "error": type(exc).__name__,
            "message": str(exc),
        }


def _deployment_exposure(
    sources: Sequence[Mapping[str, Any]],
    *,
    runtime: Mapping[str, Any],
) -> dict[str, Any]:
    exposed_sources = [
        str(row.get("source_name"))
        for row in sources
        if row.get("exposed_to_stale_watermark_gap")
    ]
    readiness_sources = [
        str(row.get("source_name"))
        for row in sources
        if _mapping(row.get("freshness_policy")).get("readiness_required")
    ]
    if not runtime.get("enabled"):
        reason = "runtime_disabled"
    elif exposed_sources:
        reason = "readiness_sources_use_incomplete_default_event_work"
    elif readiness_sources:
        reason = "readiness_sources_have_completed_scope_default_or_no_runtime_gap"
    else:
        reason = "no_readiness_required_sources_declared"
    return {
        "exposed": bool(exposed_sources),
        "reason": reason,
        "source_count": len(exposed_sources),
        "sources": sorted(exposed_sources),
    }


def _readiness_summary(
    deployment: str,
    sources: Sequence[Mapping[str, Any]],
    *,
    readiness_profiles: Sequence[str] | None = None,
    readiness_source_groups: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    declared = {
        str(row.get("source_name"))
        for row in sources
        if row.get("source_name")
    }
    ready_groups = (
        {str(group): tuple(sources) for group, sources in readiness_source_groups.items()}
        if readiness_source_groups is not None
        else readiness_source_groups_for_profiles(readiness_profiles)
    )
    ready_sources = frozenset(
        source for sources in ready_groups.values() for source in sources
    )
    missing = sorted(ready_sources - declared)
    present = sorted(ready_sources & declared)
    return {
        "profiles": list(_normalized_readiness_profiles(readiness_profiles)),
        "groups": {
            group: list(names)
            for group, names in ready_groups.items()
        },
        "required_sources": [
            f"{deployment}.{source}" for source in sorted(ready_sources)
        ],
        "present_required_sources": [
            f"{deployment}.{source}" for source in present
        ],
        "missing_required_sources": [
            f"{deployment}.{source}" for source in missing
        ],
    }


def _source_progress_freshness_capable(*, raw: Mapping[str, Any], sync: Any) -> bool:
    params = raw.get("params") if isinstance(raw.get("params"), Mapping) else {}
    append_only = bool(params.get("append_only")) or (
        str(raw.get("deletion_semantics") or "") == "append_only"
    )
    if append_only:
        return True
    if sync is None:
        return False
    default_mode = str(getattr(sync, "default_event_mode", "") or "")
    reconcile_mode = str(getattr(sync, "reconcile_mode", "") or "")
    completed_scope_configured = (
        default_mode in COMPLETED_SCOPE_RUN_MODES
        or reconcile_mode in COMPLETED_SCOPE_RUN_MODES
    )
    return not completed_scope_configured


def _manual_or_credential_constraint(
    *,
    raw: Mapping[str, Any],
    credential_refs: Sequence[str],
    sync_declared: bool,
    source_class: Any,
) -> dict[str, Any]:
    lab_boundary = _mapping(raw.get("lab_boundary"))
    freshness = str(raw.get("freshness") or lab_boundary.get("freshness") or "")
    access_posture = str(lab_boundary.get("access_posture") or "")
    manual_required = (
        freshness in {"manual", "authorization_required"}
        or source_class == "registry_only"
    )
    credential_required = bool(credential_refs) or access_posture in {
        "credentialed",
        "restricted",
        "controlled",
        "authorization_required",
    }
    return {
        "manual_required": manual_required,
        "credential_required": credential_required,
        "credential_refs": list(credential_refs),
        "freshness": freshness or None,
        "access_posture": access_posture or None,
        "source_sync_policy_declared": sync_declared,
        "registry_only": source_class == "registry_only",
    }


def _completed_scope_capable(*, sync: Any, source_class: Any) -> bool | None:
    if sync is None:
        return None
    if source_class == "registry_only":
        return False
    default_mode = str(getattr(sync, "default_event_mode", "") or "")
    reconcile_mode = str(getattr(sync, "reconcile_mode", "") or "")
    if reconcile_mode:
        return (
            default_mode in COMPLETED_SCOPE_RUN_MODES
            or reconcile_mode in COMPLETED_SCOPE_RUN_MODES
        )
    return (
        default_mode in COMPLETED_SCOPE_RUN_MODES
        or "reconcile" in set(getattr(sync, "triggers", ()) or ())
    )


def _runtime_trigger_enabled(
    runtime_source_sync: Mapping[str, Any],
    kind: str,
) -> bool:
    block = _mapping(runtime_source_sync.get(kind))
    return bool(block.get("enabled"))


def _runtime_sources(status: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    runtime = _mapping(status.get("runtime_liveness"))
    sources = _mapping(runtime.get("sources"))
    return {
        str(name): row
        for name, row in sources.items()
        if isinstance(row, Mapping)
    }


def _freshness_by_source(
    *,
    deployment: str,
    source_freshness: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    groups = _mapping(source_freshness.get("groups"))
    for group in groups.values():
        group_map = _mapping(group)
        for row in group_map.get("sources") or ():
            if not isinstance(row, Mapping):
                continue
            raw_source = str(row.get("source") or "")
            short = raw_source
            prefix = f"{deployment}."
            if short.startswith(prefix):
                short = short[len(prefix):]
            out[short] = dict(row)
            out[short]["source_name"] = short
    for status, names in (
        ("missing", source_freshness.get("missing") or ()),
        ("stale", source_freshness.get("stale") or ()),
    ):
        for raw_source in names:
            source = str(raw_source)
            prefix = f"{deployment}."
            short = source[len(prefix):] if source.startswith(prefix) else source
            out.setdefault(short, {
                "source": source,
                "source_name": short,
                "status": status,
                "last_reconcile_at": None,
                "age_seconds": None,
            })
    return out


def _active_work_for_source(
    active_work_by_source: Mapping[str, Any],
    *,
    deployment: str,
    source_name: str,
) -> Mapping[str, Any]:
    direct = active_work_by_source.get(source_name)
    if isinstance(direct, Mapping):
        return direct
    fq = active_work_by_source.get(f"{deployment}.{source_name}")
    if isinstance(fq, Mapping):
        return fq
    return {}


def _rollup_convergence_state(rows: Sequence[Mapping[str, Any]]) -> str:
    if not rows:
        return "unsupported"
    states = {str(row.get("state") or "") for row in rows}
    if "stuck" in states:
        return "stuck"
    if "blocked" in states or "unsupported" in states or "manual_required" in states:
        return "blocked"
    if "converging" in states:
        return "converging"
    return "ready"


def _state_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = {
        "fresh": 0,
        "converging": 0,
        "blocked": 0,
        "stuck": 0,
        "manual_required": 0,
        "unsupported": 0,
    }
    for row in rows:
        state = str(row.get("state") or "unsupported")
        counts[state] = counts.get(state, 0) + 1
    return counts


def _next_action(
    state: str,
    recovery_actions: Sequence[Mapping[str, Any]],
    blockers: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if state == "ready":
        return {"code": "none", "owner": "agent"}
    blocking_actions = [
        _mapping(blocker.get("recovery_action"))
        for blocker in blockers
        if _mapping(blocker.get("recovery_action")).get("action_kind") != "no_op"
    ]
    prioritized_actions = blocking_actions or list(recovery_actions)
    if prioritized_actions:
        first = min(
            prioritized_actions,
            key=_recovery_action_priority,
        )
        return {
            "code": str(first.get("action_kind") or "recover_source"),
            "owner": _recovery_action_owner(first),
            "source_name": first.get("source_name"),
            "command": first.get("command"),
        }
    if blockers:
        first = blockers[0]
        return {
            "code": "diagnose_blocking_stage",
            "owner": "maintainer",
            "source_name": first.get("source_name"),
            "blocking_stage": first.get("blocking_stage"),
        }
    return {"code": "inspect_convergence_status", "owner": "maintainer"}


def _recovery_action_priority(action: Mapping[str, Any]) -> tuple[int, str]:
    action_kind = str(action.get("action_kind") or "")
    if action_kind in {
        "restart_started_workflow",
        "requeue_protected_workflow",
        "cancel_replaceable_workflow",
    }:
        return (0, str(action.get("source_name") or ""))
    if action.get("command"):
        return (10, str(action.get("source_name") or ""))
    if action_kind == "manual_or_credential_action":
        return (20, str(action.get("source_name") or ""))
    if action_kind == "unsupported":
        return (30, str(action.get("source_name") or ""))
    if action_kind == "observe_active_completed_scope_recovery":
        return (40, str(action.get("source_name") or ""))
    return (50, str(action.get("source_name") or ""))


def _recovery_action_owner(action: Mapping[str, Any]) -> str:
    action_kind = str(action.get("action_kind") or "")
    if action_kind == "manual_or_credential_action" or action.get("command"):
        return "operator"
    if action_kind == "unsupported":
        return "maintainer"
    return "runtime-worker"


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = [
    "COMPLETED_SCOPE_RUN_MODES",
    "CI_READY_SOURCE_GROUPS",
    "DOGFOOD_READY_SOURCE_GROUPS",
    "READINESS_SOURCE_GROUPS_BY_PROFILE",
    "READY_SOURCE_GROUPS",
    "READY_SOURCES",
    "SOURCE_SYNC_CONVERGENCE_SCHEMA",
    "SOURCE_SYNC_INVENTORY_SCHEMA",
    "build_convergence_inventory",
    "build_convergence_status",
    "deployment_convergence_inventory",
    "readiness_profiles_for_deployment_dir",
    "readiness_profiles_for_manifest",
    "readiness_source_groups_for_deployment_dir",
    "readiness_source_groups_for_manifest",
    "readiness_source_groups_for_profiles",
    "readiness_sources_for_profiles",
    "source_convergence_for_source",
    "source_inventory_for_registry",
    "source_inventory_row",
]
