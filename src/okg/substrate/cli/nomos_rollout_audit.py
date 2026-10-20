"""Nomos rollout + source-lineage audit helpers (`_nomos*`).

Extracted from ``cli/deployment.py`` by ``decompose-deployment-cli-godmodules``
as a PURE re-export-shim split. Every reference to a shared helper, constant, or
sibling function is routed through ``_dep`` (the live ``cli.deployment`` module
object) so that tests which ``monkeypatch.setattr`` names on
``okg.substrate.cli.deployment`` still affect calls made here. ``cli.deployment``
remains the canonical home of the lazy helper globals + ``_load_main_helpers``
and re-exports these functions at its bottom.
"""
from __future__ import annotations

import argparse  # noqa: F401 - used by moved function signatures/bodies
import hashlib  # noqa: F401
import json  # noqa: F401
import os  # noqa: F401
import shlex  # noqa: F401
from collections.abc import Mapping  # noqa: F401
from datetime import date
from pathlib import Path  # noqa: F401
from typing import Any  # noqa: F401
from okg.substrate.support.user_hints import okg_prefix

# NOTE: `import okg.substrate.cli.deployment as _dep` is done at the BOTTOM of
# this module (after all defs) — see the end of the file. The moved functions
# below reference `_dep` only at CALL time, so a forward module-global is fine,
# and deferring the import keeps the circular `deployment <-> nomos_rollout_audit`
# cycle safe regardless of which module is imported first.

__all__ = [
    "_nomos_add_string_or_list",
    "_nomos_audited_source_policies",
    "_nomos_empty_source_lineage_policy_coverage",
    "_nomos_manifest_dsn_env_vars",
    "_nomos_match_value_contains_wildcard",
    "_nomos_normalized_source_policies",
    "_nomos_policy_fingerprint",
    "_nomos_required_deployment_names",
    "_nomos_rollout_deployment_entries",
    "_nomos_rollout_entry",
    "_nomos_rollout_inventory",
    "_nomos_rollout_profile_entries",
    "_nomos_source_lineage_apply_baseline_command",
    "_nomos_source_lineage_attach_runtime_rollout_state",
    "_nomos_source_lineage_dsn_recovery_command",
    "_nomos_source_lineage_entry_deployment_dir",
    "_nomos_source_lineage_entry_readiness_profile",
    "_nomos_source_lineage_ids_for_source_entry",
    "_nomos_source_lineage_policy_coverage",
    "_nomos_source_lineage_read_policy_source_ids",
    "_nomos_source_lineage_resolve_entry_dsn",
    "_nomos_source_lineage_restamp_active_policy_command",
    "_nomos_source_lineage_repair_laundered_derived_command",
    "_nomos_source_lineage_restamp_publisher_derived_command",
    "_nomos_source_lineage_rollout_deployment_entries",
    "_nomos_source_lineage_rollout_entry",
    "_nomos_source_lineage_rollout_inventory",
    "_nomos_source_lineage_rollout_owner",
    "_nomos_source_lineage_rollout_profile_entries",
    "_nomos_source_lineage_rollout_report",
    "_nomos_source_lineage_rollout_rollback",
    "_nomos_source_lineage_rollout_state_from_runtime_reports",
    "_nomos_source_lineage_runtime_rollout_state",
    "_nomos_source_lineage_source_entries",
    "_nomos_source_lineage_static_rollout_state",
    "_nomos_source_names",
    "_nomos_source_registry_entries",
]


def _nomos_required_deployment_names(
    entries: list[dict[str, Any]],
    *,
    start: Path | None = None,
) -> set[str]:
    """Resolve the required-deployment set for a rollout audit.

    Single source of truth for both `_nomos_rollout_inventory` and
    `_nomos_source_lineage_rollout_inventory`, so the two audits can never
    bake disagreeing snapshots. The names are repository configuration
    (`deployment_fleet.nomos_required`; the `..._when_present` names are
    required only when the scanned inventory has them). A checkout that
    declares none requires none, and each audit reports where its list came
    from (`_nomos_required_source`)."""
    from okg.substrate.deployment_default import deployment_fleet

    fleet = deployment_fleet(start)
    required = set(fleet.nomos_required)
    present = {str(entry.get("name")) for entry in entries}
    required.update(
        name for name in fleet.nomos_required_when_present if name in present
    )
    return required


def _nomos_required_source(start: Path | None = None) -> str:
    """Where the audit's required-deployment list came from, stated plainly."""
    from okg.substrate.deployment_default import FLEET_HINT, deployment_fleet

    fleet = deployment_fleet(start)
    if fleet.source is None or not (
        fleet.nomos_required or fleet.nomos_required_when_present
    ):
        return f"none configured: no nomos_required under {FLEET_HINT}"
    return fleet.source


def _nomos_source_names(source_registry: Mapping[str, Any]) -> list[str]:
    from okg.substrate.deployment_upgrade import _nomos_source_names as _names

    return _names(dict(source_registry))


def _nomos_source_lineage_read_policy_source_ids(
    source_registry: Mapping[str, Any],
    *,
    deployment: str,
    deployment_dir: Path | None = None,
) -> list[str]:
    from okg.substrate.deployment_upgrade import _nomos_policy_source_ids

    return _nomos_policy_source_ids(
        dict(source_registry),
        deployment=deployment,
        deployment_dir=deployment_dir,
    )


def _nomos_audited_source_policies(
    *,
    manifest: Mapping[str, Any],
    source_registry: Mapping[str, Any],
    deployment: str,
) -> dict[str, Mapping[str, Any]]:
    from okg.substrate.nomos import audit_deployment_policy

    audit = audit_deployment_policy(
        manifest,
        source_registry=source_registry,
        deployment=deployment,
    )
    policies = audit.get("source_policies")
    if isinstance(policies, Mapping):
        from okg.substrate.deployment_upgrade import _nomos_source_policies

        filtered = {
            str(name): dict(policy)
            for name, policy in policies.items()
            if isinstance(policy, Mapping)
        }
        return _nomos_source_policies(dict(source_registry), filtered)
    return {}


def _nomos_source_lineage_rollout_inventory(
    *,
    repo_deployment_dir: Path,
    external_deployment_dir: Path,
    profile_dir: Path,
    runtime_report: bool = False,
    readiness_profile: str | None = None,
) -> dict[str, Any]:
    enforce_dsn_keys = _dep._enforce_mode_dsn_keys(
        repo_deployment_dir,
        external_deployment_dir,
    )
    entries = [
        *_dep._nomos_source_lineage_rollout_deployment_entries(
            repo_deployment_dir,
            origin="repo",
            enforce_dsn_keys=enforce_dsn_keys,
        ),
        *_dep._nomos_source_lineage_rollout_profile_entries(
            profile_dir,
            enforce_dsn_keys=enforce_dsn_keys,
        ),
    ]
    if external_deployment_dir.exists():
        entries.extend(
            _dep._nomos_source_lineage_rollout_deployment_entries(
                external_deployment_dir,
                origin="okg-deployments",
                enforce_dsn_keys=enforce_dsn_keys,
            ),
        )
    for entry in entries:
        entry["rollout_state"] = _dep._nomos_source_lineage_static_rollout_state(entry)
    if runtime_report:
        _dep._nomos_source_lineage_attach_runtime_rollout_state(
            entries,
            external_deployment_dir=external_deployment_dir,
            readiness_profile=readiness_profile,
        )
    required = _dep._nomos_required_deployment_names(
        entries, start=repo_deployment_dir
    )
    present = {str(entry.get("name")) for entry in entries}
    has_personal = any(
        (entry.get("deployment_class") == "personal_okg")
        for entry in entries
    )
    missing = sorted(required - present)
    if not has_personal:
        missing.append("personal_okg")
    failed = [
        entry for entry in entries
        if entry.get("status") not in {"pass", "deferred"}
    ]
    return {
        "schema": "okg.nomos.source_lineage_rollout_inventory.v1",
        "ok": not failed and not missing,
        "repo_deployment_dir": str(repo_deployment_dir),
        "external_deployment_dir": str(external_deployment_dir),
        "profile_dir": str(profile_dir),
        "rollout_report": _dep._nomos_source_lineage_rollout_report(
            entries,
            runtime_report=runtime_report,
        ),
        "required_deployments": sorted(required),
        "required_source": _nomos_required_source(repo_deployment_dir),
        "required_missing": missing,
        "entry_count": len(entries),
        "failed_count": len(failed),
        "entries": entries,
    }


def _nomos_source_lineage_static_rollout_state(
    entry: Mapping[str, Any],
) -> dict[str, Any]:
    name = str(entry.get("name") or "unknown")
    status = str(entry.get("status") or "unknown")
    owner = _dep._nomos_source_lineage_rollout_owner(entry)
    apply_command = _dep._nomos_source_lineage_apply_baseline_command(entry)
    if status == "deferred":
        state = "deferred"
        recovery_command = "revisit nomos.rollout.status before agent-facing use"
    elif status != "pass":
        state = "blocked"
        recovery_command = (
            f"edit {shlex.quote(str(entry.get('path') or '<manifest>'))} and "
            f"rerun {okg_prefix()} deployment nomos-source-lineage-rollout-audit"
        )
    elif entry.get("kind") == "profile":
        state = "profile_defaults_ready"
        recovery_command = (
            f"{okg_prefix()} init --profile {shlex.quote(name)} "
            "--deployment-name <deployment> --postgres-dsn <dsn>"
        )
    else:
        state = "unapplied"
        recovery_command = apply_command
    lineage_policy = entry.get("lineage_policy")
    lineage_policy = lineage_policy if isinstance(lineage_policy, Mapping) else {}
    return {
        "schema": "okg.nomos.source_lineage_rollout_state.v1",
        "status": state,
        "mode": "static",
        "owner": owner,
        "recovery_command": recovery_command,
        "apply_command": apply_command if entry.get("kind") == "deployment" else None,
        "rollback": _dep._nomos_source_lineage_rollout_rollback(entry),
        "expected_policy_bundle_id": lineage_policy.get("expected_policy_bundle_id"),
        "expected_policy_fingerprint": lineage_policy.get(
            "expected_policy_fingerprint",
        ),
        "runtime_checked": False,
    }


def _nomos_source_lineage_attach_runtime_rollout_state(
    entries: list[dict[str, Any]],
    *,
    external_deployment_dir: Path,
    readiness_profile: str | None,
) -> None:
    old_external_dir = os.environ.get("OKG_DEPLOYMENTS_DIR")
    if external_deployment_dir.exists():
        os.environ["OKG_DEPLOYMENTS_DIR"] = str(external_deployment_dir)
    try:
        for entry in entries:
            if entry.get("kind") != "deployment" or entry.get("status") != "pass":
                continue
            entry["rollout_state"] = _dep._nomos_source_lineage_runtime_rollout_state(
                entry,
                readiness_profile=(
                    readiness_profile
                    or _dep._nomos_source_lineage_entry_readiness_profile(entry)
                ),
            )
    finally:
        if old_external_dir is None:
            os.environ.pop("OKG_DEPLOYMENTS_DIR", None)
        else:
            os.environ["OKG_DEPLOYMENTS_DIR"] = old_external_dir


def _nomos_source_lineage_runtime_rollout_state(
    entry: Mapping[str, Any],
    *,
    readiness_profile: str,
) -> dict[str, Any]:
    dsn, dsn_error = _dep._nomos_source_lineage_resolve_entry_dsn(entry)
    if dsn_error is not None:
        state = _dep._nomos_source_lineage_static_rollout_state(entry)
        state.update({
            "status": "blocked",
            "mode": "dsn_unresolved",
            "runtime_checked": True,
            "readiness_profile": readiness_profile,
            "blocker": dsn_error,
            "recovery_command": _dep._nomos_source_lineage_dsn_recovery_command(
                entry,
                dsn_error,
            ),
        })
        return state
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.nomos.doctor import source_lineage_readiness_report

    deployment = str(entry.get("name") or "")
    manifest = _dep._ready_load_yaml_mapping(Path(str(entry["path"])))
    try:
        active_state = _dep._ready_nomos_active_state_status(
            deployment=deployment,
            dsn=dsn,
            manifest=manifest,
            deployment_dir=Path(str(entry["path"])).parent,
            readiness_profile=readiness_profile,
        )
        with db_connect(dsn) as conn:
            lineage = source_lineage_readiness_report(
                conn,
                deployment_name=deployment,
                readiness_profile=readiness_profile,
            )
    except Exception as exc:  # noqa: BLE001 - report the runtime blocker.
        from okg.substrate.db.dsn import redact_text

        state = _dep._nomos_source_lineage_static_rollout_state(entry)
        state.update({
            "status": "blocked",
            "mode": "runtime_unavailable",
            "runtime_checked": True,
            "readiness_profile": readiness_profile,
            "blocker": {
                "code": "nomos.source_lineage_rollout.runtime_unavailable",
                # Same leak class as the active-state probe: the connection
                # error text embeds the DSN, password included, and this
                # blocker is operator-visible.
                # Masked line by line, so the lines of the error survive.
                "message": redact_text(str(exc), dsn_flags=()),
            },
        })
        return state
    return _dep._nomos_source_lineage_rollout_state_from_runtime_reports(
        entry,
        active_state=active_state,
        lineage_report=lineage,
        readiness_profile=readiness_profile,
    )


def _nomos_source_lineage_rollout_state_from_runtime_reports(
    entry: Mapping[str, Any],
    *,
    active_state: Mapping[str, Any],
    lineage_report: Mapping[str, Any],
    readiness_profile: str,
) -> dict[str, Any]:
    state = _dep._nomos_source_lineage_static_rollout_state(entry)
    lineage_mode = str(lineage_report.get("mode") or "unknown")
    active_findings = [
        finding for finding in active_state.get("findings", [])
        if isinstance(finding, Mapping)
    ]
    active_codes = {
        str(finding.get("reason_code") or finding.get("code"))
        for finding in active_findings
    }
    lineage_codes = {
        str(error.get("code"))
        for error in (lineage_report.get("errors") or [])
        if isinstance(error, Mapping)
    }
    if lineage_mode == "active" and active_state.get("ok"):
        status = "active"
        recovery_command = (
            f"{okg_prefix()} deployment ready "
            f"{shlex.quote(str(entry.get('name') or ''))} "
            f"--checks {shlex.quote(readiness_profile)} --json"
        )
    elif lineage_mode == "stale_fingerprint" and active_state.get("ok"):
        status = "stale_policy"
        recovery_command = (
            f"{_dep._nomos_source_lineage_restamp_active_policy_command(entry)} "
            f"&& {okg_prefix()} run --once --deployment "
            f"{shlex.quote(str(entry.get('name') or ''))} --inline --apply --json"
        )
    elif (
        lineage_mode == "stale_fingerprint"
        or "nomos.runtime.active_policy_mismatch" in active_codes
        or "nomos.runtime.active_config_mismatch" in active_codes
    ):
        status = "stale_policy"
        recovery_command = _dep._nomos_source_lineage_apply_baseline_command(entry)
    elif (
        lineage_mode == "missing_policy"
        or "nomos.runtime.no_active_policy_bundle" in active_codes
        or "nomos.runtime.no_active_config_set" in active_codes
    ):
        status = "missing_policy"
        missing_sources = [
            str(source)
            for source in (lineage_report.get("sources_missing_policy") or [])
        ]
        if active_state.get("ok") and missing_sources == ["_publisher_derived"]:
            recovery_command = (
                f"{_dep._nomos_source_lineage_restamp_publisher_derived_command(entry)} "
                f"&& {okg_prefix()} run --once --deployment "
                f"{shlex.quote(str(entry.get('name') or ''))} --inline --apply --json"
            )
        else:
            recovery_command = _dep._nomos_source_lineage_apply_baseline_command(entry)
    elif lineage_mode == "bypassed":
        status = "bypass"
        recovery_command = (
            f"set nomos.runtime_enforcement: enforce in "
            f"{shlex.quote(str(entry.get('path') or '<manifest>'))}, then "
            f"{_dep._nomos_source_lineage_apply_baseline_command(entry)}"
        )
    elif "nomos_source_lineage_derived_producer_laundered" in lineage_codes:
        # Quarantined by #1544 rows, not by a schema gap: `migrate --apply`
        # would do nothing here. The repair verb is the only exit.
        status = "blocked"
        recovery_command = (
            f"{_dep._nomos_source_lineage_repair_laundered_derived_command(entry)} "
            f"&& {okg_prefix()} run --once --deployment "
            f"{shlex.quote(str(entry.get('name') or ''))} --inline --apply --json"
        )
    elif lineage_mode in {"quarantine", "missing_schema"}:
        status = "blocked"
        recovery_command = (
            f"{okg_prefix()} migrate --apply --deployment "
            f"{shlex.quote(str(entry.get('name') or ''))} && "
            f"{_dep._nomos_source_lineage_apply_baseline_command(entry)}"
        )
    elif not active_state.get("ok") or not lineage_report.get("ok"):
        status = "blocked"
        recovery_command = _dep._nomos_source_lineage_apply_baseline_command(entry)
    else:
        status = "unapplied"
        recovery_command = _dep._nomos_source_lineage_apply_baseline_command(entry)
    state.update({
        "status": status,
        "mode": lineage_mode,
        "runtime_checked": True,
        "readiness_profile": readiness_profile,
        "recovery_command": recovery_command,
        "active_state": {
            "ok": bool(active_state.get("ok")),
            "status": active_state.get("status"),
            "effective_mode": active_state.get("effective_mode"),
            "mode_source": active_state.get("mode_source"),
            "mode_source_detail": active_state.get("mode_source_detail"),
            "blocks_actions": active_state.get("blocks_actions"),
            "rollout_exception": active_state.get("rollout_exception"),
            "findings": active_findings,
            "active_policy": active_state.get("active_policy"),
            "expected_policy": active_state.get("expected_policy"),
        },
        "source_lineage": {
            "ok": bool(lineage_report.get("ok")),
            "mode": lineage_mode,
            "runtime_enforcement_mode": lineage_report.get(
                "runtime_enforcement_mode"
            ),
            "runtime_enforcement_mode_source": lineage_report.get(
                "runtime_enforcement_mode_source"
            ),
            "blocks_actions": lineage_report.get("blocks_actions"),
            "rollout_exception": lineage_report.get("rollout_exception"),
            "read_surfaces": lineage_report.get("read_surfaces") or {},
            "sources_missing_policy": (
                lineage_report.get("sources_missing_policy") or []
            ),
            "stale_policy_sources": (
                lineage_report.get("stale_policy_sources") or []
            ),
            "missing_lineage": lineage_report.get("missing_lineage") or {},
            "errors": lineage_report.get("errors") or [],
        },
    })
    return state


def _nomos_source_lineage_rollout_report(
    entries: list[Mapping[str, Any]],
    *,
    runtime_report: bool,
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    blocked_entries = []
    for entry in entries:
        state = entry.get("rollout_state")
        state = state if isinstance(state, Mapping) else {}
        status = str(state.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
        if status in {
            "blocked",
            "missing_policy",
            "stale_policy",
            "bypass",
            "unapplied",
        }:
            blocked_entries.append({
                "name": entry.get("name"),
                "kind": entry.get("kind"),
                "origin": entry.get("origin"),
                "status": status,
                "mode": state.get("mode"),
                "owner": state.get("owner"),
                "recovery_command": state.get("recovery_command"),
            })
    return {
        "schema": "okg.nomos.source_lineage_rollout_report.v1",
        "runtime_report": runtime_report,
        "ok": not blocked_entries,
        "state_counts": dict(sorted(counts.items())),
        "blocked_entries": blocked_entries,
        "rollback_policy": {
            "agent_visible_bypass_allowed": False,
            "safe_rollback_modes": [
                "mark deployment/profile non-agent-facing",
                "reactivate previous Nomos policy fingerprint",
            ],
            "forbidden_rollback_modes": [
                "raw graph reads",
                "dropping source-lineage attrs",
                "turning off source-lineage filtering for agent-visible reads",
            ],
        },
    }


def _nomos_source_lineage_rollout_owner(entry: Mapping[str, Any]) -> str:
    name = str(entry.get("name") or "unknown")
    if entry.get("kind") == "profile":
        return f"profile-owner:{name}"
    if entry.get("origin") == "okg-deployments":
        return f"okg-deployments:{name}"
    return f"deployment-owner:{name}"


def _nomos_source_lineage_apply_baseline_command(
    entry: Mapping[str, Any],
) -> str:
    name = shlex.quote(str(entry.get("name") or ""))
    deployment_dir = _dep._nomos_source_lineage_entry_deployment_dir(entry)
    command = f"{okg_prefix()} deployment nomos-source-lineage-rollout-audit"
    if deployment_dir is not None:
        command += f" --deployment-dir {shlex.quote(str(deployment_dir))}"
    command += f" --apply-baseline {name} --json"
    return command


def _nomos_source_lineage_restamp_publisher_derived_command(
    entry: Mapping[str, Any],
) -> str:
    name = shlex.quote(str(entry.get("name") or ""))
    deployment_dir = _dep._nomos_source_lineage_entry_deployment_dir(entry)
    command = f"{okg_prefix()} deployment nomos-source-lineage-rollout-audit"
    if deployment_dir is not None:
        command += f" --deployment-dir {shlex.quote(str(deployment_dir))}"
    command += f" --restamp-publisher-derived {name} --json"
    return command


def _nomos_source_lineage_repair_laundered_derived_command(
    entry: Mapping[str, Any],
) -> str:
    """The #1544 repair verb for one rollout-inventory entry."""
    name = shlex.quote(str(entry.get("name") or ""))
    deployment_dir = _dep._nomos_source_lineage_entry_deployment_dir(entry)
    command = f"{okg_prefix()} deployment nomos-source-lineage-rollout-audit"
    if deployment_dir is not None:
        command += f" --deployment-dir {shlex.quote(str(deployment_dir))}"
    command += f" --repair-laundered-derived {name} --json"
    return command


def _nomos_source_lineage_restamp_active_policy_command(
    entry: Mapping[str, Any],
) -> str:
    name = shlex.quote(str(entry.get("name") or ""))
    deployment_dir = _dep._nomos_source_lineage_entry_deployment_dir(entry)
    command = f"{okg_prefix()} deployment nomos-source-lineage-rollout-audit"
    if deployment_dir is not None:
        command += f" --deployment-dir {shlex.quote(str(deployment_dir))}"
    command += f" --restamp-active-policy-fingerprints {name} --json"
    return command


def _nomos_source_lineage_entry_deployment_dir(
    entry: Mapping[str, Any],
) -> Path | None:
    if entry.get("kind") != "deployment":
        return None
    if entry.get("origin") != "okg-deployments":
        return None
    path = Path(str(entry.get("path") or ""))
    if not str(path):
        return None
    parent = path.parent.parent
    return parent if str(parent) != "." else None


def _nomos_source_lineage_rollout_rollback(
    entry: Mapping[str, Any],
) -> dict[str, Any]:
    name = str(entry.get("name") or "unknown")
    verify_command = None
    if entry.get("kind") == "deployment":
        verify_command = (
            f"{okg_prefix()} deployment ready "
            f"{shlex.quote(name)} --checks "
            f"{shlex.quote(_dep._nomos_source_lineage_entry_readiness_profile(entry))} "
            "--json"
        )
    return {
        "agent_visible_bypass_allowed": False,
        "behavior": (
            "rollback may remove the deployment/profile from agent-facing use "
            "or restore a previous Nomos policy activation, but must keep "
            "source-lineage-filtered reads on every agent-visible surface"
        ),
        "verify_command": verify_command,
    }


def _nomos_source_lineage_entry_readiness_profile(
    entry: Mapping[str, Any],
) -> str:
    posture = entry.get("posture")
    posture = posture if isinstance(posture, Mapping) else {}
    profiles = posture.get("readiness_profiles")
    if isinstance(profiles, list):
        values = [str(profile) for profile in profiles if profile]
        if "dogfood" in values:
            return "dogfood"
        if "ci" in values:
            return "ci"
        if values:
            return values[0]
    return "general"


def _nomos_source_lineage_resolve_entry_dsn(
    entry: Mapping[str, Any],
) -> tuple[str | None, dict[str, Any] | None]:
    path = Path(str(entry.get("path") or ""))
    try:
        dsn = _dep._resolve_deployment_dsn_from_dir(
            argparse.Namespace(dsn=None),
            path.parent,
        )
        return dsn, None
    except Exception as exc:  # noqa: BLE001 - runtime report should continue.
        try:
            dsn = _dep._resolve_deployment_dsn(argparse.Namespace(
                deployment=entry.get("name"),
                dsn=None,
            ))
            return dsn, None
        except Exception as fallback_exc:  # noqa: BLE001
            return None, {
                "code": "nomos.source_lineage_rollout.dsn_unresolved",
                "message": str(exc),
                "fallback_message": str(fallback_exc),
                "required_env": _dep._nomos_manifest_dsn_env_vars(path),
            }


def _nomos_source_lineage_dsn_recovery_command(
    entry: Mapping[str, Any],
    dsn_error: Mapping[str, Any],
) -> str:
    required = [
        str(name) for name in (dsn_error.get("required_env") or []) if name
    ]
    prefix = ""
    if required:
        prefix = " ".join(f"{name}=<dsn>" for name in required) + " "
    return prefix + _dep._nomos_source_lineage_apply_baseline_command(entry)


def _nomos_manifest_dsn_env_vars(path: Path) -> list[str]:
    if not path.is_file():
        return []
    try:
        raw = _dep._ready_load_yaml_mapping(path)
    except Exception:  # noqa: BLE001 - best-effort recovery hint only.
        return []
    postgres = raw.get("postgres") if isinstance(raw, Mapping) else {}
    raw_dsn = postgres.get("dsn") if isinstance(postgres, Mapping) else None
    if not isinstance(raw_dsn, str):
        return []
    import re
    return sorted(set(re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", raw_dsn)))


def _nomos_source_lineage_rollout_deployment_entries(
    root: Path,
    *,
    origin: str,
    enforce_dsn_keys: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    entries = []
    for manifest_path in sorted(root.glob("*/deployment.yaml")):
        deployment_dir = manifest_path.parent
        manifest = _dep._ready_load_yaml_mapping(manifest_path)
        name = str(manifest.get("name") or deployment_dir.name)
        registry_path = _dep._ready_rollout_source_registry(
            deployment_dir=deployment_dir,
            manifest=manifest,
        )
        registry = (
            _dep._ready_load_yaml_mapping(registry_path)
            if registry_path.is_file() else None
        )
        entries.append(
            _dep._nomos_source_lineage_rollout_entry(
                name=name,
                kind="deployment",
                origin=origin,
                path=manifest_path,
                manifest=manifest,
                source_registry=registry,
                source_registry_path=registry_path,
                enforce_dsn_keys=enforce_dsn_keys,
            ),
        )
    return entries


def _nomos_source_lineage_rollout_profile_entries(
    root: Path,
    *,
    enforce_dsn_keys: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    entries = []
    for defaults_path in sorted(root.glob("*/deployment-defaults.yaml")):
        manifest = _dep._ready_load_yaml_mapping(defaults_path)
        entries.append(
            _dep._nomos_source_lineage_rollout_entry(
                name=defaults_path.parent.name,
                kind="profile",
                origin="profile-defaults",
                path=defaults_path,
                manifest=manifest,
                source_registry=None,
                source_registry_path=None,
                enforce_dsn_keys=enforce_dsn_keys,
            ),
        )
    return entries


def _nomos_profile_source_lineage_policy(
    *,
    manifest: Mapping[str, Any],
    name: str,
    policy_source_ids: list[str],
    source_policies: Mapping[str, Mapping[str, Any]],
    registry: Mapping[str, Any],
):
    """Compile non-activatable profile defaults for static coverage only."""
    from okg.substrate.nomos import (
        compile_policy_graph,
        deployment_baseline_policy_graph,
    )

    return compile_policy_graph(
        deployment_baseline_policy_graph(
            manifest,
            deployment_name=name,
            scope=f"deployment:{name}",
            activated_by="subject:nomos-rollout",
            source_names=policy_source_ids,
            source_policies=source_policies,
            runtime_source_names=sorted(
                _nomos_runtime_source_subject_ids(registry)
            ),
        ),
    )


def _nomos_source_lineage_rollout_entry(
    *,
    name: str,
    kind: str,
    origin: str,
    path: Path,
    manifest: Mapping[str, Any],
    source_registry: Mapping[str, Any] | None,
    source_registry_path: Path | None,
    enforce_dsn_keys: Mapping[str, str] | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    nomos = manifest.get("nomos")
    deferred = (
        isinstance(nomos, Mapping)
        and isinstance(nomos.get("rollout"), Mapping)
        and str(nomos["rollout"].get("status") or "") == "deferred"
    )
    if deferred:
        rollout = dict(nomos["rollout"])
        deferred_findings = _dep.audit_deferred_rollout(
            rollout,
            deployment_name=name,
            deployment_dsn_key=_dep._rollout_deployment_dsn_key(manifest),
            enforce_dsn_keys=enforce_dsn_keys,
            today=today,
        )
        return {
            "name": name,
            "kind": kind,
            "origin": origin,
            "path": str(path),
            "kept": True,
            "status": "deferred" if not deferred_findings else "failed",
            "deferred_reason": rollout.get("reason"),
            "deferred_owner": _dep._normalize_rollout_owner(rollout.get("owner")),
            "deferred_expiry": (
                rollout.get("expiry")
                if rollout.get("expiry") is not None
                else rollout.get("review_by")
            ),
            "deployment_class": None,
            "runtime_enforcement": None,
            "source_registry": (
                str(source_registry_path) if source_registry_path else None
            ),
            "source_count": 0,
            "source_policy_count": 0,
            "findings": deferred_findings,
        }
    if not isinstance(nomos, Mapping) or not isinstance(
        nomos.get("deployment_posture"),
        Mapping,
    ):
        return {
            "name": name,
            "kind": kind,
            "origin": origin,
            "path": str(path),
            "kept": True,
            "status": "failed",
            "deployment_class": None,
            "runtime_enforcement": (
                nomos.get("runtime_enforcement")
                if isinstance(nomos, Mapping) else None
            ),
            "source_registry": (
                str(source_registry_path) if source_registry_path else None
            ),
            "source_count": 0,
            "source_policy_count": 0,
            "findings": [{
                "code": "nomos.source_lineage_rollout.deployment_posture_missing",
                "reason_code": (
                    "nomos.source_lineage_rollout.deployment_posture_missing"
                ),
                "severity": "error",
                "message": (
                    "kept deployment/profile has no Nomos posture for "
                    "source-lineage rollout"
                ),
                "path": "nomos.deployment_posture",
            }],
        }
    from okg.substrate.nomos import (
        DEPLOYMENT_POSTURE_FIELDS,
        audit_deployment_policy,
    )

    registry = source_registry or {}
    # Expand `code_repos:` into `sources` so this audit's expected baseline
    # matches what the seed actually activates (both use the same helper).
    # Best-effort: a governance-only context without OKG_CODE_REPOS_ROOT keeps
    # the literal `sources` rather than failing the audit.
    if (
        isinstance(registry, dict)
        and registry.get("code_repos")
        and source_registry_path
    ):
        from okg.substrate.sources.registry import expand_code_repos_sources

        registry = {
            **registry,
            "sources": expand_code_repos_sources(
                registry,
                Path(str(source_registry_path)),
                allow_unresolved_base=True,
            ),
        }
    audit = audit_deployment_policy(
        manifest,
        source_registry=registry if source_registry is not None else None,
        deployment=name,
    )
    posture = audit.get("deployment_posture") or {}
    source_names = sorted(_dep._nomos_source_registry_entries(registry))
    policy_source_ids = _dep._nomos_source_lineage_read_policy_source_ids(
        registry,
        deployment=name,
        deployment_dir=path.parent,
    )
    source_policies = _dep._nomos_normalized_source_policies(
        registry,
        audit.get("source_policies") or {},
    )
    findings = list(audit.get("findings") or [])
    source_policy_missing = sorted(
        source_name
        for source_name in source_names
        if source_name not in source_policies
    )
    if source_policy_missing:
        findings.append({
            "code": "nomos.source_lineage_rollout.source_policy_missing",
            "reason_code": "nomos.source_lineage_rollout.source_policy_missing",
            "severity": "error",
            "message": "source-lineage rollout requires a policy for every source",
            "path": "nomos.source_policy_defaults",
            "source_names": source_policy_missing,
        })

    baseline_error = None
    expected_policy_graph = None
    expected_policy_fingerprint = None
    coverage = _dep._nomos_empty_source_lineage_policy_coverage()
    try:
        if kind == "deployment":
            from okg.substrate.nomos.baseline import deployment_baseline_spec

            baseline_spec = deployment_baseline_spec(
                deployment=name,
                deployment_dir=path.parent,
            )
            if not baseline_spec.get("required"):
                raise RuntimeError(
                    "Nomos rollout audit requires a deployment baseline"
                )
            expected_policy_graph = baseline_spec["policy_graph"]
            expected_policy_fingerprint = baseline_spec["policy_fingerprint"]
        else:
            profile_policy = _nomos_profile_source_lineage_policy(
                manifest=manifest,
                name=name,
                policy_source_ids=policy_source_ids,
                source_policies=source_policies,
                registry=registry,
            )
            expected_policy_graph = profile_policy.to_dict()
            expected_policy_fingerprint = profile_policy.policy_fingerprint
        coverage = _dep._nomos_source_lineage_policy_coverage(
            expected_policy_graph,
            registry_source_ids=policy_source_ids,
        )
    except Exception as exc:  # noqa: BLE001 - inventory reports the blocker.
        baseline_error = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        findings.append({
            "code": "nomos.source_lineage_rollout.baseline_policy_invalid",
            "reason_code": (
                "nomos.source_lineage_rollout.baseline_policy_invalid"
            ),
            "severity": "error",
            "message": str(exc),
            "path": "nomos.baseline_policy",
        })

    covered = set(coverage.get("covered_source_ids") or [])
    missing_lineage_policy = sorted(
        source_id for source_id in policy_source_ids if source_id not in covered
    )
    if missing_lineage_policy:
        findings.append({
            "code": "nomos.source_lineage_rollout.read_policy_missing",
            "reason_code": "nomos.source_lineage_rollout.read_policy_missing",
            "severity": "error",
            "message": (
                "baseline graph-read policy does not cover every source id"
            ),
            "path": "nomos.baseline_policy.rules",
            "source_names": missing_lineage_policy,
        })
    if source_names and coverage.get("generic_read_policy_count"):
        findings.append({
            "code": "nomos.source_lineage_rollout.generic_read_policy",
            "reason_code": "nomos.source_lineage_rollout.generic_read_policy",
            "severity": "error",
            "message": (
                "source-lineage rollout must not rely on unscoped graph-read "
                "policy rules"
            ),
            "path": "nomos.baseline_policy.rules",
            "generic_read_policy_count": coverage.get(
                "generic_read_policy_count"
            ),
        })
    source_entries = _dep._nomos_source_lineage_source_entries(
        source_registry=registry,
        source_names=source_names,
        policy_source_ids=policy_source_ids,
        source_policies=source_policies,
        covered_source_ids=covered,
    )
    error_findings = [
        finding for finding in findings
        if str(finding.get("severity") or "error") == "error"
    ]
    return {
        "name": name,
        "kind": kind,
        "origin": origin,
        "path": str(path),
        "kept": True,
        "status": "pass" if not error_findings else "failed",
        "deployment_class": posture.get("deployment_class"),
        "runtime_enforcement": nomos.get("runtime_enforcement"),
        "posture_present": True,
        "posture": {
            field: posture.get(field)
            for field in DEPLOYMENT_POSTURE_FIELDS
        },
        "source_registry": (
            str(source_registry_path) if source_registry_path else None
        ),
        "source_count": len(source_names),
        "lineage_source_count": len(policy_source_ids),
        "source_policy_count": len(source_policies),
        "source_entries": source_entries,
        "lineage_policy": {
            "expected_policy_bundle_id": (
                expected_policy_graph.get("policy_bundle_id")
                if expected_policy_graph else None
            ),
            "expected_policy_fingerprint": expected_policy_fingerprint,
            "baseline_error": baseline_error,
            "coverage": coverage,
            "missing_source_ids": missing_lineage_policy,
        },
        "source_policy_missing": source_policy_missing,
        "findings": findings,
        "warnings": audit.get("warnings") or [],
    }


def _nomos_source_lineage_source_entries(
    *,
    source_registry: Mapping[str, Any],
    source_names: list[str],
    policy_source_ids: list[str],
    source_policies: Mapping[str, Mapping[str, Any]],
    covered_source_ids: set[str],
) -> list[dict[str, Any]]:
    source_entries = _dep._nomos_source_registry_entries(source_registry)
    out = []
    for source_name in source_names:
        entry = source_entries.get(source_name) or {}
        policy = dict(source_policies.get(source_name) or {})
        lineage_ids = _dep._nomos_source_lineage_ids_for_source_entry(
            source_name,
            entry,
            policy_source_ids=policy_source_ids,
        )
        out.append({
            "source_name": source_name,
            "source_class": (
                entry.get("source_class") if isinstance(entry, Mapping) else None
            ),
            "lineage_source_ids": lineage_ids,
            "policy_origin": policy.get("policy_origin"),
            "source_policy_fingerprint": (
                _dep._nomos_policy_fingerprint(policy) if policy else None
            ),
            "lineage_policy_covered": all(
                source_id in covered_source_ids for source_id in lineage_ids
            ),
        })
    return out


def _nomos_normalized_source_policies(
    source_registry: Mapping[str, Any],
    audited_policies: Mapping[str, Any],
) -> dict[str, Mapping[str, Any]]:
    from okg.substrate.deployment_upgrade import _nomos_source_policies

    filtered = {
        str(name): dict(policy)
        for name, policy in audited_policies.items()
        if isinstance(policy, Mapping)
    }
    return _nomos_source_policies(dict(source_registry), filtered)


def _nomos_source_lineage_ids_for_source_entry(
    source_name: str,
    entry: Mapping[str, Any],
    *,
    policy_source_ids: list[str],
) -> list[str]:
    ids = {source_name}
    raw_ownership = entry.get("ownership_id") if isinstance(entry, Mapping) else None
    if isinstance(raw_ownership, str) and raw_ownership.strip():
        ids.add(raw_ownership.strip())
    admission = entry.get("admission_policy") if isinstance(entry, Mapping) else None
    if isinstance(admission, Mapping):
        raw_producer = admission.get("producer_id")
        if isinstance(raw_producer, str) and raw_producer.strip():
            ids.add(raw_producer.strip())
    suffix = f".{source_name}"
    ids.update(source_id for source_id in policy_source_ids if source_id.endswith(suffix))
    return sorted(source_id for source_id in ids if source_id in policy_source_ids)


def _nomos_source_registry_entries(
    source_registry: Mapping[str, Any],
) -> dict[str, Mapping[str, Any]]:
    sources = source_registry.get("sources")
    if isinstance(sources, Mapping):
        return {
            str(name): entry if isinstance(entry, Mapping) else {}
            for name, entry in sources.items()
        }
    if isinstance(sources, list):
        out: dict[str, Mapping[str, Any]] = {}
        for index, entry in enumerate(sources):
            if not isinstance(entry, Mapping):
                continue
            raw_name = entry.get("name") or entry.get("id") or entry.get("source")
            name = str(raw_name) if raw_name else f"source_{index}"
            out[name] = entry
        return out
    return {}


def _nomos_runtime_source_subject_ids(
    source_registry: Mapping[str, Any],
) -> set[str]:
    """Identities that runtime source-admission may present as the emit
    subject: each source's registry name PLUS its producer_id / ownership_id.

    For most sources these coincide, but a ``code_repos`` source is named
    ``git-files-<slug>`` while its runtime producer_id is ``<slug>.git-files``
    (rendered from the codebase-index template's ``{{ deployment_name }}``). The
    baseline emit rule ``rollout_allow_registered_source_runs`` is built from
    these ids via ``_source_subject_variants``; omit the producer_id and every
    code source's ``source.emit`` authorization falls through to
    ``default_deny``. Callers should pass a registry whose ``sources`` already
    has ``code_repos`` expanded (see ``expand_code_repos_sources``).
    """
    from okg.substrate.sources.registry import runtime_source_subject_ids

    return runtime_source_subject_ids(
        dict(_nomos_source_registry_entries(source_registry)),
    )


def _nomos_policy_fingerprint(policy: Mapping[str, Any]) -> str:
    payload = json.dumps(
        _dep._json_safe(dict(policy)) if callable(_dep._json_safe) else dict(policy),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _nomos_source_lineage_policy_coverage(
    bundle: Mapping[str, Any],
    *,
    registry_source_ids: list[str],
) -> dict[str, Any]:
    from okg.substrate.nomos.vocabulary import READ_BOUNDARY_RESOURCE_KINDS

    explicit: set[str] = set()
    families: set[str] = set()
    bundles_named: set[str] = set()
    generic_read_policy_count = 0
    boundary_read_policy_count = 0
    wildcard = False
    rules = bundle.get("rules") if isinstance(bundle, Mapping) else []
    if not isinstance(rules, list):
        rules = []
    for rule in rules:
        if not isinstance(rule, Mapping):
            continue
        if rule.get("effect") not in {"allow", "redact"}:
            continue
        actions = rule.get("actions") or []
        if "*" not in actions and not any(
            str(action).startswith("graph.read")
            for action in actions
        ):
            continue
        match = rule.get("match") or {}
        if not isinstance(match, Mapping):
            continue
        if match.get("resource_kind") in READ_BOUNDARY_RESOURCE_KINDS:
            boundary_read_policy_count += 1
            continue
        if not any(
            key in match
            for key in (
                "source_id",
                "source_ids",
                "source_family",
                "source_bundle",
            )
        ):
            generic_read_policy_count += 1
            continue
        if _dep._nomos_match_value_contains_wildcard(
            match.get("source_id"),
            match.get("source_ids"),
            match.get("source_family"),
            match.get("source_bundle"),
        ):
            wildcard = True
            continue
        _dep._nomos_add_string_or_list(explicit, match.get("source_id"))
        _dep._nomos_add_string_or_list(explicit, match.get("source_ids"))
        _dep._nomos_add_string_or_list(families, match.get("source_family"))
        _dep._nomos_add_string_or_list(bundles_named, match.get("source_bundle"))
    covered = set(explicit)
    for source_id in registry_source_ids:
        if any(
            source_id == family or source_id.startswith(f"{family}:")
            for family in families
        ):
            covered.add(source_id)
        if any(
            source_id == bundle_name
            or source_id.startswith(f"{bundle_name}:")
            for bundle_name in bundles_named
        ):
            covered.add(source_id)
    if wildcard:
        covered.update(registry_source_ids)
    return {
        "covered_source_ids": sorted(covered),
        "explicit_source_ids": sorted(explicit),
        "source_families": sorted(families),
        "source_bundles": sorted(bundles_named),
        "wildcard_source_policy": wildcard,
        "generic_read_policy_count": generic_read_policy_count,
        "boundary_read_policy_count": boundary_read_policy_count,
    }


def _nomos_empty_source_lineage_policy_coverage() -> dict[str, Any]:
    return {
        "covered_source_ids": [],
        "explicit_source_ids": [],
        "source_families": [],
        "source_bundles": [],
        "wildcard_source_policy": False,
        "generic_read_policy_count": 0,
        "boundary_read_policy_count": 0,
    }


def _nomos_add_string_or_list(target: set[str], value: Any) -> None:
    if isinstance(value, str) and value:
        target.add(value)
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, str) and item:
                target.add(item)


def _nomos_match_value_contains_wildcard(*values: Any) -> bool:
    for value in values:
        if value == "*":
            return True
        if isinstance(value, list) and "*" in value:
            return True
    return False


def _nomos_rollout_inventory(
    *,
    repo_deployment_dir: Path,
    external_deployment_dir: Path,
    profile_dir: Path,
) -> dict[str, Any]:
    enforce_dsn_keys = _dep._enforce_mode_dsn_keys(
        repo_deployment_dir,
        external_deployment_dir,
    )
    entries = [
        *_dep._nomos_rollout_deployment_entries(
            repo_deployment_dir,
            origin="repo",
            enforce_dsn_keys=enforce_dsn_keys,
        ),
        *_dep._nomos_rollout_profile_entries(
            profile_dir,
            enforce_dsn_keys=enforce_dsn_keys,
        ),
    ]
    if external_deployment_dir.exists():
        entries.extend(
            _dep._nomos_rollout_deployment_entries(
                external_deployment_dir,
                origin="okg-deployments",
                enforce_dsn_keys=enforce_dsn_keys,
            ),
        )
    required = _dep._nomos_required_deployment_names(
        entries, start=repo_deployment_dir
    )
    present = {str(entry.get("name")) for entry in entries}
    has_personal = any(
        (entry.get("deployment_class") == "personal_okg")
        for entry in entries
    )
    missing = sorted(required - present)
    if not has_personal:
        missing.append("personal_okg")
    failed = [
        entry for entry in entries
        if entry.get("status") not in {"pass", "deferred"}
    ]
    return {
        "schema": "okg.nomos.deployment_rollout_inventory.v1",
        "ok": not failed and not missing,
        "repo_deployment_dir": str(repo_deployment_dir),
        "external_deployment_dir": str(external_deployment_dir),
        "profile_dir": str(profile_dir),
        "required_deployments": sorted(required),
        "required_source": _nomos_required_source(repo_deployment_dir),
        "required_missing": missing,
        "entry_count": len(entries),
        "failed_count": len(failed),
        "entries": entries,
    }


def _nomos_rollout_deployment_entries(
    root: Path,
    *,
    origin: str,
    enforce_dsn_keys: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    entries = []
    for manifest_path in sorted(root.glob("*/deployment.yaml")):
        deployment_dir = manifest_path.parent
        manifest = _dep._ready_load_yaml_mapping(manifest_path)
        name = str(manifest.get("name") or deployment_dir.name)
        registry_path = _dep._ready_rollout_source_registry(
            deployment_dir=deployment_dir,
            manifest=manifest,
        )
        registry = (
            _dep._ready_load_yaml_mapping(registry_path)
            if registry_path.is_file() else None
        )
        entries.append(
            _dep._nomos_rollout_entry(
                name=name,
                kind="deployment",
                origin=origin,
                path=manifest_path,
                manifest=manifest,
                source_registry=registry,
                source_registry_path=registry_path,
                enforce_dsn_keys=enforce_dsn_keys,
            ),
        )
    return entries


def _nomos_rollout_profile_entries(
    root: Path,
    *,
    enforce_dsn_keys: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    entries = []
    for defaults_path in sorted(root.glob("*/deployment-defaults.yaml")):
        manifest = _dep._ready_load_yaml_mapping(defaults_path)
        profile_name = defaults_path.parent.name
        entries.append(
            _dep._nomos_rollout_entry(
                name=profile_name,
                kind="profile",
                origin="profile-defaults",
                path=defaults_path,
                manifest=manifest,
                source_registry=None,
                source_registry_path=None,
                enforce_dsn_keys=enforce_dsn_keys,
            ),
        )
    return entries


def _nomos_rollout_entry(
    *,
    name: str,
    kind: str,
    origin: str,
    path: Path,
    manifest: Mapping[str, Any],
    source_registry: Mapping[str, Any] | None,
    source_registry_path: Path | None,
    enforce_dsn_keys: Mapping[str, str] | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    nomos = manifest.get("nomos")
    deferred = (
        isinstance(nomos, Mapping)
        and isinstance(nomos.get("rollout"), Mapping)
        and str(nomos["rollout"].get("status") or "") == "deferred"
    )
    if deferred:
        rollout = dict(nomos["rollout"])
        deferred_findings = _dep.audit_deferred_rollout(
            rollout,
            deployment_name=name,
            deployment_dsn_key=_dep._rollout_deployment_dsn_key(manifest),
            enforce_dsn_keys=enforce_dsn_keys,
            today=today,
        )
        return {
            "name": name,
            "kind": kind,
            "origin": origin,
            "path": str(path),
            "kept": True,
            "status": "deferred" if not deferred_findings else "failed",
            "deferred_reason": rollout.get("reason"),
            "deferred_owner": _dep._normalize_rollout_owner(rollout.get("owner")),
            "deferred_expiry": (
                rollout.get("expiry")
                if rollout.get("expiry") is not None
                else rollout.get("review_by")
            ),
            "deployment_class": None,
            "runtime_enforcement": None,
            "posture_present": False,
            "source_registry": (
                str(source_registry_path) if source_registry_path else None
            ),
            "source_policy_count": 0,
            "findings": deferred_findings,
        }
    if not isinstance(nomos, Mapping) or not isinstance(
        nomos.get("deployment_posture"),
        Mapping,
    ):
        return {
            "name": name,
            "kind": kind,
            "origin": origin,
            "path": str(path),
            "kept": True,
            "status": "failed",
            "deployment_class": None,
            "runtime_enforcement": (
                nomos.get("runtime_enforcement")
                if isinstance(nomos, Mapping) else None
            ),
            "posture_present": False,
            "source_registry": (
                str(source_registry_path) if source_registry_path else None
            ),
            "source_policy_count": 0,
            "findings": [{
                "code": "nomos.rollout.deployment_posture_missing",
                "reason_code": "nomos.rollout.deployment_posture_missing",
                "severity": "error",
                "message": "kept deployment/profile has no Nomos posture",
                "path": "nomos.deployment_posture",
            }],
        }
    from okg.substrate.nomos import DEPLOYMENT_POSTURE_FIELDS, audit_deployment_policy

    audit = audit_deployment_policy(
        manifest,
        source_registry=source_registry,
        deployment=name,
    )
    posture = audit.get("deployment_posture") or {}
    return {
        "name": name,
        "kind": kind,
        "origin": origin,
        "path": str(path),
        "kept": True,
        "status": "pass" if audit.get("ok") else "failed",
        "deployment_class": posture.get("deployment_class"),
        "runtime_enforcement": nomos.get("runtime_enforcement"),
        "posture_present": True,
        "posture": {
            field: posture.get(field)
            for field in DEPLOYMENT_POSTURE_FIELDS
        },
        "source_registry": (
            str(source_registry_path) if source_registry_path else None
        ),
        "source_policy_count": audit.get("source_policy_count", 0),
        "source_policies": audit.get("source_policies") or {},
        "findings": audit.get("findings") or [],
        "warnings": audit.get("warnings") or [],
    }


# Bound at module bottom (after all defs) to keep the circular
# `deployment <-> nomos_rollout_audit` import safe in either import order. The
# functions above reference `_dep` only at call time.
import okg.substrate.cli.deployment as _dep  # noqa: E402
