"""Resumable deployment upgrade phase runner.

This is the phase-composition layer for ``okg up``. It intentionally keeps the
primitive operations visible: resolve/preflight, backup, stop services, sync
code/deps, migrate, catalog apply, restart, smoke publish, and readiness.
"""
from __future__ import annotations
from okg.substrate import fastyaml
from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.cli.renamed_flags import (
    COMPAT_CHILD_ENV as RENAMED_FLAG_COMPAT_ENV,
    COMPAT_READY_CHECKS_FLAG,
    COMPAT_TARGET_FLAG,
    COMPAT_UP_CHECKS_FLAG,
)

import ast
import io
import json
import hashlib
import math
import os
import re
import signal
import shlex
import shutil
import subprocess
from functools import lru_cache
import sys
import tempfile
import threading
import time
import tarfile
from uuid import uuid4
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping, Sequence

from okg.substrate.catalog.module_provenance import (
    UpgradeModuleProviderDropped,
    module_provenance_entries,
    module_provenance_from_composed,
    refuse_dropped_module_owner,
)
from okg.substrate.db.connection import redact, redact_value
from okg.substrate.deployment_lifecycle_contract import (
    CANONICAL_CONTRACT,
    PHASES,
    PLANNER_REPO_PATH,
    validate_lifecycle_plan_receipt,
)
from okg.substrate.deployment_lifecycle import (
    integrated_deployment_gate,
    invalidated_up_phases,
    phase_fingerprints,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.support.user_hints import okg_prefix

def _code_checkout_fleet():
    """The ``deployment_fleet`` block of the checkout this code runs from.

    The kept fleet, the strict PACT smoke deployments and the extra protected
    databases are repository configuration, not package constants (#2823):
    an installed package or the public copy declares none.
    """
    from okg.substrate.deployment_default import deployment_fleet

    return deployment_fleet(Path(__file__).resolve().parents[3])

_GENERATION_RECOVERY_FAILPOINT: Callable[[str], None] | None = None
BACKUP_ARCHIVE_CHECKS = (
    "size_ok",
    "private_mode",
    "current_owner",
    "digest_ok",
    "toc_readable",
    "manifest_ok",
    "symlink_safe",
)
BUDGET_RESUME_HEADROOM_FACTOR = 1.25
BUDGET_RESUME_IDENTITY_CHECKS = frozenset({
    "database_owner_equal",
    "extension_versions_equal",
    "schema_catalog_equal",
    "schema_migrations_equal",
})
PHASE_ATTEMPT_STATUSES = frozenset({
    "not_started", "active", "uncertain", "succeeded", "failed",
    "skipped", "superseded",
})
PHASE_IDEMPOTENCY = {
    "preflight": "pure_observation",
    "backup": "reconcile_cleanup_and_archive",
    "stop_services": "reconcile_fence_and_process",
    "sync_code": "converge_exact_commit",
    "dependency_sync": "converge_exact_lock",
    "migrate": "reconcile_migration_ledger",
    "catalog_apply": "converge_catalog_fingerprints",
    "nomos_baseline": "converge_nomos_fingerprints",
    "generation_recovery": "operation_bound_successor",
    "attribute_read_model": "generation_bound_rebuild",
    "restart_services": "reconcile_service_identity",
    "source_convergence": "stable_run_key",
    "smoke_publish": "non_idempotent_when_external",
    "readiness": "generation_bound_observation",
}
PHASES_WITH_POSSIBLE_EFFECT = frozenset(PHASES) - {"preflight", "readiness"}


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    def json_payload(self) -> Any | None:
        try:
            return json.loads(self.stdout)
        except (TypeError, ValueError):
            return None


CommandRunner = Callable[[list[str], dict[str, str]], CommandResult]


@dataclass(frozen=True)
class UpgradeOptions:
    deployment: str
    profile: str = "prod"
    dsn: str | None = None
    include: tuple[str, ...] = ()
    state_file: Path | None = None
    backup_dir: Path | None = None
    # Keep-last-N retention for verified backup archives, applied only at the
    # end of a fully successful upgrade (see _finalize_backup_retention).
    backup_keep: int = 1
    verified_backup_state: Path | None = None
    verified_backup_source_revision: str | None = None
    lifecycle_plan: Path | None = None
    # Exact digest emitted by ``--plan``.  It binds the resolved database,
    # immutable effect plan, and restore-point source.  Generic yes/no flags
    # cannot acknowledge a substituted target or irreversible provider call.
    confirm_safety_plan: str | None = None
    # Baseline-owned, read-only bridge for mutable dev-channel controllers.
    # The installed planner inspects this immutable Git object without
    # importing or executing candidate Python, then stops after backup.
    precheckout_target_revision: str | None = None
    readiness_profile: str = "general"
    # Mutate phases default ON: a bare `okg up` refreshes the worker (code,
    # deps, service restart) so it can never report success while leaving the
    # worker on stale code. `--plan` (plan_only) opts into a plan-only preview.
    apply_services: bool = True
    skip_backup: bool = False
    skip_migrate: bool = False
    skip_destructive_migrations: bool = False
    confirm_destructive_migrations: bool = False
    sync_code: bool = True
    sync_deps: bool = True
    plan_only: bool = False
    resume: bool = True
    reset_state: bool = False
    stop_after: str | None = None
    # Stable owner shared with a controller's pre-checkout runtime fence.
    # Hidden from ordinary CLI help; deployment controllers persist it before
    # any stop-side effect and every resume command carries it forward.
    runtime_fence_owner: str | None = None
    # Set only by ``okg deployment backup verify``.  The independent
    # operation always creates and restores a current archive, then writes the
    # recovery receipt consumed by risk-admitted routine upgrades.
    independent_backup_verify: bool = False
    # #3415: only for a deployment that runs from an installed okg package
    # (deployment_installed_upgrade.py). The previous release's contract file,
    # and the loud opt-in to upgrade when compatibility is unknown.
    from_contract: Path | None = None
    accept_unknown_compat: bool = False


@dataclass(frozen=True)
class FleetUpgradeOptions:
    # No default: the kept fleet is repository configuration, and the runner
    # refuses an empty fleet ("deployment fleet is empty").
    deployments: tuple[str, ...] = ()
    profile: str = "prod"
    include: tuple[str, ...] = ()
    dispositions: dict[str, "FleetDisposition"] = field(default_factory=dict)
    state_dir: Path | None = None
    backup_dir: Path | None = None
    backup_keep: int = 1
    readiness_profile: str = "general"
    apply_services: bool = True
    skip_backup: bool = False
    skip_migrate: bool = False
    skip_destructive_migrations: bool = False
    confirm_destructive_migrations: bool = False
    sync_code: bool = True
    sync_deps: bool = True
    plan_only: bool = False
    resume: bool = True
    reset_state: bool = False
    stop_after: str | None = None


@dataclass(frozen=True)
class FleetDisposition:
    status: str
    reason: str
    evidence: str | None = None

    def to_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "status": self.status,
            "reason": self.reason,
        }
        if self.evidence:
            payload["evidence"] = self.evidence
        return payload


@dataclass
class UpgradeContext:
    options: UpgradeOptions
    dsn: str
    resolution: dict[str, Any]
    env: dict[str, str]
    state_file: Path
    backup_dir: Path
    command_runner: CommandRunner
    data: dict[str, Any] = field(default_factory=dict)


def run_deployment_fleet_upgrade(
    options: FleetUpgradeOptions,
    *,
    command_runner: CommandRunner | None = None,
) -> dict[str, Any]:
    """Run ``okg up`` across the kept fleet without aborting the sweep."""
    if options.profile not in {"dev", "prod"}:
        raise SubstrateError(
            "deployment upgrade profile must be dev or prod",
            data={"profile": options.profile},
            fix_hint="pass --target dev or --target prod",
        )
    if not options.deployments:
        raise SubstrateError(
            "deployment fleet is empty",
            fix_hint="pass at least one deployment name",
        )
    from okg.substrate.deployments.admission import require_active_deployment

    for deployment in options.deployments:
        if deployment not in options.dispositions:
            require_active_deployment(deployment)
    results: list[dict[str, Any]] = []
    runner = command_runner
    for deployment in options.deployments:
        disposition = options.dispositions.get(deployment)
        if disposition is not None:
            results.append(_disposed_fleet_row(deployment, disposition))
            continue
        before = _fleet_release_snapshot(
            deployment,
            options=options,
            command_runner=runner,
        )
        row: dict[str, Any] = {
            "deployment": deployment,
            "release_before": before,
        }
        try:
            upgrade = run_deployment_upgrade(
                _fleet_upgrade_options(deployment, options),
                command_runner=runner,
            )
            row["upgrade"] = upgrade
            row["release_after"] = upgrade.get("release")
            row["readiness"] = upgrade.get("readiness")
            row["smoke_generation"] = upgrade.get("smoke_generation")
            if upgrade.get("ok"):
                row["status"] = "succeeded"
                row["ok"] = True
            elif upgrade.get("stopped_after"):
                row["status"] = "stopped"
                row["ok"] = False
                row["stopped_after"] = upgrade.get("stopped_after")
            else:
                row["status"] = "failed"
                row["ok"] = False
            if row["status"] == "failed":
                row["reason"] = upgrade.get("reason") or "upgrade failed"
        except SubstrateError as exc:
            row.update({
                "ok": False,
                "status": "failed",
                "reason": exc.message,
                "error": exc.to_json()["error"],
            })
        except Exception as exc:  # noqa: BLE001 - a fleet sweep must finish.
            row.update({
                "ok": False,
                "status": "failed",
                "reason": str(exc),
                "error": {
                    "type": type(exc).__name__,
                    "message": str(exc),
                },
            })
        results.append(row)

    succeeded = [
        row["deployment"] for row in results
        if row.get("status") == "succeeded"
    ]
    failed = [
        row["deployment"] for row in results
        if row.get("status") == "failed"
    ]
    stopped = [
        row["deployment"] for row in results
        if row.get("status") == "stopped"
    ]
    retired = [
        row["deployment"] for row in results
        if row.get("status") == "retired"
    ]
    waived = [
        row["deployment"] for row in results
        if row.get("status") == "waived"
    ]
    return {
        "ok": not failed and not stopped,
        "mode": "fleet",
        "profile": options.profile,
        "deployments": results,
        "summary": {
            "total": len(results),
            "succeeded": len(succeeded),
            "failed": len(failed),
            "stopped": len(stopped),
            "retired": len(retired),
            "waived": len(waived),
            "succeeded_deployments": succeeded,
            "failed_deployments": failed,
            "stopped_deployments": stopped,
            "retired_deployments": retired,
            "waived_deployments": waived,
        },
    }


def _disposed_fleet_row(
    deployment: str,
    disposition: FleetDisposition,
) -> dict[str, Any]:
    if disposition.status not in {"retired", "waived"}:
        raise SubstrateError(
            "unsupported fleet disposition status",
            data={
                "deployment": deployment,
                "status": disposition.status,
                "supported": ["retired", "waived"],
            },
            fix_hint="use status retired or waived",
        )
    if not disposition.reason.strip():
        raise SubstrateError(
            "fleet disposition requires a reason",
            data={"deployment": deployment, "status": disposition.status},
            fix_hint="add a non-empty reason to the fleet disposition file",
        )
    return {
        "deployment": deployment,
        "ok": True,
        "status": disposition.status,
        "reason": disposition.reason,
        "disposition": disposition.to_json(),
        "release_before": None,
        "release_after": None,
        "readiness": {
            "ok": None,
            "status": disposition.status,
            "reason": disposition.reason,
        },
        "smoke_generation": None,
    }


def _latest_phase_result(
    phase_results: list[dict[str, Any]],
    phase: str,
) -> dict[str, Any] | None:
    return next(
        (row for row in reversed(phase_results) if row.get("phase") == phase),
        None,
    )


def _prior_backup_attempt_for_resume(
    phase_results: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Keep a retained budget proof authoritative after a no-I/O refusal."""
    latest = _latest_phase_result(phase_results, "backup")
    if not (
        isinstance(latest, dict)
        and latest.get("budget_resume_refused") is True
    ):
        return latest
    retained = next(
        (
            row for row in reversed(phase_results)
            if row.get("phase") == "backup"
            and _budget_resume_failure_candidate(row)
        ),
        None,
    )
    if retained is None:
        raise SubstrateError(
            "budget-resume refusal lost its retained verified proof",
            data={"latest_backup": latest},
            fix_hint=(
                "inspect the state receipt; use --reset-state only when a new "
                "full backup is intentionally required"
            ),
        )
    return retained


def _enforce_total_runtime_budget(
    ctx: UpgradeContext,
    result: dict[str, Any],
    *,
    total_duration_seconds: float,
) -> None:
    if ctx.options.plan_only or result.get("ok") is not True:
        return
    backup = ctx.data.get("backup_result")
    budgets = backup.get("budgets_seconds") if isinstance(backup, dict) else None
    if not isinstance(budgets, Mapping):
        return
    limit = budgets.get("total")
    if (
        isinstance(limit, (int, float))
        and not isinstance(limit, bool)
        and total_duration_seconds > float(limit)
    ):
        failure = {
            "ok": False,
            "reason": "deployment total runtime budget exceeded",
            "total_budget_seconds": float(limit),
            "total_duration_seconds": total_duration_seconds,
            "budget_exceeded": True,
            "budget_exceeded_names": ["total"],
        }
        if result.get("resumed_budget_verified_backup") is True:
            failure.update({
                "budget_resume_refused": True,
                "mutation_authorized": False,
                "physical_backup_started": False,
                "restore_started": False,
                "backup_tool_child_processes_started": 0,
            })
        result.update(failure)


def _failure_service_cleanup(
    ctx: UpgradeContext,
    *,
    failed_phase: str,
) -> dict[str, Any] | None:
    service_obligation = (
        isinstance(ctx.data.get("cleanup_obligations"), Mapping)
        and "service" in ctx.data["cleanup_obligations"]
    )
    if (
        not ctx.data.get("services_stopped")
        and not service_obligation
    ) or not ctx.options.apply_services:
        return None
    obligation = ctx.data.get("cleanup_obligations", {}).get("service", {})
    prior_state = str(obligation.get("prior_state") or "unknown")
    if prior_state == "stopped":
        # The attempt observed an already-stopped worker; compensation must
        # preserve that state rather than turning a failed upgrade into a
        # service start.
        ctx.data["service_obligation_discharged"] = True
        ctx.data["services_stopped"] = False
        return {
            "phase": "failure_cleanup",
            "action": "preserve_service_state",
            "failed_phase": failed_phase,
            "prior_state": prior_state,
            "ok": True,
        }
    if prior_state not in {"running", "unknown"}:
        return {
            "phase": "failure_cleanup",
            "action": "preserve_service_state",
            "failed_phase": failed_phase,
            "prior_state": prior_state,
            "ok": False,
            "reason": "unrecognized prior service state",
        }
    try:
        cleanup = _run_phase(ctx, "restart_services")
    except BaseException as exc:  # cleanup evidence must not replace primary failure.
        timing = getattr(exc, "_okg_phase_timing", {})
        cleanup = {
            "phase": "restart_services",
            "ok": False,
            "reason": "failure cleanup restart interrupted",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "started_at": timing.get("started_at"),
            "completed_at": timing.get("completed_at", _now()),
            "duration_seconds": timing.get("duration_seconds", 0.0),
        }
    cleanup.update({
        "phase": "failure_cleanup",
        "action": "restart_services",
        "failed_phase": failed_phase,
    })
    if cleanup.get("ok") is True:
        ctx.data["services_stopped"] = False
        ctx.data["service_obligation_discharged"] = True
    return cleanup


def _persist_cleanup_obligation(
    ctx: UpgradeContext,
    obligation: Mapping[str, Any],
) -> None:
    """Durably record cleanup before a disruptive effect starts."""

    path = getattr(ctx, "state_file", None)
    # Direct phase-unit tests may omit a state file; the production
    # orchestrator establishes one before entering its phase loop.
    if not isinstance(path, Path) or not path.is_file():
        return
    state = redact_value(_load_state_file(path))
    obligations = state.get("cleanup_obligations")
    obligations = dict(obligations) if isinstance(obligations, Mapping) else {}
    key = str(obligation.get("kind") or obligation.get("action") or "unknown")
    prior_obligation = obligations.get(key)
    merged = dict(prior_obligation) if isinstance(prior_obligation, Mapping) else {}
    merged.update(dict(obligation))
    obligations[key] = redact_value(merged)
    state["cleanup_obligations"] = obligations
    state["updated_at"] = _now()
    _write_state(path, state)
    ctx.data["cleanup_obligations"] = obligations


def _refresh_cleanup_obligations(
    ctx: UpgradeContext,
    state: dict[str, Any],
) -> None:
    prior = ctx.data.get("cleanup_obligations")
    obligations = dict(prior) if isinstance(prior, Mapping) else {}
    if ctx.data.get("backup_obligation_discharged"):
        obligations.pop("backup_artifacts", None)
    if ctx.data.get("service_obligation_discharged"):
        obligations.pop("service", None)
    if ctx.data.get("services_stopped") and ctx.options.apply_services:
        obligations["service"] = {
            "kind": "service",
            "action": "restart_services",
            "deployment": ctx.options.deployment,
        }
    state["cleanup_obligations"] = obligations
    ctx.data["cleanup_obligations"] = obligations


def _recover_backup_cleanup_obligation(
    ctx: UpgradeContext, obligation: Mapping[str, Any]
) -> dict[str, Any]:
    """Discharge exact interrupted backup artifacts on process restart.

    The archive path is constrained to this run's backup directory and the
    database name is constrained to the verification prefix.  We never infer
    or glob a target, so a corrupt/ambiguous receipt blocks with a recovery
    command instead of risking the source database or rollback archive.
    """
    report: dict[str, Any] = {"kind": "backup_artifacts", "ok": True}
    temporary = obligation.get("temporary_archive_path")
    if temporary:
        path = Path(str(temporary))
        try:
            if path.resolve().parent != ctx.backup_dir.resolve():
                raise ValueError("temporary archive is outside the backup directory")
            report["temporary_archive"] = _remove_partial_backup_artifact(path)
        except Exception as exc:  # noqa: BLE001 - preserve blocked evidence.
            report["ok"] = False
            report["temporary_archive_error"] = {
                "type": type(exc).__name__, "message": str(exc)
            }
    database = str(obligation.get("database_name") or "")
    if database:
        if not re.fullmatch(r"okg_upgrade_verify_[0-9a-f]{16}", database):
            report["ok"] = False
            report["database_error"] = {"message": "invalid disposable database identity"}
        else:
            try:
                from okg.substrate.db.connection import connect as db_connect
                from psycopg import sql
                from psycopg.conninfo import conninfo_to_dict, make_conninfo

                info = conninfo_to_dict(ctx.dsn)
                info["dbname"] = "postgres"
                with db_connect(make_conninfo(**info), autocommit=True) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                            "WHERE datname = %s AND pid <> pg_backend_pid()",
                            (database,),
                        )
                        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}\n").format(sql.Identifier(database)))
                report["database_dropped"] = database
            except Exception as exc:  # noqa: BLE001
                report["ok"] = False
                report["database_error"] = {
                    "type": type(exc).__name__, "message": str(exc)
                }
    return report


class StandaloneUpgradeUnsupported(SubstrateError):
    """`okg up` or `okg deployment backup verify` met a standalone deployment.

    Standalone (#1955) declares that no Git checkout backs the deployment, and
    every `okg up` phase that syncs code, dependencies or effects resolves a
    Git revision. `okg deployment backup verify` runs the same backup phase and
    then collects the effect inventory, which also resolves a Git revision, so
    it can never succeed there either. Both are refused up front (#1983)
    instead of failing later, after a dump and restore, with a
    repository-authority error that names Git rather than the declaration.
    """

    code = "standalone_upgrade_unsupported"


def _refuse_standalone_upgrade(options: UpgradeOptions) -> None:
    """Refuse `okg up` and backup verify for a `release.kind: standalone` deployment.

    Runs before the upgrade context is built and before any backup starts, so
    no Git command, dump or restore has been issued yet. It reads only the
    manifest file. A manifest that cannot be found or parsed is left to the
    existing resolution refusals further on, which fail closed with their own
    causes; this check never turns an unreadable manifest into a pass or a
    refusal of its own.

    `okg deployment backup verify` (``independent_backup_verify``) is refused
    too: after its dump and restore it collects the effect inventory, which
    resolves a Git revision and so always fails on a standalone deployment.
    """
    from okg.substrate.deployment_authority import _read_manifest

    raw, _readable = _read_manifest(options.deployment)
    release = raw.get("release") if isinstance(raw, dict) else None
    kind = release.get("kind") if isinstance(release, dict) else None
    if not (isinstance(kind, str) and kind.strip() == "standalone"):
        return
    deployment = options.deployment
    if options.independent_backup_verify:
        raise StandaloneUpgradeUnsupported(
            "okg deployment backup verify cannot verify a standalone deployment",
            data={
                "deployment": deployment,
                "release_kind": "standalone",
                "operation": "backup_verify",
                "reason": (
                    "backup verify checks the effect inventory against a Git "
                    "code revision, and a standalone deployment has no Git "
                    "checkout"
                ),
            },
            fix_hint=(
                "take your own database backup with pg_dump before changing "
                "this deployment; if it really lives in a committed Git "
                "checkout, remove `release.kind: standalone` from its "
                "deployment.yaml"
            ),
        )
    raise StandaloneUpgradeUnsupported(
        "okg up cannot upgrade a standalone deployment",
        data={
            "deployment": deployment,
            "release_kind": "standalone",
            "operation": "up",
            "reason": (
                "a standalone deployment has no Git checkout, so there is no "
                "code revision for okg up to sync to"
            ),
        },
        fix_hint=(
            "upgrade the okg package itself: take your own database backup "
            "with pg_dump, install the newer okg wheel into the environment "
            "that runs this deployment, run "
            f"`okg migrate --deployment {deployment} --apply`, then restart "
            "its worker (see docs/operate/lifecycle.md). If this deployment really "
            "lives in a committed Git checkout and should be upgraded by "
            "`okg up`, remove `release.kind: standalone` from its "
            "deployment.yaml"
        ),
    )


def run_deployment_upgrade(
    options: UpgradeOptions,
    *,
    command_runner: CommandRunner | None = None,
) -> dict[str, Any]:
    """Run or resume the deployment upgrade phases."""
    from okg.substrate.deployments.admission import inactive_plan, require_active_deployment

    if options.plan_only:
        archived = inactive_plan(options.deployment)
        if archived is not None:
            return archived
    require_active_deployment(options.deployment)
    if options.profile not in {"dev", "prod"}:
        raise SubstrateError(
            "deployment upgrade profile must be dev or prod",
            data={"profile": options.profile},
            fix_hint="pass --target dev or --target prod",
        )
    if options.precheckout_target_revision is not None:
        revision = options.precheckout_target_revision
        invalid_shape = {
            "profile_prod": options.profile == "prod",
            "full_commit_id": (
                isinstance(revision, str)
                and re.fullmatch(r"[0-9a-f]{40,64}", revision) is not None
            ),
            "stop_after_backup": options.stop_after == "backup",
            "services_disabled": options.apply_services is False,
            "code_sync_disabled": options.sync_code is False,
            "dependency_sync_disabled": options.sync_deps is False,
            "backup_enabled": options.skip_backup is False,
            "not_plan_only": options.plan_only is False,
            "fresh_state": options.reset_state is True,
            "lifecycle_plan_supplied": options.lifecycle_plan is not None,
            "not_independent_backup_verify": (
                options.independent_backup_verify is False
            ),
            "no_verified_backup_input": options.verified_backup_state is None,
            "no_verified_backup_source_revision": (
                options.verified_backup_source_revision is None
            ),
            "default_smoke_sources": not options.include,
        }
        failed = [name for name, passed in invalid_shape.items() if not passed]
        if failed:
            raise SubstrateError(
                "precheckout target revision requires the exact read-only backup shape",
                data={
                    "failed_checks": failed,
                    "checks": invalid_shape,
                },
                fix_hint=(
                    "use a full commit with --target prod --stop-after backup "
                    "--no-apply-services --no-sync-code --no-sync-deps "
                    "--reset-state --lifecycle-plan <receipt>"
                ),
            )
    if options.verified_backup_state is not None:
        independently_verified = False
        try:
            receipt_path = options.verified_backup_state
            if not receipt_path.is_symlink():
                receipt_preview = json.loads(
                    receipt_path.read_text(encoding="utf-8")
                )
                independently_verified = (
                    isinstance(receipt_preview, Mapping)
                    and receipt_preview.get("operation")
                    == "deployment_backup_verify"
                    and receipt_preview.get("completed") is True
                )
        except (OSError, ValueError):
            independently_verified = False
        if not independently_verified:
            source_revision = options.verified_backup_source_revision
            verified_backup_checks = {
                "lifecycle_plan_supplied": options.lifecycle_plan is not None,
                "source_revision_full_commit": (
                    isinstance(source_revision, str)
                    and re.fullmatch(r"[0-9a-f]{40,64}", source_revision)
                    is not None
                ),
            }
            failed = [
                name for name, passed in verified_backup_checks.items() if not passed
            ]
            if failed:
                raise SubstrateError(
                    "verified backup admission requires an exact source commit and lifecycle plan",
                    data={"failed_checks": failed, "checks": verified_backup_checks},
                    fix_hint=(
                        "pass --verified-backup-source-revision <full-commit> "
                        "--lifecycle-plan <receipt>, or use a completed "
                        "deployment backup verify receipt"
                    ),
                )
    if options.stop_after and options.stop_after not in PHASES:
        raise SubstrateError(
            "unknown upgrade stop phase",
            data={"stop_after": options.stop_after, "phases": list(PHASES)},
            fix_hint=f"choose one of: {', '.join(PHASES)}",
        )
    if options.backup_keep < 1:
        # Loud misconfiguration, never a silent clamp: keeping fewer than one
        # archive would delete the backup this very run just verified.
        raise SubstrateError(
            "backup retention must keep at least one verified archive",
            data={"backup_keep": options.backup_keep},
            fix_hint="pass --backup-keep N with N >= 1 (default 1)",
        )
    if (
        options.skip_destructive_migrations
        and options.confirm_destructive_migrations
    ):
        raise SubstrateError(
            "destructive migration policy is contradictory",
            fix_hint=(
                "choose either --skip-destructive-migrations or "
                "--confirm-destructive-migrations"
            ),
        )

    # #3415: a deployment that runs from an installed okg package (standalone,
    # or a wheel install: release.kind distribution, declared or detected) is
    # upgraded by upgrading the package and running this same verb. Route it
    # before the context is built and before any Git revision is resolved.
    # `okg deployment backup verify` keeps its own refusal below.
    if not options.independent_backup_verify:
        from okg.substrate.deployment_installed_upgrade import (
            installed_upgrade_route,
            run_installed_upgrade,
        )

        installed_route = installed_upgrade_route(options.deployment)
        if installed_route is not None:
            return run_installed_upgrade(
                options, installed_route, command_runner=command_runner,
            )
    if options.from_contract is not None or options.accept_unknown_compat:
        raise SubstrateError(
            "--from-contract and --accept-unknown-compat apply only to an "
            "installed deployment",
            data={"deployment": options.deployment},
            fix_hint=(
                "drop them: this deployment is upgraded from its Git "
                "checkout, and okg up checks its code revision instead"
            ),
        )

    # #1983: refuse a standalone deployment before the context is built and
    # before any Git revision is resolved.
    _refuse_standalone_upgrade(options)

    run_started_monotonic = time.monotonic()
    run_started_at = _now()
    ctx = _build_context(options, command_runner=command_runner)
    if options.lifecycle_plan is not None:
        repo_root = Path(__file__).resolve().parents[3]
        candidate_planner_bytes = None
        if options.precheckout_target_revision is not None:
            manifest = _resolve_upgrade_deployment(ctx)
            try:
                authority = _git_target_revision(ctx, manifest)
            except RevisionAuthorityError as exc:
                raise SubstrateError(
                    "precheckout revision authority is unavailable",
                    data={"authority_error": exc.data},
                ) from exc
            target_revision = authority.code_revision
            planner_unreadable = SubstrateError(
                "precheckout candidate lifecycle planner is unreadable",
                data={
                    "target_revision": target_revision,
                    "path": PLANNER_REPO_PATH,
                },
                fix_hint=(
                    "a lifecycle plan binds the okg-ci autoupdate planner "
                    f"({PLANNER_REPO_PATH}) at the target revision; a "
                    "revision without it cannot be upgraded with "
                    "--lifecycle-plan"
                ),
            )
            try:
                planner_payload = _git_command(
                    authority.code_repo_root,
                    ["show", f"{target_revision}:{PLANNER_REPO_PATH}"],
                    binary=True,
                )
            except ValueError as exc:
                # `git show` fails when the revision does not carry the
                # planner (every revision of the public copy); that is the
                # same refusal, not a raw git error. Keep git's own message:
                # an unfetched revision or a broken repository fails here too.
                planner_unreadable.data["git_error"] = str(exc)
                raise planner_unreadable from exc
            if not isinstance(planner_payload, bytes):
                raise planner_unreadable
            candidate_planner_bytes = planner_payload
        ctx.data["lifecycle_plan_admission"] = validate_lifecycle_plan_receipt(
            options.lifecycle_plan,
            candidate_revision=_upgrade_state_revision(ctx),
            planner_path=repo_root / PLANNER_REPO_PATH,
            planner_bytes=candidate_planner_bytes,
            authorized_baseline_revision=options.verified_backup_source_revision,
            bridge_state_file=ctx.state_file,
        )
    # State is operator-visible and survives failed/resumed runs.  Sanitize old
    # state on read as well as new phase results on write so a report produced
    # by an older release cannot keep a credential alive indefinitely.
    state = redact_value(_load_or_initialize_state(ctx))
    resumed_state = bool(ctx.data.get("resumed_existing_state"))
    prior_total_duration = state.get("total_duration_seconds")
    if prior_total_duration is None and not resumed_state:
        prior_total_duration = 0.0
    elif (
        not isinstance(prior_total_duration, (int, float))
        or isinstance(prior_total_duration, bool)
        or not math.isfinite(float(prior_total_duration))
        or float(prior_total_duration) < 0
    ):
        raise SubstrateError(
            "deployment upgrade cumulative runtime evidence is missing or invalid",
            data={
                "state_file": str(ctx.state_file),
                "total_duration_seconds": prior_total_duration,
                "resumed_state": resumed_state,
            },
            fix_hint=(
                "inspect the retained receipt; use --reset-state only when "
                "discarding its elapsed-runtime evidence is intentional"
            ),
        )
    prior_total_duration = float(prior_total_duration)
    ctx.data["prior_total_duration_seconds"] = prior_total_duration
    prior_completed = set(state.get("completed_phases") or [])
    phase_statuses = state.get("phase_statuses")
    if not isinstance(phase_statuses, Mapping):
        phase_statuses = {}
    state["phase_statuses"] = {
        phase: (
            str(phase_statuses.get(phase))
            if phase_statuses.get(phase) in PHASE_ATTEMPT_STATUSES
            else "succeeded" if phase in prior_completed else "not_started"
        )
        for phase in PHASES
    }
    if not isinstance(state.get("phase_attempts"), Mapping):
        state["phase_attempts"] = {}
    persisted_contract = state.get("safety_contract")
    persisted_plan = (
        persisted_contract.get("plan")
        if isinstance(persisted_contract, Mapping)
        else None
    )
    if isinstance(persisted_plan, Mapping):
        ctx.data["safety_plan"] = dict(persisted_plan)
        if ctx.data.get("resumed_existing_state") is True:
            _revalidate_resumed_safety_contract(ctx, persisted_plan)
    elif (
        ctx.data.get("resumed_existing_state") is True
        and not options.plan_only
        and not options.independent_backup_verify
        and (
            options.stop_after is None
            or PHASES.index(options.stop_after)
            >= PHASES.index(
                "migrate" if not options.skip_migrate else "catalog_apply"
            )
        )
    ):
        raise SubstrateError(
            "resumed deployment state has no safety contract",
            data={"state_file": str(ctx.state_file)},
            fix_hint=(
                "inspect the retained lifecycle receipt; do not resume mutation "
                "without its exact target, effect, and restore-point contract"
            ),
        )
    if _runtime_quiescence_required(ctx):
        owner = _initialize_runtime_fence_owner(ctx, state)
        state["runtime_fence_owner"] = owner
        ctx.data["runtime_fence_owner"] = owner
        # Fence ownership is external-effect authority. Persist it before any
        # phase can acquire the database fence or stop a process.
        _write_state(ctx.state_file, state)
    state["run_started_at"] = run_started_at
    resolved_target = ctx.resolution.get("target")
    state["requested_target"] = {
        "deployment": options.deployment,
        "profile": options.profile,
        "declared_database": (
            resolved_target.get("expected_database")
            if isinstance(resolved_target, Mapping)
            else None
        ),
    }
    state["resolved_target"] = (
        dict(resolved_target) if isinstance(resolved_target, Mapping) else None
    )
    state["target_substitution"] = _target_substitution_notice(resolved_target)
    # Establish an on-disk receipt before any phase can create a child,
    # archive, disposable database, or service-stop side effect.  This is the
    # anchor used by the compensation path when the process is interrupted
    # between an effect and its phase result.
    state["updated_at"] = _now()
    _write_state(ctx.state_file, state)
    ctx.data["cleanup_obligations"] = dict(
        state.get("cleanup_obligations")
        if isinstance(state.get("cleanup_obligations"), Mapping)
        else {}
    )
    ctx.data["production_orchestrator"] = True
    # A process restart may leave only the durable compensation receipt. Make
    # that receipt actionable before resuming any later phase.
    if "service" in ctx.data["cleanup_obligations"] and options.apply_services:
        startup_cleanup = _failure_service_cleanup(
            ctx,
            failed_phase=str(state.get("active_phase") or "interrupted"),
        )
        if startup_cleanup is not None:
            ctx.data["startup_cleanup"] = startup_cleanup
            _refresh_cleanup_obligations(ctx, state)
            state["updated_at"] = _now()
            _write_state(ctx.state_file, state)
            if startup_cleanup.get("ok") is not True:
                raise SubstrateError(
                    "persisted service compensation could not be discharged",
                    data={"cleanup": startup_cleanup},
                    fix_hint=_next_command(ctx),
                )
    if "backup_artifacts" in ctx.data["cleanup_obligations"]:
        backup_cleanup = _recover_backup_cleanup_obligation(
            ctx, ctx.data["cleanup_obligations"]["backup_artifacts"]
        )
        ctx.data["startup_backup_cleanup"] = backup_cleanup
        if backup_cleanup.get("ok") is not True:
            raise SubstrateError(
                "persisted backup compensation could not be discharged",
                data={"cleanup": backup_cleanup},
                fix_hint=_next_command(ctx),
            )
        ctx.data["backup_obligation_discharged"] = True
        _refresh_cleanup_obligations(ctx, state)
        state["updated_at"] = _now()
        _write_state(ctx.state_file, state)
    # Reconcile a terminal effect receipt written by the phase runner before a
    # process died in the outer orchestration write.  Only an exact attempt and
    # current semantic fingerprint can be promoted; an ambiguous receipt stays
    # failed/blocked and is never replayed speculatively.
    active_phase = state.get("active_phase")
    active_attempt = state.get("phase_attempt_id")
    terminal_receipts = state.get("phase_terminal_receipts")
    terminal_receipts = (
        terminal_receipts if isinstance(terminal_receipts, Mapping) else {}
    )
    if active_phase and active_attempt:
        candidate = terminal_receipts.get(str(active_attempt))
        expected_fp = (ctx.data.get("phase_fingerprints") or {}).get(active_phase)
        if (
            isinstance(candidate, Mapping)
            and candidate.get("phase") == active_phase
            and candidate.get("phase_attempt_id") == active_attempt
            and candidate.get("semantic_fingerprint") == expected_fp
            and candidate.get("ok") is True
        ):
            reconciled = list(state.get("phase_results") or [])
            # The outer BaseException handler may have durably recorded an
            # uncertain diagnostic for this same attempt after the phase had
            # already committed its success receipt. Replace that diagnostic;
            # otherwise latest-result selection would discard the promoted
            # completion and replay the effect.
            reconciled = [
                row
                for row in reconciled
                if not (
                    isinstance(row, Mapping)
                    and row.get("phase_attempt_id") == active_attempt
                )
            ]
            reconciled.append(redact_value(dict(candidate)))
            completed_receipts = set(state.get("completed_phases") or [])
            completed_receipts.add(str(active_phase))
            phase_attempts = dict(state.get("phase_attempts") or {})
            attempt = dict(phase_attempts.get(str(active_attempt)) or {})
            attempt.update({
                "status": "succeeded",
                "terminal_at": candidate.get("completed_at") or _now(),
                "effect_may_have_happened": False,
                "terminal_receipt": redact_value(dict(candidate)),
                "reconcile": {"decision": "exact_terminal_receipt_promoted"},
            })
            phase_attempts[str(active_attempt)] = attempt
            state.update({
                "phase_results": reconciled,
                "completed_phases": [name for name in PHASES if name in completed_receipts],
                "phase_attempts": phase_attempts,
                "phase_statuses": {
                    **dict(state.get("phase_statuses") or {}),
                    str(active_phase): "succeeded",
                },
                "failed_phase": None,
                "next_phase": _next_phase(str(active_phase)),
                "active_phase": None,
                "phase_attempt_id": None,
                "updated_at": _now(),
            })
            _write_state(ctx.state_file, state)
        else:
            if (
                str(active_phase) == "backup"
                and ctx.data.get("startup_backup_cleanup", {}).get("ok") is True
            ):
                # The exact temporary archive/disposable DB were inspected and
                # discharged on restart; retrying this failed phase is now
                # safe, while unrelated completed phases remain untouched.
                state["active_phase"] = None
                state["phase_attempt_id"] = None
                phase_attempts = dict(state.get("phase_attempts") or {})
                attempt = dict(phase_attempts.get(str(active_attempt)) or {})
                attempt.update({
                    "status": "superseded",
                    "effect_may_have_happened": False,
                    "reconcile": {
                        "decision": "backup_cleanup_observed_safe_retry",
                    },
                    "superseded_reason": "interrupted backup artifacts discharged",
                })
                phase_attempts[str(active_attempt)] = attempt
                state["phase_attempts"] = phase_attempts
                state["phase_statuses"] = {
                    **dict(state.get("phase_statuses") or {}),
                    "backup": "superseded",
                }
                state["next_phase"] = "backup"
                state["updated_at"] = _now()
                _write_state(ctx.state_file, state)
            else:
                phase_attempts = dict(state.get("phase_attempts") or {})
                attempt = dict(phase_attempts.get(str(active_attempt)) or {})
                attempt.update({
                    "status": "uncertain",
                    "effect_may_have_happened": (
                        str(active_phase) in PHASES_WITH_POSSIBLE_EFFECT
                    ),
                    "reconcile": {
                        "decision": "blocked_missing_exact_terminal_receipt",
                    },
                    "recovery_command": _next_command(ctx),
                })
                phase_attempts[str(active_attempt)] = attempt
                state.update({
                    "phase_attempts": phase_attempts,
                    "phase_statuses": {
                        **dict(state.get("phase_statuses") or {}),
                        str(active_phase): "uncertain",
                    },
                    "failed_phase": str(active_phase),
                    "next_phase": str(active_phase),
                    "status": "blocked",
                    "recovery_required": True,
                    "updated_at": _now(),
                })
                _write_state(ctx.state_file, state)
                raise SubstrateError(
                    "interrupted phase effect has no matching terminal receipt",
                    data={"phase": active_phase, "phase_attempt_id": active_attempt},
                    fix_hint=_next_command(ctx),
                )
    completed = set(state.get("completed_phases") or [])
    state.setdefault("operation_id", f"up-{uuid4().hex}")
    state["active_phase"] = None
    state["phase_attempt_id"] = None
    _write_state(ctx.state_file, state)
    phase_results: list[dict[str, Any]] = list(state.get("phase_results") or [])
    for phase in tuple(completed):
        latest = _latest_phase_result(phase_results, phase)
        if latest is not None and latest.get("ok") is not True:
            completed.discard(phase)
    def cumulative_duration() -> float:
        return prior_total_duration + (time.monotonic() - run_started_monotonic)

    backup_result = _latest_phase_result(phase_results, "backup")
    if "backup" not in completed or (
        isinstance(backup_result, dict) and backup_result.get("ok") is not True
    ):
        backup_result = None
    if isinstance(backup_result, dict):
        ctx.data["backup_result"] = backup_result
    external_plan = ctx.data.get("safety_plan")
    external_record = (
        external_plan.get("external_effects")
        if isinstance(external_plan, Mapping)
        else None
    )
    unresolved_effect_attempts = [
        attempt
        for attempt in (state.get("phase_attempts") or {}).values()
        if isinstance(attempt, Mapping)
        and attempt.get("status") in {"failed", "uncertain"}
        and attempt.get("effect_may_have_happened") is True
    ]
    smoke_attempts = [
        attempt for attempt in unresolved_effect_attempts
        if attempt.get("phase") == "smoke_publish"
    ]
    if (
        isinstance(external_record, Mapping)
        and external_record.get("present") is True
        and smoke_attempts
    ):
        state.update({
            "status": "blocked",
            "failed_phase": "smoke_publish",
            "next_phase": "smoke_publish",
            "recovery_required": True,
            "recovery_actions": _recovery_actions(ctx),
            "updated_at": _now(),
        })
        _write_state(ctx.state_file, state)
        raise SubstrateError(
            "external smoke effect may have happened; automatic replay is blocked",
            data={
                "phase": "smoke_publish",
                "attempts": smoke_attempts,
                "safety_plan": external_plan,
                "recovery_actions": state["recovery_actions"],
            },
            fix_hint=(
                "reconcile the provider-side receipt and published generation; "
                "a database restore does not authorize replay"
            ),
        )
    effectful_resume_phases = {
        str(attempt.get("phase"))
        for attempt in unresolved_effect_attempts
        if attempt.get("phase") in {
            "migrate", "catalog_apply", "nomos_baseline",
            "generation_recovery", "attribute_read_model", "smoke_publish",
        }
    }
    prior_backup_attempt = _prior_backup_attempt_for_resume(phase_results)
    if isinstance(prior_backup_attempt, dict):
        ctx.data["prior_backup_attempt"] = prior_backup_attempt
    preflight_result = _latest_phase_result(phase_results, "preflight")
    if "preflight" not in completed or (
        isinstance(preflight_result, dict)
        and preflight_result.get("ok") is not True
    ):
        preflight_result = None
    if isinstance(preflight_result, dict) and isinstance(
        preflight_result.get("migration_status"), dict
    ):
        ctx.data["migrations_before"] = preflight_result["migration_status"]
    smoke_result = _latest_phase_result(phase_results, "smoke_publish")
    if "smoke_publish" not in completed or (
        isinstance(smoke_result, dict) and smoke_result.get("ok") is not True
    ):
        smoke_result = None
    if isinstance(smoke_result, dict):
        stored_publish = smoke_result.get("result")
        if (
            isinstance(stored_publish, dict)
            and isinstance(stored_publish.get("publish"), dict)
        ):
            stored_publish = stored_publish["publish"]
        if isinstance(stored_publish, dict):
            ctx.data["smoke_publish"] = stored_publish
    readiness_result = _latest_phase_result(phase_results, "readiness")
    if "readiness" not in completed or (
        isinstance(readiness_result, dict)
        and readiness_result.get("ok") is not True
    ):
        readiness_result = None
    if isinstance(readiness_result, dict):
        stored_readiness = readiness_result.get("readiness")
        if isinstance(stored_readiness, dict):
            ctx.data["readiness"] = stored_readiness
        stored_release = readiness_result.get("release")
        if isinstance(stored_release, dict):
            ctx.data["release_after"] = stored_release
    if any(
        row.get("phase") == "catalog_apply"
        and row.get("views_registry_deferred")
        for row in phase_results
    ):
        ctx.data["views_registry_deferred"] = True
    for row in reversed(phase_results):
        if row.get("phase") == "preflight" and isinstance(
            row.get("attribute_recovery_classification"), dict
        ):
            ctx.data["attribute_recovery_classification"] = row[
                "attribute_recovery_classification"
            ]
            break
    ctx.data["source_recovery_workflows"] = list(
        state.get("source_recovery_workflows") or ()
    )
    stored_runtime_witness = state.get("runtime_readiness_witness")
    ctx.data["runtime_readiness_witness"] = (
        dict(stored_runtime_witness)
        if isinstance(stored_runtime_witness, Mapping)
        else None
    )
    ctx.data["source_convergence_deadline_epoch"] = state.get(
        "source_convergence_deadline_epoch"
    )
    ctx.data["runtime_fence_snapshot_digest"] = state.get(
        "runtime_fence_snapshot_digest"
    )
    if (
        ctx.data.get("resumed_existing_state")
        and not options.independent_backup_verify
    ):
        observation_scope = completed | effectful_resume_phases
        phase_results, completed, resume_revalidation = (
            _revalidate_resume_postconditions(
                ctx,
                phase_results=phase_results,
                completed=observation_scope,
            )
        )
        observation_checks = resume_revalidation.get("checks") or {}
        unresolved_without_observation = sorted(
            phase for phase in effectful_resume_phases
            if phase not in observation_checks
        )
        if unresolved_without_observation:
            state.update({
                "status": "blocked",
                "failed_phase": unresolved_without_observation[0],
                "next_phase": unresolved_without_observation[0],
                "recovery_required": True,
                "recovery_actions": _recovery_actions(ctx),
                "resume_revalidation": resume_revalidation,
                "updated_at": _now(),
            })
            _write_state(ctx.state_file, state)
            raise SubstrateError(
                "effectful failed phase has no exact resume observation",
                data={
                    "phases": unresolved_without_observation,
                    "attempts": unresolved_effect_attempts,
                    "recovery_actions": state["recovery_actions"],
                },
                fix_hint=(
                    "inspect the retained phase receipt and satisfy its exact "
                    "postcondition recovery instruction before resuming"
                ),
            )
        if effectful_resume_phases:
            attempts = dict(state.get("phase_attempts") or {})
            for attempt_id, raw_attempt in list(attempts.items()):
                if not isinstance(raw_attempt, Mapping):
                    continue
                attempt = dict(raw_attempt)
                phase = str(attempt.get("phase") or "")
                if phase not in effectful_resume_phases or attempt.get("status") not in {
                    "failed", "uncertain"
                }:
                    continue
                postcondition_ok = observation_checks.get(phase) is True
                attempt.update({
                    "status": "succeeded" if postcondition_ok else "superseded",
                    "effect_may_have_happened": False,
                    "terminal_at": _now(),
                    "reconcile": {
                        "decision": (
                            "exact_postcondition_observed"
                            if postcondition_ok
                            else "exact_postcondition_absent_safe_convergence"
                        ),
                        "evidence": (resume_revalidation.get("evidence") or {}).get(phase),
                    },
                    "superseded_reason": (
                        None if postcondition_ok
                        else "resume observation did not prove the failed phase complete"
                    ),
                })
                attempts[str(attempt_id)] = attempt
                if postcondition_ok:
                    phase_results.append({
                        "phase": phase,
                        "ok": True,
                        "resume_reconciled": True,
                        "phase_attempt_id": attempt_id,
                        "completed_at": _now(),
                        "postcondition_evidence": (
                            resume_revalidation.get("evidence") or {}
                        ).get(phase),
                    })
            state["phase_attempts"] = attempts
            phase_results.sort(
                key=lambda row: PHASES.index(str(row.get("phase")))
                if row.get("phase") in PHASES else len(PHASES)
            )
        state["phase_results"] = phase_results
        state["completed_phases"] = [
            phase for phase in PHASES if phase in completed
        ]
        state["resume_revalidation"] = resume_revalidation
        superseded_rows = resume_revalidation.get("superseded_phase_results")
        superseded_phases = {
            str(row.get("phase"))
            for row in superseded_rows or []
            if isinstance(row, Mapping) and row.get("phase") in PHASES
        }
        if superseded_phases:
            attempts = dict(state.get("phase_attempts") or {})
            for attempt_id, raw_attempt in list(attempts.items()):
                if not isinstance(raw_attempt, Mapping):
                    continue
                attempt = dict(raw_attempt)
                phase = str(attempt.get("phase") or "")
                if phase in superseded_phases and attempt.get("status") in {
                    "succeeded", "skipped"
                }:
                    attempt.update({
                        "status": "superseded",
                        "superseded_reason": (
                            "resume postcondition invalidated this attempt"
                        ),
                        "reconcile": {
                            "decision": "superseded_by_resume_revalidation",
                            "earliest_unproven_phase": resume_revalidation.get(
                                "earliest_unproven_phase"
                            ),
                        },
                    })
                    attempts[str(attempt_id)] = attempt
            state["phase_attempts"] = attempts
            state["phase_statuses"] = {
                **dict(state.get("phase_statuses") or {}),
                **{phase: "superseded" for phase in superseded_phases},
            }
        state["updated_at"] = _now()
        _write_state(ctx.state_file, state)
    last_stop = max(
        (
            index
            for index, row in enumerate(phase_results)
            if row.get("phase") == "stop_services" and row.get("ok") is True
        ),
        default=-1,
    )
    last_restart = max(
        (
            index
            for index, row in enumerate(phase_results)
            if (
                row.get("phase") == "restart_services"
                and row.get("ok") is True
            ) or (
                isinstance(row.get("failure_cleanup"), dict)
                and row["failure_cleanup"].get("ok") is True
            )
        ),
        default=-1,
    )
    if last_stop > last_restart:
        ctx.data["services_stopped"] = True

    substitution = _target_substitution_notice(ctx.resolution.get("target"))
    payload: dict[str, Any] = {
        "ok": False,
        "deployment": options.deployment,
        "profile": options.profile,
        "requested_target": state.get("requested_target"),
        "resolved_target": ctx.resolution.get("target"),
        # Ahead of `phases` deliberately: an operator scanning the head of the
        # output must see that a different database was operated on before
        # they read any per-phase success.
        **({"target_substitution": substitution} if substitution else {}),
        **(
            {"safety_contract": state.get("safety_contract")}
            if isinstance(state.get("safety_contract"), Mapping)
            else {}
        ),
        "state_file": str(ctx.state_file),
        "lifecycle_contract": CANONICAL_CONTRACT.receipt(),
        "lifecycle_plan_admission": ctx.data.get("lifecycle_plan_admission"),
        "phases": phase_results,
        "integrated_gate": integrated_deployment_gate(phase_results),
        # Stable, executable recovery surface.  ``next_command`` remains for
        # compatibility; this named field makes the failed-phase resume
        # contract machine-readable for callers that do not parse prose.
        "recovery_command": _next_command(ctx),
        "recovery_actions": _recovery_actions(ctx),
    }

    # ``stop_after`` is an operation boundary, including on resume.  A crash
    # after the requested phase row became durable but before the caller could
    # finalize its wrapper state must never let a later invocation fall through
    # into unrelated mutation phases.
    if options.stop_after in completed:
        total_duration = cumulative_duration()
        state.update({
            "updated_at": _now(),
            "stopped_after": options.stop_after,
            "next_phase": None,
            "total_duration_seconds": total_duration,
        })
        _write_state(ctx.state_file, state)
        payload.update({
            "phases": phase_results,
            "stopped_after": options.stop_after,
            "resumed_stop_boundary": True,
            "next_command": _next_command(ctx),
            "reason": "stopped after requested phase",
            "total_duration_seconds": total_duration,
        })
        return redact_value(payload)

    for phase in PHASES:
        if phase in completed:
            continue
        # A phase-start receipt is durable before its effect begins. If the
        # process dies after this write, resume has an explicit uncertain
        # attempt to inspect rather than treating a missing result as a clean
        # invitation to replay blindly.
        phase_attempt_id = f"{state['operation_id']}:{phase}:{uuid4().hex}"
        phase_attempts = dict(state.get("phase_attempts") or {})
        for prior_id, raw_prior in list(phase_attempts.items()):
            if not isinstance(raw_prior, Mapping):
                continue
            prior = dict(raw_prior)
            if (
                prior.get("phase") == phase
                and prior.get("status") == "superseded"
                and not prior.get("superseded_by")
            ):
                prior["superseded_by"] = phase_attempt_id
                phase_attempts[str(prior_id)] = prior
        phase_attempts[phase_attempt_id] = {
            "attempt_id": phase_attempt_id,
            "phase": phase,
            "status": "active",
            "started_at": _now(),
            "terminal_at": None,
            "guarantees": _phase_attempt_guarantees(ctx, phase=phase),
            "effect_may_have_happened": False,
            "terminal_receipt": None,
            "reconcile": None,
            "superseded_by": None,
            "superseded_reason": None,
            "recovery_command": _next_command(ctx),
        }
        state.update({
            "active_phase": phase,
            "phase_attempt_id": phase_attempt_id,
            "phase_attempts": phase_attempts,
            "phase_statuses": {
                **dict(state.get("phase_statuses") or {}),
                phase: "active",
            },
            "next_phase": phase,
            "updated_at": _now(),
        })
        _write_state(ctx.state_file, state)
        ctx.data["phase_attempt_id"] = phase_attempt_id
        try:
            terminal_receipts = state.get("phase_terminal_receipts")
            receipt = (
                terminal_receipts.get(phase_attempt_id)
                if isinstance(terminal_receipts, Mapping)
                else None
            )
            if isinstance(receipt, Mapping) and receipt.get("ok") is True:
                # The effect handler durably wrote this terminal receipt before
                # returning.  A crash between that write and the outer state
                # update must resume from the receipt, never replay the effect.
                result = redact_value(dict(receipt))
            else:
                result = redact_value(_run_phase(ctx, phase))
        except BaseException as exc:
            timing = getattr(exc, "_okg_phase_timing", {})
            interrupt_evidence = getattr(exc, "_okg_phase_evidence", {})
            result = {
                "phase": phase,
                "phase_attempt_id": phase_attempt_id,
                "semantic_fingerprint": (
                    (ctx.data.get("phase_fingerprints") or {}).get(phase)
                    if isinstance(ctx.data.get("phase_fingerprints"), Mapping)
                    else None
                ),
                "ok": False,
                "reason": f"{phase} interrupted",
                "error": {"type": type(exc).__name__, "message": str(exc)},
                "started_at": timing.get("started_at"),
                "completed_at": timing.get("completed_at", _now()),
                "duration_seconds": timing.get("duration_seconds", 0.0),
                **(
                    redact_value(interrupt_evidence)
                    if isinstance(interrupt_evidence, dict)
                    else {}
                ),
            }
            backup_guarantee = ctx.data.get("backup_result")
            safety_contract = _safety_contract_snapshot(
                ctx,
                restore_point_verified=(
                    isinstance(backup_guarantee, Mapping)
                    and backup_guarantee.get("ok") is True
                    and (
                        backup_guarantee.get("restore_verified") is True
                        or backup_guarantee.get("mutation_authorized") is True
                    )
                ),
                phase=phase,
            )
            if safety_contract is not None:
                result["safety_contract"] = safety_contract
                state["safety_contract"] = safety_contract
                payload["safety_contract"] = safety_contract
                state["recovery_actions"] = _recovery_actions(ctx)
                payload["recovery_actions"] = state["recovery_actions"]
            phase_attempts = dict(state.get("phase_attempts") or {})
            attempt = dict(phase_attempts.get(phase_attempt_id) or {})
            attempt.update({
                "status": "uncertain",
                "effect_may_have_happened": phase in PHASES_WITH_POSSIBLE_EFFECT,
                "guarantees": _phase_attempt_guarantees(ctx, phase=phase),
                "reconcile": {
                    "decision": "blocked_pending_exact_observation",
                    "reason": result["reason"],
                },
                "recovery_command": _next_command(ctx),
            })
            phase_attempts[phase_attempt_id] = attempt
            state["phase_attempts"] = phase_attempts
            state["phase_statuses"] = {
                **dict(state.get("phase_statuses") or {}),
                phase: "uncertain",
            }
            phase_results.append(result)
            payload["integrated_gate"] = integrated_deployment_gate(phase_results)
            _merge_phase_durable_state(ctx, state)
            if phase == "backup" and result.get("ok") is True:
                ctx.data["backup_obligation_discharged"] = True
            if phase == "restart_services" and result.get("ok") is True:
                ctx.data["service_obligation_discharged"] = True
            _refresh_cleanup_obligations(ctx, state)
            state.update(
                {
                    "updated_at": _now(),
                    "phase_results": phase_results,
                    "completed_phases": [name for name in PHASES if name in completed],
                    "failed_phase": phase,
                    "next_phase": phase,
                    "total_duration_seconds": cumulative_duration(),
                }
            )
            persistence_errors: list[dict[str, str]] = []
            try:
                _write_state(ctx.state_file, state)
            except BaseException as persist_exc:
                persistence_errors.append(
                    {
                        "type": type(persist_exc).__name__,
                        "message": str(persist_exc),
                    }
                )
            finally:
                # Cleanup is a safety boundary, not a reward for successfully
                # persisting diagnostics.  A full disk or interrupted state
                # write must not leave a stopped worker behind.
                cleanup = _failure_service_cleanup(ctx, failed_phase=phase)
            if cleanup is not None:
                result["failure_cleanup"] = redact_value(cleanup)
                if cleanup.get("ok") is True:
                    ctx.data["service_obligation_discharged"] = True
            if persistence_errors:
                result["state_persistence_errors"] = persistence_errors
            _refresh_cleanup_obligations(ctx, state)
            state.update(
                {
                    "updated_at": _now(),
                    "phase_results": phase_results,
                    "total_duration_seconds": cumulative_duration(),
                }
            )
            try:
                _write_state(ctx.state_file, state)
            except BaseException as persist_exc:
                persistence_errors.append(
                    {
                        "type": type(persist_exc).__name__,
                        "message": str(persist_exc),
                    }
                )
            raise
        _enforce_total_runtime_budget(
            ctx,
            result,
            total_duration_seconds=cumulative_duration(),
        )
        cleanup = (
            _failure_service_cleanup(ctx, failed_phase=phase)
            if not result.get("ok")
            else None
        )
        if cleanup is not None:
            result["failure_cleanup"] = redact_value(cleanup)
            if cleanup.get("ok") is True:
                ctx.data["service_obligation_discharged"] = True
        backup_guarantee = ctx.data.get("backup_result")
        safety_contract = (
            result.get("safety_contract")
            if isinstance(result.get("safety_contract"), Mapping)
            else _safety_contract_snapshot(
                ctx,
                restore_point_verified=(
                    isinstance(backup_guarantee, Mapping)
                    and backup_guarantee.get("ok") is True
                    and (
                        backup_guarantee.get("restore_verified") is True
                        or backup_guarantee.get("mutation_authorized") is True
                    )
                ),
                phase=phase,
            )
        )
        if safety_contract is not None:
            result["safety_contract"] = dict(safety_contract)
            state["safety_contract"] = dict(safety_contract)
            payload["safety_contract"] = dict(safety_contract)
            state["recovery_actions"] = _recovery_actions(ctx)
            payload["recovery_actions"] = state["recovery_actions"]
            # Preflight necessarily runs before the effect inventory is built.
            # Once backup has produced the canonical contract, backfill it into
            # every earlier row and attempt so persisted/per-phase output never
            # loses the external-effect warning or restore guarantee.
            for prior_result in phase_results:
                prior_result["safety_contract"] = dict(safety_contract)
            for raw_attempt in (state.get("phase_attempts") or {}).values():
                if isinstance(raw_attempt, dict):
                    raw_attempt["guarantees"] = _phase_attempt_guarantees(
                        ctx, phase=str(raw_attempt.get("phase"))
                    )
        attempt_status = (
            "skipped"
            if result.get("skipped")
            else "succeeded" if result.get("ok") is True else "failed"
        )
        phase_attempts = dict(state.get("phase_attempts") or {})
        attempt = dict(phase_attempts.get(phase_attempt_id) or {})
        attempt.update({
            "status": attempt_status,
            "terminal_at": result.get("completed_at") or _now(),
            "effect_may_have_happened": (
                attempt_status == "failed"
                and phase in PHASES_WITH_POSSIBLE_EFFECT
            ),
            "guarantees": _phase_attempt_guarantees(ctx, phase=phase),
            "terminal_receipt": {
                "phase": phase,
                "attempt_id": phase_attempt_id,
                "semantic_fingerprint": result.get("semantic_fingerprint"),
                "ok": result.get("ok") is True,
                "generation": result.get("generation_id"),
            },
            "reconcile": {
                "decision": (
                    "terminal_failure_requires_postcondition_observation"
                    if attempt_status == "failed"
                    and phase in PHASES_WITH_POSSIBLE_EFFECT
                    else "terminal_receipt_observed"
                )
            },
            "recovery_command": _next_command(ctx),
        })
        phase_attempts[phase_attempt_id] = attempt
        state["phase_attempts"] = phase_attempts
        state["phase_statuses"] = {
            **dict(state.get("phase_statuses") or {}),
            phase: attempt_status,
        }
        phase_results.append(result)
        phase_results.sort(
            key=lambda row: (
                PHASES.index(str(row.get("phase")))
                if row.get("phase") in PHASES
                else len(PHASES)
            )
        )
        payload["integrated_gate"] = integrated_deployment_gate(phase_results)
        # A phase may durably checkpoint finer-grained recovery identities while
        # it is running. Reload those writes before recording the phase result so
        # the outer state update cannot erase the only crash-resume authority.
        _merge_phase_durable_state(ctx, state)
        if phase == "backup" and result.get("ok") is True:
            ctx.data["backup_obligation_discharged"] = True
        if phase == "restart_services" and result.get("ok") is True:
            ctx.data["service_obligation_discharged"] = True
        _refresh_cleanup_obligations(ctx, state)
        next_completed = set(completed)
        if result.get("ok"):
            next_completed.add(phase)
        state.update(
            {
                "updated_at": _now(),
                "phase_results": phase_results,
                "completed_phases": [name for name in PHASES if name in next_completed],
                "failed_phase": None if result.get("ok") else phase,
                "next_phase": _next_phase(phase) if result.get("ok") else phase,
                "total_duration_seconds": cumulative_duration(),
                "active_phase": None,
                "phase_attempt_id": None,
            }
        )
        if (
            phase == "backup"
            and result.get("ok") is True
            and isinstance(state.get("precheckout_authority"), dict)
        ):
            state["precheckout_authority"] = _complete_precheckout_authority(
                state["precheckout_authority"],
                result,
            )
        try:
            _write_state(ctx.state_file, state)
        except BaseException as exc:
            result["state_persistence_errors"] = [
                {
                    "type": type(exc).__name__,
                    "message": str(exc),
                }
            ]
            cleanup = _failure_service_cleanup(ctx, failed_phase=phase)
            if cleanup is not None:
                result["failure_cleanup"] = redact_value(cleanup)
            # A best-effort second write captures cleanup evidence, but cannot
            # replace the persistence exception that triggered it.
            try:
                _write_state(ctx.state_file, state)
            except BaseException:
                pass
            raise
        if not result.get("ok"):
            total_duration = cumulative_duration()
            state["total_duration_seconds"] = total_duration
            _write_state(ctx.state_file, state)
            payload.update(
                {
                    "phases": phase_results,
                    "failed_phase": phase,
                    "next_command": _next_command(ctx),
                    "reason": result.get("reason") or "phase failed",
                    "total_duration_seconds": total_duration,
                }
            )
            # #1791: a refused backup admission on a deployment whose publish
            # was refused must not leave `next_command` (resume `okg up`) as
            # the only guidance at the top of the output. Hoisted from the
            # phase row only; never written to the durable state file.
            if phase == "backup" and isinstance(
                result.get("publish_recovery"), Mapping
            ):
                payload["publish_recovery"] = result["publish_recovery"]
                if result.get("fix_hint"):
                    payload["fix_hint"] = result["fix_hint"]
            return redact_value(payload)
        completed = next_completed
        if options.stop_after == phase:
            total_duration = cumulative_duration()
            state.update(
                {
                    "stopped_after": phase,
                    "next_phase": None,
                    "total_duration_seconds": total_duration,
                }
            )
            _write_state(ctx.state_file, state)
            payload.update(
                {
                    "phases": phase_results,
                    "stopped_after": phase,
                    "next_command": _next_command(ctx),
                    "reason": "stopped after requested phase",
                    "total_duration_seconds": total_duration,
                }
            )
            return redact_value(payload)

    readiness_generation = _readiness_generation(ctx.data.get("readiness"))
    smoke_generation = _bound_smoke_generation(
        ctx,
        _smoke_generation(ctx.data.get("smoke_publish")),
        readiness_generation,
    )
    total_duration = cumulative_duration()
    if not options.plan_only and (
        smoke_generation is None
        or readiness_generation is None
        or readiness_generation != smoke_generation
    ):
        # A crash can land after the readiness phase row is durable but before
        # finalization. Re-check the exact-generation join on resume instead of
        # converting missing hydrated context into a successful completion.
        resumable_phases = [
            name for name in state.get("completed_phases") or []
            if name != "readiness"
        ]
        state.update({
            "updated_at": _now(),
            "completed": False,
            "completed_phases": resumable_phases,
            "failed_phase": "readiness",
            "next_phase": "readiness",
            "smoke_generation_id": smoke_generation,
            "readiness_generation_id": readiness_generation,
            "total_duration_seconds": total_duration,
        })
        _write_state(ctx.state_file, state)
        payload.update({
            "phases": phase_results,
            "integrated_gate": integrated_deployment_gate(phase_results),
            "failed_phase": "readiness",
            "reason": "deployment readiness does not match smoke generation",
            "smoke_generation": smoke_generation,
            "readiness_generation": readiness_generation,
            "next_command": _next_command(ctx),
            "total_duration_seconds": total_duration,
        })
        return redact_value(payload)
    state.update({
        "updated_at": _now(),
        "completed": True,
        "status": "succeeded",
        "failed_phase": None,
        "next_phase": None,
        "phase_results": phase_results,
        "smoke_generation_id": smoke_generation,
        "readiness_generation_id": readiness_generation,
        "total_duration_seconds": total_duration,
    })
    _refresh_cleanup_obligations(ctx, state)
    integrated_gate = integrated_deployment_gate(phase_results)
    if not options.plan_only and integrated_gate.get("ok") is not True:
        state.update({
            "updated_at": _now(),
            "completed": False,
            "failed_phase": "readiness",
            "next_phase": "readiness",
            "integrated_gate": integrated_gate,
            "total_duration_seconds": total_duration,
        })
        _write_state(ctx.state_file, state)
        payload.update({
            "ok": False,
            "phases": phase_results,
            "integrated_gate": integrated_gate,
            "failed_phase": "readiness",
            "reason": "integrated backup and source-readiness gate failed",
            "next_command": _next_command(ctx),
            "total_duration_seconds": total_duration,
        })
        return redact_value(payload)
    _write_state(ctx.state_file, state)
    payload.update({
        "ok": True,
        "phases": phase_results,
        "integrated_gate": integrated_gate,
        "release": ctx.data.get("release_after"),
        "readiness": ctx.data.get("readiness"),
        "smoke_generation": smoke_generation,
        "readiness_generation": readiness_generation,
        "total_duration_seconds": total_duration,
    })
    if isinstance(ctx.data.get("capacity_warning"), dict):
        payload["capacity_warning"] = ctx.data["capacity_warning"]

    # Honest status: a bare `okg up` must refresh the worker (code, deps,
    # service restart). If a mutate-requiring phase was SKIPPED (operator opted
    # out, but did not request the plan-only preview), the run is only PARTIAL —
    # it must not report full success while leaving the worker on stale code.
    if not ctx.options.plan_only:
        skipped_mutate = _skipped_mutate_phases(phase_results)
        if skipped_mutate:
            payload.update({
                "ok": False,
                "status": "partial",
                "skipped_mutate_phases": skipped_mutate,
                "reason": (
                    "upgrade left mutate phases unapplied: "
                    + ", ".join(skipped_mutate)
                ),
                "next_command": _finishing_command(ctx, skipped_mutate),
            })
            return redact_value(payload)
    if ctx.options.plan_only:
        payload["status"] = "plan"
    else:
        payload["status"] = "applied"
        # Tail of the finalize: backup retention runs ONLY here, after every
        # phase (including readiness) succeeded and the run was not downgraded
        # to partial. A failed, stopped, partial, or plan-only run never
        # deletes an archive — a failed run's dump may be the only good backup.
        retention = redact_value(_finalize_backup_retention(ctx, phase_results))
        payload["backup_retention"] = retention
        state.update({
            "updated_at": _now(),
            "backup_retention": retention,
        })
        if retention.get("ok") is not True:
            state.update({
                "completed": False,
                "failed_phase": "backup",
                "next_phase": "backup",
            })
            payload.update({
                "ok": False,
                "status": "blocked",
                "failed_phase": "backup",
                "reason": retention.get("reason") or "backup retention cleanup failed",
                "next_command": _next_command(ctx),
            })
        _write_state(ctx.state_file, state)
    return redact_value(payload)


def run_deployment_backup_verify(
    options: UpgradeOptions,
    *,
    command_runner: CommandRunner | None = None,
) -> dict[str, Any]:
    """Create and restore a recovery archive outside deployment-up.

    The operation deliberately uses the normal preflight and backup machinery
    but has its own state file and success contract.  A routine deployment can
    consume only the typed receipt emitted by this operation; an ordinary
    forced backup cannot accidentally mint one.

    A standalone deployment is refused by ``run_deployment_upgrade`` before
    any backup starts (#1983).
    """
    state_file = options.state_file or (
        Path(".okg")
        / "up"
        / f"{options.deployment}-{options.profile}-backup-verify.json"
    )
    verify_options = UpgradeOptions(
        **{
            **options.__dict__,
            "state_file": state_file,
            "apply_services": False,
            "skip_backup": False,
            "plan_only": False,
            "resume": options.resume,
            "reset_state": options.reset_state,
            "stop_after": "backup",
            "independent_backup_verify": True,
        }
    )
    payload = run_deployment_upgrade(
        verify_options,
        command_runner=command_runner,
    )
    ctx = _build_context(verify_options, command_runner=command_runner)
    backup = next(
        (
            row
            for row in reversed(payload.get("phases") or [])
            if row.get("phase") == "backup"
        ),
        None,
    )
    verified = (
        isinstance(backup, dict)
        and backup.get("ok") is True
        and backup.get("verification_mode") == "current_full_restore"
        and isinstance(backup.get("disaster_recovery_receipt"), dict)
    )
    completion_revalidation: dict[str, Any] | None = None
    if verified and payload.get("resumed_stop_boundary") is True:
        completion_revalidation = _revalidate_independent_backup_completion(
            ctx,
            payload=payload,
            backup=backup,
        )
        verified = completion_revalidation.get("ok") is True
    if not verified:
        reset_state = (
            completion_revalidation is not None
            or _backup_verify_failure_requires_reset(payload)
        )
        return {
            **payload,
            "ok": False,
            "operation": "deployment_backup_verify",
            "reason": (
                completion_revalidation.get("reason")
                if completion_revalidation is not None
                else payload.get("reason") or "backup verification failed"
            ),
            **(
                {"completion_revalidation": completion_revalidation}
                if completion_revalidation is not None
                else {}
            ),
            "next_command": _backup_verify_next_command(
                ctx, reset_state=reset_state
            ),
        }
    retention = _finalize_backup_retention(ctx, payload.get("phases") or [])
    try:
        terminal_state = json.loads(ctx.state_file.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return {
            **payload,
            "ok": False,
            "operation": "deployment_backup_verify",
            "reason": "verified backup state could not be finalized",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "next_command": _backup_verify_next_command(ctx),
        }
    producer_target_key = dict(terminal_state.get("target_key") or {})
    producer_target_key["code_revision"] = (
        producer_target_key.get("code_revision")
        or _upgrade_state_revision(ctx)
    )
    producer_target_key["manifest_digest"] = (
        producer_target_key.get("manifest_digest")
        or _upgrade_manifest_digest(ctx)
    )
    ctx.data["target_key"] = producer_target_key
    ctx.data["lifecycle_plan_admission"] = terminal_state.get(
        "lifecycle_plan_admission"
    )
    effects, effect_errors, effect_report = _collect_effect_inventory(ctx)
    effect_checks = effects.to_dict() if effects is not None else {}
    failed_effects = [
        name
        for name in _REFUSAL_EFFECT_AUTHORITY_CHECKS
        if effect_checks.get(name) is not True
    ]
    if effects is None or effect_errors or failed_effects:
        return {
            **payload,
            "ok": False,
            "operation": "deployment_backup_verify",
            "database_restore_point_verified": True,
            "mutation_authorized": False,
            "reason": "verified backup target effect authority is not closed",
            "effect_inventory": effect_report,
            "failed_checks": failed_effects,
            "effect_errors": list(effect_errors),
            "next_command": _backup_verify_next_command(ctx),
        }
    restore_evidence = _verified_backup_restore_point_evidence(backup)
    _deployment_safety_plan(
        ctx,
        effect_report=effect_report,
        restore_point_source="independent_backup_verify",
        restore_point_verified=True,
        restore_point_evidence=restore_evidence,
    )
    canonical_backup = {
        **backup,
        "effect_inventory": effect_report,
    }
    ctx.data["backup_result"] = canonical_backup
    safety_contract = _safety_contract_snapshot(
        ctx,
        restore_point_verified=True,
        phase="backup",
    )
    canonical_backup["safety_contract"] = safety_contract

    def with_canonical_backup(rows: object) -> list[dict[str, Any]]:
        return [
            canonical_backup
            if isinstance(row, Mapping) and row.get("phase") == "backup"
            else dict(row)
            for row in rows or []
            if isinstance(row, Mapping)
        ]

    payload["phases"] = with_canonical_backup(payload.get("phases"))
    terminal_state["phase_results"] = with_canonical_backup(
        terminal_state.get("phase_results")
    )
    terminal_state.update({
        "updated_at": _now(),
        "completed": True,
        "failed_phase": None,
        "next_phase": None,
        "operation": "deployment_backup_verify",
        "backup_retention": retention,
        "safety_contract": safety_contract,
        **(
            {"completion_revalidation": completion_revalidation}
            if completion_revalidation is not None
            else {}
        ),
    })
    _write_state(ctx.state_file, terminal_state)
    up_options = UpgradeOptions(**{
        **options.__dict__,
        "state_file": None,
        "verified_backup_state": ctx.state_file,
        "skip_backup": True,
        "apply_services": True,
        "stop_after": None,
        "reset_state": False,
        "independent_backup_verify": False,
    })
    up_ctx = UpgradeContext(
        options=up_options,
        dsn=ctx.dsn,
        resolution=ctx.resolution,
        env=ctx.env,
        state_file=ctx.state_file,
        backup_dir=ctx.backup_dir,
        command_runner=ctx.command_runner,
    )
    return redact_value({
        **payload,
        "ok": True,
        "status": "verified",
        "operation": "deployment_backup_verify",
        "stopped_after": None,
        "reason": None,
        "next_command": _display_recovery_command(
            up_ctx,
            _recovery_command_parts(
                up_ctx,
                apply_services=True,
                sync_code=options.sync_code,
                sync_deps=options.sync_deps,
                plan_only=True,
            ),
        ),
        "backup_retention": retention,
        "safety_contract": safety_contract,
        "disaster_recovery_receipt": backup[
            "disaster_recovery_receipt"
        ],
    })


def _revalidate_independent_backup_completion(
    ctx: UpgradeContext,
    *,
    payload: Mapping[str, Any],
    backup: Mapping[str, Any],
) -> dict[str, Any]:
    """Revalidate a crash-window success before terminalizing its state."""
    preflight = next(
        (
            row
            for row in reversed(payload.get("phases") or [])
            if isinstance(row, Mapping)
            and row.get("phase") == "preflight"
            and row.get("ok") is True
        ),
        None,
    )
    migration_status = (
        preflight.get("migration_status")
        if isinstance(preflight, Mapping)
        else None
    )
    if not isinstance(migration_status, Mapping):
        return {"ok": False, "reason": "backup preflight state is unavailable"}
    ctx.data["migrations_before"] = dict(migration_status)
    toolchain = backup.get("toolchain")
    if not isinstance(toolchain, dict) or toolchain.get("ok") is not True:
        return {"ok": False, "reason": "backup toolchain state is unavailable"}
    try:
        policy = _backup_policy_config(ctx)
        snapshot = _backup_compatibility_snapshot(ctx, toolchain)
        checks, errors, report, _size = _disaster_recovery_receipt_evidence(
            ctx, snapshot, policy
        )
    except Exception as exc:  # noqa: BLE001 - terminalization is fail-closed.
        return {
            "ok": False,
            "reason": "verified backup completion could not be revalidated",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    return {
        "ok": bool(checks is not None and checks.compatible and not errors),
        "reason": (
            None
            if checks is not None and checks.compatible and not errors
            else "verified backup archive or receipt changed before finalization"
        ),
        "errors": list(errors),
        "receipt": report,
    }


def _build_context(
    options: UpgradeOptions,
    *,
    command_runner: CommandRunner | None,
) -> UpgradeContext:
    resolution = _resolve_target(options)
    dsn = resolution.get("dsn")
    if not dsn:
        raise SubstrateError(
            "deployment upgrade DSN unresolved",
            data={"deployment": options.deployment, "resolution": resolution},
            fix_hint=(
                f"run `okg deployment env doctor {options.deployment} --json`"
            ),
        )
    env = os.environ.copy()
    env["OKG_DSN"] = str(dsn)
    # The up subprocesses (okg migrate / db / catalog / ingest) run with
    # --deployment, so they re-resolve their DSN via the MANIFEST's postgres.dsn
    # env-refs (e.g. ${CERN_TWIKI_OKG_DSN}) rather than via OKG_DSN. Under
    # --target dev that fell through to the DECLARED (canonical) DB, so migrate
    # applied the schema to the wrong database while only the extension bootstrap
    # (which uses ctx.dsn directly) hit the isolated DB. Override the manifest's
    # own DSN env-refs with the RESOLVED target so every subprocess targets the
    # same DB as ctx.dsn. For --target prod this is a no-op (the ref already
    # resolves to the declared DB); the env-var resolution path carries no
    # explicit-DSN identity lock, so an isolated dev DB name is admitted.
    from okg.substrate.deployment_authority import _env_refs, _read_manifest

    _manifest_raw, _ = _read_manifest(options.deployment)
    _manifest_pg = (
        _manifest_raw.get("postgres") if isinstance(_manifest_raw, dict) else None
    )
    _manifest_dsn = (
        _manifest_pg.get("dsn") if isinstance(_manifest_pg, dict) else None
    )
    if isinstance(_manifest_dsn, str) and _manifest_dsn.strip():
        for _ref in _env_refs(_manifest_dsn):
            env[_ref] = str(dsn)
    state_file = (
        options.state_file
        if options.state_file is not None
        else Path(".okg") / "up" / f"{options.deployment}-{options.profile}.json"
    )
    backup_dir = (
        options.backup_dir
        if options.backup_dir is not None
        else Path(".okg") / "backups" / options.deployment
    )
    runtime_raw = (
        _manifest_raw.get("runtime")
        if isinstance(_manifest_raw, dict)
        else None
    )
    return UpgradeContext(
        options=options,
        dsn=str(dsn),
        resolution=resolution,
        env=env,
        state_file=state_file,
        backup_dir=backup_dir,
        command_runner=command_runner or _default_command_runner,
        data={
            "runtime_enabled": bool(
                isinstance(runtime_raw, dict)
                and runtime_raw.get("enabled") is True
            ),
            "runtime_dbos_schema": _declared_dbos_schema(runtime_raw),
        },
    )


_SUBSTITUTED_TARGET_IDENTITIES = {
    "isolated": (
        "operated on a worktree-isolated database, NOT the deployment's "
        "declared database"
    ),
    "mismatch": (
        "operated on a database that is not the deployment's declared "
        "database"
    ),
}
_DEV_ISOLATED_SUBSTITUTION = (
    "--target dev operated on a worktree-isolated database; the deployment "
    "declares no comparable database name, so the operated database cannot be "
    "checked against a declared one"
)
_UNREADABLE_TARGET_SUBSTITUTION = (
    "the resolved target could not be read, so the operated database cannot "
    "be shown to be the deployment's declared one"
)
_UNRECOGNIZED_IDENTITY_SUBSTITUTION = (
    "the resolved target reported an identity this build does not recognize, "
    "so the operated database cannot be shown to be the declared one"
)
# The ONLY identity that proves no substitution: the operated database is the
# declared one.  Everything else is either a known substitution or an unproven
# claim, and unproven must not read as proven.
_DECLARED_TARGET_IDENTITY = "ok"
# Outside dev isolation these two mean "there is no declared name to compare
# against", not "we swapped your database".  ``resolved_target`` already
# reports them, so they add no notice - but under dev isolation
# ``authority_mode`` is direct proof of substitution and overrides that.
_UNDETERMINED_TARGET_IDENTITIES = frozenset({"undeclared", "unresolved"})


def _target_substitution_notice(target: Any) -> dict[str, Any] | None:
    """Name a substituted database at the top of the result, not inside it.

    ``--target dev`` resolves the worktree-isolated database by design, and a
    run against it reports the same shape of success as a run against the real
    deployment.  An operator who reached for ``--target dev`` because the
    declared target refused then reads that success as the deployment being
    repaired.  Whenever the operated database is not the declared one, say so
    as a top-level key rather than leaving it to be inferred from
    ``resolved_target.identity``.

    Identity alone is not enough to decide this.  A deployment that declares no
    ``postgres.database`` resolves to identity ``undeclared`` - and one whose
    manifest will not parse resolves to ``unresolved`` - even under dev
    isolation, because ``_declared_database`` cannot tell "no declared name"
    from "unreadable manifest".  ``authority_mode`` still says
    ``dev_isolated``, which is direct proof of substitution, so trust that
    independently.

    Silence is reserved for the cases that PROVE no substitution, never
    inferred from an identity we failed to recognize: an unreadable target or
    an unknown classification says so loudly instead of reading as `ok`.
    """
    if not isinstance(target, Mapping):
        return {
            "substituted": True,
            "identity": None,
            "expected_database": None,
            "operated_database": None,
            "host": None,
            "port": None,
            "authority_mode": None,
            "explanation": _UNREADABLE_TARGET_SUBSTITUTION,
        }
    identity = str(target.get("identity"))
    if identity == _DECLARED_TARGET_IDENTITY:
        return None
    explanation = _SUBSTITUTED_TARGET_IDENTITIES.get(identity)
    if explanation is None:
        if target.get("authority_mode") == "dev_isolated":
            explanation = _DEV_ISOLATED_SUBSTITUTION
        elif identity in _UNDETERMINED_TARGET_IDENTITIES:
            return None
        else:
            explanation = _UNRECOGNIZED_IDENTITY_SUBSTITUTION
    return {
        "substituted": True,
        "identity": target.get("identity"),
        "expected_database": target.get("expected_database"),
        "operated_database": target.get("database"),
        "host": target.get("host"),
        "port": target.get("port"),
        "authority_mode": target.get("authority_mode"),
        "explanation": explanation,
    }


def _resolve_target(options: UpgradeOptions) -> dict[str, Any]:
    if options.profile == "dev":
        from okg.substrate.catalog.ownership import suggested_worktree_dsn
        from okg.substrate.deployment_authority import (
            _declared_database,
            resolved_target,
        )

        dsn = options.dsn or suggested_worktree_dsn(
            deployment_name=options.deployment,
        )
        expected_database = _declared_database(options.deployment)
        return {
            "dsn": dsn,
            "authority": {"mode": "dev_isolated"},
            "target": resolved_target(
                dsn,
                deployment=options.deployment,
                authority_mode="dev_isolated",
                expected_database=expected_database,
            ),
        }
    from okg.substrate.deployment_authority import (
        _read_manifest,
        deployment_self_hosted_authoritative,
        resolve_deployment_dsn_authority,
    )

    resolution = resolve_deployment_dsn_authority(
        options.deployment,
        explicit_dsn=options.dsn,
        local_profile_authoritative=deployment_self_hosted_authoritative(
            options.deployment
        ),
    )
    raw, _manifest_readable = _read_manifest(options.deployment)
    lifecycle = raw.get("deployment_up") if isinstance(raw, dict) else None
    require_exact = (
        lifecycle.get("require_exact_declared_database", False)
        if isinstance(lifecycle, dict)
        else False
    )
    if not isinstance(require_exact, bool):
        raise SubstrateError(
            "deployment_up.require_exact_declared_database must be a boolean",
            data={
                "deployment": options.deployment,
                "value": require_exact,
            },
        )
    target = resolution.get("target")
    if (
        require_exact
        and isinstance(target, dict)
        and target.get("identity") != "ok"
    ):
        return {
            **resolution,
            "dsn": None,
            "reason": (
                f"deployment {options.deployment} requires the exact declared "
                f"database {target.get('expected_database')!r}; target identity "
                f"was {target.get('identity')!r} for {target.get('database')!r}"
            ),
            "next_action": "fix_deployment_database_identity",
        }
    return resolution


def _fleet_upgrade_options(
    deployment: str,
    options: FleetUpgradeOptions,
) -> UpgradeOptions:
    backup_dir = (
        options.backup_dir / deployment
        if options.backup_dir is not None
        else None
    )
    state_file = (
        options.state_dir / f"{deployment}-{options.profile}.json"
        if options.state_dir is not None
        else None
    )
    return UpgradeOptions(
        deployment=deployment,
        profile=options.profile,
        include=options.include,
        state_file=state_file,
        backup_dir=backup_dir,
        backup_keep=options.backup_keep,
        readiness_profile=options.readiness_profile,
        apply_services=options.apply_services,
        skip_backup=options.skip_backup,
        skip_migrate=options.skip_migrate,
        skip_destructive_migrations=options.skip_destructive_migrations,
        confirm_destructive_migrations=options.confirm_destructive_migrations,
        sync_code=options.sync_code,
        sync_deps=options.sync_deps,
        plan_only=options.plan_only,
        resume=options.resume,
        reset_state=options.reset_state,
        stop_after=options.stop_after,
    )


def _fleet_release_snapshot(
    deployment: str,
    *,
    options: FleetUpgradeOptions,
    command_runner: CommandRunner | None,
) -> dict[str, Any] | None:
    try:
        ctx = _build_context(
            _fleet_upgrade_options(deployment, options),
            command_runner=command_runner,
        )
    except SubstrateError as exc:
        return {
            "ok": False,
            "reason": exc.message,
            "error": exc.to_json()["error"],
        }
    result = _run_okg(
        ctx,
        ["deployment", "release", deployment, "--process-role", "up"],
    )
    if not result.ok:
        return {
            "ok": False,
            "reason": "release snapshot failed",
            "returncode": result.returncode,
            "result": result.json_payload(),
            "stdout_tail": (result.stdout or "")[-1000:],
            "stderr_tail": (result.stderr or "")[-1000:],
        }
    payload = result.json_payload()
    if isinstance(payload, dict):
        return payload
    return {"ok": True, "payload": payload}


def _budget_only_resume_target_transition(
    ctx: UpgradeContext,
    *,
    state: Mapping[str, Any],
    target_key: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Admit only the code/manifest transition that installs raised budgets.

    The full backup phase performs the expensive archive, database, toolchain,
    compatibility, and effect-authority checks.  This earlier state boundary
    exists only so that exact failed proof remains reachable after installing
    the implementation and manifest that can revalidate it.
    """
    phase_results = state.get("phase_results")
    phase_results = phase_results if isinstance(phase_results, list) else []
    prior = _prior_backup_attempt_for_resume(phase_results)
    if not isinstance(prior, Mapping) or not _budget_resume_failure_candidate(
        prior
    ):
        return None
    failure_names = _budget_resume_failure_names(prior)
    prior_key = state.get("target_key")
    if not isinstance(prior_key, Mapping):
        return None
    ignored = {"code_revision", "manifest_digest"}
    stable_prior = {
        key: value for key, value in prior_key.items() if key not in ignored
    }
    stable_current = {
        key: value for key, value in target_key.items() if key not in ignored
    }
    if stable_prior != stable_current:
        return None
    transition_checks = {
        "budget_failure_shape_complete": failure_names is not None,
        "code_revision_declared": all(
            isinstance(value, str) and bool(value)
            for value in (
                prior_key.get("code_revision"),
                target_key.get("code_revision"),
            )
        ),
        "manifest_digest_declared": all(
            isinstance(value, str) and value.startswith("sha256:")
            for value in (
                prior_key.get("manifest_digest"),
                target_key.get("manifest_digest"),
            )
        ),
        "target_changed": prior_key != target_key,
    }
    try:
        current_policy = _backup_policy_config(ctx)
        current_budgets = _selected_backup_budgets(
            current_policy,
            routine=False,
        )
    except (KeyError, TypeError, ValueError, SubstrateError) as exc:
        raise SubstrateError(
            "budget-only resume target transition is unreadable",
            data={"error": str(exc)},
            fix_hint=(
                "inspect the retained receipt and current deployment backup "
                "policy, then rerun with --reset-state only if a new backup is intended"
            ),
        ) from exc
    assert isinstance(prior, Mapping)
    transition_checks["policy_equal_except_forced_budgets"] = (
        _budget_resume_policy(prior.get("backup_policy"))
        == _budget_resume_policy(current_policy)
    )
    budget_checks = _budget_resume_budget_checks(
        prior,
        current_budgets=current_budgets,
    )
    transition_checks["forced_budgets_qualified"] = all(
        check.get("ok") is True for check in budget_checks.values()
    )
    report = {
        "schema": "okg.deployment_budget_resume_transition.v1",
        "ok": all(transition_checks.values()),
        "prior_target_key": dict(prior_key),
        "current_target_key": dict(target_key),
        "failure_names": sorted(failure_names or ()),
        "checks": transition_checks,
        "budget_checks": budget_checks,
    }
    if report["ok"] is not True:
        raise SubstrateError(
            "budget-only resume target transition refused",
            data=report,
            fix_hint=(
                "change only the qualified forced budgets, or inspect the "
                "retained receipt before intentionally using --reset-state"
            ),
        )
    return report


def _load_or_initialize_state(ctx: UpgradeContext) -> dict[str, Any]:
    opts = ctx.options
    target_key = _target_state_key(ctx)
    # Bind operator confirmation to the same immutable target artifacts that
    # determine whether a retained lifecycle state is resumable.
    ctx.data["target_key"] = dict(target_key)
    current_phase_fingerprints = phase_fingerprints(
        options=opts,
        target_key=target_key,
        lifecycle_plan_digest=(
            (ctx.data.get("lifecycle_plan_admission") or {}).get("plan_digest")
        ),
    )
    ctx.data["phase_fingerprints"] = current_phase_fingerprints
    if opts.reset_state and ctx.state_file.exists():
        if _runtime_fence_authority_available(ctx):
            from okg.substrate.runtime.promotion_fence import (
                promotion_fence_status,
            )

            fence = promotion_fence_status(
                deployment=opts.deployment,
                dsn=ctx.dsn,
            )
            if (
                isinstance(fence, dict)
                and fence.get("status") != "released"
            ):
                raise SubstrateError(
                    "cannot reset deployment state while its runtime fence is active",
                    data={
                        "deployment": opts.deployment,
                        "state_file": str(ctx.state_file),
                        "fence_owner": fence.get("promotion_id"),
                        "fence_status": fence.get("status"),
                    },
                    fix_hint=(
                        "resume the owning deployment operation without "
                        "--reset-state"
                    ),
                )
        ctx.state_file.unlink()
    if opts.resume and ctx.state_file.exists():
        try:
            state = json.loads(ctx.state_file.read_text())
        except ValueError as exc:
            raise SubstrateError(
                "deployment upgrade state file is not valid JSON",
                data={"state_file": str(ctx.state_file)},
                fix_hint="rerun with --reset-state after inspecting the file",
            ) from exc
        if state.get("lifecycle_contract_digest") != CANONICAL_CONTRACT.digest:
            raise SubstrateError(
                "deployment upgrade state uses an unknown or changed lifecycle contract",
                data={
                    "state_file": str(ctx.state_file),
                    "state_contract_digest": state.get("lifecycle_contract_digest"),
                    "expected_contract_digest": CANONICAL_CONTRACT.digest,
                },
                fix_hint="inspect the old receipt, then start a new operation with --reset-state",
            )
        same_operation = (
            state.get("deployment") == opts.deployment
            and state.get("profile") == opts.profile
            and state.get("plan_only") == bool(opts.plan_only)
            and not bool(state.get("completed"))
        )
        budget_target_transition = None
        prior_target_key = state.get("target_key")
        target_identity_fields = tuple(
            key for key in set((prior_target_key or {})) | set(target_key)
            if key != "lifecycle_policy"
        )
        target_identity_same = (
            isinstance(prior_target_key, Mapping)
            and all(
                prior_target_key.get(field) == target_key.get(field)
                for field in target_identity_fields
            )
        )
        if same_operation and prior_target_key != target_key and not target_identity_same:
            budget_target_transition = _budget_only_resume_target_transition(
                ctx,
                state=state,
                target_key=target_key,
            )
        if (
            same_operation
            and (
                state.get("target_key") == target_key
                or target_identity_same
                or budget_target_transition is not None
            )
        ):
            ctx.data["resumed_existing_state"] = True
            if budget_target_transition is not None or target_identity_same:
                ctx.data["budget_resume_target_transition"] = (
                    budget_target_transition
                )
                state["budget_resume_target_transition"] = (
                    budget_target_transition
                )
                state["target_key"] = dict(target_key)
            phase_inputs = {
                "smoke_publish": {"include": list(opts.include)},
                "readiness": {"profile": opts.readiness_profile},
            }
            if opts.independent_backup_verify:
                phase_inputs["preflight"] = {
                    "code_revision": _upgrade_state_revision(ctx),
                    "manifest_digest": _upgrade_manifest_digest(ctx),
                }
            prior_inputs = state.get("phase_inputs") or {}
            invalidated: set[str] = set()
            prior_fingerprints = state.get("phase_fingerprints")
            if isinstance(prior_fingerprints, Mapping):
                invalidated.update(
                    invalidated_up_phases(
                        prior_fingerprints,
                        current_phase_fingerprints,
                    )
                )
            if budget_target_transition is not None:
                # The retained failed backup remains evidence, but target and
                # migration preflight must be re-observed under the new code.
                invalidated.add("preflight")
            if (
                opts.independent_backup_verify
                and prior_inputs.get("preflight") != phase_inputs["preflight"]
            ):
                # Independent verification deliberately retains the large
                # archive row across checkout churn, but cheap target/migration
                # preflight must be current before archive compatibility is
                # evaluated or a new receipt is finalized.
                invalidated.add("preflight")
            if prior_inputs.get("smoke_publish") != phase_inputs["smoke_publish"]:
                invalidated.update({"smoke_publish", "readiness"})
            elif prior_inputs.get("readiness") != phase_inputs["readiness"]:
                invalidated.add("readiness")
            if invalidated:
                state["completed_phases"] = [
                    phase for phase in state.get("completed_phases") or []
                    if phase not in invalidated
                ]
                state["failed_phase"] = min(
                    invalidated, key=PHASES.index
                )
                state["next_phase"] = state["failed_phase"]
                state["invalidated_phases"] = [
                    phase for phase in PHASES if phase in invalidated
                ]
            state["phase_inputs"] = phase_inputs
            state["phase_fingerprints"] = current_phase_fingerprints
            return state
    if opts.verified_backup_state is not None:
        return _state_with_admitted_verified_backup(ctx, target_key=target_key)
    phase_inputs = {
        "smoke_publish": {"include": list(opts.include)},
        "readiness": {"profile": opts.readiness_profile},
    }
    if opts.independent_backup_verify:
        phase_inputs["preflight"] = {
            "code_revision": _upgrade_state_revision(ctx),
            "manifest_digest": _upgrade_manifest_digest(ctx),
        }
    state = {
        "deployment": opts.deployment,
        "profile": opts.profile,
        "target_key": target_key,
        "plan_only": bool(opts.plan_only),
        "lifecycle_contract_digest": CANONICAL_CONTRACT.digest,
        "lifecycle_contract_version": CANONICAL_CONTRACT.contract_version,
        "lifecycle_plan_admission": ctx.data.get("lifecycle_plan_admission"),
        "phase_inputs": phase_inputs,
        "phase_fingerprints": current_phase_fingerprints,
        "operation_id": f"up-{uuid4().hex}",
        "active_phase": None,
        "phase_attempt_id": None,
        "created_at": _now(),
        "updated_at": _now(),
        "completed": False,
        "completed_phases": [],
        "phase_results": [],
        "phase_statuses": {phase: "not_started" for phase in PHASES},
        "phase_attempts": {},
        "cleanup_obligations": {},
        "failed_phase": None,
        "next_phase": PHASES[0],
    }
    if opts.precheckout_target_revision is not None:
        state["precheckout_authority"] = _precheckout_authority_seed(
            ctx,
            target_key=target_key,
        )
    return state


def _revalidate_resume_postconditions(
    ctx: UpgradeContext,
    *,
    phase_results: list[dict[str, Any]],
    completed: set[str],
) -> tuple[list[dict[str, Any]], set[str], dict[str, Any]]:
    """Re-observe mutable authority before trusting an interrupted receipt."""
    checks: dict[str, bool] = {}
    evidence: dict[str, Any] = {}

    def phase_row(name: str) -> dict[str, Any]:
        return next(
            (row for row in reversed(phase_results) if row.get("phase") == name),
            {},
        )

    if _runtime_quiescence_required(ctx):
        runtime_fence = _revalidate_runtime_fence_resume(
            ctx,
            completed=completed,
        )
        checks["runtime_fence"] = True
        evidence["runtime_fence"] = runtime_fence
    else:
        not_bootstrapped = _runtime_not_bootstrapped_report(ctx)
        if not_bootstrapped is not None:
            checks["runtime_fence"] = True
            evidence["runtime_fence"] = {
                "ok": True,
                "runtime_quiescence": not_bootstrapped,
            }

    if "preflight" in completed:
        observed = _run_phase(ctx, "preflight")
        checks["preflight"] = observed.get("ok") is True
        evidence["preflight"] = observed
        if checks["preflight"]:
            observed["resume_revalidated"] = True
            phase_results = [
                row for row in phase_results if row.get("phase") != "preflight"
            ]
            phase_results.append(observed)

    if "backup" in completed:
        backup = phase_row("backup")
        if backup.get("skipped"):
            checks["backup"] = True
        else:
            archive = Path(str(backup.get("backup_path") or ""))
            try:
                with archive.open("rb") as stream:
                    digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
            except OSError:
                digest = None
            checks["backup"] = bool(digest) and digest == backup.get("backup_digest")
        evidence["backup"] = {"ok": checks["backup"]}

    if "stop_services" in completed and "restart_services" not in completed:
        # Readiness/dogfood worker telemetry describes internal threads and may
        # be stale, so it cannot prove current process absence. Always invalidate
        # this mutable phase and rerun the idempotent stop before DB mutation.
        checks["stop_services"] = False
        evidence["stop_services"] = {
            "reason": "current_process_inactivity_requires_stop_phase_reexecution",
        }
    for phase in ("sync_code",):
        if phase in completed:
            checks[phase] = True  # target_key equality was checked on state admission.
            evidence[phase] = {"target_key_revalidated": True}
    if "dependency_sync" in completed:
        try:
            dependency_target = _dependency_sync_target(ctx, check=True)
        except SubstrateError as exc:
            # A target the planner cannot name cannot prove a completed
            # dependency sync either. Fail the check (the phase re-runs and
            # reports the refusal in its own result) rather than raising
            # through a resume that has already done work.
            checks["dependency_sync"] = False
            evidence["dependency_sync"] = {
                "ok": False,
                "reason": exc.message,
                "error": exc.to_json()["error"],
            }
        else:
            dependency = ctx.command_runner(dependency_target.command, ctx.env)
            checks["dependency_sync"] = dependency.ok
            evidence["dependency_sync"] = {
                "returncode": dependency.returncode,
                "command": _display_command(dependency_target.command),
                "project_root": str(dependency_target.project_root),
                "environment": str(dependency_target.environment),
                "python_extras": list(dependency_target.extras),
            }

    if completed & {"migrate", "catalog_apply"}:
        release = _run_okg(
            ctx,
            ["deployment", "release", ctx.options.deployment, "--process-role", "up"],
        )
        release_payload = release.json_payload()
        release_ok = release.ok and isinstance(release_payload, dict) and (
            release_payload.get("ok") is True
        )
        for phase in ("migrate", "catalog_apply"):
            if phase in completed:
                checks[phase] = release_ok
                evidence[phase] = release_payload

    readiness_phases = completed & {
        "nomos_baseline",
        "generation_recovery",
        "attribute_read_model",
        "restart_services",
        "source_convergence",
        "smoke_publish",
        "readiness",
    }
    if readiness_phases:
        # renamed-flag compat: the child may run pinned pre-rename code.
        ready_result = _run_okg(
            ctx,
            [
                "deployment", "ready", ctx.options.deployment,
                COMPAT_READY_CHECKS_FLAG, ctx.options.readiness_profile,
            ],
            env_overrides=RENAMED_FLAG_COMPAT_ENV,
        )
        ready = ready_result.json_payload()
        ready = ready if isinstance(ready, dict) else {}
        generation = ready.get("generation")
        generation = generation if isinstance(generation, dict) else {}
        integrity = generation.get("count_integrity")
        integrity = integrity if isinstance(integrity, dict) else {}
        source_sync = ready.get("source_sync_status")
        source_sync = source_sync if isinstance(source_sync, dict) else {}
        convergence = source_sync.get("convergence")
        convergence = convergence if isinstance(convergence, dict) else {}
        phase_checks = {
            "nomos_baseline": (
                isinstance(ready.get("nomos_audit"), dict)
                and ready["nomos_audit"].get("ok") is True
                and isinstance(ready.get("nomos_runtime"), dict)
                and ready["nomos_runtime"].get("ok") is True
            ),
            "generation_recovery": integrity.get("ok") is True,
            "attribute_read_model": integrity.get("ok") is True,
            "restart_services": (
                isinstance(ready.get("source_sync_liveness"), dict)
                and ready["source_sync_liveness"].get("ok") is True
            ),
            "source_convergence": (
                convergence.get("ok") is True and convergence.get("state") == "ready"
            ),
            "smoke_publish": (
                ready_result.ok and generation.get("status") == "published"
            ),
            "readiness": ready_result.ok and ready.get("ok") is True,
        }
        for phase in readiness_phases:
            checks[phase] = phase_checks[phase]
            evidence[phase] = ready

    failed = next(
        (phase for phase in PHASES if phase in completed and checks.get(phase) is False),
        None,
    )
    superseded: list[dict[str, Any]] = []
    if failed is not None:
        failed_index = PHASES.index(failed)
        completed = {
            phase for phase in completed if PHASES.index(phase) < failed_index
        }
        kept: list[dict[str, Any]] = []
        for row in phase_results:
            phase = row.get("phase")
            if phase in PHASES and PHASES.index(str(phase)) >= failed_index:
                superseded.append(row)
            else:
                kept.append(row)
        phase_results = kept
    phase_results.sort(
        key=lambda row: PHASES.index(str(row.get("phase")))
        if row.get("phase") in PHASES else len(PHASES)
    )
    return phase_results, completed, {
        "schema": "okg.deployment_up.resume_revalidation.v1",
        "ok": failed is None,
        "earliest_unproven_phase": failed,
        "checks": checks,
        "evidence": evidence,
        "superseded_phase_results": superseded,
        "observed_at": _now(),
    }


def _verified_backup_effect_failures(
    *,
    effects: Any,
    effect_errors: Sequence[str],
    observed_digest: Any,
    receipt_digest: Any,
) -> tuple[list[str], bool]:
    """Which claims refuse a carried-in verified backup, and did its digest hold.

    This admission path takes a backup that was verified EARLIER, against an
    earlier plan, and it never reaches ``select_physical_rollback_policy`` -
    so the forced-current-backup class does not apply and cannot compensate
    for anything here.  What makes the carried-in proof usable is that the
    plan has not moved, which makes ``effect_digest_stable`` load-bearing at
    this boundary even though it is not an authority claim, and makes the
    receipt digest's exact reproduction mandatory.  Recording either without
    refusing on it would admit a verified backup for a plan nobody verified.

    A missing or empty receipt digest is a failure, not a pass: absence of the
    thing we compare against cannot read as a successful comparison.
    """
    effect_checks = effects.to_dict() if effects is not None else {}
    digest_reproduced = (
        isinstance(receipt_digest, str)
        and bool(receipt_digest)
        and observed_digest == receipt_digest
    )
    failures = [
        name for name in (*_REFUSAL_EFFECT_AUTHORITY_CHECKS, "effect_digest_stable")
        if effect_checks.get(name) is not True
    ]
    if not digest_reproduced:
        failures.append("effect_digest_reproduced")
    failures.extend(effect_errors)
    return failures, digest_reproduced


def _state_with_independent_verified_backup(
    ctx: UpgradeContext,
    *,
    target_key: dict[str, Any],
    receipt_path: Path,
    receipt: Mapping[str, Any],
    receipt_stat: os.stat_result,
) -> dict[str, Any]:
    """Consume the exact receipt minted by ``deployment backup verify``."""
    producer_contract = receipt.get("safety_contract")
    producer_contract = (
        producer_contract if isinstance(producer_contract, Mapping) else {}
    )
    producer_plan = producer_contract.get("plan")
    producer_plan = producer_plan if isinstance(producer_plan, Mapping) else {}
    producer_confirmation = producer_plan.get("confirmation")
    producer_confirmation = (
        producer_confirmation
        if isinstance(producer_confirmation, Mapping)
        else {}
    )
    producer_restore = producer_plan.get("restore_point")
    producer_restore = (
        producer_restore if isinstance(producer_restore, Mapping) else {}
    )
    receipt_target = receipt.get("target_key")
    receipt_target = receipt_target if isinstance(receipt_target, Mapping) else {}
    identity_keys = (
        "deployment", "authority_mode", "host", "port", "database",
        "expected_database", "identity",
    )
    mismatches = {
        key: {"receipt": receipt_target.get(key), "target": target_key.get(key)}
        for key in identity_keys
        if receipt_target.get(key) != target_key.get(key)
    }
    rows = [
        dict(row) for row in receipt.get("phase_results") or []
        if isinstance(row, Mapping)
    ]
    preflight = next(
        (row for row in rows if row.get("phase") == "preflight" and row.get("ok") is True),
        None,
    )
    backup = next(
        (row for row in rows if row.get("phase") == "backup" and row.get("ok") is True),
        None,
    )
    receipt_checks = {
        "schema_operation": receipt.get("operation") == "deployment_backup_verify",
        "completed": receipt.get("completed") is True,
        "deployment": receipt.get("deployment") == ctx.options.deployment,
        "profile": receipt.get("profile") == ctx.options.profile,
        "target_identity": not mismatches,
        "receipt_not_symlink": not receipt_path.is_symlink(),
        "receipt_mode_0600": receipt_stat.st_mode & 0o777 == 0o600,
        "receipt_owner_current": receipt_stat.st_uid == os.geteuid(),
        "canonical_phases": [row.get("phase") for row in rows] == [
            "preflight", "backup"
        ],
        "preflight_ok": isinstance(preflight, Mapping),
        "backup_ok": isinstance(backup, Mapping),
        "canonical_safety_contract": (
            producer_contract.get("schema")
            == "okg.deployment_safety_contract.v1"
            and producer_plan.get("schema")
            == "okg.deployment_safety_plan.v1"
            and isinstance(producer_confirmation.get("digest"), str)
            and producer_restore.get("verified") is True
            and isinstance(producer_restore.get("archive_digest"), str)
            and isinstance(producer_restore.get("restore_checks"), Mapping)
        ),
    }
    if not all(receipt_checks.values()) or not isinstance(backup, dict):
        raise SubstrateError(
            "independent verified backup receipt failed admission",
            data={"path": str(receipt_path), "checks": receipt_checks, "mismatches": mismatches},
        )
    archive = Path(str(backup.get("backup_path") or ""))
    try:
        archive_stat = archive.stat()
        archive.resolve().relative_to(ctx.backup_dir.resolve())
        archive_contained = True
    except (OSError, ValueError) as exc:
        raise SubstrateError(
            "independent verified backup archive is unavailable or outside its authority directory",
            data={"path": str(archive)},
        ) from exc
    validation = _validate_backup_archive(ctx, archive)
    age_seconds = max(
        0.0,
        datetime.now(timezone.utc).timestamp() - archive_stat.st_mtime,
    )
    archive_checks = {
        "not_symlink": not archive.is_symlink(),
        "contained": archive_contained,
        "owner_current": archive_stat.st_uid == os.geteuid(),
        "private_mode": validation.get("archive_checks", {}).get("private_mode") is True,
        "archive_readable": validation.get("ok") is True,
        "digest_match": (
            validation.get("backup_digest") == backup.get("backup_digest")
        ),
        "size_match": (
            validation.get("backup_size_bytes") == backup.get("backup_size_bytes")
        ),
        "restore_verified": backup.get("restore_verified") is True,
        "current_full_restore": backup.get("verification_mode") == "current_full_restore",
        "fresh": age_seconds <= 3600,
    }
    if not all(archive_checks.values()):
        raise SubstrateError(
            "independent verified backup archive failed admission",
            data={"path": str(archive), "checks": archive_checks, "age_seconds": age_seconds},
        )
    completion = _revalidate_independent_backup_completion(
        ctx,
        payload=receipt,
        backup=backup,
    )
    if completion.get("ok") is not True:
        raise SubstrateError(
            "independent verified backup no longer matches the live database",
            data={"completion_revalidation": completion},
        )
    producer_effect_digest = producer_plan.get("effect_digest")
    effects, effect_errors, effect_report = _collect_effect_inventory(
        ctx,
        expected_digest=producer_effect_digest,
    )
    failed_effects, effect_digest_reproduced = _verified_backup_effect_failures(
        effects=effects,
        effect_errors=effect_errors,
        observed_digest=effect_report.get("effect_digest"),
        receipt_digest=producer_effect_digest,
    )
    if effects is None or effect_errors or failed_effects:
        raise SubstrateError(
            "independent verified backup target effect authority is not closed",
            data={
                "failed_checks": failed_effects,
                "effect_errors": list(effect_errors),
                "effect_inventory": effect_report,
            },
        )
    restore_evidence = {
        "receipt_path": producer_restore.get("receipt_path"),
        "receipt_digest": producer_restore.get("receipt_digest"),
        "archive_digest": producer_restore.get("archive_digest"),
        "fresh": producer_restore.get("fresh"),
        "owner_current": producer_restore.get("owner_current"),
        "restore_checks": producer_restore.get("restore_checks") or {},
        "policy_digest": (
            (producer_restore.get("producer_contract") or {}).get(
                "policy_digest"
            )
            if isinstance(producer_restore.get("producer_contract"), Mapping)
            else None
        ),
        "toolchain_digest": (
            (producer_restore.get("producer_contract") or {}).get(
                "toolchain_digest"
            )
            if isinstance(producer_restore.get("producer_contract"), Mapping)
            else None
        ),
    }
    safety_plan = _deployment_safety_plan(
        ctx,
        effect_report=effect_report,
        restore_point_source="independent_backup_verify",
        restore_point_verified=True,
        restore_point_evidence=restore_evidence,
    )
    current_confirmation = safety_plan.get("confirmation")
    current_confirmation = (
        current_confirmation if isinstance(current_confirmation, Mapping) else {}
    )
    if (
        current_confirmation.get("digest")
        != producer_confirmation.get("digest")
    ):
        raise SubstrateError(
            "independent verified backup safety plan drifted",
            data={
                "receipt_safety_plan_digest": producer_confirmation.get("digest"),
                "current_safety_plan_digest": current_confirmation.get("digest"),
            },
        )
    refusal = _safety_plan_refusal(ctx, safety_plan)
    if refusal is not None and not ctx.options.plan_only:
        raise SubstrateError(
            str(refusal["reason"]),
            data={
                "safety_plan": safety_plan,
                "mutation_authorized": False,
                "next_command": refusal.get("next_command"),
            },
            fix_hint=str(refusal.get("fix_hint") or "inspect and confirm the safety plan"),
        )
    admitted_backup = {
        **backup,
        "verification_mode": "current_full_restore",
        "effect_inventory": effect_report,
        "independent_restore_point_admission": {
            "schema": "okg.deployment_restore_point.v1",
            "receipt_path": str(receipt_path),
            "receipt_checks": receipt_checks,
            "archive_checks": archive_checks,
            "age_seconds": age_seconds,
            "completion_revalidation": completion,
            "state_receipt_path": str(receipt_path),
            "state_receipt_digest": "sha256:" + hashlib.sha256(
                receipt_path.read_bytes()
            ).hexdigest(),
            "effect_digest_reproduced": effect_digest_reproduced,
        },
    }
    ctx.data["backup_result"] = admitted_backup
    contract = _safety_contract_snapshot(
        ctx, restore_point_verified=True, phase="backup"
    )
    now = _now()
    return {
        "deployment": ctx.options.deployment,
        "profile": ctx.options.profile,
        "target_key": target_key,
        "plan_only": bool(ctx.options.plan_only),
        "lifecycle_contract_digest": CANONICAL_CONTRACT.digest,
        "lifecycle_contract_version": CANONICAL_CONTRACT.contract_version,
        "lifecycle_plan_admission": ctx.data.get("lifecycle_plan_admission"),
        "created_at": now,
        "updated_at": now,
        "completed": False,
        "completed_phases": ["preflight", "backup"],
        "phase_results": [dict(preflight or {}), admitted_backup],
        "phase_statuses": {
            phase: "succeeded" if phase in {"preflight", "backup"} else "not_started"
            for phase in PHASES
        },
        "phase_attempts": _admitted_phase_attempts(
            ctx,
            preflight=dict(preflight or {}),
            backup=admitted_backup,
            source="independent_backup_verify",
        ),
        "safety_contract": contract,
        "phase_inputs": {
            "smoke_publish": {"include": list(ctx.options.include)},
            "readiness": {"profile": ctx.options.readiness_profile},
        },
        "phase_fingerprints": phase_fingerprints(
            options=ctx.options,
            target_key=target_key,
            lifecycle_plan_digest=None,
        ),
        "operation_id": f"up-{uuid4().hex}",
        "active_phase": None,
        "phase_attempt_id": None,
        "cleanup_obligations": {},
        "failed_phase": None,
        "next_phase": "stop_services",
    }


def _state_with_admitted_verified_backup(
    ctx: UpgradeContext,
    *,
    target_key: dict[str, Any],
) -> dict[str, Any]:
    """Admit one pre-checkout verified backup into the new-code lifecycle."""
    receipt_path = ctx.options.verified_backup_state
    assert receipt_path is not None
    try:
        if receipt_path.is_symlink():
            raise OSError("verified backup state must not be a symlink")
        receipt_stat = receipt_path.stat()
        receipt = json.loads(receipt_path.read_text())
    except (OSError, ValueError) as exc:
        raise SubstrateError(
            "verified backup state is unavailable or invalid",
            data={"path": str(receipt_path)},
        ) from exc
    if (
        receipt.get("operation") == "deployment_backup_verify"
        and receipt.get("completed") is True
    ):
        return _state_with_independent_verified_backup(
            ctx,
            target_key=target_key,
            receipt_path=receipt_path,
            receipt=receipt,
            receipt_stat=receipt_stat,
        )
    source_revision = ctx.options.verified_backup_source_revision
    if not isinstance(source_revision, str) or not source_revision:
        raise SubstrateError(
            "pre-checkout verified backup admission requires an exact source commit",
            fix_hint=(
                "pass --verified-backup-source-revision <full-commit> "
                "--lifecycle-plan <receipt>, or use a completed deployment backup verify receipt"
            ),
        )
    source_repo: Path | None = None
    target_authority_error = target_key.get("authority_error")
    if isinstance(target_authority_error, dict):
        raise SubstrateError(
            "verified backup transition authority is unavailable",
            data={"authority_error": target_authority_error},
        )
    try:
        manifest = _resolve_upgrade_deployment(ctx)
        deployment_dir = Path(manifest.deployment_dir).resolve()
        # The verified-backup source revision names okg code, so it must be
        # resolved in the repository that holds okg -- not the deployment
        # repository, which for a vendored install has no such object.
        release = getattr(manifest, "release", None) or {}
        pin = release.get("pin") if isinstance(release.get("pin"), dict) else {}
        explicit_code_repository = (
            pin.get("code_repository")
            if isinstance(pin, dict) and "code_repository" in pin
            else None
        )
        source_repo, _deployment_repo = _resolve_repo_authorities(
            deployment_dir,
            explicit_code_repository=(
                str(explicit_code_repository)
                if explicit_code_repository is not None
                else None
            ),
        )
        resolved_source_revision = str(
            _git_command(
                source_repo,
                ["rev-parse", f"{source_revision}^{{commit}}"],
            )
        ).strip()
    except RevisionAuthorityError as exc:
        raise SubstrateError(
            "verified backup source authority is unavailable",
            data={"authority_error": exc.data},
        ) from exc
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        raise SubstrateError(
            "verified backup source revision is unavailable",
            data={
                "authority_error": {
                    "authority_role": "framework",
                    "repository": (
                        str(source_repo) if source_repo is not None else None
                    ),
                    "requested_revision": source_revision,
                    "resolved_revision": None,
                    "operation": "resolve_verified_backup_source_revision",
                    "reason": str(exc),
                },
            },
        ) from exc
    if source_revision != resolved_source_revision:
        raise SubstrateError(
            "verified backup source revision is not the exact commit object",
            data={
                "authority_error": {
                    "authority_role": "framework",
                    "repository": (
                        str(source_repo) if source_repo is not None else None
                    ),
                    "requested_revision": source_revision,
                    "resolved_revision": resolved_source_revision,
                    "operation": "resolve_verified_backup_source_revision",
                    "reason": "declared revision is not the exact commit object",
                },
            },
        )
    receipt_target = receipt.get("target_key") or {}
    precheckout_authority = receipt.get("precheckout_authority")
    precheckout_authority = (
        precheckout_authority if isinstance(precheckout_authority, dict) else {}
    )
    identity_keys = (
        "deployment",
        "authority_mode",
        "host",
        "port",
        "database",
        "expected_database",
        "identity",
    )
    receipt_database_identity = precheckout_authority.get("database_identity")
    receipt_database_identity = (
        receipt_database_identity if isinstance(receipt_database_identity, dict) else {}
    )
    expected_authority_fields = {
        "schema",
        "framework_repository",
        "deployment_repository",
        "planner_revision",
        "target_revision",
        "deployment_revision",
        "target_manifest_digest",
        "lifecycle_plan_digest",
        "database_identity",
        "effect_digest",
        "backup_digest",
    }
    current_database_identity = {
        key: target_key.get(key) for key in identity_keys if key in target_key
    }
    transition_checks = {
        "authority_shape": set(precheckout_authority) == expected_authority_fields,
        "schema": precheckout_authority.get("schema")
        == "okg.deployment_precheckout_authority.v1",
        "deployment": receipt.get("deployment") == ctx.options.deployment,
        "profile": receipt.get("profile") == ctx.options.profile,
        "planner_revision": (
            bool(source_revision)
            and precheckout_authority.get("planner_revision") == source_revision
        ),
        "target_revision": (
            precheckout_authority.get("target_revision")
            == target_key.get("code_revision")
            == receipt_target.get("code_revision")
        ),
        "deployment_revision": (
            precheckout_authority.get("deployment_revision")
            == target_key.get("deployment_revision")
            == receipt_target.get("deployment_revision")
        ),
        "framework_repository": (
            precheckout_authority.get("framework_repository")
            == target_key.get("framework_repository")
            == receipt_target.get("framework_repository")
        ),
        "deployment_repository": (
            precheckout_authority.get("deployment_repository")
            == target_key.get("deployment_repository")
            == receipt_target.get("deployment_repository")
        ),
        "target_manifest_digest": (
            precheckout_authority.get("target_manifest_digest")
            == target_key.get("manifest_digest")
            == receipt_target.get("manifest_digest")
        ),
        "lifecycle_plan_digest": (
            isinstance(target_key.get("lifecycle_plan_digest"), str)
            and target_key.get("lifecycle_plan_digest", "").startswith("sha256:")
            and precheckout_authority.get("lifecycle_plan_digest")
            == target_key.get("lifecycle_plan_digest")
            == receipt_target.get("lifecycle_plan_digest")
            == (ctx.data.get("lifecycle_plan_admission") or {}).get("plan_digest")
        ),
        "database_identity": (receipt_database_identity == current_database_identity),
    }
    if not all(transition_checks.values()):
        raise SubstrateError(
            "verified backup state does not match the declared transition",
            data={
                "path": str(receipt_path),
                "source_revision": source_revision,
                "checks": transition_checks,
                "precheckout_authority": precheckout_authority,
            },
        )
    mismatched_identity = {
        key: {"receipt": receipt_target.get(key), "target": target_key.get(key)}
        for key in identity_keys
        if receipt_target.get(key) != target_key.get(key)
    }
    if mismatched_identity:
        raise SubstrateError(
            "verified backup state belongs to a different database target",
            data={"mismatches": mismatched_identity},
        )
    receipt_phase_rows = [
        dict(row)
        for row in receipt.get("phase_results") or ()
        if isinstance(row, dict)
    ]
    preflight = next(
        (
            row for row in receipt_phase_rows
            if row.get("phase") == "preflight" and row.get("ok") is True
        ),
        None,
    )
    backup = next(
        (
            row for row in receipt_phase_rows
            if row.get("phase") == "backup" and row.get("ok") is True
        ),
        None,
    )
    if not isinstance(backup, dict):
        raise SubstrateError("verified backup state has no passing backup receipt")
    receipt_effect_digest = str(
        ((backup.get("effect_inventory") or {}).get("effect_digest"))
        or backup.get("effect_digest")
        or ""
    )
    receipt_backup_digest = str(backup.get("backup_digest") or "")
    completed_transition_checks = {
        "effect_digest": (
            bool(receipt_effect_digest)
            and precheckout_authority.get("effect_digest") == receipt_effect_digest
        ),
        "backup_digest": (
            bool(receipt_backup_digest)
            and precheckout_authority.get("backup_digest") == receipt_backup_digest
        ),
    }
    if not all(completed_transition_checks.values()):
        raise SubstrateError(
            "verified backup transition evidence is incomplete or inconsistent",
            data={
                "path": str(receipt_path),
                "checks": completed_transition_checks,
            },
        )
    archive = Path(str(backup.get("backup_path") or ""))
    expected_digest = str(backup.get("backup_digest") or "")
    try:
        stat = archive.stat()
    except OSError as exc:
        raise SubstrateError(
            "verified backup archive is unavailable",
            data={"path": str(archive)},
        ) from exc
    archive_validation = _validate_backup_archive(ctx, archive)
    observed_digest = str(archive_validation.get("backup_digest") or "")
    age_seconds = max(0.0, datetime.now(timezone.utc).timestamp() - stat.st_mtime)
    try:
        archive.resolve().relative_to(ctx.backup_dir.resolve())
        archive_contained = True
    except ValueError:
        archive_contained = False
    observed_phases = [str(row.get("phase")) for row in receipt_phase_rows]
    checks = {
        "archive_not_symlink": not archive.is_symlink(),
        "receipt_mode_0600": receipt_stat.st_mode & 0o777 == 0o600,
        "receipt_owner_current": receipt_stat.st_uid == os.geteuid(),
        "receipt_incomplete": receipt.get("completed") is False,
        "preflight_ok": isinstance(preflight, dict),
        "canonical_preflight_backup_order": observed_phases == ["preflight", "backup"],
        "archive_readable": (
            backup.get("archive_readable") is True
            and archive_validation.get("ok") is True
        ),
        "ownership_ok": backup.get("ownership_ok") is True,
        "schema_ok": backup.get("schema_ok") is True,
        "restore_verified": backup.get("restore_verified") is True,
        "private_mode": archive_validation.get("archive_checks", {}).get("private_mode")
        is True,
        "archive_owner_current": stat.st_uid == os.geteuid(),
        "owners_match": stat.st_uid == receipt_stat.st_uid,
        "archive_contained": archive_contained,
        "size_match": (
            isinstance(backup.get("backup_size_bytes"), int)
            and not isinstance(backup.get("backup_size_bytes"), bool)
            and backup.get("backup_size_bytes")
            == archive_validation.get("backup_size_bytes")
        ),
        "reported_mode": (
            str(backup.get("backup_mode") or "")
            == str(archive_validation.get("backup_mode") or "")
        ),
        "pg_dump_command": str(backup.get("command") or "").startswith("pg_dump "),
        "digest_match": bool(expected_digest) and observed_digest == expected_digest,
        "fresh": age_seconds <= 3600,
    }
    if not all(checks.values()):
        raise SubstrateError(
            "verified backup receipt failed admission",
            data={"path": str(archive), "checks": checks, "age_seconds": age_seconds},
        )
    legacy_preflight = next(
        (
            row for row in receipt.get("phase_results") or ()
            if row.get("phase") == "preflight" and row.get("ok") is True
        ),
        None,
    )
    migration_status = (
        legacy_preflight.get("migration_status")
        if isinstance(legacy_preflight, dict)
        else None
    )
    if not isinstance(migration_status, dict):
        raise SubstrateError(
            "verified backup state lacks database-state-keyed preflight evidence",
            data={"path": str(receipt_path)},
        )
    observed_preflight = _run_phase(ctx, "preflight")
    if observed_preflight.get("ok") is not True:
        raise SubstrateError(
            "verified backup preflight could not be revalidated",
            data={
                "path": str(receipt_path),
                "preflight": observed_preflight,
            },
        )
    observed_migration_status = observed_preflight.get("migration_status")
    if not isinstance(observed_migration_status, dict):
        raise SubstrateError(
            "verified backup preflight revalidation lacks migration evidence",
            data={
                "path": str(receipt_path),
                "preflight": observed_preflight,
            },
        )
    ctx.data["migrations_before"] = observed_migration_status
    toolchain = backup.get("toolchain")
    if not isinstance(toolchain, dict) or not toolchain.get("ok"):
        toolchain = _backup_tool_preflight(ctx)
    if not toolchain.get("ok"):
        raise SubstrateError(
            "verified backup toolchain cannot be revalidated",
            data={"path": str(receipt_path), "toolchain": toolchain},
        )
    try:
        compatibility_snapshot = _backup_compatibility_snapshot(ctx, toolchain)
        backup_policy = _backup_policy_config(ctx)
        source_database_size = _estimate_source_db_bytes(ctx)
    except Exception as exc:  # noqa: BLE001 - legacy admission is fail-closed.
        raise SubstrateError(
            "verified backup database state cannot be revalidated",
            data={"path": str(receipt_path), "error": str(exc)},
        ) from exc
    if source_database_size is None or source_database_size <= 0:
        raise SubstrateError(
            "verified backup source database size cannot be revalidated",
            data={"path": str(receipt_path)},
        )
    admitted = {
        **backup,
        "verification_mode": "current_full_restore",
        "candidate_plan_digest": compatibility_snapshot["candidate_plan_digest"],
        "compatibility_snapshot": compatibility_snapshot,
        "backup_policy": backup_policy,
        "toolchain": toolchain,
        "source_database_size_bytes": source_database_size,
        "admission": {
            "schema": "okg.deployment_backup_admission.v1",
            "receipt_path": str(receipt_path),
            "source_revision": source_revision,
            "target_revision": target_key.get("code_revision"),
            "observed_digest": observed_digest,
            "age_seconds": age_seconds,
            "checks": checks,
        },
    }
    assert isinstance(preflight, dict)
    admitted_preflight = {
        **preflight,
        "resume_revalidated": True,
        "revalidation": observed_preflight,
        "admission": {
            "schema": "okg.deployment_precheckout_preflight_admission.v1",
            "receipt_path": str(receipt_path),
            "source_revision": source_revision,
            "target_revision": target_key.get("code_revision"),
            "checks": {
                "database_identity": not mismatched_identity,
                "canonical_prefix": True,
                "preflight_passed": True,
            },
        },
    }
    effects, effect_errors, effect_report = _collect_effect_inventory(
        ctx,
        expected_digest=receipt_effect_digest,
    )
    refusal_failures, effect_digest_reproduced = _verified_backup_effect_failures(
        effects=effects,
        effect_errors=effect_errors,
        observed_digest=effect_report.get("effect_digest"),
        receipt_digest=receipt_effect_digest,
    )
    if effects is None or effect_errors or refusal_failures:
        raise SubstrateError(
            "verified backup target effect authority is not closed",
            data={
                "effect_errors": list(effect_errors),
                "failed_checks": refusal_failures,
                "expected_effect_digest": receipt_effect_digest,
                "observed_effect_digest": effect_report.get("effect_digest"),
                "effect_inventory": effect_report,
            },
        )
    admitted["effect_inventory"] = effect_report
    admitted["admission"]["transition_checks"] = {
        **transition_checks,
        **completed_transition_checks,
        "effect_digest_reproduced": effect_digest_reproduced,
    }
    ctx.data["backup_result"] = admitted
    safety_plan = _deployment_safety_plan(
        ctx,
        effect_report=effect_report,
        restore_point_source="precheckout_verified_backup",
        restore_point_verified=True,
        restore_point_evidence={
            "receipt_path": str(receipt_path),
            "receipt_digest": "sha256:" + hashlib.sha256(
                receipt_path.read_bytes()
            ).hexdigest(),
            "archive_digest": observed_digest,
            "fresh": checks["fresh"],
            "owner_current": checks["receipt_owner_current"]
            and checks["archive_owner_current"],
            "restore_checks": checks,
        },
    )
    refusal = _safety_plan_refusal(ctx, safety_plan)
    if refusal is not None and not ctx.options.plan_only:
        raise SubstrateError(
            str(refusal["reason"]),
            data={
                "safety_plan": safety_plan,
                "mutation_authorized": False,
                "next_command": refusal.get("next_command"),
            },
            fix_hint=str(refusal.get("fix_hint") or "inspect and confirm the safety plan"),
        )
    safety_contract = _safety_contract_snapshot(
        ctx,
        restore_point_verified=True,
        phase="backup",
    )
    now = _now()
    return {
        "deployment": ctx.options.deployment,
        "profile": ctx.options.profile,
        "target_key": target_key,
        "plan_only": bool(ctx.options.plan_only),
        "lifecycle_contract_digest": CANONICAL_CONTRACT.digest,
        "lifecycle_contract_version": CANONICAL_CONTRACT.contract_version,
        "lifecycle_plan_admission": ctx.data.get("lifecycle_plan_admission"),
        "created_at": now,
        "updated_at": now,
        "completed": False,
        "completed_phases": ["preflight", "backup"],
        "phase_results": [admitted_preflight, admitted],
        "phase_statuses": {
            phase: (
                "succeeded" if phase in {"preflight", "backup"} else "not_started"
            )
            for phase in PHASES
        },
        "phase_attempts": _admitted_phase_attempts(
            ctx,
            preflight=admitted_preflight,
            backup=admitted,
            source="precheckout_verified_backup",
        ),
        "safety_contract": safety_contract,
        "phase_inputs": {
            "smoke_publish": {"include": list(ctx.options.include)},
            "readiness": {"profile": ctx.options.readiness_profile},
        },
        "phase_fingerprints": phase_fingerprints(
            options=ctx.options,
            target_key=target_key,
            lifecycle_plan_digest=(
                (ctx.data.get("lifecycle_plan_admission") or {}).get(
                    "plan_digest"
                )
            ),
        ),
        "operation_id": f"up-{uuid4().hex}",
        "active_phase": None,
        "phase_attempt_id": None,
        "cleanup_obligations": {},
        "failed_phase": None,
        "next_phase": "stop_services",
    }


def _target_state_key(ctx: UpgradeContext) -> dict[str, Any]:
    resolution = getattr(ctx, "resolution", {})
    target = resolution.get("target") if isinstance(resolution, Mapping) else None
    if not isinstance(target, dict):
        target = {"identity": "unresolved"}
    keys = (
        "deployment",
        "authority_mode",
        "host",
        "port",
        "database",
        "identity",
        "expected_database",
    )
    key = {key: target.get(key) for key in keys if key in target}
    if ctx.options.independent_backup_verify:
        # A retained archive is admitted by its own exact policy, toolchain,
        # live logical-identity, digest and size checks.  Unrelated checkout or
        # manifest churn must not discard it and force another multi-minute
        # dump after a late restore failure.
        key["operation"] = "independent_backup_verify"
        return key
    authority: RevisionAuthority | None = None
    authority_error: dict[str, Any] | None = None
    try:
        manifest = _resolve_upgrade_deployment(ctx)
        authority = _git_target_revision(ctx, manifest)
    except RevisionAuthorityError as exc:
        # The effect-inventory phase remains the authoritative refusal point
        # for an unreadable target. Preserve legacy state-key behavior here so
        # malformed or partially scaffolded test/development deployments can
        # still report the later, structured authority error.
        authority_error = exc.data
        authority = None
    except SubstrateError:
        authority_error = None
        authority = None
    try:
        fallback_revision = (
            authority.code_revision
            if authority is not None
            else _upgrade_state_revision(ctx)
        )
    except SubstrateError as exc:
        fallback_revision = None
        if authority_error is None:
            candidate_error = exc.data.get("authority_error")
            if isinstance(candidate_error, dict):
                authority_error = candidate_error
    key.update({
        "code_revision": fallback_revision,
        "manifest_digest": _upgrade_manifest_digest(ctx),
        "lifecycle_contract_digest": CANONICAL_CONTRACT.digest,
        "lifecycle_plan_digest": (
            (ctx.data.get("lifecycle_plan_admission") or {}).get("plan_digest")
        ),
        "lifecycle_policy": {
            "apply_services": ctx.options.apply_services,
            "sync_code": ctx.options.sync_code,
            "sync_deps": ctx.options.sync_deps,
            "skip_backup": ctx.options.skip_backup,
            "skip_migrate": ctx.options.skip_migrate,
            "skip_destructive_migrations": (
                ctx.options.skip_destructive_migrations
            ),
            "confirm_destructive_migrations": (
                ctx.options.confirm_destructive_migrations
            ),
            "plan_only": ctx.options.plan_only,
            "independent_backup_verify": (
                ctx.options.independent_backup_verify
            ),
        },
    })
    if authority is not None:
        key.update({
            "framework_repository": str(authority.code_repo_root),
            "deployment_repository": str(authority.deployment_repo_root),
            "deployment_revision": authority.deployment_revision,
        })
    elif authority_error is not None:
        key["authority_error"] = authority_error
    return key


def _precheckout_authority_seed(
    ctx: UpgradeContext,
    *,
    target_key: Mapping[str, Any],
) -> dict[str, Any]:
    manifest = _resolve_upgrade_deployment(ctx)
    authority = _git_target_revision(ctx, manifest)
    target_revision = authority.code_revision
    planner_revision = authority.planner_revision
    identity_keys = (
        "deployment",
        "authority_mode",
        "host",
        "port",
        "database",
        "expected_database",
        "identity",
    )
    return {
        "schema": "okg.deployment_precheckout_authority.v1",
        "framework_repository": str(authority.code_repo_root),
        "deployment_repository": str(authority.deployment_repo_root),
        "planner_revision": planner_revision,
        "target_revision": target_revision,
        "deployment_revision": authority.deployment_revision,
        "target_manifest_digest": target_key.get("manifest_digest"),
        "lifecycle_plan_digest": target_key.get("lifecycle_plan_digest"),
        "database_identity": {
            key: target_key.get(key) for key in identity_keys if key in target_key
        },
        "effect_digest": None,
        "backup_digest": None,
    }


def _complete_precheckout_authority(
    seed: Mapping[str, Any],
    backup: Mapping[str, Any],
) -> dict[str, Any]:
    effect_inventory = backup.get("effect_inventory")
    effect_inventory = effect_inventory if isinstance(effect_inventory, Mapping) else {}
    effect_digest = effect_inventory.get("effect_digest") or backup.get("effect_digest")
    backup_digest = backup.get("backup_digest")
    if not (
        isinstance(effect_digest, str)
        and effect_digest.startswith("sha256:")
        and isinstance(backup_digest, str)
        and backup_digest.startswith("sha256:")
    ):
        raise SubstrateError(
            "precheckout backup did not produce complete transition authority",
            data={
                "effect_digest_present": bool(effect_digest),
                "backup_digest_present": bool(backup_digest),
            },
        )
    return {
        **dict(seed),
        "effect_digest": effect_digest,
        "backup_digest": backup_digest,
    }


def _upgrade_state_revision(ctx: UpgradeContext) -> str | None:
    try:
        manifest = _resolve_upgrade_deployment(ctx)
    except SubstrateError:
        # State identity is additional resume protection, not permission to
        # replace the structured deployment-resolution preflight with a crash.
        return None
    try:
        return _git_target_revision(ctx, manifest).code_revision
    except (OSError, ValueError, subprocess.SubprocessError):
        # This helper supplies auxiliary state identity only. The effect
        # inventory and admission path perform the authoritative resolution and
        # preserve RevisionAuthorityError.data; a temporary/non-Git deployment
        # must not fail earlier merely because it has no state-key revision.
        return None


def _upgrade_manifest_digest(ctx: UpgradeContext) -> str | None:
    try:
        manifest = _resolve_upgrade_deployment(ctx)
    except SubstrateError:
        return None
    if ctx.options.precheckout_target_revision is not None:
        try:
            authority = _git_target_revision(ctx, manifest)
            relative = authority.deployment_relative / "deployment.yaml"
            payload = _git_command(
                authority.deployment_repo_root,
                [
                    "show",
                    f"{authority.deployment_revision}:{relative.as_posix()}",
                ],
                binary=True,
            )
            if not isinstance(payload, bytes):
                raise ValueError("candidate deployment manifest is not binary data")
            return "sha256:" + hashlib.sha256(payload).hexdigest()
        except RevisionAuthorityError as exc:
            raise SubstrateError(
                "precheckout candidate manifest authority is unavailable",
                data={"authority_error": exc.data},
            ) from exc
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            raise SubstrateError(
                "precheckout candidate manifest is unavailable",
                data={
                    "authority_error": {
                        "authority_role": "deployment",
                        "repository": (
                            str(authority.deployment_repo_root)
                            if "authority" in locals()
                            else None
                        ),
                        "requested_revision": (
                            authority.deployment_revision
                            if "authority" in locals()
                            else ctx.options.precheckout_target_revision
                        ),
                        "resolved_revision": (
                            authority.deployment_revision
                            if "authority" in locals()
                            else None
                        ),
                        "operation": "read_precheckout_candidate_manifest",
                        "reason": str(exc),
                    },
                },
            ) from exc
    path = Path(manifest.deployment_dir) / "deployment.yaml"
    if not path.is_file():
        return None
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _run_phase(ctx: UpgradeContext, phase: str) -> dict[str, Any]:
    handler = {
        "preflight": _phase_preflight,
        "backup": _phase_backup,
        "stop_services": _phase_stop_services,
        "sync_code": _phase_sync_code,
        "dependency_sync": _phase_dependency_sync,
        "migrate": _phase_migrate,
        "catalog_apply": _phase_catalog_apply,
        "nomos_baseline": _phase_nomos_baseline,
        "generation_recovery": _phase_generation_recovery,
        "attribute_read_model": _phase_attribute_read_model,
        "restart_services": _phase_restart_services,
        "source_convergence": _phase_source_convergence,
        "smoke_publish": _phase_smoke_publish,
        "readiness": _phase_readiness,
    }[phase]
    started_at = _now()
    started_monotonic = time.monotonic()
    try:
        result = handler(ctx)
    except SubstrateError as exc:
        return {
            "phase": phase,
            "phase_attempt_id": ctx.data.get("phase_attempt_id"),
            "semantic_fingerprint": (
                (ctx.data.get("phase_fingerprints") or {}).get(phase)
                if isinstance(ctx.data.get("phase_fingerprints"), Mapping)
                else None
            ),
            "ok": False,
            "reason": exc.message,
            "error": exc.to_json()["error"],
            "started_at": started_at,
            "completed_at": _now(),
            "duration_seconds": time.monotonic() - started_monotonic,
        }
    except Exception as exc:  # noqa: BLE001 - phase failures must be durable.
        return {
            "phase": phase,
            "phase_attempt_id": ctx.data.get("phase_attempt_id"),
            "semantic_fingerprint": (
                (ctx.data.get("phase_fingerprints") or {}).get(phase)
                if isinstance(ctx.data.get("phase_fingerprints"), Mapping)
                else None
            ),
            "ok": False,
            "reason": f"{phase} raised an unexpected error",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "started_at": started_at,
            "completed_at": _now(),
            "duration_seconds": time.monotonic() - started_monotonic,
        }
    except BaseException as exc:
        completed_at = _now()
        duration_seconds = time.monotonic() - started_monotonic
        try:
            setattr(exc, "_okg_phase_timing", {
                "started_at": started_at,
                "completed_at": completed_at,
                "duration_seconds": duration_seconds,
            })
        except Exception:
            # Some BaseException implementations reject dynamic attributes;
            # timing metadata is diagnostic only, so preserve the interrupt.
            pass
        raise
    row = {
        "phase": phase,
        "phase_attempt_id": ctx.data.get("phase_attempt_id"),
        "semantic_fingerprint": (
            (ctx.data.get("phase_fingerprints") or {}).get(phase)
            if isinstance(ctx.data.get("phase_fingerprints"), Mapping)
            else None
        ),
        "started_at": started_at,
        "completed_at": _now(),
        "duration_seconds": time.monotonic() - started_monotonic,
        **result,
    }
    if not ctx.options.plan_only:
        budgets: Mapping[str, Any] | None = None
        observations: dict[str, float] = {}
        if phase == "backup" and isinstance(row.get("budgets_seconds"), Mapping):
            budgets = row["budgets_seconds"]
            observations["backup"] = float(row["duration_seconds"])
            dump_duration = row.get("dump_duration_seconds")
            if (
                isinstance(dump_duration, (int, float))
                and not isinstance(dump_duration, bool)
            ):
                observations["dump"] = float(dump_duration)
        elif phase == "attribute_read_model":
            backup = ctx.data.get("backup_result")
            if isinstance(backup, dict) and isinstance(
                backup.get("budgets_seconds"), Mapping
            ):
                budgets = backup["budgets_seconds"]
                observations["attribute_read_model"] = float(
                    row["duration_seconds"]
                )
        if budgets is not None and observations:
            checks: dict[str, dict[str, float | bool]] = {}
            exceeded: list[str] = []
            for budget_name, observed in observations.items():
                limit = budgets.get(budget_name)
                if not isinstance(limit, (int, float)) or isinstance(limit, bool):
                    continue
                checks[budget_name] = {
                    "observed_seconds": observed,
                    "budget_seconds": float(limit),
                    "ok": observed <= float(limit),
                }
                if observed > float(limit):
                    exceeded.append(budget_name)
            if checks:
                row["budget_checks"] = checks
            primary = (
                "attribute_read_model"
                if "attribute_read_model" in observations
                else "backup"
            )
            if primary in checks:
                row["budget_seconds"] = checks[primary]["budget_seconds"]
            if exceeded:
                row.update({
                    "ok": False,
                    "reason": f"{exceeded[0]} phase budget exceeded",
                    "budget_exceeded": True,
                    "budget_exceeded_names": exceeded,
                })
    # Terminal effect receipt: write the post-budget terminal row immediately
    # after the handler, closing the crash window before the outer orchestration
    # updates completed_phases.
    state_file = getattr(ctx, "state_file", None)
    if not ctx.options.plan_only and isinstance(state_file, Path) and state_file.is_file():
        durable_state = redact_value(_load_state_file(ctx.state_file))
        receipts = durable_state.get("phase_terminal_receipts")
        receipts = dict(receipts) if isinstance(receipts, Mapping) else {}
        attempt_id = str(row.get("phase_attempt_id") or "")
        if attempt_id:
            receipts[attempt_id] = row
            durable_state["phase_terminal_receipts"] = receipts
            durable_state["updated_at"] = _now()
            _write_state(ctx.state_file, durable_state)
    return row


def _adopt_deferred_recovery_classification(
    ctx: UpgradeContext, deferred: dict[str, Any]
) -> dict[str, Any]:
    """First-classification adoption for a preflight that recorded a deferral.

    Preflight could not run the recovery classifier (its queries need the
    schema the pending migration creates), so this is the binding BASELINE,
    not a re-inspection: verify the migration actually applied, run the
    classifier once against the migrated schema, validate the payload shape
    exactly as preflight would have, and hand the result back for the
    ordinary flow — whose subsequent binding re-inspection then compares two
    real post-migrate reads, never a vacuous self-comparison.
    """
    pending_migration = str(
        deferred.get("pending_migration") or _NARROW_SPLIT_MIGRATION_ID
    )
    migrations = _run_okg(
        ctx, ["db", "migrations", "--deployment", ctx.options.deployment]
    )
    if not migrations.ok:
        return _failed_command(
            "deferred recovery adoption could not read migration status",
            migrations,
        )
    status = migrations.json_payload()
    if not isinstance(status, dict):
        return _unreadable_command(
            "deferred recovery adoption migration status was unreadable",
            migrations,
        )
    if "pending" not in status or not isinstance(status["pending"], list):
        # Fail closed on the KEY's absence, not just on membership: a
        # migrations payload without a readable pending list must never
        # read as "the migration applied" (absence is not agreement).
        return {
            "ok": False,
            "reason": (
                "deferred recovery adoption could not read the pending "
                "migration list from the migration status"
            ),
            "status_keys": sorted(status.keys()),
            "deferred_classification": deferred,
        }
    still_pending = pending_migration in {
        str(row.get("migration_id") or "")
        for row in status["pending"]
        if isinstance(row, dict)
    }
    if still_pending:
        return {
            "ok": False,
            "reason": (
                "recovery classification was deferred at preflight for "
                f"pending migration {pending_migration}, and migrate did "
                "not apply it (skipped or failed); refusing to treat the "
                "deferral as a satisfied classification"
            ),
            "deferred_classification": deferred,
            "pending_migration": pending_migration,
            "fix_hint": (
                "re-run okg up without --skip-migrate so the migration "
                "applies through the multi-phase driver"
            ),
        }
    fresh_result = _run_okg(
        ctx,
        [
            "db",
            "inspect-effective-attribute-recovery",
            "--deployment",
            ctx.options.deployment,
        ],
    )
    if not fresh_result.ok:
        return _failed_command(
            "deferred recovery classification failed after migrate",
            fresh_result,
        )
    fresh = fresh_result.json_payload()
    if not isinstance(fresh, dict):
        return _unreadable_command(
            "deferred recovery classification was unreadable", fresh_result
        )
    if fresh.get("classification") not in {
        "no_published_generation",
        "no_repair",
        "ordinary_repair",
        "corrupt_state_recovery",
    } or not str(fresh.get("binding_digest") or "").startswith("sha256:"):
        return {
            "ok": False,
            "reason": (
                "deferred recovery classification returned an invalid "
                "payload after migrate"
            ),
            "result": fresh,
            "deferred_classification": deferred,
        }
    ctx.data["attribute_recovery_classification"] = fresh
    return {"ok": True, "classification": fresh}


def _phase_generation_recovery(ctx: UpgradeContext) -> dict[str, Any]:
    """Break a mis-stamped predecessor/read-model deadlock via one successor."""
    if ctx.options.plan_only:
        return {
            "ok": True,
            "planned": "classify and recover a corrupt published predecessor",
            "skipped": "generation recovery skipped (--plan)",
        }
    classification = ctx.data.get("attribute_recovery_classification")
    if not isinstance(classification, dict):
        return {
            "ok": False,
            "reason": "preflight did not supply an attribute recovery classification",
        }
    adopted_deferred = False
    if (
        classification.get("classification")
        == _DEFERRED_RECOVERY_CLASSIFICATION
    ):
        # phase-interval-narrow-split-migration (#1114): preflight recorded a
        # deferral because the recovery classifier needed the schema that
        # migration 0020 creates. Migrate has run by this phase — perform the
        # FIRST classification now and adopt it as the binding baseline. Fail
        # closed if the migration is still pending (--skip-migrate): the
        # deferred payload must never be treated as a satisfied
        # classification.
        adoption = _adopt_deferred_recovery_classification(ctx, classification)
        if adoption.get("ok") is not True:
            return adoption
        classification = adoption["classification"]
        adopted_deferred = True
    kind = classification.get("classification")
    known = {
        "no_published_generation",
        "no_repair",
        "ordinary_repair",
        "corrupt_state_recovery",
        "schema_migration_pending",
    }
    if kind not in known:
        return {
            "ok": False,
            "reason": "preflight returned an unknown recovery classification",
            "classification": classification,
        }
    deferred_from_preflight: dict[str, Any] | None = None
    if kind == "schema_migration_pending":
        # okg#1114: preflight could not classify at all -- the database's
        # attribute relations still predated the interval narrow split, so the
        # payload-parity fingerprint had no column to hash. Preflight recorded
        # that as a schema verdict rather than inventing a data one. The
        # migrate phase has since run, so take the real classification NOW and
        # continue on it; there is no earlier graph anchor to preserve because
        # preflight never bound one. If the schema is STILL pre-split here,
        # migrate did not do its job and this refuses rather than proceeding on
        # an unverifiable read model.
        deferred_result = _run_okg(
            ctx,
            [
                "db",
                "inspect-effective-attribute-recovery",
                "--deployment",
                ctx.options.deployment,
            ],
        )
        if not deferred_result.ok:
            return _failed_command(
                "post-migrate attribute recovery classification failed",
                deferred_result,
            )
        deferred = deferred_result.json_payload()
        if not isinstance(deferred, dict):
            return _unreadable_command(
                "could not read post-migrate attribute recovery classification",
                deferred_result,
            )
        deferred_kind = deferred.get("classification")
        if deferred_kind not in known or deferred_kind == "schema_migration_pending":
            return {
                "ok": False,
                "reason": (
                    "the migrate phase did not install the attribute payload "
                    "columns the recovery classification needs"
                ),
                "classification": classification,
                "observed": deferred,
            }
        if not str(deferred.get("binding_digest") or "").startswith("sha256:"):
            return {
                "ok": False,
                "reason": "post-migrate attribute recovery classification is unbound",
                "observed": deferred,
            }
        deferred_from_preflight = classification
        classification = deferred
        kind = deferred_kind
        ctx.data["attribute_recovery_classification"] = classification
    if kind != "corrupt_state_recovery":
        # Migrate/catalog phases are authorized to rebuild derived attribute
        # snapshots before this boundary. That can legitimately change the
        # recovery binding digest. The required production backup also leaves
        # the worker live, so the graph can advance before stop_services while
        # remaining an ordinary, count-exact same-branch successor. Re-inspect
        # after services stop and bind the no-op preparation to that complete
        # current identity. Corrupt-state recovery keeps its separate one-use
        # successor authorization below.
        current_result = _run_okg(
            ctx,
            [
                "db",
                "inspect-effective-attribute-recovery",
                "--deployment",
                ctx.options.deployment,
            ],
        )
        if not current_result.ok:
            return _failed_command(
                "attribute recovery no-op binding inspection failed",
                current_result,
            )
        current = current_result.json_payload()
        if not isinstance(current, dict):
            return _unreadable_command(
                "attribute recovery no-op binding inspection was unreadable",
                current_result,
            )
        current_kind = current.get("classification")
        graph_anchor_fields = (
            "deployment",
            "branch",
            "branch_id",
            "generation_id",
            "ordinal",
            "checkpoint_counts",
            "pinned_counts",
            "count_stamp_recovery",
        )
        graph_anchor_matches = all(
            current.get(field) == classification.get(field)
            for field in graph_anchor_fields
        )
        compatible_kind = (
            current_kind == kind
            or (
                kind in {"no_repair", "ordinary_repair"}
                and current_kind in {"no_repair", "ordinary_repair"}
            )
        )
        prior_ordinal = classification.get("ordinal")
        current_ordinal = current.get("ordinal")
        prior_generation = classification.get("generation_id")
        current_generation = current.get("generation_id")
        prior_checkpoint_counts = classification.get("checkpoint_counts")
        current_checkpoint_counts = current.get("checkpoint_counts")
        prior_pinned_counts = classification.get("pinned_counts")
        current_pinned_counts = current.get("pinned_counts")
        same_graph_scope = (
            classification.get("schema")
            == "okg.effective_attribute_recovery_classification.v1"
            and current.get("schema")
            == "okg.effective_attribute_recovery_classification.v1"
            and all(
                isinstance(classification.get(field), str)
                and bool(classification.get(field))
                and current.get(field) == classification.get(field)
                for field in ("deployment", "branch", "branch_id")
            )
        )
        monotonic_ordinary_successor = (
            kind in {"no_repair", "ordinary_repair"}
            and current_kind in {"no_repair", "ordinary_repair"}
            and same_graph_scope
            and isinstance(prior_generation, str)
            and bool(prior_generation)
            and isinstance(current_generation, str)
            and bool(current_generation)
            and current_generation != prior_generation
            and isinstance(prior_ordinal, int)
            and not isinstance(prior_ordinal, bool)
            and isinstance(current_ordinal, int)
            and not isinstance(current_ordinal, bool)
            and current_ordinal > prior_ordinal
            and isinstance(prior_checkpoint_counts, dict)
            and prior_checkpoint_counts == prior_pinned_counts
            and isinstance(current_checkpoint_counts, dict)
            and current_checkpoint_counts == current_pinned_counts
            and "count_stamp_recovery" in classification
            and classification.get("count_stamp_recovery") is None
            and "count_stamp_recovery" in current
            and current.get("count_stamp_recovery") is None
            and str(current.get("binding_digest") or "").startswith("sha256:")
        )
        if not compatible_kind or (
            not graph_anchor_matches and not monotonic_ordinary_successor
        ):
            return {
                "ok": False,
                "reason": "attribute recovery graph anchor changed after preflight",
                "classification": classification,
                "observed": current,
                "graph_anchor_matches": graph_anchor_matches,
                "compatible_classification": compatible_kind,
                "same_graph_scope": same_graph_scope,
                "monotonic_ordinary_successor": monotonic_ordinary_successor,
            }
        revalidated_result = _run_okg(
            ctx,
            [
                "db",
                "prepare-effective-attribute-recovery",
                "--deployment",
                ctx.options.deployment,
                "--expected-binding-digest",
                str(current.get("binding_digest") or ""),
                "--expected-classification",
                json.dumps(current, sort_keys=True, separators=(",", ":")),
                "--apply",
            ],
        )
        if not revalidated_result.ok:
            return _failed_command(
                "attribute recovery no-op binding revalidation failed",
                revalidated_result,
            )
        revalidated = revalidated_result.json_payload()
        if not isinstance(revalidated, dict) or revalidated.get("prepared") is not False:
            return _unreadable_command(
                "attribute recovery no-op revalidation returned an invalid receipt",
                revalidated_result,
            )
        ctx.data["attribute_recovery_classification"] = current
        return {
            "ok": True,
            "classification": classification,
            "revalidated_classification": revalidated.get("classification"),
            "binding_rebased": (
                current.get("binding_digest") != classification.get("binding_digest")
            ),
            "graph_anchor_rebased": not graph_anchor_matches,
            "monotonic_ordinary_successor": monotonic_ordinary_successor,
            "adopted_deferred_preflight_classification": adopted_deferred,
            "recovered": False,
            **(
                {"deferred_preflight_classification": deferred_from_preflight}
                if deferred_from_preflight is not None
                else {}
            ),
        }
    current_result = _run_okg(
        ctx,
        [
            "db",
            "inspect-effective-attribute-recovery",
            "--deployment",
            ctx.options.deployment,
        ],
    )
    if not current_result.ok:
        return _failed_command("recovery binding revalidation failed", current_result)
    current = current_result.json_payload()
    if not isinstance(current, dict):
        return _unreadable_command("could not read recovery binding revalidation", current_result)
    current_receipt = current.get("count_stamp_recovery")
    if current.get("generation_id") != classification.get("generation_id"):
        if isinstance(current_receipt, dict) and current_receipt.get(
            "classification_binding_digest"
        ) == classification.get("binding_digest"):
            return {
                "ok": True,
                "recovered": True,
                "resumed_after_successor_publish": True,
                "classification": classification,
                "successor_generation": current.get("generation_id"),
                "successor_classification": current,
            }
        return {
            "ok": False,
            "reason": "published predecessor changed after recovery preflight",
            "classification": classification,
            "observed": current,
        }
    prepared_result = _run_okg(
        ctx,
        [
            "db",
            "prepare-effective-attribute-recovery",
            "--deployment",
            ctx.options.deployment,
            "--expected-binding-digest",
            str(classification.get("binding_digest") or ""),
            "--expected-classification",
            json.dumps(classification, sort_keys=True, separators=(",", ":")),
            "--apply",
        ],
    )
    if not prepared_result.ok:
        return _failed_command("corrupt-state recovery preparation failed", prepared_result)
    prepared = prepared_result.json_payload()
    if not isinstance(prepared, dict):
        return _unreadable_command(
            "could not read corrupt-state recovery preparation", prepared_result
        )
    authorization = prepared.get("authorization")
    if prepared.get("prepared") is not True or not isinstance(authorization, dict):
        return {
            "ok": False,
            "reason": "corrupt-state preparation did not issue an authorization",
            "result": prepared,
        }
    publish_result = _run_okg(
        ctx,
        _deployment_publish_command(ctx),
        env_overrides={
            "OKG_COUNT_STAMP_RECOVERY_AUTHORIZATION": json.dumps(
                authorization, sort_keys=True, separators=(",", ":")
            )
        },
    )
    if not publish_result.ok:
        return _failed_command("recovery successor publish failed", publish_result)
    published = publish_result.json_payload()
    if not isinstance(published, dict):
        return _unreadable_command("could not read recovery successor publish", publish_result)
    successor = _smoke_generation(published)
    statuses = [
        row.get("status")
        for row in published.get("publishes", [])
        if isinstance(row, dict)
    ]
    if successor is None or (statuses and statuses[-1] != "published"):
        return {
            "ok": False,
            "reason": "recovery successor was not published",
            "result": published,
        }
    if _GENERATION_RECOVERY_FAILPOINT is not None:
        _GENERATION_RECOVERY_FAILPOINT("after_successor_publish")
    described_result = _run_okg(
        ctx,
        [
            "generation",
            "describe",
            str(successor),
            "--deployment",
            ctx.options.deployment,
        ],
    )
    if not described_result.ok:
        return _failed_command("recovery successor describe failed", described_result)
    described = described_result.json_payload()
    if not isinstance(described, dict):
        return _unreadable_command("could not read recovery successor", described_result)
    metadata = described.get("metadata") if isinstance(described.get("metadata"), dict) else described
    recovery_receipt = metadata.get("count_stamp_recovery") if isinstance(metadata, dict) else None
    if not isinstance(recovery_receipt, dict) or recovery_receipt.get(
        "authorization_digest"
    ) != authorization.get("authorization_digest"):
        return {
            "ok": False,
            "reason": "successor does not carry the exact recovery authorization receipt",
            "generation": described,
        }
    return {
        "ok": True,
        "recovered": True,
        "classification": classification,
        "preparation": prepared,
        "successor_publish": published,
        "successor_generation": successor,
        "successor_describe": described,
        "adopted_deferred_preflight_classification": adopted_deferred,
        **(
            {"deferred_preflight_classification": deferred_from_preflight}
            if deferred_from_preflight is not None
            else {}
        ),
    }


def _phase_source_convergence(ctx: UpgradeContext) -> dict[str, Any]:
    """Execute the readiness surface's own source recovery plan to convergence."""
    if ctx.options.plan_only:
        return {
            "ok": True,
            "planned": "execute readiness-required source recovery to convergence",
            "skipped": "source convergence skipped (--plan)",
        }
    action_timeout = _positive_env_float(
        ctx, "OKG_DEPLOYMENT_SOURCE_ACTION_TIMEOUT_SECONDS", 1800.0
    )
    global_timeout = _positive_env_float(
        ctx, "OKG_DEPLOYMENT_SOURCE_CONVERGENCE_TIMEOUT_SECONDS", 3600.0
    )
    poll_interval = _positive_env_float(
        ctx, "OKG_DEPLOYMENT_SOURCE_POLL_SECONDS", 2.0
    )
    now_epoch = time.time()
    raw_global_deadline = ctx.data.get("source_convergence_deadline_epoch")
    try:
        global_deadline_epoch = float(raw_global_deadline)
    except (TypeError, ValueError):
        global_deadline_epoch = now_epoch + global_timeout
        _persist_source_convergence_deadline(ctx, global_deadline_epoch)
    workflows: list[dict[str, Any]] = list(
        ctx.data.get("source_recovery_workflows") or ()
    )
    completed_sources = {
        str(row.get("source_name"))
        for row in workflows
        if row.get("terminal_status") == "SUCCESS"
    }
    observations: list[dict[str, Any]] = []
    for _iteration in range(100):
        # renamed-flag compat: the child may run pinned pre-rename code.
        ready_result = _run_okg(
            ctx,
            [
                "deployment",
                "ready",
                ctx.options.deployment,
                COMPAT_READY_CHECKS_FLAG,
                ctx.options.readiness_profile,
            ],
            env_overrides=RENAMED_FLAG_COMPAT_ENV,
        )
        ready = ready_result.json_payload()
        if not isinstance(ready, dict):
            return _unreadable_command("could not read source convergence status", ready_result)
        convergence = _source_convergence_from_readiness(ready)
        observations.append(convergence)
        if (
            convergence.get("ok") is True
            and convergence.get("state") == "ready"
        ):
            return {
                "ok": True,
                "state": "ready",
                "workflows": workflows,
                "observations": observations,
                "readiness": ready,
            }
        if time.time() >= global_deadline_epoch:
            return {
                "ok": False,
                "reason": "source convergence exceeded the global deployment deadline",
                "workflows": workflows,
                "observations": observations,
            }
        actions = [
            dict(row)
            for row in (convergence.get("recovery_plan") or {}).get("actions", [])
            if isinstance(row, dict)
        ]
        if not actions:
            return {
                "ok": False,
                "reason": "readiness is blocked but supplied no executable source recovery action",
                "blocking_stage": (convergence.get("next_action") or {}).get("blocking_stage"),
                "readiness": ready,
            }
        action = actions[0]
        source = str(action.get("source_name") or "")
        kind = str(action.get("action_kind") or "")
        if kind in {"manual_or_credential_action", "unsupported"}:
            return {
                "ok": False,
                "reason": "source recovery requires non-automated intervention",
                "source_name": source,
                "blocking_stage": "planned_action",
                "action": action,
            }
        if source in completed_sources:
            return {
                "ok": False,
                "reason": "source workflow succeeded but its readiness watermark did not move",
                "source_name": source,
                "blocking_stage": "completed_scope_watermark",
                "action": action,
            }
        existing = next(
            (
                row for row in reversed(workflows)
                if row.get("source_name") == source
                and row.get("terminal_status") is None
            ),
            None,
        )
        workflow_id = str((existing or {}).get("workflow_id") or "")
        observe_kinds = {
            "observe_active_completed_scope_recovery",
            "observe_active_source_progress_recovery",
            "restart_started_workflow",
        }
        if not workflow_id and kind in observe_kinds:
            workflow_id = str(action.get("workflow_id") or "")
            if not workflow_id:
                return {
                    "ok": False,
                    "reason": "readiness observe action omitted workflow identity",
                    "source_name": source,
                    "blocking_stage": "runtime_orchestration",
                    "action": action,
                }
            if kind == "restart_started_workflow":
                restarted = _run_okg(
                    ctx,
                    [
                        "dogfood",
                        "restart",
                        "--deployment",
                        ctx.options.deployment,
                        "--apply",
                    ],
                )
                if not restarted.ok:
                    return _failed_command(
                        f"source recovery worker restart failed for {source}",
                        restarted,
                    )
            existing = {
                "source_name": source,
                "action_kind": kind,
                "run_mode": str(action.get("run_mode") or ""),
                "run_key": None,
                "workflow_id": workflow_id,
                "supersedes_workflow_id": None,
                "enqueued_at": None,
                "observed_at": _now(),
                "terminal_status": None,
                "deadline_epoch": min(time.time() + action_timeout, global_deadline_epoch),
            }
            workflows.append(existing)
            _persist_source_recovery_workflows(ctx, workflows)
        if not workflow_id:
            if kind not in {
                "completed_scope_reconcile",
                "source_progress_reconcile",
                "requeue_protected_workflow",
                "cancel_replaceable_workflow",
            }:
                return {
                    "ok": False,
                    "reason": "readiness recovery action is not safely automatable",
                    "source_name": source,
                    "blocking_stage": "runtime_orchestration",
                    "action": action,
                }
            run_mode = str(action.get("run_mode") or "")
            if not run_mode:
                return {"ok": False, "reason": "source recovery action omitted run_mode", "action": action}
            run_key = str(
                (existing or {}).get("run_key")
                or f"deploy-recovery:{_target_key_digest(ctx)}:{source}"
            )
            action_scope = _recovery_action_scope(action)
            if len(action_scope) > 1:
                return {
                    "ok": False,
                    "reason": "source recovery action has unsupported multi-scope work",
                    "source_name": source,
                    "scope": action_scope,
                }
            if kind == "cancel_replaceable_workflow":
                from okg.substrate.runtime.partition_queue import (
                    source_sync_queue_name,
                )

                cancelled = _run_okg(
                    ctx,
                    [
                        "db", "dbos-gc",
                        "--deployment", ctx.options.deployment,
                        "--stale-after-seconds", str(
                            int(action.get("stale_after_seconds") or 600)
                        ),
                        "--status", "ENQUEUED",
                        "--queue-name", source_sync_queue_name(
                            ctx.options.deployment
                        ),
                        "--workflow-name", "okg_source_sync",
                        "--confirm", "--apply",
                    ],
                )
                if not cancelled.ok:
                    return _failed_command(
                        f"replaceable source workflow cancellation failed for {source}",
                        cancelled,
                    )
            if existing is None:
                supersedes = _matching_current_failure(
                    ctx,
                    source=source,
                    run_mode=run_mode,
                    scope=action_scope,
                )
                existing = {
                    "source_name": source,
                    "action_kind": kind,
                    "run_mode": run_mode,
                    "run_key": run_key,
                    "workflow_id": None,
                    "supersedes_workflow_id": supersedes,
                    "enqueued_at": None,
                    "terminal_status": None,
                    "deadline_epoch": min(
                        time.time() + action_timeout, global_deadline_epoch
                    ),
                }
                workflows.append(existing)
                # Persist intent before enqueue. If the process dies after DBOS
                # accepts work but before its response is recorded, resume can
                # resolve this same run key instead of creating duplicate work.
                _persist_source_recovery_workflows(ctx, workflows)
            supersedes = existing.get("supersedes_workflow_id")
            found_result = _run_okg(
                ctx,
                [
                    "jobs",
                    "find-source-run",
                    "--deployment",
                    ctx.options.deployment,
                    "--runtime-deployment",
                    ctx.options.deployment,
                    "--run-key",
                    run_key,
                ],
            )
            if not found_result.ok:
                return _failed_command(
                    f"source recovery identity lookup failed for {source}",
                    found_result,
                )
            found = found_result.json_payload()
            if not isinstance(found, dict):
                return _unreadable_command(
                    "could not read source recovery identity lookup", found_result
                )
            workflow_id = str(found.get("dbos_workflow_id") or "")
            if not workflow_id:
                enqueue_cmd = [
                    "jobs", "enqueue-source",
                    "--deployment", ctx.options.deployment,
                    "--runtime-deployment", ctx.options.deployment,
                    "--source", source,
                    "--run-mode", run_mode,
                    "--run-key", run_key,
                    "--apply",
                ]
                promotion_id = ctx.data.get("runtime_fence_owner")
                if promotion_id:
                    enqueue_cmd.extend(["--promotion-id", str(promotion_id)])
                if action_scope:
                    enqueue_cmd.extend(["--scope-key", action_scope[0]])
                if supersedes:
                    enqueue_cmd.extend(["--supersedes-workflow-id", str(supersedes)])
                enqueued_result = _run_okg(ctx, enqueue_cmd)
                if not enqueued_result.ok:
                    return _failed_command(
                        f"source recovery enqueue failed for {source}", enqueued_result
                    )
                enqueued = enqueued_result.json_payload()
                if not isinstance(enqueued, dict) or not enqueued.get("dbos_workflow_id"):
                    return _unreadable_command(
                        "could not read source recovery workflow identity",
                        enqueued_result,
                    )
                workflow_id = str(enqueued["dbos_workflow_id"])
                existing["enqueued_at"] = _now()
            existing["workflow_id"] = workflow_id
            _persist_source_recovery_workflows(ctx, workflows)
        assert existing is not None
        try:
            action_deadline_epoch = float(existing.get("deadline_epoch"))
        except (TypeError, ValueError):
            action_deadline_epoch = min(
                time.time() + action_timeout, global_deadline_epoch
            )
            existing["deadline_epoch"] = action_deadline_epoch
            _persist_source_recovery_workflows(ctx, workflows)
        remaining_seconds = min(
            action_deadline_epoch, global_deadline_epoch
        ) - time.time()
        if remaining_seconds <= 0:
            return {
                "ok": False,
                "reason": "source recovery exceeded its durable deadline",
                "source_name": source,
                "workflow_id": workflow_id,
            }
        terminal = _wait_for_source_recovery_workflow(
            ctx,
            workflow_id=workflow_id,
            source=source,
            timeout_seconds=remaining_seconds,
            poll_interval_seconds=poll_interval,
        )
        if terminal.get("ok") is not True:
            return {**terminal, "workflows": workflows}
        existing["terminal_status"] = "SUCCESS"
        existing["terminal_at"] = _now()
        existing["terminal_report"] = terminal.get("report")
        completed_sources.add(source)
        _persist_source_recovery_workflows(ctx, workflows)
    return {
        "ok": False,
        "reason": "source convergence exceeded its bounded dynamic replan count",
        "workflows": workflows,
    }


def _source_convergence_from_readiness(ready: dict[str, Any]) -> dict[str, Any]:
    source_sync = ready.get("source_sync_status")
    if isinstance(source_sync, dict) and isinstance(source_sync.get("convergence"), dict):
        return dict(source_sync["convergence"])
    direct = ready.get("source_sync_convergence")
    if isinstance(direct, dict):
        return dict(direct)
    # General readiness profiles intentionally do not require the CI/dogfood
    # source-group convergence contract.  Their canonical top-level verdict is
    # still authoritative: only a clean result with no blockers may satisfy
    # this phase when there is no more-specific convergence document.
    if ready.get("ok") is True and not ready.get("blockers"):
        return {
            "ok": True,
            "state": "ready",
            "authority": "clean_deployment_readiness",
            "recovery_plan": {"actions": []},
        }
    return {
        "ok": False,
        "state": "unavailable",
        "recovery_plan": {"actions": []},
        "next_action": {"blocking_stage": "readiness_evidence"},
    }


def _positive_env_float(ctx: UpgradeContext, name: str, default: float) -> float:
    try:
        value = float(ctx.env.get(name, default))
    except (TypeError, ValueError):
        value = -1.0
    if value <= 0:
        raise SubstrateError(f"{name} must be a positive number")
    return value


def _declared_dbos_schema(runtime_raw: object) -> str:
    """Validate ``runtime.dbos_schema`` when the upgrade context is built.

    The upgrade reads the raw manifest, not a parsed ``RuntimeConfig``, so it
    applies the same rule here: an unsupported schema refuses the upgrade up
    front instead of steering the bootstrap probe at a schema DBOS never uses.
    """
    from okg.substrate.runtime.config import require_supported_dbos_schema

    declared = (
        runtime_raw.get("dbos_schema") if isinstance(runtime_raw, dict) else None
    )
    try:
        return require_supported_dbos_schema(declared)
    except ValueError as exc:
        raise SubstrateError(
            str(exc),
            data={"runtime_dbos_schema": declared},
            fix_hint="remove runtime.dbos_schema from deployment.yaml",
        ) from exc


def _runtime_dbos_schema(ctx: UpgradeContext) -> str:
    from okg.substrate.runtime.config import DBOS_SYSTEM_SCHEMA

    data = getattr(ctx, "data", None)
    declared = (
        data.get("runtime_dbos_schema") if isinstance(data, Mapping) else None
    )
    if declared is None:
        return DBOS_SYSTEM_SCHEMA
    if declared != DBOS_SYSTEM_SCHEMA:
        raise SubstrateError(
            f"upgrade context carries unsupported dbos schema {declared!r}",
            data={"runtime_dbos_schema": declared},
            fix_hint="remove runtime.dbos_schema from deployment.yaml",
        )
    return declared


def _probe_runtime_bootstrapped(ctx: UpgradeContext) -> bool:
    """Is the DBOS workflow table present in the target database?

    ``to_regclass`` answers with NULL rather than raising, so an absent
    schema is an answer and not an error. Any other database fault fails
    closed (reported as bootstrapped) so the quiescence path still runs and
    refuses on the real error instead of skipping the fence silently.
    """
    from okg.substrate.db.connection import connect as db_connect

    # Resolved outside the try: an unsupported schema is a refusal, not a
    # database fault to fail closed on.
    relation = f'"{_runtime_dbos_schema(ctx)}".workflow_status'
    try:
        with db_connect(ctx.dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT to_regclass(%s) IS NOT NULL",
                    (relation,),
                )
                row = cur.fetchone()
    except Exception:  # noqa: BLE001 - fail closed; the fence path reports it.
        return True
    return bool(row and row[0])


def _runtime_bootstrapped(ctx: UpgradeContext) -> bool:
    """Whether this deployment's runtime has ever been bootstrapped.

    A database that was migrated but never runtime-bootstrapped has no
    ``dbos.workflow_status``; every queue read against it raises
    UndefinedTable (2026-09-03: `okg up` exited 1 with a raw traceback out
    of resume revalidation). Quiescence over a queue that cannot exist is
    not a safety property.

    Cached for the operation: `okg up` itself bootstraps the runtime when it
    restarts services, and a quiescence requirement that flipped mid-run
    would have readiness release a fence that stop never acquired.
    """
    data = getattr(ctx, "data", None)
    cached = data.get("runtime_bootstrapped") if isinstance(data, Mapping) else None
    if isinstance(cached, bool):
        return cached
    observed = _probe_runtime_bootstrapped(ctx)
    if isinstance(data, dict):
        data["runtime_bootstrapped"] = observed
    return observed


def _runtime_quiescence_disposition(ctx: UpgradeContext) -> str:
    """``required`` | ``not_applicable`` | ``not_bootstrapped``."""
    if (
        bool(getattr(ctx.options, "plan_only", False))
        or not bool(getattr(ctx.options, "apply_services", True))
        or bool(getattr(ctx.options, "independent_backup_verify", False))
    ):
        return "not_applicable"
    # Direct phase unit tests use a deliberately partial context without a
    # database authority. Full deployment contexts always carry a resolved
    # DSN before any phase runs.
    if not _runtime_fence_authority_available(ctx):
        return "not_applicable"
    if not _runtime_bootstrapped(ctx):
        return "not_bootstrapped"
    return "required"


def _runtime_quiescence_required(ctx: UpgradeContext) -> bool:
    return _runtime_quiescence_disposition(ctx) == "required"


def _runtime_not_bootstrapped_report(ctx: UpgradeContext) -> dict[str, Any] | None:
    """The report line for a runtime target with no DBOS schema, else None."""
    if _runtime_quiescence_disposition(ctx) != "not_bootstrapped":
        return None
    deployment = getattr(ctx.options, "deployment", "")
    return {
        "schema": "okg.deployment_runtime_quiescence.v1",
        "status": "not_bootstrapped",
        "dbos_schema": _runtime_dbos_schema(ctx),
        "detail": (
            "the target database has no DBOS workflow table, so it carries "
            "no runtime queue to drain and no worker to fence"
        ),
        "fix_hint": (
            "run `okg runtime bootstrap --deployment "
            f"{deployment} --json` before expecting runtime quiescence"
        ),
    }


def _runtime_fence_authority_available(ctx: UpgradeContext) -> bool:
    """Whether this target can carry a durable runtime fence.

    Unlike mutation admission, reset safety is independent of plan-only,
    service-application, and backup-verification flags.
    """
    return bool(
        getattr(ctx, "dsn", None)
        and ctx.data.get("runtime_enabled") is True
    )


def _runtime_fence_metadata(ctx: UpgradeContext) -> dict[str, Any]:
    target = _target_state_key(ctx)
    return {
        "schema": "okg.deployment_runtime_quiescence.v1",
        "deployment": ctx.options.deployment,
        "candidate_revision": target.get("code_revision"),
        "target_key_digest": _target_key_digest(ctx),
        "lifecycle_contract_digest": CANONICAL_CONTRACT.digest,
        "lifecycle_plan_digest": target.get("lifecycle_plan_digest"),
        "bootstrap_quiescence": bool(
            ctx.data.get("adopted_bootstrap_quiescence")
        ),
    }


def _runtime_fence_binding_checks(
    ctx: UpgradeContext,
    status: Mapping[str, Any],
) -> dict[str, bool]:
    metadata = status.get("metadata")
    metadata = metadata if isinstance(metadata, Mapping) else {}
    expected = _runtime_fence_metadata(ctx)
    return {
        "schema": metadata.get("schema") == expected["schema"],
        "deployment": metadata.get("deployment") == expected["deployment"],
        "candidate_revision": (
            metadata.get("candidate_revision") == expected["candidate_revision"]
        ),
        "lifecycle_contract_digest": (
            metadata.get("lifecycle_contract_digest")
            == expected["lifecycle_contract_digest"]
        ),
        "lifecycle_plan_digest": (
            metadata.get("lifecycle_plan_digest")
            == expected["lifecycle_plan_digest"]
        ),
    }


def _runtime_schedule_snapshot_digest(snapshot: Any) -> str:
    encoded = json.dumps(
        snapshot,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _revalidate_runtime_fence_resume(
    ctx: UpgradeContext,
    *,
    completed: set[str],
) -> dict[str, Any]:
    from okg.substrate.runtime.admin import client_drain
    from okg.substrate.runtime.promotion_fence import (
        mark_promotion_recovery_blocked,
        promotion_fence_status,
    )

    owner = str(ctx.data.get("runtime_fence_owner") or "")
    expected_snapshot_digest = str(
        ctx.data.get("runtime_fence_snapshot_digest") or ""
    )
    status = promotion_fence_status(
        deployment=ctx.options.deployment,
        dsn=ctx.dsn,
    )
    status = status if isinstance(status, dict) else {}
    binding = _runtime_fence_binding_checks(ctx, status)
    checks: dict[str, bool] = {
        "owner": bool(owner) and status.get("promotion_id") == owner,
        **{f"binding_{key}": value for key, value in binding.items()},
    }
    readiness_completed = "readiness" in completed
    released_before_phase_persistence = (
        status.get("status") == "released" and not readiness_completed
    )
    queue_observation: dict[str, Any] | None = None
    if readiness_completed or released_before_phase_persistence:
        release_receipt = (
            (status.get("metadata") or {}).get("release_receipt")
            if isinstance(status.get("metadata"), Mapping)
            else None
        )
        release_receipt = (
            release_receipt if isinstance(release_receipt, Mapping) else {}
        )
        checks.update({
            "status_released": status.get("status") == "released",
            "durable_release_receipt": (
                release_receipt.get("durable_release_receipt") is True
            ),
            "release_owner": release_receipt.get("promotion_id") == owner,
            "snapshot_digest": (
                bool(expected_snapshot_digest)
                and release_receipt.get("schedule_snapshot_digest")
                == expected_snapshot_digest
            ),
        })
    else:
        observed_snapshot_digest = _runtime_schedule_snapshot_digest(
            status.get("schedule_snapshot") or []
        )
        # An instantaneous observation, not a drain wait: this revalidates
        # that the queue is still owned by the resuming operation. A settle
        # window is meaningless with max_wait_seconds=0.0 (the deadline fires
        # before any window could elapse), so state the 0.0 explicitly.
        from okg.substrate.runtime.admin import DrainResult

        drain_error: dict[str, str] | None = None
        try:
            drain = client_drain(
                deployment=ctx.options.deployment,
                dsn=ctx.dsn,
                max_wait_seconds=0.0,
                poll_interval_seconds=0.01,
                stable_zero_seconds=0.0,
            )
        except Exception as drain_exc:  # noqa: BLE001
            # A queue read that raises proves nothing about the queue, so it
            # fails closed through the fence blocker below (a structured
            # refusal) rather than escaping the CLI as a traceback.
            drain_error = {
                "type": type(drain_exc).__name__,
                "message": str(drain_exc),
            }
            drain = DrainResult(
                deployment=ctx.options.deployment,
                drained=False,
                in_flight_at_timeout=0,
                elapsed_seconds=0.0,
                pending_workflows=[],
            )
        queue_observation = {
            "drained": drain.drained,
            "in_flight": drain.in_flight_at_timeout,
            "pending_workflows": drain.pending_workflows,
            **({"drain_error": drain_error} if drain_error is not None else {}),
        }
        allowed_workflow_ids = {
            str(row.get("workflow_id"))
            for row in ctx.data.get("source_recovery_workflows") or ()
            if isinstance(row, Mapping) and row.get("workflow_id")
        }
        witness = ctx.data.get("runtime_readiness_witness")
        witness = witness if isinstance(witness, Mapping) else {}
        expected_witness_deduplication_id = (
            _runtime_readiness_witness_deduplication_id(ctx, owner=owner)
            if owner
            else ""
        )
        witness_authorized = not witness or (
            witness.get("promotion_id") == owner
            and witness.get("deduplication_id")
            == expected_witness_deduplication_id
        )
        checks["runtime_readiness_witness_authority"] = witness_authorized
        if witness_authorized and witness.get("workflow_id"):
            allowed_workflow_ids.add(str(witness["workflow_id"]))
        witness_deduplication_id = str(
            witness.get("deduplication_id") or ""
            if witness_authorized
            else ""
        )
        pending_rows = [
            row for row in drain.pending_workflows
            if isinstance(row, Mapping) and row.get("workflow_id")
        ]
        pending_ids = {str(row["workflow_id"]) for row in pending_rows}
        pending_rows_owned = all(
            str(row["workflow_id"]) in allowed_workflow_ids
            or (
                bool(witness_deduplication_id)
                and row.get("name") == "doctor_workflow"
                and row.get("deduplication_id")
                == witness_deduplication_id
            )
            for row in pending_rows
        )
        queue_owned = drain.drained or (
            bool(pending_ids)
            and pending_rows_owned
            and len(pending_rows) == drain.in_flight_at_timeout
        )
        checks.update({
            "status_active": status.get("status") == "fenced",
            "snapshot_digest": (
                bool(expected_snapshot_digest)
                and observed_snapshot_digest == expected_snapshot_digest
            ),
            "queue_drained_or_owner_bound": queue_owned,
        })
    if all(checks.values()):
        return {
            "ok": True,
            "owner": owner,
            "status": status.get("status"),
            "released_before_phase_persistence": (
                released_before_phase_persistence
            ),
            "checks": checks,
            **(
                {"queue": queue_observation}
                if queue_observation is not None
                else {}
            ),
        }

    mutation_started = any(
        phase in completed
        for phase in PHASES[PHASES.index("stop_services") :]
    )
    blocker = {
        "schema": "okg.deployment_runtime_fence_recovery_blocked.v1",
        "owner": owner,
        "status": status.get("status"),
        "checks": checks,
        "queue": queue_observation,
        "observed_at": _now(),
    }
    if (
        mutation_started
        and status.get("promotion_id") == owner
        and status.get("status") != "released"
    ):
        try:
            blocker["fence"] = mark_promotion_recovery_blocked(
                deployment=ctx.options.deployment,
                dsn=ctx.dsn,
                promotion_id=owner,
                blocker=blocker,
            )
        except Exception as exc:  # noqa: BLE001
            blocker["mark_recovery_blocked_error"] = str(exc)
    raise SubstrateError(
        (
            "runtime fence recovery is blocked after deployment mutation"
            if mutation_started
            else "runtime fence resume authority is invalid"
        ),
        data=blocker,
        fix_hint=(
            "inspect the live fence, queue identities, and persisted lifecycle "
            "receipt; do not reset or replay mutation"
        ),
    )


def _initialize_runtime_fence_owner(
    ctx: UpgradeContext,
    state: dict[str, Any],
) -> str:
    from okg.substrate.runtime.promotion_fence import promotion_fence_status

    stored = state.get("runtime_fence_owner")
    requested = ctx.options.runtime_fence_owner
    if stored:
        if requested and requested != stored:
            raise SubstrateError(
                "runtime fence owner changed across deployment resume",
                data={"stored_owner": stored, "requested_owner": requested},
            )
        return str(stored)

    status = promotion_fence_status(
        deployment=ctx.options.deployment,
        dsn=ctx.dsn,
    )
    active = (
        isinstance(status, dict)
        and status.get("status") != "released"
    )
    if requested:
        if active and status.get("promotion_id") != requested:
            raise SubstrateError(
                "runtime fence is owned by a different deployment operation",
                data={
                    "requested_owner": requested,
                    "owning_promotion_id": status.get("promotion_id"),
                    "fence_status": status.get("status"),
                },
            )
        if active:
            checks = _runtime_fence_binding_checks(ctx, status)
            if not all(checks.values()):
                raise SubstrateError(
                    "controller runtime fence binding does not match the candidate",
                    data={
                        "fence_owner": requested,
                        "fence_status": status.get("status"),
                        "checks": checks,
                    },
                )
        return requested
    if active:
        metadata = status.get("metadata")
        metadata = metadata if isinstance(metadata, dict) else {}
        checks = {
            "bootstrap_quiescence": (
                metadata.get("bootstrap_quiescence") is True
            ),
            **_runtime_fence_binding_checks(ctx, status),
        }
        if not all(checks.values()):
            raise SubstrateError(
                "active runtime fence is not an exact candidate-bound bootstrap",
                data={
                    "fence_owner": status.get("promotion_id"),
                    "fence_status": status.get("status"),
                    "checks": checks,
                },
                fix_hint="resume or recover the operation that owns the fence",
            )
        ctx.data["adopted_bootstrap_quiescence"] = True
        return str(status["promotion_id"])
    return f"deployment-up:{uuid4().hex}"


def _persist_runtime_fence_state(
    ctx: UpgradeContext,
    *,
    owner: str,
    schedule_snapshot_digest: str,
) -> None:
    state = (
        _load_state_file(ctx.state_file)
        if ctx.state_file.is_file()
        else {}
    )
    state["runtime_fence_owner"] = owner
    state["runtime_fence_snapshot_digest"] = schedule_snapshot_digest
    state["updated_at"] = _now()
    _write_state(ctx.state_file, state)


def _target_key_digest(ctx: UpgradeContext) -> str:
    return hashlib.sha256(
        json.dumps(_target_state_key(ctx), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:20]


def _matching_current_failure(
    ctx: UpgradeContext,
    *,
    source: str,
    run_mode: str,
    scope: list[str],
) -> str | None:
    result = _run_okg(
        ctx,
        ["db", "current-runtime-dlq", "--deployment", ctx.options.deployment],
    )
    if not result.ok:
        raise SubstrateError(
            "current DLQ authority is unavailable before source recovery enqueue"
        )
    payload = result.json_payload()
    if not isinstance(payload, dict):
        raise SubstrateError("current DLQ authority returned unreadable output")
    matches = []
    for failure in payload.get("current_failures") or ():
        if not isinstance(failure, dict):
            continue
        responsibility = failure.get("responsibility") or {}
        expected_responsibility = {
            "deployment": ctx.options.deployment,
            "sources": [source],
            "workflow_type": "okg_source_sync",
            "application_version": payload.get("current_application_version"),
            "run_mode": run_mode,
            "scope": scope,
        }
        if (
            expected_responsibility["application_version"] is not None
            and responsibility == expected_responsibility
        ):
            matches.append(str(failure.get("workflow_id") or ""))
    matches = [workflow_id for workflow_id in matches if workflow_id]
    if len(matches) > 1:
        raise SubstrateError(
            "multiple current source-sync failures share one recovery responsibility",
            data={"source_name": source, "run_mode": run_mode, "workflow_ids": matches},
        )
    return matches[0] if matches else None


def _recovery_action_scope(action: Mapping[str, Any]) -> list[str]:
    raw = action.get("scope")
    if isinstance(raw, list):
        return sorted(str(value) for value in raw if value)
    scope_key = action.get("scope_key")
    return [str(scope_key)] if scope_key else []


def _persist_source_convergence_deadline(
    ctx: UpgradeContext,
    deadline_epoch: float,
) -> None:
    state = redact_value(_load_state_file(ctx.state_file))
    state["source_convergence_deadline_epoch"] = deadline_epoch
    state["updated_at"] = _now()
    _write_state(ctx.state_file, state)
    ctx.data["source_convergence_deadline_epoch"] = deadline_epoch


def _persist_source_recovery_workflows(
    ctx: UpgradeContext,
    workflows: list[dict[str, Any]],
) -> None:
    state = redact_value(_load_state_file(ctx.state_file))
    state["source_recovery_workflows"] = redact_value(workflows)
    state["updated_at"] = _now()
    _write_state(ctx.state_file, state)
    ctx.data["source_recovery_workflows"] = workflows


def _persist_runtime_readiness_witness(
    ctx: UpgradeContext,
    witness: Mapping[str, Any],
) -> None:
    state = redact_value(_load_state_file(ctx.state_file))
    persisted = redact_value(dict(witness))
    state["runtime_readiness_witness"] = persisted
    state["updated_at"] = _now()
    _write_state(ctx.state_file, state)
    ctx.data["runtime_readiness_witness"] = dict(witness)


def _merge_phase_durable_state(
    ctx: UpgradeContext,
    state: dict[str, Any],
) -> None:
    if not ctx.state_file.is_file():
        return
    persisted_during_phase = redact_value(_load_state_file(ctx.state_file))
    for durable_key in (
        "source_recovery_workflows",
        "source_convergence_deadline_epoch",
        "runtime_fence_owner",
        "runtime_fence_snapshot_digest",
        "runtime_readiness_witness",
        "cleanup_obligations",
        "phase_fingerprints",
        "phase_terminal_receipts",
    ):
        if durable_key in persisted_during_phase:
            state[durable_key] = persisted_during_phase[durable_key]


def _load_state_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise SubstrateError(
            "deployment operation state is unavailable while persisting source recovery"
        ) from exc
    if not isinstance(payload, dict):
        raise SubstrateError("deployment operation state is not a JSON object")
    return payload


def _wait_for_source_recovery_workflow(
    ctx: UpgradeContext,
    *,
    workflow_id: str,
    source: str,
    timeout_seconds: float,
    poll_interval_seconds: float,
) -> dict[str, Any]:
    started = time.monotonic()
    while time.monotonic() - started <= timeout_seconds:
        result = _run_okg(
            ctx,
            [
                "runtime", "debug-workflow",
                "--deployment", ctx.options.deployment,
                workflow_id,
            ],
        )
        if not result.ok:
            return _failed_command(f"source recovery status failed for {source}", result)
        report = result.json_payload()
        if not isinstance(report, dict):
            return _unreadable_command("could not read source recovery terminal status", result)
        status = str(report.get("status") or "").upper()
        if status == "SUCCESS":
            output = report.get("output")
            if isinstance(output, dict) and output.get("any_failed") is True:
                return {
                    "ok": False,
                    "reason": "source recovery workflow reported failed source work",
                    "source_name": source,
                    "workflow_id": workflow_id,
                    "blocking_stage": "source_execution",
                    "report": report,
                }
            return {"ok": True, "workflow_id": workflow_id, "report": report}
        if status in {
            "ERROR", "FAILED", "CANCELLED", "MAX_RECOVERY_ATTEMPTS",
            "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
        }:
            return {
                "ok": False,
                "reason": "source recovery workflow terminated unsuccessfully",
                "source_name": source,
                "workflow_id": workflow_id,
                "terminal_status": status,
                "blocking_stage": "dbos_workflow",
                "report": report,
            }
        time.sleep(min(poll_interval_seconds, max(0.0, timeout_seconds)))
    return {
        "ok": False,
        "reason": "source recovery workflow exceeded its action deadline",
        "source_name": source,
        "workflow_id": workflow_id,
        "blocking_stage": "dbos_workflow",
    }


_NARROW_SPLIT_MIGRATION_ID = "0020_interval_narrow_split"
_DEFERRED_RECOVERY_CLASSIFICATION = "deferred_pre_interval_narrow_split"
_DEFERRED_RECOVERY_SCHEMA = (
    "okg.effective_attribute_recovery_classification.deferred.v1"
)


def _deferred_recovery_classification(deployment: str) -> dict[str, Any]:
    """The RECORDED reason preflight defers the recovery classification.

    A schema-tagged payload with its own binding digest, so the deferral is
    auditable in the phase receipt and round-trips the resume path exactly
    like a real classification — while remaining impossible to mistake for
    one (its classification value is outside the classifier's vocabulary,
    and generation_recovery refuses to proceed on it unless migrate applied
    the pending migration).
    """
    payload = {
        "schema": _DEFERRED_RECOVERY_SCHEMA,
        "classification": _DEFERRED_RECOVERY_CLASSIFICATION,
        "deployment": deployment,
        "pending_migration": _NARROW_SPLIT_MIGRATION_ID,
        "reason": (
            "the recovery classifier reads the narrow attribute shape "
            f"(attrs_hash) that pending migration "
            f"{_NARROW_SPLIT_MIGRATION_ID} creates; classification is "
            "deferred to the generation_recovery phase, which runs after "
            "migrate"
        ),
    }
    payload["binding_digest"] = "sha256:" + hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return payload


def _phase_preflight(ctx: UpgradeContext) -> dict[str, Any]:
    dev_database = _ensure_dev_isolated_database(ctx)
    if dev_database is not None:
        ctx.data["dev_database"] = dev_database
        if not dev_database.get("ok"):
            return dev_database
        if dev_database.get("planned"):
            migration_status = {
                "planned": True,
                "inspected": False,
                "reason": "plan-only dev database bootstrap",
                "pending": [],
                "destructive_pending": [],
                "checksum_mismatches": [],
            }
            ctx.data["migrations_before"] = migration_status
            recovery = {
                "schema": "okg.effective_attribute_recovery_classification.v1",
                "classification": "planned",
                "read_only": True,
            }
            ctx.data["attribute_recovery_classification"] = recovery
            return {
                "ok": True,
                "planned": dev_database["planned"],
                "dev_database": dev_database,
                "migration_status": migration_status,
                "destructive_pending": [],
                "attribute_recovery_classification": recovery,
            }
        # A worktree-isolated dev clone is created empty. The backup phase
        # classifies the target effect by reading live catalog/nomos tables,
        # which raise UndefinedTable on a never-migrated database and dead-end
        # the dev golden path at effect_inventory_unreadable before the migrate
        # phase can install anything. Install the schema here (dev-isolated +
        # provably-empty only) so that classification is readable.
        dev_schema = _ensure_dev_isolated_schema(ctx)
        if dev_schema is not None:
            ctx.data["dev_schema"] = dev_schema
            if not dev_schema.get("ok"):
                return dev_schema
    migrations = _run_okg(ctx, ["db", "migrations", "--deployment", ctx.options.deployment])
    if not migrations.ok:
        return _failed_command("migration preflight failed", migrations)
    payload = migrations.json_payload()
    if not isinstance(payload, dict):
        # Fail closed: a zero-exit command whose JSON does not parse must NOT be
        # coerced to an empty payload (which would silently report "no
        # destructive migrations" off a malformed read).
        return _unreadable_command("could not read migration status", migrations)
    destructive_pending = payload.get("destructive_pending") or []
    if destructive_pending and ctx.options.skip_destructive_migrations:
        return {
            "ok": False,
            "reason": "destructive migrations are pending",
            "destructive_pending": destructive_pending,
            "fix_hint": (
                "clear or intentionally retire destructive migrations before "
                "rerunning okg up"
            ),
        }
    ctx.data["migrations_before"] = payload
    # phase-interval-narrow-split-migration (#1114): the recovery classifier
    # reads the narrow attribute shape (attrs_hash) that migration
    # 0020_interval_narrow_split creates, and preflight runs five phases
    # before migrate — so on a pre-0020 database the classifier fails with
    # UndefinedColumn and the guard requires the migration it guards. Detect
    # the pending migration from the status we already read and RECORD a
    # deferred classification (never a silent skip); the generation_recovery
    # phase, which runs after migrate, performs the real classification
    # against the migrated schema and fails closed if migrate was skipped.
    if _NARROW_SPLIT_MIGRATION_ID in {
        str(row.get("migration_id") or "")
        for row in (payload.get("pending") or [])
        if isinstance(row, dict)
    }:
        recovery = _deferred_recovery_classification(ctx.options.deployment)
        ctx.data["attribute_recovery_classification"] = recovery
        dev_schema = ctx.data.get("dev_schema")
        return {
            "ok": True,
            "migration_status": payload,
            "destructive_pending": destructive_pending,
            "attribute_recovery_classification": recovery,
            **({"dev_database": dev_database} if dev_database is not None else {}),
            **({"dev_schema": dev_schema} if dev_schema is not None else {}),
        }
    recovery_result = _run_okg(
        ctx,
        [
            "db",
            "inspect-effective-attribute-recovery",
            "--deployment",
            ctx.options.deployment,
        ],
    )
    if not recovery_result.ok:
        return _failed_command(
            "effective-attribute recovery preflight failed", recovery_result
        )
    recovery = recovery_result.json_payload()
    if not isinstance(recovery, dict):
        return _unreadable_command(
            "could not read effective-attribute recovery preflight",
            recovery_result,
        )
    if recovery.get("classification") not in {
        "no_published_generation",
        "no_repair",
        "ordinary_repair",
        "corrupt_state_recovery",
        # okg#1114: preflight runs five phases BEFORE migrate, so a database
        # whose attribute relations predate the interval narrow split cannot be
        # classified yet -- the parity fingerprint needs a column migrate has
        # not created. That is a schema verdict, not a data verdict, and it is
        # deliberately NOT a pass: generation_recovery reclassifies for real
        # after migrate (see _phase_generation_recovery) and refuses if the
        # schema is somehow still pre-split by then.
        "schema_migration_pending",
    } or not str(recovery.get("binding_digest") or "").startswith("sha256:"):
        return {
            "ok": False,
            "reason": "effective-attribute recovery preflight returned an invalid classification",
            "result": recovery,
        }
    ctx.data["attribute_recovery_classification"] = recovery
    dev_schema = ctx.data.get("dev_schema")
    return {
        "ok": True,
        "migration_status": payload,
        "destructive_pending": destructive_pending,
        "attribute_recovery_classification": recovery,
        **({"dev_database": dev_database} if dev_database is not None else {}),
        **({"dev_schema": dev_schema} if dev_schema is not None else {}),
    }


def _ensure_dev_isolated_database(ctx: UpgradeContext) -> dict[str, Any] | None:
    """Create the worktree-isolated dev database before migration preflight.

    ``okg up --target dev`` points OKG_DSN at a worktree-specific database.
    Fresh worktrees legitimately start without that database, but migration
    preflight needs a database connection before the migrate phase can run.
    Prod keeps the old fail-closed behavior.
    """
    if ctx.options.profile != "dev":
        return None

    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    try:
        info = conninfo_to_dict(ctx.dsn)
    except Exception as exc:  # noqa: BLE001 - surface malformed DSNs as phase data.
        return {
            "ok": False,
            "reason": "dev database DSN is not parseable",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "fix_hint": "pass a valid --dsn or use the worktree-isolated dev profile",
        }
    database = info.get("dbname")
    resolution = getattr(ctx, "resolution", {})
    target = resolution.get("target") if isinstance(resolution, dict) else None
    target = target if isinstance(target, dict) else {}
    if target.get("identity") != "isolated":
        return {
            "ok": False,
            "reason": "dev database target is not isolated",
            "database": database,
            "target": target,
            "fix_hint": (
                "use the worktree-isolated dev DSN or an explicit DSN whose "
                "database is an isolation-suffixed clone of the deployment database"
            ),
        }
    fleet = _code_checkout_fleet()
    protected = {
        "postgres", "template0", "template1", "okg",
        *fleet.protected_databases,
    }
    if not database or database in protected:
        return {
            "ok": False,
            "reason": "dev database auto-create target is not isolated",
            "database": database,
            "protected_databases": sorted(protected),
            "protected_databases_config": fleet.source or "none configured",
            "fix_hint": (
                "use `okg up <deployment> --target dev` without overriding the "
                "worktree-isolated DSN, or pass an explicit disposable database"
            ),
        }
    if ctx.options.plan_only:
        return {
            "ok": True,
            "database": database,
            "created": False,
            "planned": f"CREATE DATABASE {database} if missing",
            "skipped": "dev database bootstrap skipped (--plan)",
            "maintenance_database": "postgres",
        }

    maintenance_info = dict(info)
    maintenance_info["dbname"] = "postgres"
    maintenance_dsn = make_conninfo(**maintenance_info)
    try:
        from okg.substrate.db.connection import connect as db_connect
        from psycopg import sql

        with db_connect(
            maintenance_dsn,
            autocommit=True,
            connect_timeout=5,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (database,),
                )
                if cur.fetchone():
                    return {
                        "ok": True,
                        "database": database,
                        "created": False,
                        "maintenance_database": "postgres",
                    }
                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database))
                )
                return {
                    "ok": True,
                    "database": database,
                    "created": True,
                    "maintenance_database": "postgres",
                }
    except Exception as exc:  # noqa: BLE001 - the preflight report carries the fix.
        return {
            "ok": False,
            "reason": "dev database bootstrap failed",
            "database": database,
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "fix_hint": (
                "start the local Postgres service and rerun `okg up "
                f"{ctx.options.deployment} --target dev`"
            ),
        }


def _ensure_dev_isolated_schema(ctx: UpgradeContext) -> dict[str, Any] | None:
    """Install the schema on a provably-uninitialized isolated dev database.

    ``okg up --target dev`` is the pipeline's own recommended legal move for an
    unmerged worktree: it points ``OKG_DSN`` at a worktree-isolated clone that
    the isolation machinery creates empty. The backup phase then classifies the
    target effect by reading live catalog/nomos tables; against a never-migrated
    database those reads raise ``UndefinedTable`` and the whole dev golden path
    dead-ends at ``effect_inventory_unreadable`` -- before the migrate phase
    (``PHASES`` index 5, after backup at index 1) can install anything.

    When the resolved target is dev-isolated AND the database is provably
    uninitialized (``to_regclass('okg.schema_migrations') IS NULL`` -- the
    migration ledger is the first object every migration run creates, so its
    absence means the schema was never installed, as opposed to a partially
    applied one whose ledger is already present), install the schema here so
    downstream effect classification is readable and the golden path proceeds.
    This mirrors the manual ``okg migrate --apply`` workaround, gated to exactly
    the safe case: an empty disposable clone has no data a physical rollback
    protects. A pathological clone carrying okg tables but no ledger would run
    migrate against a non-empty database, but that fails loudly (``already
    exists``) into ``_failed_command`` rather than corrupting anything.

    Fail closed everywhere else:

    * A partially-initialized clone (ledger present, other okg tables missing)
      is NOT auto-repaired -- ``to_regclass`` is non-NULL, so this returns a
      no-op and the effect inventory keeps refusing as unreadable.
    * A fully-migrated clone is a no-op here and follows the normal incremental
      refresh path.
    * Canonical/prod targets never reach this path (``profile != "dev"`` or a
      non-isolated identity), so their fail-closed behavior is unchanged.
    """
    if ctx.options.profile != "dev":
        return None
    resolution = getattr(ctx, "resolution", {})
    target = resolution.get("target") if isinstance(resolution, dict) else None
    target = target if isinstance(target, dict) else {}
    if target.get("identity") != "isolated":
        return None
    if ctx.options.plan_only:
        return {
            "ok": True,
            "initialized": False,
            "schema_installed": False,
            "planned": (
                "okg migrate --apply if okg.schema_migrations is absent"
            ),
            "skipped": "dev database schema install skipped (--plan)",
        }

    from okg.substrate.db.connection import connect as db_connect

    try:
        with db_connect(ctx.dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('okg.schema_migrations')")
                ledger = cur.fetchone()[0]
    except Exception as exc:  # noqa: BLE001 - probe failure carries its fix.
        return {
            "ok": False,
            "reason": "dev database schema probe failed",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "fix_hint": (
                "start the local Postgres service and rerun `okg up "
                f"{ctx.options.deployment} --target dev`"
            ),
        }
    if ledger is not None:
        # Ledger present: either fully migrated (normal incremental refresh) or
        # partially broken. Never auto-repair a partial clone -- leave it to
        # refuse as unreadable rather than papering over a half-built database.
        return {"ok": True, "initialized": False, "schema_installed": False}

    migrate = _run_okg(
        ctx,
        ["migrate", "--deployment", ctx.options.deployment, "--apply"],
    )
    if not migrate.ok:
        return _failed_command(
            "dev database schema install failed",
            migrate,
        )
    return {
        "ok": True,
        "initialized": True,
        "schema_installed": True,
        "database": target.get("database"),
    }


def _backup_archive_paths(
    ctx: UpgradeContext,
    policy: dict[str, Any],
) -> tuple[Path, Path]:
    strategy = policy["dump_strategy"]
    suffix = ".dumpdir" if strategy["format"] == "directory" else ".dump"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    final = ctx.backup_dir / f"{ctx.options.deployment}-{stamp}{suffix}"
    collision = 0
    while final.exists() or final.is_symlink():
        collision += 1
        final = ctx.backup_dir / (
            f"{ctx.options.deployment}-{stamp}-{collision:02d}{suffix}"
        )
    temporary = ctx.backup_dir / f".{final.name}.partial-{uuid4().hex}"
    return final, temporary


def _backup_dump_command(
    *,
    database: str,
    artifact_path: Path,
    strategy: dict[str, Any],
) -> list[str]:
    command = [
        "pg_dump",
        f"--format={strategy['format']}",
        "--file",
        str(artifact_path),
        "--dbname",
        database,
        "--lock-wait-timeout",
        f"{float(strategy['lock_wait_timeout_seconds']):g}s",
    ]
    compression = str(strategy["compression"])
    level = int(strategy["compression_level"])
    command.append(
        "--compress=none"
        if compression == "none"
        else f"--compress={compression}:{level}"
    )
    if strategy["format"] == "directory":
        command.extend(["--jobs", str(int(strategy["jobs"]))])
    return command


def _remove_partial_backup_artifact(path: Path) -> dict[str, Any]:
    if not path.exists() and not path.is_symlink():
        return {"removed": False, "path": str(path), "reason": "absent"}
    if path.is_symlink():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()
    return {"removed": True, "path": str(path)}


def _dump_strategy_preflight(
    ctx: UpgradeContext,
    *,
    database: str,
    backup_env: dict[str, str],
    policy: dict[str, Any],
    toolchain: dict[str, Any],
) -> dict[str, Any]:
    strategy = policy["dump_strategy"]
    selected_format = strategy["format"]
    jobs = int(strategy["jobs"])
    compression = str(strategy["compression"])
    checks = {
        "format_supported": True,
        "compression_supported": True,
        "parallel_supported": selected_format == "directory" or jobs == 1,
        "connection_reserve_ok": True,
    }
    if selected_format == "custom" and jobs == 1 and compression == "gzip":
        return {
            "ok": True,
            "strategy": strategy,
            "checks": checks,
            "connections": {"required_connections": 1},
            "capability_source": "serial custom-format baseline",
        }
    help_result = ctx.command_runner(["pg_dump", "--help"], ctx.env)
    if not help_result.ok:
        return _failed_command("pg_dump capability probe failed", help_result)
    help_text = help_result.stdout or help_result.stderr or ""
    checks["format_supported"] = (
        "--format" in help_text
        and (selected_format != "directory" or "directory" in help_text)
    )
    checks["parallel_supported"] = (
        jobs == 1 or (selected_format == "directory" and "--jobs" in help_text)
    )
    checks["compression_supported"] = (
        "--compress" in help_text
        and (
            compression in {"gzip", "none"}
            or int(
                (toolchain.get("tools", {}).get("pg_dump") or {}).get("major", 0)
            )
            >= 16
        )
    )
    connection_report: dict[str, Any] = {"required_connections": jobs + 1}
    if selected_format == "directory" and jobs > 1:
        # Shared probe: the boot admission check needs these same three
        # numbers, and two divergent implementations of "how much room is
        # left" is how the answers start disagreeing.
        from okg.substrate.db.capacity import server_capacity

        cap = server_capacity(ctx.dsn)
        max_connections = cap.max_connections
        reserved = cap.superuser_reserved
        active = cap.active_client_backends
        available = cap.available
        required = jobs + 1 + int(strategy["connection_safety_margin"])
        checks["connection_reserve_ok"] = available >= required
        connection_report.update({
            "max_connections": max_connections,
            "superuser_reserved_connections": reserved,
            "active_client_connections": active,
            "available_connections": available,
            "safety_margin": strategy["connection_safety_margin"],
            "required_with_margin": required,
        })
    if not all(checks.values()):
        return {
            "ok": False,
            "reason": "declared pg_dump strategy is unsupported or unsafe",
            "strategy": strategy,
            "checks": checks,
            "connections": connection_report,
            "fix_hint": (
                "select a supported format/compression or reduce dump jobs "
                "without consuming the connection safety margin"
            ),
        }
    if compression in {"lz4", "zstd"}:
        probe = ctx.command_runner(
            [
                "pg_dump",
                "--format=custom",
                "--schema-only",
                f"--compress={compression}:1",
                "--file",
                os.devnull,
                "--dbname",
                database,
            ],
            backup_env,
        )
        if not probe.ok:
            return {
                **_failed_command(
                    "pg_dump compression method is unsupported",
                    probe,
                ),
                "strategy": strategy,
                "checks": {**checks, "compression_supported": False},
            }
    return {
        "ok": True,
        "strategy": strategy,
        "checks": checks,
        "connections": connection_report,
    }


def _directory_inode_headroom(
    ctx: UpgradeContext,
    *,
    strategy: dict[str, Any],
) -> dict[str, Any]:
    if strategy["format"] != "directory":
        return {"ok": True, "required": False, "format": strategy["format"]}
    # Only an empty directory is created here, to measure free inodes; Git
    # never lists an empty directory. _phase_backup ignores .okg/backups
    # before it writes the first file, so this read-only probe stays one.
    ctx.backup_dir.mkdir(parents=True, exist_ok=True)
    stats = os.statvfs(ctx.backup_dir)
    available = int(stats.f_favail)
    try:
        from okg.substrate.db.connection import connect as db_connect

        with db_connect(ctx.dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*)::int FROM pg_class "
                    "WHERE relkind IN ('r','p','m','i','S')"
                )
                relation_count = int(cur.fetchone()[0])
    except Exception:  # noqa: BLE001 - an unknown inode bound refuses.
        relation_count = 0
    estimated = relation_count * 4 + 256 if relation_count > 0 else None
    ok = estimated is not None and available >= estimated
    return {
        "ok": ok,
        "required": True,
        "available_inodes": available,
        "relation_count": relation_count,
        "estimated_required_inodes": estimated,
        "reason": None if ok else "directory dump inode headroom is unprobeable or low",
    }


def _resumable_independent_archive(
    ctx: UpgradeContext,
    *,
    policy: dict[str, Any],
    toolchain: dict[str, Any],
    snapshot: dict[str, Any],
) -> dict[str, Any] | None:
    prior = ctx.data.get("prior_backup_attempt")
    resume = prior.get("resume_archive") if isinstance(prior, dict) else None
    if not isinstance(resume, dict):
        return None
    path_raw = resume.get("backup_path")
    path = Path(path_raw) if isinstance(path_raw, str) and path_raw else None
    if path is None:
        return {"ok": False, "reason": "resumable backup archive path is missing"}
    try:
        path.resolve().relative_to(ctx.backup_dir.resolve())
        contained = not path.is_symlink()
    except (OSError, ValueError):
        contained = False
    archive = _validate_backup_archive(ctx, path) if contained else {"ok": False}
    checks = {
        "typed_state": resume.get("schema") == "okg.backup_verify_resume.v1",
        "contained": contained,
        "policy_equal": _independent_archive_policy(
            resume.get("backup_policy")
        ) == _independent_archive_policy(policy),
        "toolchain_equal": resume.get("toolchain") == toolchain,
        "compatibility_equal": _independent_archive_compatibility(
            resume.get("compatibility_snapshot")
        ) == _independent_archive_compatibility(snapshot),
        "archive_valid": archive.get("ok") is True,
        "archive_digest_equal": (
            archive.get("backup_digest") == resume.get("backup_digest")
        ),
        "archive_size_equal": (
            archive.get("backup_size_bytes") == resume.get("backup_size_bytes")
        ),
    }
    if not all(checks.values()):
        return {
            "ok": False,
            "reason": "resumable backup archive no longer matches its proof state",
            "resume_checks": checks,
            "fix_hint": "inspect the retained archive, then rerun with --reset-state",
        }
    return {
        "ok": True,
        "backup_path": path,
        "archive": archive,
        "dump_progress": resume.get("dump_progress") or {},
        "headroom": resume.get("headroom") or {},
        "inode_headroom": resume.get("inode_headroom") or {},
        "strategy_preflight": resume.get("dump_strategy_preflight") or {},
        "command": resume.get("command"),
        "resume_checks": checks,
    }


def _independent_archive_compatibility(value: Any) -> dict[str, Any]:
    """Applied-state identity for restore-only resume, excluding future plan."""
    if not isinstance(value, Mapping):
        return {}
    return {
        key: item
        for key, item in value.items()
        if key != "candidate_plan_digest"
    }


def _independent_archive_policy(value: Any) -> dict[str, Any]:
    """Only dump-shaping policy determines whether an archive can be reused."""
    if not isinstance(value, Mapping):
        return {}
    strategy = value.get("dump_strategy")
    return {"dump_strategy": strategy} if isinstance(strategy, Mapping) else {}


def _budget_resume_policy(value: Any) -> dict[str, Any]:
    """Policy identity excluding only the forced budgets being qualified."""
    if not isinstance(value, Mapping):
        return {}
    normalized = {str(key): item for key, item in value.items()}
    budgets = normalized.get("budgets_seconds")
    if isinstance(budgets, Mapping):
        normalized["budgets_seconds"] = {
            str(key): item
            for key, item in budgets.items()
            if key != "forced"
        }
    return normalized


def _budget_resume_failure_names(
    prior: Mapping[str, Any],
) -> frozenset[str] | None:
    """Return a complete, internally consistent budget-only failure shape."""
    if not _budget_resume_failure_candidate(prior):
        return None
    reason_to_name = {
        "backup phase budget exceeded": "backup",
        "dump phase budget exceeded": "dump",
        "deployment total runtime budget exceeded": "total",
    }
    primary = reason_to_name.get(prior.get("reason"))
    raw_names = prior.get("budget_exceeded_names")
    if not (
        primary is not None
        and isinstance(raw_names, list)
        and bool(raw_names)
        and all(isinstance(name, str) for name in raw_names)
    ):
        return None
    names = frozenset(raw_names)
    if primary not in names or not names <= {"backup", "dump", "total"}:
        return None
    return names


def _budget_resume_failure_candidate(prior: Mapping[str, Any]) -> bool:
    """Whether a failed row claims to be eligible for budget-only resume.

    Refusal rows (``budget_resume_refused``) are never candidates: they are
    stamped after a no-I/O resume attempt and carry no measured phase
    durations, so admitting them would shadow the retained verified proof.
    """
    return bool(
        prior.get("phase") == "backup"
        and prior.get("ok") is False
        and prior.get("budget_exceeded") is True
        and prior.get("budget_resume_refused") is not True
        and prior.get("reason") in {
            "backup phase budget exceeded",
            "dump phase budget exceeded",
            "deployment total runtime budget exceeded",
        }
    )


def _budget_resume_budget_checks(
    prior: Mapping[str, Any],
    *,
    current_budgets: Mapping[str, Any] | None,
) -> dict[str, dict[str, float | bool | None]]:
    """Require measured dump/backup duration plus declared 25% headroom."""
    observed = {
        "backup": prior.get("duration_seconds"),
        "dump": prior.get("dump_duration_seconds"),
    }
    checks: dict[str, dict[str, float | bool | None]] = {}
    for name, duration in observed.items():
        limit = current_budgets.get(name) if current_budgets else None
        duration_ok = (
            isinstance(duration, (int, float))
            and not isinstance(duration, bool)
            and math.isfinite(float(duration))
            and float(duration) > 0
        )
        limit_ok = (
            isinstance(limit, (int, float))
            and not isinstance(limit, bool)
            and math.isfinite(float(limit))
            and float(limit) > 0
        )
        observed_seconds = float(duration) if duration_ok else None
        required_seconds = (
            observed_seconds * BUDGET_RESUME_HEADROOM_FACTOR
            if duration_ok
            else None
        )
        budget_seconds = float(limit) if limit_ok else None
        checks[name] = {
            "observed_seconds": observed_seconds,
            "headroom_factor": BUDGET_RESUME_HEADROOM_FACTOR,
            "required_seconds": required_seconds,
            "budget_seconds": budget_seconds,
            "ok": bool(
                duration_ok
                and limit_ok
                and required_seconds is not None
                and budget_seconds is not None
                and required_seconds <= budget_seconds
            ),
        }
    return checks


def _resumable_budget_verified_backup(
    ctx: UpgradeContext,
    *,
    policy: dict[str, Any],
    toolchain: dict[str, Any],
    snapshot: dict[str, Any],
    source_database_size: int | None,
    effect_inventory: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Reuse a full restore rejected only by post-proof budget evaluation.

    This is intentionally narrower than restore-drill reuse.  The persisted
    phase must contain a current-archive full restore with every identity and
    cleanup check complete.  Resume re-observes live authority and validates
    the unchanged archive before allowing the already-proved boundary to pass
    under newly qualified budgets.
    """
    prior = ctx.data.get("prior_backup_attempt")
    if not isinstance(prior, dict) or not _budget_resume_failure_candidate(prior):
        return None
    failure_names = _budget_resume_failure_names(prior)
    if failure_names is None:
        return {
            "ok": False,
            "reason": "verified backup budget-failure evidence is incomplete",
            "budget_resume_refused": True,
            "mutation_authorized": False,
            "physical_backup_started": False,
            "restore_started": False,
            "backup_tool_child_processes_started": 0,
            "fix_hint": (
                "inspect the retained receipt, then rerun with --reset-state "
                "only if a new full backup is intended"
            ),
        }

    current_budgets = _selected_backup_budgets(policy, routine=False)
    budget_checks = _budget_resume_budget_checks(
        prior,
        current_budgets=current_budgets,
    )
    prior_total_duration = ctx.data.get("prior_total_duration_seconds")
    total_limit = current_budgets.get("total") if current_budgets else None
    cumulative_total_check = {
        "observed_seconds": (
            float(prior_total_duration)
            if isinstance(prior_total_duration, (int, float))
            and not isinstance(prior_total_duration, bool)
            and math.isfinite(float(prior_total_duration))
            and float(prior_total_duration) >= 0
            else None
        ),
        "headroom_factor": BUDGET_RESUME_HEADROOM_FACTOR,
        "required_seconds": None,
        "budget_seconds": (
            float(total_limit)
            if isinstance(total_limit, (int, float))
            and not isinstance(total_limit, bool)
            and math.isfinite(float(total_limit))
            and float(total_limit) > 0
            else None
        ),
        "ok": False,
    }
    if cumulative_total_check["observed_seconds"] is not None:
        cumulative_total_check["required_seconds"] = (
            cumulative_total_check["observed_seconds"]
            * BUDGET_RESUME_HEADROOM_FACTOR
        )
    cumulative_total_check["ok"] = bool(
        cumulative_total_check["observed_seconds"] is not None
        and cumulative_total_check["required_seconds"] is not None
        and cumulative_total_check["budget_seconds"] is not None
        and cumulative_total_check["required_seconds"]
        <= cumulative_total_check["budget_seconds"]
    )
    budgets_qualified = all(
        check.get("ok") is True for check in budget_checks.values()
    ) and cumulative_total_check["ok"] is True

    path_raw = prior.get("backup_path")
    path = Path(path_raw) if isinstance(path_raw, str) and path_raw else None
    try:
        assert path is not None
        path.resolve().relative_to(ctx.backup_dir.resolve())
        contained = not path.is_symlink()
    except (AssertionError, OSError, ValueError):
        contained = False

    prior_archive_checks = prior.get("archive_checks")
    prior_identity_checks = prior.get("identity_checks")
    cleanup = prior.get("cleanup")
    progress = prior.get("progress")
    progress = progress if isinstance(progress, Mapping) else {}
    checks = {
        "budget_failure_only": bool(failure_names),
        "full_restore_verified": (
            prior.get("verification_mode") == "current_full_restore"
            and prior.get("restore_current_archive") is True
            and prior.get("receipt_reused") is False
            and prior.get("restore_verified") is True
            and prior.get("schema_ok") is True
            and prior.get("ownership_ok") is True
        ),
        "identity_checks_complete": (
            isinstance(prior_identity_checks, Mapping)
            and set(prior_identity_checks) == BUDGET_RESUME_IDENTITY_CHECKS
            and all(value is True for value in prior_identity_checks.values())
        ),
        "prior_archive_checks_complete": (
            isinstance(prior_archive_checks, Mapping)
            and all(
                prior_archive_checks.get(name) is True
                for name in BACKUP_ARCHIVE_CHECKS
            )
        ),
        "restore_cleanup_complete": (
            isinstance(cleanup, Mapping)
            and cleanup.get("disposable_database_dropped") is True
            and cleanup.get("restore_toc_removed") is True
        ),
        "progress_complete": all(
            isinstance(progress.get(name), Mapping)
            and bool(progress[name].get("completed_at"))
            for name in ("pg_dump", "pg_restore")
        ),
        "contained": contained,
        "policy_equal_except_forced_budgets": (
            _budget_resume_policy(prior.get("backup_policy"))
            == _budget_resume_policy(policy)
        ),
        "toolchain_equal": prior.get("toolchain") == toolchain,
        "compatibility_equal": (
            prior.get("compatibility_snapshot") == snapshot
        ),
        "source_database_size_equal": (
            isinstance(source_database_size, int)
            and source_database_size > 0
            and prior.get("source_database_size_bytes")
            == source_database_size
        ),
        "backup_budget_qualified": (
            budget_checks["backup"].get("ok") is True
        ),
        "dump_budget_qualified": budget_checks["dump"].get("ok") is True,
        "cumulative_total_budget_qualified": (
            cumulative_total_check["ok"] is True
        ),
    }
    if not all(checks.values()):
        return {
            "ok": False,
            "reason": (
                "verified backup remains outside current forced budgets"
                if not budgets_qualified
                else "verified backup budget-resume proof no longer matches current authority"
            ),
            "budget_resume_refused": True,
            "mutation_authorized": False,
            "physical_backup_started": False,
            "restore_started": False,
            "backup_tool_child_processes_started": 0,
            "budget_resume_checks": checks,
            "budget_qualification_checks": budget_checks,
            "cumulative_total_budget_check": cumulative_total_check,
            "fix_hint": (
                "qualify the forced budgets against the measured full restore"
                if not budgets_qualified
                else "inspect the drift, then rerun with --reset-state for a new full proof"
            ),
        }

    assert path is not None
    banked = dict(prior)
    banked["ok"] = True
    previous_banked = ctx.data.get("backup_result")
    ctx.data["backup_result"] = banked
    try:
        archive = _validate_backup_archive_for_run(ctx, path)
    finally:
        if previous_banked is None:
            ctx.data.pop("backup_result", None)
        else:
            ctx.data["backup_result"] = previous_banked
    checks.update({
        "archive_valid": archive.get("ok") is True,
        "archive_digest_equal": (
            archive.get("backup_digest") == prior.get("backup_digest")
        ),
        "archive_size_equal": (
            archive.get("backup_size_bytes") == prior.get("backup_size_bytes")
        ),
    })
    if not all(checks.values()):
        return {
            "ok": False,
            "reason": "verified backup budget-resume archive no longer matches its proof",
            "budget_resume_refused": True,
            "mutation_authorized": False,
            "physical_backup_started": False,
            "restore_started": False,
            "backup_tool_child_processes_started": 0,
            "budget_resume_checks": checks,
            "fix_hint": "inspect the retained archive, then rerun with --reset-state",
        }

    stale_fields = {
        "phase",
        "started_at",
        "completed_at",
        "duration_seconds",
        "budget_checks",
        "budget_exceeded",
        "budget_exceeded_names",
        "budget_seconds",
        "total_budget_seconds",
        "total_duration_seconds",
        "reason",
        "ok",
    }
    archive_fields = {
        key: value
        for key, value in archive.items()
        if key in {
            "backup_size_bytes",
            "backup_digest",
            "backup_mode",
            "archive_kind",
            "archive_member_count",
            "archive_readable",
            "archive_checks",
            "archive_validation_duration_seconds",
            "archive_manifest_path",
            "archive_metadata_fingerprint",
            "archive_content_rehash_skipped",
        }
    }
    result = {
        key: value
        for key, value in prior.items()
        if key not in stale_fields
    }
    result.update({
        "ok": True,
        "backup_path": str(path),
        "backup_policy": policy,
        "toolchain": toolchain,
        "compatibility_snapshot": snapshot,
        "candidate_plan_digest": snapshot["candidate_plan_digest"],
        "source_database_size_bytes": source_database_size,
        "budgets_seconds": current_budgets,
        "resumed_budget_verified_backup": True,
        "mutation_authorized": True,
        "physical_backup_started": False,
        "restore_started": False,
        "backup_tool_child_processes_started": 0,
        "budget_resume_checks": checks,
        "budget_qualification_checks": budget_checks,
        "cumulative_total_budget_check": cumulative_total_check,
        **archive_fields,
        **(
            {"effect_inventory": effect_inventory}
            if effect_inventory is not None
            else {}
        ),
    })
    ctx.data["backup_result"] = result
    return result


def _backup_verify_resume_payload(
    *,
    backup_path: Path,
    archive: dict[str, Any],
    policy: dict[str, Any],
    toolchain: dict[str, Any],
    snapshot: dict[str, Any],
    source_database_size: int | None,
    dump_progress: dict[str, Any],
    headroom: dict[str, Any],
    inode_headroom: dict[str, Any],
    strategy_preflight: dict[str, Any],
    command: str,
) -> dict[str, Any]:
    return {
        "schema": "okg.backup_verify_resume.v1",
        "backup_path": str(backup_path),
        "backup_digest": archive.get("backup_digest"),
        "backup_size_bytes": archive.get("backup_size_bytes"),
        "backup_policy": policy,
        "toolchain": toolchain,
        "compatibility_snapshot": snapshot,
        "source_database_size_bytes": source_database_size,
        "dump_progress": dump_progress,
        "headroom": headroom,
        "inode_headroom": inode_headroom,
        "dump_strategy_preflight": strategy_preflight,
        "command": command,
    }


def _deployment_safety_plan(
    ctx: UpgradeContext,
    *,
    effect_report: Mapping[str, Any] | None,
    restore_point_source: str,
    restore_point_verified: bool,
    restore_point_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the operator acknowledgement boundary without claiming rollback.

    A database archive and a provider call answer different questions.  The
    archive can authorize database mutation after an exact restore drill; it
    never makes a provider call reversible.  The digest binds both facts to
    the exact resolved target and immutable effect plan shown to the operator.
    """
    resolution = getattr(ctx, "resolution", {})
    target = resolution.get("target") if isinstance(resolution, Mapping) else None
    target = dict(target) if isinstance(target, Mapping) else {"identity": None}
    substitution = _target_substitution_notice(target)
    smoke = (
        effect_report.get("smoke")
        if isinstance(effect_report, Mapping)
        and isinstance(effect_report.get("smoke"), Mapping)
        else {}
    )
    entries = smoke.get("entries") if isinstance(smoke, Mapping) else []
    external_sources = sorted(
        str(row.get("name"))
        for row in entries or []
        if isinstance(row, Mapping) and row.get("external") is True
    )
    external_effects = bool(external_sources)
    adapter_digests = sorted(
        str(row.get("adapter_contract_digest"))
        for row in entries or []
        if isinstance(row, Mapping) and row.get("adapter_contract_digest")
    )
    lifecycle_admission = ctx.data.get("lifecycle_plan_admission")
    target_key = ctx.data.get("target_key")
    target_key = target_key if isinstance(target_key, Mapping) else target
    restore_evidence = (
        dict(restore_point_evidence)
        if isinstance(restore_point_evidence, Mapping)
        else {}
    )
    plan = {
        "schema": "okg.deployment_safety_plan.v1",
        "deployment": ctx.options.deployment,
        "profile": ctx.options.profile,
        "resolved_target": {
            key: target.get(key)
            for key in (
                "deployment", "authority_mode", "host", "port", "database",
                "expected_database", "identity",
            )
        },
        "target_substitution": substitution,
        "code_revision": target_key.get("code_revision"),
        "manifest_digest": target_key.get("manifest_digest"),
        "effect_digest": (
            effect_report.get("effect_digest")
            if isinstance(effect_report, Mapping)
            else None
        ),
        "adapter_contract_digests": adapter_digests,
        "lifecycle_plan_digest": (
            lifecycle_admission.get("plan_digest")
            if isinstance(lifecycle_admission, Mapping)
            else None
        ),
        "verified_backup_source_revision": (
            ctx.options.verified_backup_source_revision
        ),
        "external_effects": {
            "present": external_effects,
            "phase": "smoke_publish" if external_effects else None,
            "sources": external_sources,
            "classes": ["provider_mutation"] if external_effects else [],
            "reversible": False if external_effects else None,
            "guarantee_unavailable": (
                "provider_effect_rollback" if external_effects else None
            ),
        },
        "network_effects": {
            "classes": ["read_only_network_fetch"],
            "phases": ["sync_code", "dependency_sync"],
            "provider_mutation": False,
            "reversible": None,
            "disclosure": (
                "code and dependency synchronization may read from remote endpoints; "
                "they do not authorize provider mutation"
            ),
        },
        "restore_point": {
            "schema": "okg.deployment_restore_point.v1",
            "source": restore_point_source,
            "verified": bool(restore_point_verified),
            "verification_scope": "at_confirmation",
            "observed_evidence_location": "safety_contract.observed_restore_point",
            "receipt_path": restore_evidence.get("receipt_path"),
            "receipt_digest": restore_evidence.get("receipt_digest"),
            "archive_digest": restore_evidence.get("archive_digest"),
            "fresh": restore_evidence.get("fresh"),
            "owner_current": restore_evidence.get("owner_current"),
            "restore_checks": restore_evidence.get("restore_checks", {}),
            "required_checks": [
                "target_identity",
                "archive_digest",
                "freshness",
                "current_owner",
                "restore_verified",
            ],
            "producer_contract": {
                "schema": "okg.deployment_restore_producer.v1",
                "mode": (
                    "independent_verified_receipt"
                    if restore_point_source
                    in {
                        "backup_verify_receipt",
                        "independent_backup_verify",
                        "precheckout_verified_backup",
                    }
                    else "current_run_verified_backup"
                ),
                "archive_format": "pg_dump_custom",
                "verification": "disposable_restore_drill",
                "policy_digest": restore_evidence.get("policy_digest"),
                "toolchain_digest": restore_evidence.get("toolchain_digest"),
                "required_checks": [
                    "target_identity",
                    "archive_digest",
                    "freshness",
                    "current_owner",
                    "restore_verified",
                ],
            },
            "authorizes_database_mutation": bool(restore_point_verified),
            "authorizes_provider_replay": False,
        },
    }
    digest = _canonical_digest(plan)
    required = substitution is not None or external_effects
    provided = ctx.options.confirm_safety_plan
    confirmation = {
        "required": required,
        "digest": digest,
        "provided": provided is not None,
        "exact": provided == digest,
        "confirmed_at": _now() if provided == digest else None,
        "confirmed_by": "operator_exact_digest" if provided == digest else None,
    }
    safety = {**plan, "confirmation": confirmation}
    ctx.data["safety_plan"] = safety
    return safety


def _revalidate_resumed_safety_contract(
    ctx: UpgradeContext,
    persisted_plan: Mapping[str, Any],
) -> None:
    """Recheck immutable plan identity and exact acknowledgement on resume.

    A persisted confirmation is audit evidence, not a bearer capability. The
    operator must present the same digest again, and the current target/effect
    inventory must reproduce it before any resumed side effect.
    """
    persisted_confirmation = persisted_plan.get("confirmation")
    persisted_confirmation = (
        persisted_confirmation
        if isinstance(persisted_confirmation, Mapping)
        else {}
    )
    persisted_digest = persisted_confirmation.get("digest")
    restore_point = persisted_plan.get("restore_point")
    restore_point = restore_point if isinstance(restore_point, Mapping) else {}
    effects, effect_errors, effect_report = _collect_effect_inventory(ctx)
    effect_checks = effects.to_dict() if effects is not None else {}
    refusal_failures = [
        name
        for name in _REFUSAL_EFFECT_AUTHORITY_CHECKS
        if effect_checks.get(name) is not True
    ]
    if effect_errors or refusal_failures:
        raise SubstrateError(
            "resumed deployment safety plan could not be revalidated",
            data={
                "effect_errors": list(effect_errors),
                "failed_checks": refusal_failures,
                "persisted_safety_plan_digest": persisted_digest,
            },
            fix_hint=(
                "repair target and smoke-effect authority; do not discard the "
                "retained restore receipt"
            ),
        )
    producer = restore_point.get("producer_contract")
    producer = producer if isinstance(producer, Mapping) else {}
    current = _deployment_safety_plan(
        ctx,
        effect_report=effect_report,
        restore_point_source=str(restore_point.get("source") or "unknown"),
        restore_point_verified=restore_point.get("verified") is True,
        restore_point_evidence={
            "receipt_path": restore_point.get("receipt_path"),
            "receipt_digest": restore_point.get("receipt_digest"),
            "archive_digest": restore_point.get("archive_digest"),
            "fresh": restore_point.get("fresh"),
            "owner_current": restore_point.get("owner_current"),
            "restore_checks": restore_point.get("restore_checks") or {},
            "policy_digest": producer.get("policy_digest"),
            "toolchain_digest": producer.get("toolchain_digest"),
        },
    )
    current_confirmation = current.get("confirmation")
    current_confirmation = (
        current_confirmation if isinstance(current_confirmation, Mapping) else {}
    )
    current_digest = current_confirmation.get("digest")
    if not isinstance(persisted_digest, str) or current_digest != persisted_digest:
        raise SubstrateError(
            "resumed deployment safety plan drifted",
            data={
                "persisted_safety_plan_digest": persisted_digest,
                "current_safety_plan_digest": current_digest,
                "persisted_safety_plan": dict(persisted_plan),
                "current_safety_plan": current,
            },
            fix_hint=(
                "inspect the changed target/effect contract; start a separately "
                "planned operation only after reconciling the retained state"
            ),
        )
    refusal = _safety_plan_refusal(ctx, current)
    if refusal is not None:
        raise SubstrateError(
            str(refusal["reason"]),
            data={
                "safety_plan": current,
                "next_command": refusal.get("next_command"),
            },
            fix_hint=str(refusal.get("fix_hint") or "present the exact plan digest"),
        )


def _safety_plan_refusal(
    ctx: UpgradeContext,
    safety_plan: Mapping[str, Any],
) -> dict[str, Any] | None:
    confirmation = safety_plan.get("confirmation")
    confirmation = confirmation if isinstance(confirmation, Mapping) else {}
    required = confirmation.get("required") is True
    exact = confirmation.get("exact") is True
    provided = confirmation.get("provided") is True
    if (required and not exact) or (provided and not exact):
        digest = str(confirmation.get("digest") or "")
        parts = _recovery_command_parts(ctx, plan_only=False)
        parts.extend(["--confirm-safety-plan", digest])
        return {
            "ok": False,
            "reason": (
                "exact safety-plan confirmation is required before mutation"
                if required
                else "provided safety-plan confirmation does not match this plan"
            ),
            "mutation_authorized": False,
            "safety_plan": dict(safety_plan),
            "next_command": _display_recovery_command(ctx, parts),
            "fix_hint": (
                "inspect the resolved target, irreversible external effects, and "
                "restore-point guarantee; then run the exact next_command"
            ),
        }
    return None


def _safety_contract_snapshot(
    ctx: UpgradeContext,
    *,
    restore_point_verified: bool,
    phase: str,
) -> dict[str, Any] | None:
    plan = getattr(ctx, "data", {}).get("safety_plan")
    if not isinstance(plan, Mapping):
        return None
    external = plan.get("external_effects")
    external = external if isinstance(external, Mapping) else {}
    backup = ctx.data.get("backup_result")
    backup = backup if isinstance(backup, Mapping) else {}
    planned_restore = plan.get("restore_point")
    planned_restore = (
        planned_restore if isinstance(planned_restore, Mapping) else {}
    )
    observed_checks = dict(planned_restore.get("restore_checks") or {})
    for key in ("restore_verified", "schema_ok", "ownership_ok"):
        if key in backup:
            observed_checks[key] = backup.get(key) is True
    for key in ("identity_checks", "schema_catalog_component_checks"):
        value = backup.get(key)
        if isinstance(value, Mapping):
            observed_checks[key] = dict(value)
    archive_checks = backup.get("archive_checks")
    if isinstance(archive_checks, Mapping):
        observed_checks["archive"] = dict(archive_checks)
    observed_restore = {
        "schema": "okg.deployment_restore_point.v1",
        "source": planned_restore.get("source"),
        "verified": bool(restore_point_verified),
        "archive": backup.get("backup_path"),
        "archive_digest": (
            backup.get("backup_digest")
            or planned_restore.get("archive_digest")
        ),
        "receipt_path": planned_restore.get("receipt_path"),
        "receipt_digest": planned_restore.get("receipt_digest"),
        "fresh": planned_restore.get("fresh"),
        "owner_current": planned_restore.get("owner_current"),
        "restore_checks": observed_checks,
        "authorizes_database_mutation": bool(restore_point_verified),
        "authorizes_provider_replay": False,
    }
    # The exact operator digest remains the digest of the immutable, pre-effect
    # plan.  Observations earned by the dump/restore are appended under a named
    # field, so state and output show the authority that was actually earned
    # without retroactively changing what the operator confirmed.
    plan_snapshot = dict(plan)
    plan_snapshot["observed_restore_point"] = observed_restore
    return {
        "schema": "okg.deployment_safety_contract.v1",
        "plan": plan_snapshot,
        "confirmed_plan_digest": (
            (plan.get("confirmation") or {}).get("digest")
            if isinstance(plan.get("confirmation"), Mapping)
            else None
        ),
        "observation_binding": "append_only_outside_confirmed_plan_digest",
        "observed_at_phase": phase,
        "observed_restore_point": observed_restore,
        "guarantees": {
            "database_restore_point_verified": bool(restore_point_verified),
            "database_mutation_authorized": bool(restore_point_verified),
            "provider_effects_present": external.get("present") is True,
            "provider_effects_reversible": (
                False if external.get("present") is True else None
            ),
            "provider_replay_authorized": False,
        },
    }


def _verified_backup_restore_point_evidence(
    backup: Mapping[str, Any],
) -> dict[str, Any]:
    """Normalize evidence earned by a successful physical restore.

    The disaster-recovery receipt is separate from the lifecycle state file,
    which lets the safety-plan digest bind the receipt without a circular hash
    of the state that contains the plan.
    """
    receipt = backup.get("disaster_recovery_receipt")
    receipt = receipt if isinstance(receipt, Mapping) else {}
    receipt_path_raw = receipt.get("path")
    receipt_path = (
        Path(str(receipt_path_raw)) if isinstance(receipt_path_raw, str) else None
    )
    receipt_digest = None
    receipt_checks: dict[str, Any] = {}
    if receipt_path is not None:
        try:
            receipt_stat = receipt_path.stat()
            receipt_digest = "sha256:" + hashlib.sha256(
                receipt_path.read_bytes()
            ).hexdigest()
            receipt_checks = {
                "receipt_not_symlink": not receipt_path.is_symlink(),
                "receipt_mode_0600": receipt_stat.st_mode & 0o777 == 0o600,
                "receipt_owner_current": receipt_stat.st_uid == os.geteuid(),
            }
        except OSError:
            receipt_checks = {
                "receipt_not_symlink": False,
                "receipt_mode_0600": False,
                "receipt_owner_current": False,
            }
    archive_path_raw = backup.get("backup_path")
    archive_path = (
        Path(str(archive_path_raw)) if isinstance(archive_path_raw, str) else None
    )
    archive_owner_current = False
    fresh = False
    if archive_path is not None:
        try:
            archive_stat = archive_path.stat()
            archive_owner_current = archive_stat.st_uid == os.geteuid()
            fresh = (
                max(
                    0.0,
                    datetime.now(timezone.utc).timestamp() - archive_stat.st_mtime,
                )
                <= 3600
            )
        except OSError:
            # Missing or concurrently removed evidence remains fail-closed via
            # the false freshness/ownership checks assembled below.
            pass
    restore_checks: dict[str, Any] = {
        "restore_verified": backup.get("restore_verified") is True,
        "schema_ok": backup.get("schema_ok") is True,
        "ownership_ok": backup.get("ownership_ok") is True,
        "archive_digest_present": isinstance(backup.get("backup_digest"), str),
        "archive_owner_current": archive_owner_current,
        "fresh": fresh,
        **receipt_checks,
    }
    for key in ("identity_checks", "schema_catalog_component_checks"):
        value = backup.get(key)
        if isinstance(value, Mapping):
            restore_checks[key] = dict(value)
    policy = backup.get("backup_policy")
    toolchain = backup.get("toolchain")
    return {
        "receipt_path": str(receipt_path) if receipt_path is not None else None,
        "receipt_digest": receipt_digest,
        "archive_digest": backup.get("backup_digest"),
        "fresh": fresh,
        "owner_current": (
            archive_owner_current
            and receipt_checks.get("receipt_owner_current") is True
        ),
        "restore_checks": restore_checks,
        "policy_digest": (
            _canonical_digest(policy) if isinstance(policy, Mapping) else None
        ),
        "toolchain_digest": (
            _canonical_digest(toolchain) if isinstance(toolchain, Mapping) else None
        ),
    }


def _phase_attempt_guarantees(
    ctx: UpgradeContext,
    *,
    phase: str,
) -> dict[str, Any]:
    target = ctx.resolution.get("target")
    target = dict(target) if isinstance(target, Mapping) else {"identity": None}
    plan = getattr(ctx, "data", {}).get("safety_plan")
    confirmation = plan.get("confirmation") if isinstance(plan, Mapping) else None
    backup = ctx.data.get("backup_result")
    restore_verified = bool(
        isinstance(backup, Mapping)
        and backup.get("ok") is True
        and (
            backup.get("restore_verified") is True
            or backup.get("mutation_authorized") is True
        )
    )
    external = plan.get("external_effects") if isinstance(plan, Mapping) else None
    return {
        "target_identity": {
            "requested": {
                "deployment": ctx.options.deployment,
                "profile": ctx.options.profile,
                "declared_database": target.get("expected_database"),
            },
            "resolved_digest": _canonical_digest({
                key: target.get(key)
                for key in (
                    "authority_mode", "host", "port", "database",
                    "expected_database", "identity",
                )
            }),
            "substituted": _target_substitution_notice(target) is not None,
        },
        "safety_plan_digest": (
            confirmation.get("digest")
            if isinstance(confirmation, Mapping)
            else None
        ),
        "lifecycle_plan_digest": (
            plan.get("lifecycle_plan_digest")
            if isinstance(plan, Mapping)
            else None
        ),
        "adapter_contract_digests": (
            list(plan.get("adapter_contract_digests") or [])
            if isinstance(plan, Mapping)
            else []
        ),
        "confirmed_at": (
            confirmation.get("confirmed_at")
            if isinstance(confirmation, Mapping)
            else None
        ),
        "restore_point": {
            "verified": restore_verified,
            "source": (
                (plan.get("restore_point") or {}).get("source")
                if isinstance(plan, Mapping)
                and isinstance(plan.get("restore_point"), Mapping)
                else None
            ),
            "archive_digest": (
                backup.get("backup_digest") if isinstance(backup, Mapping) else None
            ),
            "receipt_path": (
                (plan.get("restore_point") or {}).get("receipt_path")
                if isinstance(plan, Mapping)
                and isinstance(plan.get("restore_point"), Mapping)
                else None
            ),
            "fresh": (
                (plan.get("restore_point") or {}).get("fresh")
                if isinstance(plan, Mapping)
                and isinstance(plan.get("restore_point"), Mapping)
                else None
            ),
            "owner_current": (
                (plan.get("restore_point") or {}).get("owner_current")
                if isinstance(plan, Mapping)
                and isinstance(plan.get("restore_point"), Mapping)
                else None
            ),
            "restore_checks": (
                dict((plan.get("restore_point") or {}).get("restore_checks") or {})
                if isinstance(plan, Mapping)
                and isinstance(plan.get("restore_point"), Mapping)
                else {}
            ),
        },
        "external_effect": {
            "present": (
                external.get("present") is True
                if isinstance(external, Mapping)
                else False
            ),
            "sources": (
                list(external.get("sources") or [])
                if isinstance(external, Mapping)
                else []
            ),
            "reversible": (
                False
                if isinstance(external, Mapping)
                and external.get("present") is True
                else None
            ),
            "effect_digest": (
                plan.get("effect_digest") if isinstance(plan, Mapping) else None
            ),
            "classes": (
                list(external.get("classes") or [])
                if isinstance(external, Mapping)
                else []
            ),
            "read_only_network_fetch": phase in {"sync_code", "dependency_sync"},
        },
        "idempotency": PHASE_IDEMPOTENCY[phase],
    }


def _admitted_phase_attempts(
    ctx: UpgradeContext,
    *,
    preflight: Mapping[str, Any],
    backup: Mapping[str, Any],
    source: str,
) -> dict[str, dict[str, Any]]:
    attempts: dict[str, dict[str, Any]] = {}
    for phase, row in (("preflight", preflight), ("backup", backup)):
        source_attempt_id = row.get("phase_attempt_id")
        attempt_id = f"admitted:{source}:{phase}:{_canonical_digest(row)[7:23]}"
        attempts[attempt_id] = {
            "attempt_id": attempt_id,
            "source_attempt_id": source_attempt_id,
            "phase": phase,
            "status": "succeeded",
            "started_at": row.get("started_at"),
            "terminal_at": row.get("completed_at"),
            "guarantees": _phase_attempt_guarantees(ctx, phase=phase),
            "effect_may_have_happened": False,
            "terminal_receipt": {
                "phase": phase,
                "attempt_id": attempt_id,
                "source_attempt_id": source_attempt_id,
                "semantic_fingerprint": row.get("semantic_fingerprint"),
                "ok": True,
                "generation": row.get("generation_id"),
            },
            "reconcile": {
                "decision": "external_verified_receipt_admitted",
                "source": source,
            },
            "superseded_by": None,
            "superseded_reason": None,
            "recovery_command": _next_command(ctx),
        }
    return attempts


# #1791: which backup-admission authority checks are deployment.yaml
# configuration (fixable in the manifest) and which are git/pin authority.
_MANIFEST_CONFIGURATION_CHECK_HINTS: dict[str, str] = {
    "smoke_source_versioned": (
        "runtime.smoke_sources in deployment.yaml declares no deployment-owned, "
        "versioned source (an empty selection is refused: absence is not "
        "proof); declare at least one"
    ),
    "attribute_read_model_rebuildable": (
        "native_views.attribute_effective_view_empty_overlay_fast_path is not "
        "true in deployment.yaml, or OKG_IVM_GENERATION_ORDINAL disables the "
        "generation ordinal; the attribute read model cannot be rebuilt from "
        "a backup without both"
    ),
}
_AUTHORITY_CHECK_HINTS: dict[str, str] = {
    "target_revision_pinned": (
        "the target code revision is not pinned to a committed, clean checkout "
        "(release.pin or a dirty/unborn tree)"
    ),
    "planner_trusted": (
        "the planner's own tracked files differ from the pinned revision "
        "(uncommitted edits under src/okg)"
    ),
    "plan_readable": (
        "the committed tree could not be projected into a candidate tree; see "
        "effect_inventory.error"
    ),
}


def _failed_check_hints(names: Sequence[str]) -> dict[str, dict[str, str]]:
    """Plain explanation per failed authority check, grouped by what fixes it."""
    return {
        "manifest_configuration": {
            name: _MANIFEST_CONFIGURATION_CHECK_HINTS[name]
            for name in names
            if name in _MANIFEST_CONFIGURATION_CHECK_HINTS
        },
        "authority": {
            name: _AUTHORITY_CHECK_HINTS.get(
                name, "see effect_inventory.checks and effect_inventory.error"
            )
            for name in names
            if name not in _MANIFEST_CONFIGURATION_CHECK_HINTS
        },
    }


def _publish_recovery_plan(ctx: UpgradeContext) -> dict[str, Any]:
    """Read-only plan naming the recovery for a refused publish (#1791).

    Module-level so tests can substitute it; the real plan bounds its
    connection attempt and reports a read failure inside the payload, so a
    refusal never becomes an exception here.
    """
    from okg.substrate.publish_recovery import plan_publish_recovery

    runtime_enabled: bool | None
    try:
        manifest = _resolve_upgrade_deployment(ctx)
        runtime_enabled = bool(getattr(manifest.runtime, "enabled", False))
    except Exception:  # noqa: BLE001 - the plan reports an unknown route.
        runtime_enabled = None
    return plan_publish_recovery(
        dsn=ctx.dsn,
        deployment=ctx.options.deployment,
        runtime_enabled=runtime_enabled,
    )


def _backup_refusal_guidance(
    ctx: UpgradeContext,
    *,
    refusal_failures: Sequence[str],
) -> dict[str, Any]:
    """Extra fields for a refused backup admission that lead to the recovery.

    Before #1791 the refusal said only "resolve the target/source authority
    failure" and `next_command` resumed `okg up` itself, while the actual
    recovery for a refused publish is `okg ingest` after fixing the cause the
    publish named. The refusal itself is unchanged; this only explains it.
    """
    hints = _failed_check_hints(refusal_failures)
    recovery = _publish_recovery_plan(ctx)
    default_hint = (
        "resolve the target/source authority failure before creating or "
        "verifying a physical backup"
    )
    if recovery.get("ok") is True and recovery.get("okg_up_applies") is False:
        command = recovery.get("recovery_command")
        explanation = str(recovery.get("explanation") or "").rstrip(". ")
        fix_hint = (
            "`okg up` upgrades an already-published deployment and is not the "
            "recovery for a refused publish; do not delete the deployment or "
            f"database. This deployment is {recovery.get('state')}: "
            f"{explanation}. "
            + (
                f"Run {command}. "
                if command
                else "publish_recovery names the exact remedy. "
            )
            + "Then fix the failed checks named in failed_check_hints before "
            "upgrading"
        )
    elif hints["manifest_configuration"] and not hints["authority"]:
        fix_hint = (
            "declare the manifest settings named in "
            "failed_check_hints.manifest_configuration, then re-run"
        )
    else:
        fix_hint = default_hint
    return {
        "failed_check_hints": hints,
        "publish_recovery": recovery,
        "fix_hint": fix_hint,
    }


def _phase_backup(ctx: UpgradeContext) -> dict[str, Any]:
    effects = None
    effect_errors: tuple[str, ...] = ()
    effect_report: dict[str, Any] | None = None
    safety_plan: dict[str, Any] | None = None
    if not ctx.options.independent_backup_verify:
        effects, effect_errors, effect_report = _collect_effect_inventory(ctx)
        effect_checks = effects.to_dict() if effects is not None else {}
        refusal_failures = [
            name for name in _REFUSAL_EFFECT_AUTHORITY_CHECKS
            if effect_checks.get(name) is not True
        ]
        safety_plan = _deployment_safety_plan(
            ctx,
            effect_report=effect_report,
            restore_point_source=(
                "unavailable_skip_backup"
                if ctx.options.skip_backup
                else "current_run_verified_backup"
            ),
            restore_point_verified=False,
        )
        if effects is None or effect_errors or refusal_failures:
            decision = {
                "mode": "refuse",
                "reasons": [
                    *(f"effect.{name}" for name in refusal_failures),
                    *effect_errors,
                ],
                "checks": effect_checks,
            }
            guidance = _backup_refusal_guidance(
                ctx, refusal_failures=refusal_failures,
            )
            if ctx.options.plan_only:
                # A plan is an observation, never mutation authority.  Keep
                # the execution refusal fully visible while allowing the
                # remaining mutation-free phase previews to render.  The same
                # inputs still fail closed before backup or database effects
                # when execution is requested.
                return {
                    "ok": True,
                    "planned": "backup admission remains refused",
                    "skipped": "backup skipped (--plan)",
                    "mutation_authorized": False,
                    "backup_policy_decision": decision,
                    "effect_inventory": effect_report,
                    "safety_plan": safety_plan,
                    "failed_checks": refusal_failures,
                    **guidance,
                }
            return {
                "ok": False,
                "reason": "physical rollback admission refused mutation",
                "backup_policy_decision": decision,
                "effect_inventory": effect_report,
                "safety_plan": safety_plan,
                "failed_checks": refusal_failures,
                **guidance,
            }
    if ctx.options.skip_backup:
        first_mutation = (
            "migrate" if not ctx.options.skip_migrate else "catalog_apply"
        )
        stops_before_mutation = (
            ctx.options.stop_after is not None
            and PHASES.index(ctx.options.stop_after) < PHASES.index(first_mutation)
        )
        if ctx.options.plan_only or stops_before_mutation:
            return {
                "ok": True,
                "skipped": "operator requested --skip-backup before mutation",
                "mutation_authorized": False,
                "effect_inventory": effect_report,
                "safety_plan": safety_plan,
            }
        return {
            "ok": False,
            "reason": "--skip-backup cannot authorize database mutation",
            "mutation_authorized": False,
            "effect_inventory": effect_report,
            "safety_plan": safety_plan,
            "fix_hint": (
                "rerun without --skip-backup, use --plan, or stop before the "
                f"first database mutation phase ({first_mutation})"
            ),
        }
    try:
        policy = _backup_policy_config(ctx)
    except SubstrateError as exc:
        return {
            "ok": False,
            "reason": exc.message,
            "error": exc.to_json()["error"],
        }
    ctx.data["backup_policy_config"] = policy
    backup_path, temporary_backup_path = _backup_archive_paths(ctx, policy)
    try:
        database, backup_env = _libpq_command_target(ctx.dsn, ctx.env)
    except Exception as exc:  # noqa: BLE001 - structured backup preflight.
        return {
            "ok": False,
            "reason": "backup DSN is not parseable",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    # Never put a DSN/password on argv: process listings are an operator-visible
    # surface. libpq connection fields travel in the child environment instead.
    cmd = _backup_dump_command(
        database=database,
        artifact_path=temporary_backup_path,
        strategy=policy["dump_strategy"],
    )
    if ctx.options.plan_only:
        return {
            "ok": True,
            "planned": _display_command([
                str(backup_path) if value == str(temporary_backup_path) else value
                for value in cmd
            ]),
            "skipped": "backup skipped (--plan)",
            "backup_policy": policy,
            "effect_inventory": effect_report,
            "safety_plan": safety_plan,
            "next_command": (
                _safety_plan_refusal(ctx, safety_plan or {}).get("next_command")
                if safety_plan is not None
                and _safety_plan_refusal(ctx, safety_plan) is not None
                else _next_command(ctx)
            ),
        }
    if safety_plan is not None:
        safety_refusal = _safety_plan_refusal(ctx, safety_plan)
        if safety_refusal is not None:
            return safety_refusal
    physical_decision = None
    try:
        _persist_cleanup_obligation(
            ctx,
            {
                "kind": "backup_artifacts",
                "action": "remove_temporary_archive_and_restore_database",
                "backup_dir": str(ctx.backup_dir),
                "database_prefix": "okg_upgrade_verify_",
            },
        )
    except (OSError, SubstrateError) as exc:
        return {
            "ok": False,
            "reason": "could not persist backup cleanup obligation",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    disaster_recovery_report: dict[str, Any] = {"status": "not_considered"}
    disaster_recovery_size: int | None = None
    snapshot: dict[str, Any] | None = None
    if not ctx.options.independent_backup_verify:
        assert effects is not None
    toolchain = (
        _backup_tool_preflight(ctx)
        if ctx.options.independent_backup_verify
        else _receipt_bound_backup_toolchain(ctx)
    )
    if ctx.options.independent_backup_verify and not toolchain.get("ok"):
        return toolchain
    if not ctx.options.independent_backup_verify:
        try:
            snapshot = _backup_compatibility_snapshot(ctx, toolchain)
        except Exception as exc:  # noqa: BLE001 - unreadable identity refuses early.
            return {
                "ok": False,
                "reason": "disaster-recovery compatibility state is unreadable",
                "error": {"type": type(exc).__name__, "message": str(exc)},
                "fix_hint": (
                    "repair the compatibility probe, then rerun deployment up"
                ),
            }
        (
            recovery,
            recovery_errors,
            disaster_recovery_report,
            disaster_recovery_size,
        ) = _disaster_recovery_receipt_evidence(ctx, snapshot, policy)
        from okg.substrate.deployment_backup_policy import (
            NO_PHYSICAL_ROLLBACK,
            REFUSE,
            PhysicalRollbackPolicyInput,
            select_physical_rollback_policy,
        )

        manifest = _resolve_upgrade_deployment(ctx)
        release = getattr(manifest, "release", None) or {}
        posture = (
            (getattr(manifest, "nomos", None) or {}).get(
                "deployment_posture"
            )
            or {}
        )
        physical_decision = select_physical_rollback_policy(
            PhysicalRollbackPolicyInput(
                policy_mode=policy["verification_policy"],
                release_channel=release.get("channel"),
                readiness_profile=ctx.options.readiness_profile,
                deployment_class=posture.get("deployment_class"),
                destructive_confirmed=(
                    ctx.options.confirm_destructive_migrations
                ),
                effects=effects,
                recovery=recovery,
                effect_errors=effect_errors,
                recovery_errors=recovery_errors,
            )
        )
        decision_report = {
            "mode": physical_decision.mode,
            "reasons": list(physical_decision.reasons),
            "checks": physical_decision.checks,
            # Distinct from the boolean check: says WHY the TimescaleDB
            # check passed (proven / absent / risk_admitted), so an explicit
            # risk admission is visible in the decision, never silent.
            "timescale_restore_status": disaster_recovery_report.get(
                "timescale_restore_status", "not_evaluated"
            ),
        }
        if physical_decision.mode == REFUSE:
            return {
                "ok": False,
                "reason": "physical rollback admission refused mutation",
                "backup_policy": policy,
                "backup_policy_decision": decision_report,
                "effect_inventory": effect_report,
                "disaster_recovery_receipt": disaster_recovery_report,
                "fix_hint": (
                    "run `okg deployment backup verify "
                    f"{ctx.options.deployment}` and rerun deployment up"
                ),
            }
        if physical_decision.mode == NO_PHYSICAL_ROLLBACK:
            source_database_size = _estimate_source_db_bytes(ctx)
            backup_result = {
                "ok": True,
                "verification_mode": NO_PHYSICAL_ROLLBACK,
                "mutation_authorized": True,
                "physical_backup_started": False,
                "restore_started": False,
                "backup_tool_child_processes_started": 0,
                "backup_tool_validation": toolchain.get("validation"),
                "source_database_size_bytes": source_database_size,
                "budgets_seconds": _selected_backup_budgets(
                    policy,
                    routine=True,
                ),
                "backup_policy": policy,
                "toolchain": toolchain,
                "compatibility_snapshot": snapshot,
                "candidate_plan_digest": snapshot["candidate_plan_digest"],
                "effect_inventory": effect_report,
                "effect_digest": effect_report.get("effect_digest"),
                "disaster_recovery_receipt": disaster_recovery_report,
                "prior_observed_restored_size_bytes": disaster_recovery_size,
                "backup_policy_decision": decision_report,
                "safety_contract": _safety_contract_snapshot(
                    ctx,
                    restore_point_verified=True,
                    phase="backup",
                ),
                "progress": {},
            }
            ctx.data["backup_result"] = backup_result
            if safety_plan is not None:
                backup_result["safety_contract"] = _safety_contract_snapshot(
                    ctx,
                    restore_point_verified=True,
                    phase="backup",
                )
            return backup_result
        # A forced physical path may now start backup children. Re-run the
        # tool preflight through the executable itself and bind the archive to
        # that exact observation. The routine no-physical path returned above
        # without starting pg_dump or pg_restore, including version probes.
        toolchain = _backup_tool_preflight(ctx)
        if not toolchain.get("ok"):
            return toolchain
        try:
            snapshot = _backup_compatibility_snapshot(ctx, toolchain)
        except Exception as exc:  # noqa: BLE001 - forced backup is fail-closed.
            return {
                "ok": False,
                "reason": "backup compatibility preflight is unreadable",
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
        if policy["verification_policy"] != "risk_admitted":
            # The closed-effect kernel is an authority/refusal gate for every
            # mutating mode.  Restore-drill/current-full-restore still use
            # their own archive verification selector after that gate passes.
            physical_decision = None
    if ctx.options.independent_backup_verify:
        try:
            snapshot = _backup_compatibility_snapshot(ctx, toolchain)
        except Exception as exc:  # noqa: BLE001 - independent proof refuses drift.
            return {
                "ok": False,
                "reason": "backup compatibility preflight is unreadable",
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
    source_database_size = _estimate_source_db_bytes(ctx)
    budget_resume = (
        _resumable_budget_verified_backup(
            ctx,
            policy=policy,
            toolchain=toolchain,
            snapshot=snapshot,
            source_database_size=source_database_size,
            effect_inventory=effect_report,
        )
        if not ctx.options.independent_backup_verify and snapshot is not None
        else None
    )
    if budget_resume is not None:
        return budget_resume
    strategy_preflight = _dump_strategy_preflight(
        ctx,
        database=database,
        backup_env=backup_env,
        policy=policy,
        toolchain=toolchain,
    )
    if strategy_preflight.get("ok") is not True:
        declared_strategy = policy["dump_strategy"]
        fallback_strategy = {
            **declared_strategy,
            "format": "custom",
            "compression": "gzip",
            "compression_level": 6,
            "jobs": 1,
        }
        fallback_policy = {**policy, "dump_strategy": fallback_strategy}
        fallback_preflight = _dump_strategy_preflight(
            ctx,
            database=database,
            backup_env=backup_env,
            policy=fallback_policy,
            toolchain=toolchain,
        )
        if fallback_preflight.get("ok") is not True:
            return {
                "ok": False,
                "reason": "declared and serial fallback dump strategies are unsafe",
                "declared_strategy_preflight": strategy_preflight,
                "fallback_strategy_preflight": fallback_preflight,
            }
        strategy_preflight = {
            "ok": True,
            "fallback_applied": True,
            "reason": "declared dump strategy is unsupported or unsafe",
            "declared_strategy": declared_strategy,
            "declared_strategy_preflight": strategy_preflight,
            "effective_strategy": fallback_strategy,
            "fallback_strategy_preflight": fallback_preflight,
        }
        policy = fallback_policy
        ctx.data["backup_policy_config"] = policy
        backup_path, temporary_backup_path = _backup_archive_paths(ctx, policy)
        cmd = _backup_dump_command(
            database=database,
            artifact_path=temporary_backup_path,
            strategy=fallback_strategy,
        )
    command_display = _display_command(cmd)
    resume_candidate = (
        _resumable_independent_archive(
            ctx,
            policy=policy,
            toolchain=toolchain,
            snapshot=snapshot,
        )
        if ctx.options.independent_backup_verify and snapshot is not None
        else None
    )
    if resume_candidate is not None and resume_candidate.get("ok") is not True:
        return resume_candidate
    resumed_archive = resume_candidate is not None
    if resumed_archive:
        assert resume_candidate is not None
        backup_path = resume_candidate["backup_path"]
        archive = resume_candidate["archive"]
        dump_progress = resume_candidate["dump_progress"]
        headroom = resume_candidate["headroom"]
        inode_headroom = resume_candidate["inode_headroom"]
        strategy_preflight = resume_candidate["strategy_preflight"]
        command_display = str(resume_candidate.get("command") or command_display)
    else:
        inode_headroom = _directory_inode_headroom(
            ctx,
            strategy=policy["dump_strategy"],
        )
        if inode_headroom.get("ok") is not True:
            return {
                "ok": False,
                "reason": "insufficient or unprobeable inode headroom for backup",
                "inode_headroom": inode_headroom,
            }
        headroom = _backup_headroom_preflight(
            ctx,
            needed_bytes=source_database_size,
        )
        if headroom.get("refuse"):
            return {
                "ok": False,
                "reason": "insufficient host disk headroom for backup",
                "headroom": headroom,
                "mutation_authorized": False,
                "physical_backup_started": False,
                "next_command": _resume_after_reclaim_command(ctx),
                "fix_hint": (
                    "reclaim host storage, then run the exact next_command "
                    f"({headroom.get('runbook')}); the resumed backup phase "
                    "remeasures storage before any write"
                ),
            }
        from okg.substrate.runtime_dirs import mark_okg_runtime_ancestor

        mark_okg_runtime_ancestor(ctx.backup_dir / "_")
        ctx.backup_dir.mkdir(parents=True, exist_ok=True)
        ctx.backup_dir.chmod(0o700)
        try:
            _persist_cleanup_obligation(
                ctx,
                {
                    "kind": "backup_artifacts",
                    "temporary_archive_path": str(temporary_backup_path),
                },
            )
            _remove_partial_backup_artifact(temporary_backup_path)
            result, dump_progress = _run_long_command(
                ctx,
                cmd,
                backup_env,
                operation="pg_dump",
                progress_interval_seconds=policy["progress_interval_seconds"],
                artifact_path=temporary_backup_path,
            )
        except FileNotFoundError as exc:
            _remove_partial_backup_artifact(temporary_backup_path)
            return {
                "ok": False,
                "reason": "backup tool missing",
                "command": command_display,
                "error": {
                    "type": type(exc).__name__,
                    "message": str(exc),
                },
                "fix_hint": (
                    "install pg_dump and pg_restore with the same major version as "
                    "the Postgres server; production recovery cannot skip its backup"
                ),
            }
        except BaseException:
            _remove_partial_backup_artifact(temporary_backup_path)
            raise
        if not result.ok:
            cleanup = _remove_partial_backup_artifact(temporary_backup_path)
            if _pg_dump_incompatible(result):
                failure = _failed_command(
                    "backup tool incompatible with server",
                    result,
                    command=cmd,
                )
                failure["fix_hint"] = (
                    "install pg_dump with the same major version as the Postgres "
                    "server, verify `pg_dump --version`, and resume without "
                    "disabling the backup phase"
                )
                failure["partial_cleanup"] = cleanup
                return failure
            failure = _failed_command("backup failed", result, command=cmd)
            failure["partial_cleanup"] = cleanup
            return failure
        archive = _finalize_backup_artifact(
            ctx,
            temporary_path=temporary_backup_path,
            final_path=backup_path,
            strategy=policy["dump_strategy"],
        )
        if not archive.get("ok"):
            archive["partial_cleanup"] = _remove_partial_backup_artifact(
                temporary_backup_path
            )
            return archive
    resume_archive = (
        _backup_verify_resume_payload(
            backup_path=backup_path,
            archive=archive,
            policy=policy,
            toolchain=toolchain,
            snapshot=snapshot,
            source_database_size=source_database_size,
            dump_progress=dump_progress,
            headroom=headroom,
            inode_headroom=inode_headroom,
            strategy_preflight=strategy_preflight,
            command=command_display,
        )
        if ctx.options.independent_backup_verify and snapshot is not None
        else None
    )
    post_dump_headroom = _backup_post_dump_headroom_preflight(ctx)
    if post_dump_headroom.get("refuse"):
        return {
            "ok": False,
            "reason": "insufficient host disk headroom after backup",
            "headroom": post_dump_headroom,
            **({"resume_archive": resume_archive} if resume_archive else {}),
            "fix_hint": (
                "reclaim host storage before restore verification or mutation "
                f"({post_dump_headroom.get('runbook')})"
            ),
        }
    if snapshot is None:
        try:
            snapshot = _backup_compatibility_snapshot(ctx, toolchain)
        except Exception as exc:  # noqa: BLE001 - unreadable identity refuses mutation.
            return {
                "ok": False,
                "reason": "backup compatibility preflight is unreadable",
                "error": {"type": type(exc).__name__, "message": str(exc)},
                "fix_hint": "repair the database identity probe and rerun deployment up",
            }

    receipt_checks = None
    receipt_errors: tuple[str, ...] = ()
    receipt_report: dict[str, Any] = {"status": "not_considered"}
    prior_restored_size: int | None = disaster_recovery_size
    if policy["verification_policy"] == "restore_drill":
        (
            receipt_checks,
            receipt_errors,
            receipt_report,
            prior_restored_size,
        ) = _restore_drill_receipt_evidence(ctx, snapshot, policy)
    else:
        (
            _ignored_checks,
            _ignored_errors,
            receipt_report,
            prior_restored_size,
        ) = _restore_drill_receipt_evidence(ctx, snapshot, policy)

    if ctx.options.independent_backup_verify:
        from okg.substrate.deployment_backup_policy import (
            FULL_RESTORE,
            PhysicalRollbackPolicyDecision,
        )

        decision = PhysicalRollbackPolicyDecision(
            mode=FULL_RESTORE,
            reasons=("independent disaster-recovery verification requested",),
            checks={"independent_backup_verify": True},
        )
    elif physical_decision is None:
        from okg.substrate.deployment_backup_policy import (
            REFUSE,
            BackupPolicyInput,
            select_backup_verification,
        )

        manifest = _resolve_upgrade_deployment(ctx)
        release = getattr(manifest, "release", None) or {}
        nomos = getattr(manifest, "nomos", None) or {}
        posture = nomos.get("deployment_posture") or {}
        archive_checks = archive.get("archive_checks")
        current_archive_valid = (
            isinstance(archive_checks, dict)
            and all(
                archive_checks.get(name) is True
                for name in BACKUP_ARCHIVE_CHECKS
            )
        )
        decision = select_backup_verification(BackupPolicyInput(
            policy_mode=policy["verification_policy"],
            release_channel=release.get("channel"),
            readiness_profile=ctx.options.readiness_profile,
            deployment_class=posture.get("deployment_class"),
            preflight_readable=isinstance(ctx.data.get("migrations_before"), dict),
            current_archive_valid=(
                archive.get("ok") is True and current_archive_valid
            ),
            destructive_confirmed=ctx.options.confirm_destructive_migrations,
            candidate_plan_stable=True,
            policy_stable=True,
            receipt=receipt_checks,
            receipt_errors=receipt_errors,
        ))
        if decision.mode == REFUSE:
            return {
                "ok": False,
                "reason": "backup policy refused mutation",
                "backup_policy_decision": {
                    "mode": decision.mode,
                    "reasons": list(decision.reasons),
                    "checks": decision.checks,
                },
            }
    else:
        decision = physical_decision

    verification: dict[str, Any]
    if getattr(decision, "reuse_allowed", False):
        verification = {
            "ok": True,
            "verification_mode": decision.mode,
            "restore_verified": True,
            "restore_current_archive": False,
            "receipt_reused": True,
            "restore_drill_receipt": receipt_report,
        }
    else:
        try:
            verification = _run_current_full_restore_verification(
                ctx,
                backup_path,
                snapshot=snapshot,
                policy=policy,
                source_database_size=source_database_size,
                prior_restored_size=prior_restored_size,
                archive=archive,
            )
        except BaseException as exc:
            if resume_archive is not None:
                try:
                    setattr(exc, "_okg_phase_evidence", {
                        "resume_archive": resume_archive,
                        "reason": "backup restore verification interrupted",
                    })
                except Exception:
                    pass
            raise
        if not verification.get("ok"):
            return {
                **verification,
                **({"resume_archive": resume_archive} if resume_archive else {}),
            }

    combined_progress = {
        "pg_dump": dump_progress,
        **(
            verification.get("progress")
            if isinstance(verification.get("progress"), dict)
            else {}
        ),
    }
    backup_result = {
        "ok": True,
        "backup_path": str(backup_path),
        "command": command_display,
        "toolchain": toolchain,
        "headroom": headroom,
        "post_dump_headroom": post_dump_headroom,
        "source_database_size_bytes": source_database_size,
        "dump_duration_seconds": dump_progress["duration_seconds"],
        "backup_policy": policy,
        "dump_strategy_preflight": strategy_preflight,
        "inode_headroom": inode_headroom,
        "budgets_seconds": _selected_backup_budgets(
            policy,
            routine=False,
        ),
        "resumed_archive": resumed_archive,
        "candidate_plan_digest": snapshot["candidate_plan_digest"],
        "compatibility_snapshot": snapshot,
        "prior_observed_restored_size_bytes": prior_restored_size,
        "backup_policy_decision": {
            "mode": decision.mode,
            "reasons": list(decision.reasons),
            "checks": decision.checks,
            # Why the TimescaleDB check passed (proven / absent /
            # risk_admitted): an explicit risk admission stays visible in
            # the decision, never silent. The forced-physical path may have
            # no drill receipt; the disaster-recovery evaluation's answer
            # is the fallback rather than dropping it from the record.
            "timescale_restore_status": receipt_report.get(
                "timescale_restore_status",
                disaster_recovery_report.get(
                    "timescale_restore_status", "not_evaluated"
                ),
            ),
        },
        **(
            {"effect_inventory": effect_report}
            if effect_report is not None
            else {}
        ),
        **archive,
        **verification,
        **(
            {
                "safety_contract": _safety_contract_snapshot(
                    ctx,
                    restore_point_verified=True,
                    phase="backup",
                )
            }
            if safety_plan is not None
            else {}
        ),
        "progress": combined_progress,
    }
    ctx.data["backup_result"] = backup_result
    if safety_plan is not None:
        backup_result["safety_contract"] = _safety_contract_snapshot(
            ctx,
            restore_point_verified=True,
            phase="backup",
        )
    try:
        resolved = str(backup_path.resolve())
        fingerprint = _backup_archive_metadata_fingerprint(backup_path)
    except OSError:
        fingerprint = None
    if fingerprint is not None:
        # Bank the fingerprint into ``backup_result`` too: it is the only carrier
        # rehydrated on resume, so this is what lets a later revalidation reuse
        # the content digest instead of re-hashing the whole archive.
        _bank_archive_validation(ctx, resolved, fingerprint, backup_result)
    return backup_result


def _backup_policy_config(ctx: UpgradeContext) -> dict[str, Any]:
    """Load strict deployment-up backup policy; omitted means full restore."""
    manifest = _resolve_upgrade_deployment(ctx)
    lifecycle = getattr(manifest, "deployment_up", None)
    if lifecycle is None:
        lifecycle = {}
    if not isinstance(lifecycle, dict):
        raise SubstrateError("deployment_up must be a mapping")
    raw_backup = lifecycle.get("backup")
    if raw_backup is None:
        backup: dict[str, Any] = {}
    elif isinstance(raw_backup, dict):
        backup = raw_backup
    else:
        raise SubstrateError("deployment_up.backup must be a mapping")
    mode = backup.get("verification_policy", "current_full_restore")
    if mode not in {
        "current_full_restore",
        "restore_drill",
        "risk_admitted",
    }:
        raise SubstrateError(
            "deployment backup verification policy is invalid",
            data={"verification_policy": mode},
            fix_hint=(
                "use current_full_restore, restore_drill, or risk_admitted"
            ),
        )

    def positive_number(name: str, default: float | None = None) -> float:
        value = backup.get(name, default)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or float(value) <= 0
        ):
            raise SubstrateError(
                f"deployment_up.backup.{name} must be a finite positive number"
            )
        return float(value)

    interval = positive_number("progress_interval_seconds", 30.0)
    peak_factor = positive_number("restore_peak_factor")
    if peak_factor < 1.25:
        raise SubstrateError(
            "deployment_up.backup.restore_peak_factor must be at least 1.25"
        )
    jobs = backup.get("max_parallel_restore_jobs", 1)
    if isinstance(jobs, bool) or not isinstance(jobs, int) or jobs < 1:
        raise SubstrateError(
            "deployment_up.backup.max_parallel_restore_jobs must be a positive integer"
        )
    dump_format = str(backup.get("dump_format", "custom")).strip().lower()
    if dump_format not in {"custom", "directory"}:
        raise SubstrateError(
            "deployment_up.backup.dump_format must be custom or directory"
        )
    dump_compression = str(
        backup.get("dump_compression", "gzip")
    ).strip().lower()
    if dump_compression not in {"none", "gzip", "lz4", "zstd"}:
        raise SubstrateError(
            "deployment_up.backup.dump_compression is invalid",
            fix_hint="use none, gzip, lz4, or zstd",
        )
    dump_compression_level = backup.get("dump_compression_level", 6)
    if (
        isinstance(dump_compression_level, bool)
        or not isinstance(dump_compression_level, int)
        or dump_compression_level < 0
        or dump_compression_level > 22
    ):
        raise SubstrateError(
            "deployment_up.backup.dump_compression_level must be an integer "
            "between 0 and 22"
        )
    dump_jobs = backup.get("max_parallel_dump_jobs", 1)
    if isinstance(dump_jobs, bool) or not isinstance(dump_jobs, int) or dump_jobs < 1:
        raise SubstrateError(
            "deployment_up.backup.max_parallel_dump_jobs must be a positive integer"
        )
    if dump_format != "directory" and dump_jobs != 1:
        raise SubstrateError(
            "parallel pg_dump requires deployment_up.backup.dump_format: directory"
        )
    connection_margin = backup.get("dump_connection_safety_margin", 2)
    if (
        isinstance(connection_margin, bool)
        or not isinstance(connection_margin, int)
        or connection_margin < 1
    ):
        raise SubstrateError(
            "deployment_up.backup.dump_connection_safety_margin must be a "
            "positive integer"
        )
    lock_wait_seconds = positive_number("dump_lock_wait_timeout_seconds", 30.0)
    timescale_risk_admitted = backup.get("timescale_restore_risk_admitted", False)
    if type(timescale_risk_admitted) is not bool:
        raise SubstrateError(
            "deployment_up.backup.timescale_restore_risk_admitted must be a "
            "boolean",
            data={"timescale_restore_risk_admitted": timescale_risk_admitted},
            fix_hint=(
                "declare true only to explicitly accept reusing a restore "
                "proof that lacks TimescaleDB pre/post-procedure evidence; "
                "the admission is recorded in receipts and decisions"
            ),
        )
    restore_volume = backup.get("restore_volume")
    if not isinstance(restore_volume, dict):
        raise SubstrateError("deployment_up.backup.restore_volume must be a mapping")
    kind = restore_volume.get("kind")
    if kind not in {"docker_data_root", "host_path"}:
        raise SubstrateError(
            "deployment_up.backup.restore_volume.kind is invalid",
            fix_hint="use docker_data_root or host_path",
        )
    if kind == "host_path" and (
        not isinstance(restore_volume.get("path"), str)
        or not restore_volume["path"].strip()
    ):
        raise SubstrateError(
            "deployment_up.backup.restore_volume.path must name a host path"
        )
    if kind == "host_path" and not Path(restore_volume["path"]).is_absolute():
        raise SubstrateError(
            "deployment_up.backup.restore_volume.path must be absolute"
        )

    max_age: float | None = None
    disaster_recovery_archive_max_age: float | None = None
    disaster_recovery_restore_max_age: float | None = None
    recovery_point_objective: float | None = None
    budgets: dict[str, Any] | None = None

    def budget_map(
        value: Any,
        *,
        path: str,
        required_names: tuple[str, ...],
    ) -> dict[str, float]:
        if not isinstance(value, dict):
            raise SubstrateError(f"{path} must be a mapping")
        parsed: dict[str, float] = {}
        for name in required_names:
            number = value.get(name)
            if (
                isinstance(number, bool)
                or not isinstance(number, (int, float))
                or not math.isfinite(float(number))
                or float(number) <= 0
            ):
                raise SubstrateError(
                    f"{path}.{name} must be a finite positive number"
                )
            parsed[name] = float(number)
        return parsed

    def parse_budgets(value: Any, *, required: bool) -> dict[str, Any] | None:
        if value is None:
            if required:
                raise SubstrateError(
                    "restore_drill policy requires "
                    "deployment_up.backup.budgets_seconds"
                )
            return None
        if not isinstance(value, dict):
            raise SubstrateError(
                "deployment_up.backup.budgets_seconds must be a mapping"
            )
        nested = "routine" in value or "forced" in value
        if not nested:
            return budget_map(
                value,
                path="deployment_up.backup.budgets_seconds",
                required_names=("dump", "backup", "total"),
            )
        return {
            "routine": budget_map(
                value.get("routine"),
                path="deployment_up.backup.budgets_seconds.routine",
                required_names=("backup", "attribute_read_model", "total"),
            ),
            "forced": budget_map(
                value.get("forced"),
                path="deployment_up.backup.budgets_seconds.forced",
                required_names=("dump", "backup", "total"),
            ),
        }

    if mode == "restore_drill":
        max_age = positive_number("restore_drill_max_age_seconds")
        budgets = parse_budgets(backup.get("budgets_seconds"), required=True)
    elif mode == "risk_admitted":
        disaster_recovery_archive_max_age = positive_number(
            "disaster_recovery_archive_max_age_seconds"
        )
        disaster_recovery_restore_max_age = positive_number(
            "disaster_recovery_restore_max_age_seconds"
        )
        recovery_point_objective = positive_number(
            "recovery_point_objective_seconds"
        )
        budgets = parse_budgets(backup.get("budgets_seconds"), required=False)
    return {
        "verification_policy": mode,
        "timescale_restore_risk_admitted": timescale_risk_admitted,
        "restore_drill_max_age_seconds": max_age,
        "disaster_recovery_archive_max_age_seconds": (
            disaster_recovery_archive_max_age
        ),
        "disaster_recovery_restore_max_age_seconds": (
            disaster_recovery_restore_max_age
        ),
        "recovery_point_objective_seconds": recovery_point_objective,
        "restore_volume": dict(restore_volume),
        "restore_peak_factor": peak_factor,
        "max_parallel_restore_jobs": jobs,
        "dump_strategy": {
            "format": dump_format,
            "compression": dump_compression,
            "compression_level": dump_compression_level,
            "jobs": dump_jobs,
            "connection_safety_margin": connection_margin,
            "lock_wait_timeout_seconds": lock_wait_seconds,
        },
        "progress_interval_seconds": interval,
        "budgets_seconds": budgets,
    }


def _selected_backup_budgets(
    policy: Mapping[str, Any],
    *,
    routine: bool,
) -> dict[str, float] | None:
    budgets = policy.get("budgets_seconds")
    if not isinstance(budgets, Mapping):
        return None
    selected = budgets.get("routine" if routine else "forced")
    if isinstance(selected, Mapping):
        return {str(key): float(value) for key, value in selected.items()}
    # Backward-compatible manifest shape: one budget set applied to both paths.
    if all(isinstance(value, (int, float)) for value in budgets.values()):
        return {str(key): float(value) for key, value in budgets.items()}
    return None


def _migration_plan_digest(value: Any) -> str:
    if not isinstance(value, dict):
        raise ValueError("migration preflight is not a JSON object")
    encoded = json.dumps(
        _json_safe(value),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(
        _json_safe(value),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _git_command(
    repo_root: Path,
    args: list[str | bytes],
    *,
    binary: bool = False,
    timeout: float = 30,
) -> bytes | str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=not binary,
        timeout=timeout,
    )
    if result.returncode != 0:
        stderr = result.stderr
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")
        display_args = " ".join(
            os.fsdecode(arg) if isinstance(arg, bytes) else str(arg)
            for arg in args
        )
        raise ValueError(
            f"git {display_args} failed: {str(stderr).strip()[-500:]}"
        )
    return result.stdout


CODE_REPO_MODULE_PATH = "src/okg/substrate/deployment_upgrade.py"

# Git names an object by hashing "blob <size>\0<contents>"; the hex length
# of an object id states which hash the repository uses.
_GIT_OBJECT_HASHES = {40: "sha1", 64: "sha256"}
# The only tree-entry modes whose blob content is the file's own content.
# A `120000` symlink is also a blob, but its content is the LINK TARGET, so
# hashing what the installed path reads back compares two different things.
_GIT_REGULAR_FILE_MODES = frozenset({b"100644", b"100755"})

#: Tracked subtrees of ``src/okg`` that the distribution deliberately does
#: not carry, as paths relative to ``src/okg/`` with a trailing slash.
#:
#: The proof below demands that every tracked framework file is present in
#: the installed package. That is exactly right for source it runs, and
#: wrong for source it does not: ``okg/operator_ui/web/`` is the operator
#: UI's TypeScript tree plus its Playwright screenshot baselines -- 210
#: files, 6.2 MB -- built by npm and served from its own origin
#: (``chat.deep_links.operator_ui_base``). No Python in this package reads
#: a byte of it, so a wheel has no reason to carry it, and before this rule
#: every wheel-installed publish failed with ``installed_file_missing``
#: (issue 1949).
#:
#: Be exact about what this gives up: DELETION of a file inside a listed
#: tree is no longer detected. Nothing in the installed package reads those
#: files -- no StaticFiles, FileResponse, importlib.resources, pkgutil or
#: ``__file__`` path reaches operator_ui/web, which the repository-root
#: package.json builds and ``chat.deep_links.operator_ui_base`` serves -- so
#: a deletion there cannot change what a publish does. A file that IS
#: installed, as every source checkout has them all, is byte-compared like
#: any other, and a tampered byte still refuses. Adding a tree here removes
#: it from the proof, so each entry has to be source the runtime never
#: reads, and ``tests/substrate/test_wheel_ships_framework_files.py`` builds
#: the wheel and refuses any difference between this list and what the
#: packaging actually leaves out.
_UNSHIPPED_FRAMEWORK_TREES: tuple[bytes, ...] = (b"operator_ui/web/",)


def _is_unshipped_framework_path(raw_relative: bytes) -> bool:
    """Is this tracked path one the distribution declares it does not ship?"""
    return any(
        raw_relative.startswith(tree) for tree in _UNSHIPPED_FRAMEWORK_TREES
    )


@dataclass(frozen=True)
class FrameworkProvenanceResult:
    """Typed result for the installed-framework byte provenance proof.

    ``mismatch`` is reserved for an evaluated, real content mismatch.  Any
    inability to evaluate Git or the installed package is ``unevaluable`` so
    an infrastructure or encoding failure cannot masquerade as tampering.
    """

    status: str
    reason: str | None = None
    operation: str | None = None
    error_type: str | None = None
    path: str | None = None

    @property
    def matched(self) -> bool:
        return self.status == "match"

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "operation": self.operation,
            "error_type": self.error_type,
            "path": self.path,
        }


def _framework_provenance_unevaluable(
    *,
    reason: str,
    operation: str,
    exc: BaseException | None = None,
    path: str | None = None,
) -> FrameworkProvenanceResult:
    return FrameworkProvenanceResult(
        status="unevaluable",
        reason=reason,
        operation=operation,
        error_type=type(exc).__name__ if exc is not None else None,
        path=path,
    )


def _framework_git_failure(
    *,
    operation: str,
    exc: BaseException,
) -> FrameworkProvenanceResult:
    message = str(exc).lower()
    if "not a git repository" in message:
        reason = "non_git_repository"
    elif operation == "ls_tree" and (
        "bad object" in message
        or "ambiguous argument" in message
        or "unknown revision" in message
        or "invalid object name" in message
        or "not a valid object name" in message
        or "not a tree object" in message
    ):
        reason = "invalid_revision"
    elif isinstance(exc, subprocess.TimeoutExpired):
        reason = "git_timeout"
    else:
        reason = "git_command_failed"
    return _framework_provenance_unevaluable(
        reason=reason,
        operation=operation,
        exc=exc,
    )


def _installed_framework_matches_revision(
    repo_root: Path,
    revision: str,
    running_module: Path,
) -> FrameworkProvenanceResult:
    """Compare the complete installed ``okg`` package with a Git revision."""
    try:
        # ONE git process for the whole framework tree. This proof used to
        # read each blob with its own `git show` -- 1834 of them under
        # `src/okg`, on every publisher construction, because the publisher
        # proves this on the way to recording a generation's repository
        # provenance. `ls-tree` already reports each blob's object id, and
        # git names a blob by hashing `blob <size>\0<contents>`, so the
        # installed file's own object id decides the same comparison over
        # the same bytes -- under the very hash that names the revision this
        # proof is about.
        #
        # `-z` is load-bearing, not a tidy-up. Without it git applies
        # `core.quotePath` and renders any path holding a non-ASCII byte as a
        # QUOTED, octal-escaped string -- `"src/okg/.../Marroqu\303\255n.json"`
        # -- which is not a real path. `Path(...).relative_to("src/okg")` then
        # raises ValueError, the except below turns that into False, and the
        # caller reports "framework release provenance is not proven". So a
        # single accented filename anywhere under src/okg silently disables
        # this proof rather than failing it for a stated reason. Long format
        # rather than `--name-only`, because the object id is the point; it
        # sits before the TAB that starts the pathname, so a pathname holding
        # a TAB stays unambiguous.
        tracked_payload = _git_command(
            repo_root,
            ["ls-tree", "-r", "-z", revision, "src/okg"],
            binary=True,
        )
        if not isinstance(tracked_payload, bytes):
            return _framework_provenance_unevaluable(
                reason="git_output_not_bytes",
                operation="ls_tree",
            )
        tracked = [entry for entry in tracked_payload.split(b"\0") if entry]
        package_root = running_module.parents[1]
        expected_prefix = b"src/okg/"
        pending: list[tuple[bytes, bytes, Path]] = []
        for entry in tracked:
            head, separator, raw_path = entry.partition(b"\t")
            attributes = head.split(b" ")
            if not separator or len(attributes) != 3:
                return _framework_provenance_unevaluable(
                    reason="malformed_tree_entry",
                    operation="ls_tree",
                    path=os.fsdecode(entry),
                )
            mode, kind, object_id = attributes
            if kind == b"blob" and mode not in _GIT_REGULAR_FILE_MODES:
                # A `120000` symlink is a blob whose content is the link
                # target, so hashing the bytes the installed path reads back
                # -- the bytes of whatever it points AT -- would report a
                # mismatch on an untampered install. Any other mode is
                # likewise not a plain file. Fail closed: refuse to evaluate.
                return _framework_provenance_unevaluable(
                    reason="unsupported_tree_entry_mode",
                    operation="ls_tree",
                    path=os.fsdecode(raw_path),
                )
            if kind != b"blob":
                # A gitlink or a subtree cannot be byte-compared with an
                # installed file. Refusing to evaluate is not the same as
                # calling the install tampered with.
                return _framework_provenance_unevaluable(
                    reason="unexpected_tree_entry",
                    operation="ls_tree",
                    path=os.fsdecode(raw_path),
                )
            if _GIT_OBJECT_HASHES.get(len(object_id)) is None or not re.fullmatch(
                rb"[0-9a-f]+", object_id
            ):
                return _framework_provenance_unevaluable(
                    reason="unknown_git_object_format",
                    operation="ls_tree",
                    path=os.fsdecode(raw_path),
                )
            if not raw_path.startswith(expected_prefix) or raw_path == expected_prefix:
                return _framework_provenance_unevaluable(
                    reason="invalid_tree_path",
                    operation="ls_tree",
                    path=os.fsdecode(raw_path),
                )
            # os.fsdecode uses the filesystem's surrogateescape codec, so a
            # Git pathname containing arbitrary bytes round-trips to Path and
            # back to the same bytes when read from the installed checkout.
            raw_relative = raw_path[len(expected_prefix) :]
            if not raw_relative or any(
                part in {b"", b".", b".."} for part in raw_relative.split(b"/")
            ):
                return _framework_provenance_unevaluable(
                    reason="invalid_tree_path",
                    operation="path_decode",
                    path=os.fsdecode(raw_path),
                )
            try:
                relative = Path(os.fsdecode(raw_relative))
            except (OSError, UnicodeError, ValueError) as exc:
                return _framework_provenance_unevaluable(
                    reason="path_decoding_failed",
                    operation="path_decode",
                    exc=exc,
                    path=os.fsdecode(raw_path),
                )
            if relative.is_absolute() or any(
                part in {"", ".", ".."} for part in relative.parts
            ):
                return _framework_provenance_unevaluable(
                    reason="invalid_tree_path",
                    operation="path_decode",
                    path=os.fsdecode(raw_path),
                )
            installed_path = package_root / relative
            try:
                installed_exists = installed_path.is_file()
            except OSError as exc:
                return _framework_provenance_unevaluable(
                    reason="installed_path_stat_failed",
                    operation="stat_installed_file",
                    exc=exc,
                    path=os.fsdecode(raw_path),
                )
            if not installed_exists:
                if _is_unshipped_framework_path(raw_relative):
                    # Declared not to ship (see _UNSHIPPED_FRAMEWORK_TREES).
                    # Absent is the expected state in a wheel install; the
                    # proof neither demands nor claims anything about it.
                    continue
                return FrameworkProvenanceResult(
                    status="mismatch",
                    reason="installed_file_missing",
                    operation="compare_file",
                    path=os.fsdecode(raw_path),
                )
            pending.append((object_id, raw_path, installed_path))
    except subprocess.TimeoutExpired as exc:
        return _framework_provenance_unevaluable(
            reason="git_timeout",
            operation="ls_tree",
            exc=exc,
        )
    except UnicodeError as exc:
        return _framework_provenance_unevaluable(
            reason="decoding_failed",
            operation="ls_tree",
            exc=exc,
        )
    except OSError as exc:
        return _framework_git_failure(operation="ls_tree", exc=exc)
    except (ValueError, subprocess.SubprocessError) as exc:
        return _framework_git_failure(operation="ls_tree", exc=exc)
    if not tracked:
        return _framework_provenance_unevaluable(
            reason="no_tracked_framework_files",
            operation="ls_tree",
        )
    if not pending:
        # Every tracked file was skipped as unshipped-and-absent. Comparing
        # nothing is not proof of anything, so say so rather than returning
        # a vacuous match.
        return _framework_provenance_unevaluable(
            reason="no_comparable_framework_files",
            operation="compare_file",
        )
    return _compare_installed_blobs(pending)


def _compare_installed_blobs(
    pending: list[tuple[bytes, bytes, Path]],
) -> FrameworkProvenanceResult:
    """Decide each tracked file by its Git object id, spawning no process."""
    # No empty-`pending` guard here: the caller returns on every entry it
    # does not append, refuses an empty tree, and refuses an empty `pending`
    # (which an all-unshipped tree can now produce) before it gets here.
    for object_id, raw_path, installed_path in pending:
        try:
            installed = installed_path.read_bytes()
        except OSError as exc:
            return _framework_provenance_unevaluable(
                reason="installed_file_read_failed",
                operation="read_installed_file",
                exc=exc,
                path=os.fsdecode(raw_path),
            )
        try:
            digest = hashlib.new(_GIT_OBJECT_HASHES[len(object_id)])
        except ValueError as exc:
            # A hash this Python build will not produce is a reason to stop,
            # not a reason to call the installed bytes different.
            return _framework_provenance_unevaluable(
                reason="unknown_git_object_format",
                operation="compare_file",
                exc=exc,
                path=os.fsdecode(raw_path),
            )
        digest.update(b"blob %d\0" % len(installed))
        digest.update(installed)
        if digest.hexdigest().encode("ascii") != object_id:
            return FrameworkProvenanceResult(
                status="mismatch",
                reason="installed_file_bytes_differ",
                operation="compare_file",
                path=os.fsdecode(raw_path),
            )
    return FrameworkProvenanceResult(status="match", operation="compare_file")


class RevisionAuthorityError(ValueError):
    """A framework or deployment revision authority could not be proven."""

    def __init__(
        self,
        *,
        authority_role: str,
        repository: str | None,
        requested_revision: str | None,
        resolved_revision: str | None,
        operation: str,
        reason: str,
    ) -> None:
        self.data = {
            "authority_role": authority_role,
            "repository": repository,
            "requested_revision": requested_revision,
            "resolved_revision": resolved_revision,
            "operation": operation,
            "reason": reason,
        }
        super().__init__(
            f"{authority_role} authority failed during {operation}: "
            f"repository={repository!r} requested_revision="
            f"{requested_revision!r} resolved_revision={resolved_revision!r}: "
            f"{reason}"
        )


@dataclass(frozen=True)
class RevisionAuthority:
    """Which repository answers for code, and which answers for the manifest.

    A deployment that vendors okg as a submodule keeps its manifest in the
    outer repository and okg's source in the inner one.  The two are separate
    authorities: substrate source and schema reads belong to the repository
    that contains the running planner module, deployment-manifest reads belong
    to the repository that contains the deployment directory.  For a deployment
    living inside the okg repository both roots are the same path and every
    field below reduces to the single-repository behavior.
    """

    code_repo_root: Path
    code_revision: str
    planner_revision: str
    deployment_repo_root: Path
    deployment_revision: str
    deployment_relative: Path
    execution_checkout_exact: bool
    target_revision_pinned: bool

    @property
    def vendored(self) -> bool:
        return self.code_repo_root != self.deployment_repo_root


def _git_repo_root(start: Path) -> Path:
    text = _git_command(start, ["rev-parse", "--show-toplevel"])
    return Path(str(text).strip()).resolve()


def _git_common_dir(start: Path) -> Path:
    text = _git_command(start, ["rev-parse", "--path-format=absolute", "--git-common-dir"])
    return Path(str(text).strip()).resolve()


def _resolve_repo_authorities(
    deployment_dir: Path,
    *,
    explicit_code_repository: str | None = None,
    requested_code_revision: str | None = None,
) -> tuple[Path, Path]:
    """Resolve (code repo root, deployment repo root) for a deployment.

    The code repository is the Git repository holding the running planner
    module, unless the deployment pin names an explicit local framework
    checkout.  Two linked worktrees of one repository are not a vendored
    install, and remain refused by planner-integrity checks.

    An explicit repository is resolved relative to the deployment repository
    root and is never replaced with the deployment repository after failure.
    Without explicit configuration, an installed package outside Git is
    accepted only when the deployment itself is the framework repository; a
    separate deployment then fails closed instead of inventing a code
    authority.
    """
    try:
        deployment_repo_root = _git_repo_root(deployment_dir)
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        raise RevisionAuthorityError(
            authority_role="deployment",
            repository=str(deployment_dir),
            requested_revision=None,
            resolved_revision=None,
            operation="resolve_deployment_repository",
            reason=str(exc),
        ) from exc

    if explicit_code_repository is not None:
        raw = str(explicit_code_repository).strip()
        if not raw:
            raise RevisionAuthorityError(
                authority_role="framework",
                repository=raw,
                requested_revision=requested_code_revision,
                resolved_revision=None,
                operation="resolve_framework_repository",
                reason="release.pin.code_repository is empty",
            )
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = deployment_repo_root / candidate
        try:
            code_repo_root = _git_repo_root(candidate)
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            raise RevisionAuthorityError(
                authority_role="framework",
                repository=str(candidate.resolve(strict=False)),
                requested_revision=requested_code_revision,
                resolved_revision=None,
                operation="resolve_framework_repository",
                reason=str(exc),
            ) from exc
        planner_path = code_repo_root / CODE_REPO_MODULE_PATH
        if not planner_path.is_file():
            raise RevisionAuthorityError(
                authority_role="framework",
                repository=str(code_repo_root),
                requested_revision=requested_code_revision,
                resolved_revision=None,
                operation="validate_framework_repository",
                reason=(
                    f"required planner path is missing: {CODE_REPO_MODULE_PATH}"
                ),
            )
        return code_repo_root, deployment_repo_root

    module_path = Path(__file__).resolve()
    try:
        candidate = _git_repo_root(module_path.parent)
    except (OSError, ValueError, subprocess.SubprocessError):
        candidate = None
    if candidate is not None:
        try:
            same_repository = (
                _git_common_dir(candidate)
                == _git_common_dir(deployment_repo_root)
            )
        except (OSError, ValueError, subprocess.SubprocessError):
            same_repository = False
        if candidate != deployment_repo_root and same_repository:
            return deployment_repo_root, deployment_repo_root
        if (candidate / CODE_REPO_MODULE_PATH).resolve() == module_path:
            return candidate, deployment_repo_root
    if candidate is None or (
        candidate / CODE_REPO_MODULE_PATH
    ).resolve() != module_path:
        if (deployment_repo_root / CODE_REPO_MODULE_PATH).is_file():
            return deployment_repo_root, deployment_repo_root
        raise RevisionAuthorityError(
            authority_role="framework",
            repository=None,
            requested_revision=None,
            resolved_revision=None,
            operation="discover_framework_repository",
            reason=(
                "running planner is outside a discoverable framework Git "
                "checkout; set release.pin.code_repository"
            ),
        )
    return candidate, deployment_repo_root


def _git_status_paths(line: str) -> str:
    """The path a ``git status --porcelain=v1`` line refers to."""
    path = line[3:] if len(line) > 3 else ""
    # Renames print "old -> new"; the destination is what is dirty.
    _, _, tail = path.partition(" -> ")
    return (tail or path).strip().strip('"').rstrip("/")


def _git_dirty_lines(
    repo_root: Path,
    *,
    exclude: Path | None = None,
) -> tuple[list[str], list[str]]:
    """Tracked porcelain lines and untracked paths, minus one excluded path.

    ``exclude`` is the vendored okg checkout inside the deployment repository.
    Its gitlink moving *is* the upgrade under review, and its contents are
    separately proven by the code repository's own revision and pristineness
    checks, so counting it as deployment-repo dirt would refuse the very
    submodule bump the operator is trying to apply.  Everything else in the
    deployment repository still counts.
    """
    excluded = exclude.as_posix().rstrip("/") if exclude is not None else None
    tracked = [
        line
        for line in str(
            _git_command(
                repo_root,
                ["status", "--porcelain=v1", "--untracked-files=no"],
            )
        ).splitlines()
        if line.strip() and _git_status_paths(line) != excluded
    ]
    untracked = [
        line
        for line in str(
            _git_command(
                repo_root,
                ["ls-files", "--others", "--exclude-standard"],
            )
        ).splitlines()
        if line.strip() and line.strip().rstrip("/") != excluded
    ]
    return tracked, untracked


def _vendored_code_relative(
    code_repo_root: Path,
    deployment_repo_root: Path,
) -> Path | None:
    if code_repo_root == deployment_repo_root:
        return None
    try:
        return code_repo_root.relative_to(deployment_repo_root)
    except ValueError:
        return None


def _git_tracked_dirty(repo_root: Path, *, exclude: Path | None = None) -> bool:
    return bool(_git_dirty_lines(repo_root, exclude=exclude)[0])


def _git_target_revision(
    ctx: UpgradeContext,
    manifest: Any,
) -> RevisionAuthority:
    deployment_dir = Path(manifest.deployment_dir).resolve()
    release = getattr(manifest, "release", None) or {}
    pin = release.get("pin") if isinstance(release.get("pin"), dict) else {}
    deployment_requested = (
        pin.get("deployment_revision")
        if isinstance(pin, dict)
        else None
    )
    explicit_code_repository = (
        pin.get("code_repository")
        if isinstance(pin, dict) and "code_repository" in pin
        else None
    )
    repo_root, deployment_repo_root = _resolve_repo_authorities(
        deployment_dir,
        explicit_code_repository=(
            str(explicit_code_repository)
            if explicit_code_repository is not None
            else None
        ),
        requested_code_revision=(
            str(pin.get("code_revision"))
            if isinstance(pin.get("code_revision"), str)
            else None
        ),
    )
    try:
        head = str(_git_command(repo_root, ["rev-parse", "HEAD"])).strip()
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        raise RevisionAuthorityError(
            authority_role="framework",
            repository=str(repo_root),
            requested_revision=None,
            resolved_revision=None,
            operation="resolve_framework_head",
            reason=str(exc),
        ) from exc
    explicit_precheckout = ctx.options.precheckout_target_revision
    requested = (
        explicit_precheckout
        if explicit_precheckout is not None
        else (
            pin.get("code_revision")
            if (
                release.get("channel") == "prod"
                and isinstance(pin, dict)
                and isinstance(pin.get("code_revision"), str)
                and pin.get("code_revision").strip()
            )
            else head
        )
    )
    if (
        deployment_requested is None
        and explicit_precheckout is not None
        and repo_root == deployment_repo_root
    ):
        # A single-repository precheckout revision owns both the framework and
        # deployment artifacts. Split repositories keep their deployment HEAD
        # independent unless release.pin.deployment_revision is explicit.
        deployment_requested = explicit_precheckout
    if not isinstance(requested, str) or not requested.strip():
        raise RevisionAuthorityError(
            authority_role="framework",
            repository=str(repo_root),
            requested_revision=None,
            resolved_revision=None,
            operation="resolve_framework_revision",
            reason="target code revision is not pinned",
        )
    try:
        resolved = str(
            _git_command(
                repo_root,
                ["rev-parse", f"{requested.strip()}^{{commit}}"],
            )
        ).strip()
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        raise RevisionAuthorityError(
            authority_role="framework",
            repository=str(repo_root),
            requested_revision=requested.strip(),
            resolved_revision=None,
            operation="resolve_framework_revision",
            reason=str(exc),
        ) from exc
    if not re.fullmatch(r"[0-9a-f]{40,64}", resolved):
        raise RevisionAuthorityError(
            authority_role="framework",
            repository=str(repo_root),
            requested_revision=requested.strip(),
            resolved_revision=resolved or None,
            operation="resolve_framework_revision",
            reason="target code revision did not resolve to a full object id",
        )
    if explicit_precheckout is not None and explicit_precheckout != resolved:
        raise RevisionAuthorityError(
            authority_role="framework",
            repository=str(repo_root),
            requested_revision=explicit_precheckout,
            resolved_revision=resolved,
            operation="resolve_framework_revision",
            reason="precheckout target revision is not the exact commit id",
        )
    try:
        deployment_head = str(
            _git_command(deployment_repo_root, ["rev-parse", "HEAD"])
        ).strip()
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        raise RevisionAuthorityError(
            authority_role="deployment",
            repository=str(deployment_repo_root),
            requested_revision=(
                str(deployment_requested).strip()
                if deployment_requested is not None
                else None
            ),
            resolved_revision=None,
            operation="resolve_deployment_head",
            reason=str(exc),
        ) from exc
    deployment_revision = deployment_head
    if deployment_requested is not None:
        if not isinstance(deployment_requested, str) or not deployment_requested.strip():
            raise RevisionAuthorityError(
                authority_role="deployment",
                repository=str(deployment_repo_root),
                requested_revision=None,
                resolved_revision=None,
                operation="resolve_deployment_revision",
                reason="release.pin.deployment_revision is empty",
            )
        try:
            deployment_revision = str(
                _git_command(
                    deployment_repo_root,
                    [
                        "rev-parse",
                        f"{deployment_requested.strip()}^{{commit}}",
                    ],
                )
            ).strip()
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            raise RevisionAuthorityError(
                authority_role="deployment",
                repository=str(deployment_repo_root),
                requested_revision=deployment_requested.strip(),
                resolved_revision=None,
                operation="resolve_deployment_revision",
                reason=str(exc),
            ) from exc
        if not re.fullmatch(r"[0-9a-f]{40,64}", deployment_revision):
            raise RevisionAuthorityError(
                authority_role="deployment",
                repository=str(deployment_repo_root),
                requested_revision=deployment_requested.strip(),
                resolved_revision=deployment_revision or None,
                operation="resolve_deployment_revision",
                reason=(
                    "deployment revision did not resolve to a full object id"
                ),
            )
    tracked_dirty = _git_tracked_dirty(repo_root)
    vendored = repo_root != deployment_repo_root
    if vendored:
        # The manifest is served out of Git, so an uncommitted manifest edit in
        # the outer repository means the planned candidate is not the checkout
        # on disk.  Refuse to call that an exact checkout.
        deployment_dirty = _git_tracked_dirty(
            deployment_repo_root,
            exclude=_vendored_code_relative(repo_root, deployment_repo_root),
        )
    else:
        deployment_dirty = _git_tracked_dirty(deployment_repo_root)
    execution_checkout_exact = (
        resolved == head
        and deployment_revision == deployment_head
        and not tracked_dirty
        and not deployment_dirty
    )
    target_revision_pinned = (
        explicit_precheckout is not None
        or release.get("channel") != "dev"
        or deployment_requested is not None
        or execution_checkout_exact
    )
    return RevisionAuthority(
        code_repo_root=repo_root,
        code_revision=resolved,
        planner_revision=head,
        deployment_repo_root=deployment_repo_root,
        deployment_revision=deployment_revision,
        deployment_relative=deployment_dir.relative_to(deployment_repo_root),
        execution_checkout_exact=execution_checkout_exact,
        target_revision_pinned=target_revision_pinned,
    )


def _planner_integrity_inventory(
    repo_root: Path,
    planner_revision: str,
    deployment_repo_root: Path | None = None,
) -> dict[str, Any]:
    """Bind the running classifier to pristine, revisioned substrate code.

    The target planner deliberately reads candidate data through ``git show`` and
    ``git archive``.  Its implementation, however, is the code in the running
    interpreter.  Merely proving that ``__file__`` lives somewhere under the
    repository would let a dirty production checkout change the classifier while
    still calling it trusted.  Refuse whenever the running module is not the
    canonical source path or any load-bearing substrate source differs from the
    recorded planner revision.

    ``deployment_repo_root`` names the repository holding the deployment
    directory.  For an in-repo deployment it is the same repository and the
    checks below already cover it.  For a vendored install it is a second
    repository whose manifest, schemas, and ontology overrides are also read out
    of Git while the apply phase composes them from disk, so it gets the same
    tracked- and untracked-pristine requirement.  Without that, a vendored
    deployment would be admitted with uncommitted catalog files that the in-repo
    case refuses -- and unlike ``execution_checkout_exact`` this check binds on
    every release channel.
    """
    expected_module = (repo_root / CODE_REPO_MODULE_PATH).resolve()
    running_module = Path(__file__).resolve()
    head = str(_git_command(repo_root, ["rev-parse", "HEAD"])).strip()
    tracked_lines, untracked_lines = _git_dirty_lines(repo_root)
    canonical_module_path = running_module == expected_module
    planner_source_matches_revision = canonical_module_path
    provenance = FrameworkProvenanceResult(
        status="match" if canonical_module_path else "unevaluable",
        reason="canonical_module_path" if canonical_module_path else None,
        operation="repository_identity",
    )
    if not canonical_module_path:
        # A wheel/virtualenv install has no Git parent for __file__. It can be
        # trusted only when its running planner bytes exactly match the pinned
        # framework tree. A sibling worktree is deliberately not accepted by
        # this byte check: it is still a Git checkout with a different common
        # repository identity and must retain the historical refusal.
        running_repo: Path | None = None
        try:
            running_repo = _git_repo_root(running_module.parent)
        except (OSError, ValueError, subprocess.SubprocessError):
            running_repo = None
        same_repository = False
        if running_repo is not None:
            try:
                same_repository = _git_common_dir(running_repo) == _git_common_dir(
                    repo_root
                )
            except (OSError, ValueError, subprocess.SubprocessError):
                same_repository = False
        if not same_repository:
            provenance = _installed_framework_matches_revision(
                repo_root,
                planner_revision,
                running_module,
            )
            planner_source_matches_revision = provenance.matched
        else:
            provenance = FrameworkProvenanceResult(
                status="unevaluable",
                reason="running_module_is_a_sibling_checkout",
                operation="repository_identity",
            )
    checks = {
        "canonical_module_path": canonical_module_path,
        "planner_source_matches_revision": planner_source_matches_revision,
        "planner_revision_is_head": planner_revision == head,
        "tracked_worktree_pristine": not tracked_lines,
        "untracked_worktree_pristine": not untracked_lines,
    }
    dirty_paths = [
        *tracked_lines,
        *(f"?? {path}" for path in untracked_lines),
    ]
    if deployment_repo_root is not None and deployment_repo_root != repo_root:
        exclude = _vendored_code_relative(repo_root, deployment_repo_root)
        outer_tracked, outer_untracked = _git_dirty_lines(
            deployment_repo_root,
            exclude=exclude,
        )
        checks["deployment_tracked_worktree_pristine"] = not outer_tracked
        checks["deployment_untracked_worktree_pristine"] = not outer_untracked
        dirty_paths.extend(outer_tracked)
        dirty_paths.extend(f"?? {path}" for path in outer_untracked)
    return {
        "planner_revision": planner_revision,
        "running_module": str(running_module),
        "expected_module": str(expected_module),
        "dirty_paths": dirty_paths,
        "checks": checks,
        # canonical_module_path is retained as an operator-facing diagnostic,
        # but matching installed package bytes is an equally strong source
        # provenance proof when the package is outside Git.
        "framework_provenance": provenance.to_dict(),
        "trusted": all(
            value
            for key, value in checks.items()
            if key != "canonical_module_path"
        ),
    }


# A symlink may point at another symlink.  Each closure round archives the
# targets the previous round discovered, so the bound is the longest link chain
# the planner will follow before it calls the tree pathological and refuses.
CANDIDATE_TREE_LINK_ROUNDS = 8


def _candidate_link_target(member: tarfile.TarInfo) -> str:
    """Repository-relative path a tracked symlink member points at.

    Git tracks symlinks as first-class objects (mode 120000), so a repository
    may legitimately keep one copy of a schema and link to it from a deployment
    directory.  The planner is willing to follow such a link, but only back into
    the repository the link came from: an absolute target, or one that climbs
    above the repository root, is refused rather than followed.
    """
    link = PurePosixPath(member.linkname)
    parts: list[str] = []
    escapes = link.is_absolute()
    if not escapes:
        # PurePosixPath drops "." components when joining, so only ".." needs
        # unwinding here -- a guard for "." would never run.
        for part in (PurePosixPath(member.name).parent / link).parts:
            if part == "..":
                if not parts:
                    escapes = True
                    break
                parts.pop()
                continue
            parts.append(part)
    if escapes or not parts:
        raise ValueError(
            "target Git tree contains a symlink pointing outside the repository: "
            f"{member.name} -> {member.linkname}; replace it with the file it "
            "points at, or repoint it inside the repository"
        )
    target = "/".join(parts)
    # A link at `a/b` pointing to `a` (or to `a/b` itself) would materialize by
    # copying its own container into itself. Terminating is not the same as
    # meaningful; refuse rather than write a self-nested tree.
    if member.name == target or member.name.startswith(f"{target}/"):
        raise ValueError(
            "target Git tree contains a symlink pointing at itself or its own "
            f"parent: {member.name} -> {member.linkname}; repoint it at a "
            "sibling path, or replace it with the file it points at"
        )
    return target


def _scan_candidate_archive_members(
    archive: tarfile.TarFile,
) -> tuple[list[tarfile.TarInfo], list[tarfile.TarInfo]]:
    """Split archive members into (regular, symlink), refusing anything else.

    The path tests are tar-slip defenses and apply to every member.  The kind
    test exists to keep device nodes and FIFOs out of the candidate tree; it
    deliberately does not reject symlinks, which ``git archive`` emits for a
    tracked link and which the apply path would follow on disk anyway.
    """
    regular: list[tarfile.TarInfo] = []
    symlinks: list[tarfile.TarInfo] = []
    for member in archive.getmembers():
        path = PurePosixPath(member.name)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError(
                f"target Git tree contains unsafe archive member: {member.name}; "
                "a tracked path may not be absolute or contain '..' -- rename it"
            )
        if member.isfile() or member.isdir():
            regular.append(member)
        elif member.issym():
            symlinks.append(member)
        else:
            raise ValueError(
                "target Git tree contains an archive member that is neither a "
                f"file, a directory, nor a symlink: {member.name}; remove it "
                "from the tracked tree"
            )
    return regular, symlinks


def _extract_candidate_tree(
    *,
    repo_root: Path,
    revision: str,
    paths: Sequence[str],
    destination: Path,
    owners: dict[str, Path],
) -> None:
    """Project ``paths`` at ``revision`` into ``destination``, following links.

    The candidate tree exists so the planner can compose and fingerprint the
    catalog the target revision would apply.  For that to mean anything the
    projection has to match what the apply path later reads off disk, where
    ``open()`` follows symlinks.  So a tracked link is neither refused (which
    stopped ``okg up`` dead on any repository holding one) nor skipped (which
    would fingerprint fewer schemas than the revision actually has -- a wrong
    plan reported as a good one).  It is resolved: the link's target is pulled
    out of the same repository at the same revision, and the link is replaced by
    its target's content, so the tree that lands contains no links at all.

    ``owners`` maps an already-materialized member name to the repository that
    produced it.  In the vendored layout two repositories write into one tree;
    a link closure from one repository that would land on the other's path is a
    collision, and refusing it preserves the same guarantee as the deployment /
    ontology overlap check.
    """
    root = destination.resolve()
    pending = list(dict.fromkeys(paths))
    # Paths whose COMPLETE subtree has been archived. `git archive -- X` yields
    # all of X, so anything at or under a covered path is fully materialized.
    # Presence on disk is NOT the same test: a path can exist merely because it
    # was an intermediate directory entry of some other pathspec, and copying
    # that as if it were the whole subtree is a silently truncated candidate
    # tree -- the exact failure this projection exists to prevent.
    covered: set[str] = set()
    links: dict[str, str] = {}
    for round_index in range(CANDIDATE_TREE_LINK_ROUNDS):
        if not pending:
            break
        if round_index:
            _require_tracked_link_targets(
                repo_root=repo_root,
                revision=revision,
                targets=pending,
                links=links,
            )
        payload = _git_command(
            repo_root,
            ["archive", "--format=tar", revision, "--", *pending],
            binary=True,
            timeout=60,
        )
        if not isinstance(payload, bytes):
            raise ValueError(
                f"git archive of {', '.join(sorted(pending))} did not return "
                "bytes; re-run with a working git, or file a bug"
            )
        covered.update(pending)
        pending = []
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:") as archive:
            regular, symlinks = _scan_candidate_archive_members(archive)
            fresh: list[tarfile.TarInfo] = []
            for member in regular:
                # Directory entries are shared prefixes, not content; only a
                # file carries authority worth binding to one repository.
                if member.isdir():
                    fresh.append(member)
                elif _claim_candidate_member(member.name, repo_root, owners):
                    fresh.append(member)
            archive.extractall(destination, members=fresh, filter="data")
        for member in symlinks:
            target = _candidate_link_target(member)
            if not _claim_candidate_member(member.name, repo_root, owners):
                # Same repository, seen before -- a later round re-archived the
                # directory holding it. Its target was queued the first time.
                # Anything else would leave a link unresolved and unnoticed.
                if member.name not in links:
                    raise ValueError(
                        f"target Git tree symlink {member.name} was claimed "
                        "without being recorded; refusing a candidate tree that "
                        "may be missing its target"
                    )
                continue
            # A link resolves against the repository that carries it, so it
            # must not reach a path the other repository already materialized:
            # the target already being on disk is not permission to read it.
            _require_link_target_authority(member.name, target, repo_root, owners)
            links[member.name] = target
            path = destination / member.name
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.is_symlink() or path.exists():
                raise ValueError(
                    f"target Git tree symlink {member.name} -> {target} lands on "
                    "a path the candidate tree already holds; rename one of "
                    "them, or replace the symlink with the file it points at"
                )
            os.symlink(member.linkname, path)
            if not _candidate_target_covered(target, covered) and target not in pending:
                pending.append(target)
    if pending:
        unresolved = sorted(
            name for name, target in links.items() if target in pending
        )
        raise ValueError(
            "target Git tree contains a symlink chain longer than the "
            f"{CANDIDATE_TREE_LINK_ROUNDS - 1} links the planner will follow; "
            f"still unresolved: {', '.join(unresolved)} -> "
            f"{', '.join(sorted(pending))}; shorten the chain, or replace the "
            "first link with the file it ultimately points at"
        )
    for name in sorted(links, key=lambda value: value.count("/"), reverse=True):
        _materialize_candidate_link(destination / name, name, links[name], root)
    _require_no_surviving_symlinks(destination, root)


def _candidate_target_covered(target: str, covered: set[str]) -> bool:
    """True when ``target`` sits inside a subtree that was archived whole."""
    return any(
        target == path or target.startswith(f"{path}/")
        for path in covered
    )


def _require_no_surviving_symlinks(destination: Path, root: Path) -> None:
    """Post-condition: the projection replaced every link with its content.

    Downstream readers follow links, and two of them swallow the read error
    (``catalog/modules_compose.py`` returns on ``OSError``), so a link that
    survived materialization would be read as "nothing here" rather than as a
    failure. Checking the post-condition costs one walk and turns any future
    gap in the closure into a refusal instead of a quietly short catalog.
    """
    survivors = sorted(
        str(path.relative_to(root))
        for path in destination.resolve().rglob("*")
        if path.is_symlink()
    )
    if survivors:
        raise ValueError(
            "target Git tree projection left unresolved symlinks in the "
            f"candidate tree: {', '.join(survivors)}; this is a planner bug -- "
            "file it with `okg deployment bug-report`"
        )


def _claim_candidate_member(
    name: str,
    repo_root: Path,
    owners: dict[str, Path],
) -> bool:
    """Record which repository owns ``name``; False when already materialized."""
    owner = owners.get(name)
    if owner is None:
        owners[name] = repo_root
        return True
    if owner != repo_root:
        raise ValueError(
            "target Git tree link closure crosses repository authority: "
            f"{name} is already materialized from {owner}, refusing to overwrite "
            f"it from {repo_root}; keep the two repositories' trees disjoint"
        )
    return False


def _require_link_target_authority(
    name: str,
    target: str,
    repo_root: Path,
    owners: Mapping[str, Path],
) -> None:
    """Refuse a link that reaches content another repository materialized."""
    prefix = f"{target}/"
    for owned, owner in owners.items():
        if owner == repo_root:
            continue
        if owned == target or owned.startswith(prefix):
            raise ValueError(
                "target Git tree link closure crosses repository authority: "
                f"{name} -> {target} would read {owned}, which was materialized "
                f"from {owner}, not from {repo_root}; repoint the link inside "
                "its own repository, or replace it with the file it points at"
            )


def _require_tracked_link_targets(
    *,
    repo_root: Path,
    revision: str,
    targets: Sequence[str],
    links: Mapping[str, str],
) -> None:
    """Refuse before archiving when a link target is not tracked at ``revision``."""
    for target in targets:
        try:
            _git_command(repo_root, ["cat-file", "-e", f"{revision}:{target}"])
        except ValueError as exc:
            sources = sorted(name for name, value in links.items() if value == target)
            raise ValueError(
                "target Git tree contains a symlink whose target is not tracked at "
                f"the planned revision: {', '.join(sources) or '<unknown>'} -> "
                f"{target}; commit the target or replace the symlink with the file "
                "it points at"
            ) from exc


def _materialize_candidate_link(
    path: Path,
    name: str,
    target: str,
    root: Path,
) -> None:
    """Replace the extracted symlink at ``path`` with its target's content."""
    try:
        # A cycle raises RuntimeError, not OSError -- pathlib translates ELOOP.
        # Letting that escape would surface as a bare
        # `effect_inventory_unreadable:RuntimeError` naming neither the link
        # nor the remedy, which is the failure mode this change exists to end.
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ValueError(
            f"target Git tree symlink {name} -> {target} does not resolve to a "
            f"tracked path ({exc}); replace it with the file it points at, or "
            "repoint it at a committed path"
        ) from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(
            "target Git tree contains a symlink pointing outside the repository: "
            f"{name} -> {target} resolved to {resolved}; repoint it inside the "
            "repository, or replace it with the file it points at"
        ) from exc
    if resolved.is_dir():
        path.unlink()
        shutil.copytree(resolved, path, symlinks=False)
        return
    if resolved.is_file():
        payload = resolved.read_bytes()
        path.unlink()
        path.write_bytes(payload)
        return
    raise ValueError(
        f"target Git tree symlink {name} -> {target} resolves to {resolved}, "
        "which is neither a file nor a directory; replace it with the file it "
        "points at"
    )


def _candidate_tree_path(
    *,
    candidate_root: Path,
    base: Path,
    value: Any,
    label: str,
    kind: str,
) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"target {label} path is empty or invalid")
    relative = Path(value)
    if (
        relative.is_absolute()
        or relative in {Path("."), Path("")}
        or ".." in relative.parts
    ):
        raise ValueError(f"target {label} path escapes candidate Git tree")
    resolved = (base / relative).resolve(strict=False)
    try:
        resolved.relative_to(candidate_root.resolve())
    except ValueError as exc:
        raise ValueError(f"target {label} path escapes candidate Git tree") from exc
    if resolved.is_symlink():
        raise ValueError(f"target {label} path is a symlink")
    if kind == "file" and not resolved.is_file():
        raise ValueError(f"target {label} file is unavailable")
    if kind == "directory" and not resolved.is_dir():
        raise ValueError(f"target {label} directory is unavailable")
    return resolved


def _validate_candidate_catalog_paths(
    *,
    candidate_root: Path,
    deployment_dir: Path,
    modules_root: Path,
    raw_manifest: Mapping[str, Any],
) -> tuple[Path, Path]:
    registry_path = _candidate_tree_path(
        candidate_root=candidate_root,
        base=deployment_dir,
        value=raw_manifest.get("source_registry", "source_registry.yaml"),
        label="source_registry",
        kind="file",
    )
    schema_dir = _candidate_tree_path(
        candidate_root=candidate_root,
        base=deployment_dir,
        value=raw_manifest.get("schema_dir", "schemas"),
        label="schema_dir",
        kind="directory",
    )
    sibling_file_keys = (
        "enricher_registry",
        "taxonomy",
        "aliases",
        "link_config",
        "alias_resolver",
    )
    for key in sibling_file_keys:
        value = raw_manifest.get(key)
        if isinstance(value, str):
            _candidate_tree_path(
                candidate_root=candidate_root,
                base=deployment_dir,
                value=value,
                label=key,
                kind="file",
            )

    modules = raw_manifest.get("modules") or []
    if not isinstance(modules, list):
        raise ValueError("target modules must be a list")
    from okg.substrate.catalog.module_manifest import load_manifest
    from okg.substrate.catalog.module_providers import BUILTIN_OWNER, parse_module_ref
    from okg.substrate.deployments import resolve_declared_module_providers

    # A module the deployment's own `module_providers:` supplies lives under
    # the declared directory inside the candidate deployment tree, not under
    # the target ontology library. Resolving every entry against the library
    # refused those modules before composition was ever reached (issue #1933).
    declared_roots = resolve_declared_module_providers(
        raw_manifest,
        deployment_dir,
        manifest_path=deployment_dir / "deployment.yaml",
    )

    for index, module_name in enumerate(modules):
        # A bare entry keeps the original path exactly, including the refusals
        # `_candidate_tree_path` raises for a name that escapes the tree. Only
        # an owner-qualified entry, which could not resolve here at all
        # before, consults the declaration.
        provider_root = modules_root
        bare_name = module_name
        if isinstance(module_name, str) and ":" in module_name:
            owner, bare_name = parse_module_ref(module_name)
            if owner != BUILTIN_OWNER:
                if owner not in declared_roots:
                    # No fallback to the library: an owner with no provider is
                    # a refusal here, where the operator still reads which
                    # module named it.
                    raise ValueError(
                        f"target modules[{index}] names owner {owner!r}, "
                        "which the target deployment declares no module "
                        "provider for"
                    )
                provider_root = declared_roots[owner]
        module_dir = _candidate_tree_path(
            candidate_root=candidate_root,
            base=provider_root,
            value=bare_name,
            label=f"modules[{index}]",
            kind="directory",
        )
        _candidate_tree_path(
            candidate_root=candidate_root,
            base=module_dir,
            value="schema.yaml",
            label=f"modules[{index}].schema",
            kind="file",
        )
        manifest_path = module_dir / "module.yaml"
        if not manifest_path.exists():
            continue
        if manifest_path.is_symlink() or not manifest_path.is_file():
            raise ValueError(
                f"target modules[{index}].module_manifest is not a regular file"
            )
        manifest = load_manifest(manifest_path)
        referenced = [
            ("provides.subtypes_file", manifest.provides.subtypes_file),
            ("provides.invariants_preset", manifest.provides.invariants_preset),
            ("provides.dashboards", manifest.provides.dashboards),
            *(
                ("provides.intra_cross_link_rules", value)
                for value in manifest.provides.intra_cross_link_rules
            ),
            *(
                (f"extends.{target}.cross_link_rules", value)
                for target, block in manifest.extends.items()
                for value in block.cross_link_rules
            ),
        ]
        for field_name, value in referenced:
            if value is None:
                continue
            _candidate_tree_path(
                candidate_root=candidate_root,
                base=module_dir,
                value=value,
                label=f"modules[{index}].{field_name}",
                kind="file",
            )
    return registry_path, schema_dir


def _normalized_source_registry_snapshot(
    registry_path: Path,
    *,
    repo_root: Path,
) -> dict[str, dict[str, Any]]:
    from okg.substrate.sources.registry import load_source_registry

    loaded_registry = load_source_registry(
        registry_path,
        repo_root_override=repo_root,
    )
    return {
        name: {
            "ownership_id": entry.ownership_id,
            "source_class": entry.source_class,
            "source_revision_kind": entry.source_revision_kind,
            "publication_mode": entry.publication_mode,
            "admission_policy": (
                entry.admission_policy.as_dict()
                if entry.admission_policy is not None
                else None
            ),
            "raw_admission_policy": (
                entry.admission_policy.raw_dict()
                if entry.admission_policy is not None
                else None
            ),
        }
        for name, entry in loaded_registry.items()
    }


def _target_required_extensions(repo_root: Path, revision: str) -> dict[str, str]:
    path = "src/okg/substrate/db/migrate.py"
    source = _git_command(repo_root, ["show", f"{revision}:{path}"])
    tree = ast.parse(str(source), filename=path)
    for node in tree.body:
        target_name: str | None = None
        value: ast.expr | None = None
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            target_name = node.target.id
            value = node.value
        elif isinstance(node, ast.Assign) and len(node.targets) == 1:
            if isinstance(node.targets[0], ast.Name):
                target_name = node.targets[0].id
                value = node.value
        if target_name != "REQUIRED_EXTENSIONS" or value is None:
            continue
        raw = ast.literal_eval(value)
        if not isinstance(raw, dict) or not all(
            isinstance(name, str) and isinstance(version, str)
            for name, version in raw.items()
        ):
            break
        return dict(sorted(raw.items()))
    raise ValueError("target REQUIRED_EXTENSIONS is not a literal string mapping")


def _target_migration_checksum_compatibilities(
    repo_root: Path,
    revision: str,
) -> dict[str, dict[str, Any]]:
    path = "src/okg/substrate/db/management.py"
    source = _git_command(repo_root, ["show", f"{revision}:{path}"])
    tree = ast.parse(str(source), filename=path)
    constants: dict[str, object] = {}
    compatibility_node: ast.Dict | None = None
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = (
                node.targets
                if isinstance(node, ast.Assign)
                else [node.target]
            )
            value = node.value
            if value is None:
                continue
            for target in targets:
                if not isinstance(target, ast.Name):
                    continue
                if (
                    target.id == "_ORDERED_MIGRATION_CHECKSUM_COMPATIBILITIES"
                    and isinstance(value, ast.Dict)
                ):
                    compatibility_node = value
                    continue
                try:
                    constants[target.id] = ast.literal_eval(value)
                except (TypeError, ValueError):
                    continue
    if compatibility_node is None:
        raise ValueError("target migration checksum compatibility map is unreadable")
    normalized: dict[str, dict[str, Any]] = {}
    for migration_key, entry_node in zip(
        compatibility_node.keys,
        compatibility_node.values,
        strict=True,
    ):
        migration_id = (
            str(constants.get(migration_key.id))
            if isinstance(migration_key, ast.Name)
            else str(ast.literal_eval(migration_key))
        )
        entry_value = ast.literal_eval(entry_node)
        if not isinstance(entry_value, dict):
            raise ValueError("target migration compatibility entry is malformed")
        recorded = entry_value.get("compatible_recorded_checksums")
        if not isinstance(recorded, (set, list, tuple)):
            raise ValueError("target compatible checksum set is malformed")
        normalized[migration_id] = {
            "canonical_checksum": str(entry_value.get("canonical_checksum") or ""),
            "compatible_recorded_checksums": sorted(str(value) for value in recorded),
            "reason": str(entry_value.get("reason") or ""),
        }
    return normalized


def _target_known_migrations(
    repo_root: Path,
    revision: str,
) -> list[dict[str, Any]]:
    schema = str(
        _git_command(
            repo_root,
            ["show", f"{revision}:src/okg/substrate/db/schema.sql"],
        )
    )
    views = str(
        _git_command(
            repo_root,
            ["show", f"{revision}:src/okg/substrate/db/views.sql"],
        )
    )
    snapshot_checksum = hashlib.sha256(
        (schema + "\n-- views.sql\n" + views).encode("utf-8")
    ).hexdigest()
    migrations: list[dict[str, Any]] = [{
        "migration_id": "0000_schema_snapshot",
        "checksum": snapshot_checksum,
        "destructive": False,
    }]
    names = str(
        _git_command(
            repo_root,
            [
                "ls-tree",
                "-r",
                "--name-only",
                revision,
                "--",
                "src/okg/substrate/db/migrations",
            ],
        )
    ).splitlines()
    pattern = re.compile(r"^(\d{4})_(.+)\.sql$")
    for full_name in sorted(names):
        name = Path(full_name).name
        match = pattern.match(name)
        if match is None or name.endswith(".down.sql") or match.group(1) == "0000":
            continue
        body = _git_command(repo_root, ["show", f"{revision}:{full_name}"])
        slug = match.group(2)
        migrations.append({
            "migration_id": f"{match.group(1)}_{slug}",
            "checksum": hashlib.sha256(str(body).encode("utf-8")).hexdigest(),
            "destructive": slug.startswith(("retire_", "drop_", "remove_")),
        })
    return migrations


def _live_migration_and_extension_inventory(
    ctx: UpgradeContext,
    *,
    target_migrations: list[dict[str, Any]],
    checksum_compatibilities: dict[str, dict[str, Any]],
    required_extensions: dict[str, str],
) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect
    from psycopg.rows import dict_row

    with db_connect(ctx.dsn, connect_timeout=5) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT to_regclass('okg.schema_migrations') AS ledger")
            ledger_exists = cur.fetchone()["ledger"] is not None
            applied: dict[str, str] = {}
            if ledger_exists:
                cur.execute(
                    "SELECT migration_id, checksum FROM okg.schema_migrations"
                )
                applied = {
                    str(row["migration_id"]): str(row["checksum"])
                    for row in cur.fetchall()
                }
            cur.execute(
                "SELECT extname, extversion FROM pg_extension ORDER BY extname"
            )
            installed_extensions = {
                str(row["extname"]): str(row["extversion"])
                for row in cur.fetchall()
            }
    pending = [
        migration
        for migration in target_migrations
        if migration["migration_id"] not in applied
    ]
    mismatches: list[dict[str, Any]] = []
    compatible_mismatches: list[dict[str, Any]] = []
    for migration in target_migrations:
        migration_id = migration["migration_id"]
        recorded = applied.get(migration_id)
        if recorded is None or recorded == migration["checksum"]:
            continue
        compatibility = checksum_compatibilities.get(migration_id)
        compatible = (
            isinstance(compatibility, dict)
            and compatibility.get("canonical_checksum") == migration["checksum"]
            and recorded in compatibility.get("compatible_recorded_checksums", [])
        )
        row = {
            "migration_id": migration_id,
            "recorded_checksum": recorded,
            "expected_checksum": migration["checksum"],
        }
        if compatible:
            compatible_mismatches.append({
                **row,
                "reason": compatibility.get("reason"),
            })
        else:
            mismatches.append(row)

    def version_tuple(value: str) -> tuple[int, ...]:
        numbers = re.findall(r"\d+", value)
        return tuple(int(number) for number in numbers[:3]) or (0,)

    extension_work = [
        {
            "extension": name,
            "required_minimum": minimum,
            "installed": installed_extensions.get(name),
        }
        for name, minimum in required_extensions.items()
        if name not in installed_extensions
        or version_tuple(installed_extensions[name]) < version_tuple(minimum)
    ]
    return {
        "ledger_exists": ledger_exists,
        "target_migrations": target_migrations,
        "pending": pending,
        "checksum_mismatches": mismatches,
        "checksum_compatibilities": compatible_mismatches,
        "required_extensions": required_extensions,
        "installed_extensions": dict(sorted(installed_extensions.items())),
        "extension_work": extension_work,
    }


def _target_catalog_and_nomos_inventory(
    ctx: UpgradeContext,
    *,
    repo_root: Path,
    revision: str,
    deployment_repo_root: Path,
    deployment_revision: str,
    deployment_relative: Path,
    module_providers: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ontologies_path = "src/okg/substrate/library/ontologies"
    deployment_path = deployment_relative.as_posix()
    # One archive when a single repository owns both the deployment directory
    # and okg's source; two when okg is vendored, each addressed to the
    # repository that actually contains the tree.
    if repo_root == deployment_repo_root:
        archives = [(repo_root, revision, [deployment_path, ontologies_path])]
    else:
        # Two archives land in one candidate tree, so the deployment directory
        # must not overlap okg's ontology tree -- otherwise the second
        # extraction would silently overwrite proven code-repo content.
        overlapping = (
            deployment_relative == Path(".")
            or deployment_path in {"", "."}
            or deployment_path == ontologies_path
            or deployment_path.startswith(f"{ontologies_path}/")
            or ontologies_path.startswith(f"{deployment_path}/")
        )
        if overlapping:
            raise ValueError(
                "vendored deployment directory overlaps the okg ontology tree "
                "or framework root: "
                f"{deployment_path or '.'}"
            )
        archives = [
            (repo_root, revision, [ontologies_path]),
            (deployment_repo_root, deployment_revision, [deployment_path]),
        ]
    with tempfile.TemporaryDirectory(prefix="okg-target-plan-") as temp_name:
        temp_root = Path(temp_name)
        # One shared ownership ledger across both archives: link closure inside
        # either repository must not land on a path the other one already wrote.
        owners: dict[str, Path] = {}
        for archive_repo, archive_revision, archive_paths in archives:
            _extract_candidate_tree(
                repo_root=archive_repo,
                revision=archive_revision,
                paths=archive_paths,
                destination=temp_root,
                owners=owners,
            )
        deployment_dir = temp_root / deployment_relative
        manifest_path = deployment_dir / "deployment.yaml"
        raw_manifest = fastyaml.safe_load(manifest_path.read_text()) or {}
        if not isinstance(raw_manifest, dict):
            raise ValueError("target deployment manifest is not a mapping")
        modules_root = temp_root / "src/okg/substrate/library/ontologies"
        registry_path, _schema_dir = _validate_candidate_catalog_paths(
            candidate_root=temp_root,
            deployment_dir=deployment_dir,
            modules_root=modules_root,
            raw_manifest=raw_manifest,
        )
        raw_registry = fastyaml.safe_load(registry_path.read_text()) or {}
        if not isinstance(raw_registry, dict):
            raise ValueError("target source registry is not a mapping")
        normalized_registry = _normalized_source_registry_snapshot(
            registry_path,
            repo_root=temp_root,
        )

        from okg.substrate.catalog.modules_compose import (
            compose_catalog,
            providers_for_declared_deployment,
        )
        from okg.substrate.catalog.ownership import catalog_identity_for

        # The candidate tree's declared owners are added to the target
        # library, so an upgrade plans the catalog the deployment actually
        # composes rather than refusing it (issue #1933).
        composed = compose_catalog(
            deployment_dir,
            providers=providers_for_declared_deployment(
                deployment_dir,
                **(
                    {"providers": dict(module_providers)}
                    if module_providers is not None
                    else {"modules_root": modules_root}
                ),
            ),
        )
        identity = catalog_identity_for(
            composed=composed,
            deployment_dir=deployment_dir,
            command="okg deployment up (target effect planner)",
        )
        nomos = _target_nomos_inventory(
            ctx,
            raw_manifest=raw_manifest,
            raw_registry=raw_registry,
            deployment_dir=deployment_dir,
        )
        pact_emission_contract = _target_pact_emission_contract(
            repo_root,
            revision,
        )
        return {
            "raw_manifest": raw_manifest,
            "raw_registry": raw_registry,
            "normalized_registry": normalized_registry,
            "catalog_fingerprint": identity.catalog_fingerprint,
            "manifest_fingerprint": identity.manifest_fingerprint,
            "module_provenance": list(
                module_provenance_from_composed(composed)
            ),
            "nomos": nomos,
            "pact_emission_contract": pact_emission_contract,
        }


def _target_nomos_inventory(
    ctx: UpgradeContext,
    *,
    raw_manifest: dict[str, Any],
    raw_registry: dict[str, Any],
    deployment_dir: Path,
) -> dict[str, Any]:
    if not _manifest_requires_nomos_runtime(raw_manifest):
        return {"required": False, "unchanged": True}
    del raw_registry
    from okg.substrate.nomos import (
        load_active_config_set,
        load_active_policy_bundle,
    )
    from okg.substrate.nomos.baseline import deployment_baseline_spec

    scope = f"deployment:{ctx.options.deployment}"
    desired = deployment_baseline_spec(
        deployment=ctx.options.deployment,
        deployment_dir=deployment_dir,
    )
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(ctx.dsn, connect_timeout=5) as conn:
        active_policy = load_active_policy_bundle(
            conn,
            deployment_name=ctx.options.deployment,
            scope=scope,
        )
        active_config = load_active_config_set(
            conn,
            deployment_name=ctx.options.deployment,
            scope=scope,
        )
    observed_policy = (
        active_policy.policy_fingerprint if active_policy is not None else None
    )
    observed_config = (
        active_config.config_fingerprint if active_config is not None else None
    )
    return {
        "required": True,
        "expected_policy_fingerprint": desired["policy_fingerprint"],
        "active_policy_fingerprint": observed_policy,
        "expected_config_fingerprint": desired["config_fingerprint"],
        "active_config_fingerprint": observed_config,
        "unchanged": (
            observed_policy == desired["policy_fingerprint"]
            and observed_config == desired["config_fingerprint"]
        ),
    }


def _refresh_target_nomos_inventory(
    ctx: UpgradeContext,
    cached: Mapping[str, Any],
) -> dict[str, Any]:
    """Refresh only live Nomos fingerprints; target compilation is immutable."""
    if cached.get("required") is not True:
        return {"required": False, "unchanged": True}
    from okg.substrate.nomos import (
        load_active_config_set,
        load_active_policy_bundle,
    )
    from okg.substrate.db.connection import connect as db_connect

    scope = f"deployment:{ctx.options.deployment}"
    with db_connect(ctx.dsn, connect_timeout=5) as conn:
        active_policy = load_active_policy_bundle(
            conn,
            deployment_name=ctx.options.deployment,
            scope=scope,
        )
        active_config = load_active_config_set(
            conn,
            deployment_name=ctx.options.deployment,
            scope=scope,
        )
    observed_policy = (
        active_policy.policy_fingerprint if active_policy is not None else None
    )
    observed_config = (
        active_config.config_fingerprint if active_config is not None else None
    )
    expected_policy = cached.get("expected_policy_fingerprint")
    expected_config = cached.get("expected_config_fingerprint")
    return {
        "required": True,
        "expected_policy_fingerprint": expected_policy,
        "active_policy_fingerprint": observed_policy,
        "expected_config_fingerprint": expected_config,
        "active_config_fingerprint": observed_config,
        "unchanged": (
            observed_policy == expected_policy
            and observed_config == expected_config
        ),
    }


# Adapters whose parameter surface this repo knows, keyed by
# (module, class, source_class).  Membership is an AUTHORITY claim only - it
# says nothing about whether the adapter reaches outside the deployment, which
# is reported separately as ``external``.
#
# ``params_closed`` below checks param NAMES against these sets; the value-level
# half of adapter trust is ``contains_command_payload``, which refuses a
# subprocess payload nested under an allowed name.  A remote endpoint in an
# allowed value is NOT a trust failure - it sets ``external`` and forces a
# current verified backup instead.  Both current entries take filesystem roots
# and a repo name.
#
# Note this key pins ``source_class``, so no ``mutable_api`` adapter can be a
# member and every such source fails ``adapter_trusted``.  That is a separate,
# genuine gate from external reach - see the deployment-up-external-effects-
# backup PACT's out-of-scope note.
_TRUSTED_ROUTINE_SMOKE_ADAPTERS: dict[tuple[str, str, str], frozenset[str]] = {
    (
        "okg.substrate.library.sources.openspec",
        "OpenSpecSource",
        "discovery_crawl",
    ): frozenset({
        "openspec_root",
        "repo_root",
        "repo",
        "include_archived",
        "materialize_referenced_capabilities",
        "filter_unresolved_crossrefs",
        "openspec_root_optional",
    }),
    (
        "okg.substrate.library.sources.pact",
        "PACTSource",
        "discovery_crawl",
    ): frozenset({"pact_root", "repo"}),
}

_DECLARED_SMOKE_ADAPTER_CONTRACT_FIELDS = frozenset({
    "module",
    "class",
    "source_class",
    "allowed_params",
    "publishable_revision_kinds",
    "publication_mode",
    "emits_on_demand",
    "external_effects",
})


def _declared_smoke_adapter_contracts(
    raw_manifest: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Read strict deployment-owned adapter authority from the target manifest."""
    lifecycle = raw_manifest.get("deployment_up")
    lifecycle = lifecycle if isinstance(lifecycle, Mapping) else {}
    raw_contracts = lifecycle.get("smoke_adapter_contracts", {})
    if raw_contracts in (None, {}):
        return {}, []
    if not isinstance(raw_contracts, Mapping):
        return {}, ["deployment_up.smoke_adapter_contracts must be a mapping"]
    contracts: dict[str, dict[str, Any]] = {}
    problems: list[str] = []
    for raw_name, raw_contract in raw_contracts.items():
        name = str(raw_name)
        if not name or not isinstance(raw_contract, Mapping):
            problems.append(f"smoke adapter contract {name!r} must be a mapping")
            continue
        contract = dict(raw_contract)
        if set(contract) != _DECLARED_SMOKE_ADAPTER_CONTRACT_FIELDS:
            problems.append(f"smoke adapter contract {name!r} has non-exact fields")
            continue
        allowed = contract.get("allowed_params")
        revisions = contract.get("publishable_revision_kinds")
        scalar_checks = all(
            isinstance(contract.get(field), str) and bool(contract.get(field))
            for field in ("module", "class", "source_class", "publication_mode")
        )
        list_checks = (
            isinstance(allowed, list)
            and len(allowed) == len(set(allowed))
            and all(
                isinstance(value, str)
                and bool(value)
                and not any(token in value for token in ("*", "?", "[", "]"))
                for value in allowed
            )
            and isinstance(revisions, list)
            and bool(revisions)
            and len(revisions) == len(set(revisions))
            and all(
                isinstance(value, str)
                and bool(value)
                and not any(token in value for token in ("*", "?", "[", "]"))
                for value in revisions
            )
        )
        boolean_checks = (
            contract.get("emits_on_demand") is True
            and isinstance(contract.get("external_effects"), bool)
        )
        if not (scalar_checks and list_checks and boolean_checks):
            problems.append(f"smoke adapter contract {name!r} is not closed")
            continue
        contracts[name] = {
            **contract,
            "allowed_params": frozenset(allowed),
            "publishable_revision_kinds": frozenset(revisions),
        }
    return contracts, problems


def _target_pact_emission_contract(
    repo_root: Path,
    revision: str,
) -> dict[str, tuple[tuple[str, ...], ...]]:
    path = "src/okg/substrate/library/sources/pact.py"
    source = _git_command(repo_root, ["show", f"{revision}:{path}"])
    tree = ast.parse(str(source), filename=path)
    contract_assignments: list[ast.expr] = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "PACT_EMISSION_CONTRACT"
            for target in node.targets
        ):
            contract_assignments.append(node.value)
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "PACT_EMISSION_CONTRACT"
        ):
            if node.value is not None:
                contract_assignments.append(node.value)
    contract_stores = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Name)
        and node.id == "PACT_EMISSION_CONTRACT"
        and isinstance(node.ctx, (ast.Store, ast.Del))
    ]
    if len(contract_assignments) != 1 or len(contract_stores) != 1:
        raise ValueError(
            "target PACT emission contract must have one immutable binding"
        )
    contract_node = contract_assignments[0]
    pact_classes = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "PACTSource"
    ]
    class_bindings: list[ast.expr] = []
    if len(pact_classes) == 1 and pact_classes[0] in tree.body:
        for node in pact_classes[0].body:
            if (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == "emission_contract"
            ):
                class_bindings.append(node.value)
            elif (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.target.id == "emission_contract"
                and node.value is not None
            ):
                class_bindings.append(node.value)
    emission_stores = [
        node
        for node in ast.walk(tree)
        if (
            isinstance(node, ast.Name)
            and node.id == "emission_contract"
            and isinstance(node.ctx, (ast.Store, ast.Del))
        )
        or (
            isinstance(node, ast.Attribute)
            and node.attr == "emission_contract"
            and isinstance(node.ctx, (ast.Store, ast.Del))
        )
    ]
    pact_source_rebindings = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Name)
        and node.id == "PACTSource"
        and isinstance(node.ctx, (ast.Store, ast.Del))
    ]
    if (
        len(class_bindings) != 1
        or len(emission_stores) != 1
        or pact_source_rebindings
        or not isinstance(class_bindings[0], ast.Name)
        or class_bindings[0].id != "PACT_EMISSION_CONTRACT"
    ):
        raise ValueError("target PACTSource emission contract binding is invalid")
    raw = ast.literal_eval(contract_node)
    if not isinstance(raw, dict) or set(raw) != {"nodes", "edges"}:
        raise ValueError("target PACT emission contract has an invalid shape")
    normalized: dict[str, tuple[tuple[str, ...], ...]] = {}
    for key, width in (("nodes", 3), ("edges", 4)):
        rows = raw.get(key)
        if (
            not isinstance(rows, (list, tuple))
            or not rows
            or any(
                not isinstance(row, (list, tuple))
                or len(row) != width
                or any(not isinstance(value, str) or not value for value in row)
                for row in rows
            )
        ):
            raise ValueError(f"target PACT emission contract {key} are invalid")
        normalized[key] = tuple(tuple(row) for row in rows)
    suffixes = [row[-1] for rows in normalized.values() for row in rows]
    if len(suffixes) != len(set(suffixes)):
        raise ValueError("target PACT emission contract clause suffixes are not unique")
    return normalized


def _expected_pact_smoke_policy(
    producer_id: str,
    emission_contract: Mapping[str, Sequence[Sequence[str]]],
) -> dict[str, Any]:

    clause_namespace = producer_id.replace("-", "_")
    nodes = [
        {
            "subtype": subtype,
            "id_prefix": id_prefix,
            "clause_id": f"{clause_namespace}.{suffix}",
        }
        for subtype, id_prefix, suffix in emission_contract["nodes"]
    ]
    edges = [
        {
            "src_subtype": source,
            "edge_type": edge,
            "dst_subtype": target,
            "clause_id": f"{clause_namespace}.{suffix}",
        }
        for source, edge, target, suffix in emission_contract["edges"]
    ]
    clauses = [
        {
            "id": node["clause_id"],
            "effect": "allow",
            "record_kind": "node",
            "subtype": node["subtype"],
        }
        for node in nodes
    ]
    clauses.extend(
        {
            "id": edge["clause_id"],
            "effect": "allow",
            "record_kind": "edge",
            "triple": [
                edge["src_subtype"],
                edge["edge_type"],
                edge["dst_subtype"],
            ],
        }
        for edge in edges
    )
    return {
        "output_signature": {"nodes": nodes, "edges": edges},
        "policy_clauses": clauses,
    }


def _target_smoke_inventory(
    ctx: UpgradeContext,
    *,
    raw_manifest: dict[str, Any],
    raw_registry: dict[str, Any],
    normalized_registry: Mapping[str, Any] | None = None,
    pact_emission_contract: Mapping[str, Sequence[Sequence[str]]] | None = None,
) -> dict[str, Any]:
    runtime = raw_manifest.get("runtime")
    runtime = runtime if isinstance(runtime, dict) else {}
    configured = runtime.get("smoke_sources")
    configured_sources = (
        [str(value) for value in configured]
        if isinstance(configured, list)
        else []
    )
    smoke_sources = list(ctx.options.include) or configured_sources
    fleet = _code_checkout_fleet()
    strict_pact_smoke = fleet.strict_pact_smoke
    sources = raw_registry.get("sources")
    sources = sources if isinstance(sources, dict) else {}
    entries: list[dict[str, Any]] = []
    declared_contracts, declared_contract_problems = (
        _declared_smoke_adapter_contracts(raw_manifest)
    )

    command_keys = {
        "argv", "cmd", "command", "commands", "executable", "shell",
        "subprocess", "transport",
    }

    def contains_command_payload(value: Any, *, key: str | None = None) -> bool:
        """Can these params make the adapter run something we did not read?

        An AUTHORITY question, not a reach question.  ``params_closed`` checks
        param NAMES against the allowlist; it cannot see that an allowed name
        holds a subprocess payload nested inside it.  A params blob that
        selects an executable makes the adapter's behaviour unknowable, which
        no backup can compensate for, so it refuses rather than forcing one.
        """
        if key is not None and key.lower() in command_keys and value not in (None, "", []):
            return True
        if isinstance(value, Mapping):
            return any(
                contains_command_payload(item, key=str(item_key))
                for item_key, item in value.items()
            )
        if isinstance(value, (list, tuple, set)):
            return any(contains_command_payload(item, key=key) for item in value)
        return False

    def contains_remote_endpoint(value: Any) -> bool:
        """Do these params name somewhere outside the deployment?

        A REACH question: the run's calls there cannot be undone, so this
        forces a current verified backup instead of refusing.
        """
        if isinstance(value, str):
            stripped = value.strip()
            return (
                re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", stripped) is not None
                or re.match(r"^[^/\s@]+@[^:\s]+:.+", stripped) is not None
            )
        if isinstance(value, Mapping):
            return any(contains_remote_endpoint(item) for item in value.values())
        if isinstance(value, (list, tuple, set)):
            return any(contains_remote_endpoint(item) for item in value)
        return False

    for name in smoke_sources:
        raw = sources.get(name)
        entry = raw if isinstance(raw, dict) else {}
        normalized = (
            normalized_registry.get(name)
            if isinstance(normalized_registry, Mapping)
            else None
        )
        normalized = normalized if isinstance(normalized, Mapping) else {}
        admission = entry.get("admission_policy")
        admission = admission if isinstance(admission, dict) else {}
        normalized_admission = normalized.get("admission_policy")
        normalized_admission = (
            normalized_admission if isinstance(normalized_admission, Mapping) else {}
        )
        raw_normalized_admission = normalized.get("raw_admission_policy")
        raw_normalized_admission = (
            raw_normalized_admission
            if isinstance(raw_normalized_admission, Mapping)
            else {}
        )
        raw_params = entry.get("params", {})
        params_shape_valid = isinstance(raw_params, dict)
        params = raw_params if params_shape_valid else {}
        module = entry.get("module")
        adapter_class = entry.get("class")
        source_class = entry.get("source_class")
        adapter_key = (module, adapter_class, source_class)
        declared_contract = declared_contracts.get(name)
        declared_identity_exact = bool(
            isinstance(declared_contract, Mapping)
            and declared_contract.get("module") == module
            and declared_contract.get("class") == adapter_class
            and declared_contract.get("source_class") == source_class
            and declared_contract.get("publication_mode")
            == entry.get("publication_mode")
        )
        builtin_allowed_params = _TRUSTED_ROUTINE_SMOKE_ADAPTERS.get(adapter_key)
        allowed_params = (
            declared_contract.get("allowed_params")
            if declared_identity_exact and isinstance(declared_contract, Mapping)
            else builtin_allowed_params
        )
        adapter_contract_source = (
            "deployment_manifest"
            if declared_identity_exact
            else "repository_builtin" if builtin_allowed_params is not None else None
        )
        params_closed = allowed_params is not None and set(
            str(key) for key in params
        ).issubset(allowed_params)
        has_command_payload = contains_command_payload(params)
        declared_external = (
            declared_contract.get("external_effects") is True
            if declared_identity_exact and isinstance(declared_contract, Mapping)
            else False
        )
        has_external_effect = (
            has_command_payload or contains_remote_endpoint(params) or declared_external
        )
        # Adapter TRUST answers "do we know what this adapter does": allowlist
        # membership, closed param NAMES, and no subprocess payload hiding in
        # an allowed name.  External REACH is reported separately on
        # ``external`` below.  Folding reach into trust laundered "this run
        # cannot be fully undone" as "this adapter cannot be trusted", which
        # refuses instead of forcing a backup and names the wrong cause - but
        # a params blob that can execute something is genuinely unknowable, so
        # that half of the old predicate stays here.
        adapter_trusted = (
            params_closed
            and params_shape_valid
            and not has_command_payload
            and (declared_contract is None or declared_identity_exact)
        )
        publishable_revision_kinds = (
            declared_contract.get("publishable_revision_kinds")
            if declared_identity_exact and isinstance(declared_contract, Mapping)
            else {"content_hash", "git_commit", "immutable_version"}
        )
        versioned = (
            entry.get("source_revision_kind") in publishable_revision_kinds
            and entry.get("publication_mode") == "published_generation"
            and (
                not declared_identity_exact
                or declared_contract.get("emits_on_demand") is True
            )
        )
        strict_source_family = strict_pact_smoke.get(ctx.options.deployment)
        strict_pact_smoke_check = strict_source_family is not None and name == "pact"
        policy_problems: list[str] = []
        admission_contract_valid = True
        strict_checks: dict[str, bool] = {}
        if strict_pact_smoke_check:
            from okg.substrate.producer_authority import (
                check_admission_policy_block_consistency,
            )

            producer_id = f"{ctx.options.deployment}.pact"
            if pact_emission_contract is None:
                raise ValueError("target PACT emission contract is unavailable")
            expected = _expected_pact_smoke_policy(
                producer_id,
                pact_emission_contract,
            )
            required_policy_keys = {
                "producer_id",
                "producer_kind",
                "trust_label",
                "admission_mode",
                "authority_scope",
                "allowed_record_kinds",
                "output_signature",
                "policy_clauses",
                "output_scope_summary",
            }
            policy_problems.extend(
                check_admission_policy_block_consistency(
                    raw_normalized_admission,
                    source_name=name,
                )
            )
            strict_checks = {
                "exact_policy_fields": (
                    set(raw_normalized_admission) == required_policy_keys
                ),
                "write_envelope_absent": (
                    "write_envelope" not in raw_normalized_admission
                ),
                "producer_identity": (
                    normalized.get("ownership_id")
                    == normalized_admission.get("producer_id")
                    == producer_id
                ),
                "producer_kind": (
                    normalized_admission.get("producer_kind") == "source"
                ),
                "trust_label": (
                    normalized_admission.get("trust_label") == "repo-local-trusted"
                ),
                "admission_mode": (
                    normalized_admission.get("admission_mode") == "fast_track"
                ),
                "authority_scope": normalized_admission.get("authority_scope")
                == {
                    "source_family": strict_source_family,
                    "path_globs": ["pact/**/*"],
                },
                "allowed_record_kinds": (
                    normalized_admission.get("allowed_record_kinds") == ["node", "edge"]
                ),
                "repo_local_params": params
                == {
                    "repo": "okg",
                    "pact_root": "${repo_root}/pact",
                },
                "output_signature": (
                    normalized_admission.get("output_signature")
                    == expected["output_signature"]
                ),
                "policy_clauses": (
                    normalized_admission.get("policy_clauses")
                    == expected["policy_clauses"]
                ),
                "cross_block_consistency": not policy_problems,
            }
            admission_contract_valid = all(strict_checks.values())
            adapter_trusted = (
                adapter_trusted and strict_checks["repo_local_params"]
            )
        deployment_owned = (
            str(entry.get("ownership_id") or "").startswith(
                f"{ctx.options.deployment}."
            )
            and admission.get("producer_kind") == "source"
            and admission.get("trust_label") == "repo-local-trusted"
            and admission_contract_valid
        )
        external = source_class == "mutable_api" or has_external_effect
        admission_contract = (
            dict(raw_normalized_admission)
            if strict_pact_smoke_check
            else {
                key: admission.get(key)
                for key in (
                    "producer_id",
                    "producer_kind",
                    "trust_label",
                    "admission_mode",
                    "authority_scope",
                    "allowed_record_kinds",
                    "output_signature",
                    "policy_clauses",
                    "write_envelope",
                )
            }
        )
        entries.append(
            {
                "name": name,
                "present": bool(entry),
                "module": module,
                "class": adapter_class,
                "source_class": source_class,
                "source_revision_kind": entry.get("source_revision_kind"),
                "publication_mode": entry.get("publication_mode"),
                "ownership_id": entry.get("ownership_id"),
                "deployment_owned": deployment_owned,
                "admission_contract_valid": admission_contract_valid,
                "admission_contract_checks": strict_checks,
                "admission_contract_problems": policy_problems,
                "versioned": versioned,
                "external": external,
                "adapter_trusted": adapter_trusted,
                "params_shape_valid": params_shape_valid,
                "adapter_contract_source": adapter_contract_source,
                "adapter_contract_declared": declared_contract is not None,
                "adapter_contract_identity_exact": declared_identity_exact,
                "adapter_contract_digest": _canonical_digest(
                    {
                        "module": module,
                        "class": adapter_class,
                        "source_class": source_class,
                        "allowed_params": sorted(allowed_params or ()),
                        "declared_contract": (
                            {
                                **dict(declared_contract),
                                "allowed_params": sorted(
                                    declared_contract.get("allowed_params") or ()
                                ),
                                "publishable_revision_kinds": sorted(
                                    declared_contract.get(
                                        "publishable_revision_kinds"
                                    ) or ()
                                ),
                            }
                            if isinstance(declared_contract, Mapping)
                            else None
                        ),
                    }
                ),
                "params_digest": _canonical_digest(params),
                "admission_contract_digest": _canonical_digest(admission_contract),
            }
        )
    effect_capabilities = sorted(
        (
            {
                "present": row["present"],
                "deployment_owned": row["deployment_owned"],
                "admission_contract_valid": row["admission_contract_valid"],
                "versioned": row["versioned"],
                "external": row["external"],
                "adapter_trusted": row["adapter_trusted"],
                "adapter_contract_digest": row["adapter_contract_digest"],
                "params_digest": row["params_digest"],
                "admission_contract_digest": row["admission_contract_digest"],
                "effect_mode": (
                    "deployment_owned_versioned_publish"
                    if (
                        row["present"]
                        and row["deployment_owned"]
                        and row["versioned"]
                        and row["adapter_trusted"]
                        and not row["external"]
                    )
                    else "unclosed_or_external"
                ),
            }
            for row in entries
        ),
        key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")),
    )
    # Source AUTHORITY and external REACH are independent claims and must be
    # reported independently.  Authority answers "can this plan be trusted and
    # read", and its failure refuses before pg_dump.  External reach answers
    # "can every effect of this run be undone", and its failure forces a real
    # physical backup instead.  Folding `not external` into the authority
    # predicate made the two booleans identical, so an LLM-bearing deployment
    # was refused by BOTH and satisfying the claim named in the error changed
    # nothing.  An empty selection fails both: absence is not proof.
    source_authority_closed = (
        not declared_contract_problems
        and bool(entries)
        and all(
            row["present"]
            and row["deployment_owned"]
            and row["versioned"]
            and row["adapter_trusted"]
            for row in entries
        )
    )
    return {
        "configured_sources": configured_sources,
        "selected_sources": smoke_sources,
        "selected_source_digest": _canonical_digest(entries),
        "effect_capabilities": effect_capabilities,
        "operator_override": bool(ctx.options.include),
        "entries": entries,
        "declared_adapter_contract_problems": declared_contract_problems,
        # Which deployments get the strict PACT smoke check is repository
        # configuration; say where it came from, so a run without it (an
        # install outside a configured checkout) is visible, not silent.
        "strict_pact_smoke": {
            "config": fleet.source or "none configured",
            "applies_to_this_deployment": (
                ctx.options.deployment in strict_pact_smoke
            ),
        },
        "versioned_and_deployment_owned": source_authority_closed,
        "no_external_effects": (
            bool(entries) and not any(row["external"] for row in entries)
        ),
    }


def _target_attribute_read_model_rebuildable(
    ctx: UpgradeContext,
    raw_manifest: Mapping[str, Any],
) -> bool:
    from okg.substrate.graph_store_internal.generation_ordinal import (
        interval_representation_flag_enabled,
    )

    native_views = raw_manifest.get("native_views")
    native_views = native_views if isinstance(native_views, Mapping) else {}
    # The same configuration predicate the publisher and the effective-attribute
    # materializer use. This answer feeds a REFUSAL gate that runs before
    # pg_dump, so it has to be true only when the read model really can be
    # rebuilt. OKG_IVM_GENERATION_ORDINAL=0 (issue #1514) leaves generations
    # without an ordinal, and the repair verb this claim promises resolves the
    # ordinal and fails loud without one -- so claiming rebuildable there would
    # wave the upgrade past the gate and then strand it after stop_services.
    return (
        interval_representation_flag_enabled(ctx.env)
        and native_views.get("attribute_effective_view_empty_overlay_fast_path") is True
    )


_PRE_709_CATALOG_OWNERSHIP_COLUMNS = {
    "deployment_name": ("text", True),
    "owner_fingerprint": ("text", True),
    "schema_fingerprint": ("text", True),
    "catalog_fingerprint": ("text", True),
    "deployment_root_hash": ("text", False),
    "worktree_root_hash": ("text", False),
    "status": ("text", True),
    "claimed_at": ("timestamp with time zone", True),
    "claimed_by_command": ("text", False),
    "last_seen_catalog_version_id": ("bigint", False),
    "metadata": ("jsonb", True),
}
_CURRENT_CATALOG_OWNERSHIP_COLUMNS = {
    **_PRE_709_CATALOG_OWNERSHIP_COLUMNS,
    "manifest_fingerprint": ("text", False),
}


def _live_catalog_ownership(ctx: UpgradeContext) -> dict[str, Any]:
    """Read catalog identity across the exact pre-#709 schema transition.

    The effect inventory runs before migrations, so a production deployment
    may still have the ownership shape from before ``manifest_fingerprint``.
    Lock both relations before inspecting ``pg_catalog`` so DDL cannot change
    the shape between classification and the single-snapshot data read.
    """
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(ctx.dsn, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "LOCK TABLE okg.catalog_versions, "
                "okg.deployment_write_ownership IN ACCESS SHARE MODE"
            )
            cur.execute(
                """
                SELECT c.relkind, a.attname,
                       pg_catalog.format_type(a.atttypid, a.atttypmod),
                       a.attnotnull
                  FROM pg_catalog.pg_class c
                  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                  JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
                 WHERE n.nspname = 'okg'
                   AND c.relname = 'deployment_write_ownership'
                   AND a.attnum > 0
                   AND NOT a.attisdropped
                 ORDER BY a.attname
                """
            )
            shape_rows = cur.fetchall()
            relkinds = {str(row[0]) for row in shape_rows}
            observed_columns = {
                str(row[1]): (str(row[2]), bool(row[3]))
                for row in shape_rows
            }
            if relkinds != {"r"}:
                raise RuntimeError(
                    "deployment_write_ownership is not an ordinary table"
                )
            if observed_columns == _CURRENT_CATALOG_OWNERSHIP_COLUMNS:
                ownership_schema = "current"
                manifest_expression = (
                    "(SELECT manifest_fingerprint FROM ownership)"
                )
            elif observed_columns == _PRE_709_CATALOG_OWNERSHIP_COLUMNS:
                ownership_schema = "pre_709"
                manifest_expression = "NULL::text"
            else:
                raise RuntimeError(
                    "deployment_write_ownership has an unsupported column shape"
                )
            cur.execute(
                f"""
                WITH ownership AS (
                    SELECT catalog_fingerprint
                           {", manifest_fingerprint" if ownership_schema == "current" else ""}
                      FROM okg.deployment_write_ownership
                     WHERE deployment_name = %s
                )
                SELECT (
                           SELECT catalog_fingerprint
                             FROM okg.catalog_versions
                            WHERE deployment_name = %s
                              AND status = 'applied'
                            ORDER BY catalog_version_id DESC
                            LIMIT 1
                       ),
                       (SELECT catalog_fingerprint FROM ownership),
                       {manifest_expression}
                """,
                (ctx.options.deployment, ctx.options.deployment),
            )
            catalog_row = cur.fetchone()
            if catalog_row is None:
                raise RuntimeError("catalog ownership query returned no result")
    return {
        "ownership_schema": ownership_schema,
        "catalog_fingerprint": str(catalog_row[0]) if catalog_row[0] else None,
        "owner_catalog_fingerprint": (
            str(catalog_row[1]) if catalog_row[1] else None
        ),
        "manifest_fingerprint": str(catalog_row[2]) if catalog_row[2] else None,
    }


def _module_provider_report(
    previous: Any,
    target: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Per-module provider changes carried from the last planned target.

    `previous` is the provenance the upgrade planned against last time and
    `target` what this revision resolves to. Returns None when neither side
    names a packaged module, so a deployment composed from the built-in
    library alone records nothing new and its inventory digest is what it
    was. An upgrade that would leave a packaged owner supplying nothing
    refuses through `refuse_dropped_module_owner`.
    """
    recorded = module_provenance_entries(previous)
    resolved = module_provenance_entries(target)
    if not recorded and not resolved:
        return None
    return {
        "target": [dict(entry) for entry in resolved],
        "changes": refuse_dropped_module_owner(recorded, resolved),
    }


def _collect_effect_inventory(
    ctx: UpgradeContext,
    *,
    expected_digest: str | None = None,
) -> tuple[Any, tuple[str, ...], dict[str, Any]]:
    from okg.substrate.deployment_backup_policy import (
        parse_effect_inventory_checks,
    )

    try:
        manifest = _resolve_upgrade_deployment(ctx)
        authority = _git_target_revision(ctx, manifest)
        repo_root = authority.code_repo_root
        revision = authority.code_revision
        planner_revision = authority.planner_revision
        execution_checkout_exact = authority.execution_checkout_exact
        target_revision_pinned = authority.target_revision_pinned
        planner = _planner_integrity_inventory(
            repo_root,
            planner_revision,
            authority.deployment_repo_root,
        )
        static = ctx.data.get("effect_inventory_static")
        static_valid = (
            isinstance(static, Mapping)
            and static.get("repo_root") == repo_root
            and static.get("target_revision") == revision
            and static.get("planner_revision") == planner_revision
            and static.get("deployment_repo_root")
            == authority.deployment_repo_root
            and static.get("deployment_revision")
            == authority.deployment_revision
        )
        if not static_valid:
            target_migrations = _target_known_migrations(repo_root, revision)
            checksum_compatibilities = _target_migration_checksum_compatibilities(
                repo_root, revision
            )
            required_extensions = _target_required_extensions(repo_root, revision)
            target = _target_catalog_and_nomos_inventory(
                ctx,
                repo_root=repo_root,
                revision=revision,
                deployment_repo_root=authority.deployment_repo_root,
                deployment_revision=authority.deployment_revision,
                deployment_relative=authority.deployment_relative,
            )
            static = {
                "repo_root": repo_root,
                "target_revision": revision,
                "planner_revision": planner_revision,
                "deployment_repo_root": authority.deployment_repo_root,
                "deployment_revision": authority.deployment_revision,
                "target_migrations": target_migrations,
                "checksum_compatibilities": checksum_compatibilities,
                "required_extensions": required_extensions,
                "raw_manifest": target["raw_manifest"],
                "raw_registry": target["raw_registry"],
                "normalized_registry": target.get("normalized_registry", {}),
                "catalog_fingerprint": target["catalog_fingerprint"],
                "manifest_fingerprint": target["manifest_fingerprint"],
                "module_provenance": list(target.get("module_provenance") or ()),
                "nomos": target["nomos"],
                "pact_emission_contract": target["pact_emission_contract"],
            }
            ctx.data["effect_inventory_static"] = static
        else:
            assert isinstance(static, Mapping)
            target_migrations = static["target_migrations"]
            checksum_compatibilities = static["checksum_compatibilities"]
            required_extensions = static["required_extensions"]
            target = {
                "raw_manifest": static["raw_manifest"],
                "raw_registry": static["raw_registry"],
                "normalized_registry": static.get("normalized_registry", {}),
                "catalog_fingerprint": static["catalog_fingerprint"],
                "manifest_fingerprint": static["manifest_fingerprint"],
                "module_provenance": static.get("module_provenance", []),
                "nomos": _refresh_target_nomos_inventory(ctx, static["nomos"]),
                "pact_emission_contract": static["pact_emission_contract"],
            }
        target["smoke"] = _target_smoke_inventory(
            ctx,
            raw_manifest=target["raw_manifest"],
            raw_registry=target["raw_registry"],
            normalized_registry=target.get("normalized_registry", {}),
            pact_emission_contract=target["pact_emission_contract"],
        )
        database = _live_migration_and_extension_inventory(
            ctx,
            target_migrations=target_migrations,
            checksum_compatibilities=checksum_compatibilities,
            required_extensions=required_extensions,
        )
        live_catalog = _live_catalog_ownership(ctx)
        target_catalog = target["catalog_fingerprint"]
        target_manifest = target["manifest_fingerprint"]
        catalog_unchanged = (
            target_catalog == live_catalog["catalog_fingerprint"]
            and target_catalog == live_catalog["owner_catalog_fingerprint"]
            and target_manifest == live_catalog["manifest_fingerprint"]
        )
        attribute_rebuildable = _target_attribute_read_model_rebuildable(
            ctx,
            target["raw_manifest"],
        )
        # Carry provenance from the release the upgrade planned against last
        # to the one it plans now, and refuse a silent owner drop before any
        # database effect is described.
        module_providers_report = _module_provider_report(
            ctx.data.get("module_provenance_planned"),
            target.get("module_provenance") or (),
        )
        ctx.data["module_provenance_planned"] = list(
            target.get("module_provenance") or ()
        )
        details = {
            "schema": "okg.deployment_effect_inventory.v1",
            "deployment": ctx.options.deployment,
            "target_revision": revision,
            "planner_revision": planner_revision,
            "code_repo_root": str(repo_root),
            "deployment_repo_root": str(authority.deployment_repo_root),
            "deployment_revision": authority.deployment_revision,
            "vendored_code_repo": authority.vendored,
            "execution_checkout_exact": execution_checkout_exact,
            "target_revision_pinned": target_revision_pinned,
            "planner": planner,
            "migration_and_extensions": database,
            "catalog": {
                "target_catalog_fingerprint": target_catalog,
                "target_manifest_fingerprint": target_manifest,
                "live": live_catalog,
                **(
                    {"module_providers": module_providers_report}
                    if module_providers_report is not None
                    else {}
                ),
            },
            "nomos": target["nomos"],
            "smoke": target["smoke"],
            "attribute_read_model": {
                "mode": "derived_interval_repair",
                "rebuildable": attribute_rebuildable,
            },
        }
        # The effect digest describes the immutable target plan, while the full
        # inventory below also records live prerequisites.  This keeps the plan
        # digest stable as an authorized migration/catalog application converges
        # live state toward that target.  A qualified canary name remains a
        # phase-local choice, but its normalized capabilities are load-bearing:
        # changing from a closed source to a differently capable source drifts
        # the digest and refuses before the next database effect.
        effect_plan = {
            "schema": "okg.deployment_effect_plan.v1",
            "deployment": ctx.options.deployment,
            "framework_repository": str(repo_root),
            "target_revision": revision,
            "deployment_repository": str(authority.deployment_repo_root),
            "deployment_revision": authority.deployment_revision,
            "target_migrations": target_migrations,
            "required_extensions": required_extensions,
            "target_catalog_fingerprint": target_catalog,
            "target_manifest_fingerprint": target_manifest,
            "target_nomos": {
                "required": target["nomos"].get("required"),
                "expected_policy_fingerprint": target["nomos"].get(
                    "expected_policy_fingerprint"
                ),
                "expected_config_fingerprint": target["nomos"].get(
                    "expected_config_fingerprint"
                ),
            },
            "smoke_effect_capabilities": target["smoke"]["effect_capabilities"],
            "attribute_read_model": details["attribute_read_model"],
        }
        digest = _canonical_digest(effect_plan)
        inventory_digest = _canonical_digest(details)
        raw_checks = {
            "target_revision_pinned": target_revision_pinned,
            "planner_trusted": planner["trusted"],
            "plan_readable": True,
            "no_pending_migrations": (
                database["ledger_exists"]
                and not database["pending"]
                and not database["checksum_mismatches"]
            ),
            "no_extension_changes": not database["extension_work"],
            "catalog_unchanged": catalog_unchanged,
            "nomos_unchanged": target["nomos"]["unchanged"] is True,
            "smoke_source_versioned": (
                target["smoke"]["versioned_and_deployment_owned"] is True
            ),
            "attribute_read_model_rebuildable": attribute_rebuildable,
            "no_external_effects": (target["smoke"]["no_external_effects"] is True),
            "effect_digest_stable": (
                expected_digest is None or digest == expected_digest
            ),
        }
        checks, errors = parse_effect_inventory_checks(raw_checks)
        return (
            checks,
            errors,
            {
                **details,
                "effect_plan": effect_plan,
                "effect_digest": digest,
                "inventory_digest": inventory_digest,
                "checks": raw_checks,
            },
        )
    except UpgradeModuleProviderDropped as exc:
        # Named ahead of the generic arm so the operator reads the stable
        # code and its payload -- which owners were dropped, and what else
        # changed -- rather than an exception class name.
        return (
            None,
            (exc.code,),
            {
                "schema": "okg.deployment_effect_inventory.v1",
                "status": "refused",
                "error": {"code": exc.code, "message": str(exc), **exc.data},
            },
        )
    except RevisionAuthorityError as exc:
        error = "effect_inventory_unreadable:RevisionAuthorityError"
        return (
            None,
            (error,),
            {
                "schema": "okg.deployment_effect_inventory.v1",
                "status": "unreadable",
                "error": exc.data,
            },
        )
    except Exception as exc:  # noqa: BLE001 - unreadable target plans refuse.
        error = f"effect_inventory_unreadable:{type(exc).__name__}"
        return (
            None,
            (error,),
            {
                "schema": "okg.deployment_effect_inventory.v1",
                "status": "unreadable",
                "error": {"type": type(exc).__name__, "message": str(exc)},
            },
        )


def _database_function_identity(cur: Any) -> list[list[Any]]:
    """Canonical function identity using effective, not raw-stored, ACLs."""
    cur.execute(
        """
        SELECT p.proname, pg_get_function_identity_arguments(p.oid),
               pg_get_function_result(p.oid), pg_get_functiondef(p.oid),
               pg_get_userbyid(p.proowner), COALESCE((
                 SELECT jsonb_agg(
                   jsonb_build_array(
                     pg_get_userbyid(acl.grantor),
                     CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                          ELSE pg_get_userbyid(acl.grantee) END,
                     acl.privilege_type,
                     acl.is_grantable
                   ) ORDER BY pg_get_userbyid(acl.grantor),
                              CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                   ELSE pg_get_userbyid(acl.grantee) END,
                              acl.privilege_type, acl.is_grantable
                 )::text
                   FROM aclexplode(COALESCE(
                     p.proacl, acldefault('f'::"char", p.proowner)
                   )) AS acl
               ), '[]'),
               l.lanname, p.provolatile, p.proparallel, p.prosecdef,
               p.proleakproof, p.proisstrict, COALESCE(p.proconfig::text, '')
          FROM pg_proc p
          JOIN pg_namespace n ON n.oid = p.pronamespace
          JOIN pg_language l ON l.oid = p.prolang
         WHERE n.nspname = 'okg' AND p.prokind IN ('f', 'p')
         ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)
        """
    )
    return [list(row) for row in cur.fetchall()]


def _timescaledb_state(cur: Any, extensions: list[list[str]]) -> dict[str, Any]:
    """Probe the TimescaleDB state a restore proof must account for.

    ``hypertables`` and ``continuous_aggregates`` are schema-qualified name
    lists — the stable identity a receipt is compared against.
    ``hypertable_chunk_counts`` is recorded evidence only: chunk counts drift
    with normal ingest, so they are compared source-vs-restored within one
    executed verification and never enter receipt-reuse equality. A probe
    failure on a database whose extension list names timescaledb propagates
    as an exception (the callers refuse on unreadable identity) instead of
    reading as "no TimescaleDB".
    """
    extension_version = next(
        (version for name, version in extensions if name == "timescaledb"),
        None,
    )
    if extension_version is None:
        return {
            "extension_version": None,
            "hypertables": [],
            "continuous_aggregates": [],
            "hypertable_chunk_counts": [],
        }
    cur.execute(
        "SELECT hypertable_schema, hypertable_name, num_chunks "
        "FROM timescaledb_information.hypertables "
        "ORDER BY hypertable_schema, hypertable_name"
    )
    rows = cur.fetchall()
    hypertables = [[str(schema), str(name)] for schema, name, _ in rows]
    chunk_counts = [
        [str(schema), str(name), int(chunks) if chunks is not None else 0]
        for schema, name, chunks in rows
    ]
    cur.execute(
        "SELECT view_schema, view_name "
        "FROM timescaledb_information.continuous_aggregates "
        "ORDER BY view_schema, view_name"
    )
    continuous_aggregates = [[str(a), str(b)] for a, b in cur.fetchall()]
    return {
        "extension_version": str(extension_version),
        "hypertables": hypertables,
        "continuous_aggregates": continuous_aggregates,
        "hypertable_chunk_counts": chunk_counts,
    }


def _database_logical_identity(conn: Any) -> dict[str, Any]:
    """Digest restored logical identity from database state, not receipt claims."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT extname, extversion FROM pg_extension ORDER BY extname"
        )
        extensions = [[str(a), str(b)] for a, b in cur.fetchall()]
        timescaledb = _timescaledb_state(cur, extensions)
        cur.execute(
            "SELECT owner_deployment_name, db_owner_uuid::text, "
            "COALESCE(schema_fingerprint, '') "
            "FROM okg.database_owner WHERE singleton"
        )
        owner_row = cur.fetchone()
        cur.execute(
            "SELECT migration_id, checksum, destructive "
            "FROM okg.schema_migrations ORDER BY migration_id"
        )
        migrations = [
            [str(migration_id), str(checksum), bool(destructive)]
            for migration_id, checksum, destructive in cur.fetchall()
        ]
        cur.execute(
            """
            SELECT n.nspname, pg_get_userbyid(n.nspowner),
                   COALESCE((
                     SELECT jsonb_agg(acl_item::text ORDER BY acl_item::text)::text
                       FROM unnest(n.nspacl) acl_item
                   ), '[]')
              FROM pg_namespace n
             WHERE n.nspname = 'okg'
            """
        )
        schemas = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT c.relname, c.relkind, c.relispartition,
                   c.relrowsecurity, c.relforcerowsecurity,
                   COALESCE(pg_get_expr(c.relpartbound, c.oid, TRUE), ''),
                   COALESCE(pg_get_partkeydef(c.oid), ''),
                   pg_get_userbyid(c.relowner), COALESCE((
                     SELECT jsonb_agg(
                       jsonb_build_array(
                         pg_get_userbyid(acl.grantor),
                         CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                              ELSE pg_get_userbyid(acl.grantee) END,
                         acl.privilege_type,
                         acl.is_grantable
                       ) ORDER BY pg_get_userbyid(acl.grantor),
                                  CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                       ELSE pg_get_userbyid(acl.grantee) END,
                                  acl.privilege_type, acl.is_grantable
                     )::text
                       FROM aclexplode(COALESCE(
                         c.relacl,
                         -- acldefault object chars are lowercase: 's' is
                         -- sequence ('S' means foreign server and would
                         -- collapse the owner default to USAGE-only).
                         acldefault(
                           CASE WHEN c.relkind = 'S' THEN 's'::"char"
                                ELSE 'r'::"char" END,
                           c.relowner
                         )
                       )) AS acl
                   ), '[]'),
                   c.relpersistence, c.relreplident,
                   COALESCE(c.reloptions::text, ''), COALESCE(am.amname, ''),
                   a.attnum, a.attname,
                   format_type(a.atttypid, a.atttypmod), a.attnotnull,
                   a.attidentity, a.attgenerated,
                   CASE WHEN a.attcollation = 0 THEN '' ELSE
                     quote_ident(coll_n.nspname) || '.' || quote_ident(coll.collname)
                   END,
                   COALESCE(pg_get_expr(d.adbin, d.adrelid), '')
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
              LEFT JOIN pg_am am ON am.oid = c.relam
              LEFT JOIN pg_attribute a
                ON a.attrelid = c.oid
               AND a.attnum > 0
               AND NOT a.attisdropped
              LEFT JOIN pg_attrdef d
                ON d.adrelid = c.oid AND d.adnum = a.attnum
              LEFT JOIN pg_collation coll ON coll.oid = a.attcollation
              LEFT JOIN pg_namespace coll_n ON coll_n.oid = coll.collnamespace
             WHERE n.nspname = 'okg'
               AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
             ORDER BY c.relname, a.attnum NULLS FIRST
            """
        )
        relations = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT c.relname, c.relkind, pg_get_viewdef(c.oid, TRUE)
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg' AND c.relkind IN ('v', 'm')
             ORDER BY c.relname
            """
        )
        views = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT c.relname, con.conname, con.contype,
                   pg_get_constraintdef(con.oid, TRUE)
              FROM pg_constraint con
              JOIN pg_class c ON c.oid = con.conrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg'
             ORDER BY c.relname, con.conname
            """
        )
        constraints = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT t.typname, con.conname, con.contype,
                   pg_get_constraintdef(con.oid, TRUE)
              FROM pg_constraint con
              JOIN pg_type t ON t.oid = con.contypid
              JOIN pg_namespace n ON n.oid = t.typnamespace
             WHERE n.nspname = 'okg'
             ORDER BY t.typname, con.conname
            """
        )
        domain_constraints = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT tab.relname, idx.relname, pg_get_indexdef(i.indexrelid),
                   pg_get_userbyid(idx.relowner), COALESCE((
                     SELECT jsonb_agg(acl_item::text ORDER BY acl_item::text)::text
                       FROM unnest(idx.relacl) acl_item
                   ), '[]'),
                   COALESCE(am.amname, ''), COALESCE(idx.reloptions::text, ''),
                   idx.relpersistence
              FROM pg_index i
              JOIN pg_class tab ON tab.oid = i.indrelid
              JOIN pg_class idx ON idx.oid = i.indexrelid
              LEFT JOIN pg_am am ON am.oid = idx.relam
              JOIN pg_namespace n ON n.oid = tab.relnamespace
             WHERE n.nspname = 'okg'
             ORDER BY tab.relname, idx.relname
            """
        )
        indexes = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT c.relname, t.tgname, pg_get_triggerdef(t.oid, TRUE)
              FROM pg_trigger t
              JOIN pg_class c ON c.oid = t.tgrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg' AND NOT t.tgisinternal
             ORDER BY c.relname, t.tgname
            """
        )
        triggers = [list(row) for row in cur.fetchall()]
        functions = _database_function_identity(cur)
        cur.execute(
            """
            SELECT t.typname, t.typtype, t.typcategory,
                   format_type(t.oid, NULL),
                   COALESCE(format_type(t.typbasetype, t.typtypmod), ''),
                   t.typnotnull, COALESCE(t.typdefault, ''),
                   pg_get_userbyid(t.typowner), COALESCE((
                     SELECT jsonb_agg(acl_item::text ORDER BY acl_item::text)::text
                       FROM unnest(t.typacl) acl_item
                   ), '[]'),
                   CASE WHEN t.typcollation = 0 THEN '' ELSE
                     quote_ident(coll_n.nspname) || '.' || quote_ident(coll.collname)
                   END,
                   COALESCE((
                     SELECT jsonb_agg(
                       jsonb_build_array(e.enumsortorder, e.enumlabel)
                       ORDER BY e.enumsortorder
                     ) FROM pg_enum e WHERE e.enumtypid = t.oid
                   ), '[]'::jsonb)::text
              FROM pg_type t
              JOIN pg_namespace n ON n.oid = t.typnamespace
              LEFT JOIN pg_collation coll ON coll.oid = t.typcollation
              LEFT JOIN pg_namespace coll_n ON coll_n.oid = coll.collnamespace
             WHERE n.nspname = 'okg' AND t.typrelid = 0
             ORDER BY t.typname
            """
        )
        types = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT c.relname, p.polname, p.polcmd, p.polpermissive,
                   pg_get_expr(p.polqual, p.polrelid, TRUE),
                   pg_get_expr(p.polwithcheck, p.polrelid, TRUE),
                   ARRAY(
                     SELECT r.rolname FROM pg_roles r
                      WHERE r.oid = ANY(p.polroles) ORDER BY r.rolname
                   )
              FROM pg_policy p
              JOIN pg_class c ON c.oid = p.polrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg'
             ORDER BY c.relname, p.polname
            """
        )
        policies = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT c.relname, format_type(s.seqtypid, NULL), s.seqstart,
                   s.seqincrement, s.seqmax, s.seqmin, s.seqcache, s.seqcycle
              FROM pg_sequence s
              JOIN pg_class c ON c.oid = s.seqrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg'
             ORDER BY c.relname
            """
        )
        sequences = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT p.proname, pg_get_function_identity_arguments(p.oid),
                   pg_get_function_result(p.oid), a.aggkind,
                   a.aggnumdirectargs, a.aggtransfn::regprocedure::text,
                   a.aggfinalfn::regprocedure::text,
                   a.aggcombinefn::regprocedure::text,
                   a.aggserialfn::regprocedure::text,
                   a.aggdeserialfn::regprocedure::text,
                   COALESCE(a.agginitval, '')
              FROM pg_proc p
              JOIN pg_namespace n ON n.oid = p.pronamespace
              JOIN pg_aggregate a ON a.aggfnoid = p.oid
             WHERE n.nspname = 'okg'
             ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)
            """
        )
        aggregates = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT c.relname, r.rulename, pg_get_ruledef(r.oid, TRUE)
              FROM pg_rewrite r
              JOIN pg_class c ON c.oid = r.ev_class
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg'
             ORDER BY c.relname, r.rulename
            """
        )
        rules = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT pg_get_userbyid(d.defaclrole), d.defaclnamespace::regnamespace::text,
                   d.defaclobjtype, COALESCE((
                     SELECT jsonb_agg(acl_item::text ORDER BY acl_item::text)::text
                       FROM unnest(d.defaclacl) acl_item
                   ), '[]')
              FROM pg_default_acl d
             WHERE d.defaclnamespace = 'okg'::regnamespace
             ORDER BY 1, 3, 4
            """
        )
        default_privileges = [list(row) for row in cur.fetchall()]
    if not owner_row:
        raise ValueError("database owner identity row is incomplete")
    migration_bytes = json.dumps(
        migrations, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    schema_catalog = {
        "inventory_version": 4,
        "schemas": schemas,
        "relations": relations,
        "views": views,
        "constraints": constraints,
        "domain_constraints": domain_constraints,
        "indexes": indexes,
        "triggers": triggers,
        "functions": functions,
        "types": types,
        "policies": policies,
        "sequences": sequences,
        "aggregates": aggregates,
        "rules": rules,
        "default_privileges": default_privileges,
    }
    component_digests = {
        key: _canonical_digest(value)
        for key, value in schema_catalog.items()
        if key != "inventory_version"
    }
    return {
        "database_owner": {
            "deployment": str(owner_row[0]),
            "uuid": str(owner_row[1]),
            "schema_fingerprint": str(owner_row[2]),
        },
        "extension_versions": extensions,
        "timescaledb": timescaledb,
        "schema_migrations_digest": (
            "sha256:" + hashlib.sha256(migration_bytes).hexdigest()
        ),
        "schema_catalog_digest": _canonical_digest(schema_catalog),
        "schema_catalog_component_digests": component_digests,
    }


def _backup_compatibility_snapshot(
    ctx: UpgradeContext,
    toolchain: dict[str, Any],
) -> dict[str, Any]:
    """Read the exact database/tool state that a drill receipt proves."""
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(ctx.dsn, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT system_identifier::text FROM pg_control_system()")
            cluster_row = cur.fetchone()
            cur.execute("SELECT current_setting('server_version_num')")
            server_row = cur.fetchone()
        logical_identity = _database_logical_identity(conn)
    if not cluster_row or not server_row:
        raise ValueError("database identity rows are incomplete")
    target = ctx.resolution.get("target") or {}
    tools = toolchain.get("tools") or {}
    tool_identities = {
        tool: entry.get("executable_identity")
        for tool in ("pg_dump", "pg_restore")
        if isinstance((entry := tools.get(tool)), Mapping)
    }
    return {
        "target": {
            "deployment": ctx.options.deployment,
            "host": target.get("host"),
            "port": target.get("port"),
            "database": target.get("database"),
        },
        **logical_identity,
        "cluster_system_identifier": str(cluster_row[0]),
        "server_version": str(server_row[0]),
        "candidate_plan_digest": _migration_plan_digest(
            ctx.data.get("migrations_before")
        ),
        "archive_format": "postgresql-custom",
        "dump_tool_major": (tools.get("pg_dump") or {}).get("major"),
        "restore_tool_major": (tools.get("pg_restore") or {}).get("major"),
        "backup_tool_executable_identity": tool_identities,
    }


def _timescale_receipt_check(
    block: object,
    snapshot: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> tuple[bool, str]:
    """Decide whether a receipt's TimescaleDB proof covers the current target.

    Returns ``(check, status)``. ``check`` feeds the
    ``timescale_restore_proven_or_risk_admitted`` policy field; ``status`` is
    the recorded three-state answer (``proven`` / ``absent`` /
    ``risk_admitted``) plus the failure states ``unproven`` and
    ``target_state_unreadable``. The equality predicate covers stable
    identity only — extension version, hypertable name set,
    continuous-aggregate name set. Chunk counts drift with normal ingest and
    never enter it. A provable block is reported ``proven`` even when the
    deployment also declared the risk admission; the admission is a
    fallback, and it can never silently upgrade to a verified claim.
    """
    current = snapshot.get("timescaledb")
    if not isinstance(current, Mapping):
        return False, "target_state_unreadable"
    if current.get("extension_version") is None:
        return True, "absent"
    proven = (
        isinstance(block, Mapping)
        and block.get("status") == "proven"
        and block.get("pre_post_restore_used") is True
        and block.get("extension_version") == current.get("extension_version")
        and block.get("hypertables_protected") == current.get("hypertables")
        and block.get("continuous_aggregates")
        == current.get("continuous_aggregates")
    )
    if proven:
        return True, "proven"
    if policy.get("timescale_restore_risk_admitted") is True:
        return True, "risk_admitted"
    return False, "unproven"


def _restore_drill_receipt_path(ctx: UpgradeContext) -> Path:
    return ctx.backup_dir / f"{ctx.options.deployment}-latest-restore-drill.json"


def _database_owner_identity_equal(
    receipt_owner: Any,
    snapshot_owner: Any,
) -> bool:
    """Same deployment and same database-owner UUID.

    Deliberately not whole-row equality: ``database_owner`` also carries the
    ``schema_fingerprint`` that ``catalog_apply`` rewrites, so comparing the
    row would drop the size prior on exactly the runs that follow a catalog
    change - which LOWERS the headroom demand rather than raising it. The
    UUID is what says this is the same database.
    """
    if not isinstance(receipt_owner, Mapping) or not isinstance(
        snapshot_owner, Mapping
    ):
        return False
    uuid = snapshot_owner.get("uuid")
    return bool(
        uuid
        and receipt_owner.get("uuid") == uuid
        and receipt_owner.get("deployment") == snapshot_owner.get("deployment")
    )


def _receipt_target_binding(
    checks_raw: Mapping[str, Any],
    *,
    receipt_owner: Any,
    snapshot_owner: Any,
) -> dict[str, bool]:
    """The checks that say a receipt describes THIS database on THIS cluster.

    Freshness, tool majors, and schema digests all pass on a receipt minted
    against a destroyed volume, so they cannot bind one.
    """
    return {
        "cluster_system_identifier_equal": (
            checks_raw.get("cluster_system_identifier_equal") is True
        ),
        "database_equal": checks_raw.get("database_equal") is True,
        "deployment_equal": checks_raw.get("deployment_equal") is True,
        "database_owner_identity_equal": _database_owner_identity_equal(
            receipt_owner, snapshot_owner
        ),
    }


def _target_bound_prior_size(
    binding: Mapping[str, bool],
    observed_size: int | None,
) -> tuple[int | None, dict[str, Any] | None]:
    """The receipt's restored size, admitted only when it binds to this target.

    A receipt from a cluster that no longer exists still parses, and its
    ``observed_restored_size_bytes`` still sizes the restore-headroom
    estimate. On 2026-09-03 a 25 MB database was refused a backup for lack
    of 88 GB, on a 70.7 GB prior carried by a receipt whose cluster and
    database-owner identities belonged to a volume wiped four days earlier.
    An unbound size prior is not a conservative estimate of anything, so it
    is dropped and the drop is reported rather than silently applied.
    """
    if observed_size is None:
        return None, None
    failed = sorted(name for name, held in binding.items() if held is not True)
    if failed:
        return None, {
            "reason": failed,
            "observed_restored_size_bytes": observed_size,
        }
    return observed_size, None


def _restore_drill_receipt_evidence(
    ctx: UpgradeContext,
    snapshot: dict[str, Any],
    policy: dict[str, Any],
) -> tuple[Any, tuple[str, ...], dict[str, Any], int | None]:
    from okg.substrate.deployment_backup_policy import (
        parse_restore_drill_checks,
    )

    path = _restore_drill_receipt_path(ctx)
    if not path.is_file():
        return None, (), {"status": "missing", "path": str(path)}, None
    try:
        stat = path.stat()
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        error = f"receipt_malformed:{type(exc).__name__}"
        return None, (error,), {
            "status": "malformed",
            "path": str(path),
            "error": str(exc),
        }, None
    if not isinstance(raw, dict):
        return None, ("receipt_not_object",), {
            "status": "malformed",
            "path": str(path),
        }, None
    compatibility = raw.get("compatibility")
    if not isinstance(compatibility, dict):
        compatibility = {}
    receipt_target = compatibility.get("target")
    if not isinstance(receipt_target, dict):
        receipt_target = {}
    current_target = snapshot["target"]
    verified_at = raw.get("verified_at")
    age_seconds: float | None = None
    try:
        verified = datetime.fromisoformat(str(verified_at))
        if verified.tzinfo is None:
            raise ValueError("timestamp has no timezone")
        age_seconds = (
            datetime.now(timezone.utc) - verified.astimezone(timezone.utc)
        ).total_seconds()
    except (TypeError, ValueError):
        pass
    max_age = policy.get("restore_drill_max_age_seconds")
    observed_size = raw.get("observed_restored_size_bytes")
    observed_size_valid = (
        isinstance(observed_size, int)
        and not isinstance(observed_size, bool)
        and observed_size > 0
    )
    timescale_ok, timescale_status = _timescale_receipt_check(
        raw.get("timescale_restore"), snapshot, policy
    )
    checks_raw = {
        "deployment_scoped_path": (
            path == _restore_drill_receipt_path(ctx)
            and not path.is_symlink()
            and path.resolve().parent == ctx.backup_dir.resolve()
        ),
        "typed_schema": raw.get("schema") == "okg.deployment_restore_drill.v1",
        "private_mode": stat.st_mode & 0o777 == 0o600,
        "current_owner": stat.st_uid == os.geteuid(),
        "fresh": (
            age_seconds is not None
            and age_seconds >= 0
            and isinstance(max_age, (int, float))
            and age_seconds <= float(max_age)
        ),
        "deployment_equal": (
            raw.get("deployment") == ctx.options.deployment
            and receipt_target.get("deployment") == current_target.get("deployment")
        ),
        "host_equal": receipt_target.get("host") == current_target.get("host"),
        "port_equal": receipt_target.get("port") == current_target.get("port"),
        "database_equal": (
            receipt_target.get("database") == current_target.get("database")
        ),
        "database_owner_equal": (
            compatibility.get("database_owner") == snapshot["database_owner"]
        ),
        "cluster_system_identifier_equal": (
            compatibility.get("cluster_system_identifier")
            == snapshot["cluster_system_identifier"]
        ),
        "server_version_equal": (
            compatibility.get("server_version") == snapshot["server_version"]
        ),
        "extension_versions_equal": (
            compatibility.get("extension_versions")
            == snapshot["extension_versions"]
        ),
        "schema_migrations_equal": (
            compatibility.get("schema_migrations_digest")
            == snapshot["schema_migrations_digest"]
            and compatibility.get("schema_catalog_digest")
            == snapshot["schema_catalog_digest"]
        ),
        "candidate_plan_equal": (
            compatibility.get("candidate_plan_digest")
            == snapshot["candidate_plan_digest"]
        ),
        "archive_format_equal": (
            compatibility.get("archive_format") == snapshot["archive_format"]
        ),
        "dump_tool_major_equal": (
            compatibility.get("dump_tool_major") == snapshot["dump_tool_major"]
        ),
        "restore_tool_major_equal": (
            compatibility.get("restore_tool_major")
            == snapshot["restore_tool_major"]
        ),
        "timescale_restore_proven_or_risk_admitted": timescale_ok,
        "observed_restored_size_valid": observed_size_valid,
    }
    checks, errors = parse_restore_drill_checks(checks_raw)
    prior_size, prior_size_discarded = _target_bound_prior_size(
        _receipt_target_binding(
            checks_raw,
            receipt_owner=compatibility.get("database_owner"),
            snapshot_owner=snapshot.get("database_owner"),
        ),
        observed_size if observed_size_valid else None,
    )
    return checks, errors, {
        "status": "compatible" if checks and checks.compatible else "incompatible",
        "path": str(path),
        "age_seconds": age_seconds,
        "timescale_restore_status": timescale_status,
        "checks": checks_raw,
        **(
            {"prior_size_discarded": prior_size_discarded}
            if prior_size_discarded is not None
            else {}
        ),
    }, prior_size


def _write_restore_drill_receipt(
    ctx: UpgradeContext,
    snapshot: dict[str, Any],
    restore: dict[str, Any],
) -> dict[str, Any]:
    path = _restore_drill_receipt_path(ctx)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    payload = {
        "schema": "okg.deployment_restore_drill.v1",
        "deployment": ctx.options.deployment,
        "verified_at": _now(),
        "compatibility": snapshot,
        # Earned by the executed restore's success branch (both TimescaleDB
        # procedure calls ran and the restored hypertable set matched) —
        # deliberately not copied from the pre-dump snapshot.
        "timescale_restore": restore.get("timescale_restore"),
        "observed_restored_size_bytes": restore.get(
            "observed_restored_size_bytes"
        ),
    }
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
    stat = path.stat()
    if stat.st_mode & 0o777 != 0o600 or stat.st_uid != os.geteuid():
        raise OSError("restore-drill receipt privacy/owner verification failed")
    return {
        "schema": payload["schema"],
        "path": str(path),
        "mode": "0600",
        "owner_uid": stat.st_uid,
        "written_at": payload["verified_at"],
    }


def _disaster_recovery_receipt_path(ctx: UpgradeContext) -> Path:
    return ctx.backup_dir / (
        f"{ctx.options.deployment}-latest-disaster-recovery.json"
    )


def _parse_receipt_time(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _write_disaster_recovery_receipt(
    ctx: UpgradeContext,
    *,
    backup_path: Path,
    archive: dict[str, Any],
    snapshot: dict[str, Any],
    restore: dict[str, Any],
    policy: dict[str, Any],
) -> dict[str, Any]:
    if not ctx.options.independent_backup_verify:
        raise OSError("disaster-recovery receipts require independent verification")
    digest = archive.get("backup_digest")
    if not isinstance(digest, str) or not digest.startswith("sha256:"):
        raise OSError("verified archive digest is unavailable")
    stat = backup_path.stat()
    archive_kind = str(archive.get("archive_kind") or "file")
    manifest_receipt = None
    if archive_kind == "directory":
        manifest_path = backup_path / _ARCHIVE_MANIFEST_NAME
        manifest_stat = manifest_path.stat()
        manifest_receipt = {
            "path": str(manifest_path.resolve()),
            "size_bytes": manifest_stat.st_size,
            "owner_uid": manifest_stat.st_uid,
            "device": manifest_stat.st_dev,
            "inode": manifest_stat.st_ino,
            "mtime_ns": manifest_stat.st_mtime_ns,
        }
    recovery_point = datetime.fromtimestamp(stat.st_mtime, timezone.utc)
    rpo = policy.get("recovery_point_objective_seconds")
    if not isinstance(rpo, (int, float)) or isinstance(rpo, bool) or rpo <= 0:
        raise OSError("recovery-point objective is unavailable")
    verified_at = datetime.now(timezone.utc)
    next_required = recovery_point.timestamp() + float(rpo)
    payload = {
        "schema": "okg.deployment_disaster_recovery.v1",
        "operation": "independent_backup_verify",
        "deployment": ctx.options.deployment,
        "archive": {
            "path": str(backup_path.resolve()),
            "kind": archive_kind,
            "digest": digest,
            "size_bytes": archive.get("backup_size_bytes"),
            "mode": "0700" if archive_kind == "directory" else "0600",
            "owner_uid": stat.st_uid,
            "device": stat.st_dev,
            "inode": stat.st_ino,
            "mtime_ns": stat.st_mtime_ns,
            **({"manifest": manifest_receipt} if manifest_receipt else {}),
        },
        "restored_archive_digest": digest,
        "recovery_point_at": recovery_point.isoformat(),
        "recovery_point_objective_seconds": float(rpo),
        "verified_at": verified_at.isoformat(),
        "next_required_backup_at": datetime.fromtimestamp(
            next_required, timezone.utc
        ).isoformat(),
        "compatibility": snapshot,
        # Earned by the executed restore's success branch, like the drill
        # receipt's block; never copied from the pre-dump snapshot.
        "timescale_restore": restore.get("timescale_restore"),
        "observed_restored_size_bytes": restore.get(
            "observed_restored_size_bytes"
        ),
    }
    path = _disaster_recovery_receipt_path(ctx)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
    receipt_stat = path.stat()
    if receipt_stat.st_mode & 0o777 != 0o600 or receipt_stat.st_uid != os.geteuid():
        raise OSError("disaster-recovery receipt privacy/owner verification failed")
    return {
        "schema": payload["schema"],
        "path": str(path),
        "archive_digest": digest,
        "recovery_point_at": payload["recovery_point_at"],
        "verified_at": payload["verified_at"],
        "next_required_backup_at": payload["next_required_backup_at"],
    }


def _disaster_recovery_receipt_evidence(
    ctx: UpgradeContext,
    snapshot: dict[str, Any],
    policy: dict[str, Any],
) -> tuple[Any, tuple[str, ...], dict[str, Any], int | None]:
    from okg.substrate.deployment_backup_policy import (
        parse_disaster_recovery_checks,
    )

    path = _disaster_recovery_receipt_path(ctx)
    if not path.is_file():
        return None, (), {"status": "missing", "path": str(path)}, None
    try:
        receipt_stat = path.stat()
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return None, (f"disaster_recovery_malformed:{type(exc).__name__}",), {
            "status": "malformed",
            "path": str(path),
            "error": str(exc),
        }, None
    if not isinstance(raw, dict):
        return None, ("disaster_recovery_not_object",), {
            "status": "malformed",
            "path": str(path),
        }, None
    archive_raw = raw.get("archive")
    archive_raw = archive_raw if isinstance(archive_raw, dict) else {}
    compatibility = raw.get("compatibility")
    compatibility = compatibility if isinstance(compatibility, dict) else {}
    receipt_target = compatibility.get("target")
    receipt_target = receipt_target if isinstance(receipt_target, dict) else {}
    current_target = snapshot.get("target")
    current_target = current_target if isinstance(current_target, dict) else {}
    archive_path_raw = archive_raw.get("path")
    archive_path = (
        Path(archive_path_raw)
        if isinstance(archive_path_raw, str) and archive_path_raw
        else None
    )
    archive_stat = None
    if archive_path is not None:
        try:
            archive_stat = archive_path.stat()
        except OSError:
            archive_stat = None
    archive_kind = archive_raw.get("kind")
    expected_archive_type = (
        archive_path is not None
        and (
            (archive_kind == "file" and archive_path.is_file())
            or (archive_kind == "directory" and archive_path.is_dir())
        )
    )
    manifest_raw = archive_raw.get("manifest")
    manifest_raw = manifest_raw if isinstance(manifest_raw, dict) else None
    manifest_unchanged = archive_kind == "file" and manifest_raw is None
    manifest_stat = None
    if archive_kind == "directory" and manifest_raw is not None:
        manifest_path_raw = manifest_raw.get("path")
        if isinstance(manifest_path_raw, str) and manifest_path_raw:
            manifest_path = Path(manifest_path_raw)
            try:
                manifest_stat = manifest_path.stat()
            except OSError:
                manifest_stat = None
            manifest_unchanged = (
                manifest_stat is not None
                and manifest_path == archive_path / _ARCHIVE_MANIFEST_NAME
                and manifest_path.is_file()
                and not manifest_path.is_symlink()
                and manifest_raw.get("size_bytes") == manifest_stat.st_size
                and manifest_raw.get("owner_uid") == manifest_stat.st_uid
                and manifest_raw.get("device") == manifest_stat.st_dev
                and manifest_raw.get("inode") == manifest_stat.st_ino
                and manifest_raw.get("mtime_ns") == manifest_stat.st_mtime_ns
            )
    now = datetime.now(timezone.utc)
    recovery_point = _parse_receipt_time(raw.get("recovery_point_at"))
    verified_at = _parse_receipt_time(raw.get("verified_at"))
    next_required = _parse_receipt_time(raw.get("next_required_backup_at"))
    archive_age = (
        (now - recovery_point).total_seconds()
        if recovery_point is not None
        else None
    )
    restore_age = (
        (now - verified_at).total_seconds() if verified_at is not None else None
    )
    archive_max_age = policy.get(
        "disaster_recovery_archive_max_age_seconds"
    )
    restore_max_age = policy.get(
        "disaster_recovery_restore_max_age_seconds"
    )
    configured_rpo = policy.get("recovery_point_objective_seconds")
    recorded_rpo = raw.get("recovery_point_objective_seconds")
    digest = archive_raw.get("digest")
    recorded_archive_size = archive_raw.get("size_bytes")
    recorded_archive_size_ok = (
        isinstance(recorded_archive_size, int)
        and not isinstance(recorded_archive_size, bool)
        and recorded_archive_size > 0
    )
    archive_unchanged = (
        archive_stat is not None
        and archive_path is not None
        and expected_archive_type
        and not archive_path.is_symlink()
        and archive_path.resolve().parent == ctx.backup_dir.resolve()
        and recorded_archive_size_ok
        and archive_raw.get("owner_uid") == archive_stat.st_uid
        and archive_raw.get("device") == archive_stat.st_dev
        and archive_raw.get("inode") == archive_stat.st_ino
        and archive_raw.get("mtime_ns") == archive_stat.st_mtime_ns
        and manifest_unchanged
    )
    archive_inventory: dict[str, Any] | None = None
    archive_inventory_error: str | None = None
    archive_fingerprint = (
        (
            str(archive_path),
            archive_stat.st_uid,
            archive_stat.st_dev,
            archive_stat.st_ino,
            archive_stat.st_mode,
            archive_stat.st_size,
            archive_stat.st_mtime_ns,
            archive_stat.st_ctime_ns,
            receipt_stat.st_dev,
            receipt_stat.st_ino,
            receipt_stat.st_size,
            receipt_stat.st_mtime_ns,
            receipt_stat.st_ctime_ns,
            (
                (
                    manifest_stat.st_dev,
                    manifest_stat.st_ino,
                    manifest_stat.st_size,
                    manifest_stat.st_mtime_ns,
                    manifest_stat.st_ctime_ns,
                )
                if manifest_stat is not None
                else None
            ),
        )
        if archive_stat is not None and archive_path is not None
        else None
    )
    cache = (
        getattr(ctx, "data", {}).get("dr_archive_inventory_cache")
        if isinstance(getattr(ctx, "data", None), dict)
        else None
    )
    if archive_unchanged and archive_path is not None:
        if (
            isinstance(cache, Mapping)
            and cache.get("fingerprint") == archive_fingerprint
            and isinstance(cache.get("inventory"), dict)
        ):
            archive_inventory = cache["inventory"]
        else:
            try:
                archive_inventory = _archive_member_inventory(archive_path)
                if isinstance(getattr(ctx, "data", None), dict):
                    ctx.data["dr_archive_inventory_cache"] = {
                        "fingerprint": archive_fingerprint,
                        "inventory": archive_inventory,
                    }
            except (OSError, ValueError) as exc:
                archive_inventory_error = f"{type(exc).__name__}: {exc}"
    archive_digest_recomputed = (
        archive_inventory is not None
        and archive_inventory.get("archive_kind") == archive_kind
        and archive_inventory.get("root_digest") == digest
        and archive_inventory.get("total_size_bytes") == recorded_archive_size
    )
    timescale_ok, timescale_status = _timescale_receipt_check(
        raw.get("timescale_restore"), snapshot, policy
    )
    checks_raw = {
        "typed_schema": (
            raw.get("schema") == "okg.deployment_disaster_recovery.v1"
            and raw.get("operation") == "independent_backup_verify"
        ),
        "deployment_scoped_path": (
            path == _disaster_recovery_receipt_path(ctx)
            and not path.is_symlink()
            and path.resolve().parent == ctx.backup_dir.resolve()
            and archive_unchanged
        ),
        "private_mode": (
            receipt_stat.st_mode & 0o777 == 0o600
            and archive_stat is not None
            and archive_stat.st_mode & 0o777
            == (0o700 if archive_kind == "directory" else 0o600)
            and archive_raw.get("mode")
            == ("0700" if archive_kind == "directory" else "0600")
        ),
        "current_owner": (
            receipt_stat.st_uid == os.geteuid()
            and archive_stat is not None
            and archive_stat.st_uid == os.geteuid()
        ),
        "archive_digest_bound": (
            isinstance(digest, str)
            and re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is not None
            and archive_unchanged
            and archive_digest_recomputed
        ),
        "same_archive_restored": raw.get("restored_archive_digest") == digest,
        "archive_fresh": (
            archive_age is not None
            and archive_age >= 0
            and isinstance(archive_max_age, (int, float))
            and archive_age <= float(archive_max_age)
            and next_required is not None
            and now <= next_required
        ),
        "restore_proof_fresh": (
            restore_age is not None
            and restore_age >= 0
            and isinstance(restore_max_age, (int, float))
            and restore_age <= float(restore_max_age)
        ),
        "deployment_equal": (
            raw.get("deployment") == ctx.options.deployment
            and receipt_target.get("deployment") == current_target.get("deployment")
        ),
        "host_equal": receipt_target.get("host") == current_target.get("host"),
        "port_equal": receipt_target.get("port") == current_target.get("port"),
        "database_equal": (
            receipt_target.get("database") == current_target.get("database")
        ),
        "database_owner_equal": (
            compatibility.get("database_owner") == snapshot.get("database_owner")
        ),
        "cluster_system_identifier_equal": (
            compatibility.get("cluster_system_identifier")
            == snapshot.get("cluster_system_identifier")
        ),
        "server_version_equal": (
            compatibility.get("server_version") == snapshot.get("server_version")
        ),
        # Tool checks demand a well-formed snapshot side, not mere equality:
        # a failed toolchain probe yields None majors / an empty identity
        # map, and a receipt degenerated the same way must not vacuously
        # match (None == None, {} == {}).
        "dump_tool_major_equal": (
            isinstance(snapshot.get("dump_tool_major"), int)
            and not isinstance(snapshot.get("dump_tool_major"), bool)
            and snapshot["dump_tool_major"] > 0
            and compatibility.get("dump_tool_major")
            == snapshot.get("dump_tool_major")
        ),
        "restore_tool_major_equal": (
            isinstance(snapshot.get("restore_tool_major"), int)
            and not isinstance(snapshot.get("restore_tool_major"), bool)
            and snapshot["restore_tool_major"] > 0
            and compatibility.get("restore_tool_major")
            == snapshot.get("restore_tool_major")
        ),
        "backup_tool_executable_identity_equal": (
            isinstance(
                snapshot.get("backup_tool_executable_identity"), Mapping
            )
            and set(snapshot["backup_tool_executable_identity"])
            == {"pg_dump", "pg_restore"}
            and compatibility.get("backup_tool_executable_identity")
            == snapshot.get("backup_tool_executable_identity")
        ),
        "extension_versions_equal": (
            compatibility.get("extension_versions")
            == snapshot.get("extension_versions")
        ),
        "timescale_restore_proven_or_risk_admitted": timescale_ok,
        "schema_lineage_equal": (
            compatibility.get("schema_migrations_digest")
            == snapshot.get("schema_migrations_digest")
            and compatibility.get("schema_catalog_digest")
            == snapshot.get("schema_catalog_digest")
        ),
        "recovery_point_objective_declared": (
            isinstance(configured_rpo, (int, float))
            and not isinstance(configured_rpo, bool)
            and configured_rpo > 0
            and recorded_rpo == float(configured_rpo)
            and recovery_point is not None
            and next_required is not None
            and abs(
                (next_required - recovery_point).total_seconds()
                - float(configured_rpo)
            )
            < 0.001
        ),
    }
    checks, errors = parse_disaster_recovery_checks(checks_raw)
    observed_size = raw.get("observed_restored_size_bytes")
    prior_size, prior_size_discarded = _target_bound_prior_size(
        _receipt_target_binding(
            checks_raw,
            receipt_owner=compatibility.get("database_owner"),
            snapshot_owner=snapshot.get("database_owner"),
        ),
        observed_size
        if isinstance(observed_size, int)
        and not isinstance(observed_size, bool)
        and observed_size > 0
        else None,
    )
    return checks, errors, {
        "status": "compatible" if checks and checks.compatible else "incompatible",
        "path": str(path),
        "archive_age_seconds": archive_age,
        "restore_proof_age_seconds": restore_age,
        "next_required_backup_at": raw.get("next_required_backup_at"),
        "timescale_restore_status": timescale_status,
        "checks": checks_raw,
        **(
            {"prior_size_discarded": prior_size_discarded}
            if prior_size_discarded is not None
            else {}
        ),
        "archive_inventory": {
            "ok": archive_digest_recomputed,
            "observed_digest": (
                archive_inventory.get("root_digest")
                if archive_inventory is not None
                else None
            ),
            "observed_size_bytes": (
                archive_inventory.get("total_size_bytes")
                if archive_inventory is not None
                else None
            ),
            "member_count": (
                archive_inventory.get("member_count")
                if archive_inventory is not None
                else None
            ),
            "error": archive_inventory_error,
        },
    }, prior_size


def _run_current_full_restore_verification(
    ctx: UpgradeContext,
    backup_path: Path,
    *,
    snapshot: dict[str, Any],
    policy: dict[str, Any],
    source_database_size: int | None,
    prior_restored_size: int | None,
    archive: dict[str, Any] | None = None,
) -> dict[str, Any]:
    restore_headroom = _restore_headroom_preflight(
        ctx,
        policy,
        source_database_size=source_database_size,
        prior_restored_size=prior_restored_size,
    )
    if restore_headroom.get("refuse"):
        return {
            "ok": False,
            "reason": "insufficient or unprobeable restore-volume headroom",
            "restore_headroom": restore_headroom,
            "fix_hint": (
                "declare/provision the restore volume and reclaim storage before "
                "rerunning full backup verification"
            ),
        }
    restore = _verify_backup_restore(
        ctx,
        backup_path,
        policy=policy,
        expected_snapshot=snapshot,
    )
    if not restore.get("ok"):
        return {**restore, "restore_headroom": restore_headroom}
    try:
        receipt = _write_restore_drill_receipt(ctx, snapshot, restore)
    except OSError as exc:
        return {
            "ok": False,
            "reason": "restore-drill receipt could not be written atomically",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "restore_headroom": restore_headroom,
        }
    disaster_recovery_receipt = None
    if ctx.options.independent_backup_verify:
        try:
            disaster_recovery_receipt = _write_disaster_recovery_receipt(
                ctx,
                backup_path=backup_path,
                archive=archive or {},
                snapshot=snapshot,
                restore=restore,
                policy=policy,
            )
        except OSError as exc:
            return {
                "ok": False,
                "reason": "disaster-recovery receipt could not be written",
                "error": {"type": type(exc).__name__, "message": str(exc)},
                "restore_headroom": restore_headroom,
            }
    return {
        **restore,
        "verification_mode": "current_full_restore",
        "restore_current_archive": True,
        "receipt_reused": False,
        "restore_headroom": restore_headroom,
        "restore_drill_receipt": receipt,
        **(
            {"disaster_recovery_receipt": disaster_recovery_receipt}
            if disaster_recovery_receipt is not None
            else {}
        ),
    }


def _estimate_source_db_bytes(ctx: UpgradeContext) -> int | None:
    """Cheap upper-bound estimate of the dump size: ``pg_database_size``
    of the source database (one SQL call; the custom-format dump is
    compressed, so the raw size overestimates — conservative in the
    right direction). Returns None when the probe is not cheaply
    available (unreachable DB, no psycopg); the headroom preflight then
    refuses on the blocker threshold alone.
    """
    try:
        from okg.substrate.db.connection import connect as db_connect

        with db_connect(ctx.dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_database_size(current_database())"
                )
                row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None
    except Exception:  # noqa: BLE001 - estimate is best-effort; the preflight still enforces the blocker floor without it.
        return None


def _backup_headroom_preflight(
    ctx: UpgradeContext,
    *,
    needed_bytes: int | None = None,
) -> dict[str, Any]:
    """Host-headroom preflight for the backup phase (PACT
    ``bound-host-storage-pressure``): the 2026-07-17 incident had
    ``okg up`` writing a ~4.2 GB dump into an already-starved disk.

    Enforcement requires the deployment to declare
    ``host_storage.blocker_available_pct`` (thresholds are never
    silently defaulted — P0); with thresholds declared, the dump is
    refused when writing an estimated ``pg_database_size`` worth of
    bytes would land the backup filesystem below the blocker floor
    (shortfall named), or — when the estimate is not cheaply available
    and refuses when the database-size probe is unreadable. An unknown write
    bound is not admitted merely because the filesystem starts above the floor.
    """
    from okg.substrate.deployments import find_deployment_dir
    from okg.substrate.runtime import host_headroom

    deployment_dir = find_deployment_dir(ctx.options.deployment)
    thresholds, problems, threshold_authority = (
        _headroom_threshold_authority(ctx, deployment_dir=deployment_dir)
    )
    # Only pay for the size estimate (a DB round-trip) when enforcement
    # is armed; undeclared thresholds are themselves a refusal below.
    if needed_bytes is None and thresholds is not None:
        needed_bytes = _estimate_source_db_bytes(ctx)
    report = host_headroom.big_writer_preflight(
        operation="okg-up backup (pg_dump)",
        deployment=ctx.options.deployment,
        deployment_dir=deployment_dir,
        target_path=ctx.backup_dir,
        needed_bytes=needed_bytes,
        threshold_override=(thresholds, problems),
    )
    report["threshold_authority"] = threshold_authority
    if thresholds is None:
        report.update({
            "ok": False,
            "refuse": True,
            "status": "misconfigured",
            "message": (
                "backup refused because archive-volume headroom thresholds "
                "are not enforceable"
            ),
        })
    elif needed_bytes is None:
        report.update({
            "ok": False,
            "refuse": True,
            "status": "unreadable_source_size",
            "code": "backup_source_size_unreadable",
            "message": (
                "backup refused because pg_database_size is unavailable and "
                "the archive write bound cannot be enforced"
            ),
        })
    return report


def _backup_post_dump_headroom_preflight(ctx: UpgradeContext) -> dict[str, Any]:
    """Re-probe archive storage after the dump consumed shared capacity."""
    from okg.substrate.deployments import find_deployment_dir
    from okg.substrate.runtime import host_headroom

    deployment_dir = find_deployment_dir(ctx.options.deployment)
    thresholds, problems, threshold_authority = (
        _headroom_threshold_authority(ctx, deployment_dir=deployment_dir)
    )
    report = host_headroom.big_writer_preflight(
        operation="okg-up post-dump archive headroom",
        deployment=ctx.options.deployment,
        deployment_dir=deployment_dir,
        target_path=ctx.backup_dir,
        needed_bytes=0,
        threshold_override=(thresholds, problems),
    )
    report["threshold_authority"] = threshold_authority
    if report.get("status") == "misconfigured":
        report["refuse"] = True
        report["message"] = (
            "post-dump archive verification refused because host-storage "
            "thresholds are not enforceable"
        )
    return report


def _restore_headroom_preflight(
    ctx: UpgradeContext,
    policy: dict[str, Any],
    *,
    source_database_size: int | None,
    prior_restored_size: int | None,
) -> dict[str, Any]:
    """Resolve and enforce disposable-restore peak storage independently."""
    from okg.substrate.deployments import find_deployment_dir
    from okg.substrate.runtime import host_headroom

    deployment_dir = find_deployment_dir(ctx.options.deployment)
    thresholds, problems, threshold_authority = (
        _headroom_threshold_authority(ctx, deployment_dir=deployment_dir)
    )
    volume = policy.get("restore_volume") or {}
    kind = volume.get("kind")
    target: Path | None
    if kind == "docker_data_root":
        target = host_headroom.resolve_docker_data_root()
    elif kind == "host_path":
        raw_path = volume.get("path")
        target = Path(raw_path).expanduser() if isinstance(raw_path, str) else None
    else:
        target = None
    if target is None:
        return {
            "ok": False,
            "refuse": True,
            "status": "unresolved",
            "operation": "okg-up disposable restore",
            "volume_kind": kind,
            "message": "disposable restore volume could not be resolved",
        }
    if not target.exists() or not target.is_dir():
        return {
            "ok": False,
            "refuse": True,
            "status": "restore_volume_missing",
            "operation": "okg-up disposable restore",
            "target_path": str(target),
            "volume_kind": kind,
            "message": (
                "declared disposable restore volume must exist as a directory; "
                "refusing to probe a parent filesystem for a missing path"
            ),
        }
    if source_database_size is None or source_database_size <= 0:
        return {
            "ok": False,
            "refuse": True,
            "status": "unreadable_source_size",
            "operation": "okg-up disposable restore",
            "target_path": str(target),
            "message": "source database size is unavailable; restore peak is unknown",
        }
    prior = prior_restored_size or 0
    factor = float(policy["restore_peak_factor"])
    needed = math.ceil(max(source_database_size, prior) * factor)
    report = host_headroom.big_writer_preflight(
        operation="okg-up disposable restore",
        deployment=ctx.options.deployment,
        deployment_dir=deployment_dir,
        target_path=target,
        needed_bytes=needed,
        threshold_override=(thresholds, problems),
    )
    report.update({
        "threshold_authority": threshold_authority,
        "volume_kind": kind,
        "resolved_volume_path": str(target.resolve()),
        "volume_device_id": target.stat().st_dev,
        "source_database_size_bytes": source_database_size,
        "prior_observed_restored_size_bytes": prior_restored_size,
        "restore_peak_factor": factor,
        "estimated_peak_bytes": needed,
    })
    if report.get("status") == "misconfigured":
        report["refuse"] = True
        report["message"] = (
            "disposable restore refused because host-storage thresholds are "
            "not enforceable"
        )
    if (
        report.get("refuse") is True
        and report.get("status") == "blocker"
        and report.get("code") == "host_headroom_blocker"
    ):
        from okg.substrate.db.cold_history import (
            cold_history_reclaim_advisory,
        )

        advisory = cold_history_reclaim_advisory(
            ctx.dsn,
            deployment=ctx.options.deployment,
        )
        report["cold_history_reclaim"] = advisory
        inspect_command = advisory["inspect_command"]
        if advisory["status"] == "candidates":
            activate_command = advisory["activate_command"]
            report["message"] += (
                "; cold-history found "
                f"{advisory['rotatable_full_index_bytes']} gross bytes across "
                f"{advisory['rotatable_index_count']} rotatable full-history "
                "visibility indexes. Inspect the read-only plan with "
                f"`{inspect_command}`. If the coverage tradeoff is accepted, "
                f"rotate explicitly with `{activate_command}`; this drops the "
                "listed full-history B-tree predecessors, and may still leave "
                "insufficient net headroom"
            )
        elif advisory["status"] == "no_candidates":
            report["message"] += (
                "; cold-history found 0 remaining rotatable full-history "
                f"visibility indexes. Recheck with `{inspect_command}`"
            )
        else:
            error = advisory["error"]
            report["message"] += (
                "; cold-history reclaim could not be measured "
                f"({error['type']}: {error['message']}). Retry the read-only "
                f"plan with `{inspect_command}`"
            )
    return report


def _headroom_threshold_authority(
    ctx: UpgradeContext,
    *,
    deployment_dir: Path | None,
) -> tuple[Any, list[str], dict[str, Any]]:
    """Resolve storage floors from the revision authorized to write.

    Normal upgrades use the installed manifest. A precheckout backup runs
    trusted OLD code for candidate NEW, so it must use NEW's inert manifest
    already loaded and revision-bound by effect inventory. Falling back to
    OLD here would make a candidate configuration repair impossible.
    """
    from okg.substrate.runtime import host_headroom

    target_revision = getattr(
        ctx.options, "precheckout_target_revision", None
    )
    if target_revision is None:
        thresholds, problems = host_headroom.load_host_storage_thresholds(
            deployment_dir,
        )
        return thresholds, problems, {
            "kind": "installed_manifest",
            "deployment_dir": (
                str(deployment_dir) if deployment_dir is not None else None
            ),
        }

    data = getattr(ctx, "data", {})
    static = (
        data.get("effect_inventory_static")
        if isinstance(data, Mapping)
        else None
    )
    static = static if isinstance(static, Mapping) else {}
    observed_revision = static.get("target_revision")
    raw_manifest = static.get("raw_manifest")
    # For a vendored install the manifest was read from the deployment
    # repository at its own revision, so name that revision rather than
    # implying the code revision carried the manifest.
    manifest_revision = static.get("deployment_revision") or target_revision
    source_label = f"candidate deployment.yaml at {manifest_revision}"
    if observed_revision != target_revision:
        thresholds = None
        problems = [
            "candidate host-storage authority is absent or revision-mismatched "
            f"(expected {target_revision}, observed {observed_revision!r}); "
            "refusing to fall back to the installed predecessor manifest"
        ]
    else:
        thresholds, problems = host_headroom.parse_host_storage_thresholds(
            raw_manifest if isinstance(raw_manifest, Mapping) else None,
            source_label=source_label,
        )
    return thresholds, problems, {
        "kind": "candidate_manifest",
        "target_revision": target_revision,
        "manifest_revision": manifest_revision,
        "effect_inventory_target_revision": observed_revision,
        "target_manifest_digest": _upgrade_manifest_digest(ctx),
        "manifest_bound_by": "precheckout_authority.target_manifest_digest",
    }


def _backup_tool_executable_identity(tool: str) -> dict[str, Any]:
    """Fingerprint a backup executable without starting a child process."""
    located = shutil.which(tool)
    if not located:
        raise FileNotFoundError(tool)
    invoked = Path(located)
    resolved = invoked.resolve(strict=True)
    stat = resolved.stat()
    digest = hashlib.sha256()
    with resolved.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {
        "invoked_path": str(invoked),
        "resolved_path": str(resolved),
        "device": stat.st_dev,
        "inode": stat.st_ino,
        "mode": stat.st_mode,
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "sha256": digest.hexdigest(),
    }


def _receipt_bound_backup_toolchain(ctx: UpgradeContext) -> dict[str, Any]:
    """Revalidate recovery tools from receipt-bound file identity, without exec."""
    path = _disaster_recovery_receipt_path(ctx)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        compatibility = raw["compatibility"]
        recorded = compatibility["backup_tool_executable_identity"]
        majors = {
            "pg_dump": compatibility["dump_tool_major"],
            "pg_restore": compatibility["restore_tool_major"],
        }
        if not isinstance(recorded, dict):
            raise TypeError("backup tool identity is not an object")
        tools: dict[str, Any] = {}
        for tool in ("pg_dump", "pg_restore"):
            major = majors[tool]
            if isinstance(major, bool) or not isinstance(major, int) or major <= 0:
                raise TypeError(f"{tool} major is invalid")
            observed = _backup_tool_executable_identity(tool)
            if recorded.get(tool) != observed:
                return {
                    "ok": False,
                    "reason": "backup tool executable changed after recovery proof",
                    "required_tool": tool,
                    "receipt_path": str(path),
                    "recorded_identity": recorded.get(tool),
                    "observed_identity": observed,
                }
            tools[tool] = {
                "version": f"receipt-bound PostgreSQL {major}",
                "major": major,
                "executable_identity": observed,
            }
    except (FileNotFoundError, KeyError, OSError, TypeError, ValueError) as exc:
        return {
            "ok": False,
            "reason": "receipt-bound backup tool identity is unavailable",
            "receipt_path": str(path),
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    return {
        "ok": True,
        "validation": "receipt_bound_executable_identity_without_child",
        "tools": tools,
    }


def _backup_tool_preflight(ctx: UpgradeContext) -> dict[str, Any]:
    """Require a coherent dump/restore toolchain before the worker is stopped."""
    versions: dict[str, dict[str, Any]] = {}
    for tool in ("pg_dump", "pg_restore"):
        cmd = [tool, "--version"]
        try:
            result = ctx.command_runner(cmd, ctx.env)
        except FileNotFoundError as exc:
            return {
                "ok": False,
                "reason": "backup tool missing",
                "required_tool": tool,
                "command": _display_command(cmd),
                "error": {"type": type(exc).__name__, "message": str(exc)},
                "fix_hint": (
                    "install pg_dump and pg_restore with the same major version "
                    "as the Postgres server before resuming; production recovery "
                    "cannot skip its backup"
                ),
            }
        if not result.ok:
            return _failed_command(
                "backup tool version check failed",
                result,
                command=cmd,
            )
        version = (result.stdout or result.stderr or "").strip()
        match = re.search(r"\b(\d+)(?:\.\d+)?\b", version)
        if match is None:
            return {
                "ok": False,
                "reason": "backup tool version unreadable",
                "required_tool": tool,
                "observed": version[-500:],
                "fix_hint": f"verify `{tool} --version` reports a PostgreSQL major version",
            }
        try:
            executable_identity = _backup_tool_executable_identity(tool)
        except (FileNotFoundError, OSError) as exc:
            return {
                "ok": False,
                "reason": "backup tool executable identity is unreadable",
                "required_tool": tool,
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
        versions[tool] = {
            "version": version,
            "major": int(match.group(1)),
            "executable_identity": executable_identity,
        }
    if versions["pg_dump"]["major"] != versions["pg_restore"]["major"]:
        return {
            "ok": False,
            "reason": "backup tools have incompatible major versions",
            "tools": versions,
            "fix_hint": (
                "install pg_dump and pg_restore from the same PostgreSQL major "
                "that matches the server"
            ),
        }
    return {"ok": True, "tools": versions}


def _libpq_command_target(
    dsn: str,
    base_env: dict[str, str],
) -> tuple[str, dict[str, str]]:
    """Return a non-secret dbname argv target plus libpq child environment."""
    from psycopg.conninfo import conninfo_to_dict

    info = conninfo_to_dict(dsn)
    database = str(info.get("dbname") or "")
    if not database:
        raise ValueError("DSN does not name a database")
    env = dict(base_env)
    mapping = {
        "host": "PGHOST",
        "hostaddr": "PGHOSTADDR",
        "port": "PGPORT",
        "user": "PGUSER",
        "password": "PGPASSWORD",
        "passfile": "PGPASSFILE",
        "sslmode": "PGSSLMODE",
        "sslrootcert": "PGSSLROOTCERT",
        "sslcert": "PGSSLCERT",
        "sslkey": "PGSSLKEY",
        "options": "PGOPTIONS",
    }
    for key, env_name in mapping.items():
        value = info.get(key)
        if value is not None:
            env[env_name] = str(value)
    env["PGDATABASE"] = database
    return database, env


_ARCHIVE_MANIFEST_NAME = ".okg-archive-manifest.json"


def _archive_member_inventory(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.exists():
        raise ValueError("backup archive root is missing or a symlink")
    if path.is_file():
        candidates = [(path.name, path)]
        archive_kind = "file"
    elif path.is_dir():
        candidates = []
        for candidate in sorted(path.rglob("*")):
            if candidate.is_symlink():
                raise ValueError(
                    f"backup archive contains symlink: {candidate.relative_to(path)}"
                )
            if candidate.is_dir():
                if candidate.stat().st_mode & 0o077:
                    raise ValueError("backup archive directory is not private")
                continue
            if not candidate.is_file():
                raise ValueError("backup archive contains a non-regular member")
            relative = candidate.relative_to(path).as_posix()
            if relative == _ARCHIVE_MANIFEST_NAME:
                continue
            candidates.append((relative, candidate))
        archive_kind = "directory"
    else:
        raise ValueError("backup archive is not a regular file or directory")
    members: list[dict[str, Any]] = []
    total_size = 0
    for relative, candidate in candidates:
        stat = candidate.stat()
        digest = hashlib.sha256()
        hashed = 0
        with candidate.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
                hashed += len(chunk)
        if hashed != stat.st_size:
            raise ValueError(f"backup member changed while hashing: {relative}")
        total_size += stat.st_size
        members.append({
            "path": relative,
            "size_bytes": stat.st_size,
            "mode": f"{stat.st_mode & 0o777:04o}",
            "owner_uid": stat.st_uid,
            "sha256": digest.hexdigest(),
        })
    if archive_kind == "file":
        root_digest = "sha256:" + members[0]["sha256"] if members else ""
    else:
        root_digest = _canonical_digest(members)
    return {
        "archive_kind": archive_kind,
        "members": members,
        "member_count": len(members),
        "total_size_bytes": total_size,
        "root_digest": root_digest,
    }


def _validate_backup_archive(
    ctx: UpgradeContext,
    path: Path,
    *,
    allow_missing_manifest: bool = False,
) -> dict[str, Any]:
    """Prove a file/directory archive is private, closed, and restorable."""
    started = time.monotonic()
    try:
        if path.is_file() and not path.is_symlink():
            path.chmod(0o600)
        inventory = _archive_member_inventory(path)
        stat = path.stat()
    except (OSError, ValueError) as exc:
        return {
            "ok": False,
            "reason": "backup artifact manifest is unsafe or unreadable",
            "backup_path": str(path),
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    manifest_ok = inventory["archive_kind"] == "file"
    manifest_path: Path | None = None
    if inventory["archive_kind"] == "directory":
        manifest_path = path / _ARCHIVE_MANIFEST_NAME
        if manifest_path.is_file() and not manifest_path.is_symlink():
            try:
                raw_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                raw_manifest = None
            manifest_ok = (
                isinstance(raw_manifest, dict)
                and raw_manifest.get("schema") == "okg.backup_archive_manifest.v1"
                and raw_manifest.get("archive_kind") == "directory"
                and raw_manifest.get("members") == inventory["members"]
                and raw_manifest.get("root_digest") == inventory["root_digest"]
            )
        elif allow_missing_manifest:
            manifest_ok = True
    try:
        listing = ctx.command_runner(["pg_restore", "--list", str(path)], ctx.env)
    except FileNotFoundError as exc:
        return {
            "ok": False,
            "reason": "backup restore tool disappeared after preflight",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    if not listing.ok:
        return _failed_command(
            "backup archive validation failed",
            listing,
            command=["pg_restore", "--list", str(path)],
        )
    root_mode = stat.st_mode & 0o777
    expected_root_mode = 0o700 if inventory["archive_kind"] == "directory" else 0o600
    members_private = all(
        row["mode"] == "0600" and row["owner_uid"] == os.geteuid()
        for row in inventory["members"]
    )
    checks = {
        "size_ok": inventory["total_size_bytes"] > 0,
        "private_mode": root_mode == expected_root_mode and members_private,
        "current_owner": stat.st_uid == os.geteuid(),
        "digest_ok": bool(inventory["root_digest"]),
        "toc_readable": bool(listing.stdout.strip()),
        "manifest_ok": manifest_ok,
        "symlink_safe": True,
    }
    if not all(checks.values()):
        return {
            "ok": False,
            "reason": "backup archive privacy or integrity checks failed",
            "backup_path": str(path),
            "archive_checks": checks,
        }
    return {
        "ok": True,
        "backup_size_bytes": inventory["total_size_bytes"],
        "backup_digest": inventory["root_digest"],
        "backup_mode": f"{expected_root_mode:04o}",
        "archive_kind": inventory["archive_kind"],
        "archive_member_count": inventory["member_count"],
        "archive_readable": True,
        "archive_checks": checks,
        "archive_validation_duration_seconds": time.monotonic() - started,
        "_archive_members": inventory["members"],
        **(
            {"archive_manifest_path": str(manifest_path)}
            if manifest_path is not None
            else {}
        ),
    }


def _backup_archive_metadata_fingerprint(path: Path) -> tuple[Any, ...]:
    """Cheap run-local drift key; directory members are statted, never hashed."""
    root = path.stat()
    root_row = (
        root.st_dev,
        root.st_ino,
        root.st_mode,
        root.st_uid,
        root.st_size,
        root.st_mtime_ns,
        root.st_ctime_ns,
    )
    if not path.is_dir() or path.is_symlink():
        return ("file", root_row)
    members: list[tuple[Any, ...]] = []
    for member in sorted(path.rglob("*")):
        relative = member.relative_to(path).as_posix()
        stat = member.lstat()
        members.append((
            relative,
            stat.st_dev,
            stat.st_ino,
            stat.st_mode,
            stat.st_uid,
            stat.st_size,
            stat.st_mtime_ns,
            stat.st_ctime_ns,
        ))
    return ("directory", root_row, tuple(members))


def _canonical_fingerprint(value: Any) -> Any:
    """Normalize a metadata fingerprint so a JSON-round-tripped copy compares
    equal to a freshly built one. State is persisted as JSON, which turns the
    fingerprint's nested tuples into lists; comparing raw tuple-vs-list would
    always miss and force a full re-hash on every resume."""
    if isinstance(value, (list, tuple)):
        return [_canonical_fingerprint(item) for item in value]
    return value


def _resolved_backup_path(banked: Mapping[str, Any]) -> str | None:
    raw = banked.get("backup_path")
    if not isinstance(raw, str) or not raw:
        return None
    try:
        return str(Path(raw).resolve())
    except OSError:
        return None


def _usable_validation_cache(cache: Any, resolved: str) -> bool:
    return (
        isinstance(cache, Mapping)
        and cache.get("path") == resolved
        and cache.get("fingerprint") is not None
        and isinstance(cache.get("validation"), dict)
    )


def _seed_validation_cache_from_backup_result(
    ctx: UpgradeContext,
    resolved: str,
) -> dict[str, Any] | None:
    """Rebuild the in-run validation cache from the persisted backup result.

    The validation cache lives only in ``ctx.data`` and is not rehydrated on
    resume, but the banked ``backup_result`` (which is rehydrated) carries the
    persisted metadata fingerprint. Seeding from it lets a clean resume trust
    the banked content digest without re-hashing the whole archive."""
    banked = ctx.data.get("backup_result")
    if not isinstance(banked, dict):
        return None
    fingerprint = banked.get("archive_metadata_fingerprint")
    if fingerprint is None:
        return None
    if _resolved_backup_path(banked) != resolved:
        return None
    if banked.get("ok") is not True or not isinstance(
        banked.get("backup_digest"), str
    ):
        return None
    return {
        "path": resolved,
        "fingerprint": fingerprint,
        "validation": banked,
    }


def _bank_archive_validation(
    ctx: UpgradeContext,
    resolved: str,
    fingerprint: Any,
    validation: Mapping[str, Any],
) -> None:
    """Cache the content-digest proof behind a cheap metadata drift key.

    The fingerprint is a cheap KEY, never evidence: it is re-banked into the
    persisted ``backup_result`` only when the content digest still matches what
    banking proved, so a later resume can reuse the digest. A content mismatch
    (tamper) leaves the persisted key untouched so the mutation-boundary refusal
    still fires."""
    canonical = _canonical_fingerprint(fingerprint)
    ctx.data["backup_archive_validation_cache"] = {
        "path": resolved,
        "fingerprint": canonical,
        "validation": validation,
    }
    banked = ctx.data.get("backup_result")
    if (
        isinstance(banked, dict)
        and _resolved_backup_path(banked) == resolved
        and validation.get("ok") is True
        and isinstance(validation.get("backup_digest"), str)
        and validation.get("backup_digest") == banked.get("backup_digest")
    ):
        banked["archive_metadata_fingerprint"] = canonical


def _reuse_validation_if_toc_readable(
    ctx: UpgradeContext,
    path: Path,
    banked: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Cheap re-validation when the metadata drift key matches.

    The matched fingerprint already proves member mode/size/owner and archive
    timestamps are unchanged since the digest was banked, so the content is
    trusted without re-reading it. We still re-list the archive table of
    contents (cheap) to re-confirm it is restorable before reusing the digest;
    an unreadable or changed TOC returns ``None`` so the caller falls through to
    a full content re-digest. This decouples the cheap restorability check from
    the expensive per-member re-hash."""
    if not isinstance(banked, Mapping) or banked.get("ok") is not True:
        return None
    if not isinstance(banked.get("backup_digest"), str):
        return None
    try:
        listing = ctx.command_runner(["pg_restore", "--list", str(path)], ctx.env)
    except OSError:
        # Any spawn failure degrades to the safe slow path (full re-digest),
        # never a reuse of the banked digest.
        return None
    if not getattr(listing, "ok", False) or not listing.stdout.strip():
        return None
    reused = dict(banked)
    reused["archive_content_rehash_skipped"] = True
    return reused


def _validate_backup_archive_for_run(
    ctx: UpgradeContext,
    path: Path,
) -> dict[str, Any]:
    """Hash once per run and rehash only after observable metadata drift.

    The metadata fingerprint is a cheap drift KEY (persisted across resumes via
    the banked backup result); the content digest is the evidence. When the key
    matches, the archive TOC is re-listed (cheap) and the banked digest reused
    without re-reading the whole archive. Any drift forces a full content
    re-digest (never a skip); a matching re-digest re-banks the key so
    subsequent boundaries stay cheap."""
    try:
        resolved = str(path.resolve())
        fingerprint = _backup_archive_metadata_fingerprint(path)
    except OSError:
        return _validate_backup_archive(ctx, path)
    canonical = _canonical_fingerprint(fingerprint)
    cache = ctx.data.get("backup_archive_validation_cache")
    if not _usable_validation_cache(cache, resolved):
        cache = _seed_validation_cache_from_backup_result(ctx, resolved)
    if (
        _usable_validation_cache(cache, resolved)
        and _canonical_fingerprint(cache["fingerprint"]) == canonical
    ):
        reused = _reuse_validation_if_toc_readable(ctx, path, cache["validation"])
        if reused is not None:
            # Keep the drift key hot for later boundaries in this run.
            ctx.data["backup_archive_validation_cache"] = {
                "path": resolved,
                "fingerprint": canonical,
                "validation": cache["validation"],
            }
            return reused
    validation = _validate_backup_archive(ctx, path)
    try:
        # Validation may converge private modes with chmod, which updates ctime.
        # Cache the post-validation fingerprint or the next unchanged boundary
        # would look like drift and hash the archive again.
        fingerprint = _backup_archive_metadata_fingerprint(path)
    except OSError:
        pass
    _bank_archive_validation(ctx, resolved, fingerprint, validation)
    return validation


def _secure_backup_tree(path: Path) -> None:
    if path.is_symlink():
        raise ValueError("backup temporary artifact is a symlink")
    if path.is_dir():
        path.chmod(0o700)
        for candidate in path.rglob("*"):
            if candidate.is_symlink():
                raise ValueError("backup temporary artifact contains a symlink")
            candidate.chmod(0o700 if candidate.is_dir() else 0o600)
    elif path.is_file():
        path.chmod(0o600)
    else:
        raise ValueError("backup temporary artifact is missing")


def _finalize_backup_artifact(
    ctx: UpgradeContext,
    *,
    temporary_path: Path,
    final_path: Path,
    strategy: dict[str, Any],
) -> dict[str, Any]:
    started = time.monotonic()
    if temporary_path.parent.resolve() != final_path.parent.resolve():
        return {
            "ok": False,
            "reason": "backup temporary and final artifacts are on different filesystems",
        }
    if final_path.exists() or final_path.is_symlink():
        return {
            "ok": False,
            "reason": "backup final artifact already exists",
            "backup_path": str(final_path),
        }
    try:
        _secure_backup_tree(temporary_path)
    except (OSError, ValueError) as exc:
        return {
            "ok": False,
            "reason": "backup temporary artifact could not be secured",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    archive = _validate_backup_archive(
        ctx,
        temporary_path,
        allow_missing_manifest=True,
    )
    if not archive.get("ok"):
        return archive
    members = archive.pop("_archive_members", [])
    if archive.get("archive_kind") == "directory":
        manifest_path = temporary_path / _ARCHIVE_MANIFEST_NAME
        payload = {
            "schema": "okg.backup_archive_manifest.v1",
            "archive_kind": "directory",
            "format": strategy["format"],
            "compression": strategy["compression"],
            "compression_level": strategy["compression_level"],
            "member_count": archive["archive_member_count"],
            "total_size_bytes": archive["backup_size_bytes"],
            "root_digest": archive["backup_digest"],
            "members": members,
        }
        manifest_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest_path.chmod(0o600)
        archive["archive_manifest_path"] = str(
            final_path / _ARCHIVE_MANIFEST_NAME
        )
    try:
        os.replace(temporary_path, final_path)
        directory_fd = os.open(final_path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as exc:
        return {
            "ok": False,
            "reason": "backup artifact atomic finalization failed",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    return {
        **archive,
        "backup_path": str(final_path),
        "atomic_finalized": True,
        "archive_finalization_duration_seconds": time.monotonic() - started,
    }


def _restore_toc_without_server_pinned_extensions(
    listing_stdout: str,
    *,
    timescaledb_prestaged: bool,
) -> str:
    """Filter a ``pg_restore --list`` TOC for the disposable verification DB.

    Two extension families cannot restore verbatim into a disposable
    database and are handled at the TOC layer (the production archive
    remains byte-for-byte complete):

    - ``pg_cron`` can only be created in the server-wide
      ``cron.database_name``; a disposable database necessarily has a
      different name, so its extension-owned entries are omitted.
    - ``timescaledb`` catalog data (``_timescaledb_catalog.*`` config
      tables such as ``dimension_slice``) only COPYs correctly when the
      extension is pre-created at the SOURCE's exact version with
      ``timescaledb_pre_restore()`` active; the dump's own unversioned
      ``CREATE EXTENSION`` entry would instead create the newest
      installed version and misalign the catalog columns. When the
      caller pre-stages the extension, the dump's creation entries are
      omitted and the catalog data entries are kept.
    """
    drop_patterns = [
        # TOC entry lines end at the extension name (no trailing text):
        # `2; 3079 16385 EXTENSION - pg_cron`.
        re.compile(r" EXTENSION - pg_cron\s*$"),
        re.compile(r" COMMENT - EXTENSION pg_cron\s*$"),
        re.compile(r" TABLE DATA cron "),
        re.compile(r" SEQUENCE SET cron "),
        # An isolated test source is itself registered as ephemeral. The
        # disposable restore DB receives its own marker immediately after
        # creation, so restoring the source marker would collide. Drop the
        # exact marker table/data/constraint entries; similarly named tables
        # remain ordinary archive content.
        re.compile(
            r" (?:TABLE|TABLE DATA|CONSTRAINT) public "
            r"okg_ephemeral_database(?: |$)"
        ),
    ]
    if timescaledb_prestaged:
        drop_patterns += [
            re.compile(r" EXTENSION - timescaledb\s*$"),
            re.compile(r" COMMENT - EXTENSION timescaledb\s*$"),
        ]
    return "\n".join(
        line
        for line in listing_stdout.splitlines()
        if not any(pattern.search(line) for pattern in drop_patterns)
    ) + "\n"


def _create_verification_database(maintenance_dsn: str, database: str) -> None:
    """Create the disposable backup-restore verification database.

    ``TEMPLATE template0`` is load-bearing. The default template is
    ``template1``, which on an image prepared by ``ops/pg/initdb`` already
    carries TimescaleDB at the CLUSTER's version (issue #1972). A database
    cloned from it would arrive with the extension already present, the
    caller's ``CREATE EXTENSION IF NOT EXISTS timescaledb VERSION
    <source>`` would silently no-op at the wrong version, and
    :func:`_prestage_source_timescaledb` would refuse. ``template0`` is
    never touched by ``initdb`` scripts, so the verification database
    starts bare and receives exactly the source's version.
    """
    from okg.substrate.db.connection import connect as db_connect
    from psycopg import sql

    with db_connect(
        maintenance_dsn, autocommit=True, connect_timeout=5
    ) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
                sql.Identifier(database)
            )
        )


def _prestage_source_timescaledb(disposable_dsn: str, timescaledb_version: str) -> None:
    """Install TimescaleDB at the source's exact version and enter restore mode.

    Raises ``ValueError`` when the installed version does not match the
    source: catalog config-table data only COPYs into the exact catalog
    layout it was dumped from, so a mismatch must refuse rather than
    restore into a misaligned catalog.
    """
    from okg.substrate.db.connection import connect as db_connect
    from psycopg import sql

    with db_connect(
        disposable_dsn, autocommit=True, connect_timeout=5
    ) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "CREATE EXTENSION IF NOT EXISTS timescaledb VERSION {}"
            ).format(sql.Literal(timescaledb_version))
        )
        cur.execute(
            "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
        )
        installed = cur.fetchone()
        if installed is None or str(installed[0]) != timescaledb_version:
            raise ValueError("disposable TimescaleDB version does not match source")
        cur.execute("SELECT timescaledb_pre_restore()")


def _verify_backup_restore(
    ctx: UpgradeContext,
    path: Path,
    *,
    policy: dict[str, Any] | None = None,
    expected_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Restore into a disposable DB and verify OKG schema ownership identity.

    ``policy`` is the caller's already-resolved backup policy. It must be
    passed by any caller that can run without ``_phase_backup`` having
    executed in this process: mutation-time revalidation resumes from the
    state file, so the in-memory ``backup_policy_config`` is absent there
    and the restore would otherwise silently fall back to serial jobs.
    """
    from okg.substrate.db.connection import connect as db_connect
    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    info = conninfo_to_dict(ctx.dsn)
    database = f"okg_upgrade_verify_{uuid4().hex[:16]}"
    # Upgrade the durable obligation with the exact identities before any
    # CREATE DATABASE/restore effect.  A process restart can therefore clean
    # this one disposable database without globbing or touching the source.
    _persist_cleanup_obligation(
        ctx,
        {
            "kind": "backup_artifacts",
            "action": "remove_temporary_archive_and_restore_database",
            "backup_dir": str(getattr(ctx, "backup_dir", path.parent)),
            "archive_path": str(path),
            "database_name": database,
        },
    )
    maintenance = dict(info)
    maintenance["dbname"] = "postgres"
    disposable = dict(info)
    disposable["dbname"] = database
    maintenance_dsn = make_conninfo(**maintenance)
    disposable_dsn = make_conninfo(**disposable)
    created = False
    restore_toc_path: Path | None = None
    verification: dict[str, Any]
    expected_identity: dict[str, Any]
    primary_interrupt: BaseException | None = None
    cleanup_errors: list[dict[str, str]] = []
    cleanup = {
        "restore_toc_removed": True,
        "disposable_database_dropped": True,
    }
    try:
        # The source's timescaledb version pins the disposable DB's
        # pre-created extension: catalog config-table data (e.g.
        # dimension_slice) only COPYs into the exact catalog layout it
        # was dumped from.
        with db_connect(
            ctx.dsn, autocommit=True, connect_timeout=5
        ) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
            )
            row = cur.fetchone()
            timescaledb_version = str(row[0]) if row else None
            expected_identity = (
                {
                    key: expected_snapshot.get(key)
                    for key in (
                        "database_owner",
                        "extension_versions",
                        "timescaledb",
                        "schema_migrations_digest",
                        "schema_catalog_digest",
                        "schema_catalog_component_digests",
                    )
                }
                if expected_snapshot is not None
                else _database_logical_identity(conn)
            )
        # ``timescaledb`` is in the required set: a snapshot persisted before
        # the probe existed must refuse and re-snapshot, not verify without
        # the hypertable evidence.
        if any(
            expected_identity.get(key) is None
            for key in (
                "database_owner",
                "extension_versions",
                "timescaledb",
                "schema_migrations_digest",
                "schema_catalog_digest",
            )
        ):
            raise ValueError("source restore identity snapshot is incomplete")
        _create_verification_database(maintenance_dsn, database)
        created = True
        # Register the disposable DB as ephemeral at creation so a
        # crash-leaked okg_upgrade_verify_* is droppable by the janitor
        # (prefix alone is never drop authority).
        from okg.substrate.db.management import stamp_ephemeral_database
        with db_connect(
            disposable_dsn, autocommit=True, connect_timeout=5
        ) as conn:
            stamp_ephemeral_database(
                conn,
                registered_by="okg.substrate.deployment_upgrade",
                purpose="disposable backup-restore verification database",
            )
        if timescaledb_version is not None:
            _prestage_source_timescaledb(disposable_dsn, timescaledb_version)
        restore_database, restore_env = _libpq_command_target(
            disposable_dsn,
            ctx.env,
        )
        existing_pgoptions = restore_env.get("PGOPTIONS", "").strip()
        serial_restore_options = (
            "-c default_transaction_read_only=off "
            "-c max_parallel_maintenance_workers=0"
        )
        restore_env["PGOPTIONS"] = " ".join(
            part for part in (existing_pgoptions, serial_restore_options) if part
        )
        listing = ctx.command_runner(["pg_restore", "--list", str(path)], ctx.env)
        if not listing.ok:
            raise RuntimeError("backup restore TOC could not be read")
        restore_toc = _restore_toc_without_server_pinned_extensions(
            listing.stdout,
            timescaledb_prestaged=timescaledb_version is not None,
        )
        fd, restore_toc_name = tempfile.mkstemp(prefix="okg-upgrade-", suffix=".toc")
        os.close(fd)
        restore_toc_path = Path(restore_toc_name)
        restore_toc_path.write_text(restore_toc, encoding="utf-8")
        restore_toc_path.chmod(0o600)
        if policy is None:
            policy = getattr(ctx, "data", {}).get("backup_policy_config") or {}
        configured_jobs = policy.get("max_parallel_restore_jobs", 1)
        parallel_jobs = (
            1 if timescaledb_version is not None else int(configured_jobs)
        )
        parallel_reason = (
            "TimescaleDB logical restores remain serial per upstream guidance"
            if timescaledb_version is not None
            else "plain PostgreSQL restore uses the deployment-bounded job count"
        )
        restore_cmd = [
            "pg_restore",
            "--exit-on-error",
            "--use-list",
            str(restore_toc_path),
        ]
        if parallel_jobs > 1:
            restore_cmd.extend(["--jobs", str(parallel_jobs)])
        restore_cmd.extend([
            "--dbname",
            restore_database,
            str(path),
        ])
        restored, restore_progress = _run_long_command(
            ctx,
            restore_cmd,
            restore_env,
            operation="pg_restore",
            progress_interval_seconds=float(
                policy.get("progress_interval_seconds", 30.0)
            ),
        )
        if timescaledb_version is not None:
            # Leave restoring mode even when pg_restore failed so the
            # identity probes below run against a consistent catalog.
            with db_connect(
                disposable_dsn, autocommit=True, connect_timeout=5
            ) as conn, conn.cursor() as cur:
                cur.execute("SELECT timescaledb_post_restore()")
        if not restored.ok:
            verification = _failed_command(
                "backup disposable restore failed",
                restored,
            )
        else:
            verification = {"ok": True}
        with db_connect(
            disposable_dsn,
            connect_timeout=5,
        ) as conn, conn.cursor() as cur:
            restored_identity = _database_logical_identity(conn)
            cur.execute("SELECT pg_database_size(current_database())")
            size_row = cur.fetchone()
            observed_restored_size = (
                int(size_row[0]) if size_row and size_row[0] is not None else None
            )
        server_pinned_extensions = {"pg_cron"}
        expected_restorable_extensions = [
            row for row in expected_identity["extension_versions"]
            if row[0] not in server_pinned_extensions
        ]
        restored_restorable_extensions = [
            row for row in restored_identity["extension_versions"]
            if row[0] not in server_pinned_extensions
        ]
        expected_timescale = expected_identity["timescaledb"]
        restored_timescale = restored_identity["timescaledb"]
        identity_checks = {
            "database_owner_equal": (
                restored_identity["database_owner"]
                == expected_identity["database_owner"]
                and restored_identity["database_owner"].get("deployment")
                == ctx.options.deployment
            ),
            "extension_versions_equal": (
                restored_restorable_extensions
                == expected_restorable_extensions
            ),
            # extension_versions_equal cannot stand in for this: the
            # disposable database prestages timescaledb at the pinned
            # version, so the extension list matches even when the
            # hypertable catalog restored hollow. Stable identity only —
            # chunk counts are recorded below, never gated on.
            "timescale_state_equal": (
                restored_timescale["extension_version"]
                == expected_timescale["extension_version"]
                and restored_timescale["hypertables"]
                == expected_timescale["hypertables"]
                and restored_timescale["continuous_aggregates"]
                == expected_timescale["continuous_aggregates"]
            ),
            "schema_migrations_equal": (
                restored_identity["schema_migrations_digest"]
                == expected_identity["schema_migrations_digest"]
            ),
            "schema_catalog_equal": (
                restored_identity["schema_catalog_digest"]
                == expected_identity["schema_catalog_digest"]
            ),
        }
        expected_components = expected_identity.get(
            "schema_catalog_component_digests"
        )
        restored_components = restored_identity.get(
            "schema_catalog_component_digests"
        )
        component_checks = (
            {
                name: restored_components.get(name) == expected_digest
                for name, expected_digest in expected_components.items()
            }
            if (
                isinstance(expected_components, Mapping)
                and isinstance(restored_components, Mapping)
            )
            else {}
        )
        ownership_ok = identity_checks["database_owner_equal"]
        schema_ok = (
            identity_checks["schema_migrations_equal"]
            and identity_checks["schema_catalog_equal"]
            and identity_checks["extension_versions_equal"]
            and identity_checks["timescale_state_equal"]
        )
        if not verification.get("ok"):
            pass
        elif not schema_ok or not ownership_ok:
            verification = {
                "ok": False,
                "reason": "backup disposable restore identity check failed",
                "schema_ok": schema_ok,
                "ownership_ok": ownership_ok,
                "identity_checks": identity_checks,
                "schema_catalog_component_checks": component_checks,
                "server_pinned_extensions_excluded": sorted(
                    server_pinned_extensions
                ),
            }
        else:
            verification = {
                "ok": True,
                "restore_verified": True,
                "schema_ok": True,
                "ownership_ok": True,
                "identity_checks": identity_checks,
                "restored_identity": restored_identity,
                # Stamped only on the success branch: reaching here means
                # both procedure calls executed (pre_restore before the
                # restore, post_restore after) and the restored-side probe
                # matched, so the claims below are earned by this restore,
                # never copied from the pre-dump snapshot.
                "timescale_restore": {
                    "status": (
                        "proven"
                        if expected_timescale["extension_version"] is not None
                        else "absent"
                    ),
                    "extension_version": (
                        expected_timescale["extension_version"]
                    ),
                    "pre_post_restore_used": timescaledb_version is not None,
                    "hypertables_protected": expected_timescale["hypertables"],
                    "continuous_aggregates": (
                        expected_timescale["continuous_aggregates"]
                    ),
                    "source_hypertable_chunk_counts": (
                        expected_timescale["hypertable_chunk_counts"]
                    ),
                    "restored_hypertable_chunk_counts": (
                        restored_timescale["hypertable_chunk_counts"]
                    ),
                },
                "server_pinned_extensions_excluded": sorted(
                    server_pinned_extensions
                ),
                "observed_restored_size_bytes": observed_restored_size,
                "restore_parallel_jobs": parallel_jobs,
                "restore_parallel_reason": parallel_reason,
                "restore_duration_seconds": restore_progress["duration_seconds"],
                "progress": {"pg_restore": restore_progress},
            }
    except BaseException as exc:  # cleanup must run for interrupts too.
        if isinstance(exc, Exception):
            verification = {
                "ok": False,
                "reason": "backup disposable restore verification failed",
                "error": {"type": type(exc).__name__, "message": str(exc)},
                "fix_hint": (
                    "repair backup restore verification and resume before migration"
                ),
            }
        else:
            primary_interrupt = exc
            verification = {
                "ok": False,
                "reason": "backup disposable restore interrupted",
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
    finally:
        if restore_toc_path is not None:
            try:
                restore_toc_path.unlink(missing_ok=True)
            except OSError as exc:
                cleanup["restore_toc_removed"] = False
                cleanup_errors.append({
                    "operation": "remove_restore_toc",
                    "type": type(exc).__name__,
                    "message": str(exc),
                })
        if created:
            try:
                with db_connect(
                    maintenance_dsn, autocommit=True, connect_timeout=5
                ) as conn, conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname = %s AND pid <> pg_backend_pid()",
                        (database,),
                    )
                    cur.execute(
                        sql.SQL("DROP DATABASE {}").format(sql.Identifier(database))
                    )
            except Exception as exc:  # noqa: BLE001 - cleanup is proof evidence.
                cleanup["disposable_database_dropped"] = False
                cleanup_errors.append({
                    "operation": "drop_disposable_database",
                    "type": type(exc).__name__,
                    "message": str(exc),
                })
    if primary_interrupt is not None:
        if cleanup_errors:
            primary_interrupt.add_note(
                "backup restore cleanup errors: "
                + json.dumps(cleanup_errors, sort_keys=True)
            )
        raise primary_interrupt
    if cleanup_errors:
        return {
            "ok": False,
            "reason": "backup verification cleanup failed",
            "cleanup": cleanup,
            "cleanup_errors": cleanup_errors,
            "primary_failure": verification if not verification.get("ok") else None,
            "database_prefix": "okg_upgrade_verify_",
            "fix_hint": (
                "remove the guarded disposable artifacts and resume before migration"
            ),
        }
    verification["cleanup"] = cleanup
    return verification


def _finalize_backup_retention(
    ctx: UpgradeContext,
    phase_results: list[dict[str, Any]],
) -> dict[str, Any]:
    """Keep-last-N pruning of superseded verified backup archives.

    Invoked ONLY at the tail of a fully successful (status ``applied``)
    upgrade. Retention semantics (bound-host-storage-pressure
    req.host.workspace-hygiene):

    - Scope: regular files or directories in this deployment's backup dir
      whose name matches ``<deployment>-<UTC timestamp>.(dump|dumpdir)``.
      Nothing else is ever touched.
    - The archive this run wrote (and verified via disposable restore) is
      NEVER deleted, regardless of N.
    - The newest ``backup_keep`` archives are kept; older ones are
      superseded by the just-verified dump and deleted, each reported with
      its size.
    - A run that wrote no archive (``--skip-backup``) prunes nothing: the
      archives on disk are not superseded by this run.
    """
    keep = ctx.options.backup_keep
    report: dict[str, Any] = {
        "phase": "backup_retention",
        "ok": True,
        "keep": keep,
        "backup_dir": str(ctx.backup_dir),
        "deleted": [],
        "deleted_bytes": 0,
    }
    backup_row = next(
        (
            row
            for row in phase_results
            if row.get("phase") == "backup" and row.get("ok") is True
        ),
        None,
    )
    current_raw = str((backup_row or {}).get("backup_path") or "")
    if not current_raw:
        report.update({
            "skipped": (
                "no verified backup archive was written by this run; "
                "existing archives are not superseded"
            ),
            "completed_at": _now(),
        })
        return report
    current = Path(current_raw)
    if current.is_symlink() or not (current.is_file() or current.is_dir()):
        # Fail loud, delete nothing: if the just-verified archive vanished,
        # every older archive may be the only good backup.
        report.update({
            "ok": False,
            "reason": "verified backup archive is missing at retention time",
            "backup_path": str(current),
            "completed_at": _now(),
        })
        return report
    pattern = re.compile(
        rf"^{re.escape(ctx.options.deployment)}-\d{{8}}T\d{{6}}Z"
        rf"(?:-\d{{2}})?\.(?:dump|dumpdir)$"
    )
    try:
        # Fixed-width UTC timestamps make the name ordering the age ordering.
        archives = sorted(
            (
                path
                for path in ctx.backup_dir.iterdir()
                if not path.is_symlink()
                and (path.is_file() or path.is_dir())
                and pattern.match(path.name)
            ),
            key=lambda path: path.name,
            reverse=True,
        )
    except OSError as exc:
        report.update({
            "ok": False,
            "reason": "backup directory unreadable at retention time",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "completed_at": _now(),
        })
        return report
    current_resolved = current.resolve()
    newest = set(archives[:keep])
    kept: list[dict[str, Any]] = []
    deleted: list[dict[str, Any]] = []
    failed_deletes: list[dict[str, Any]] = []
    deleted_bytes = 0
    for path in archives:
        try:
            if path.is_dir():
                size = sum(
                    child.stat().st_size
                    for child in path.rglob("*")
                    if child.is_file() and not child.is_symlink()
                )
            else:
                size = path.stat().st_size
        except OSError:
            size = None
        if path in newest or path.resolve() == current_resolved:
            kept.append({"path": str(path), "size_bytes": size})
            continue
        try:
            if path.resolve().parent != ctx.backup_dir.resolve():
                raise OSError("archive resolved outside the backup directory")
            if path.is_dir():
                if any(child.is_symlink() for child in path.rglob("*")):
                    raise OSError("archive directory contains a symlink")
                shutil.rmtree(path)
            else:
                path.unlink()
        except OSError as exc:
            failed_deletes.append({
                "path": str(path),
                "size_bytes": size,
                "error": {"type": type(exc).__name__, "message": str(exc)},
            })
            continue
        deleted.append({"path": str(path), "size_bytes": size})
        deleted_bytes += size or 0
    report.update({
        "kept": kept,
        "deleted": deleted,
        "deleted_bytes": deleted_bytes,
        "completed_at": _now(),
    })
    if failed_deletes:
        report.update({
            "ok": False,
            "reason": "superseded backup archives could not be deleted",
            "failed_deletes": failed_deletes,
        })
    return report


DOGFOOD_STATUS_SCHEMA = "okg.dogfood.runtime_status.v1"


def _service_state_from_dogfood_status(payload: Any) -> str:
    """Map an ``okg dogfood status --json`` payload to running/stopped/unknown.

    The status contract (``okg.dogfood.runtime_status.v1``) reports process
    authority under ``runtime_processes``: ``root_count`` counts live worker
    process roots, and ``status`` says whether that count could be measured
    at all. Reading instead for a ``worker_state``/``status`` string, which
    that schema never emits, made every production stop refuse with "could
    not observe service state before stop" (2026-09-03).

    A crash-looping or split worker still owns process roots and reads as
    running: stop has work to do. Only an unmeasurable audit is unknown.
    """
    if not isinstance(payload, Mapping):
        return "unknown"
    if payload.get("schema") != DOGFOOD_STATUS_SCHEMA:
        return "unknown"
    processes = payload.get("runtime_processes")
    if not isinstance(processes, Mapping):
        return "unknown"
    if str(processes.get("status") or "") == "unavailable":
        return "unknown"
    root_count = processes.get("root_count")
    if (
        not isinstance(root_count, int)
        or isinstance(root_count, bool)
        or root_count < 0
    ):
        return "unknown"
    return "running" if root_count > 0 else "stopped"


def _observe_service_state(ctx: UpgradeContext) -> str:
    """Observe the worker state before issuing a stop command."""
    data = getattr(ctx, "data", None)
    if isinstance(data, Mapping) and isinstance(data.get("prior_service_state"), str):
        return str(data["prior_service_state"])
    # Direct phase tests and adapter callers without a durable receipt retain
    # the historical running default; the production orchestrator always has
    # a state file and therefore takes the explicit observation path below.
    if not isinstance(getattr(ctx, "state_file", None), Path):
        return "running"
    # The preflight adapter probe may populate this field.  Avoid issuing a
    # second mutating-surface command here: stop itself must be the next
    # service command after the fence/drain boundary.
    observed = data.get("service_state_observed") if isinstance(data, Mapping) else None
    if isinstance(observed, str) and observed in {"running", "stopped"}:
        return observed
    if isinstance(data, Mapping) and data.get("production_orchestrator"):
        if ctx.command_runner is _default_command_runner:
            try:
                result = _run_okg(
                    ctx,
                    [
                        "dogfood",
                        "status",
                        "--deployment",
                        ctx.options.deployment,
                        "--json",
                    ],
                )
                # `dogfood status` exits non-zero whenever the deployment
                # has blockers - which is exactly the state a stop is called
                # for - while still printing its typed payload. The schema
                # check inside the reader is what binds this observation,
                # not the exit code.
                state = _service_state_from_dogfood_status(result.json_payload())
                if state != "unknown":
                    return state
            except Exception:  # noqa: BLE001 - unknown state blocks safely.
                pass
            return "unknown"
        # Controlled test/service adapters expose their state through the
        # command seam; keep compatibility for those harnesses.
        return "running"
    return "running"


def _reopen_runtime_fence(
    ctx: UpgradeContext,
    *,
    owner: str,
    key: str,
) -> dict[str, Any]:
    """Release a fence this phase acquired, so a failure cannot wedge the worker.

    The worker's dynamic scheduler waits on the fence; a stop phase that
    fails after acquisition and returns without releasing leaves it waiting
    for a release nobody will issue (2026-09-03: fence
    deployment-up:e070a618... left ``fenced`` after the phase refused).
    "restore" puts the pre-fence schedule snapshot back, because no
    candidate schedule was promoted.
    """
    from okg.substrate.runtime.promotion_fence import release_promotion_fence

    try:
        return {key: release_promotion_fence(
            deployment=ctx.options.deployment,
            dsn=ctx.dsn,
            promotion_id=owner,
            schedule_disposition="restore",
        )}
    except Exception as exc:  # noqa: BLE001 - report a retained fence.
        return {
            f"{key}_error": {
                "type": type(exc).__name__,
                "message": str(exc),
                "fence_retained": True,
            },
        }


def _stop_failure_quiescence(
    ctx: UpgradeContext,
    *,
    quiescence: Mapping[str, Any] | None,
    fence_owner: str,
    key: str,
) -> dict[str, Any]:
    """The ``runtime_quiescence`` block for a failing stop, fence released.

    Keyed on fence ownership, not on the report: whenever this phase took a
    fence, a failing exit releases it.
    """
    if not fence_owner:
        return {"runtime_quiescence": dict(quiescence)} if quiescence else {}
    return {
        "runtime_quiescence": {
            **(quiescence or {}),
            **_reopen_runtime_fence(ctx, owner=fence_owner, key=key),
        },
    }


def _phase_stop_services(ctx: UpgradeContext) -> dict[str, Any]:
    """Stop the deployment services under a runtime fence.

    Wrapper: every exit after the fence is acquired must release it, raised
    errors included. `_run_phase` turns an escaping exception into a durable
    failure row, which would leave the fence held and the worker scheduler
    waiting on a release nobody issues.
    """
    if isinstance(getattr(ctx, "data", None), dict):
        ctx.data.pop("stop_services_fence_owner", None)
        ctx.data.pop("stop_services_quiescence", None)
    try:
        return _stop_services_under_fence(ctx)
    except Exception as exc:  # noqa: BLE001 - reported, never swallowed.
        data = getattr(ctx, "data", None)
        owner = str(
            data.get("stop_services_fence_owner") or ""
            if isinstance(data, Mapping)
            else ""
        )
        if not owner:
            raise
        return {
            "ok": False,
            "reason": "service stop raised after the runtime fence was acquired",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            **_stop_failure_quiescence(
                ctx,
                quiescence=(
                    data.get("stop_services_quiescence")
                    if isinstance(data, Mapping)
                    else None
                ),
                fence_owner=owner,
                key=(
                    "post_stop_reopen"
                    if isinstance(data, Mapping) and data.get("services_stopped")
                    else "pre_mutation_reopen"
                ),
            ),
        }


def _stop_services_under_fence(ctx: UpgradeContext) -> dict[str, Any]:
    cmd = [
        "dogfood",
        "down",
        "--deployment",
        ctx.options.deployment,
        "--apply",
    ]
    if not ctx.options.apply_services:
        return {
            "ok": True,
            "planned": _display_okg_command(cmd),
            "skipped": "service mutation skipped (--plan / --no-apply-services)",
        }
    quiescence: dict[str, Any] | None = None
    fence_owner = ""
    if _runtime_quiescence_required(ctx):
        from okg.substrate.runtime.admin import client_drain
        from okg.substrate.runtime.promotion_fence import (
            acquire_promotion_fence,
            release_promotion_fence,
        )

        owner = str(ctx.data.get("runtime_fence_owner") or "")
        if not owner:
            return {
                "ok": False,
                "reason": "runtime quiescence owner was not persisted before stop",
            }
        fence = acquire_promotion_fence(
            deployment=ctx.options.deployment,
            dsn=ctx.dsn,
            promotion_id=owner,
            metadata=_runtime_fence_metadata(ctx),
        )
        fence_owner = owner
        if isinstance(getattr(ctx, "data", None), dict):
            ctx.data["stop_services_fence_owner"] = owner
        snapshot_digest = str(fence.get("schedule_snapshot_digest") or "")
        ctx.data["runtime_fence_snapshot_digest"] = snapshot_digest
        _persist_runtime_fence_state(
            ctx,
            owner=owner,
            schedule_snapshot_digest=snapshot_digest,
        )
        try:
            drain = client_drain(
                deployment=ctx.options.deployment,
                dsn=ctx.dsn,
                max_wait_seconds=_positive_env_float(
                    ctx, "OKG_DEPLOYMENT_DRAIN_TIMEOUT_SECONDS", 600.0
                ),
                poll_interval_seconds=_positive_env_float(
                    ctx, "OKG_DEPLOYMENT_DRAIN_POLL_SECONDS", 2.0
                ),
                stable_zero_seconds=_positive_env_float(
                    ctx, "OKG_DEPLOYMENT_DRAIN_STABLE_ZERO_SECONDS", 31.0
                ),
            )
        except Exception as drain_exc:  # noqa: BLE001
            try:
                reopen = release_promotion_fence(
                    deployment=ctx.options.deployment,
                    dsn=ctx.dsn,
                    promotion_id=owner,
                    schedule_disposition="restore",
                )
                reopen_evidence: dict[str, Any] = {
                    "pre_mutation_reopen": reopen,
                }
            except Exception as reopen_exc:  # noqa: BLE001
                reopen_evidence = {
                    "pre_mutation_reopen_error": {
                        "type": type(reopen_exc).__name__,
                        "message": str(reopen_exc),
                        "fence_retained": True,
                    },
                }
            return {
                "ok": False,
                "reason": "runtime queue drain raised before service stop",
                "runtime_quiescence": {
                    "schema": "okg.deployment_runtime_quiescence.v1",
                    "owner": owner,
                    "fence": fence,
                    "drain_error": {
                        "type": type(drain_exc).__name__,
                        "message": str(drain_exc),
                    },
                    **reopen_evidence,
                },
            }
        quiescence = {
            "schema": "okg.deployment_runtime_quiescence.v1",
            "owner": owner,
            "fence": fence,
            "drain": {
                "deployment": drain.deployment,
                "drained": drain.drained,
                "in_flight_at_timeout": drain.in_flight_at_timeout,
                "elapsed_seconds": drain.elapsed_seconds,
                "pending_workflows": drain.pending_workflows,
            },
        }
        if not drain.drained:
            reopen = release_promotion_fence(
                deployment=ctx.options.deployment,
                dsn=ctx.dsn,
                promotion_id=owner,
                schedule_disposition="restore",
            )
            return {
                "ok": False,
                "reason": "runtime queues did not drain before service stop",
                "runtime_quiescence": {
                    **quiescence,
                    "pre_mutation_reopen": reopen,
                },
                "fix_hint": (
                    "inspect the reported workflow identities; resume after "
                    "the active work is terminal"
                ),
            }
    else:
        quiescence = _runtime_not_bootstrapped_report(ctx)
    if isinstance(getattr(ctx, "data", None), dict):
        ctx.data["stop_services_quiescence"] = quiescence
    prior_state = _observe_service_state(ctx)
    if prior_state == "unknown":
        return {
            "ok": False,
            "reason": "could not observe service state before stop",
            "fix_hint": (
                "run `okg dogfood status --deployment "
                f"{ctx.options.deployment} --json` and confirm it reports "
                "runtime_processes.root_count, then resume"
            ),
            **_stop_failure_quiescence(
                ctx,
                quiescence=quiescence,
                fence_owner=fence_owner,
                key="pre_mutation_reopen",
            ),
        }
    try:
        _persist_cleanup_obligation(
            ctx,
            {
                "kind": "service",
                "action": "restart_services",
                "deployment": ctx.options.deployment,
                "prior_state": prior_state,
            },
        )
    except (OSError, SubstrateError) as exc:
        return {
            "ok": False,
            "reason": "could not persist service compensation obligation",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            **_stop_failure_quiescence(
                ctx,
                quiescence=quiescence,
                fence_owner=fence_owner,
                key="pre_mutation_reopen",
            ),
        }
    result = _run_okg(ctx, cmd)
    if not result.ok:
        return {
            **_failed_command("stop services failed", result),
            **_stop_failure_quiescence(
                ctx,
                quiescence=quiescence,
                fence_owner=fence_owner,
                key="post_stop_reopen",
            ),
        }
    if isinstance(getattr(ctx, "data", None), dict):
        ctx.data["services_stopped"] = True
    payload = {
        "ok": True,
        "result": result.json_payload(),
        **(
            {"runtime_quiescence": quiescence}
            if quiescence is not None
            else {}
        ),
    }
    backup = getattr(ctx, "data", {}).get("backup_result")
    if (
        isinstance(backup, dict)
        and backup.get("verification_mode") == "no_physical_rollback_required"
    ):
        revalidation = _revalidate_backup_before_mutation(ctx)
        if revalidation.get("ok") is not True:
            return {
                **revalidation,
                "service_stop": result.json_payload(),
                "reason": (
                    revalidation.get("reason")
                    or "physical rollback admission drifted after service stop"
                ),
                **_stop_failure_quiescence(
                    ctx,
                    quiescence=quiescence,
                    fence_owner=fence_owner,
                    key="post_stop_reopen",
                ),
            }
        payload["backup_revalidation"] = revalidation
    return payload


def _phase_sync_code(ctx: UpgradeContext) -> dict[str, Any]:
    from okg.substrate.deployment_release import (
        _git_revision,
        current_release_artifact,
    )

    manifest = _resolve_upgrade_deployment(ctx)
    release = manifest.release or {}
    channel = release.get("channel")
    pin = release.get("pin") if isinstance(release.get("pin"), dict) else {}
    if channel == "dev" and not pin:
        return {
            "ok": True,
            "skipped": "dev channel tracks the current checkout",
            "code_revision": _git_revision(Path(manifest.deployment_dir)),
        }
    target_revision = pin.get("code_revision") if isinstance(pin, dict) else None
    authority = _git_target_revision(ctx, manifest)
    target_revision = authority.code_revision if target_revision else target_revision
    current = current_release_artifact(manifest)
    if channel == "dev":
        return {
            "ok": True,
            "skipped": "dev channel tracks the current checkout",
            "code_revision": current.code_revision,
        }
    # An opt-out (--plan / --no-sync-code) reports a skip BEFORE the pin
    # requirement: not syncing code is a (partial-flagged) skip, not a hard
    # failure on a missing pin.
    if not ctx.options.sync_code:
        return {
            "ok": True,
            "planned": (
                f"git -C {authority.code_repo_root} fetch --all --prune && "
                f"git -C {authority.code_repo_root} checkout {target_revision}"
                if target_revision
                else (
                    f"git -C {authority.code_repo_root} fetch --all --prune && "
                    "git checkout <release.pin.code_revision>"
                )
            ),
            "skipped": "code sync skipped (--plan / --no-sync-code)",
            "expected": target_revision,
            "observed": current.code_revision,
        }
    if not target_revision:
        return {
            "ok": False,
            "reason": "prod release has no target code_revision",
            "fix_hint": f"run `okg up {ctx.options.deployment}` after pinning release.pin.code_revision",
        }
    if current.code_revision == target_revision:
        return {"ok": True, "code_revision": current.code_revision}
    for cmd in (
        ["git", "-C", str(authority.code_repo_root), "fetch", "--all", "--prune"],
        ["git", "-C", str(authority.code_repo_root), "checkout", target_revision],
    ):
        result = ctx.command_runner(cmd, ctx.env)
        if not result.ok:
            return _failed_command("code sync failed", result, command=cmd)
    return {"ok": True, "code_revision": target_revision}


@dataclass(frozen=True)
class DependencySyncTarget:
    """The exact project and environment a dependency sync converges."""

    command: list[str]
    extras: tuple[str, ...]
    project_root: Path
    environment: Path


@lru_cache(maxsize=64)
def _dependency_sync_repo_root(
    deployment_dir: str,
    explicit_code_repository: str | None,
    requested_code_revision: str | None,
) -> Path:
    """The code repository root, memoised for the life of the process.

    ``_resolve_repo_authorities`` costs four Git subprocesses (~60ms here) and
    answers with repository ROOTS, not revisions. A root cannot change for a
    given deployment directory while the process runs -- ``sync_code`` checks
    a revision out INSIDE that root, it never moves it -- so memoising the
    lookup changes no answer. Exceptions are not memoised, so a refusal is
    re-derived (and re-reported) every time it is asked for.
    """
    project_root, _deployment_repo_root = _resolve_repo_authorities(
        Path(deployment_dir),
        explicit_code_repository=explicit_code_repository,
        requested_code_revision=requested_code_revision,
    )
    return project_root


def _dependency_sync_project(ctx: UpgradeContext) -> tuple[Path, Path]:
    """Resolve (project root, environment) the dependency sync converges.

    ``uv sync`` with no named project resolves one by walking up from the
    PROCESS working directory, and then converges that project's environment
    by REMOVING every distribution its lock does not name.  An operator who
    runs ``okg up`` from anywhere other than the deployment therefore
    converged, and pruned, a project that has nothing to do with it.

    The project named here is the repository ``sync_code`` checks out.  That
    is not a paraphrase: this calls ``_resolve_repo_authorities`` with the same
    arguments ``_git_target_revision`` passes for the code-sync phase, so the
    lock applied always comes out of the tree the code came out of.  Resolving
    it any other way -- for instance from the planner module's own path --
    disagrees with ``sync_code`` in the layouts that helper has branches for
    (two linked worktrees of one repository, an installed planner whose
    deployment repository is the framework), which is precisely the
    code-from-one-tree, dependencies-from-another failure this exists to stop.

    This does NOT refuse a sync that converges the interpreter okg is
    executing from.  On a deployment host that is the intended behaviour, and
    from inside the process it is indistinguishable from a developer checkout.
    What is refused is a target that cannot be named -- and only when a sync
    would actually run; see ``_phase_dependency_sync``, which keeps the
    ``--no-sync-deps`` escape hatch open for exactly the hosts whose target has
    no name.
    """
    cached = ctx.data.get("dependency_sync_project")
    if isinstance(cached, tuple):
        return cached
    manifest = _resolve_upgrade_deployment(ctx)
    deployment_dir = getattr(manifest, "deployment_dir", None)
    release = getattr(manifest, "release", None) or {}
    pin = release.get("pin") if isinstance(release, Mapping) else None
    pin = pin if isinstance(pin, Mapping) else {}
    explicit_repository = pin.get("code_repository")
    requested_revision = pin.get("code_revision")
    refusal = {
        "deployment": ctx.options.deployment,
        "deployment_dir": str(deployment_dir) if deployment_dir else None,
        "code_repository": (
            str(explicit_repository) if explicit_repository is not None else None
        ),
    }
    fix_hint = (
        "rerun with --no-sync-deps (it plans and skips this phase rather than "
        "failing) and converge the deployment's environment by hand, or pin "
        "release.pin.code_repository at the checkout the deployment runs"
    )
    if not deployment_dir:
        raise SubstrateError(
            "dependency sync target repository unresolved",
            data={
                **refusal,
                "reason": "the resolved deployment declares no directory",
            },
            fix_hint=fix_hint,
        )
    try:
        project_root = _dependency_sync_repo_root(
            str(Path(deployment_dir).resolve()),
            (
                str(explicit_repository)
                if explicit_repository is not None
                else None
            ),
            (
                str(requested_revision)
                if isinstance(requested_revision, str)
                else None
            ),
        )
    except RevisionAuthorityError as exc:
        raise SubstrateError(
            "dependency sync target repository unresolved",
            data={**refusal, "authority": dict(exc.data)},
            fix_hint=fix_hint,
        ) from exc
    if not (project_root / "pyproject.toml").is_file():
        raise SubstrateError(
            "dependency sync target is not a uv project",
            data={**refusal, "project_root": str(project_root)},
            fix_hint=fix_hint,
        )
    # uv resolves the project environment to `<project>/.venv` unless
    # UV_PROJECT_ENVIRONMENT names one, and that variable has no command-line
    # equivalent. Read it from the same env the command runs with, so the
    # reported target is the path uv will actually converge. An absolute value
    # replaces the default and a relative one is joined, exactly as uv does.
    declared_environment = (ctx.env.get("UV_PROJECT_ENVIRONMENT") or "").strip()
    environment = project_root / (declared_environment or ".venv")
    resolved = (project_root, environment)
    ctx.data["dependency_sync_project"] = resolved
    return resolved


def _dependency_sync_target(
    ctx: UpgradeContext,
    *,
    check: bool = False,
) -> DependencySyncTarget:
    deployment = _resolve_upgrade_deployment(ctx)
    runtime = getattr(deployment, "runtime", None)
    extras = tuple(getattr(runtime, "python_extras", ()) or ())
    project_root, environment = _dependency_sync_project(ctx)
    cmd = ["uv", "sync", "--project", str(project_root)]
    if check:
        cmd.append("--check")
    cmd.append("--frozen")
    for extra in extras:
        cmd.extend(["--extra", extra])
    return DependencySyncTarget(
        command=cmd,
        extras=extras,
        project_root=project_root,
        environment=environment,
    )


def _phase_dependency_sync(ctx: UpgradeContext) -> dict[str, Any]:
    if not ctx.options.sync_deps:
        # The opt-out is resolved FIRST and mutates nothing, so an unnameable
        # target is DISCLOSED here rather than failed. `--no-sync-deps` is the
        # documented escape hatch for exactly the hosts whose target has no
        # name -- an installed, non-checkout planner, which is what sealed
        # releases replace (deployments/okg-ci/RUNBOOK.md). Failing
        # this branch would close the hatch its own fix hint points at, and
        # would break the read-only `--plan` that worked before.
        try:
            target = _dependency_sync_target(ctx)
        except SubstrateError as exc:
            return {
                "ok": True,
                "skipped": "dependency sync skipped (--plan / --no-sync-deps)",
                "target_unresolved": exc.to_json()["error"],
            }
        return {
            "ok": True,
            "planned": _display_command(target.command),
            "project_root": str(target.project_root),
            "environment": str(target.environment),
            "skipped": "dependency sync skipped (--plan / --no-sync-deps)",
        }
    target = _dependency_sync_target(ctx)
    cmd = target.command
    resolved = {
        "project_root": str(target.project_root),
        "environment": str(target.environment),
    }
    try:
        result = ctx.command_runner(cmd, ctx.env)
    except FileNotFoundError as exc:
        return {
            "ok": False,
            "reason": "dependency sync tool missing",
            "command": _display_command(cmd),
            **resolved,
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "fix_hint": (
                "install the deployment's pinned uv bridge and resume; sealed "
                "releases remove this mutable host prerequisite"
            ),
        }
    if not result.ok:
        return {
            **_failed_command("dependency sync failed", result, command=cmd),
            **resolved,
        }
    return {
        "ok": True,
        "command": _display_command(cmd),
        **resolved,
        "python_extras": list(target.extras),
    }


def _ensure_backup_revalidated_for_mutation(
    ctx: UpgradeContext,
) -> dict[str, Any]:
    backup = ctx.data.get("backup_result")
    if (
        isinstance(backup, dict)
        and backup.get("verification_mode") == "no_physical_rollback_required"
    ):
        # The admitted fast path has no current physical rollback point.  Its
        # exact target/effect/recovery proof is therefore re-read at every DB
        # effect boundary; a once-per-run cache would turn later drift into an
        # unproved mutation.
        return _revalidate_backup_before_mutation(ctx)
    authority = _revalidate_effect_authority(ctx, backup)
    if authority.get("ok") is not True:
        return authority
    cached = ctx.data.get("backup_mutation_revalidation")
    if isinstance(cached, dict) and cached.get("ok") is True:
        return {**cached, "effect_authority": authority}
    result = _revalidate_backup_before_mutation(ctx)
    if result.get("ok") is True:
        ctx.data["backup_mutation_revalidation"] = result
        return {**result, "effect_authority": authority}
    return result


# Authority claims only: whether the target plan can be trusted, read, and
# rebuilt.  ``no_external_effects`` is deliberately NOT here.  External reach
# means a run's provider calls cannot be undone, which forces a current full
# physical backup (``_FORCED_CURRENT_BACKUP_EFFECTS`` in
# ``deployment_backup_policy``) — it is not evidence that the plan is
# untrustworthy, and refusing on it left LLM-bearing deployments with no
# supported path to a known-good state.
_REFUSAL_EFFECT_AUTHORITY_CHECKS = (
    "target_revision_pinned",
    "planner_trusted",
    "plan_readable",
    "smoke_source_versioned",
    "attribute_read_model_rebuildable",
)


def _revalidate_effect_authority(
    ctx: UpgradeContext,
    backup: Any,
) -> dict[str, Any]:
    """Re-read fail-closed target authority at every database effect boundary."""
    if not isinstance(backup, Mapping):
        return {"ok": False, "reason": "backup effect authority is unavailable"}
    effects, errors, report = _collect_effect_inventory(ctx)
    checks = effects.to_dict() if effects is not None else {}
    failed = [
        name for name in _REFUSAL_EFFECT_AUTHORITY_CHECKS
        if checks.get(name) is not True
    ]
    if report.get("execution_checkout_exact") is not True:
        failed.append("execution_checkout_exact")
    prior_report = backup.get("effect_inventory")
    prior_revision = (
        prior_report.get("target_revision")
        if isinstance(prior_report, Mapping)
        else None
    )
    prior_digest = (
        prior_report.get("effect_digest")
        if isinstance(prior_report, Mapping)
        else None
    )
    if not isinstance(prior_digest, str) or not prior_digest:
        candidate = backup.get("effect_digest")
        prior_digest = candidate if isinstance(candidate, str) else None
    current_revision = report.get("target_revision")
    current_digest = report.get("effect_digest")
    revision_stable = (
        isinstance(prior_revision, str)
        and prior_revision
        and current_revision == prior_revision
    )
    digest_stable = (
        isinstance(prior_digest, str)
        and prior_digest
        and current_digest == prior_digest
    )
    if (
        effects is None
        or errors
        or failed
        or not revision_stable
        or not digest_stable
    ):
        return {
            "ok": False,
            "reason": "deployment effect authority drifted before mutation",
            "effect_errors": list(errors),
            "failed_checks": failed,
            "target_revision_stable": revision_stable,
            "expected_target_revision": prior_revision,
            "effect_digest_stable": digest_stable,
            "expected_effect_digest": prior_digest,
            "observed_effect_digest": current_digest,
            "effect_inventory": report,
            "fix_hint": (
                "resolve the target/source authority drift, then resume; use "
                "--reset-state only when the retained backup state is invalid"
            ),
        }

    # Archive content is hashed once per run and the cache is guarded by a
    # complete metadata fingerprint. An operator or cleanup process changing it
    # mid-run invalidates that fingerprint and must stop the next effect even
    # though the expensive restore proof already succeeded.
    path_raw = backup.get("backup_path")
    if isinstance(path_raw, str) and path_raw:
        archive = _validate_backup_archive_for_run(ctx, Path(path_raw))
        if (
            archive.get("ok") is not True
            or archive.get("backup_digest") != backup.get("backup_digest")
        ):
            return {
                "ok": False,
                "reason": "current backup archive changed before mutation",
                "archive": archive,
                "effect_inventory": report,
            }
    return {
        "ok": True,
        "target_revision_stable": True,
        "effect_digest_stable": True,
        "effect_inventory": report,
        "smoke_source_digest": (report.get("smoke") or {}).get(
            "selected_source_digest"
        ),
    }


def _phase_migrate(ctx: UpgradeContext) -> dict[str, Any]:
    if ctx.options.skip_migrate:
        return {"ok": True, "skipped": "operator requested --skip-migrate"}
    migrate_cmd = ["migrate", "--deployment", ctx.options.deployment, "--apply"]
    if ctx.options.plan_only:
        return {
            "ok": True,
            "planned": _display_okg_command(migrate_cmd),
            "skipped": "schema migration skipped (--plan)",
        }
    backup_revalidation = _ensure_backup_revalidated_for_mutation(ctx)
    if backup_revalidation.get("ok") is not True:
        return backup_revalidation
    if (
        backup_revalidation.get("verification_mode")
        == "no_physical_rollback_required"
    ):
        return {
            "ok": True,
            "backup_revalidation": backup_revalidation,
            "schema_unchanged": True,
            "extensions_unchanged": True,
            "skipped": "closed effect inventory proves no migration work",
        }
    try:
        extensions = _ensure_upgrade_extensions(ctx)
    except Exception as exc:  # noqa: BLE001 - surface extension bootstrap errors.
        return {
            "ok": False,
            "reason": "extension bootstrap failed",
            "error": str(exc),
        }
    before = ctx.data.get("migrations_before") or {}
    destructive_pending = before.get("destructive_pending") or []
    destructive_results: list[dict[str, Any]] = []
    if destructive_pending and ctx.options.confirm_destructive_migrations:
        for pending in destructive_pending:
            migration_id = str(pending.get("migration_id") or "")
            if migration_id != "0002_remove_job_runtime_subsystem":
                return {
                    "ok": False,
                    "reason": "destructive migration has no deployment-up adapter",
                    "migration_id": migration_id,
                    "fix_hint": (
                        "add a reviewed, migration-specific deployment adapter; "
                        "never execute unknown destructive SQL generically"
                    ),
                }
            cleanup = _run_okg(
                ctx,
                [
                    "db",
                    "remove-job-runtime-subsystem",
                    "--deployment",
                    ctx.options.deployment,
                    "--confirm",
                    "--apply",
                ],
            )
            if not cleanup.ok:
                return _failed_command(
                    "destructive migration failed",
                    cleanup,
                )
            destructive_results.append(
                {"migration_id": migration_id, "result": cleanup.json_payload()}
            )
    migrate = _run_okg(
        ctx,
        migrate_cmd,
    )
    if not migrate.ok:
        return _failed_command("schema migrate failed", migrate)
    after = _run_okg(ctx, ["db", "migrations", "--deployment", ctx.options.deployment])
    if not after.ok:
        return _failed_command("post-migration status failed", after)
    after_status = after.json_payload()
    if not isinstance(after_status, dict):
        # Fail closed: a zero-exit post-migration status whose JSON does not
        # parse must not be coerced to an empty payload.
        return _unreadable_command("could not read post-migration status", after)
    return {
        "ok": True,
        "backup_revalidation": backup_revalidation,
        "extensions": extensions,
        "migrate": migrate.json_payload(),
        "destructive_migrations": destructive_results,
        "destructive_migrations_deferred": (
            [] if ctx.options.confirm_destructive_migrations else destructive_pending
        ),
        "destructive_migration_policy": (
            "destructive cleanup ran through its reviewed adapter after explicit "
            "operator confirmation"
            if ctx.options.confirm_destructive_migrations
            else "destructive cleanup is reported but not auto-applied by okg up; "
            "rerun with --confirm-destructive-migrations after staging proof"
        ),
        "migration_status": after_status,
    }


def _revalidate_backup_before_mutation(ctx: UpgradeContext) -> dict[str, Any]:
    """Revalidate the rollback archive and database state at mutation time."""
    backup = ctx.data.get("backup_result")
    if not isinstance(backup, dict):
        return {
            "ok": False,
            "reason": "backup proof is unavailable before mutation",
        }

    if backup.get("verification_mode") == "no_physical_rollback_required":
        from okg.substrate.deployment_backup_policy import (
            NO_PHYSICAL_ROLLBACK,
            PhysicalRollbackPolicyInput,
            select_physical_rollback_policy,
        )

        try:
            current_policy = _backup_policy_config(ctx)
        except SubstrateError as exc:
            return {
                "ok": False,
                "reason": "physical rollback policy changed before mutation",
                "error": exc.to_json()["error"],
            }
        prior_policy = backup.get("backup_policy")
        if not isinstance(prior_policy, dict) or prior_policy != current_policy:
            return {
                "ok": False,
                "reason": "physical rollback policy changed before mutation",
                "policy_stable": False,
            }
        toolchain = _receipt_bound_backup_toolchain(ctx)
        if not toolchain.get("ok"):
            return toolchain
        try:
            snapshot = _backup_compatibility_snapshot(ctx, toolchain)
        except Exception as exc:  # noqa: BLE001 - unreadable state refuses.
            return {
                "ok": False,
                "reason": "recovery compatibility state is unreadable",
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
        expected_digest = backup.get("effect_digest")
        effects, effect_errors, effect_report = _collect_effect_inventory(
            ctx,
            expected_digest=(
                expected_digest if isinstance(expected_digest, str) else ""
            ),
        )
        checkout_exact = effect_report.get("execution_checkout_exact") is True
        if not checkout_exact:
            return {
                "ok": False,
                "reason": "deployment checkout does not match the admitted target revision",
                "failed_checks": ["execution_checkout_exact"],
                "effect_inventory": effect_report,
            }
        recovery, recovery_errors, recovery_report, prior_size = (
            _disaster_recovery_receipt_evidence(ctx, snapshot, current_policy)
        )
        manifest = _resolve_upgrade_deployment(ctx)
        release = getattr(manifest, "release", None) or {}
        posture = (
            (getattr(manifest, "nomos", None) or {}).get(
                "deployment_posture"
            )
            or {}
        )
        decision = select_physical_rollback_policy(
            PhysicalRollbackPolicyInput(
                policy_mode=current_policy["verification_policy"],
                release_channel=release.get("channel"),
                readiness_profile=ctx.options.readiness_profile,
                deployment_class=posture.get("deployment_class"),
                destructive_confirmed=(
                    ctx.options.confirm_destructive_migrations
                ),
                effects=effects,
                recovery=recovery,
                effect_errors=effect_errors,
                recovery_errors=recovery_errors,
            )
        )
        if decision.mode != NO_PHYSICAL_ROLLBACK:
            return {
                "ok": False,
                "reason": "physical rollback admission drifted before mutation",
                "decision": {
                    "mode": decision.mode,
                    "reasons": list(decision.reasons),
                    "checks": decision.checks,
                },
                "effect_inventory": effect_report,
                "disaster_recovery_receipt": recovery_report,
                "fix_hint": (
                    "run `okg deployment backup verify "
                    f"{ctx.options.deployment}` after resolving target drift"
                ),
            }
        ctx.data["backup_result"] = {
            **backup,
            "toolchain": toolchain,
            "compatibility_snapshot": snapshot,
            "effect_inventory": effect_report,
            "disaster_recovery_receipt": recovery_report,
            "prior_observed_restored_size_bytes": prior_size,
        }
        return {
            "ok": True,
            "verification_mode": NO_PHYSICAL_ROLLBACK,
            "effect_digest": expected_digest,
            "effect_digest_stable": True,
            "policy_stable": True,
            "disaster_recovery_revalidated": True,
            "physical_archive_required": False,
            "backup_tool_child_processes_started": 0,
            "backup_tool_validation": toolchain.get("validation"),
        }

    backup_path_raw = backup.get("backup_path")
    if not isinstance(backup_path_raw, str) or not backup_path_raw.strip():
        return {
            "ok": False,
            "reason": "backup archive path is unavailable before mutation",
        }
    backup_path = Path(backup_path_raw)
    archive = _validate_backup_archive_for_run(ctx, backup_path)
    archive_checks = archive.get("archive_checks")
    archive_valid = (
        archive.get("ok") is True
        and isinstance(archive_checks, dict)
        and all(
            archive_checks.get(name) is True
            for name in BACKUP_ARCHIVE_CHECKS
        )
        and isinstance(backup.get("backup_digest"), str)
        and archive.get("backup_digest") == backup.get("backup_digest")
    )
    if not archive_valid:
        return {
            "ok": False,
            "reason": "current backup archive changed before mutation",
            "archive": archive,
        }

    status_result = _run_okg(
        ctx,
        ["db", "migrations", "--deployment", ctx.options.deployment],
    )
    if not status_result.ok:
        return _failed_command(
            "backup candidate-plan revalidation failed",
            status_result,
        )
    status = status_result.json_payload()
    if not isinstance(status, dict):
        return _unreadable_command(
            "could not read backup candidate-plan revalidation",
            status_result,
        )
    current_digest = _migration_plan_digest(status)
    prior_digest = backup.get("candidate_plan_digest")
    candidate_plan_stable = current_digest == prior_digest
    ctx.data["migrations_before"] = status
    try:
        current_snapshot = _backup_compatibility_snapshot(
            ctx,
            backup.get("toolchain") or {},
        )
    except Exception as exc:  # noqa: BLE001 - unreadable identity refuses mutation.
        return {
            "ok": False,
            "reason": "backup compatibility revalidation is unreadable",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    prior_snapshot = backup.get("compatibility_snapshot")
    if not isinstance(prior_snapshot, dict):
        return {
            "ok": False,
            "reason": "backup compatibility snapshot is unavailable before mutation",
        }
    state_keys = set(prior_snapshot) | set(current_snapshot)
    state_keys.discard("candidate_plan_digest")
    state_changed = sorted(
        key
        for key in state_keys
        if prior_snapshot.get(key) != current_snapshot.get(key)
    )
    if state_changed:
        return {
            "ok": False,
            "reason": "database state changed after the current backup archive",
            "changed_compatibility_fields": state_changed,
        }

    prior_policy = backup.get("backup_policy")
    current_policy = _backup_policy_config(ctx)
    policy_stable = (
        isinstance(prior_policy, dict)
        and prior_policy == current_policy
    )
    if (
        backup.get("verification_mode") != "routine_restore_drill_reused"
        and candidate_plan_stable
        and policy_stable
    ):
        ctx.data["backup_result"] = {
            **backup,
            "candidate_plan_digest": current_digest,
            "compatibility_snapshot": current_snapshot,
        }
        return {
            "ok": True,
            "verification_mode": backup.get("verification_mode"),
            "candidate_plan_stable": candidate_plan_stable,
            "candidate_plan_digest": current_digest,
            "archive_revalidated": True,
            "database_state_revalidated": True,
            "policy_stable": True,
        }

    receipt_checks = None
    receipt_errors: tuple[str, ...] = ()
    receipt_report: dict[str, Any] = {"status": "not_considered"}
    prior_restored_size: int | None = None
    if backup.get("verification_mode") == "routine_restore_drill_reused":
        (
            receipt_checks,
            receipt_errors,
            receipt_report,
            prior_restored_size,
        ) = _restore_drill_receipt_evidence(
            ctx,
            current_snapshot,
            current_policy,
        )
    from okg.substrate.deployment_backup_policy import (
        BackupPolicyInput,
        select_backup_verification,
    )

    manifest = _resolve_upgrade_deployment(ctx)
    release = getattr(manifest, "release", None) or {}
    posture = (
        (getattr(manifest, "nomos", None) or {}).get("deployment_posture") or {}
    )
    decision = select_backup_verification(BackupPolicyInput(
        policy_mode=current_policy["verification_policy"],
        release_channel=release.get("channel"),
        readiness_profile=ctx.options.readiness_profile,
        deployment_class=posture.get("deployment_class"),
        preflight_readable=True,
        current_archive_valid=archive_valid,
        destructive_confirmed=ctx.options.confirm_destructive_migrations,
        candidate_plan_stable=candidate_plan_stable,
        policy_stable=policy_stable,
        receipt=receipt_checks,
        receipt_errors=receipt_errors,
    ))
    if decision.reuse_allowed:
        ctx.data["backup_result"] = {
            **backup,
            "candidate_plan_digest": current_digest,
            "compatibility_snapshot": current_snapshot,
            "restore_drill_receipt": receipt_report,
            "prior_observed_restored_size_bytes": prior_restored_size,
        }
        return {
            "ok": True,
            "verification_mode": decision.mode,
            "candidate_plan_stable": True,
            "candidate_plan_digest": current_digest,
            "archive_revalidated": True,
            "database_state_revalidated": True,
            "receipt_revalidated": True,
            "policy_stable": True,
        }
    if decision.mode != "current_full_restore":
        return {
            "ok": False,
            "reason": "backup policy refused mutation during revalidation",
            "decision": {
                "mode": decision.mode,
                "reasons": list(decision.reasons),
            },
        }
    restored_size = (
        prior_restored_size
        or backup.get("prior_observed_restored_size_bytes")
        or backup.get("observed_restored_size_bytes")
    )
    verification = _run_current_full_restore_verification(
        ctx,
        backup_path,
        snapshot=current_snapshot,
        policy=current_policy,
        source_database_size=backup.get("source_database_size_bytes"),
        prior_restored_size=restored_size,
    )
    if verification.get("ok") is not True:
        return verification
    ctx.data["backup_result"] = {
        **backup,
        **verification,
        "candidate_plan_digest": current_digest,
        "compatibility_snapshot": current_snapshot,
        "prior_observed_restored_size_bytes": restored_size,
    }
    return {
        "ok": True,
        "verification_mode": "current_full_restore",
        "candidate_plan_stable": candidate_plan_stable,
        "candidate_plan_before": prior_digest,
        "candidate_plan_after": current_digest,
        "archive_revalidated": True,
        "database_state_revalidated": True,
        "receipt_revalidated": (
            backup.get("verification_mode") == "routine_restore_drill_reused"
        ),
        "policy_stable": policy_stable,
        "decision": {
            "mode": decision.mode,
            "reasons": list(decision.reasons),
        },
        "restore": verification,
    }


def _ensure_upgrade_extensions(ctx: UpgradeContext) -> dict[str, list[str]]:
    from okg.substrate.db.migrate import ensure_required_extensions

    return ensure_required_extensions(ctx.dsn)


def _latest_applied_catalog_fingerprint(
    dsn: str,
    deployment_name: str,
) -> str | None:
    """Fingerprint of the deployment's latest applied catalog version.

    Returns ``None`` when the catalog-versions table is absent or no
    applied row exists for this deployment.
    """
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.catalog_versions')")
            if cur.fetchone()[0] is None:
                return None
            cur.execute(
                """
                SELECT catalog_fingerprint
                  FROM okg.catalog_versions
                 WHERE status = 'applied'
                   AND deployment_name = %s
                   AND catalog_fingerprint IS NOT NULL
                 ORDER BY catalog_version_id DESC
                 LIMIT 1
                """,
                (deployment_name,),
            )
            row = cur.fetchone()
            return row[0] if row else None


def _composed_catalog_fingerprint(deployment_dir: str) -> str | None:
    """Content fingerprint of the deployment's composed catalog.

    Reuses the canonical ``catalog_identity_for`` fingerprint so the
    reconcile-on-change check matches what the apply path stamps. Returns
    ``None`` (forcing a normal apply) if composition is unavailable so a
    missing fingerprint never silently skips the apply.
    """
    from pathlib import Path

    try:
        from okg.substrate.catalog.modules_compose import compose_catalog
        from okg.substrate.catalog.ownership import catalog_identity_for

        composed = compose_catalog(Path(deployment_dir))
        identity = catalog_identity_for(
            composed=composed,
            deployment_dir=deployment_dir,
            command="okg up (reconcile-on-change probe)",
        )
        return identity.catalog_fingerprint
    except Exception:  # noqa: BLE001 — probe failure falls through to a real apply.
        return None


def _restamp_manifest_fingerprint(
    ctx: UpgradeContext,
    deployment_dir: str,
) -> dict[str, Any]:
    """Claim-family restamp of the ownership row's manifest fingerprint
    on the ontology-unchanged `okg up` path (#709 ruling R4)."""
    from pathlib import Path

    try:
        from okg.substrate.db.connection import connect as db_connect

        from okg.substrate.catalog.modules_compose import compose_catalog
        from okg.substrate.catalog.ownership import (
            catalog_identity_for,
            restamp_manifest_fingerprint,
        )

        composed = compose_catalog(Path(deployment_dir))
        identity = catalog_identity_for(
            composed=composed,
            deployment_dir=deployment_dir,
            command="okg up (manifest restamp)",
        )
        with db_connect(ctx.dsn, autocommit=True) as conn:
            return restamp_manifest_fingerprint(conn, identity=identity)
    except Exception as exc:  # noqa: BLE001 — fail-closed: restamp best-effort.
        # fail-closed: any failure leaves the ownership row's old manifest
        # fingerprint in place, so a later publish refuses the un-restamped
        # edit rather than silently blessing it. The failure is reported in
        # the result (restamped=False + reason), never swallowed as success.
        return {"restamped": False, "reason": f"{type(exc).__name__}: {exc}"}


def _reclaim_changed_catalog(ctx: UpgradeContext, deployment_dir: str) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        reclaim_same_writer_catalog_ownership,
    )

    identity = catalog_identity_for(
        composed=compose_catalog(Path(deployment_dir)),
        deployment_dir=deployment_dir,
        command="okg up (catalog ownership refresh)",
    )
    with db_connect(ctx.dsn, autocommit=True) as conn:
        return reclaim_same_writer_catalog_ownership(conn, identity=identity)


def _phase_catalog_apply(ctx: UpgradeContext) -> dict[str, Any]:
    manifest = _resolve_upgrade_deployment(ctx)
    deployment_dir = str(manifest.deployment_dir)
    catalog_cmd = [
        "catalog",
        "load",
        "--deployment",
        deployment_dir,
        "--apply",
    ]
    if ctx.options.plan_only:
        return {
            "ok": True,
            "planned": _display_okg_command(catalog_cmd),
            "skipped": "catalog apply skipped (--plan)",
        }
    backup_revalidation = _ensure_backup_revalidated_for_mutation(ctx)
    if backup_revalidation.get("ok") is not True:
        return backup_revalidation
    if (
        backup_revalidation.get("verification_mode")
        == "no_physical_rollback_required"
    ):
        return {
            "ok": True,
            "backup_revalidation": backup_revalidation,
            "catalog_unchanged": True,
            "skipped": "closed effect inventory proves catalog ownership is current",
        }

    # Reconcile-on-change: only stamp a new catalog_version when the
    # composed catalog's fingerprint actually differs from the latest
    # applied one. A no-op `up` (unchanged catalog) must NOT add a new
    # catalog_versions row. This consults the existing fingerprint dedup
    # (catalog_identity_for) rather than always running `catalog load
    # --apply`, which re-stamps an identical cv on every run (churn).
    composed_fingerprint = _composed_catalog_fingerprint(deployment_dir)
    if composed_fingerprint is not None:
        applied_fingerprint = _latest_applied_catalog_fingerprint(
            ctx.dsn, ctx.options.deployment,
        )
        if (
            applied_fingerprint is not None
            and applied_fingerprint == composed_fingerprint
        ):
            # #709 (ruling R4): the ontology is unchanged, but the manifest's
            # behavior-bearing blocks (enrichers, maintained_derivations,
            # source_registry, ...) may have been edited. Because we skip
            # catalog apply here, restamp the ownership row's manifest
            # fingerprint on this convergence path so a later publish does
            # not refuse a sanctioned enricher-only edit.
            restamp = _restamp_manifest_fingerprint(ctx, deployment_dir)
            return {
                "ok": True,
                "backup_revalidation": backup_revalidation,
                "catalog_unchanged": True,
                "reason": (
                    "catalog fingerprint matches the latest applied "
                    "catalog_version; skipped re-stamping a new cv"
                ),
                "catalog_fingerprint": composed_fingerprint,
                "manifest_restamp": restamp,
            }

    ownership_refresh = _reclaim_changed_catalog(ctx, deployment_dir)
    result = _run_okg(ctx, catalog_cmd)
    if not result.ok:
        if _is_views_registry_baseline_error(result):
            payload = _command_json_payload(result)
            ctx.data["views_registry_deferred"] = True
            return {
                "ok": True,
                "backup_revalidation": backup_revalidation,
                "views_registry_deferred": True,
                "ownership_refresh": ownership_refresh,
                "reason": (
                    "views_registry requires a published baseline; "
                    "will re-apply catalog after smoke publish"
                ),
                "result": payload,
            }
        # The reclaim has to precede the apply -- the apply is the call that
        # would otherwise refuse this writer -- and a failed apply does not
        # revert it. Report the transfer on the failure path too, so the
        # report never hides an ownership row that now advertises a catalog
        # and schema fingerprint which was never applied.
        return {
            **_failed_command("catalog apply failed", result),
            "ownership_refresh": ownership_refresh,
        }
    return {
        "ok": True,
        "backup_revalidation": backup_revalidation,
        "ownership_refresh": ownership_refresh,
        "result": result.json_payload(),
    }


def _phase_nomos_baseline(ctx: UpgradeContext) -> dict[str, Any]:
    if ctx.options.plan_only:
        return {
            "ok": True,
            "planned": "seed deployment Nomos baseline",
            "skipped": "Nomos baseline skipped (--plan)",
        }
    backup_revalidation = None
    if isinstance(ctx.data.get("backup_result"), dict):
        backup_revalidation = _ensure_backup_revalidated_for_mutation(ctx)
        if backup_revalidation.get("ok") is not True:
            return backup_revalidation
        if (
            backup_revalidation.get("verification_mode")
            == "no_physical_rollback_required"
        ):
            return {
                "ok": True,
                "backup_revalidation": backup_revalidation,
                "nomos_unchanged": True,
                "skipped": "closed effect inventory proves Nomos baseline is current",
            }
    manifest = _resolve_upgrade_deployment(ctx)
    manifest_path = manifest.deployment_dir / "deployment.yaml"
    raw_manifest = fastyaml.safe_load(manifest_path.read_text()) or {}
    if not _manifest_requires_nomos_runtime(raw_manifest):
        return {
            "ok": True,
            "skipped": "nomos runtime enforcement not enabled",
        }

    # `okg deployment up` owns release concerns, but declarative Nomos
    # convergence is delegated to the same public boundary as
    # `okg deployment apply`; this avoids two drifting seed algorithms.
    from okg.substrate.nomos.baseline import reconcile_deployment_baseline

    public_manifest = _resolve_upgrade_deployment(ctx)
    public_result = reconcile_deployment_baseline(
        deployment=ctx.options.deployment,
        deployment_dir=public_manifest.deployment_dir,
        dsn=ctx.dsn,
        apply=True,
    )
    # fix-nomos-read-path-ux (req.readux.mcp-subject-default, #640): the
    # nomos_baseline phase output names the MCP subject contract so
    # non-default setups are discoverable at bring-up time.  Delegating the
    # seed must not drop this from `okg deployment up`'s output --
    # `okg deployment provision` still emits it.
    from okg.substrate.nomos import mcp_subject_hint_for_manifest

    raw_manifest = fastyaml.safe_load(
        (Path(public_manifest.deployment_dir) / "deployment.yaml").read_text()
    ) or {}
    return {
        "ok": bool(public_result.get("ok")),
        "backup_revalidation": backup_revalidation,
        "mcp_subject_hint": mcp_subject_hint_for_manifest(raw_manifest),
        "result": _json_safe(public_result),
    }


def _manifest_requires_nomos_runtime(manifest: dict[str, Any]) -> bool:
    # Single source of truth: honors the OKG_NOMOS_RUNTIME_ENFORCEMENT override
    # and the audit/enforce tri-state (audit also evaluates the boundary).
    from okg.substrate.nomos.runtime_enforcement import (
        manifest_nomos_enforcement_mode,
    )

    return manifest_nomos_enforcement_mode(manifest) != "off"


def _nomos_source_names(source_registry: dict[str, Any]) -> list[str]:
    sources = source_registry.get("sources")
    names: set[str] = set()
    if isinstance(sources, dict):
        for name, raw_entry in sources.items():
            if str(name).strip():
                names.add(str(name))
            if isinstance(raw_entry, dict):
                ownership_id = str(raw_entry.get("ownership_id") or "").strip()
                if ownership_id:
                    names.add(ownership_id)
        return sorted(names)
    if isinstance(sources, list):
        for index, entry in enumerate(sources):
            if isinstance(entry, dict):
                raw = entry.get("name") or entry.get("id") or entry.get("source")
                name = str(raw).strip() if raw else f"source_{index}"
                if name:
                    names.add(name)
                ownership_id = str(entry.get("ownership_id") or "").strip()
                if ownership_id:
                    names.add(ownership_id)
        return sorted(names)
    return []


def _nomos_policy_source_ids(
    source_registry: dict[str, Any],
    *,
    deployment: str,
    deployment_dir: Path | None = None,
) -> list[str]:
    """Return every source identity that the runtime read boundary may see."""
    # Catalog reconciliation can publish a generation whose rows carry this
    # substrate-owned lineage even though it is not a deployment source. It
    # must be present in every deployment baseline before the first restamp;
    # otherwise strict Nomos read boundaries fail closed immediately after a
    # successful catalog apply.
    source_ids = set(_nomos_source_names(source_registry))
    source_ids.add("_catalog:restamp")
    if deployment_dir is not None:
        if (deployment_dir / "views_registry.yaml").exists():
            source_ids.add("catalog:views_registry")
            source_ids.add("_system:native_views")
        if (deployment_dir / "monitored_hosts.yaml").exists():
            source_ids.add("system:external_backed_collector")

        # Deployment-owned derived producers bypass the source registry but
        # stamp their own exact identities into graph lineage.  Baseline
        # compilation must therefore inventory the same configured enrichers
        # and derived producers that the publisher can load; otherwise the
        # first valid derived edge makes the read boundary fail closed.
        # Two deliberate widenings: enricher construction runs here (a bad
        # rules file now raises inside readiness/provision inventory, which
        # reports failed — loud fail-closed rather than a silent gap), and
        # profile-gated enrichers are inventoried even when the runtime
        # profile never loads them — extra ALLOW ids grant reads on lineage
        # that never exists, which is safe; omitting a producer that does
        # emit is not.
        from okg.substrate.publisher.deployment_stack import (
            load_derived_producer_policies,
        )
        from okg.substrate.publisher.enricher_loader import load_enrichers
        from okg.substrate.library.linkers.declarative import DeclarativeLinker

        for enricher in load_enrichers(deployment_dir):
            enricher_name = str(
                getattr(enricher, "name", type(enricher).__name__)
            ).strip()
            if enricher_name:
                source_ids.add(f"_enricher:{enricher_name}")
                if isinstance(enricher, DeclarativeLinker):
                    source_ids.update(
                        f"_enricher:{enricher_name}:{rule_id}"
                        for rule in enricher.rules
                        if (rule_id := str(rule.id).strip())
                    )
        source_ids.update(load_derived_producer_policies(deployment_dir))
    sources = source_registry.get("sources")
    if isinstance(sources, dict):
        entries = sources.items()
    elif isinstance(sources, list):
        entries = (
            (
                str(entry.get("name") or entry.get("id") or entry.get("source") or ""),
                entry,
            )
            for entry in sources
            if isinstance(entry, dict)
        )
    else:
        entries = ()
    for raw_name, entry in entries:
        source_name = str(raw_name).strip()
        if not source_name or not isinstance(entry, dict):
            continue
        ownership_id = str(entry.get("ownership_id") or "").strip()
        if ownership_id:
            source_ids.add(ownership_id)
        admission = entry.get("admission_policy")
        if isinstance(admission, dict):
            producer_id = str(admission.get("producer_id") or "").strip()
            if producer_id:
                source_ids.add(producer_id)
        if deployment and "." not in source_name:
            source_ids.add(f"{deployment}.{source_name}")
    return sorted(source_ids)


def _nomos_runtime_source_names(source_registry: dict[str, Any]) -> list[str]:
    sources = source_registry.get("sources")
    names: set[str] = set()
    if isinstance(sources, dict):
        for name in sources:
            if str(name).strip():
                names.add(str(name))
        return sorted(names)
    if isinstance(sources, list):
        for index, entry in enumerate(sources):
            if isinstance(entry, dict):
                raw = entry.get("name") or entry.get("id") or entry.get("source")
                name = str(raw).strip() if raw else f"source_{index}"
                if name:
                    names.add(name)
        return sorted(names)
    return []


def _nomos_source_policies(
    source_registry: dict[str, Any],
    audited_policies: dict[str, Any],
) -> dict[str, Any]:
    policies = dict(audited_policies or {})
    sources = source_registry.get("sources")
    if isinstance(sources, dict):
        for name, raw_entry in sources.items():
            policy = policies.get(str(name))
            if policy is None or not isinstance(raw_entry, dict):
                continue
            ownership_id = str(raw_entry.get("ownership_id") or "").strip()
            if ownership_id:
                policies.setdefault(ownership_id, policy)
    elif isinstance(sources, list):
        for index, raw_entry in enumerate(sources):
            if not isinstance(raw_entry, dict):
                continue
            raw = raw_entry.get("name") or raw_entry.get("id") or raw_entry.get("source")
            name = str(raw).strip() if raw else f"source_{index}"
            policy = policies.get(name)
            ownership_id = str(raw_entry.get("ownership_id") or "").strip()
            if policy is not None and ownership_id:
                policies.setdefault(ownership_id, policy)
    return policies


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(child) for key, child in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(child) for child in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _is_views_registry_baseline_error(result: CommandResult) -> bool:
    payload = _command_json_payload(result)
    if not isinstance(payload, dict):
        return False
    error = payload.get("error")
    if not isinstance(error, dict):
        return False
    message = str(error.get("message") or "")
    return (
        "no published generation for deployment" in message
        and "views registry" in message
    )


def _command_json_payload(result: CommandResult) -> Any | None:
    for text in (result.stdout, result.stderr):
        try:
            return json.loads(text)
        except (TypeError, ValueError):
            continue
    return None


def _resolve_upgrade_deployment(ctx: UpgradeContext):
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )

    try:
        return resolve_deployment(ctx.options.deployment)
    except DeploymentResolutionError as exc:
        raise SubstrateError(
            "deployment manifest could not be resolved",
            data={"deployment": ctx.options.deployment},
            fix_hint=(
                "pass a valid deployment name, set OKG_DEPLOYMENTS_DIR, "
                "or pass an explicit deployment manifest path"
            ),
        ) from exc


def _phase_attribute_read_model(ctx: UpgradeContext) -> dict[str, Any]:
    """Make generation-pinned attribute operators ready before service start."""
    if ctx.options.plan_only:
        return {
            "ok": True,
            "planned": "repair latest published effective-attribute interval view",
            "skipped": "attribute read-model repair skipped (--plan)",
        }
    from okg.substrate.graph_store_internal.generation_ordinal import (
        interval_representation_flag_enabled,
    )

    # Either switch disables the interval read model. OKG_IVM_GENERATION_ORDINAL=0
    # (issue #1514) additionally leaves the latest generations with no ordinal,
    # which the repair verb below resolves and fails loud on -- refuse by name
    # here rather than crashing mid-upgrade, after stop_services.
    if not interval_representation_flag_enabled(ctx.env):
        return {
            "ok": False,
            "reason": "interval-backed attribute view is disabled",
            "fix_hint": (
                "remove OKG_IVM_INTERVAL_VIEW=0 / OKG_IVM_GENERATION_ORDINAL=0 "
                "and rerun deployment up; Nomos-safe attribute operators "
                "require the interval read model"
            ),
        }
    backup_revalidation = None
    if isinstance(getattr(ctx, "data", {}).get("backup_result"), dict):
        backup_revalidation = _ensure_backup_revalidated_for_mutation(ctx)
        if backup_revalidation.get("ok") is not True:
            return backup_revalidation

    result = _run_okg(
        ctx,
        [
            "db",
            "repair-effective-attribute-intervals",
            "--deployment",
            ctx.options.deployment,
            "--apply",
        ],
    )
    if not result.ok:
        return _failed_command("attribute read-model repair failed", result)
    payload = result.json_payload()
    if not isinstance(payload, dict):
        return _unreadable_command(
            "could not read attribute read-model repair result", result
        )
    if payload.get("ok") is not True:
        return {
            "ok": False,
            "reason": "attribute read-model repair did not report success",
            "result": payload,
        }
    return {
        "ok": True,
        **(
            {"backup_revalidation": backup_revalidation}
            if backup_revalidation is not None
            else {}
        ),
        "result": payload,
    }


def _phase_restart_services(ctx: UpgradeContext) -> dict[str, Any]:
    cmd = [
        "dogfood",
        "restart",
        "--deployment",
        ctx.options.deployment,
        "--apply",
    ]
    if not ctx.options.apply_services:
        return {
            "ok": True,
            "planned": _display_okg_command(cmd),
            "skipped": "service mutation skipped (--plan / --no-apply-services)",
        }
    result = _run_okg(ctx, cmd)
    if not result.ok:
        return _failed_command("restart services failed", result)
    if isinstance(getattr(ctx, "data", None), dict):
        ctx.data["services_stopped"] = False
    return {"ok": True, "result": result.json_payload()}


def _deployment_publish_command(ctx: UpgradeContext) -> list[str]:
    manifest = _resolve_upgrade_deployment(ctx)
    runtime = getattr(manifest, "runtime", None)
    smoke_sources = tuple(getattr(runtime, "smoke_sources", ()) or ())
    smoke_reset_cursor = bool(
        getattr(runtime, "smoke_reset_cursor", False)
    )
    selected_sources = ctx.options.include or smoke_sources
    cmd = [
        "ingest",
        "--deployment",
        ctx.options.deployment,
        "--inline",
        "--mode",
        "scope_complete",
    ]
    if selected_sources:
        cmd.extend(["--include", ",".join(selected_sources)])
    # Cursor reset is safe only for the deployment-owned canary. An operator
    # override may name a mutable API source, so it must never inherit the
    # deployment canary's replay policy implicitly.
    if smoke_reset_cursor and not ctx.options.include:
        cmd.append("--reset-cursor")
    return cmd


def _phase_smoke_publish(ctx: UpgradeContext) -> dict[str, Any]:
    manifest = _resolve_upgrade_deployment(ctx)
    runtime = getattr(manifest, "runtime", None)
    selected_sources = ctx.options.include or tuple(
        getattr(runtime, "smoke_sources", ()) or ()
    )
    cmd = _deployment_publish_command(ctx)
    if ctx.options.plan_only:
        return {
            "ok": True,
            "planned": _display_okg_command(cmd),
            "skipped": "smoke publish skipped (--plan)",
        }
    backup_revalidation = None
    if isinstance(ctx.data.get("backup_result"), dict):
        backup_revalidation = _ensure_backup_revalidated_for_mutation(ctx)
        if backup_revalidation.get("ok") is not True:
            return backup_revalidation
    authority = (
        backup_revalidation.get("effect_authority")
        if isinstance(backup_revalidation, Mapping)
        else None
    )
    if not isinstance(authority, Mapping):
        authority = backup_revalidation
    current_effects = (
        authority.get("effect_inventory")
        if isinstance(authority, Mapping)
        else None
    )
    if not isinstance(current_effects, Mapping):
        current_effects = (
            ctx.data.get("backup_result", {}).get("effect_inventory")
            if isinstance(ctx.data.get("backup_result"), Mapping)
            else None
        )
    smoke_inventory = (
        current_effects.get("smoke")
        if isinstance(current_effects, Mapping)
        else None
    )
    # Recovery privilege belongs only to generation_recovery. This ordinary
    # canary proves the post-recovery publisher path with no force switch.
    result = _run_okg(ctx, cmd)
    if not result.ok:
        return _failed_command("smoke publish failed", result)
    publish_payload = result.json_payload()
    if not isinstance(publish_payload, dict):
        # Fail closed: a zero-exit smoke publish whose JSON does not parse must
        # not be trusted as a successful publish.
        return _unreadable_command("could not read smoke publish output", result)
    ctx.data["smoke_publish"] = publish_payload
    payload: dict[str, Any] = publish_payload
    if ctx.data.get("views_registry_deferred"):
        registry = _run_okg(
            ctx,
            [
                "catalog",
                "load",
                "--deployment",
                str(manifest.deployment_dir),
                "--apply",
            ],
        )
        if not registry.ok:
            return _failed_command("views registry apply failed", registry)
        payload = {
            "publish": publish_payload,
            "views_registry_apply": registry.json_payload(),
        }
        ctx.data["views_registry_after_smoke"] = payload["views_registry_apply"]

    # Uniform real-publish gate: the smoke publish must terminate at the SAME
    # bar as `deployment bootstrap verify` — a published (not blocked)
    # generation with a positive node/edge row delta and the DLQ in bounds.
    # Exit-code-only success is no longer enough.
    proof = _smoke_publish_proof(ctx, publish_payload)
    if not proof.get("ok"):
        return {
            "ok": False,
            "reason": proof.get("reason") or "smoke publish proof failed",
            "publish_proof": proof,
            "result": payload,
            **({"fix_hint": proof["fix_hint"]} if proof.get("fix_hint") else {}),
        }
    # Decision 23a: a lock hold over the capacity bar passes the proof; say so
    # on stderr and at the top of the upgrade report. The exit code is unchanged.
    from okg.substrate.publish_proof import surface_capacity_warning

    capacity_warning = surface_capacity_warning(proof, surface="smoke publish")
    if capacity_warning is not None:
        ctx.data["capacity_warning"] = capacity_warning
    return {
        "ok": True,
        **(
            {"capacity_warning": capacity_warning}
            if capacity_warning is not None
            else {}
        ),
        "smoke_generation_id": _smoke_generation(publish_payload),
        **(
            {"backup_revalidation": backup_revalidation}
            if backup_revalidation is not None
            else {}
        ),
        "result": payload,
        "publish_proof": proof,
        "smoke_sources": list(selected_sources),
        "smoke_source_digest": (
            smoke_inventory.get("selected_source_digest")
            if isinstance(smoke_inventory, Mapping)
            else None
        ),
    }


def _smoke_publish_proof(
    ctx: UpgradeContext,
    publish_payload: dict[str, Any],
) -> dict[str, Any]:
    from okg.substrate.publish_proof import assert_publish_proof
    from okg.substrate.runtime_queue_health import (
        DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES,
    )

    # The upgrade smoke phase is the ONLY caller that accepts the two validated
    # healthy no-ops over a populated deployment:
    #   * ``allow_no_source_work`` — the publish is SKIPPED because the source
    #     produced no work and emits the ``no_source_work`` triple; and
    #   * ``allow_caught_up_replay`` — the publish is NOT skipped but commits a
    #     head generation whose live-row differential is empty because a
    #     continuously running worker already made the graph current, so every
    #     requested source reaches an authorized zero-output terminal state
    #     (authoritative replay / empty snapshot). This is the steady state for a
    #     healthy okg-workspace: a routine `deployment up` has nothing new to add.
    # Both stay fail-closed (a wedged, degraded, blocked, or poisoned-queue
    # publish still fails). Bootstrap-verify and provision keep the strict insert
    # proof (make-migrate-idempotent, ruling 4).
    plan = getattr(ctx, "data", {}).get("safety_plan")
    external = plan.get("external_effects") if isinstance(plan, Mapping) else None
    provider_effect_present = (
        isinstance(external, Mapping) and external.get("present") is True
    )
    return assert_publish_proof(
        publish_payload,
        dsn=ctx.dsn,
        allow_updated_rows=True,
        # A provider-effect smoke is the deployment canary.  It must produce a
        # real published differential; otherwise the run has neither exercised
        # the declared adapter nor proved the expected live-row change.
        allow_no_source_work=not provider_effect_present,
        allow_caught_up_replay=not provider_effect_present,
        ignored_runtime_workflow_names=set(
            DEPLOYMENT_READINESS_DELEGATED_RUNTIME_WORKFLOW_NAMES
        ),
    )


def _phase_readiness(ctx: UpgradeContext) -> dict[str, Any]:
    # renamed-flag compat: this child runs the checked-out release pin, which
    # may predate the rename; flip to "--checks" when the aliases are removed.
    ready_cmd = [
        "deployment",
        "ready",
        ctx.options.deployment,
        COMPAT_READY_CHECKS_FLAG,
        ctx.options.readiness_profile,
    ]
    if ctx.options.plan_only:
        return {
            "ok": True,
            "planned": _display_okg_command(ready_cmd),
            "skipped": "readiness skipped (--plan)",
        }
    runtime_witness = None
    if _runtime_quiescence_required(ctx):
        runtime_witness = _prepare_runtime_readiness_witness(ctx)
        if runtime_witness.get("ok") is not True:
            return {
                "ok": False,
                "reason": "runtime readiness witness failed under promotion fence",
                "runtime_witness": runtime_witness,
            }
        ready_cmd.extend([
            "--max-producer-age-seconds",
            str(runtime_witness["max_producer_age_seconds"]),
        ])
    release = _run_okg(
        ctx,
        ["deployment", "release", ctx.options.deployment, "--process-role", "up"],
    )
    if not release.ok:
        return _failed_command("release readiness failed", release)
    release_payload = release.json_payload()
    ctx.data["release_after"] = release_payload
    ready = _run_okg(ctx, ready_cmd, env_overrides=RENAMED_FLAG_COMPAT_ENV)
    if not ready.ok:
        return {
            **_failed_command("deployment readiness failed", ready),
            **(
                {"runtime_witness": runtime_witness}
                if runtime_witness is not None
                else {}
            ),
        }
    ready_payload = ready.json_payload()
    if not isinstance(ready_payload, dict):
        return _unreadable_command("could not read deployment readiness", ready)
    readiness_generation = _readiness_generation(ready_payload)
    smoke_generation = _bound_smoke_generation(
        ctx,
        _smoke_generation(ctx.data.get("smoke_publish")),
        readiness_generation,
    )
    if (
        smoke_generation is None
        or readiness_generation is None
        or readiness_generation != smoke_generation
    ):
        return {
            "ok": False,
            "reason": "deployment readiness does not match smoke generation",
            "smoke_generation_id": smoke_generation,
            "readiness_generation_id": readiness_generation,
        }
    final_proof = None
    if ctx.options.readiness_profile == "ci":
        final_proof = _collect_ci_deployment_final_proof(
            ctx,
            release=release_payload,
            readiness=ready_payload,
        )
        if final_proof.get("ok") is not True:
            return {
                "ok": False,
                "reason": "CI deployment final proof is incomplete or cross-bound",
                "final_proof": final_proof,
            }
    runtime_reopen = None
    if _runtime_quiescence_required(ctx):
        from okg.substrate.runtime.promotion_fence import (
            release_promotion_fence,
        )

        owner = str(ctx.data.get("runtime_fence_owner") or "")
        if not owner:
            return {
                "ok": False,
                "reason": "generation-bound readiness has no runtime fence owner",
            }
        runtime_reopen = release_promotion_fence(
            deployment=ctx.options.deployment,
            dsn=ctx.dsn,
            promotion_id=owner,
            schedule_disposition="keep",
        )
        if (
            runtime_reopen.get("released") is not True
            or runtime_reopen.get("status") != "released"
            or runtime_reopen.get("durable_release_receipt") is not True
            or not str(
                runtime_reopen.get("candidate_schedule_snapshot_digest") or ""
            ).startswith("sha256:")
        ):
            return {
                "ok": False,
                "reason": "runtime writers were not durably reopened",
                "runtime_reopen": runtime_reopen,
            }
    ctx.data["readiness"] = ready_payload
    return {
        "ok": True,
        "release": release_payload,
        "readiness": ready_payload,
        "smoke_generation_id": smoke_generation,
        "readiness_generation_id": readiness_generation,
        **(
            {"runtime_witness": runtime_witness}
            if runtime_witness is not None
            else {}
        ),
        **({"final_proof": final_proof} if final_proof is not None else {}),
        **(
            {"runtime_reopen": runtime_reopen}
            if runtime_reopen is not None
            else {}
        ),
    }


def _runtime_readiness_witness_deduplication_id(
    ctx: UpgradeContext,
    *,
    owner: str,
) -> str:
    identity = f"{ctx.options.deployment}\0{owner}".encode("utf-8")
    digest = hashlib.sha256(identity).hexdigest()[:32]
    return f"deployment-readiness-doctor:{digest}"


def _prepare_runtime_readiness_witness(ctx: UpgradeContext) -> dict[str, Any]:
    """Prove the fenced worker is live without reopening routine schedules.

    A promotion fence deliberately removes DBOS schedules. A long smoke publish
    can therefore outlive the ordinary producer-heartbeat TTL even though source
    convergence already proved the candidate and the worker is healthy. Refreshing
    the normal poll path would violate the fence and can create a new generation
    after the smoke-generation binding. Instead, run the allowlisted doctor
    workflow under the exact fence owner and relax only the producer-heartbeat age
    to the observed fence age plus one normal TTL. Watermark freshness, queue/DLQ
    health, doctor health, and generation binding remain strict.
    """
    from okg.substrate.runtime.promotion_fence import promotion_fence_status

    owner = str(ctx.data.get("runtime_fence_owner") or "")
    status = promotion_fence_status(
        deployment=ctx.options.deployment,
        dsn=ctx.dsn,
    )
    if (
        not owner
        or not isinstance(status, Mapping)
        or status.get("status") == "released"
        or status.get("promotion_id") != owner
    ):
        return {
            "ok": False,
            "reason": "runtime readiness witness has no matching active fence",
            "promotion_id": owner or None,
            "fence_status": dict(status) if isinstance(status, Mapping) else None,
        }

    fenced_at = status.get("fenced_at")
    if isinstance(fenced_at, datetime):
        fenced_at_dt = fenced_at
    else:
        try:
            fenced_at_dt = datetime.fromisoformat(str(fenced_at))
        except (TypeError, ValueError):
            return {
                "ok": False,
                "reason": "runtime readiness fence has no parseable fenced_at",
                "promotion_id": owner,
                "fenced_at": fenced_at,
            }
    if fenced_at_dt.tzinfo is None:
        fenced_at_dt = fenced_at_dt.replace(tzinfo=timezone.utc)
    fence_age_seconds = max(
        0.0,
        (datetime.now(timezone.utc) - fenced_at_dt.astimezone(timezone.utc))
        .total_seconds(),
    )
    normal_producer_ttl_seconds = 15 * 60
    max_producer_age_seconds = max(
        normal_producer_ttl_seconds,
        math.ceil(fence_age_seconds) + normal_producer_ttl_seconds,
    )

    deduplication_id = _runtime_readiness_witness_deduplication_id(
        ctx,
        owner=owner,
    )
    stored = ctx.data.get("runtime_readiness_witness")
    stored = dict(stored) if isinstance(stored, Mapping) else None
    if stored is not None and (
        stored.get("promotion_id") != owner
        or stored.get("deduplication_id") != deduplication_id
    ):
        return {
            "ok": False,
            "reason": "persisted runtime readiness witness has different authority",
            "promotion_id": owner,
            "expected_deduplication_id": deduplication_id,
            "persisted_witness": stored,
        }
    witness = stored or {
        "schema": "okg.deployment_runtime_readiness_witness.v1",
        "promotion_id": owner,
        "deduplication_id": deduplication_id,
        "status": "intent",
        "created_at": _now(),
    }
    if stored is None:
        # Persist the deterministic identity before enqueue. If the process dies
        # after DBOS accepts the row but before its workflow id is recorded, the
        # resume gate can still bind that pending row to this exact operation.
        _persist_runtime_readiness_witness(ctx, witness)

    workflow_id = str(witness.get("workflow_id") or "")
    if not workflow_id:
        enqueued = _run_okg(
            ctx,
            [
                "runtime",
                "enqueue-fenced-recovery",
                "doctor_workflow",
                "--deployment",
                ctx.options.deployment,
                "--promotion-id",
                owner,
                "--deduplication-id",
                deduplication_id,
            ],
        )
        if not enqueued.ok:
            return _failed_command(
                "fenced runtime doctor enqueue failed",
                enqueued,
            )
        enqueue_payload = enqueued.json_payload()
        workflow_id = str(
            enqueue_payload.get("workflow_id")
            if isinstance(enqueue_payload, Mapping)
            else ""
        )
        if not workflow_id:
            return _unreadable_command(
                "could not read fenced runtime doctor workflow identity",
                enqueued,
            )
        witness.update({
            "workflow_id": workflow_id,
            "status": "pending",
            "enqueued_at": _now(),
        })
        _persist_runtime_readiness_witness(ctx, witness)
    terminal = _wait_for_source_recovery_workflow(
        ctx,
        workflow_id=workflow_id,
        source="runtime-doctor",
        timeout_seconds=_positive_env_float(
            ctx,
            "OKG_DEPLOYMENT_READINESS_WITNESS_TIMEOUT_SECONDS",
            300.0,
        ),
        poll_interval_seconds=_positive_env_float(
            ctx,
            "OKG_DEPLOYMENT_SOURCE_POLL_SECONDS",
            2.0,
        ),
    )
    witness.update({
        "status": "success" if terminal.get("ok") is True else "pending",
        "checked_at": _now(),
        "terminal": terminal,
    })
    _persist_runtime_readiness_witness(ctx, witness)
    if terminal.get("ok") is not True:
        return terminal
    return {
        "ok": True,
        "promotion_id": owner,
        "fenced_at": fenced_at_dt.isoformat(),
        "fence_age_seconds": fence_age_seconds,
        "max_producer_age_seconds": max_producer_age_seconds,
        "doctor_workflow_id": workflow_id,
        "deduplication_id": deduplication_id,
        "doctor_workflow": terminal.get("report"),
    }


def _collect_ci_deployment_final_proof(
    ctx: UpgradeContext,
    *,
    release: Any,
    readiness: dict[str, Any],
) -> dict[str, Any]:
    generation = readiness.get("generation") if isinstance(readiness.get("generation"), dict) else {}
    queues = readiness.get("queues") if isinstance(readiness.get("queues"), dict) else {}
    freshness = readiness.get("source_freshness") if isinstance(readiness.get("source_freshness"), dict) else {}
    source_sync = readiness.get("source_sync_status") if isinstance(readiness.get("source_sync_status"), dict) else {}
    convergence = source_sync.get("convergence") if isinstance(source_sync.get("convergence"), dict) else {}
    doctor = readiness.get("doctor") if isinstance(readiness.get("doctor"), dict) else {}
    mcp = readiness.get("mcp") if isinstance(readiness.get("mcp"), dict) else {}
    database = readiness.get("database") if isinstance(readiness.get("database"), dict) else {}
    liveness = readiness.get("source_sync_liveness") if isinstance(readiness.get("source_sync_liveness"), dict) else {}
    nomos_audit = readiness.get("nomos_audit") if isinstance(readiness.get("nomos_audit"), dict) else {}
    nomos_runtime = readiness.get("nomos_runtime") if isinstance(readiness.get("nomos_runtime"), dict) else {}
    release_payload = release if isinstance(release, dict) else {}
    ready_generation = generation.get("generation_id") or generation.get("checkpoint_id")
    smoke_generation = _bound_smoke_generation(
        ctx,
        _smoke_generation(ctx.data.get("smoke_publish")),
        ready_generation,
    )
    mcp_generation = mcp.get("generation_id") or mcp.get("pinned_generation_id")
    state = _load_state_file(ctx.state_file)
    phase_results = state.get("phase_results") or []
    completed = {
        str(row.get("phase")): row
        for row in phase_results
        if isinstance(row, dict) and row.get("ok") is True
    }
    dlq_result = _run_okg(
        ctx,
        ["db", "current-runtime-dlq", "--deployment", ctx.options.deployment],
    )
    current_dlq = dlq_result.json_payload() if dlq_result.ok else None
    current_dlq = current_dlq if isinstance(current_dlq, dict) else {}
    active_depth = queues.get("active_queue_depth")
    current_dlq_count = current_dlq.get("current_count")
    integrity = generation.get("count_integrity")
    integrity = integrity if isinstance(integrity, dict) else {}
    checkpoint_counts = integrity.get("checkpoint")
    pinned_counts = integrity.get("pinned")
    exact_integer_counts = (
        isinstance(checkpoint_counts, dict)
        and isinstance(pinned_counts, dict)
        and all(
            isinstance(checkpoint_counts.get(key), int)
            and not isinstance(checkpoint_counts.get(key), bool)
            and isinstance(pinned_counts.get(key), int)
            and not isinstance(pinned_counts.get(key), bool)
            for key in ("node_count", "edge_count")
        )
    )
    release_current = (
        release_payload.get("current")
        if isinstance(release_payload.get("current"), Mapping)
        else {}
    )
    producer_liveness_ok = (
        liveness.get("producer_liveness_ok") is True
        if "producer_liveness_ok" in liveness
        else liveness.get("ok") is True
    )
    checks = {
        "release_identity": release_payload.get("ok") is True
        and bool(
            release_payload.get("code_revision")
            or release_payload.get("release_revision")
            or release_payload.get("revision")
            or release_current.get("code_revision")
        ),
        # Production readiness reports relation evidence rather than a
        # synthetic `ok` bit. Missing or malformed evidence must fail closed.
        "database": isinstance(database.get("required_relations"), list)
        and bool(database.get("required_relations"))
        and isinstance(database.get("present_relations"), list)
        and isinstance(database.get("missing_relations"), list)
        and not database.get("missing_relations"),
        "generation_published": generation.get("status") == "published"
        and bool(ready_generation),
        "generation_counts_exact": integrity.get("ok") is True
        and exact_integer_counts and checkpoint_counts == pinned_counts,
        "smoke_generation_bound": bool(smoke_generation)
        and str(smoke_generation) == str(ready_generation),
        "queue_depth_zero": isinstance(active_depth, int)
        and not isinstance(active_depth, bool) and active_depth == 0,
        "current_dlq_zero": isinstance(current_dlq_count, int)
        and not isinstance(current_dlq_count, bool) and current_dlq_count == 0,
        "source_freshness": freshness.get("ok") is True
        and not freshness.get("missing")
        and not freshness.get("stale"),
        "source_convergence": convergence.get("ok") is True
        and convergence.get("state") == "ready",
        # The readiness payload carries two different aggregates here:
        # producer_liveness_ok is the direct missing/stale producer verdict,
        # while liveness.ok also folds an unbound convergence view that has no
        # generation/freshness inputs.  The authoritative convergence and
        # runtime-service/doctor gates are checked independently below.
        "producer_freshness": producer_liveness_ok
        and not liveness.get("missing_producers")
        and not liveness.get("stale_producers"),
        "nomos_readiness": nomos_audit.get("ok") is True
        and nomos_runtime.get("ok") is True,
        "doctor": doctor.get("ok") is True,
        "mcp_alignment": mcp.get("ok") is True
        and bool(mcp_generation)
        and str(mcp_generation) == str(ready_generation),
        "catalog_applied": "catalog_apply" in completed
        and not completed["catalog_apply"].get("skipped"),
        "nomos_applied": "nomos_baseline" in completed
        and not completed["nomos_baseline"].get("skipped"),
    }
    return {
        "schema": "okg.deployment.ci_final_proof.v1",
        "ok": all(checks.values()),
        "observed_at": _now(),
        "deployment": ctx.options.deployment,
        "contract_digest": CANONICAL_CONTRACT.digest,
        "release": release_payload,
        "generation_id": ready_generation,
        "smoke_generation_id": smoke_generation,
        "mcp_generation_id": mcp_generation,
        "checks": checks,
        "evidence": {
            "database": database,
            "queues": queues,
            "current_dlq": current_dlq,
            "generation_count_integrity": integrity,
            "source_freshness": freshness,
            "source_sync_liveness": liveness,
            "source_convergence": convergence,
            "nomos_audit": nomos_audit,
            "nomos_runtime": nomos_runtime,
            "doctor": doctor,
            "mcp": mcp,
        },
    }


def _run_okg(
    ctx: UpgradeContext,
    args: list[str],
    *,
    env_overrides: dict[str, str] | None = None,
) -> CommandResult:
    env = ctx.env if not env_overrides else {**ctx.env, **env_overrides}
    return ctx.command_runner(
        [*running_okg_argv(), "--json", *args],
        env,
    )


def _artifact_progress_bytes(path: Path) -> int:
    """Return byte progress for file or directory artifacts."""
    try:
        if path.is_symlink() or not path.exists():
            return 0
        if path.is_file():
            return path.stat().st_size
        if path.is_dir():
            return sum(
                candidate.stat().st_size
                for candidate in path.rglob("*")
                if candidate.is_file() and not candidate.is_symlink()
            )
    except OSError:
        return 0
    return 0


def _run_long_command(
    ctx: UpgradeContext,
    argv: list[str],
    env: dict[str, str],
    *,
    operation: str,
    progress_interval_seconds: float,
    artifact_path: Path | None = None,
) -> tuple[CommandResult, dict[str, Any]]:
    """Run a blocking command while emitting credential-free heartbeats."""
    if ctx.command_runner is _default_command_runner:
        return _run_default_long_command(
            argv,
            env,
            operation=operation,
            progress_interval_seconds=progress_interval_seconds,
            artifact_path=artifact_path,
        )
    started_at = _now()
    started_monotonic = time.monotonic()
    done = threading.Event()
    outcome: dict[str, Any] = {}

    def invoke() -> None:
        try:
            outcome["result"] = ctx.command_runner(argv, env)
        except BaseException as exc:  # noqa: BLE001 - re-raised on caller thread.
            outcome["error"] = exc
        finally:
            done.set()

    worker = threading.Thread(
        target=invoke,
        name=f"okg-up-{operation}",
        daemon=True,
    )
    worker.start()
    heartbeat_count = 0
    first_heartbeat_at: str | None = None
    last_heartbeat_at: str | None = None
    while not done.wait(progress_interval_seconds):
        heartbeat_count += 1
        observed_at = _now()
        if first_heartbeat_at is None:
            first_heartbeat_at = observed_at
        last_heartbeat_at = observed_at
        heartbeat: dict[str, Any] = {
            "schema": "okg.deployment_up_progress.v1",
            "operation": operation,
            "elapsed_seconds": round(time.monotonic() - started_monotonic, 3),
            "observed_at": observed_at,
        }
        if artifact_path is not None:
            heartbeat["artifact_bytes"] = _artifact_progress_bytes(artifact_path)
        print(json.dumps(heartbeat, sort_keys=True), file=sys.stderr, flush=True)
    worker.join()
    completed_at = _now()
    duration = time.monotonic() - started_monotonic
    if "error" in outcome:
        raise outcome["error"]
    result = outcome.get("result")
    if not isinstance(result, CommandResult):
        raise TypeError("command runner returned a non-CommandResult value")
    return result, {
        "operation": operation,
        "started_at": started_at,
        "completed_at": completed_at,
        "duration_seconds": duration,
        "heartbeat_count": heartbeat_count,
        "first_heartbeat_at": first_heartbeat_at,
        "last_heartbeat_at": last_heartbeat_at,
        "heartbeat_before_completion": heartbeat_count > 0,
    }


def _run_default_long_command(
    argv: list[str],
    env: dict[str, str],
    *,
    operation: str,
    progress_interval_seconds: float,
    artifact_path: Path | None,
) -> tuple[CommandResult, dict[str, Any]]:
    """Run a real long-lived child on the main thread with cancellation."""
    started_at = _now()
    started_monotonic = time.monotonic()
    process = subprocess.Popen(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        start_new_session=True,
    )
    heartbeat_count = 0
    first_heartbeat_at: str | None = None
    last_heartbeat_at: str | None = None
    try:
        while True:
            try:
                stdout, stderr = process.communicate(
                    timeout=progress_interval_seconds
                )
                break
            except subprocess.TimeoutExpired:
                heartbeat_count += 1
                observed_at = _now()
                if first_heartbeat_at is None:
                    first_heartbeat_at = observed_at
                last_heartbeat_at = observed_at
                heartbeat: dict[str, Any] = {
                    "schema": "okg.deployment_up_progress.v1",
                    "operation": operation,
                    "elapsed_seconds": round(
                        time.monotonic() - started_monotonic,
                        3,
                    ),
                    "observed_at": observed_at,
                }
                if artifact_path is not None:
                    heartbeat["artifact_bytes"] = _artifact_progress_bytes(
                        artifact_path
                    )
                print(
                    json.dumps(heartbeat, sort_keys=True),
                    file=sys.stderr,
                    flush=True,
                )
    except BaseException:
        _terminate_long_process(process)
        raise
    completed_at = _now()
    duration = time.monotonic() - started_monotonic
    return CommandResult(process.returncode, stdout, stderr), {
        "operation": operation,
        "started_at": started_at,
        "completed_at": completed_at,
        "duration_seconds": duration,
        "heartbeat_count": heartbeat_count,
        "first_heartbeat_at": first_heartbeat_at,
        "last_heartbeat_at": last_heartbeat_at,
        "heartbeat_before_completion": heartbeat_count > 0,
    }


def _terminate_long_process(process: subprocess.Popen[str]) -> None:
    """Terminate the entire dump/restore process group after interruption."""
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except (OSError, ProcessLookupError):
        process.terminate()
    try:
        process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            process.kill()
        process.communicate()


def _default_command_runner(argv: list[str], env: dict[str, str]) -> CommandResult:
    proc = subprocess.run(
        argv,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    return CommandResult(proc.returncode, proc.stdout, proc.stderr)


def _failed_command(
    reason: str,
    result: CommandResult,
    *,
    command: list[str] | None = None,
) -> dict[str, Any]:
    payload = result.json_payload()
    return {
        "ok": False,
        "reason": reason,
        "returncode": result.returncode,
        "result": payload,
        "stdout_tail": (result.stdout or "")[-1000:],
        "stderr_tail": (result.stderr or "")[-1000:],
        **({"command": _display_command(command)} if command else {}),
    }


def _unreadable_command(
    reason: str,
    result: CommandResult,
    *,
    command: list[str] | None = None,
) -> dict[str, Any]:
    """Fail-closed result for a zero-exit command whose JSON did not parse.

    A command that exits 0 but emits unparseable output must NOT be coerced to
    an empty-safe payload (which silently reports e.g. "no destructive
    migrations" off a malformed read). It halts the phase with a
    ``could not read <status>`` reason.
    """
    return {
        "ok": False,
        "reason": reason,
        "returncode": result.returncode,
        "unparseable_output": True,
        "stdout_tail": (result.stdout or "")[-1000:],
        "stderr_tail": (result.stderr or "")[-1000:],
        "fix_hint": (
            "the command exited 0 but its JSON output did not parse; rerun it "
            "directly and inspect the raw output"
        ),
        **({"command": _display_command(command)} if command else {}),
    }


def _display_okg_command(args: list[str]) -> str:
    return f"{okg_prefix()} " + " ".join(shlex.quote(part) for part in args)


def _display_command(args: list[str]) -> str:
    return " ".join(shlex.quote(redact(part)) for part in args)


def _pg_dump_incompatible(result: CommandResult) -> bool:
    output = f"{result.stdout}\n{result.stderr}".lower()
    return "server version:" in output and "pg_dump version:" in output


def _next_phase(phase: str) -> str | None:
    idx = PHASES.index(phase)
    if idx + 1 >= len(PHASES):
        return None
    return PHASES[idx + 1]


# Phases that mutate the deployment runtime (service stop/restart, code +
# dependency sync). Skipping any of these while not in plan-only mode means the
# worker may stay on stale code, so the run can only be PARTIAL.
_MUTATE_PHASES = (
    "stop_services",
    "sync_code",
    "dependency_sync",
    "restart_services",
)

# Map a skipped mutate phase to the flag that re-enables it, so the finishing
# command says exactly what to rerun.
_MUTATE_PHASE_FLAG = {
    "stop_services": "--apply-services",
    "restart_services": "--apply-services",
    "sync_code": "--sync-code",
    "dependency_sync": "--sync-deps",
}


def _skipped_mutate_phases(phase_results: list[dict[str, Any]]) -> list[str]:
    """Return the mutate phases that completed but were SKIPPED (not applied).

    A phase that reports ``skipped``/``planned`` did not mutate the runtime.
    Channel-driven no-ops (e.g. ``sync_code`` on a dev channel that already
    tracks the checkout, or a code revision already at the pin) are genuine
    applies — they are NOT skips and must not downgrade the run to partial.
    """
    skipped: list[str] = []
    for row in phase_results:
        phase = row.get("phase")
        if phase not in _MUTATE_PHASES:
            continue
        if not row.get("ok"):
            continue
        if _is_mutate_skip(phase, row):
            skipped.append(phase)
    return skipped


def _is_mutate_skip(phase: str, row: dict[str, Any]) -> bool:
    if "planned" in row:
        # stop/restart report a planned command when not applied.
        return True
    skipped = row.get("skipped")
    if not isinstance(skipped, str):
        return False
    # Distinguish an operator opt-out (worker left stale) from a genuine
    # channel-driven no-op apply (dev channel / already-at-pin).
    benign = (
        "dev channel tracks the current checkout",
    )
    if any(token in skipped for token in benign):
        return False
    return True


def _finishing_command(ctx: UpgradeContext, skipped_mutate: list[str]) -> str:
    """The exact command that applies the skipped mutate phases."""
    flags = {
        _MUTATE_PHASE_FLAG[phase]
        for phase in skipped_mutate
        if phase in _MUTATE_PHASE_FLAG
    }
    return _display_recovery_command(
        ctx,
        _recovery_command_parts(
            ctx,
            apply_services=(
                "--apply-services" in flags or ctx.options.apply_services
            ),
            sync_code="--sync-code" in flags or ctx.options.sync_code,
            sync_deps="--sync-deps" in flags or ctx.options.sync_deps,
            plan_only=False,
        ),
    )


def _next_command(ctx: UpgradeContext) -> str:
    return _display_recovery_command(ctx, _recovery_command_parts(ctx))


def _recovery_actions(ctx: UpgradeContext) -> list[dict[str, Any]]:
    """Return machine-readable recovery without implying provider rollback."""
    actions: list[dict[str, Any]] = [
        {
            "kind": "resume",
            "command": _next_command(ctx),
            "replays_uncertain_provider_effect": False,
        }
    ]
    backup = ctx.data.get("backup_result")
    if isinstance(backup, Mapping) and backup.get("ok") is True:
        actions.append({
            "kind": "database_restore",
            "archive": backup.get("backup_path"),
            "archive_digest": backup.get("backup_digest"),
            "verified": (
                backup.get("restore_verified") is True
                or backup.get("mutation_authorized") is True
            ),
            "scope": "database_only",
        })
    plan = ctx.data.get("safety_plan")
    external = plan.get("external_effects") if isinstance(plan, Mapping) else None
    if isinstance(external, Mapping) and external.get("present") is True:
        actions.append({
            "kind": "provider_manual_reconciliation",
            "phase": "smoke_publish",
            "sources": list(external.get("sources") or []),
            "provider_effects_reversible": False,
            "automatic_replay_authorized": False,
            "instruction": (
                "inspect provider-side receipts before retrying smoke_publish; "
                "a database restore does not undo or authorize replay of these calls"
            ),
        })
    return actions


def _resume_after_reclaim_command(ctx: UpgradeContext) -> str:
    parts = _recovery_command_parts(ctx)
    if "--no-resume" in parts:
        parts[parts.index("--no-resume")] = "--resume"
    if "--reset-state" in parts:
        parts.remove("--reset-state")
    return _display_recovery_command(ctx, parts)


def _backup_verify_failure_requires_reset(payload: Mapping[str, Any]) -> bool:
    phases = payload.get("phases")
    if not isinstance(phases, list):
        return False
    latest_backup = next(
        (
            row
            for row in reversed(phases)
            if isinstance(row, Mapping) and row.get("phase") == "backup"
        ),
        None,
    )
    return isinstance(latest_backup, Mapping) and "--reset-state" in str(
        latest_backup.get("fix_hint") or ""
    )


def _backup_verify_next_command(
    ctx: UpgradeContext,
    *,
    reset_state: bool = False,
) -> str:
    parts = [
        _okg_executable(),
        "deployment",
        "backup",
        "verify",
        ctx.options.deployment,
        # renamed-flag compat: the operator may run this on a pinned
        # pre-rename checkout; flip to "--target" when the aliases go.
        COMPAT_TARGET_FLAG,
        ctx.options.profile,
        "--state-file",
        str(ctx.state_file),
        "--backup-dir",
        str(ctx.backup_dir),
        "--backup-keep",
        str(ctx.options.backup_keep),
    ]
    if ctx.options.dsn is not None:
        parts.extend(["--dsn", "__OKG_DEPLOYMENT_UP_DSN__"])
    if reset_state:
        parts.append("--reset-state")
    return _display_recovery_command(ctx, parts)


def _recovery_command_parts(
    ctx: UpgradeContext,
    *,
    apply_services: bool | None = None,
    sync_code: bool | None = None,
    sync_deps: bool | None = None,
    plan_only: bool | None = None,
) -> list[str]:
    """Preserve the authority and lifecycle policy of an upgrade invocation."""
    apply_services = (
        ctx.options.apply_services if apply_services is None else apply_services
    )
    sync_code = ctx.options.sync_code if sync_code is None else sync_code
    sync_deps = ctx.options.sync_deps if sync_deps is None else sync_deps
    plan_only = ctx.options.plan_only if plan_only is None else plan_only
    parts = [
        _okg_executable(),
        "deployment",
        "up",
        ctx.options.deployment,
        # renamed-flag compat: a failed run can leave the checkout on a pinned
        # pre-rename revision, where only the old spellings parse. Flip to
        # "--target"/"--checks" when the aliases are removed.
        COMPAT_TARGET_FLAG,
        ctx.options.profile,
        COMPAT_UP_CHECKS_FLAG,
        ctx.options.readiness_profile,
    ]
    if ctx.options.dsn is not None:
        parts.extend(["--dsn", "__OKG_DEPLOYMENT_UP_DSN__"])
    if ctx.options.state_file is not None:
        parts.extend(["--state-file", str(ctx.options.state_file)])
    if ctx.options.backup_dir is not None:
        parts.extend(["--backup-dir", str(ctx.options.backup_dir)])
    if ctx.options.lifecycle_plan is not None:
        parts.extend(["--lifecycle-plan", str(ctx.options.lifecycle_plan)])
    if ctx.options.verified_backup_state is not None:
        parts.extend([
            "--verified-backup-state",
            str(ctx.options.verified_backup_state),
        ])
    if ctx.options.verified_backup_source_revision is not None:
        parts.extend([
            "--verified-backup-source-revision",
            ctx.options.verified_backup_source_revision,
        ])
    if ctx.options.precheckout_target_revision is not None:
        parts.extend(
            [
                "--precheckout-target-revision",
                ctx.options.precheckout_target_revision,
            ]
        )
    if ctx.options.confirm_safety_plan is not None:
        parts.extend([
            "--confirm-safety-plan",
            ctx.options.confirm_safety_plan,
        ])
    parts.append("--apply-services" if apply_services else "--no-apply-services")
    parts.append("--sync-code" if sync_code else "--no-sync-code")
    parts.append("--sync-deps" if sync_deps else "--no-sync-deps")
    parts.append("--resume" if ctx.options.resume else "--no-resume")
    if ctx.options.reset_state:
        parts.append("--reset-state")
    if ctx.options.runtime_fence_owner:
        parts.extend(["--runtime-fence-owner", ctx.options.runtime_fence_owner])
    if ctx.options.skip_backup:
        parts.append("--skip-backup")
    if ctx.options.skip_migrate:
        parts.append("--skip-migrate")
    if ctx.options.skip_destructive_migrations:
        parts.append("--skip-destructive-migrations")
    if ctx.options.confirm_destructive_migrations:
        parts.append("--confirm-destructive-migrations")
    if ctx.options.include:
        parts.extend(["--include", *ctx.options.include])
    if plan_only:
        parts.append("--plan")
    if ctx.options.stop_after is not None:
        parts.extend(["--stop-after", ctx.options.stop_after])
    return parts


def _display_recovery_command(ctx: UpgradeContext, parts: list[str]) -> str:
    """Render an executable, fail-closed command without embedding a password."""
    dsn_env = _recovery_dsn_env_name(ctx)
    # renamed-flag compat: the parts use old spellings on purpose (see
    # _recovery_command_parts), so silence the rename warning they would
    # trigger on new code. Drop this prefix when the aliases are removed.
    rendered: list[str] = [
        f"{key}={value}" for key, value in RENAMED_FLAG_COMPAT_ENV.items()
    ]
    for part in parts:
        if part == "__OKG_DEPLOYMENT_UP_DSN__":
            rendered.append(
                f'"${{{dsn_env}:?export {dsn_env} to the original explicit target}}"'
            )
        else:
            rendered.append(shlex.quote(redact(part)))
    return " ".join(rendered)


def _recovery_dsn_env_name(ctx: UpgradeContext) -> str:
    dsn = ctx.options.dsn
    if dsn is not None:
        candidates = sorted(
            name
            for name, value in os.environ.items()
            if value == dsn and re.fullmatch(r"[A-Z][A-Z0-9_]*DSN", name)
        )
        if candidates:
            return candidates[0]
    return "OKG_DEPLOYMENT_UP_DSN"


def _okg_executable() -> str:
    """Use the current virtualenv's CLI for executable recovery commands."""
    candidate = Path(sys.executable).parent / "okg"
    return str(candidate) if candidate.exists() else "okg"


def _write_state(path: Path, state: dict[str, Any]) -> None:
    from okg.substrate.runtime_dirs import mark_okg_runtime_ancestor

    mark_okg_runtime_ancestor(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    safe_state = redact_value(state)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(safe_state, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _smoke_settled_caught_up(ctx: UpgradeContext) -> bool:
    """True iff the smoke publish settled as a validated caught-up no-op.

    Re-derived from the (resume-rehydrated) smoke publish payload rather than a
    transient proof dict, so the head-generation binding below survives a crash
    between the durable readiness row and finalization.
    """
    from okg.substrate.publish_proof import is_caught_up_noop

    return is_caught_up_noop(ctx.data.get("smoke_publish"))


def _bound_smoke_generation(
    ctx: UpgradeContext,
    smoke_generation: str | int | None,
    readiness_generation: str | int | None,
) -> str | int | None:
    """Resolve the generation the smoke proof settled the deployment at.

    Normally the smoke commit mints a head generation and that is the answer. On
    a continuously published deployment whose worker already made the graph
    current, a routine smoke instead settles as a validated caught-up no-op — a
    ``no_source_work`` skip (no generation minted) or a committed zero-delta
    authoritative replay — so it advanced no distinct content but PROVED the
    current published head is current. In that case bind the proof to the head
    (the readiness generation) so readiness / MCP / budget align on it. Fail
    closed: the binding is reached only after ``_phase_smoke_publish`` accepted
    the healthy caught-up no-op; a wedged smoke fails the phase before here.
    """
    if smoke_generation is not None:
        return smoke_generation
    if readiness_generation is not None and _smoke_settled_caught_up(ctx):
        return readiness_generation
    return None


def _smoke_generation(payload: Any) -> str | int | None:
    if not isinstance(payload, dict):
        return None
    for key in ("generation_id", "target_generation_id"):
        if payload.get(key):
            return payload[key]
    publish = payload.get("publish")
    if isinstance(publish, dict):
        return publish.get("generation_id") or publish.get("target_generation_id")
    publishes = payload.get("publishes")
    if isinstance(publishes, list):
        for item in reversed(publishes):
            if isinstance(item, dict):
                generation = (
                    item.get("generation_id")
                    or item.get("target_generation_id")
                )
                if generation:
                    return generation
    return None


def _readiness_generation(payload: Any) -> str | int | None:
    if not isinstance(payload, dict):
        return None
    generation = payload.get("generation")
    if isinstance(generation, dict):
        value = generation.get("generation_id") or generation.get(
            "target_generation_id"
        )
        if value:
            return value
    for key in ("generation_id", "target_generation_id"):
        if payload.get(key):
            return payload[key]
    read_authority = payload.get("read_authority")
    if isinstance(read_authority, dict):
        return read_authority.get("generation_id")
    return None


def load_fleet_dispositions(path: Path | str) -> dict[str, FleetDisposition]:
    """Load explicit retired/waived fleet dispositions from JSON or YAML."""

    raw_path = Path(path).expanduser()
    if not raw_path.is_file():
        raise SubstrateError(
            "fleet disposition file not found",
            data={"path": str(raw_path)},
            fix_hint="pass a readable JSON/YAML file to --fleet-dispositions",
        )
    try:
        raw = fastyaml.safe_load(raw_path.read_text()) or {}
    except Exception as exc:  # noqa: BLE001 - operator file parse surface.
        raise SubstrateError(
            "fleet disposition file could not be parsed",
            data={"path": str(raw_path), "error": str(exc)},
            fix_hint="write valid JSON or YAML",
        ) from exc
    rows = raw.get("deployments") if isinstance(raw, dict) else raw
    if isinstance(rows, dict):
        iterable = [
            {"deployment": deployment, **(value or {})}
            for deployment, value in rows.items()
        ]
    elif isinstance(rows, list):
        iterable = rows
    else:
        raise SubstrateError(
            "fleet disposition file must contain a deployments mapping or list",
            data={"path": str(raw_path)},
            fix_hint="use deployments.<name>.status/reason entries",
        )
    out: dict[str, FleetDisposition] = {}
    for row in iterable:
        if not isinstance(row, dict):
            raise SubstrateError(
                "fleet disposition row must be a mapping",
                data={"path": str(raw_path), "row": row},
            )
        deployment = str(row.get("deployment") or "").strip()
        status = str(row.get("status") or "").strip().lower()
        reason = str(row.get("reason") or "").strip()
        evidence = str(row.get("evidence") or "").strip() or None
        if not deployment:
            raise SubstrateError(
                "fleet disposition row requires deployment",
                data={"path": str(raw_path), "row": row},
            )
        disposition = FleetDisposition(
            status=status,
            reason=reason,
            evidence=evidence,
        )
        _disposed_fleet_row(deployment, disposition)
        out[deployment] = disposition
    return out
