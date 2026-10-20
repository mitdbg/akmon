#!/usr/bin/env python3
"""Fail-closed performance and health gate for a large deployment-up run."""
from __future__ import annotations

import argparse
import json
import math
import subprocess
from pathlib import Path
from typing import Any, Mapping


ARCHIVE_CHECKS = (
    "size_ok",
    "private_mode",
    "current_owner",
    "digest_ok",
    "toc_readable",
)


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _phase(state: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    # Resumed deployment state is append-only: a retried phase follows its
    # earlier failed or interrupted attempt. Budget proof must therefore use
    # the newest observation, not stale work from the first attempt.
    for row in reversed(tuple(state.get("phase_results") or ())):
        if isinstance(row, Mapping) and row.get("phase") == name:
            return row
    return {}


def evaluate_backup_budget(
    state: Mapping[str, Any],
    *,
    deployment: str,
    min_database_bytes: int,
    baseline_backup_seconds: float | None = None,
    max_ratio: float | None = None,
    require_no_physical_rollback: bool = False,
    require_attribute_already_repaired: bool = False,
    max_backup_seconds: float | None = None,
    max_attribute_read_model_seconds: float | None = None,
    max_total_seconds: float | None = None,
    live_generation_id: str | None = None,
    dogfood_status: Mapping[str, Any] | None = None,
    readiness: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate state evidence; return a stable JSON-ready report."""
    errors: list[str] = []
    attribute_only = (
        require_attribute_already_repaired
        and not require_no_physical_rollback
        and baseline_backup_seconds is None
        and max_ratio is None
        and max_backup_seconds is None
        and max_total_seconds is None
    )
    backup = _phase(state, "backup")
    migrate = _phase(state, "migrate")
    attribute = _phase(state, "attribute_read_model")

    if state.get("deployment") != deployment:
        errors.append("deployment_mismatch")
    if state.get("completed") is not True:
        errors.append("upgrade_not_completed")
    if not backup or backup.get("ok") is not True:
        errors.append("passing_backup_phase_missing")
    expected_mode = (
        "no_physical_rollback_required"
        if require_no_physical_rollback
        else "routine_restore_drill_reused"
    )
    if not attribute_only and backup.get("verification_mode") != expected_mode:
        errors.append(
            "no_physical_rollback_decision_missing"
            if require_no_physical_rollback
            else "routine_restore_drill_not_reused"
        )
    revalidation = migrate.get("backup_revalidation")
    if not isinstance(revalidation, Mapping):
        revalidation = {}
        if not attribute_only:
            errors.append("mutation_backup_revalidation_missing")
    if attribute_only:
        pass
    elif require_no_physical_rollback:
        if revalidation.get("verification_mode") != expected_mode:
            errors.append("mutation_no_physical_rollback_decision_missing")
        for check in (
            "effect_digest_stable",
            "policy_stable",
            "disaster_recovery_revalidated",
        ):
            if revalidation.get(check) is not True:
                errors.append(f"mutation_backup_check_failed:{check}")
        if revalidation.get("physical_archive_required") is not False:
            errors.append("mutation_physical_archive_unexpected")
        if backup.get("physical_backup_started") is not False:
            errors.append("routine_pg_dump_started")
        if backup.get("restore_started") is not False:
            errors.append("routine_pg_restore_started")
        if backup.get("backup_tool_child_processes_started") != 0:
            errors.append("routine_backup_tool_child_started")
        if (
            backup.get("backup_tool_validation")
            != "receipt_bound_executable_identity_without_child"
        ):
            errors.append("routine_backup_tool_identity_not_receipt_bound")
        if revalidation.get("backup_tool_child_processes_started") != 0:
            errors.append("mutation_backup_tool_child_started")
        if (
            revalidation.get("backup_tool_validation")
            != "receipt_bound_executable_identity_without_child"
        ):
            errors.append("mutation_backup_tool_identity_not_receipt_bound")
    else:
        if revalidation.get("verification_mode") != expected_mode:
            errors.append("mutation_routine_restore_drill_not_reused")
        if revalidation.get("candidate_plan_stable") is not True:
            errors.append("mutation_candidate_plan_not_stable")
        if revalidation.get("policy_stable") is not True:
            errors.append("mutation_backup_policy_not_stable")
        for check in (
            "archive_revalidated",
            "database_state_revalidated",
            "receipt_revalidated",
        ):
            if revalidation.get(check) is not True:
                errors.append(f"mutation_backup_check_failed:{check}")
        restore = revalidation.get("restore")
        if not isinstance(restore, Mapping):
            restore = {}
        if (
            revalidation.get("restore_current_archive") is True
            or restore.get("restore_current_archive") is True
        ):
            errors.append("mutation_current_archive_restore_fallback_observed")

    database_bytes = _number(backup.get("source_database_size_bytes"))
    if database_bytes is None or database_bytes < min_database_bytes:
        errors.append("database_size_below_representative_floor")

    if not require_no_physical_rollback and not attribute_only:
        archive_checks = backup.get("archive_checks")
        if not isinstance(archive_checks, Mapping):
            archive_checks = {}
        for check in ARCHIVE_CHECKS:
            if archive_checks.get(check) is not True:
                errors.append(f"archive_check_failed:{check}")

    budgets = backup.get("budgets_seconds")
    if not isinstance(budgets, Mapping):
        budgets = {}
    observed = {
        "dump": _number(backup.get("dump_duration_seconds")),
        "backup": _number(backup.get("duration_seconds")),
        "total": _number(state.get("total_duration_seconds")),
    }
    declared: dict[str, float | None] = {
        name: _number(budgets.get(name)) for name in observed
    }
    budget_names = (
        ()
        if attribute_only
        else (("backup", "total") if require_no_physical_rollback else tuple(observed))
    )
    for name in budget_names:
        if declared[name] is None or declared[name] <= 0:
            errors.append(f"budget_missing_or_invalid:{name}")
        if observed[name] is None or observed[name] < 0:
            errors.append(f"duration_missing_or_invalid:{name}")
        elif declared[name] is not None and observed[name] > declared[name]:
            errors.append(f"budget_exceeded:{name}")

    baseline = _number(baseline_backup_seconds)
    ratio = _number(max_ratio)
    if not require_no_physical_rollback and not attribute_only:
        if baseline is None or baseline <= 0:
            errors.append("baseline_backup_seconds_invalid")
        if ratio is None or not 0 < ratio < 1:
            errors.append("max_ratio_invalid")
    material_limit = (
        baseline * ratio
        if baseline is not None and baseline > 0
        and ratio is not None and 0 < ratio < 1
        else None
    )
    if (
        material_limit is not None
        and declared["backup"] is not None
        and declared["backup"] > material_limit
    ):
        errors.append("declared_backup_budget_exceeds_reduction_target")
    if (
        material_limit is not None
        and observed["backup"] is not None
        and observed["backup"] > material_limit
    ):
        errors.append("measured_backup_exceeds_reduction_target")

    if not require_no_physical_rollback and not attribute_only:
        progress = backup.get("progress")
        if not isinstance(progress, Mapping):
            progress = {}
        dump_progress = progress.get("pg_dump")
        if not isinstance(dump_progress, Mapping):
            dump_progress = {}
        heartbeat_count = dump_progress.get("heartbeat_count")
        if (
            not isinstance(heartbeat_count, int)
            or isinstance(heartbeat_count, bool)
            or heartbeat_count < 1
        ):
            errors.append("pg_dump_progress_heartbeat_missing")
        if not dump_progress.get("first_heartbeat_at"):
            errors.append("pg_dump_first_heartbeat_timestamp_missing")
        if not dump_progress.get("completed_at"):
            errors.append("pg_dump_completion_timestamp_missing")
        if dump_progress.get("heartbeat_before_completion") is not True:
            errors.append("pg_dump_heartbeat_not_observed_before_completion")

    attribute_duration = _number(attribute.get("duration_seconds"))
    attribute_result = attribute.get("result")
    if not isinstance(attribute_result, Mapping):
        attribute_result = {}
    if require_attribute_already_repaired:
        if attribute.get("ok") is not True:
            errors.append("attribute_read_model_phase_missing")
        if attribute_result.get("reason") != "already_repaired":
            errors.append("attribute_read_model_not_already_repaired")
    explicit_limits = {
        "backup": _number(max_backup_seconds),
        "attribute_read_model": _number(max_attribute_read_model_seconds),
        "total": _number(max_total_seconds),
    }
    explicit_observed = {
        "backup": observed["backup"],
        "attribute_read_model": attribute_duration,
        "total": observed["total"],
    }
    for name, limit in explicit_limits.items():
        if limit is None:
            continue
        if limit <= 0:
            errors.append(f"explicit_budget_invalid:{name}")
        elif explicit_observed[name] is None:
            errors.append(f"duration_missing_or_invalid:{name}")
        elif explicit_observed[name] > limit:
            errors.append(f"explicit_budget_exceeded:{name}")

    smoke_generation = state.get("smoke_generation_id")
    readiness_generation = state.get("readiness_generation_id")
    if not isinstance(smoke_generation, str) or not smoke_generation:
        errors.append("smoke_generation_missing")
    if not isinstance(readiness_generation, str) or not readiness_generation:
        errors.append("readiness_generation_missing")
    elif readiness_generation != smoke_generation:
        errors.append("readiness_generation_mismatch")
    if live_generation_id is not None and live_generation_id != smoke_generation:
        errors.append("live_generation_mismatch")

    if dogfood_status is not None:
        generation = dogfood_status.get("generation") or {}
        count_integrity = generation.get("count_integrity") or {}
        substrate_queue = (dogfood_status.get("queues") or {}).get("substrate") or {}
        mcp = dogfood_status.get("mcp") or {}
        read_authority = dogfood_status.get("read_authority") or {}
        checks = {
            "dogfood_trust_ready": dogfood_status.get("trust_level") == "ready",
            "dogfood_generation_exact": generation.get("generation_id") == smoke_generation,
            "dogfood_count_integrity_clean": (
                count_integrity.get("ok") is True
                and count_integrity.get("status") == "clean"
            ),
            "dogfood_sources_fresh": (dogfood_status.get("source_freshness") or {}).get("ok") is True,
            "dogfood_worker_healthy": (dogfood_status.get("runtime_worker") or {}).get("ok") is True,
            "dogfood_substrate_queue_healthy": (
                substrate_queue.get("ok") is True
                and substrate_queue.get("active_queue_depth") == 0
                and substrate_queue.get("dlq_count") == 0
            ),
            "dogfood_mcp_exact": (
                mcp.get("ok") is True
                and mcp.get("pinned_generation_id") == smoke_generation
                and read_authority.get("mcp_graph_read") is True
                and read_authority.get("generation_id") == smoke_generation
            ),
        }
        errors.extend(name for name, passed in checks.items() if not passed)

    if readiness is not None:
        ready_generation = (readiness.get("generation") or {}).get("generation_id")
        if readiness.get("ok") is not True:
            errors.append("deployment_readiness_failed")
        if ready_generation != smoke_generation:
            errors.append("deployment_readiness_generation_mismatch")
        if (readiness.get("doctor") or {}).get("ok") is not True:
            errors.append("deployment_doctor_failed")

    return {
        "schema": "okg.deployment_backup_budget.v1",
        "ok": not errors,
        "deployment": deployment,
        "errors": errors,
        "observed": {
            "database_bytes": database_bytes,
            "durations_seconds": observed,
            "smoke_generation_id": smoke_generation,
            "readiness_generation_id": readiness_generation,
            "live_generation_id": live_generation_id,
            "mutation_verification_mode": revalidation.get("verification_mode"),
            "attribute_read_model_seconds": attribute_duration,
            "attribute_read_model_reason": attribute_result.get("reason"),
        },
        "limits": {
            "min_database_bytes": min_database_bytes,
            "baseline_backup_seconds": baseline,
            "max_ratio": ratio,
            "material_backup_limit_seconds": material_limit,
            "declared_budgets_seconds": declared,
            "explicit_budgets_seconds": explicit_limits,
        },
    }


def _run_json(command: list[str]) -> dict[str, Any]:
    proc = subprocess.run(command, check=False, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"{' '.join(command)} failed ({proc.returncode}): "
            f"{proc.stderr[-1000:]}"
        )
    value = json.loads(proc.stdout)
    if not isinstance(value, dict):
        raise RuntimeError(f"{' '.join(command)} did not emit a JSON object")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--deployment", required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--min-database-bytes", type=int, required=True)
    parser.add_argument("--baseline-backup-seconds", type=float)
    parser.add_argument("--max-ratio", type=float)
    parser.add_argument("--require-no-physical-rollback", action="store_true")
    parser.add_argument("--require-attribute-already-repaired", action="store_true")
    parser.add_argument("--max-backup-seconds", type=float)
    parser.add_argument("--max-attribute-read-model-seconds", type=float)
    parser.add_argument("--max-total-seconds", type=float)
    parser.add_argument("--verify-live-generation", action="store_true")
    args = parser.parse_args(argv)

    try:
        state = json.loads(args.state.read_text(encoding="utf-8"))
        if not isinstance(state, dict):
            raise ValueError("state root is not an object")
        dogfood = readiness = None
        live_generation = None
        if args.verify_live_generation:
            dogfood = _run_json([
                "uv", "run", "okg", "dogfood", "status", "--deployment",
                args.deployment, "--json",
            ])
            readiness = _run_json([
                "uv", "run", "okg", "deployment", "ready", args.deployment,
                "--checks", "dogfood", "--json",
            ])
            live_generation = str(
                (dogfood.get("generation") or {}).get("generation_id") or ""
            )
        report = evaluate_backup_budget(
            state,
            deployment=args.deployment,
            min_database_bytes=args.min_database_bytes,
            baseline_backup_seconds=args.baseline_backup_seconds,
            max_ratio=args.max_ratio,
            require_no_physical_rollback=args.require_no_physical_rollback,
            require_attribute_already_repaired=(
                args.require_attribute_already_repaired
            ),
            max_backup_seconds=args.max_backup_seconds,
            max_attribute_read_model_seconds=(
                args.max_attribute_read_model_seconds
            ),
            max_total_seconds=args.max_total_seconds,
            live_generation_id=live_generation,
            dogfood_status=dogfood,
            readiness=readiness,
        )
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        report = {
            "schema": "okg.deployment_backup_budget.v1",
            "ok": False,
            "deployment": args.deployment,
            "errors": [f"checker_setup_failed:{type(exc).__name__}:{exc}"],
        }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("ok") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
