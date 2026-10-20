from copy import deepcopy
from pathlib import Path

import pytest

from scripts.check_deployment_backup_budget import (
    ARCHIVE_CHECKS,
    evaluate_backup_budget,
    main,
)


GIB = 1024**3
GENERATION = "okg:okg-workspace:branch:default:gen:test:802"


def _state() -> dict[str, object]:
    return {
        "deployment": "okg-workspace",
        "completed": True,
        "total_duration_seconds": 1500.0,
        "smoke_generation_id": GENERATION,
        "readiness_generation_id": GENERATION,
        "phase_results": [
            {
                "phase": "backup",
                "ok": True,
                "verification_mode": "routine_restore_drill_reused",
                "source_database_size_bytes": 68 * GIB,
                "archive_checks": {name: True for name in ARCHIVE_CHECKS},
                "dump_duration_seconds": 900.0,
                "duration_seconds": 1000.0,
                "budgets_seconds": {
                    "dump": 1200.0,
                    "backup": 1800.0,
                    "total": 2400.0,
                },
                "progress": {
                    "pg_dump": {
                        "heartbeat_count": 2,
                        "first_heartbeat_at": "2026-07-19T20:00:30+00:00",
                        "completed_at": "2026-07-19T20:15:00+00:00",
                        "heartbeat_before_completion": True,
                    }
                },
            },
            {
                "phase": "migrate",
                "ok": True,
                "backup_revalidation": {
                    "ok": True,
                    "verification_mode": "routine_restore_drill_reused",
                    "candidate_plan_stable": True,
                    "policy_stable": True,
                    "archive_revalidated": True,
                    "database_state_revalidated": True,
                    "receipt_revalidated": True,
                },
            },
        ],
    }


def _routine_fast_state() -> dict[str, object]:
    state = _state()
    state["total_duration_seconds"] = 90.0
    backup = _backup(state)
    backup.update({
        "verification_mode": "no_physical_rollback_required",
        "physical_backup_started": False,
        "restore_started": False,
        "backup_tool_child_processes_started": 0,
        "backup_tool_validation": "receipt_bound_executable_identity_without_child",
        "duration_seconds": 12.0,
        "budgets_seconds": {"backup": 60.0, "total": 600.0},
        "progress": {},
    })
    migrate = state["phase_results"][1]  # type: ignore[index]
    migrate["backup_revalidation"] = {  # type: ignore[index]
        "ok": True,
        "verification_mode": "no_physical_rollback_required",
        "effect_digest_stable": True,
        "policy_stable": True,
        "disaster_recovery_revalidated": True,
        "physical_archive_required": False,
        "backup_tool_child_processes_started": 0,
        "backup_tool_validation": "receipt_bound_executable_identity_without_child",
    }
    state["phase_results"].append({  # type: ignore[union-attr]
        "phase": "attribute_read_model",
        "ok": True,
        "duration_seconds": 0.4,
        "result": {
            "ok": True,
            "reason": "already_repaired",
            "integrity_receipt": {"reason": "already_repaired"},
        },
    })
    return state


def _evaluate(state: dict[str, object], **changes: object) -> dict[str, object]:
    options = {
        "deployment": "okg-workspace",
        "min_database_bytes": 60 * GIB,
        "baseline_backup_seconds": 3302.0,
        "max_ratio": 0.60,
    }
    options.update(changes)
    return evaluate_backup_budget(state, **options)


def _backup(state: dict[str, object]) -> dict[str, object]:
    return state["phase_results"][0]  # type: ignore[index,return-value]


def _dogfood() -> dict[str, object]:
    return {
        "trust_level": "ready",
        "generation": {
            "generation_id": GENERATION,
            "count_integrity": {"ok": True, "status": "clean"},
        },
        "source_freshness": {"ok": True},
        "runtime_worker": {"ok": True},
        "queues": {
            "substrate": {
                "ok": True,
                "active_queue_depth": 0,
                "dlq_count": 0,
            }
        },
        "mcp": {"ok": True, "pinned_generation_id": GENERATION},
        "read_authority": {
            "mcp_graph_read": True,
            "generation_id": GENERATION,
        },
    }


def _readiness() -> dict[str, object]:
    return {
        "ok": True,
        "generation": {"generation_id": GENERATION},
        "doctor": {"ok": True},
    }


def _set_path(value: dict[str, object], path: str, replacement: object) -> None:
    parts = path.split(".")
    cursor = value
    for part in parts[:-1]:
        cursor = cursor[part]  # type: ignore[assignment]
    cursor[parts[-1]] = replacement


def test_representative_routine_run_passes_budget_gate() -> None:
    report = _evaluate(
        _state(),
        live_generation_id=GENERATION,
        dogfood_status=_dogfood(),
        readiness=_readiness(),
    )

    assert report["ok"] is True
    assert report["errors"] == []


def test_no_physical_rollback_and_attribute_fast_path_pass_routine_gate() -> None:
    report = _evaluate(
        _routine_fast_state(),
        baseline_backup_seconds=None,
        max_ratio=None,
        require_no_physical_rollback=True,
        require_attribute_already_repaired=True,
        max_backup_seconds=60,
        max_attribute_read_model_seconds=30,
        max_total_seconds=600,
    )

    assert report["ok"] is True, report
    assert report["observed"]["attribute_read_model_reason"] == "already_repaired"


def test_resumed_budget_gate_uses_latest_duplicate_phase_attempt() -> None:
    state = _routine_fast_state()
    latest_backup = deepcopy(_backup(state))
    _backup(state).update({
        "ok": False,
        "duration_seconds": 61.0,
        "source_database_size_bytes": None,
    })
    state["phase_results"].append(latest_backup)  # type: ignore[union-attr]

    report = _evaluate(
        state,
        baseline_backup_seconds=None,
        max_ratio=None,
        require_no_physical_rollback=True,
        require_attribute_already_repaired=True,
        max_backup_seconds=60,
        max_attribute_read_model_seconds=30,
        max_total_seconds=600,
    )

    assert report["ok"] is True, report
    assert report["observed"]["durations_seconds"]["backup"] == 12.0


def test_routine_gate_refuses_physical_child_or_slow_attribute_phase() -> None:
    state = _routine_fast_state()
    _backup(state)["physical_backup_started"] = True
    state["phase_results"][2]["duration_seconds"] = 31.0  # type: ignore[index]

    report = _evaluate(
        state,
        baseline_backup_seconds=None,
        max_ratio=None,
        require_no_physical_rollback=True,
        require_attribute_already_repaired=True,
        max_backup_seconds=60,
        max_attribute_read_model_seconds=30,
        max_total_seconds=600,
    )

    assert "routine_pg_dump_started" in report["errors"]
    assert "explicit_budget_exceeded:attribute_read_model" in report["errors"]


def test_routine_gate_requires_receipt_bound_zero_child_tool_proof() -> None:
    state = _routine_fast_state()
    _backup(state)["backup_tool_child_processes_started"] = 1
    state["phase_results"][1]["backup_revalidation"][  # type: ignore[index]
        "backup_tool_validation"
    ] = "version_probe"

    report = _evaluate(
        state,
        baseline_backup_seconds=None,
        max_ratio=None,
        require_no_physical_rollback=True,
    )

    assert "routine_backup_tool_child_started" in report["errors"]
    assert "mutation_backup_tool_identity_not_receipt_bound" in report["errors"]


def test_attribute_only_gate_does_not_require_dump_evidence() -> None:
    report = _evaluate(
        _routine_fast_state(),
        baseline_backup_seconds=None,
        max_ratio=None,
        require_attribute_already_repaired=True,
        max_attribute_read_model_seconds=30,
    )

    assert report["ok"] is True, report


def test_incomplete_upgrade_fails() -> None:
    state = _state()
    state["completed"] = False

    assert "upgrade_not_completed" in _evaluate(state)["errors"]


def test_database_below_representative_floor_fails() -> None:
    state = _state()
    _backup(state)["source_database_size_bytes"] = 59 * GIB

    assert "database_size_below_representative_floor" in _evaluate(state)["errors"]


def test_non_reuse_decision_fails() -> None:
    state = _state()
    _backup(state)["verification_mode"] = "current_full_restore"

    assert "routine_restore_drill_not_reused" in _evaluate(state)["errors"]


def test_mutation_time_full_restore_fallback_fails_budget_gate() -> None:
    state = _state()
    revalidation = state["phase_results"][1]["backup_revalidation"]  # type: ignore[index]
    revalidation.update({  # type: ignore[union-attr]
        "verification_mode": "current_full_restore",
        "candidate_plan_stable": False,
        "restore": {"restore_current_archive": True},
    })
    state["total_duration_seconds"] = 3500.0
    _backup(state)["budgets_seconds"]["total"] = 3600.0  # type: ignore[index]

    errors = _evaluate(state)["errors"]

    assert "mutation_routine_restore_drill_not_reused" in errors
    assert "mutation_current_archive_restore_fallback_observed" in errors


@pytest.mark.parametrize("check", ARCHIVE_CHECKS)
def test_each_current_archive_check_is_required(check: str) -> None:
    state = _state()
    _backup(state)["archive_checks"][check] = False  # type: ignore[index]

    assert f"archive_check_failed:{check}" in _evaluate(state)["errors"]


@pytest.mark.parametrize(
    ("duration_field", "budget_name", "error"),
    [
        ("dump_duration_seconds", "dump", "budget_exceeded:dump"),
        ("duration_seconds", "backup", "budget_exceeded:backup"),
        ("total_duration_seconds", "total", "budget_exceeded:total"),
    ],
)
def test_each_declared_duration_budget_is_enforced(
    duration_field: str, budget_name: str, error: str
) -> None:
    state = _state()
    budgets = _backup(state)["budgets_seconds"]
    if duration_field == "total_duration_seconds":
        state[duration_field] = budgets[budget_name] + 1  # type: ignore[index,operator]
    else:
        _backup(state)[duration_field] = budgets[budget_name] + 1  # type: ignore[index,operator]

    assert error in _evaluate(state)["errors"]


@pytest.mark.parametrize("non_finite", [float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize(
    ("surface", "name"),
    [
        ("duration", "dump"),
        ("duration", "backup"),
        ("duration", "total"),
        ("budget", "dump"),
        ("budget", "backup"),
        ("budget", "total"),
    ],
)
def test_non_finite_durations_and_budgets_fail_closed(
    surface: str, name: str, non_finite: float
) -> None:
    state = _state()
    if surface == "budget":
        _backup(state)["budgets_seconds"][name] = non_finite  # type: ignore[index]
    elif name == "total":
        state["total_duration_seconds"] = non_finite
    else:
        field = "dump_duration_seconds" if name == "dump" else "duration_seconds"
        _backup(state)[field] = non_finite

    assert _evaluate(state)["ok"] is False


@pytest.mark.parametrize("baseline", [float("nan"), float("inf"), 0.0, -1.0])
def test_invalid_baseline_fails_closed(baseline: float) -> None:
    report = _evaluate(_state(), baseline_backup_seconds=baseline)

    assert "baseline_backup_seconds_invalid" in report["errors"]


@pytest.mark.parametrize(
    "ratio", [float("nan"), float("inf"), 0.0, -0.1, 1.0, 2.0]
)
def test_invalid_reduction_ratio_fails_closed(ratio: float) -> None:
    report = _evaluate(_state(), max_ratio=ratio)

    assert "max_ratio_invalid" in report["errors"]


def test_missing_precompletion_heartbeat_fails() -> None:
    state = _state()
    progress = _backup(state)["progress"]["pg_dump"]  # type: ignore[index]
    progress["heartbeat_before_completion"] = False

    assert "pg_dump_heartbeat_not_observed_before_completion" in _evaluate(state)["errors"]


@pytest.mark.parametrize(
    ("field", "error"),
    [
        ("smoke_generation_id", "smoke_generation_missing"),
        ("readiness_generation_id", "readiness_generation_missing"),
    ],
)
def test_generation_evidence_is_required(field: str, error: str) -> None:
    state = _state()
    state[field] = None

    assert error in _evaluate(state)["errors"]


def test_readiness_and_live_generation_must_match_smoke() -> None:
    state = _state()
    state["readiness_generation_id"] = "different"
    errors = _evaluate(state, live_generation_id="also-different")["errors"]

    assert "readiness_generation_mismatch" in errors
    assert "live_generation_mismatch" in errors


@pytest.mark.parametrize(
    ("surface", "path", "replacement", "error"),
    [
        ("dogfood", "trust_level", "stale", "dogfood_trust_ready"),
        (
            "dogfood",
            "generation.generation_id",
            "different",
            "dogfood_generation_exact",
        ),
        (
            "dogfood",
            "generation.count_integrity.ok",
            False,
            "dogfood_count_integrity_clean",
        ),
        (
            "dogfood",
            "source_freshness.ok",
            False,
            "dogfood_sources_fresh",
        ),
        (
            "dogfood",
            "runtime_worker.ok",
            False,
            "dogfood_worker_healthy",
        ),
        (
            "dogfood",
            "queues.substrate.active_queue_depth",
            1,
            "dogfood_substrate_queue_healthy",
        ),
        (
            "dogfood",
            "queues.substrate.dlq_count",
            1,
            "dogfood_substrate_queue_healthy",
        ),
        (
            "dogfood",
            "mcp.pinned_generation_id",
            "different",
            "dogfood_mcp_exact",
        ),
        (
            "dogfood",
            "read_authority.mcp_graph_read",
            False,
            "dogfood_mcp_exact",
        ),
        ("readiness", "ok", False, "deployment_readiness_failed"),
        (
            "readiness",
            "generation.generation_id",
            "different",
            "deployment_readiness_generation_mismatch",
        ),
        ("readiness", "doctor.ok", False, "deployment_doctor_failed"),
    ],
)
def test_each_live_health_and_generation_guard_fails_closed(
    surface: str, path: str, replacement: object, error: str
) -> None:
    dogfood = deepcopy(_dogfood())
    readiness = deepcopy(_readiness())
    target = dogfood if surface == "dogfood" else readiness
    _set_path(target, path, replacement)

    report = _evaluate(
        _state(),
        live_generation_id=GENERATION,
        dogfood_status=dogfood,
        readiness=readiness,
    )

    assert error in report["errors"]


def test_cli_returns_nonzero_for_budget_failure(tmp_path: Path) -> None:
    state = _state()
    _backup(state)["duration_seconds"] = 2000.0
    state_path = tmp_path / "state.json"
    state_path.write_text(__import__("json").dumps(state), encoding="utf-8")

    assert main([
        "--deployment", "okg-workspace",
        "--state", str(state_path),
        "--min-database-bytes", str(60 * GIB),
        "--baseline-backup-seconds", "3302",
        "--max-ratio", "0.60",
    ]) == 1
