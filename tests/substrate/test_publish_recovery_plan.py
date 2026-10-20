"""#1791: the read-only publish recovery plan names the right operation.

No database: the three readers are injected, so every state the plan can
report is driven here, including the read failure that must surface as
``ok: false`` rather than as a healthy verdict.
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from okg.substrate import publish_recovery as pr


def _redrive(**overrides: Any) -> dict[str, Any]:
    state = {
        "staged_work_present": False,
        "latest_checkpoint_id": None,
        "latest_checkpoint_status": None,
        "latest_blocked_reason": None,
        "latest_blocked_receipt": {},
        "latest_published_checkpoint_id": None,
        "branch_mutation_token": None,
    }
    state.update(overrides)
    return state


def _history(**overrides: Any) -> dict[str, Any]:
    history = {
        "last_published": None,
        "checkpoint_count_any_branch": 0,
        "legacy_published_generation_id": None,
    }
    history.update(overrides)
    return history


_PUBLISHED = {
    "generation_id": "okg:dep:branch:default:gen:1:abc",
    "recorded_node_count": 3,
    "recorded_edge_count": 1,
    "published_at": "2026-09-11T00:00:00+00:00",
    "predecessor_generation_id": None,
}


def _plan(
    redrive: dict[str, Any],
    history: dict[str, Any],
    *,
    queues: dict[str, Any] | None = None,
    runtime_enabled: bool | None = False,
) -> dict[str, Any]:
    plan = pr.plan_publish_recovery(
        dsn="postgresql://unused",
        deployment="dep",
        runtime_enabled=runtime_enabled,
        read_state=lambda _dsn, _dep: redrive,
        read_history=lambda _dsn, _dep: history,
        read_queues=lambda _dsn: queues
        if queues is not None
        else {"measured": True, "dlq_count": 0, "active_queue_depth": 0},
    )
    json.dumps(plan)
    assert plan["schema"] == pr.PUBLISH_RECOVERY_SCHEMA
    assert plan["deployment"] == "dep"
    return plan


def test_healthy_deployment_needs_no_recovery_and_admits_okg_up() -> None:
    plan = _plan(
        _redrive(
            latest_checkpoint_id=_PUBLISHED["generation_id"],
            latest_checkpoint_status="published",
            latest_published_checkpoint_id=_PUBLISHED["generation_id"],
        ),
        _history(last_published=_PUBLISHED, checkpoint_count_any_branch=1),
    )

    assert plan["ok"] is True
    assert plan["state"] == pr.STATE_HEALTHY
    assert plan["recovery_command"] is None
    assert plan["okg_up_applies"] is True
    assert plan["last_known_good"] == _PUBLISHED
    assert plan["queues_ok"] is True
    assert plan["execution"]["route"] == "inline"


def test_staged_work_names_okg_ingest_and_keeps_last_known_good() -> None:
    plan = _plan(
        _redrive(
            staged_work_present=True,
            latest_checkpoint_id=_PUBLISHED["generation_id"],
            latest_checkpoint_status="published",
            latest_published_checkpoint_id=_PUBLISHED["generation_id"],
        ),
        _history(last_published=_PUBLISHED, checkpoint_count_any_branch=1),
    )

    assert plan["state"] == pr.STATE_STAGED_WORK_UNPUBLISHED
    assert plan["recovery_command"] == (
        "uv run okg ingest --deployment dep --json"
    )
    assert plan["diagnose_command"] == plan["recovery_command"]
    assert plan["okg_up_applies"] is False
    assert plan["staged_source_work"] is True
    assert plan["last_known_good"]["generation_id"] == _PUBLISHED["generation_id"]
    assert "no_source_work" in plan["explanation"]


def test_staged_work_on_a_first_install_still_names_okg_ingest() -> None:
    """Never-published plus staged work is an ingest, not a re-provision."""
    plan = _plan(_redrive(staged_work_present=True), _history())

    assert plan["state"] == pr.STATE_STAGED_WORK_UNPUBLISHED
    assert plan["recovery_command"] == "uv run okg ingest --deployment dep --json"
    assert plan["last_known_good"] is None


def test_never_published_with_nothing_staged_names_okg_provision() -> None:
    plan = _plan(_redrive(), _history())

    assert plan["state"] == pr.STATE_NEVER_PUBLISHED
    assert plan["recovery_command"] == (
        "uv run okg provision --deployment dep --json"
    )
    assert plan["okg_up_applies"] is False


@pytest.mark.parametrize(
    "history",
    [
        _history(checkpoint_count_any_branch=2),
        _history(legacy_published_generation_id=7),
    ],
    ids=["other_branch_checkpoints", "legacy_generation_row"],
)
def test_unpublished_default_branch_with_history_is_unknown_not_provision(
    history: dict[str, Any],
) -> None:
    """Re-provisioning re-runs every source; refuse to name it on a guess."""
    plan = _plan(_redrive(), history)

    assert plan["state"] == pr.STATE_UNKNOWN
    assert plan["recovery_command"] is None
    assert plan["okg_up_applies"] is False
    assert "okg generation list" in plan["explanation"]


def test_incomplete_checkpoint_in_repair_requires_the_exact_selection() -> None:
    receipt = {
        "reason": "ingest_source_incomplete",
        "recovery": {
            "schema": "okg.ingest_incomplete_recovery.v1",
            "state": "repair_required",
            "source_contract": [{"source": "git"}, {"source": "sessions"}],
            "admission_ids": [],
            "predecessor_checkpoint_id": _PUBLISHED["generation_id"],
        },
    }
    plan = _plan(
        _redrive(
            latest_checkpoint_id="okg:dep:branch:default:gen:2:blocked",
            latest_checkpoint_status="blocked",
            latest_blocked_reason="ingest_source_incomplete",
            latest_blocked_receipt=receipt,
            latest_published_checkpoint_id=_PUBLISHED["generation_id"],
        ),
        _history(last_published=_PUBLISHED, checkpoint_count_any_branch=2),
    )

    assert plan["state"] == pr.STATE_CHECKPOINT_REPAIR_REQUIRED
    assert plan["recovery_command"] == (
        "uv run okg ingest --deployment dep --include git,sessions --json"
    )
    assert plan["required_sources"] == ["git", "sessions"]
    assert "supersede-checkpoint" in plan["escape_hatch_command"]
    assert "incomplete_recovery_contract_mismatch" in plan["explanation"]
    assert plan["okg_up_applies"] is False


def test_frozen_incomplete_checkpoint_is_promoted_by_a_bare_ingest() -> None:
    receipt = {
        "reason": "ingest_source_incomplete",
        "recovery": {
            "schema": "okg.ingest_incomplete_recovery.v1",
            "state": "frozen",
            "source_contract": [{"source": "git"}],
            "admission_ids": ["a"],
            "predecessor_checkpoint_id": _PUBLISHED["generation_id"],
        },
    }
    plan = _plan(
        _redrive(
            latest_checkpoint_id="okg:dep:branch:default:gen:2:frozen",
            latest_checkpoint_status="blocked",
            latest_blocked_reason="ingest_source_incomplete",
            latest_blocked_receipt=receipt,
        ),
        _history(last_published=_PUBLISHED, checkpoint_count_any_branch=2),
    )

    assert plan["state"] == pr.STATE_CHECKPOINT_BLOCKED
    assert plan["recovery_command"] == "uv run okg ingest --deployment dep --json"
    assert "promotes" in plan["explanation"]


def test_legacy_incomplete_receipt_names_the_explicit_remedy_not_a_rerun() -> None:
    plan = _plan(
        _redrive(
            latest_checkpoint_id="okg:dep:branch:default:gen:2:legacy",
            latest_checkpoint_status="blocked",
            latest_blocked_reason="ingest_source_incomplete",
            latest_blocked_receipt={"reason": "ingest_source_incomplete"},
        ),
        _history(last_published=_PUBLISHED, checkpoint_count_any_branch=2),
    )

    assert plan["state"] == pr.STATE_CHECKPOINT_BLOCKED
    assert plan["recovery_command"] is None
    assert plan["legacy_receipt"] is True
    assert "okg run --once --inline --force-enrichers --deployment dep --apply" in (
        plan["explanation"]
    )
    assert "supersede-checkpoint" in plan["escape_hatch_command"]


def test_superseded_incomplete_checkpoint_falls_through_to_ordinary_state() -> None:
    receipt = {
        "reason": "ingest_source_incomplete",
        "recovery": {
            "schema": "okg.ingest_incomplete_recovery.v1",
            "state": "superseded",
            "source_contract": [{"source": "git"}],
            "admission_ids": [],
            "predecessor_checkpoint_id": _PUBLISHED["generation_id"],
        },
    }
    plan = _plan(
        _redrive(
            latest_checkpoint_id="okg:dep:branch:default:gen:2:superseded",
            latest_checkpoint_status="blocked",
            latest_blocked_reason="ingest_source_incomplete",
            latest_blocked_receipt=receipt,
        ),
        _history(last_published=_PUBLISHED, checkpoint_count_any_branch=2),
    )

    # Retired authority: the checkpoint is still `blocked`, so the ordinary
    # blocked-generation guidance applies, not the incomplete-source contract.
    assert plan["state"] == pr.STATE_CHECKPOINT_BLOCKED
    assert plan["required_sources"] is None
    assert plan["diagnose_command"] == (
        "uv run okg generation describe okg:dep:branch:default:gen:2:superseded "
        "--deployment dep --json"
    )


def test_blocked_generation_from_a_publication_check_names_describe() -> None:
    plan = _plan(
        _redrive(
            latest_checkpoint_id="okg:dep:branch:default:gen:2:blocked",
            latest_checkpoint_status="blocked",
            latest_blocked_reason="invariant_failed",
            latest_published_checkpoint_id=_PUBLISHED["generation_id"],
        ),
        _history(last_published=_PUBLISHED, checkpoint_count_any_branch=2),
    )

    assert plan["state"] == pr.STATE_CHECKPOINT_BLOCKED
    assert plan["recovery_command"] == "uv run okg ingest --deployment dep --json"
    assert plan["diagnose_command"].startswith("uv run okg generation describe ")
    assert plan["last_known_good"] == _PUBLISHED


def test_dbos_route_is_named_and_never_appends_inline() -> None:
    plan = _plan(
        _redrive(staged_work_present=True),
        _history(last_published=_PUBLISHED, checkpoint_count_any_branch=1),
        runtime_enabled=True,
    )

    assert plan["execution"]["route"] == "dbos_enqueue"
    assert plan["execution"]["follow_up_command"] == (
        "uv run okg sync health --deployment dep --json"
    )
    assert "--inline" not in plan["recovery_command"]

    unknown = _plan(_redrive(), _history(), runtime_enabled=None)
    assert unknown["execution"]["route"] == "unknown"


def test_dead_letter_backlog_is_reported_not_hidden() -> None:
    plan = _plan(
        _redrive(
            latest_checkpoint_status="published",
            latest_published_checkpoint_id=_PUBLISHED["generation_id"],
        ),
        _history(last_published=_PUBLISHED, checkpoint_count_any_branch=1),
        queues={"measured": True, "dlq_count": 2, "active_queue_depth": 0},
    )

    assert plan["state"] == pr.STATE_HEALTHY
    assert plan["queues_ok"] is False
    assert plan["queues"]["dlq_count"] == 2


def test_queue_read_failure_is_named_and_does_not_fail_the_plan() -> None:
    def boom(_dsn: str) -> dict[str, Any]:
        raise RuntimeError("no runtime tables")

    plan = pr.plan_publish_recovery(
        dsn="postgresql://unused",
        deployment="dep",
        runtime_enabled=False,
        read_state=lambda _dsn, _dep: _redrive(),
        read_history=lambda _dsn, _dep: _history(),
        read_queues=boom,
    )

    assert plan["ok"] is True
    assert plan["queues"]["measured"] is False
    assert plan["queues"]["error"] == {
        "type": "RuntimeError", "message": "no runtime tables",
    }
    assert plan["queues_ok"] is None  # unmeasured is not clean


def test_state_read_failure_is_ok_false_never_a_verdict() -> None:
    def boom(_dsn: str, _dep: str) -> dict[str, Any]:
        raise ConnectionError("connection refused")

    plan = pr.plan_publish_recovery(
        dsn="postgresql://unused",
        deployment="dep",
        read_state=boom,
        read_history=lambda _dsn, _dep: _history(),
    )
    json.dumps(plan)

    assert plan["ok"] is False
    assert plan["state"] == pr.STATE_UNKNOWN
    assert plan["recovery_command"] is None
    assert plan["okg_up_applies"] is False
    assert plan["error"] == {"type": "ConnectionError", "message": "connection refused"}


def test_real_readers_refuse_an_unreachable_database_quickly() -> None:
    """The production readers bound their connect attempt (unit tests of
    `okg up` carry fake DSNs, so the plan must fail fast, not hang)."""
    plan = pr.plan_publish_recovery(
        dsn="postgresql://nobody@127.0.0.1:1/nothing",
        deployment="dep",
    )

    assert plan["ok"] is False
    assert plan["error"]["type"] == "OperationalError"


def test_plan_is_stable_across_repeated_reads() -> None:
    redrive = _redrive(staged_work_present=True)
    history = _history(last_published=_PUBLISHED, checkpoint_count_any_branch=1)

    first = _plan(redrive, history)
    second = _plan(redrive, history)

    assert first == second
