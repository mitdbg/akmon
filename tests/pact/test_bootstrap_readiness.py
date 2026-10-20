"""The kickoff packet must not look ready while a precondition blocks.

The failure this guards against is not a crash. It is a packet that correctly
diagnoses a blocker (`next_action: configure_environment`) and simultaneously
tells the caller to proceed (`ok: true`, plus a `next_command` pointing
somewhere unrelated). That turns a blocker knowable at step zero into one
discovered after the work is finished and proven.
"""

from __future__ import annotations

from typing import Any

from okg.substrate.pact.cli import (
    _bootstrap_blocker_detail,
    _bootstrap_blocking_check,
    _bootstrap_actionable,
    _bootstrap_next_action,
    _bootstrap_summary,
)


def _check(
    category: str,
    status: str,
    *,
    next_action: str = "clear_it",
    command: str = "uv run okg fix-it",
    owner: str = "operator",
    detail: Any = None,
) -> dict[str, Any]:
    return {
        "category": category,
        "status": status,
        "owner": owner,
        "next_action": next_action,
        "command": command,
        "detail": detail if detail is not None else {"reason": f"{category} unmet"},
    }


def _gate(ready: bool, *checks: dict[str, Any]) -> dict[str, Any]:
    return {"ready": ready, "checks": list(checks)}


def _payload(gate: dict[str, Any], **over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        # `ok` is command success and stays true; readiness to start is
        # `actionable`. Production derivation, not a reconstruction of the rule:
        # reimplementing it here would let the literal come back undetected.
        "ok": True,
        "actionable": _bootstrap_actionable(gate),
        "change": "demo",
        "cutover_gate": gate,
        "next_action": _bootstrap_next_action(gate, {"task_id": "task.demo"}, {}),
        "commands": {
            "coordination_next": "uv run okg pact view --format=review-queue --json",
            "continue": "uv run okg pact view demo --format=bootstrap --json",
        },
        "pact": {"title": "Demo"},
        "task_bundle": {"task_id": "task.demo", "title": "Demo task"},
        "graph_readiness": {},
        "coordination": {},
        "skill_freshness": {},
        "project_settings": {},
    }
    base.update(over)
    return base


# ---------------------------------------------------------------------------
# ok_or_command
# ---------------------------------------------------------------------------


def test_ok_or_command_actionable_is_false_when_the_gate_blocks() -> None:
    gate = _gate(False, _check("runtime_authority", "blocker"))
    assert _bootstrap_actionable(gate) is False
    assert _bootstrap_summary(_payload(gate))["actionable"] is False


def test_ok_or_command_actionable_is_true_only_when_the_gate_is_ready() -> None:
    gate = _gate(True, _check("runtime_authority", "pass"))
    assert _bootstrap_actionable(gate) is True
    assert _bootstrap_summary(_payload(gate))["actionable"] is True


def test_ok_or_command_actionable_is_not_a_literal() -> None:
    """The derivation must actually vary with the gate.

    Guards the specific regression: a readiness field written straight into the
    payload, reporting a ready packet beside a blocking next_action.
    """

    assert _bootstrap_actionable({"ready": False}) != _bootstrap_actionable({"ready": True})
    assert _bootstrap_actionable({}) is False  # absent readiness is not readiness


def test_ok_or_command_ok_stays_command_success_not_readiness() -> None:
    """A blocked packet is still a successfully rendered view.

    `ok` drives the process exit status for every pact verb. Folding readiness
    into it would turn "you cannot start yet" into "the command failed" for
    every existing caller.
    """

    gate = _gate(False, _check("runtime_authority", "blocker"))
    summary = _bootstrap_summary(_payload(gate))
    assert summary["ok"] is True
    assert summary["actionable"] is False


def test_ok_or_command_next_command_is_the_blocking_checks_command() -> None:
    """The command offered must clear the blocker that was named."""

    gate = _gate(
        False,
        _check(
            "runtime_authority",
            "blocker",
            next_action="configure_environment",
            command="uv run okg deployment env okg-workspace --write-local",
        ),
    )
    summary = _bootstrap_summary(_payload(gate))
    assert summary["next_action"] == "configure_environment"
    assert summary["next_command"] == "uv run okg deployment env okg-workspace --write-local"


def test_ok_or_command_action_and_command_never_disagree() -> None:
    """Across every blocking category, the pair comes from one row."""

    categories = [
        ("runtime_authority", "configure_environment", "uv run okg deployment env x"),
        ("coordination", "fix_coordination_status", "uv run okg pact gate post-edit x"),
        ("catalog_ownership", "claim_catalog", "uv run okg catalog claim"),
        ("skills", "refresh_skills", "uv run okg pact hooks install"),
    ]
    for category, action, command in categories:
        gate = _gate(
            False, _check(category, "blocker", next_action=action, command=command)
        )
        summary = _bootstrap_summary(_payload(gate))
        assert summary["next_action"] == action, category
        assert summary["next_command"] == command, category


def test_ok_or_command_unblocked_packet_keeps_the_coordination_command() -> None:
    gate = _gate(True, _check("runtime_authority", "pass"))
    summary = _bootstrap_summary(_payload(gate))
    assert summary["next_command"] == "uv run okg pact view --format=review-queue --json"
    assert summary["blocker"] is None


def test_ok_or_command_a_skipped_check_is_the_blocker_when_nothing_else_is() -> None:
    """A skipped check still explains why the gate is not ready.

    Excluding skipped rows outright regressed `run_doctor_for_verification` to a
    generic `clear_cutover_blocker` (tests/okg-workspace/
    test_agent_dogfood_bootstrap.py). A skipped check is pending work, not
    noise: it loses to a real blocker but beats reporting nothing.
    """

    gate = _gate(
        False,
        _check("doctor", "skipped", next_action="run_doctor", command="uv run okg doctor"),
    )
    chosen = _bootstrap_blocking_check(gate)
    assert chosen is not None and chosen["next_action"] == "run_doctor"
    assert _bootstrap_summary(_payload(gate))["next_command"] == "uv run okg doctor"


def test_ok_or_command_skipped_checks_do_not_count_as_blockers() -> None:
    """A real blocker outranks a skipped check for the next command."""

    gate = _gate(
        False,
        _check("doctor", "skipped", next_action="run_doctor", command="uv run okg doctor"),
        _check(
            "runtime_authority",
            "blocker",
            next_action="configure_environment",
            command="uv run okg deployment env x",
        ),
    )
    summary = _bootstrap_summary(_payload(gate))
    assert summary["next_action"] == "configure_environment"
    assert summary["next_command"] == "uv run okg deployment env x"


def test_ok_or_command_first_blocking_check_wins_consistently() -> None:
    """next_action and next_command must select the SAME row, not two rows."""

    gate = _gate(
        False,
        _check("runtime_authority", "blocker", next_action="a1", command="c1"),
        _check("coordination", "blocker", next_action="a2", command="c2"),
    )
    chosen = _bootstrap_blocking_check(gate)
    summary = _bootstrap_summary(_payload(gate))
    assert chosen is not None
    assert summary["next_action"] == chosen["next_action"] == "a1"
    assert summary["next_command"] == chosen["command"] == "c1"


# ---------------------------------------------------------------------------
# legible
# ---------------------------------------------------------------------------


def test_legible_blocker_names_the_precondition_and_the_remediation() -> None:
    gate = _gate(
        False,
        _check(
            "runtime_authority",
            "blocker",
            next_action="configure_environment",
            command="uv run okg deployment env okg-workspace --write-local",
            owner="operator",
            detail={"unresolved": ["OKG_DSN"]},
        ),
    )
    summary = _bootstrap_summary(_payload(gate))
    blocker = summary["blocker"]
    assert blocker is not None
    assert blocker["category"] == "runtime_authority"
    assert blocker["owner"] == "operator"
    assert blocker["next_action"] == "configure_environment"
    assert "deployment env" in blocker["command"]
    # The specific unmet precondition, not just an action token.
    assert blocker["detail"] == {"unresolved": ["OKG_DSN"]}


def test_legible_blocker_is_absent_when_nothing_blocks() -> None:
    assert _bootstrap_blocker_detail(None) is None
    gate = _gate(True, _check("runtime_authority", "pass"))
    assert _bootstrap_blocking_check(gate) is None


def test_legible_blocker_survives_a_check_missing_optional_fields() -> None:
    """A sparse check must still yield a blocker, not raise."""

    gate = {"ready": False, "checks": [{"category": "x", "status": "blocker"}]}
    detail = _bootstrap_blocker_detail(_bootstrap_blocking_check(gate))
    assert detail is not None
    assert detail["category"] == "x"
    assert detail["command"] is None
