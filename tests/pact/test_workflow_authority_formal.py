from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.pact import workflow_authority as wa
from _v5_formal import carried_bindings
from okg.substrate.pact.workflow_authority import (
    GitEvidence,
    WorkflowAuthorityInput,
    evaluate_workflow_authority,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
CHANGE_ID = "formalize-workflow-authority-staleness"
MODEL_PATH = REPO_ROOT / "formal" / "pact" / "WorkflowAuthority.tla"
CONFIG_PATH = REPO_ROOT / "formal" / "pact" / "WorkflowAuthority.cfg"
CHECKER_PATH = REPO_ROOT / "scripts" / "check_formal_workflow_authority.py"
REFINEMENT_TESTS = {
    "tests/pact/test_workflow_authority_formal.py::"
    "test_workflow_authority_refuses_stale_or_unpinned_surfaces",
    "tests/pact/test_workflow_authority_formal.py::"
    "test_workflow_authority_requires_git_evidence_for_code_claims",
    "tests/pact/test_workflow_authority_formal.py::"
    "test_workflow_authority_requires_explicit_freshness_surfaces",
    "tests/pact/test_workflow_authority_formal.py::"
    "test_git_evidence_must_be_live_checked",
    "tests/pact/test_workflow_authority_formal.py::"
    "test_workflow_authority_does_not_invent_unclaimed_graph_authority",
    "tests/pact/test_workflow_authority_formal.py::"
    "test_event_plane_intent_authority_replaces_default_graph_projection",
    "tests/pact/test_workflow_authority_formal.py::"
    "test_event_plane_intent_authority_requires_allowed_foreign_checkout",
    "tests/pact/test_workflow_authority_formal.py::"
    "test_event_plane_intent_authority_refuses_stale_surfaces",
}


def _fresh_authority_inputs() -> dict[str, Any]:
    return {
        "file_fresh": True,
        "graph_required": True,
        "graph_ready": True,
        "graph_fresh": True,
        "ledger_fresh": True,
        "ledger_approval": wa.LEDGER_APPROVAL_APPROVED,
        "generation_pin_matches": True,
        "evidence_artifacts_fresh": True,
        "pact_coordination_agree": True,
    }


def _manifest_path(change_id: str) -> Path:
    for base in ["changes", "archive"]:
        candidate = REPO_ROOT / "pact" / base / change_id / "pact.yaml"
        if candidate.is_file():
            return candidate
    raise AssertionError(f"PACT manifest not found: {change_id}")


def _checker_properties() -> set[str]:
    spec = importlib.util.spec_from_file_location(
        "check_formal_workflow_authority",
        CHECKER_PATH,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {property.property_id for property in module.PROPERTIES}


@pytest.mark.parametrize(
    ("field", "blocker", "live_check"),
    [
        ("file_fresh", "file_not_fresh", "reload pact.yaml"),
        ("graph_ready", "graph_not_ready", "run graph readiness check"),
        ("graph_fresh", "graph_not_fresh", "refresh graph projection"),
        ("ledger_fresh", "ledger_not_fresh", "refresh decision ledger"),
        (
            "generation_pin_matches",
            "generation_pin_mismatch",
            "repin graph generation",
        ),
        (
            "evidence_artifacts_fresh",
            "evidence_artifacts_stale",
            "rerun evidence checks",
        ),
        (
            "pact_coordination_agree",
            "pact_coordination_disagreement",
            "reconcile pact and coordination state",
        ),
    ],
)
def test_workflow_authority_refuses_stale_or_unpinned_surfaces(
    field: str,
    blocker: str,
    live_check: str,
) -> None:
    inputs = _fresh_authority_inputs()
    inputs[field] = False

    decision = evaluate_workflow_authority(WorkflowAuthorityInput(**inputs))

    assert decision.accepted is False
    assert blocker in decision.blockers
    assert live_check in decision.required_live_checks


def test_workflow_authority_requires_git_evidence_for_code_claims() -> None:
    missing_git = evaluate_workflow_authority(
        WorkflowAuthorityInput(
            **_fresh_authority_inputs(),
            code_claims=("changed formal gate",),
        )
    )
    assert missing_git.accepted is False
    assert "missing_git_evidence" in missing_git.blockers

    with_git = evaluate_workflow_authority(
        WorkflowAuthorityInput(
            **_fresh_authority_inputs(),
            code_claims=("changed formal gate",),
            git_evidence=GitEvidence(
                commit="abc123",
                file_paths=("src/okg/substrate/pact/workflow_authority.py",),
                live_checked=True,
            ),
        )
    )
    assert with_git.accepted is True
    assert with_git.blockers == ()


def test_missing_ledger_approval_is_not_reported_as_a_stale_ledger() -> None:
    """Issue #1443: "the ledger answered, and nobody approved" is its own fact.

    Reported as `ledger_not_fresh` it named a broken database and routed the
    refusal to the operator-action ledger remedy, which publishes no command.
    """
    decision = evaluate_workflow_authority(
        WorkflowAuthorityInput(
            **{
                **_fresh_authority_inputs(),
                "ledger_approval": wa.LEDGER_APPROVAL_ABSENT,
            }
        )
    )

    assert decision.accepted is False
    assert "no_ledger_approval" in decision.blockers
    assert "approval_not_archived" not in decision.blockers
    assert "ledger_not_fresh" not in decision.blockers
    assert "ledger_dsn_not_configured" not in decision.blockers
    assert "pact_coordination_disagreement" not in decision.blockers
    assert "refresh decision ledger" not in decision.required_live_checks
    assert (
        "record an operator decision with okg pact decide"
        in decision.required_live_checks
    )


def test_unarchived_approval_is_its_own_blocker_with_the_archive_remedy() -> None:
    """An approval binding the current digest exists; only the archive step is
    missing. Calling that "no approval recorded" would be false, and the
    FSM-legal verdict there is `defer` — an inert command."""
    decision = evaluate_workflow_authority(
        WorkflowAuthorityInput(
            **{
                **_fresh_authority_inputs(),
                "ledger_approval": wa.LEDGER_APPROVAL_NOT_ARCHIVED,
            }
        )
    )

    assert decision.accepted is False
    assert "approval_not_archived" in decision.blockers
    assert "no_ledger_approval" not in decision.blockers
    assert "ledger_not_fresh" not in decision.blockers
    assert "pact_coordination_disagreement" not in decision.blockers
    assert "archive the approved change" in decision.required_live_checks
    assert (
        "record an operator decision with okg pact decide"
        not in decision.required_live_checks
    )


@pytest.mark.parametrize(
    "approval",
    [wa.LEDGER_APPROVAL_ABSENT, wa.LEDGER_APPROVAL_NOT_ARCHIVED],
)
def test_unread_ledger_never_testifies_about_approval(approval: str) -> None:
    """An unreachable ledger cannot testify to what it holds, so neither
    approval blocker fires until the ledger has actually answered."""
    decision = evaluate_workflow_authority(
        WorkflowAuthorityInput(
            **{
                **_fresh_authority_inputs(),
                "ledger_fresh": False,
                "ledger_approval": approval,
            }
        )
    )

    assert decision.accepted is False
    assert "ledger_not_fresh" in decision.blockers
    assert "no_ledger_approval" not in decision.blockers
    assert "approval_not_archived" not in decision.blockers


def test_not_judged_is_clean_and_is_the_default_for_non_merge_callers() -> None:
    """`gate task-done` and the cutover report answer a different question: a
    merge approval is no part of their bar, so the surface must add no blocker.

    The default is permissive BY DESIGN for exactly those callers. The
    fail-closed guard lives at the merge boundary instead, where
    `authority_payload` takes `ledger_approval` as a required keyword — see
    `test_merge_boundary_requires_an_explicit_approval_answer`.
    """
    inputs = _fresh_authority_inputs()
    del inputs["ledger_approval"]

    decision = evaluate_workflow_authority(WorkflowAuthorityInput(**inputs))

    assert WorkflowAuthorityInput(**inputs).ledger_approval == (
        wa.LEDGER_APPROVAL_NOT_JUDGED
    )
    assert decision.accepted is True
    assert decision.blockers == ()


def test_merge_boundary_requires_an_explicit_approval_answer() -> None:
    """The fail-closed half of the default: the merge gate's authority seam
    takes `ledger_approval` as a REQUIRED keyword, so a new branch cannot
    inherit the permissive answer by forgetting one argument."""
    import inspect

    from okg.substrate.pact import cli as pact_cli

    source = inspect.getsource(pact_cli._ledger_decision_gate)
    signature = source.split("    ) -> dict[str, object]:")[0]
    assert "ledger_approval: str," in signature, signature
    assert "ledger_approval: str = " not in signature, signature


def test_ledger_answer_tokens_are_not_operator_action_tokens() -> None:
    """Both are cleared inside this process — one by a decision, one by the
    archive step — so neither may be scored or presented as an unrepairable
    ledger fault."""
    from okg.substrate.pact import repair
    from okg.substrate.pact.bench import refusals

    decide_token = repair.NO_LEDGER_APPROVAL_BLOCKER
    archive_token = repair.APPROVAL_NOT_ARCHIVED_BLOCKER
    assert decide_token == "no_ledger_approval"
    assert archive_token == "approval_not_archived"

    for token in (decide_token, archive_token):
        assert token not in repair.LEDGER_BLOCKERS
        assert token not in refusals.OPERATOR_ACTION_TOKENS
        assert refusals.is_operator_action_token(token) is False
        assert repair.is_operator_action_only([token]) is False
        assert repair.is_operator_action_only(["ledger_not_fresh", token]) is False

    # Only the decide token is command-repairable. `approval_not_archived` is
    # deliberately excluded: no decide verdict clears it, and recommending one
    # is the inert-command loop.
    assert repair.COMMAND_REPAIRABLE_AUTHORITY_BLOCKERS == frozenset({decide_token})
    assert archive_token not in repair.COMMAND_REPAIRABLE_AUTHORITY_BLOCKERS
    assert (
        repair.COMMAND_REPAIRABLE_AUTHORITY_BLOCKERS & repair.LEDGER_BLOCKERS
        == frozenset()
    )


def test_every_post_read_gate_branch_is_classified_and_refuses() -> None:
    """The merge gate's branch-to-ledger-answer table, pinned end to end.

    Each branch that READ the ledger reports `ledger_fresh=True` and one
    specific fault. The table is the fail-closed mechanism: a status the gate
    can emit but nobody classified would otherwise inherit whichever answer sat
    nearest in the file (issue #1443).
    """
    from okg.substrate.pact import cli as pact_cli

    expected = {
        "approval_not_terminal_archive": "approval_not_archived",
        "final_verdict_receipt_missing_decision": "pact_coordination_disagreement",
        "final_verdict_event_missing_or_duplicate": "pact_coordination_disagreement",
        "final_verdict_event_mismatch": "pact_coordination_disagreement",
        "approval_superseded_by_later_decision": "no_ledger_approval",
        "approval_stale_intent_changed": "no_ledger_approval",
        "no_approval_recorded": "no_ledger_approval",
    }
    table = pact_cli._POST_READ_LEDGER_ANSWER
    assert set(table) == set(expected), set(table) ^ set(expected)

    approval_tokens = {"no_ledger_approval", "approval_not_archived"}
    for status, blocker in expected.items():
        agree, approval = table[status]
        decision = evaluate_workflow_authority(
            WorkflowAuthorityInput(
                **{
                    **_fresh_authority_inputs(),
                    "pact_coordination_agree": agree,
                    "ledger_approval": approval,
                }
            )
        )
        assert decision.accepted is False, status
        assert blocker in decision.blockers, (status, decision.blockers)
        # The ledger was READ on every one of these branches.
        assert "ledger_not_fresh" not in decision.blockers, status
        assert "ledger_dsn_not_configured" not in decision.blockers, status
        # Exactly one approval-answer token, never both.
        assert len(approval_tokens & set(decision.blockers)) <= 1, (
            status,
            decision.blockers,
        )
        if blocker == "pact_coordination_disagreement":
            # An approval row EXISTS on these two; claiming otherwise is false.
            assert not (approval_tokens & set(decision.blockers)), (
                status,
                decision.blockers,
            )

    # The fallback for an unclassified status refuses on BOTH axes.
    agree, approval = pact_cli._POST_READ_LEDGER_ANSWER_FALLBACK
    fallback = evaluate_workflow_authority(
        WorkflowAuthorityInput(
            **{
                **_fresh_authority_inputs(),
                "pact_coordination_agree": agree,
                "ledger_approval": approval,
            }
        )
    )
    assert fallback.accepted is False
    assert "pact_coordination_disagreement" in fallback.blockers
    assert "no_ledger_approval" in fallback.blockers


def test_workflow_authority_model_carries_the_approval_surface() -> None:
    model_text = MODEL_PATH.read_text(encoding="utf-8")
    config_text = CONFIG_PATH.read_text(encoding="utf-8")
    properties = _checker_properties()

    assert "ledgerApproval" in model_text
    assert "LedgerApprovals ==" in model_text
    for value in wa.LEDGER_APPROVALS:
        assert f'"{value}"' in model_text, value
    assert '"no_ledger_approval"' in model_text
    assert '"approval_not_archived"' in model_text
    assert '"record an operator decision with okg pact decide"' in model_text
    assert '"archive the approved change"' in model_text
    for invariant in ("NoUnapprovedLedgerAuthority", "NoUnarchivedApprovalAuthority"):
        assert f"{invariant} ==" in model_text, invariant
        assert invariant in config_text, invariant
        assert invariant in properties, invariant


def test_workflow_authority_requires_explicit_freshness_surfaces() -> None:
    with pytest.raises(TypeError):
        WorkflowAuthorityInput(
            graph_ready=True,
            graph_fresh=True,
            pact_coordination_agree=True,
        )


def test_git_evidence_must_be_live_checked() -> None:
    decision = evaluate_workflow_authority(
        WorkflowAuthorityInput(
            **_fresh_authority_inputs(),
            code_claims=("changed formal gate",),
            git_evidence=GitEvidence(
                commit="abc123",
                file_paths=("src/okg/substrate/pact/workflow_authority.py",),
            ),
        )
    )

    assert decision.accepted is False
    assert "missing_git_evidence" in decision.blockers


def test_workflow_authority_does_not_invent_unclaimed_graph_authority() -> None:
    decision = evaluate_workflow_authority(
        WorkflowAuthorityInput(
            **{
                **_fresh_authority_inputs(),
                "graph_required": False,
                "graph_ready": False,
                "graph_fresh": False,
            },
        )
    )

    assert decision.accepted is True
    assert "graph_not_ready" not in decision.blockers
    assert "graph_not_fresh" not in decision.blockers

    exact_private = evaluate_workflow_authority(
        WorkflowAuthorityInput(
            **_fresh_authority_inputs(),
            projection_authority="published_generation_evidence",
        )
    )
    assert exact_private.accepted is True

    unavailable_private = evaluate_workflow_authority(
        WorkflowAuthorityInput(
            **{
                **_fresh_authority_inputs(),
                "graph_ready": False,
                "graph_fresh": False,
            },
            projection_authority="published_generation_evidence",
        )
    )
    assert unavailable_private.accepted is False
    assert "graph_not_ready" in unavailable_private.blockers
    assert "graph_not_fresh" in unavailable_private.blockers
    assert "verify exact published generation" in (
        unavailable_private.required_live_checks
    )
    assert "refresh graph projection" not in unavailable_private.required_live_checks


def test_event_plane_intent_authority_replaces_default_graph_projection() -> None:
    decision = evaluate_workflow_authority(
        WorkflowAuthorityInput(
            **{
                **_fresh_authority_inputs(),
                "graph_ready": False,
                "graph_fresh": False,
                "generation_pin_matches": False,
            },
            projection_authority="event_plane_intent",
            event_plane_authority_allowed=True,
            intent_event_fresh=True,
            intent_digest_matches=True,
            worktree_head_matches=True,
            worktree_clean=True,
        )
    )

    assert decision.accepted is True
    assert "graph_not_ready" not in decision.blockers
    assert "graph_not_fresh" not in decision.blockers
    assert "generation_pin_mismatch" not in decision.blockers


def test_event_plane_intent_authority_requires_allowed_foreign_checkout() -> None:
    decision = evaluate_workflow_authority(
        WorkflowAuthorityInput(
            **{
                **_fresh_authority_inputs(),
                "graph_ready": False,
                "graph_fresh": False,
                "generation_pin_matches": False,
            },
            projection_authority="event_plane_intent",
            event_plane_authority_allowed=False,
            intent_event_fresh=True,
            intent_digest_matches=True,
            worktree_head_matches=True,
            worktree_clean=True,
        )
    )

    assert decision.accepted is False
    assert "event_plane_not_allowed" in decision.blockers
    assert "use sync-root graph projection" in decision.required_live_checks


@pytest.mark.parametrize(
    ("field", "blocker", "live_check"),
    [
        ("intent_event_fresh", "intent_event_not_fresh", "refresh pact intent event"),
        ("intent_digest_matches", "intent_digest_mismatch", "refresh pact intent digest"),
        ("worktree_head_matches", "worktree_head_mismatch", "refresh pact intent head"),
        ("worktree_clean", "worktree_not_clean", "clean worktree"),
    ],
)
def test_event_plane_intent_authority_refuses_stale_surfaces(
    field: str,
    blocker: str,
    live_check: str,
) -> None:
    event_inputs = {
        "projection_authority": "event_plane_intent",
        "event_plane_authority_allowed": True,
        "intent_event_fresh": True,
        "intent_digest_matches": True,
        "worktree_head_matches": True,
        "worktree_clean": True,
    }
    event_inputs[field] = False
    decision = evaluate_workflow_authority(
        WorkflowAuthorityInput(
            **{
                **_fresh_authority_inputs(),
                "graph_ready": False,
                "graph_fresh": False,
                "generation_pin_matches": False,
            },
            **event_inputs,
        )
    )

    assert decision.accepted is False
    assert blocker in decision.blockers
    assert live_check in decision.required_live_checks


def test_workflow_authority_formal_binding_covers_contract() -> None:
    properties = _checker_properties()
    model_text = MODEL_PATH.read_text(encoding="utf-8")
    config_text = CONFIG_PATH.read_text(encoding="utf-8")

    assert "WorkflowAuthorityContract" in properties
    assert "WorkflowAuthorityContract ==" in model_text
    assert "NoStaleWorktreeIntentAuthority ==" in model_text
    assert "NoUnauthorizedEventPlaneAuthority ==" in model_text
    assert "WorkflowAuthorityContract" in config_text
    assert "NoStaleWorktreeIntentAuthority" in config_text
    assert "NoUnauthorizedEventPlaneAuthority" in config_text

    # v5 has no `formal[]`: `pact migrate` carried each binding into a
    # `formal_check` evidence row naming the property, the module and the
    # checker command. `config` and `refinement_tests` were not carried, so
    # they are asserted against the files themselves below instead of being
    # restated by the manifest.
    formal = {
        property_id: binding
        for property_id, binding in carried_bindings(CHANGE_ID).items()
        if binding.module == "formal/pact/WorkflowAuthority.tla"
    }
    assert set(formal) >= {"WorkflowAuthorityContract"}
    binding = formal["WorkflowAuthorityContract"]
    assert binding.requirement == (
        "req.formalize-workflow-authority-staleness.current-authority"
    )
    assert binding.checker_command == (
        "uv run python scripts/check_formal_workflow_authority.py "
        "--out .scratch/formal/workflow-authority"
    )
    for test in REFINEMENT_TESTS:
        assert (REPO_ROOT / test.partition("::")[0]).is_file(), test

    for property_id in properties:
        assert property_id in model_text
        assert property_id in config_text
