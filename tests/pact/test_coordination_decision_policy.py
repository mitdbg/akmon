from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.pact.decision_policy import (
    ActionPolicy,
    DecisionPolicyError,
    HumanInvolvementProfile,
    HumanInvolvementPolicy,
    TrustedDecisionContext,
    evaluate_policy,
    parse_human_involvement_policy,
    stable_request_digest,
)
from okg.substrate.pact.project_settings import coordination_policy

from tests.private_policy import private_policy_path


REPO_ROOT = Path(__file__).resolve().parents[2]
ROUTE = {"hops": ["worker", "coordinator", "chief", "operator"]}
SUPPRESSION = {
    "digest_fields": [
        "assignment_id",
        "assignment_generation",
        "principal",
        "assignment_session_id",
        "mandate",
        "action_digest",
        "resource_digest",
        "scope_digest",
        "human_policy_digest",
    ],
    "terminal_statuses": ["granted", "denied", "revoked", "consumed"],
}
EXPECTED_ACTIONS = {
    "read": "allow",
    "analyze": "allow",
    "local_edit": "allow",
    "run_tests": "allow",
    "commit": "allow",
    "helper_use": "allow",
    "record_task": "allow",
    "record_evidence": "allow",
    "coordinator_order": "allow",
    "branch_push": "notify",
    "draft_pr_change": "notify",
    "terminal_result": "notify",
    "capacity_release": "notify",
    "policy_change": "require_approval",
    "tier3_program_intent": "require_approval",
    "protected_resource": "require_approval",
    "destructive_external": "require_approval",
    "irreversible_external": "require_approval",
    "merge": "require_approval",
    "credentials_privilege": "require_approval",
    "risk_acceptance": "require_approval",
    "unresolved_p0_p1": "require_approval",
    "out_of_charter": "require_approval",
    "self_approval": "deny",
    "session_laundering": "deny",
    "wrong_scope": "deny",
    "stale_generation": "deny",
}
EXPECTED_SUPERVISED_OVERRIDES = {
    "local_edit": "require_approval",
    "commit": "require_approval",
    "branch_push": "require_approval",
}


def _context(policy: HumanInvolvementPolicy, **updates: object) -> TrustedDecisionContext:
    values: dict[str, object] = {
        "principal": "worker-a",
        "role": "worker",
        "assignment_id": "assignment-a",
        "assignment_generation": 1,
        "assignment_principal": "worker-a",
        "assignment_session_id": "session-a",
        "assignment_status": "active",
        "mandate": "sprint-a",
        "project_id": "okg",
        "sprint_id": "sprint-a",
        "change_id": "change-a",
        "task_id": "task-a",
        "action_digest": "sha256:action",
        "resource_digest": "sha256:resource",
        "scope_digest": "sha256:scope",
        "human_policy_digest": policy.digest,
    }
    values.update(updates)
    return TrustedDecisionContext(**values)


def _repo_policy():
    """This repository's tracked coordination policy (private; not in the public copy)."""
    private_policy_path(".okg/coordination.yaml")
    return coordination_policy(REPO_ROOT)


def _synthetic_policy() -> HumanInvolvementPolicy:
    """A human-involvement policy built from this file's declared registry.

    The behaviour tests need a valid policy, not this repository's, so they
    run in the public copy too. Two private-only tests below read the tracked
    policy: test_current_policy_is_closed_and_separately_digest_bound asserts
    its human-policy digest equals this one's, and
    test_tracked_policy_tables_match_the_declared_registry asserts its action
    and supervised-override tables equal the declared ones. So this synthetic
    policy cannot drift from the real one unnoticed.
    """
    return parse_human_involvement_policy(
        EXPECTED_ACTIONS,
        {
            "exception_only": {"overrides": {}},
            "supervised": {"overrides": EXPECTED_SUPERVISED_OVERRIDES},
        },
        ROUTE,
        SUPPRESSION,
        default_profile="exception_only",
    )


def test_current_policy_is_closed_and_separately_digest_bound() -> None:
    policy = _repo_policy()

    assert policy.human_policy_digest.startswith("sha256:")
    assert policy.human_policy_digest != policy.policy_digest
    assert all(isinstance(action, ActionPolicy) for action in policy.action_registry.values())
    assert policy.human_policy.default_profile == "exception_only"
    assert policy.human_policy.route == tuple(ROUTE["hops"])
    assert policy.human_policy.suppression_fields == tuple(SUPPRESSION["digest_fields"])
    assert policy.human_policy.terminal_statuses == tuple(SUPPRESSION["terminal_statuses"])
    # The synthetic policy the behaviour tests use is this one, digest for digest.
    assert policy.human_policy_digest == _synthetic_policy().digest


def test_tracked_policy_tables_match_the_declared_registry() -> None:
    # Private only: the equality with this repository's tracked tables.
    policy = _repo_policy().human_policy

    assert {
        action: action_policy.disposition
        for action, action_policy in policy.actions.items()
    } == EXPECTED_ACTIONS
    assert dict(policy.profiles["supervised"].overrides) == EXPECTED_SUPERVISED_OVERRIDES


def test_every_tracked_action_has_the_declared_scalar_disposition() -> None:
    # Behaviour over every registered action; runs on the synthetic policy,
    # which the private-only tests above tie to the tracked one.
    policy = _synthetic_policy()
    context = _context(policy)

    assert set(policy.actions) == set(EXPECTED_ACTIONS)
    for action, action_policy in policy.actions.items():
        assert action_policy.disposition == EXPECTED_ACTIONS[action]
        result = evaluate_policy(policy, action, context)
        assert result.disposition == action_policy.disposition
        assert result.request_digest == stable_request_digest(action, context)
        assert result.allowed is (action_policy.disposition in {"allow", "notify"})


def test_unknown_action_and_bad_identity_fail_closed() -> None:
    policy = _synthetic_policy()
    unknown = evaluate_policy(policy, "not-registered", _context(policy))
    wrong_principal = evaluate_policy(
        policy,
        "local_edit",
        _context(policy, assignment_principal="other-principal"),
    )

    assert unknown.disposition == "deny"
    assert unknown.refusal_code == "unknown_action"
    assert wrong_principal.refusal_code == "wrong_principal"


def test_missing_or_stale_human_digest_and_self_approval_are_refused() -> None:
    policy = _synthetic_policy()
    missing = evaluate_policy(
        policy, "local_edit", _context(policy, human_policy_digest=None)
    )
    stale = evaluate_policy(
        policy, "local_edit", _context(policy, human_policy_digest="sha256:old-policy")
    )
    self_approval = evaluate_policy(
        policy,
        "merge",
        _context(policy, authored_by_principal="worker-a"),
    )

    assert missing.refusal_code == "missing_human_policy"
    assert stale.refusal_code == "stale_human_policy"
    assert self_approval.refusal_code == "self_approval"
    assert self_approval.disposition == "deny"


@pytest.mark.parametrize(
    ("field_name", "expected_code"),
    [
        ("mandate", "missing_mandate"),
        ("action_digest", "missing_action_digest"),
        ("resource_digest", "missing_resource"),
        ("scope_digest", "missing_scope"),
    ],
)
def test_policy_requires_exact_mandate_and_scope_digests(
    field_name: str, expected_code: str
) -> None:
    policy = _synthetic_policy()
    result = evaluate_policy(policy, "local_edit", _context(policy, **{field_name: None}))
    assert result.refusal_code == expected_code


def test_profile_is_sparse_and_pointwise_tightening_only() -> None:
    base = {"edit": "allow", "publish": "notify", "delete": "require_approval"}
    strict = parse_human_involvement_policy(
        base,
        {
            "exception_only": {"overrides": {}},
            "strict": {"overrides": {"edit": "require_approval", "publish": "deny"}},
        },
        ROUTE,
        SUPPRESSION,
        default_profile="exception_only",
    )
    assert strict.is_tightening("strict")
    assert (
        evaluate_policy(
            strict,
            "edit",
            _context(strict, human_policy_digest=strict.digest),
            profile="strict",
        ).disposition
        == "require_approval"
    )

    with pytest.raises(DecisionPolicyError, match="relaxes"):
        parse_human_involvement_policy(
            base,
            {
                "exception_only": {"overrides": {}},
                "loose": {"overrides": {"delete": "notify"}},
            },
            ROUTE,
            SUPPRESSION,
            default_profile="exception_only",
        )


@pytest.mark.parametrize(
    ("registry", "profiles", "route", "suppression"),
    [
        ({"edit": {"disposition": "allow"}}, {"default": {"overrides": {}}}, ROUTE, SUPPRESSION),
        ({"edit": "allow"}, {"default": {"rules": []}}, ROUTE, SUPPRESSION),
        ({"edit": "allow"}, {"default": {"overrides": {"edit": {"when": {"role": "worker"}}}}}, ROUTE, SUPPRESSION),
        ({"edit-bar": "allow", "edit_bar": "allow"}, {"default": {"overrides": {}}}, ROUTE, SUPPRESSION),
        ({"edit": "allow"}, {"default": {"overrides": {}}}, ["worker", "coordinator"], SUPPRESSION),
        ({"edit": "allow"}, {"default": {"overrides": {}}}, ROUTE, {**SUPPRESSION, "extra": True}),
    ],
)
def test_ambiguous_or_unknown_policy_shapes_are_rejected(
    registry: object,
    profiles: object,
    route: object,
    suppression: object,
) -> None:
    with pytest.raises(DecisionPolicyError):
        parse_human_involvement_policy(
            registry,
            profiles,
            route,
            suppression,
            default_profile="default",
        )


def test_digest_excludes_route_and_reconnect_fields() -> None:
    policy = _synthetic_policy()
    first = _context(policy)
    second = _context(policy, ancestry=("chief-old",))

    assert stable_request_digest("local_edit", first) == stable_request_digest(
        "local_edit", second
    )


def test_policy_object_rejects_untyped_ambiguous_actions() -> None:
    with pytest.raises(DecisionPolicyError):
        HumanInvolvementPolicy(
            actions={"edit": {"disposition": "allow"}},  # type: ignore[arg-type]
            profiles={"exception_only": HumanInvolvementProfile("exception_only")},
        )


def test_default_profile_must_be_exception_only() -> None:
    with pytest.raises(DecisionPolicyError, match="exception_only"):
        parse_human_involvement_policy(
            {"edit": "allow"},
            {"strict": {"overrides": {}}},
            ROUTE,
            SUPPRESSION,
            default_profile="strict",
        )
