"""Pure evaluation of the tracked human-involvement policy.

The policy is intentionally small. The tracked registry maps each closed
action name to one scalar disposition. A named profile is a sparse map of
whole-action overrides and may only make an action stricter. There is no
expression language or conditional rule precedence in this module.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import hashlib
import json
from types import MappingProxyType
from typing import Any


DISPOSITIONS = ("allow", "notify", "require_approval", "deny")
DISPOSITION_RANK = {name: index for index, name in enumerate(DISPOSITIONS)}
_PROTECTED_ACTIONS = frozenset(
    {
        "policy_change",
        "tier3_program_intent",
        "protected_resource",
        "destructive_external",
        "irreversible_external",
        "merge",
        "credentials_privilege",
        "risk_acceptance",
        "unresolved_p0_p1",
        "out_of_charter",
    }
)
_DENIED_ACTIONS = frozenset(
    {"self_approval", "session_laundering", "wrong_scope", "stale_generation"}
)
_REQUIRED_ROUTE = ("worker", "coordinator", "chief", "operator")
_DEFAULT_PROFILE = "exception_only"
_SUPPRESSION_FIELDS = (
    "assignment_id",
    "assignment_generation",
    "principal",
    "assignment_session_id",
    "mandate",
    "action_digest",
    "resource_digest",
    "scope_digest",
    "human_policy_digest",
)
_TERMINAL_STATUSES = ("granted", "denied", "revoked", "consumed")


class DecisionPolicyError(ValueError):
    """Malformed or unsafe tracked policy input."""


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DecisionPolicyError(f"{field_name} must be a non-blank string")
    return value.strip()


def _disposition(value: Any, field_name: str) -> str:
    result = _text(value, field_name).lower().replace("-", "_")
    if result not in DISPOSITION_RANK:
        raise DecisionPolicyError(
            f"{field_name} must be one of {', '.join(DISPOSITIONS)}"
        )
    return result


def _mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise DecisionPolicyError(f"{field_name} must be a mapping")
    if not all(isinstance(key, str) for key in value):
        raise DecisionPolicyError(f"{field_name} keys must be strings")
    return dict(value)


@dataclass(frozen=True, slots=True)
class ActionPolicy:
    """One registered action and its scalar base disposition."""

    name: str
    disposition: str

    def __post_init__(self) -> None:
        name = _text(self.name, "action.name").lower().replace("-", "_")
        disposition = _disposition(
            self.disposition, f"action.{name}.disposition"
        )
        if name in _PROTECTED_ACTIONS and DISPOSITION_RANK[disposition] < DISPOSITION_RANK[
            "require_approval"
        ]:
            raise DecisionPolicyError(
                f"protected action {name!r} cannot be less strict than require_approval"
            )
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "disposition", disposition)

    def to_json(self) -> str:
        return self.disposition


@dataclass(frozen=True, slots=True)
class HumanInvolvementProfile:
    """Sparse whole-action overrides over the base action registry."""

    name: str
    overrides: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        name = _text(self.name, "profile.name")
        normalized: dict[str, str] = {}
        for action, disposition in dict(self.overrides).items():
            action_name = _text(action, "profile action").lower().replace("-", "_")
            normalized[action_name] = _disposition(
                disposition, f"profile {name}.{action_name}"
            )
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "overrides", MappingProxyType(normalized))

    def to_json(self) -> dict[str, Any]:
        return {"overrides": dict(sorted(self.overrides.items()))}


@dataclass(frozen=True, slots=True)
class TrustedDecisionContext:
    """Server-derived values used by policy evaluation.

    The evaluator accepts this value object but never authenticates it. Only a
    native writer may construct it from locked assignment and route rows.
    """

    principal: str
    role: str
    assignment_id: str
    assignment_generation: int
    assignment_kind: str = ""
    assignment_principal: str | None = None
    assignment_session_id: str | None = None
    assignment_status: str = "active"
    ancestry: tuple[str, ...] = ()
    mandate: str | None = None
    project_id: str | None = None
    sprint_id: str | None = None
    change_id: str | None = None
    task_id: str | None = None
    resource_digest: str | None = None
    action_digest: str | None = None
    scope_digest: str | None = None
    human_policy_digest: str | None = None
    # Retained for callers that already collect these facts. The closed action
    # registry, rather than caller classification, decides the disposition.
    tier: int | None = None
    protected_resource: bool = False
    destructive: bool = False
    irreversible: bool = False
    out_of_charter: bool = False
    authored_by_principal: str | None = None
    implemented_by_principal: str | None = None
    approver_principal: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "principal": self.principal,
            "role": self.role,
            "assignment_id": self.assignment_id,
            "assignment_generation": self.assignment_generation,
            "assignment_kind": self.assignment_kind,
            "assignment_principal": self.assignment_principal,
            "assignment_session_id": self.assignment_session_id,
            "assignment_status": self.assignment_status,
            "ancestry": list(self.ancestry),
            "mandate": self.mandate,
            "project_id": self.project_id,
            "sprint_id": self.sprint_id,
            "change_id": self.change_id,
            "task_id": self.task_id,
            "resource_digest": self.resource_digest,
            "action_digest": self.action_digest,
            "scope_digest": self.scope_digest,
            "human_policy_digest": self.human_policy_digest,
            "tier": self.tier,
            "protected_resource": self.protected_resource,
            "destructive": self.destructive,
            "irreversible": self.irreversible,
            "out_of_charter": self.out_of_charter,
            "authored_by_principal": self.authored_by_principal,
            "implemented_by_principal": self.implemented_by_principal,
            "approver_principal": self.approver_principal,
        }


DecisionContext = TrustedDecisionContext


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    action: str
    disposition: str
    allowed: bool
    human_required: bool
    request_digest: str
    reason: str
    profile: str
    matched_rule: str | None = None
    refusal_code: str | None = None

    @property
    def requires_approval(self) -> bool:
        return self.disposition == "require_approval"

    @property
    def denied(self) -> bool:
        return self.disposition == "deny"

    def as_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "disposition": self.disposition,
            "allowed": self.allowed,
            "human_required": self.human_required,
            "request_digest": self.request_digest,
            "reason": self.reason,
            "profile": self.profile,
            "matched_rule": self.matched_rule,
            "refusal_code": self.refusal_code,
        }


DecisionResult = PolicyDecision


@dataclass(frozen=True, slots=True)
class HumanInvolvementPolicy:
    """Validated tracked action registry, sparse profiles, and route contract."""

    actions: Mapping[str, ActionPolicy]
    profiles: Mapping[str, HumanInvolvementProfile]
    default_profile: str = "exception_only"
    route: tuple[str, ...] = _REQUIRED_ROUTE
    suppression_fields: tuple[str, ...] = _SUPPRESSION_FIELDS
    terminal_statuses: tuple[str, ...] = _TERMINAL_STATUSES

    def __post_init__(self) -> None:
        actions = dict(self.actions)
        if not actions:
            raise DecisionPolicyError("action_registry must contain at least one action")
        if any(not isinstance(action, ActionPolicy) for action in actions.values()):
            raise DecisionPolicyError("action_registry values must be scalar action policies")
        if set(actions) != {action.name for action in actions.values()}:
            raise DecisionPolicyError("action_registry keys must match action names")
        profiles = dict(self.profiles)
        if not profiles:
            raise DecisionPolicyError("human_involvement_profiles must not be empty")
        if self.default_profile not in profiles:
            raise DecisionPolicyError(
                f"unknown default human-involvement profile {self.default_profile!r}"
            )
        if self.default_profile != _DEFAULT_PROFILE:
            raise DecisionPolicyError(
                "default human-involvement profile must be exception_only"
            )
        if any(not isinstance(profile, HumanInvolvementProfile) for profile in profiles.values()):
            raise DecisionPolicyError("human-involvement profiles must be typed")
        if tuple(self.route) != _REQUIRED_ROUTE:
            raise DecisionPolicyError(
                "escalation_route must be worker -> coordinator -> chief -> operator"
            )
        if tuple(self.suppression_fields) != _SUPPRESSION_FIELDS:
            raise DecisionPolicyError(
                "decision_suppression.digest_fields are not the closed contract"
            )
        if tuple(self.terminal_statuses) != _TERMINAL_STATUSES:
            raise DecisionPolicyError(
                "decision_suppression.terminal_statuses are not the closed contract"
            )
        for profile_name, profile in profiles.items():
            if profile.name != profile_name:
                raise DecisionPolicyError(
                    f"profile {profile_name!r} key does not match its name"
                )
            for action, disposition in profile.overrides.items():
                if action not in actions:
                    raise DecisionPolicyError(
                        f"profile {profile_name!r} names unknown action {action!r}"
                    )
                if DISPOSITION_RANK[disposition] < DISPOSITION_RANK[actions[action].disposition]:
                    raise DecisionPolicyError(
                        f"profile {profile_name!r} relaxes action {action!r}"
                    )
        object.__setattr__(self, "actions", MappingProxyType(actions))
        object.__setattr__(self, "profiles", MappingProxyType(profiles))
        object.__setattr__(self, "route", tuple(self.route))
        object.__setattr__(self, "suppression_fields", tuple(self.suppression_fields))
        object.__setattr__(self, "terminal_statuses", tuple(self.terminal_statuses))

    @property
    def digest(self) -> str:
        return _digest(self.to_json())

    @property
    def human_policy_digest(self) -> str:
        return self.digest

    @property
    def action_registry(self) -> Mapping[str, ActionPolicy]:
        return self.actions

    def to_json(self) -> dict[str, Any]:
        return {
            "action_registry": {
                name: action.to_json() for name, action in sorted(self.actions.items())
            },
            "human_involvement_profiles": {
                name: profile.to_json()
                for name, profile in sorted(self.profiles.items())
            },
            "default_profile": self.default_profile,
            "escalation_route": {"hops": list(self.route)},
            "decision_suppression": {
                "digest_fields": list(self.suppression_fields),
                "terminal_statuses": list(self.terminal_statuses),
            },
        }

    def is_tightening(self, profile_name: str) -> bool:
        return profile_name in self.profiles

    def evaluate(
        self,
        action: str,
        context: TrustedDecisionContext,
        *,
        profile: str | None = None,
    ) -> PolicyDecision:
        return evaluate_policy(self, action, context, profile=profile)


TrackedHumanPolicy = HumanInvolvementPolicy


def stable_request_digest(action: str, context: TrustedDecisionContext) -> str:
    """Return the reconnect-stable, exact-action authorization key."""

    payload = {
        "action": _text(action, "action").lower().replace("-", "_"),
        "assignment_id": context.assignment_id,
        "assignment_generation": context.assignment_generation,
        "principal": context.principal,
        "assignment_session_id": context.assignment_session_id,
        "assignment_kind": context.assignment_kind,
        "project_id": context.project_id,
        "sprint_id": context.sprint_id,
        "change_id": context.change_id,
        "task_id": context.task_id,
        "mandate": context.mandate,
        "human_policy_digest": context.human_policy_digest,
        "action_digest": context.action_digest,
        "resource_digest": context.resource_digest,
        "scope_digest": context.scope_digest,
    }
    return _digest(payload)


request_digest = stable_request_digest
authorization_digest = stable_request_digest


def _invalid_context(context: TrustedDecisionContext) -> tuple[str, str] | None:
    if not _is_non_blank(context.principal) or not _is_non_blank(context.role):
        return "missing_identity", "authenticated principal and role are required"
    if not _is_non_blank(context.assignment_id):
        return "missing_assignment", "assignment identity is required"
    if isinstance(context.assignment_generation, bool) or context.assignment_generation < 1:
        return "stale_generation", "assignment generation is invalid"
    for field_name, value, code in (
        ("mandate", context.mandate, "missing_mandate"),
        ("action digest", context.action_digest, "missing_action_digest"),
        ("resource digest", context.resource_digest, "missing_resource"),
        ("scope digest", context.scope_digest, "missing_scope"),
    ):
        if not _is_non_blank(value):
            return code, f"{field_name} is required"
    if context.assignment_status != "active":
        return "assignment_not_active", "assignment is not active"
    if context.assignment_principal and context.assignment_principal != context.principal:
        return "wrong_principal", "principal does not own the current assignment"
    if context.approver_principal and context.approver_principal != context.principal:
        return "wrong_principal", "approver principal is not the authenticated principal"
    if context.authored_by_principal and context.authored_by_principal == context.principal:
        return "self_approval", "a principal cannot approve work it authored"
    if context.implemented_by_principal and context.implemented_by_principal == context.principal:
        return "self_approval", "a principal cannot approve work it implemented"
    if context.principal in context.ancestry:
        return "self_approval", "a principal cannot approve work in its own ancestry"
    return None


def _is_non_blank(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def evaluate_policy(
    policy: HumanInvolvementPolicy,
    action: str,
    context: TrustedDecisionContext,
    *,
    profile: str | None = None,
) -> PolicyDecision:
    """Evaluate one action with deterministic, fail-closed precedence."""

    action_name = str(action or "").strip().lower().replace("-", "_")
    selected = profile or policy.default_profile
    digest = stable_request_digest(action_name or "<unknown>", context)

    def denied(reason: str, code: str) -> PolicyDecision:
        return PolicyDecision(
            action=action_name,
            disposition="deny",
            allowed=False,
            human_required=False,
            request_digest=digest,
            reason=reason,
            profile=selected,
            refusal_code=code,
        )

    if not action_name or action_name not in policy.actions:
        return denied("action is not in the closed action registry", "unknown_action")
    if selected not in policy.profiles:
        return denied("selected human-involvement profile is unknown", "unknown_profile")
    context_error = _invalid_context(context)
    if context_error:
        code, reason = context_error
        return denied(reason, code)
    if not _is_non_blank(context.human_policy_digest):
        return denied("context has no human policy digest", "missing_human_policy")
    if context.human_policy_digest != policy.digest:
        return denied(
            "context was evaluated under a stale human policy digest",
            "stale_human_policy",
        )

    action_policy = policy.actions[action_name]
    disposition = policy.profiles[selected].overrides.get(
        action_name, action_policy.disposition
    )
    if action_name in _DENIED_ACTIONS:
        disposition = "deny"
        matched_rule = "safety-floor"
    else:
        matched_rule = (
            f"profile:{selected}"
            if action_name in policy.profiles[selected].overrides
            else None
        )
    return PolicyDecision(
        action=action_name,
        disposition=disposition,
        allowed=disposition in {"allow", "notify"},
        human_required=disposition == "require_approval",
        request_digest=digest,
        reason={
            "allow": "action is authorized by the current mandate",
            "notify": "action is authorized and recorded for chief visibility",
            "require_approval": "current policy requires an operator decision",
            "deny": "current policy denies this action",
        }[disposition],
        profile=selected,
        matched_rule=matched_rule,
        refusal_code="policy_denied" if disposition == "deny" else None,
    )


evaluate_decision = evaluate_policy
evaluate_human_involvement = evaluate_policy


def parse_human_involvement_policy(
    action_registry: Any,
    human_involvement_profiles: Any,
    escalation_route: Any,
    decision_suppression: Any,
    *,
    default_profile: str = "exception_only",
) -> HumanInvolvementPolicy:
    """Parse the four tracked PACT-owned keys as one closed shape."""

    registry = _mapping(action_registry, "coordination.action_registry")
    if any(not isinstance(value, str) for value in registry.values()):
        raise DecisionPolicyError(
            "coordination.action_registry values must be scalar dispositions"
        )
    normalized_names = [
        _text(name, "action name").lower().replace("-", "_")
        for name in registry
    ]
    if len(normalized_names) != len(set(normalized_names)):
        raise DecisionPolicyError("action_registry contains ambiguous normalized names")
    actions = {
        name: ActionPolicy(name=name, disposition=value)
        for name, value in registry.items()
    }

    profile_raw = _mapping(
        human_involvement_profiles, "coordination.human_involvement_profiles"
    )
    profiles: dict[str, HumanInvolvementProfile] = {}
    for name, value in profile_raw.items():
        profile = _mapping(value, f"profile {name}")
        if set(profile) != {"overrides"}:
            raise DecisionPolicyError(
                f"profile {name} must contain only the overrides mapping"
            )
        overrides = _mapping(profile["overrides"], f"profile {name}.overrides")
        if any(not isinstance(item, str) for item in overrides.values()):
            raise DecisionPolicyError(
                f"profile {name}.overrides values must be scalar dispositions"
            )
        normalized_overrides = [
            _text(action, "profile action").lower().replace("-", "_")
            for action in overrides
        ]
        if len(normalized_overrides) != len(set(normalized_overrides)):
            raise DecisionPolicyError(
                f"profile {name}.overrides contains ambiguous normalized names"
            )
        profiles[name] = HumanInvolvementProfile(name=name, overrides=overrides)

    default_profile = _text(default_profile, "coordination.default_profile")
    route = _mapping(escalation_route, "coordination.escalation_route")
    if set(route) != {"hops"} or route["hops"] != list(_REQUIRED_ROUTE):
        raise DecisionPolicyError(
            "coordination.escalation_route must contain exactly the canonical hops"
        )
    suppression = _mapping(
        decision_suppression, "coordination.decision_suppression"
    )
    if set(suppression) != {"digest_fields", "terminal_statuses"}:
        raise DecisionPolicyError(
            "coordination.decision_suppression has an unsupported shape"
        )
    if suppression["digest_fields"] != list(_SUPPRESSION_FIELDS):
        raise DecisionPolicyError(
            "coordination.decision_suppression.digest_fields are not the closed contract"
        )
    if suppression["terminal_statuses"] != list(_TERMINAL_STATUSES):
        raise DecisionPolicyError(
            "coordination.decision_suppression.terminal_statuses are not the closed contract"
        )
    return HumanInvolvementPolicy(
        actions=actions,
        profiles=profiles,
        default_profile=default_profile,
    )
