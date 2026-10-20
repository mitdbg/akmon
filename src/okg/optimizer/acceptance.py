"""Runtime acceptance records for Althea rewrite promotion."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from types import MappingProxyType
from typing import Any


class RewriteAcceptanceError(ValueError):
    """Raised when a rewrite promotion request is not acceptance-authoritative."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


class RewriteAssertionStatus(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    BLOCKED = "blocked"


class RewriteReplayDecision(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    BLOCKED = "blocked"


class RewritePromotionDecisionValue(str, Enum):
    PROMOTE = "promote"
    REJECT = "reject"
    BLOCK = "block"


@dataclass(frozen=True)
class RewriteProgram:
    program_id: str
    program_digest: str
    dialect: str
    surface: str
    parameter_schema_digest: str
    materialization_plan_digest: str
    owner: str
    created_at: datetime

    def __post_init__(self) -> None:
        _require_non_empty("program_id", self.program_id)
        _require_non_empty("program_digest", self.program_digest)
        _require_non_empty("dialect", self.dialect)
        _require_non_empty("surface", self.surface)
        _require_non_empty("parameter_schema_digest", self.parameter_schema_digest)
        _require_non_empty(
            "materialization_plan_digest",
            self.materialization_plan_digest,
        )
        _require_non_empty("owner", self.owner)
        _require_aware_datetime("created_at", self.created_at)


@dataclass(frozen=True)
class RewriteCandidateSet:
    candidate_set_id: str
    program_digest: str
    source_generation_id: str
    candidate_ids: tuple[str, ...]
    candidate_set_digest: str
    evidence_refs: tuple[str, ...]
    conflict_groups: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def __post_init__(self) -> None:
        _require_non_empty("candidate_set_id", self.candidate_set_id)
        _require_non_empty("program_digest", self.program_digest)
        _require_non_empty("source_generation_id", self.source_generation_id)
        _require_non_empty("candidate_set_digest", self.candidate_set_digest)
        object.__setattr__(
            self,
            "candidate_ids",
            _tuple_of_non_empty("candidate_ids", self.candidate_ids),
        )
        object.__setattr__(
            self,
            "evidence_refs",
            _tuple_of_non_empty("evidence_refs", self.evidence_refs),
        )
        object.__setattr__(
            self,
            "conflict_groups",
            _string_mapping("conflict_groups", self.conflict_groups),
        )


@dataclass(frozen=True)
class RewriteBranchTrial:
    trial_id: str
    source_generation_id: str
    trial_branch: str
    trial_generation_id: str
    program_digest: str
    candidate_set_digest: str
    delta_digest: str
    assertion_digest: str
    created_at: datetime

    def __post_init__(self) -> None:
        _require_non_empty("trial_id", self.trial_id)
        _require_non_empty("source_generation_id", self.source_generation_id)
        _require_non_empty("trial_branch", self.trial_branch)
        _require_non_empty("trial_generation_id", self.trial_generation_id)
        _require_non_empty("program_digest", self.program_digest)
        _require_non_empty("candidate_set_digest", self.candidate_set_digest)
        _require_non_empty("delta_digest", self.delta_digest)
        _require_non_empty("assertion_digest", self.assertion_digest)
        _require_aware_datetime("created_at", self.created_at)


@dataclass(frozen=True)
class RewriteAssertionEvidence:
    assertion_id: str
    trial_id: str
    checker_id: str
    property_id: str
    source_generation_id: str
    trial_generation_id: str
    artifact: str
    status: RewriteAssertionStatus
    fresh_until: datetime
    input_digest: str

    def __post_init__(self) -> None:
        _require_non_empty("assertion_id", self.assertion_id)
        _require_non_empty("trial_id", self.trial_id)
        _require_non_empty("checker_id", self.checker_id)
        _require_non_empty("property_id", self.property_id)
        _require_non_empty("source_generation_id", self.source_generation_id)
        _require_non_empty("trial_generation_id", self.trial_generation_id)
        _require_non_empty("artifact", self.artifact)
        object.__setattr__(self, "status", RewriteAssertionStatus(self.status))
        _require_aware_datetime("fresh_until", self.fresh_until)
        _require_non_empty("input_digest", self.input_digest)


@dataclass(frozen=True)
class RewriteReplayMeasurement:
    measurement_id: str
    trial_id: str
    workload_id: str
    workload_owner: str
    baseline_generation_id: str
    trial_generation_id: str
    runner_id: str
    runner_digest: str
    measurement_command: str
    artifact_refs: tuple[str, ...]
    metric_values: Mapping[str, float]
    policy_id: str
    threshold_values: Mapping[str, float]
    decision: RewriteReplayDecision
    measured_at: datetime
    fresh_until: datetime

    def __post_init__(self) -> None:
        _require_non_empty("measurement_id", self.measurement_id)
        _require_non_empty("trial_id", self.trial_id)
        _require_non_empty("workload_id", self.workload_id)
        _require_non_empty("workload_owner", self.workload_owner)
        _require_non_empty("baseline_generation_id", self.baseline_generation_id)
        _require_non_empty("trial_generation_id", self.trial_generation_id)
        _require_non_empty("runner_id", self.runner_id)
        _require_non_empty("runner_digest", self.runner_digest)
        _require_non_empty("measurement_command", self.measurement_command)
        object.__setattr__(
            self,
            "artifact_refs",
            _tuple_of_non_empty("artifact_refs", self.artifact_refs),
        )
        object.__setattr__(
            self,
            "metric_values",
            _numeric_mapping("metric_values", self.metric_values),
        )
        _require_non_empty("policy_id", self.policy_id)
        object.__setattr__(
            self,
            "threshold_values",
            _numeric_mapping("threshold_values", self.threshold_values),
        )
        object.__setattr__(self, "decision", RewriteReplayDecision(self.decision))
        _require_aware_datetime("measured_at", self.measured_at)
        _require_aware_datetime("fresh_until", self.fresh_until)


@dataclass(frozen=True)
class RewritePromotionDecision:
    promotion_id: str
    trial_id: str
    target_branch: str
    actor: str
    policy_id: str
    decision: RewritePromotionDecisionValue
    reason: str
    evidence_digest: str
    measurement_id: str
    created_at: datetime

    def __post_init__(self) -> None:
        _require_non_empty("promotion_id", self.promotion_id)
        _require_non_empty("trial_id", self.trial_id)
        _require_non_empty("target_branch", self.target_branch)
        _require_non_empty("actor", self.actor)
        _require_non_empty("policy_id", self.policy_id)
        object.__setattr__(
            self,
            "decision",
            RewritePromotionDecisionValue(self.decision),
        )
        _require_non_empty("reason", self.reason)
        _require_non_empty("evidence_digest", self.evidence_digest)
        _require_non_empty("measurement_id", self.measurement_id)
        _require_aware_datetime("created_at", self.created_at)


@dataclass(frozen=True)
class RewriteAcceptanceReport:
    ok: bool
    promotion_id: str
    trial_id: str
    program_digest: str
    candidate_set_digest: str
    delta_digest: str
    evidence_digest: str
    measurement_id: str
    target_branch: str


def digest_rewrite_assertions(
    assertions: Iterable[RewriteAssertionEvidence],
) -> str:
    """Return a deterministic digest for assertion evidence rows."""

    rows = []
    for assertion in assertions:
        rows.append(
            {
                "artifact": assertion.artifact,
                "assertion_id": assertion.assertion_id,
                "checker_id": assertion.checker_id,
                "fresh_until": assertion.fresh_until.isoformat(),
                "input_digest": assertion.input_digest,
                "property_id": assertion.property_id,
                "source_generation_id": assertion.source_generation_id,
                "status": assertion.status.value,
                "trial_generation_id": assertion.trial_generation_id,
                "trial_id": assertion.trial_id,
            }
        )
    payload = json.dumps(
        sorted(rows, key=lambda row: row["assertion_id"]),
        separators=(",", ":"),
        sort_keys=True,
    )
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def check_rewrite_promotion_acceptance(
    *,
    program: RewriteProgram,
    candidate_set: RewriteCandidateSet,
    trial: RewriteBranchTrial,
    assertions: Iterable[RewriteAssertionEvidence],
    replay: RewriteReplayMeasurement | None,
    promotion: RewritePromotionDecision,
    supported_dialects: Iterable[str],
    now: datetime,
    default_branch: str = "default",
) -> RewriteAcceptanceReport:
    """Validate that a promotion decision is bound to fresh trial evidence."""

    _require_aware_datetime("now", now)
    assertions_tuple = tuple(assertions)
    supported = frozenset(supported_dialects)
    if program.dialect not in supported:
        raise RewriteAcceptanceError(
            "unsupported_dialect",
            f"rewrite dialect {program.dialect!r} is not supported",
        )
    _require_equal(
        "candidate_program_digest_mismatch",
        "candidate set program digest must match rewrite program",
        candidate_set.program_digest,
        program.program_digest,
    )
    _require_equal(
        "trial_program_digest_mismatch",
        "trial program digest must match rewrite program",
        trial.program_digest,
        program.program_digest,
    )
    _require_equal(
        "trial_candidate_set_digest_mismatch",
        "trial candidate set digest must match candidate set",
        trial.candidate_set_digest,
        candidate_set.candidate_set_digest,
    )
    _require_equal(
        "trial_source_generation_mismatch",
        "trial source generation must match candidate source generation",
        trial.source_generation_id,
        candidate_set.source_generation_id,
    )
    if trial.trial_branch == default_branch:
        raise RewriteAcceptanceError(
            "default_branch_trial",
            "rewrite promotion requires a non-default branch trial",
        )
    _require_equal(
        "promotion_trial_mismatch",
        "promotion decision must reference the exact trial",
        promotion.trial_id,
        trial.trial_id,
    )
    _require_equal(
        "promotion_target_mismatch",
        "promotion gate only accepts the configured default branch target",
        promotion.target_branch,
        default_branch,
    )
    if promotion.decision is not RewritePromotionDecisionValue.PROMOTE:
        raise RewriteAcceptanceError(
            "promotion_not_approved",
            "promotion decision must be promote",
        )
    _require_assertions_fresh_and_bound(assertions_tuple, trial=trial, now=now)
    assertion_digest = digest_rewrite_assertions(assertions_tuple)
    _require_equal(
        "trial_assertion_digest_mismatch",
        "trial assertion digest must match checked assertion evidence",
        trial.assertion_digest,
        assertion_digest,
    )
    _require_equal(
        "promotion_evidence_digest_mismatch",
        "promotion evidence digest must match checked assertion evidence",
        promotion.evidence_digest,
        assertion_digest,
    )
    if replay is None:
        raise RewriteAcceptanceError(
            "missing_replay_measurement",
            "model scores are hints; promotion requires replay measurement",
        )
    _require_replay_fresh_and_bound(
        replay,
        trial=trial,
        promotion=promotion,
        now=now,
    )
    return RewriteAcceptanceReport(
        ok=True,
        promotion_id=promotion.promotion_id,
        trial_id=trial.trial_id,
        program_digest=program.program_digest,
        candidate_set_digest=candidate_set.candidate_set_digest,
        delta_digest=trial.delta_digest,
        evidence_digest=assertion_digest,
        measurement_id=replay.measurement_id,
        target_branch=promotion.target_branch,
    )


def _require_assertions_fresh_and_bound(
    assertions: tuple[RewriteAssertionEvidence, ...],
    *,
    trial: RewriteBranchTrial,
    now: datetime,
) -> None:
    if not assertions:
        raise RewriteAcceptanceError(
            "missing_assertion_evidence",
            "promotion requires at least one assertion evidence record",
        )
    for assertion in assertions:
        _require_equal(
            "assertion_trial_mismatch",
            "assertion evidence must reference the exact trial",
            assertion.trial_id,
            trial.trial_id,
        )
        _require_equal(
            "assertion_source_generation_mismatch",
            "assertion evidence must reference the trial source generation",
            assertion.source_generation_id,
            trial.source_generation_id,
        )
        _require_equal(
            "assertion_trial_generation_mismatch",
            "assertion evidence must reference the trial generation",
            assertion.trial_generation_id,
            trial.trial_generation_id,
        )
        if assertion.status is not RewriteAssertionStatus.PASS:
            raise RewriteAcceptanceError(
                "assertion_not_passing",
                f"assertion {assertion.assertion_id!r} status is "
                f"{assertion.status.value!r}",
            )
        _require_fresh(
            "stale_assertion_evidence",
            "assertion evidence is stale",
            assertion.fresh_until,
            now,
        )


def _require_replay_fresh_and_bound(
    replay: RewriteReplayMeasurement,
    *,
    trial: RewriteBranchTrial,
    promotion: RewritePromotionDecision,
    now: datetime,
) -> None:
    _require_equal(
        "replay_trial_mismatch",
        "replay measurement must reference the exact trial",
        replay.trial_id,
        trial.trial_id,
    )
    _require_equal(
        "replay_baseline_generation_mismatch",
        "replay baseline must match the trial source generation",
        replay.baseline_generation_id,
        trial.source_generation_id,
    )
    _require_equal(
        "replay_trial_generation_mismatch",
        "replay measurement must reference the trial generation",
        replay.trial_generation_id,
        trial.trial_generation_id,
    )
    _require_equal(
        "promotion_measurement_mismatch",
        "promotion decision must reference the replay measurement",
        promotion.measurement_id,
        replay.measurement_id,
    )
    _require_equal(
        "promotion_policy_mismatch",
        "promotion policy must match replay policy",
        promotion.policy_id,
        replay.policy_id,
    )
    if replay.decision is not RewriteReplayDecision.PASS:
        raise RewriteAcceptanceError(
            "replay_not_passing",
            f"replay measurement {replay.measurement_id!r} decision is "
            f"{replay.decision.value!r}",
        )
    _require_fresh(
        "stale_replay_measurement",
        "replay measurement is stale",
        replay.fresh_until,
        now,
    )
    if replay.measured_at > now:
        raise RewriteAcceptanceError(
            "future_replay_measurement",
            "replay measurement timestamp is in the future",
        )


def _require_non_empty(field_name: str, value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        raise RewriteAcceptanceError(
            "missing_required_field",
            f"{field_name} must be non-empty",
        )


def _tuple_of_non_empty(field_name: str, values: Iterable[str]) -> tuple[str, ...]:
    if isinstance(values, str | bytes):
        raise RewriteAcceptanceError(
            "missing_required_field",
            f"{field_name} must contain at least one value",
        )
    tuple_value = tuple(values)
    if not tuple_value:
        raise RewriteAcceptanceError(
            "missing_required_field",
            f"{field_name} must contain at least one value",
        )
    for value in tuple_value:
        _require_non_empty(field_name, value)
    return tuple_value


def _string_mapping(field_name: str, values: Mapping[str, str]) -> Mapping[str, str]:
    copied = dict(values)
    for key, value in copied.items():
        _require_non_empty(f"{field_name} key", key)
        _require_non_empty(f"{field_name}[{key}]", value)
    return MappingProxyType(copied)


def _numeric_mapping(
    field_name: str,
    values: Mapping[str, float],
) -> Mapping[str, float]:
    copied = dict(values)
    if not copied:
        raise RewriteAcceptanceError(
            "missing_required_field",
            f"{field_name} must contain at least one value",
        )
    normalized: dict[str, float] = {}
    for key, value in copied.items():
        _require_non_empty(f"{field_name} key", key)
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise RewriteAcceptanceError(
                "invalid_numeric_field",
                f"{field_name}[{key}] must be numeric",
            )
        normalized[key] = float(value)
    return MappingProxyType(normalized)


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise RewriteAcceptanceError(
            "naive_datetime",
            f"{field_name} must include timezone information",
        )


def _require_fresh(
    code: str,
    message: str,
    fresh_until: datetime,
    now: datetime,
) -> None:
    if fresh_until <= now:
        raise RewriteAcceptanceError(code, message)


def _require_equal(code: str, message: str, left: Any, right: Any) -> None:
    if left != right:
        raise RewriteAcceptanceError(code, message)


__all__ = [
    "RewriteAcceptanceError",
    "RewriteAcceptanceReport",
    "RewriteAssertionEvidence",
    "RewriteAssertionStatus",
    "RewriteBranchTrial",
    "RewriteCandidateSet",
    "RewriteProgram",
    "RewritePromotionDecision",
    "RewritePromotionDecisionValue",
    "RewriteReplayDecision",
    "RewriteReplayMeasurement",
    "check_rewrite_promotion_acceptance",
    "digest_rewrite_assertions",
]
