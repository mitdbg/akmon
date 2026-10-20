from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

import pytest

from okg.optimizer import (
    RewriteAcceptanceError,
    RewriteAssertionEvidence,
    RewriteAssertionStatus,
    RewriteBranchTrial,
    RewriteCandidateSet,
    RewriteProgram,
    RewritePromotionDecision,
    RewritePromotionDecisionValue,
    RewriteReplayDecision,
    RewriteReplayMeasurement,
    check_rewrite_promotion_acceptance,
    digest_rewrite_assertions,
)


NOW = datetime(2026, 1, 1, tzinfo=UTC)
FRESH_UNTIL = NOW + timedelta(hours=1)
STALE_AT = NOW - timedelta(seconds=1)


@dataclass(frozen=True)
class _AcceptanceRecords:
    program: RewriteProgram
    candidate_set: RewriteCandidateSet
    trial: RewriteBranchTrial
    assertions: tuple[RewriteAssertionEvidence, ...]
    replay: RewriteReplayMeasurement
    promotion: RewritePromotionDecision


def test_acceptance_gate_accepts_exact_fresh_default_branch_promotion() -> None:
    records = _records()

    report = check_rewrite_promotion_acceptance(
        program=records.program,
        candidate_set=records.candidate_set,
        trial=records.trial,
        assertions=records.assertions,
        replay=records.replay,
        promotion=records.promotion,
        supported_dialects=("althea.ir.v1",),
        now=NOW,
    )

    assert report.ok is True
    assert report.promotion_id == "promotion:1"
    assert report.trial_id == "trial:1"
    assert report.program_digest == "sha256:program"
    assert report.candidate_set_digest == "sha256:candidates"
    assert report.delta_digest == "sha256:delta"
    assert report.evidence_digest == records.trial.assertion_digest
    assert report.measurement_id == "measurement:1"
    assert report.target_branch == "default"


def test_records_normalize_and_freeze_collection_fields() -> None:
    records = _records()

    assert records.candidate_set.candidate_ids == ("candidate:1",)
    assert records.candidate_set.evidence_refs == ("trace:1",)
    assert records.replay.artifact_refs == (".scratch/replay.json",)
    assert records.replay.metric_values == {"latency_ms": 10.0}
    assert records.replay.threshold_values == {"max_latency_ms": 20.0}

    with pytest.raises(TypeError):
        records.candidate_set.conflict_groups["candidate:1"] = "other"

    with pytest.raises(TypeError):
        records.replay.metric_values["latency_ms"] = 12.0

    with pytest.raises(TypeError):
        records.replay.threshold_values["max_latency_ms"] = 12.0


def test_records_reject_missing_required_string_fields() -> None:
    with pytest.raises(RewriteAcceptanceError) as excinfo:
        RewriteProgram(
            program_id=None,
            program_digest="sha256:program",
            dialect="althea.ir.v1",
            surface="topology",
            parameter_schema_digest="sha256:params",
            materialization_plan_digest="sha256:materialization",
            owner="optimizer",
            created_at=NOW,
        )

    assert excinfo.value.code == "missing_required_field"


def test_records_reject_string_instead_of_sequence_fields() -> None:
    with pytest.raises(RewriteAcceptanceError) as excinfo:
        RewriteCandidateSet(
            candidate_set_id="candidate-set:1",
            program_digest="sha256:program",
            source_generation_id="gen:source",
            candidate_ids="candidate:1",
            candidate_set_digest="sha256:candidates",
            evidence_refs=("trace:1",),
        )

    assert excinfo.value.code == "missing_required_field"


def test_acceptance_gate_rejects_model_score_only_promotion() -> None:
    records = _records()

    with pytest.raises(RewriteAcceptanceError) as excinfo:
        check_rewrite_promotion_acceptance(
            program=records.program,
            candidate_set=records.candidate_set,
            trial=records.trial,
            assertions=records.assertions,
            replay=None,
            promotion=records.promotion,
            supported_dialects=("althea.ir.v1",),
            now=NOW,
        )

    assert excinfo.value.code == "missing_replay_measurement"


def test_acceptance_gate_rejects_missing_assertion_evidence() -> None:
    records = _records()

    with pytest.raises(RewriteAcceptanceError) as excinfo:
        check_rewrite_promotion_acceptance(
            program=records.program,
            candidate_set=records.candidate_set,
            trial=records.trial,
            assertions=(),
            replay=records.replay,
            promotion=records.promotion,
            supported_dialects=("althea.ir.v1",),
            now=NOW,
        )

    assert excinfo.value.code == "missing_assertion_evidence"


def test_acceptance_gate_rejects_default_branch_trial() -> None:
    records = _records(trial_branch="default")

    with pytest.raises(RewriteAcceptanceError) as excinfo:
        _check(records)

    assert excinfo.value.code == "default_branch_trial"


def test_acceptance_gate_rejects_stale_assertion_evidence() -> None:
    records = _records(assertion_fresh_until=STALE_AT)

    with pytest.raises(RewriteAcceptanceError) as excinfo:
        _check(records)

    assert excinfo.value.code == "stale_assertion_evidence"


def test_acceptance_gate_rejects_failed_assertion_evidence() -> None:
    records = _records(assertion_status=RewriteAssertionStatus.FAIL)

    with pytest.raises(RewriteAcceptanceError) as excinfo:
        _check(records)

    assert excinfo.value.code == "assertion_not_passing"


def test_acceptance_gate_rejects_wrong_generation_replay() -> None:
    records = _records()
    records = replace(
        records,
        replay=replace(records.replay, trial_generation_id="gen:other-trial"),
    )

    with pytest.raises(RewriteAcceptanceError) as excinfo:
        _check(records)

    assert excinfo.value.code == "replay_trial_generation_mismatch"


@pytest.mark.parametrize(
    "decision",
    [RewriteReplayDecision.FAIL, RewriteReplayDecision.BLOCKED],
)
def test_acceptance_gate_rejects_non_passing_replay(
    decision: RewriteReplayDecision,
) -> None:
    records = _records(replay_decision=decision)

    with pytest.raises(RewriteAcceptanceError) as excinfo:
        _check(records)

    assert excinfo.value.code == "replay_not_passing"


def test_acceptance_gate_rejects_stale_replay() -> None:
    records = _records(replay_fresh_until=STALE_AT)

    with pytest.raises(RewriteAcceptanceError) as excinfo:
        _check(records)

    assert excinfo.value.code == "stale_replay_measurement"


def test_acceptance_gate_rejects_future_replay_measurement() -> None:
    records = _records(replay_measured_at=NOW + timedelta(seconds=1))

    with pytest.raises(RewriteAcceptanceError) as excinfo:
        _check(records)

    assert excinfo.value.code == "future_replay_measurement"


def test_acceptance_gate_rejects_promotion_policy_mismatch() -> None:
    records = _records()
    records = replace(
        records,
        promotion=replace(records.promotion, policy_id="policy:other"),
    )

    with pytest.raises(RewriteAcceptanceError) as excinfo:
        _check(records)

    assert excinfo.value.code == "promotion_policy_mismatch"


def test_acceptance_gate_rejects_unsupported_dialect() -> None:
    records = _records()

    with pytest.raises(RewriteAcceptanceError) as excinfo:
        check_rewrite_promotion_acceptance(
            program=records.program,
            candidate_set=records.candidate_set,
            trial=records.trial,
            assertions=records.assertions,
            replay=records.replay,
            promotion=records.promotion,
            supported_dialects=("althea.ir.v2",),
            now=NOW,
        )

    assert excinfo.value.code == "unsupported_dialect"


def test_acceptance_gate_rejects_wrong_promotion_evidence_digest() -> None:
    records = _records()
    records = replace(
        records,
        promotion=replace(records.promotion, evidence_digest="sha256:wrong"),
    )

    with pytest.raises(RewriteAcceptanceError) as excinfo:
        _check(records)

    assert excinfo.value.code == "promotion_evidence_digest_mismatch"


def test_acceptance_gate_rejects_wrong_promotion_measurement() -> None:
    records = _records()
    records = replace(
        records,
        promotion=replace(records.promotion, measurement_id="measurement:other"),
    )

    with pytest.raises(RewriteAcceptanceError) as excinfo:
        _check(records)

    assert excinfo.value.code == "promotion_measurement_mismatch"


def test_acceptance_gate_rejects_non_promote_decision() -> None:
    records = _records()
    records = replace(
        records,
        promotion=replace(
            records.promotion,
            decision=RewritePromotionDecisionValue.REJECT,
        ),
    )

    with pytest.raises(RewriteAcceptanceError) as excinfo:
        _check(records)

    assert excinfo.value.code == "promotion_not_approved"


def _check(records: _AcceptanceRecords) -> None:
    check_rewrite_promotion_acceptance(
        program=records.program,
        candidate_set=records.candidate_set,
        trial=records.trial,
        assertions=records.assertions,
        replay=records.replay,
        promotion=records.promotion,
        supported_dialects=("althea.ir.v1",),
        now=NOW,
    )


def _records(
    *,
    dialect: str = "althea.ir.v1",
    trial_branch: str = "trial/rewrite-1",
    assertion_status: RewriteAssertionStatus = RewriteAssertionStatus.PASS,
    assertion_fresh_until: datetime = FRESH_UNTIL,
    replay_decision: RewriteReplayDecision = RewriteReplayDecision.PASS,
    replay_fresh_until: datetime = FRESH_UNTIL,
    replay_measured_at: datetime = NOW,
) -> _AcceptanceRecords:
    program = RewriteProgram(
        program_id="program:1",
        program_digest="sha256:program",
        dialect=dialect,
        surface="topology",
        parameter_schema_digest="sha256:params",
        materialization_plan_digest="sha256:materialization",
        owner="optimizer",
        created_at=NOW,
    )
    candidate_set = RewriteCandidateSet(
        candidate_set_id="candidate-set:1",
        program_digest=program.program_digest,
        source_generation_id="gen:source",
        candidate_ids=["candidate:1"],
        candidate_set_digest="sha256:candidates",
        evidence_refs=["trace:1"],
        conflict_groups={"candidate:1": "target:1"},
    )
    assertion = RewriteAssertionEvidence(
        assertion_id="assertion:1",
        trial_id="trial:1",
        checker_id="checker:branch-trial",
        property_id="rewrite.branch.trial",
        source_generation_id=candidate_set.source_generation_id,
        trial_generation_id="gen:trial",
        artifact=".scratch/assertion.json",
        status=assertion_status,
        fresh_until=assertion_fresh_until,
        input_digest="sha256:assertion-input",
    )
    assertions = (assertion,)
    assertion_digest = digest_rewrite_assertions(assertions)
    trial = RewriteBranchTrial(
        trial_id="trial:1",
        source_generation_id=candidate_set.source_generation_id,
        trial_branch=trial_branch,
        trial_generation_id=assertion.trial_generation_id,
        program_digest=program.program_digest,
        candidate_set_digest=candidate_set.candidate_set_digest,
        delta_digest="sha256:delta",
        assertion_digest=assertion_digest,
        created_at=NOW,
    )
    replay = RewriteReplayMeasurement(
        measurement_id="measurement:1",
        trial_id=trial.trial_id,
        workload_id="workload:1",
        workload_owner="product",
        baseline_generation_id=trial.source_generation_id,
        trial_generation_id=trial.trial_generation_id,
        runner_id="runner:1",
        runner_digest="sha256:runner",
        measurement_command="okg replay workload:1 --baseline gen:source --trial gen:trial",
        artifact_refs=(".scratch/replay.json",),
        metric_values={"latency_ms": 10.0},
        policy_id="policy:1",
        threshold_values={"max_latency_ms": 20.0},
        decision=replay_decision,
        measured_at=replay_measured_at,
        fresh_until=replay_fresh_until,
    )
    promotion = RewritePromotionDecision(
        promotion_id="promotion:1",
        trial_id=trial.trial_id,
        target_branch="default",
        actor="operator",
        policy_id=replay.policy_id,
        decision=RewritePromotionDecisionValue.PROMOTE,
        reason="fresh branch trial and replay passed",
        evidence_digest=assertion_digest,
        measurement_id=replay.measurement_id,
        created_at=NOW,
    )
    return _AcceptanceRecords(
        program=program,
        candidate_set=candidate_set,
        trial=trial,
        assertions=assertions,
        replay=replay,
        promotion=promotion,
    )
