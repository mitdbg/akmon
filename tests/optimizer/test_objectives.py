from __future__ import annotations

import copy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

from okg.optimizer import (
    RewriteAcceptanceError,
    RewriteAssertionEvidence,
    RewriteAssertionStatus,
    RewriteBranchTrial,
    RewriteCandidateSet,
    RewriteProgram,
    RewritePromotionDecision,
    RewritePromotionDecisionValue,
    RewriteReplayMeasurement,
    check_rewrite_promotion_acceptance,
    digest_rewrite_assertions,
)
from okg.optimizer.objectives import (
    HARD_CONSTRAINTS,
    MEASURES,
    MEASUREMENT_SCHEMA,
    Direction,
    ObjectivesError,
    Unit,
    acceptance_thresholds,
    load_objectives,
    measured_values,
    parse_measurement,
    parse_objectives,
    replay_decision,
    score_objectives,
    validate_measurement,
    validate_objectives,
)
from okg.substrate.deployment_lint import lint_deployment


VALID = {
    "version": 1,
    "hard_constraints": ["provenance", "versioning", "access_control"],
    "noise": {"min_tries": 3},
    "objectives": [
        {"id": "recall", "measure": "required_fact_recall", "question_set": "qs-1"},
        {"id": "freshness", "measure": "source_lag_seconds"},
    ],
    "costs": [
        {"id": "read-cost", "measure": "usd_per_question", "question_set": "qs-1"},
        {"id": "storage", "measure": "storage_bytes"},
    ],
    "specific": [
        {"id": "release-notes-floor", "measure": "required_fact_recall",
         "question_set": "qs-1", "scope_tag": "release-notes", "floor": 0.95},
        {"id": "cost-ceiling", "measure": "usd_per_question",
         "question_set": "qs-1", "ceiling": 0.02},
    ],
}

RECORD = {
    "schema": MEASUREMENT_SCHEMA,
    "deployment": "demo",
    "generation_id": "gen:trial",
    "run_id": "run:1",
    "setup_digest": "sha256:setup",
    "question_set": {"id": "qs-1", "digest": "sha256:qs"},
    "tries": 3,
    "measures": {
        "required_fact_recall": 0.86,
        "usd_per_question": 0.015,
        "storage_bytes": 17_000_000_000,
    },
    "per_scope_tag": {"release-notes": {"required_fact_recall": 0.97}},
    "constraints": {name: "satisfied" for name in HARD_CONSTRAINTS},
}


def _codes(raw) -> list[str]:
    return [problem.code for problem in validate_objectives(raw)]


def _with(**changes):
    raw = copy.deepcopy(VALID)
    raw.update(changes)
    return raw


def _without(key: str):
    raw = copy.deepcopy(VALID)
    del raw[key]
    return raw


# --- measure registry -------------------------------------------------------


def test_measure_every_entry_has_unit_and_direction() -> None:
    assert MEASURES
    for name, measure in MEASURES.items():
        assert measure.name == name
        assert isinstance(measure.unit, Unit)
        assert isinstance(measure.direction, Direction)


def test_measure_unknown_name_refused_by_both_lints() -> None:
    raw = _with(costs=[{"id": "x", "measure": "vibes"}])
    assert "measure_unknown" in _codes(raw)
    record = copy.deepcopy(RECORD)
    record["measures"]["vibes"] = 1.0
    assert "measure_unknown" in [p.code for p in validate_measurement(record)]


# --- objectives schema ------------------------------------------------------


def test_schema_valid_file_loads(tmp_path: Path) -> None:
    path = tmp_path / "objectives.yaml"
    path.write_text(yaml.safe_dump(VALID), encoding="utf-8")
    objectives = load_objectives(path)
    assert objectives.hard_constraints == HARD_CONSTRAINTS
    assert objectives.min_tries == 3
    assert [o.id for o in objectives.objectives] == ["recall", "freshness"]
    assert objectives.specific[0].floor == 0.95
    assert objectives.specific[1].ceiling == 0.02
    assert objectives.policy_id.startswith("akb-objectives:")
    assert parse_objectives(copy.deepcopy(VALID)).policy_id == objectives.policy_id
    # Round trip: every entry comes back with the fields it was written with.
    for block in ("objectives", "costs", "specific"):
        loaded = [
            {k: v for k, v in entry.__dict__.items() if v is not None}
            for entry in getattr(objectives, block)
        ]
        assert loaded == VALID[block]


@pytest.mark.parametrize(
    ("raw", "code"),
    [
        ("not a mapping", "not_mapping"),
        (_with(version=2), "version_unsupported"),
        (_with(hard_constraints=["provenance", "versioning"]), "hard_constraints_mismatch"),
        (_with(hard_constraints=[*HARD_CONSTRAINTS, "speed"]), "hard_constraints_mismatch"),
        (_with(surprise=True), "unknown_key"),
        (_with(costs=[{"id": "x", "measure": "storage_bytes", "direction": "maximize"}]), "unknown_key"),
        (_with(objectives=[{"id": "r", "measure": "required_fact_recall"}]), "question_set_required"),
        (_with(costs=[{"id": "recall", "measure": "storage_bytes"}]), "duplicate_id"),
        (_with(costs=[{"measure": "storage_bytes"}]), "id_missing"),
        (_with(specific=[{"id": "s", "measure": "storage_bytes", "floor": 1, "ceiling": 2}]), "threshold_both"),
        (_with(specific=[{"id": "s", "measure": "storage_bytes"}]), "threshold_missing"),
        (_with(specific=[{"id": "s", "measure": "required_fact_recall",
                          "question_set": "qs-1", "floor": 95}]), "threshold_out_of_range"),
        (_with(specific=[{"id": "s", "measure": "storage_bytes", "ceiling": "lots"}]), "threshold_not_number"),
        (_with(noise={"min_tries": 0}), "noise_invalid"),
        (_with(costs="storage"), "block_not_list"),
        (_with(costs=["storage"]), "entry_not_mapping"),
        (_with(version=True), "version_unsupported"),
        (_with(version=1.0), "version_unsupported"),
        (_with(specific=[{"id": "s", "measure": "usd_per_question", "ceiling": 0.1}]), "question_set_required"),
        (_with(specific=[{"id": "s", "measure": "usd_per_question", "question_set": "qs-1",
                          "scope_tag": "", "ceiling": 0.1}]), "scope_tag_invalid"),
        (_with(noise=3), "noise_invalid"),
        (_without("noise"), "noise_missing"),
        (_with(noise={}), "noise_missing"),
        (_with(specific=[{"id": "s", "measure": "required_fact_recall",
                          "question_set": "qs-1", "ceiling": 0.5}]), "threshold_direction"),
        (_with(specific=[{"id": "s", "measure": "storage_bytes", "floor": 10}]), "threshold_direction"),
        (_with(specific=[{"id": "s", "measure": "storage_bytes", "ceiling": -1}]), "threshold_out_of_range"),
        (_with(costs=[{"id": "x", "measure": "storage_bytes", "question_set": 123}]),
         "question_set_not_applicable"),
    ],
)
def test_schema_refusals(raw, code: str) -> None:
    assert code in _codes(raw)
    with pytest.raises(ObjectivesError) as excinfo:
        parse_objectives(raw)
    assert code in [p.code for p in excinfo.value.problems]


def test_schema_reports_every_problem_not_just_the_first() -> None:
    raw = _with(version=2, surprise=True)
    assert {"version_unsupported", "unknown_key"} <= set(_codes(raw))


def test_schema_unparseable_yaml(tmp_path: Path) -> None:
    path = tmp_path / "objectives.yaml"
    path.write_text("version: [1\n", encoding="utf-8")
    with pytest.raises(ObjectivesError) as excinfo:
        load_objectives(path)
    assert excinfo.value.problems[0].code == "yaml_parse"


def test_schema_duplicate_yaml_key_refused(tmp_path: Path) -> None:
    path = tmp_path / "objectives.yaml"
    text = yaml.safe_dump(VALID) + "specific: []\n"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(ObjectivesError) as excinfo:
        load_objectives(path)
    assert excinfo.value.problems[0].code == "yaml_parse"
    assert "duplicate key" in excinfo.value.problems[0].message


def test_schema_unreadable_file(tmp_path: Path) -> None:
    directory = tmp_path / "objectives.yaml"
    directory.mkdir()
    with pytest.raises(ObjectivesError) as excinfo:
        load_objectives(directory)
    assert excinfo.value.problems[0].code == "unreadable"
    binary = tmp_path / "binary.yaml"
    binary.write_bytes(b"version: 1\n\xff\xfe\n")
    with pytest.raises(ObjectivesError) as excinfo:
        load_objectives(binary)
    assert excinfo.value.problems[0].code == "unreadable"


# --- measurement record -----------------------------------------------------


def test_record_valid_round_trips() -> None:
    record = parse_measurement(copy.deepcopy(RECORD))
    assert record.generation_id == "gen:trial"
    assert record.question_set_id == "qs-1"
    assert record.measures["required_fact_recall"] == 0.86
    assert record.per_scope_tag["release-notes"]["required_fact_recall"] == 0.97


def _record_with(**changes):
    raw = copy.deepcopy(RECORD)
    raw.update(changes)
    return raw


@pytest.mark.parametrize(
    ("raw", "code"),
    [
        (_record_with(schema="other"), "schema_mismatch"),
        (_record_with(generation_id=""), "field_missing"),
        (_record_with(run_id=None), "field_missing"),
        (_record_with(setup_digest=" "), "field_missing"),
        (_record_with(measures={"required_fact_recall": 1.2}), "rate_out_of_range"),
        (_record_with(measures={"storage_bytes": -1}), "negative_value"),
        (_record_with(measures={"storage_bytes": True}), "value_not_number"),
        (_record_with(tries=0), "tries_invalid"),
        (_record_with(question_set={"id": "qs-1"}), "question_set_invalid"),
        (_record_with(constraints={"speed": "satisfied"}), "constraint_unknown"),
        (_record_with(constraints={"provenance": "probably"}), "constraint_status_invalid"),
        (_record_with(extra=1), "unknown_key"),
        (_record_with(measures={"model_calls_per_try": -1}), "negative_value"),
        (_record_with(per_scope_tag={"release-notes": None}), "block_not_mapping"),
        (_record_with(per_scope_tag={5: {"required_fact_recall": 0.5}}), "scope_tag_invalid"),
        (_record_with(constraints={"provenance": ["satisfied"]}), "constraint_status_invalid"),
    ],
)
def test_record_refusals(raw, code: str) -> None:
    assert code in [p.code for p in validate_measurement(raw)]
    with pytest.raises(ObjectivesError):
        parse_measurement(raw)


def test_record_missing_generation_id_refused() -> None:
    raw = copy.deepcopy(RECORD)
    del raw["generation_id"]
    problems = validate_measurement(raw)
    assert [(p.code, p.pointer) for p in problems] == [("field_missing", "/generation_id")]


# --- scoring ----------------------------------------------------------------


def _score(record_raw=None, objectives_raw=None):
    objectives = parse_objectives(objectives_raw or copy.deepcopy(VALID))
    record = parse_measurement(record_raw or copy.deepcopy(RECORD))
    return score_objectives(objectives, record, deployment="demo")


def _entry(score, entry_id):
    return next(e for e in score.entries if e.id == entry_id)


def test_score_measured_and_unmeasured() -> None:
    score = _score()
    assert _entry(score, "recall").status == "measured"
    assert _entry(score, "recall").value == 0.86
    assert _entry(score, "recall").unit == "rate"
    freshness = _entry(score, "freshness")
    assert freshness.status == "unmeasured"
    assert freshness.reason == "not_recorded"
    assert freshness.value is None
    assert _entry(score, "storage").status == "measured"


def test_score_specific_met_violated_and_scoped() -> None:
    score = _score()
    assert _entry(score, "release-notes-floor").status == "met"
    assert _entry(score, "cost-ceiling").status == "met"
    assert score.passes is True

    record = copy.deepcopy(RECORD)
    record["per_scope_tag"]["release-notes"]["required_fact_recall"] = 0.90
    record["measures"]["usd_per_question"] = 0.03
    score = _score(record)
    assert _entry(score, "release-notes-floor").status == "violated"
    assert _entry(score, "cost-ceiling").status == "violated"
    assert score.passes is False


def test_score_scope_tag_absent_is_unmeasured() -> None:
    record = copy.deepcopy(RECORD)
    del record["per_scope_tag"]
    score = _score(record)
    entry = _entry(score, "release-notes-floor")
    assert entry.status == "unmeasured"
    assert entry.reason == "scope_tag_not_recorded"
    assert score.passes is False


def test_score_hard_constraints() -> None:
    record = copy.deepcopy(RECORD)
    record["constraints"] = {"provenance": "satisfied", "versioning": "violated"}
    score = _score(record)
    assert _entry(score, "provenance").status == "satisfied"
    assert _entry(score, "provenance").blocking is False
    assert _entry(score, "versioning").status == "violated"
    assert _entry(score, "versioning").blocking is True
    access = _entry(score, "access_control")
    assert access.status == "unmeasured"
    assert access.blocking is True
    assert score.passes is False


def test_score_question_set_mismatch_and_too_few_tries() -> None:
    record = copy.deepcopy(RECORD)
    record["question_set"] = {"id": "qs-other", "digest": "sha256:x"}
    score = _score(record)
    assert _entry(score, "recall").reason == "question_set_mismatch"
    assert _entry(score, "storage").status == "measured"  # not answer-based

    record = copy.deepcopy(RECORD)
    record["tries"] = 1
    score = _score(record)
    assert _entry(score, "recall").reason == "too_few_tries"
    assert _entry(score, "storage").status == "measured"

    record = copy.deepcopy(RECORD)
    del record["tries"]
    score = _score(record)
    assert _entry(score, "recall").reason == "too_few_tries"


def test_score_empty_record_is_unmeasured_everywhere() -> None:
    record = {
        "schema": MEASUREMENT_SCHEMA,
        "deployment": "demo",
        "generation_id": "gen:trial",
        "run_id": "run:empty",
        "setup_digest": "sha256:setup",
    }
    score = _score(record)
    assert all(e.status == "unmeasured" for e in score.entries)
    assert all(e.value is None for e in score.entries)
    assert score.passes is False


def test_score_refuses_a_record_from_another_deployment() -> None:
    objectives = parse_objectives(copy.deepcopy(VALID))
    record = copy.deepcopy(RECORD)
    record["deployment"] = "other"
    with pytest.raises(ObjectivesError) as excinfo:
        score_objectives(objectives, parse_measurement(record), deployment="demo")
    assert [p.code for p in excinfo.value.problems] == ["deployment_mismatch"]


# --- acceptance gate --------------------------------------------------------


NOW = datetime(2026, 10, 29, tzinfo=UTC)
LATER = NOW + timedelta(hours=1)


def _gate(score, objectives):
    policy_id, thresholds = acceptance_thresholds(objectives)
    program = RewriteProgram(
        program_id="program:1", program_digest="sha256:program", dialect="althea.ir.v1",
        surface="scent", parameter_schema_digest="sha256:params",
        materialization_plan_digest="sha256:materialization", owner="optimizer",
        created_at=NOW,
    )
    candidates = RewriteCandidateSet(
        candidate_set_id="candidates:1", program_digest="sha256:program",
        source_generation_id="gen:base", candidate_ids=["candidate:1"],
        candidate_set_digest="sha256:candidates", evidence_refs=["trace:1"],
    )
    assertion = RewriteAssertionEvidence(
        assertion_id="assert:1", trial_id="trial:1", checker_id="checker:objectives",
        property_id="akb.objectives", source_generation_id="gen:base",
        trial_generation_id="gen:trial", artifact="record:1",
        status=RewriteAssertionStatus.PASS, fresh_until=LATER,
        input_digest="sha256:record",
    )
    digest = digest_rewrite_assertions((assertion,))
    trial = RewriteBranchTrial(
        trial_id="trial:1", source_generation_id="gen:base", trial_branch="exp-1",
        trial_generation_id="gen:trial", program_digest="sha256:program",
        candidate_set_digest="sha256:candidates", delta_digest="sha256:delta",
        assertion_digest=digest, created_at=NOW,
    )
    replay = RewriteReplayMeasurement(
        measurement_id="measurement:1", trial_id="trial:1", workload_id="qs-1",
        workload_owner="operator", baseline_generation_id="gen:base",
        trial_generation_id="gen:trial", runner_id="replay", runner_digest="sha256:runner",
        measurement_command="replay qs-1", artifact_refs=("record:1",),
        metric_values=measured_values(score), policy_id=policy_id,
        threshold_values=thresholds, decision=replay_decision(score),
        measured_at=NOW, fresh_until=LATER,
    )
    # The loop promotes onto its own branch, never default (operator decision
    # 2026-10-29); the gate takes that branch as its target.
    promotion = RewritePromotionDecision(
        promotion_id="promotion:1", trial_id="trial:1", target_branch="akb-loop",
        actor="akb-loop", policy_id=policy_id,
        decision=RewritePromotionDecisionValue.PROMOTE, reason="objectives met",
        evidence_digest=digest, measurement_id="measurement:1", created_at=NOW,
    )
    return check_rewrite_promotion_acceptance(
        program=program, candidate_set=candidates, trial=trial,
        assertions=(assertion,), replay=replay, promotion=promotion,
        supported_dialects=("althea.ir.v1",), now=NOW, default_branch="akb-loop",
    )


def test_acceptance_thresholds_shape() -> None:
    objectives = parse_objectives(copy.deepcopy(VALID))
    policy_id, thresholds = acceptance_thresholds(objectives)
    assert policy_id == objectives.policy_id
    assert thresholds == {"release-notes-floor.floor": 0.95, "cost-ceiling.ceiling": 0.02}


def test_acceptance_gate_accepts_when_every_threshold_holds() -> None:
    objectives = parse_objectives(copy.deepcopy(VALID))
    score = score_objectives(objectives, parse_measurement(copy.deepcopy(RECORD)), deployment="demo")
    report = _gate(score, objectives)
    assert report.ok is True


def test_acceptance_thresholds_refused_without_specific_entries() -> None:
    objectives = parse_objectives(_with(specific=[]))
    with pytest.raises(ObjectivesError) as excinfo:
        acceptance_thresholds(objectives)
    assert excinfo.value.problems[0].code == "thresholds_empty"


def test_acceptance_gate_refuses_when_the_scorer_fails_a_threshold() -> None:
    # The gate records the thresholds but does not compare against them; the
    # FAIL it refuses on comes from replay_decision.
    objectives = parse_objectives(copy.deepcopy(VALID))
    record = copy.deepcopy(RECORD)
    record["measures"]["usd_per_question"] = 0.05
    score = score_objectives(objectives, parse_measurement(record), deployment="demo")
    with pytest.raises(RewriteAcceptanceError) as excinfo:
        _gate(score, objectives)
    assert excinfo.value.code == "replay_not_passing"


def test_acceptance_policy_id_changes_with_the_file() -> None:
    first = parse_objectives(copy.deepcopy(VALID)).policy_id
    changed = copy.deepcopy(VALID)
    changed["specific"][1]["ceiling"] = 0.03
    assert parse_objectives(changed).policy_id != first


# --- deployment lint ----------------------------------------------------------


def _deployment(tmp_path: Path, objectives_text: str | None) -> Path:
    root = tmp_path / "demo"
    (root / "schemas").mkdir(parents=True)
    (root / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    (root / "deployment.yaml").write_text(
        "name: demo\nschema_dir: schemas\nsource_registry: source_registry.yaml\n",
        encoding="utf-8",
    )
    if objectives_text is not None:
        (root / "objectives.yaml").write_text(objectives_text, encoding="utf-8")
    return root


def _objective_findings(report) -> list[dict]:
    return [
        f for f in report["findings"] if f["code"].startswith("deployment.objectives.")
    ]


def test_lint_bad_file_yields_named_blocker(tmp_path: Path) -> None:
    bad = copy.deepcopy(VALID)
    bad["hard_constraints"] = ["provenance"]
    root = _deployment(tmp_path, yaml.safe_dump(bad))
    findings = _objective_findings(lint_deployment(root))
    assert [f["code"] for f in findings] == ["deployment.objectives.hard_constraints_mismatch"]
    assert findings[0]["severity"] == "blocker"
    assert findings[0]["location"]["pointer"] == "/hard_constraints"


def test_lint_good_file_yields_loaded(tmp_path: Path) -> None:
    root = _deployment(tmp_path, yaml.safe_dump(VALID))
    findings = _objective_findings(lint_deployment(root))
    assert [f["code"] for f in findings] == ["deployment.objectives.loaded"]
    assert findings[0]["severity"] == "info"
    assert findings[0]["details"]["policy_id"].startswith("akb-objectives:")


def test_lint_unreadable_path_yields_blocker(tmp_path: Path) -> None:
    root = _deployment(tmp_path, None)
    (root / "objectives.yaml").mkdir()
    findings = _objective_findings(lint_deployment(root))
    assert [(f["code"], f["severity"]) for f in findings] == [
        ("deployment.objectives.unreadable", "blocker"),
    ]


def test_lint_missing_file_yields_nothing(tmp_path: Path) -> None:
    root = _deployment(tmp_path, None)
    assert _objective_findings(lint_deployment(root)) == []
