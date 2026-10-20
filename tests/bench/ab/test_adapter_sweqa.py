"""req.abx.adapters: SWE-QA adapter — loading, pilot spread, judge scoring."""

from __future__ import annotations

import json

import pytest

from okg.bench.ab.adapters.swe_qa import (
    JUDGE_MODEL_ENV,
    JUDGE_RUBRIC_VERSION,
    SweQaAdapter,
    judge_config,
)
from okg.bench.ab.errors import DatasetError, ScoringError
from okg.bench.ab.model import TaskMetrics, TaskResult


@pytest.fixture
def adapter() -> SweQaAdapter:
    return SweQaAdapter()


@pytest.fixture
def fixture(tmp_path):
    payload = {
        "flask": [
            {"question": f"flask q{i}", "answer": f"flask a{i}"} for i in range(30)
        ],
        "django": [
            {"question": f"django q{i}", "answer": f"django a{i}"} for i in range(30)
        ],
    }
    path = tmp_path / "sweqa.json"
    path.write_text(json.dumps(payload))
    return path


def _result(instance_id: str, answer: str, status: str = "completed") -> TaskResult:
    return TaskResult(
        instance_id=instance_id,
        arm="echo",
        status=status,
        answer=answer,
        metrics=TaskMetrics(0, 0, 0, 0, 0, 0.0),
    )


def test_load_synthesizes_stable_ids(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    assert len(instances) == 60
    assert instances[0].instance_id.startswith("sweqa/")
    again = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    assert [i.instance_id for i in again] == [i.instance_id for i in instances]


def test_load_rejects_missing_answer(adapter, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"flask": [{"question": "q"}]}))
    with pytest.raises(DatasetError, match="missing question/answer"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)


def test_load_rejects_unknown_split(adapter, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"leftpad": [{"question": "q", "answer": "a"}]}))
    with pytest.raises(DatasetError, match="unknown SWE-QA split"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)


def test_pilot_spreads_across_repos(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    pilot = adapter.pilot_subset(instances)
    assert len(pilot) == 20
    repos = {i.repo for i in pilot}
    assert repos == {"pallets/flask", "django/django"}


def test_unjudged_scoring_is_unscored_not_zero(
    adapter, tmp_path, fixture, monkeypatch
):
    monkeypatch.delenv(JUDGE_MODEL_ENV, raising=False)
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    score = adapter.score_task(
        run_dir=tmp_path, data_root=tmp_path, instance=instances[0], result=_result("x", "an answer")
    )
    assert score.value is None
    assert JUDGE_MODEL_ENV in score.unscored_reason


def test_judged_scoring_records_judge_identity(
    adapter, tmp_path, fixture, monkeypatch
):
    monkeypatch.setenv(JUDGE_MODEL_ENV, "cheap/model")
    monkeypatch.setattr(
        "okg.bench.ab.adapters.swe_qa._call_judge",
        lambda model, prompt: '{"score": 0.5, "rationale": "half right"}',
    )
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    score = adapter.score_task(
        run_dir=tmp_path, data_root=tmp_path, instance=instances[0], result=_result("x", "an answer")
    )
    assert score.value == 0.5
    assert score.detail["judge_model"] == "cheap/model"
    assert score.detail["rubric_version"] == JUDGE_RUBRIC_VERSION


def test_judge_garbage_fails_loudly(adapter, tmp_path, fixture, monkeypatch):
    monkeypatch.setenv(JUDGE_MODEL_ENV, "cheap/model")
    monkeypatch.setattr(
        "okg.bench.ab.adapters.swe_qa._call_judge",
        lambda model, prompt: "I think it's pretty good!",
    )
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    with pytest.raises(ScoringError, match="not JSON"):
        adapter.score_task(
            run_dir=tmp_path, data_root=tmp_path, instance=instances[0], result=_result("x", "a")
        )


def test_no_answer_scores_zero(adapter, tmp_path, fixture, monkeypatch):
    monkeypatch.setenv(JUDGE_MODEL_ENV, "cheap/model")
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    score = adapter.score_task(
        run_dir=tmp_path,
        data_root=tmp_path,
        instance=instances[0],
        result=_result("x", "", status="timeout"),
    )
    assert score.value == 0.0


def test_judge_config_carries_prompt_for_the_manifest(monkeypatch):
    monkeypatch.setenv(JUDGE_MODEL_ENV, "cheap/model")
    cfg = judge_config()
    assert cfg["judge_model"] == "cheap/model"
    assert cfg["rubric_version"] == JUDGE_RUBRIC_VERSION
    assert "reference_answer" in cfg["prompt_template"]
