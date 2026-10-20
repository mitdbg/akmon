"""req.abx.adapters + req.abx.trackc: SWE-bench Verified adapter."""

from __future__ import annotations

import pytest

from okg.bench.ab.adapters.swebench_verified import (
    SweBenchVerifiedAdapter,
    _pr_number,
    regression_match,
)
from okg.bench.ab.errors import DatasetError, ScoringError
from okg.bench.ab.model import TaskMetrics, TaskResult


@pytest.fixture
def adapter() -> SweBenchVerifiedAdapter:
    return SweBenchVerifiedAdapter()


def _result(instance_id: str, patch: str | None) -> TaskResult:
    return TaskResult(
        instance_id=instance_id,
        arm="echo",
        status="completed",
        answer="",
        patch=patch,
        metrics=TaskMetrics(0, 0, 0, 0, 0, 0.0),
    )


def test_load_and_seal(adapter, tmp_path, swebench_fixture):
    instances = adapter.load_instances(
        data_root=tmp_path, offline_fixture=swebench_fixture
    )
    assert len(instances) == 2
    inst = {i.instance_id: i for i in instances}["acme__widgets-101"]
    assert inst.base_commit.startswith("a")
    assert inst.cutoff == inst.created_at
    assert inst.extra["gold_pr_number"] == 101
    assert "problem_statement" in inst.extra
    assert inst.gold["patch"]  # sealed payload present
    assert "patch" not in inst.public_dict().get("gold", {})


def test_chronological_order_within_repo(adapter, tmp_path, swebench_fixture):
    instances = adapter.load_instances(
        data_root=tmp_path, offline_fixture=swebench_fixture
    )
    ordered = adapter.order(instances)
    stamps = [i.created_at for i in ordered]
    assert stamps == sorted(stamps), "Track B requires chronological order"


def test_regression_subset_is_deterministic(adapter, tmp_path, swebench_fixture):
    instances = adapter.load_instances(
        data_root=tmp_path, offline_fixture=swebench_fixture
    )
    subset = adapter.subset(instances, "regression")
    assert [i.instance_id for i in subset] == ["acme__widgets-99"]
    assert regression_match(subset[0]) is not None


def test_repo_subset_and_unknown_subset(adapter, tmp_path, swebench_fixture):
    instances = adapter.load_instances(
        data_root=tmp_path, offline_fixture=swebench_fixture
    )
    assert len(adapter.subset(instances, "repo:acme/widgets")) == 2
    assert [
        i.instance_id for i in adapter.subset(instances, "instance:acme__widgets-99")
    ] == ["acme__widgets-99"]
    with pytest.raises(DatasetError):
        adapter.subset(instances, "repo:nobody/nothing")
    with pytest.raises(DatasetError):
        adapter.subset(instances, "no-such-subset")


def test_malformed_row_fails_loudly(adapter, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text('[{"instance_id": "x-1", "repo": "a/b"}]')
    with pytest.raises(DatasetError, match="missing required fields"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)


def test_pr_number_parse():
    assert _pr_number("django__django-11133") == 11133
    with pytest.raises(DatasetError):
        _pr_number("no-trailing-number-")


def test_per_task_scoring_refuses(adapter, tmp_path, swebench_fixture):
    instances = adapter.load_instances(
        data_root=tmp_path, offline_fixture=swebench_fixture
    )
    with pytest.raises(ScoringError, match="run-level"):
        adapter.score_task(
            run_dir=tmp_path, data_root=tmp_path, instance=instances[0], result=_result("acme__widgets-101", None)
        )


def test_finalize_scores_empty_patches_are_definitional_zero(
    adapter, tmp_path, swebench_fixture
):
    instances = adapter.load_instances(
        data_root=tmp_path, offline_fixture=swebench_fixture
    )
    results = [_result(i.instance_id, None) for i in instances]
    scores = adapter.finalize_scores(
        run_dir=tmp_path, data_root=tmp_path, instances=instances, results=results
    )
    assert all(s.value == 0.0 and s.kind == "resolve" for s in scores)
    assert all(s.detail.get("empty_patch") for s in scores)


def test_finalize_scores_marks_unscored_when_harness_unavailable(
    adapter, tmp_path, swebench_fixture, monkeypatch
):
    monkeypatch.setattr(
        "okg.bench.ab.adapters.swebench_verified._run_official_harness",
        lambda **kw: (None, "docker not on PATH (harness unavailable)"),
    )
    instances = adapter.load_instances(
        data_root=tmp_path, offline_fixture=swebench_fixture
    )
    results = [_result(i.instance_id, "diff --git a/x b/x\n") for i in instances]
    scores = adapter.finalize_scores(
        run_dir=tmp_path, data_root=tmp_path, instances=instances, results=results
    )
    assert all(s.value is None for s in scores)
    assert all(
        "did not score: docker not on PATH" in s.unscored_reason for s in scores
    )
    # The predictions file exists in the official format for later scoring.
    preds = (tmp_path / "predictions.jsonl").read_text().strip().splitlines()
    assert len(preds) == 2
