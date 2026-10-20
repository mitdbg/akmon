"""req.abx.trackc: RegMiner format donor — schema round-trip, loud gaps,
bug-inducing-commit scoring."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.bench.ab.adapters.regminer import RegMinerAdapter, _extract_commit
from okg.bench.ab.errors import DatasetError
from okg.bench.ab.model import TaskMetrics, TaskResult

RIC = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"


@pytest.fixture
def adapter() -> RegMinerAdapter:
    return RegMinerAdapter()


@pytest.fixture
def fixture(tmp_path) -> Path:
    payload = {
        "format": "regminer.v1",
        "bugs": [
            {
                "id": "mockito-42",
                "repo": "mockito/mockito",
                "regression_fixing_commit": "feedface" + "0" * 32,
                "regression_inducing_commit": RIC,
                "work_commit": "abadcafe" + "0" * 32,
                "test": {"module": "core", "case": "ShouldFrobnicateTest"},
            }
        ],
    }
    path = tmp_path / "regminer.json"
    path.write_text(json.dumps(payload))
    return path


def _result(answer: str) -> TaskResult:
    return TaskResult(
        instance_id="regminer/mockito-42", arm="echo", status="completed",
        answer=answer, metrics=TaskMetrics(0, 0, 0, 0, 0, 0.0),
    )


def test_fetch_refuses_by_license_ruling(adapter, tmp_path):
    with pytest.raises(DatasetError, match="no license"):
        adapter.fetch(data_root=tmp_path)


def test_load_requires_operator_supplied_file(adapter, tmp_path):
    with pytest.raises(DatasetError, match="operator-supplied"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=None)


def test_donor_format_round_trips(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    assert len(instances) == 1
    inst = instances[0]
    assert inst.base_commit.startswith("abadcafe")  # work commit, not the fix
    assert inst.gold["regression_inducing_commit"] == RIC
    assert "REGRESSION_COMMIT" in inst.prompt
    assert RIC not in inst.prompt, "the gold answer must never appear in the prompt"


def test_wrong_format_and_missing_fields_fail_loudly(adapter, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"format": "regminer.v0", "bugs": []}))
    with pytest.raises(DatasetError, match="declares format"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)
    bad.write_text(json.dumps({
        "format": "regminer.v1",
        "bugs": [{"id": "x", "repo": "a/b"}],
    }))
    with pytest.raises(DatasetError, match="missing fields"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)


def test_bic_scoring(adapter, tmp_path, fixture):
    inst = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)[0]
    hit = adapter.score_task(
        run_dir=tmp_path, data_root=tmp_path, instance=inst,
        result=_result(f"...analysis...\nREGRESSION_COMMIT: {RIC[:12]}"),
    )
    assert hit.value == 1.0
    miss = adapter.score_task(
        run_dir=tmp_path, data_root=tmp_path, instance=inst,
        result=_result("REGRESSION_COMMIT: feedface00000"),
    )
    assert miss.value == 0.0
    silent = adapter.score_task(
        run_dir=tmp_path, data_root=tmp_path, instance=inst, result=_result("I could not find it.")
    )
    assert silent.value == 0.0
    assert "no REGRESSION_COMMIT" in silent.detail["reason"]


def test_extract_commit_takes_the_last_marker():
    text = "REGRESSION_COMMIT: aaaaaaa\nwait, actually\nREGRESSION_COMMIT: bbbbbbb"
    assert _extract_commit(text) == "bbbbbbb"
    assert _extract_commit("REGRESSION_COMMIT: 12345") is None  # too short
