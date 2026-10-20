"""req.abx.trackc: GitGoodBench adapter — real scenario types, loud bisect
premise-falsification, merge exact-match scoring."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from okg.bench.ab.adapters.gitgoodbench import GitGoodBenchAdapter
from okg.bench.ab.errors import DatasetError
from okg.bench.ab.model import TaskMetrics, TaskResult


@pytest.fixture
def adapter() -> GitGoodBenchAdapter:
    return GitGoodBenchAdapter()


def _rows() -> list[dict]:
    merge_scenario = (
        "{'merge_commit_hash': 'cafe1234', 'parents': ['aaaa1111', 'bbbb2222'],"
        " 'files_in_merge_conflict': ['src/main.py']}"
    )
    chain_scenario = (
        "{'file': 'src/x.py', 'newest_commit': 'dddd4444',"
        " 'oldest_commit': 'cccc3333', 'branch': 'origin/main'}"
    )
    return [
        {"id": "repo_merge_0001", "name": "acme/widgets", "default_branch": "main",
         "sample_type": "merge", "scenario": merge_scenario,
         "programming_language": "python", "difficulty": "easy"},
        {"id": "repo_chain_0001", "name": "acme/widgets", "default_branch": "main",
         "sample_type": "file_commit_chain", "scenario": chain_scenario,
         "programming_language": "python", "difficulty": "medium"},
    ]


@pytest.fixture
def fixture(tmp_path) -> Path:
    path = tmp_path / "ggb.json"
    path.write_text(json.dumps(_rows()))
    return path


def test_load_parses_python_literal_scenarios(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    merge = next(i for i in instances if i.extra["sample_type"] == "merge")
    assert merge.base_commit == "aaaa1111"  # first parent
    assert merge.gold["scenario"]["merge_commit_hash"] == "cafe1234"
    chain = next(i for i in instances if i.extra["sample_type"] == "file_commit_chain")
    assert chain.base_commit == "cccc3333^"


def test_unknown_sample_type_fails_loudly(adapter, tmp_path):
    rows = _rows()
    rows[0]["sample_type"] = "bisect"
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(rows))
    with pytest.raises(DatasetError, match="unknown git_good_bench sample_type"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)


def test_bisect_subset_reports_the_falsified_premise(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    with pytest.raises(DatasetError, match="PREMISE FALSIFIED"):
        adapter.subset(instances, "bisect")


def test_pilot_is_merge_scenarios_only(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    pilot = adapter.pilot_subset(instances)
    assert pilot and all(i.extra["sample_type"] == "merge" for i in pilot)


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(cwd), *args], capture_output=True, text=True, check=True
    ).stdout


def test_merge_scoring_exact_match(adapter, tmp_path, tiny_repo):
    """Construct a real merged history and score a correct and an incorrect
    resolution against the merge commit."""
    # Branch from HEAD~1, diverge, merge with a hand resolution.
    _git(tiny_repo, "checkout", "-b", "side", "HEAD~1")
    (tiny_repo / "file.txt").write_text("side change\n")
    _git(tiny_repo, "commit", "-am", "side edit")
    _git(tiny_repo, "checkout", "main")
    merge = subprocess.run(
        ["git", "-C", str(tiny_repo), "merge", "side"],
        capture_output=True, text=True, check=False,
    )
    assert merge.returncode != 0  # conflict as constructed
    (tiny_repo / "file.txt").write_text("resolved: both\n")
    _git(tiny_repo, "add", "file.txt")
    _git(tiny_repo, "commit", "-m", "merge side")
    merge_sha = _git(tiny_repo, "rev-parse", "HEAD").strip()

    instance_rows = [{
        "id": "tiny_merge_0001", "name": str(tiny_repo), "default_branch": "main",
        "sample_type": "merge",
        "scenario": str({
            "merge_commit_hash": merge_sha,
            "parents": [
                _git(tiny_repo, "rev-parse", "HEAD^1").strip(),
                _git(tiny_repo, "rev-parse", "HEAD^2").strip(),
            ],
            "files_in_merge_conflict": ["file.txt"],
        }),
    }]
    fixture = tmp_path / "tiny.json"
    fixture.write_text(json.dumps(instance_rows))
    inst = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)[0]

    # "Workdir" = the repo itself with the correct resolution present.
    (tiny_repo / "file.txt").write_text("resolved: both\n")
    result = TaskResult(
        instance_id=inst.instance_id, arm="echo", status="completed", answer="",
        metrics=TaskMetrics(0, 0, 0, 0, 0, 0.0),
        detail={"workdir": str(tiny_repo)},
    )
    score = adapter.score_task(run_dir=tmp_path, data_root=tmp_path, instance=inst, result=result)
    assert score.value == 1.0

    (tiny_repo / "file.txt").write_text("resolved: wrong\n")
    score = adapter.score_task(run_dir=tmp_path, data_root=tmp_path, instance=inst, result=result)
    assert score.value == 0.0
    assert score.detail["files"] == {"file.txt": False}
