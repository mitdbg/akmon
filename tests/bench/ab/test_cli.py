"""req.abx.skeleton: CLI list/run/report + the echo-arm end-to-end loop."""

from __future__ import annotations

import json
from pathlib import Path

from okg.bench.ab.cli import main


def _roots(tmp_path: Path) -> list[str]:
    return [
        "--data-root", str(tmp_path / "data"),
        "--runs-root", str(tmp_path / "runs"),
    ]


def test_list_names_the_four_benchmarks_and_arms(tmp_path, capsys):
    assert main(_roots(tmp_path) + ["list"]) == 0
    out = capsys.readouterr().out
    for name in ("swebench-verified", "swe-qa", "gitgoodbench", "regminer"):
        assert name in out
    for arm in ("fresh", "native", "okg", "echo"):
        assert arm in out


def test_list_regression_subset_count(tmp_path, capsys, swebench_fixture):
    rc = main(
        _roots(tmp_path)
        + ["list", "--benchmark", "swebench-verified", "--subset", "regression",
           "--offline-fixture", str(swebench_fixture)]
    )
    assert rc == 0
    out = capsys.readouterr().out
    # Exactly one of the two fixture instances is regression-phrased.
    assert "1 of 2 instances" in out


def test_echo_pilot_end_to_end(tmp_path, capsys, swebench_fixture):
    rc = main(
        _roots(tmp_path)
        + ["run", "--benchmark", "swebench-verified", "--arm", "echo",
           "--pilot", "--limit", "2", "--no-workdir",
           "--offline-fixture", str(swebench_fixture)]
    )
    assert rc == 0
    runs = list((tmp_path / "runs").iterdir())
    assert len(runs) == 1
    run_dir = runs[0]
    manifest = json.loads((run_dir / "manifest.json").read_text())
    assert manifest["benchmark"] == "swebench-verified"
    assert manifest["arm"] == "echo"
    assert manifest["pilot"] is True
    assert len(manifest["ordering"]["instance_ids"]) == 2
    assert manifest["prompt_digest"]
    task_records = sorted((run_dir / "tasks").glob("*.json"))
    assert len(task_records) == 2
    record = json.loads(task_records[0].read_text())
    assert record["result"]["status"] == "completed"
    assert "gold" not in record["instance"], "gold must stay sealed"
    # scores.json exists: empty patches score 0.0 resolve by definition.
    scores = json.loads((run_dir / "scores.json").read_text())
    assert {s["value"] for s in scores} == {0.0}

    # report renders from the stored records
    rc = main(_roots(tmp_path) + ["report", "--run", run_dir.name])
    assert rc == 0
    out = capsys.readouterr().out
    assert "tasks_recorded: 2" in out
    assert "partial: False" in out


def test_unknown_benchmark_fails_loudly(tmp_path, capsys):
    rc = main(_roots(tmp_path) + ["list", "--benchmark", "nope"])
    assert rc == 2
    assert "unknown benchmark" in capsys.readouterr().err
