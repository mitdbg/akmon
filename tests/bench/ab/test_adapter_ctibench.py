"""req.cti.adapter + req.cti.integration: CTIBench adapter."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.bench.ab.adapters import adapter_names, get_adapter
from okg.bench.ab.adapters.ctibench import (
    SNAPSHOT_TOKEN,
    CtiBenchAdapter,
    _parse_cvss,
)
from okg.bench.ab.errors import DatasetError
from okg.bench.ab.model import TaskMetrics, TaskResult


@pytest.fixture
def adapter() -> CtiBenchAdapter:
    return CtiBenchAdapter()


def _fixture_payload() -> dict:
    mcq = [
        {"URL": "u", "Question": f"q{i}", "Option A": "a", "Option B": "b",
         "Option C": "c", "Option D": "d", "Prompt": "Choose the best option.",
         "GT": "B"}
        for i in range(10)
    ]
    rcm = [
        {"URL": "u", "Description": f"vuln {i}", "Prompt": "Map to CWE.",
         "GT": "CWE-416"}
        for i in range(6)
    ]
    vsp = [
        {"URL": "u", "Description": f"vuln {i}", "Prompt": "Compute CVSS.",
         "GT": "CVSS:3.1/AV:L/AC:L/PR:L/UI:N/S:U/C:N/I:N/A:H"}
        for i in range(6)
    ]
    ate = [
        {"URL": "u", "Description": f"malware {i}", "Platform": "Enterprise",
         "Prompt": "Extract techniques.", "GT": "T1071, T1573, T1083"}
        for i in range(3)
    ]
    return {"cti-mcq": mcq, "cti-rcm": rcm, "cti-vsp": vsp, "cti-ate": ate}


@pytest.fixture
def fixture(tmp_path) -> Path:
    path = tmp_path / "cti.json"
    path.write_text(json.dumps(_fixture_payload()))
    return path


def _result(instance_id: str, answer: str) -> TaskResult:
    return TaskResult(
        instance_id=instance_id, arm="echo", status="completed", answer=answer,
        metrics=TaskMetrics(0, 0, 0, 0, 0, 0.0),
    )


def test_registered_as_fifth_benchmark():
    assert "ctibench" in adapter_names()
    assert get_adapter("ctibench").requires_corpus_config is True


def test_load_all_configs_and_open_book_token(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    assert len(instances) == 25
    assert all(SNAPSHOT_TOKEN in i.prompt for i in instances)
    assert all(i.repo is None for i in instances)
    mcq = next(i for i in instances if i.extra["config"] == "cti-mcq")
    assert "A. a" in mcq.prompt and "D. d" in mcq.prompt
    assert mcq.gold["gt"] == "B"


def test_cti_taa_is_excluded_loudly(adapter, tmp_path):
    bad = tmp_path / "taa.json"
    bad.write_text(json.dumps({"cti-taa": [{"Prompt": "p", "Text": "t", "URL": "u"}]}))
    with pytest.raises(DatasetError, match="cti-taa is excluded"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)
    instances = []
    with pytest.raises(DatasetError, match="no deterministic ground truth"):
        adapter.subset(instances, "cti-taa")


def test_bind_corpus_substitutes_and_mounts(adapter, tmp_path, fixture):
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    bound = adapter.bind_corpus(instances, {"snapshot_dir": str(snapshot)})
    assert all(SNAPSHOT_TOKEN not in i.prompt for i in bound)
    assert all(str(snapshot) in i.prompt for i in bound)
    assert all(i.extra["add_dirs"] == [str(snapshot)] for i in bound)
    with pytest.raises(DatasetError, match="snapshot_dir"):
        adapter.bind_corpus(instances, {})


def test_pilot_spreads_across_the_four_configs(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    pilot = adapter.pilot_subset(instances)
    counts: dict[str, int] = {}
    for inst in pilot:
        counts[inst.extra["config"]] = counts.get(inst.extra["config"], 0) + 1
    assert counts == {"cti-mcq": 8, "cti-rcm": 5, "cti-vsp": 5, "cti-ate": 2}


def test_deterministic_scorers(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    by_cfg = {i.extra["config"]: i for i in instances}

    def score(inst, answer):
        return adapter.score_task(
            run_dir=tmp_path, data_root=tmp_path, instance=inst,
            result=_result(inst.instance_id, answer),
        )

    mcq = by_cfg["cti-mcq"]
    assert score(mcq, "Reasoning...\nThe answer is B").value == 1.0
    assert score(mcq, "Answer: A").value == 0.0
    assert score(mcq, "no letter here").detail["reason"] == "no parseable answer"

    rcm = by_cfg["cti-rcm"]
    assert score(rcm, "This maps to cwe-79... no wait.\nCWE-416").value == 1.0
    assert score(rcm, "CWE-79").value == 0.0

    vsp = by_cfg["cti-vsp"]
    exact = score(vsp, "CVSS:3.1/AV:L/AC:L/PR:L/UI:N/S:U/C:N/I:N/A:H")
    assert exact.value == 1.0
    near = score(vsp, "CVSS:3.1/AV:N/AC:L/PR:L/UI:N/S:U/C:N/I:N/A:H")
    assert near.value == 0.0
    assert near.detail["per_metric"]["AV"] is False
    assert near.detail["metric_agreement"] == 7 / 8

    ate = by_cfg["cti-ate"]
    perfect = score(ate, "Techniques found.\nT1071, T1573, T1083")
    assert perfect.value == 1.0
    # Sub-technique normalizes to parent; one wrong + two right of three.
    partial = score(ate, "T1071.001, T1573, T9999")
    assert partial.detail["precision"] == pytest.approx(2 / 3, abs=1e-3)
    assert partial.detail["recall"] == pytest.approx(2 / 3, abs=1e-3)
    assert partial.value == pytest.approx(2 / 3, abs=1e-3)
    assert score(ate, "no ids at all").value == 0.0


def test_parse_cvss_rejects_incomplete_vectors():
    assert _parse_cvss("CVSS:3.1/AV:L/AC:L") is None
    assert _parse_cvss("AV:L/AC:L/PR:L/UI:N/S:U/C:N/I:N/A:H") is None
    parsed = _parse_cvss("CVSS:3.0/AV:N/AC:H/PR:N/UI:R/S:C/C:H/I:H/A:N")
    assert parsed and parsed["S"] == "C"


def test_real_mcq_option_shapes(adapter, tmp_path):
    """Released rows are not uniformly 4-option (verified 2026-07-27:
    5/2500 carry a null option, one of them a GAP with GT naming the
    absent option). Three-option rows load with their original letters;
    gold-naming-an-absent-option is excluded WITH A COUNT, not silently."""
    payload = {"cti-mcq": [
        {"URL": "u", "Question": "three options", "Option A": "a",
         "Option B": "b", "Option C": "c", "Option D": None,
         "Prompt": "Choose.", "GT": "C"},
        {"URL": "u", "Question": "gap row", "Option A": "a", "Option B": "b",
         "Option C": None, "Option D": "d", "Prompt": "Choose.", "GT": "D"},
        {"URL": "u", "Question": "bad gold", "Option A": "a", "Option B": "b",
         "Option C": None, "Option D": "d", "Prompt": "Choose.", "GT": "C"},
    ]}
    path = tmp_path / "mcq.json"
    path.write_text(json.dumps(payload))
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=path)
    assert len(instances) == 2  # the bad-gold row is excluded
    assert "D. d" not in instances[0].prompt  # null option omitted
    gap = instances[1]
    assert "B. b" in gap.prompt and "D. d" in gap.prompt
    assert "C." not in gap.prompt  # letters keep their identity, no renumber
    report = gap.extra["load_report"]
    assert report["excluded_malformed"] == 1
    assert "names no present option" in report["reasons"][0]


def test_mass_malformation_is_schema_drift(adapter, tmp_path):
    """Row-level noise is excluded and counted; a high rate is drift and
    must fail loudly rather than quietly shrink the benchmark."""
    rows = [
        {"URL": "u", "Question": f"q{i}", "Option A": "a", "Option B": None,
         "Option C": None, "Option D": None, "Prompt": "Choose.", "GT": "A"}
        for i in range(10)
    ]
    path = tmp_path / "drift.json"
    path.write_text(json.dumps({"cti-mcq": rows}))
    with pytest.raises(DatasetError, match="schema drift"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=path)


def test_malformed_rows_fail_loudly(adapter, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"cti-mcq": [{"Prompt": "p", "GT": "A"}]}))
    with pytest.raises(DatasetError, match="missing"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)
    bad.write_text(json.dumps({"not-a-config": [{"Prompt": "p", "GT": "A"}]}))
    with pytest.raises(DatasetError, match="unknown cti-bench config"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)


def test_add_dirs_reach_the_driver_argv(tmp_path, monkeypatch):
    """req.cti.integration: run_task mounts task.extra['add_dirs'] via
    --add-dir, and a missing mount dir refuses loudly."""
    import subprocess as sp

    from okg.bench.ab.arms.base import RunContext
    from okg.bench.ab.arms.claude_code import ClaudeCodeDriver
    from okg.bench.ab.errors import ArmDriverError
    from okg.bench.ab.model import TaskInstance

    driver = ClaudeCodeDriver("fresh")
    monkeypatch.setattr(
        "okg.bench.ab.arms.claude_code._init_config_dir",
        lambda d: d.mkdir(parents=True, exist_ok=True),
    )
    captured: dict = {}

    def fake_run(argv, **kwargs):
        captured["argv"] = argv
        return sp.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr("okg.bench.ab.arms.claude_code.subprocess.run", fake_run)
    snapshot = tmp_path / "snap"
    snapshot.mkdir()
    workdir = tmp_path / "work"
    workdir.mkdir()
    ctx = RunContext(
        run_dir=tmp_path / "run", data_root=tmp_path, model="sonnet",
        task_timeout_s=1.0, permission_mode="bypassPermissions",
    )
    task = TaskInstance(
        instance_id="x", benchmark="ctibench", prompt="p",
        extra={"add_dirs": [str(snapshot)]},
    )
    driver.run_task(task, ctx, workdir)
    argv = captured["argv"]
    assert argv[argv.index("--add-dir") + 1] == str(snapshot)

    missing = TaskInstance(
        instance_id="y", benchmark="ctibench", prompt="p",
        extra={"add_dirs": [str(tmp_path / "nope")]},
    )
    with pytest.raises(ArmDriverError, match="missing dir"):
        driver.run_task(missing, ctx, workdir)
