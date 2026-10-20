"""req.dom.itbench: ITBench adapter, scoped to recorded-telemetry diagnosis.

Synthetic fixture dirs only (3 scenario JSONs, 2 with recordings): loader
validation, unrecorded-scenario exclusion + scope_report counts, the
prompt never containing solutions/injection text, fault-localization
hit/miss, remediation-overlap fraction, unparseable -> 0.0, and the
zero-recorded scope-reduction raise.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.bench.ab.adapters.itbench import (
    SNAPSHOT_TOKEN,
    ItBenchAdapter,
    _identifier_tokens,
)
from okg.bench.ab.errors import DatasetError
from okg.bench.ab.model import TaskMetrics, TaskResult

# Distinctive gold content that must never reach a prompt.
GOLD_TOKENS = (
    "payment-flags", "paymentServiceFailure", "kubectl",
    "ad-service-v2", "wrong-image",
)


def _scenario_1() -> dict:
    """Fault in a ConfigMap feature flag; clean (non-leaking) description."""
    return {
        "alerts": [{"id": "AL-1", "text": "High error rate on checkout "
                                          "requests in namespace shop"}],
        "category": "sre",
        "complexity": "low",
        "description": "Users report checkout requests failing intermittently.",
        "disruptions": [{"injections": [{
            "configmap": "payment-flags",
            "flag_key": "paymentServiceFailure",
            "value": "on",
        }]}],
        "environment": {"cluster": "kind", "services": ["frontend", "checkout"]},
        "platforms": ["kubernetes"],
        "solutions": [
            {"command": "kubectl patch configmap payment-flags -n shop",
             "text": "Reset the paymentServiceFailure flag"},
            {"command": "kubectl rollout restart deployment/payment-svc",
             "text": "Restart the payment deployment"},
        ],
    }


def _scenario_2() -> dict:
    """Description names the injection target -> must be excluded."""
    return {
        "alerts": [{"id": "AL-2", "text": "Ad responses stale beyond SLO"}],
        "category": "sre",
        "complexity": "medium",
        "description": "The ad-service-v2 deployment serves stale ads.",
        "disruptions": [{"injections": [{
            "deployment_name": "ad-service-v2", "misconfig": "wrong-image",
        }]}],
        "environment": {"cluster": "kind", "services": ["ads"]},
        "platforms": ["kubernetes"],
        "solutions": [
            {"command": "kubectl set image deployment/ad-service-v2 "
                        "ads=registry/ads:1.2",
             "text": "Fix the container image"},
        ],
    }


def _scenario_3() -> dict:
    """Valid scenario with NO recording -> excluded, counted."""
    scenario = _scenario_1()
    scenario["alerts"] = [{"id": "AL-3", "text": "Latency above threshold"}]
    return scenario


def _write_fixture(tmp_path: Path, *, recorded: tuple[int, ...] = (1, 2)) -> Path:
    scenarios = tmp_path / "scenarios"
    scenarios.mkdir(exist_ok=True)
    for number, doc in ((1, _scenario_1()), (2, _scenario_2()),
                        (3, _scenario_3())):
        (scenarios / f"{number}.json").write_text(json.dumps(doc))
    trajectories = tmp_path / "trajectories"
    trajectories.mkdir(exist_ok=True)
    for number in recorded:
        run = trajectories / "gpt-4o" / "sre" / str(number) / "run-0"
        run.mkdir(parents=True)
        (run / "agent_output.json").write_text("{}")
    roots = tmp_path / "roots.json"
    roots.write_text(json.dumps({
        "scenarios_root": str(scenarios),
        "trajectories_root": str(trajectories),
    }))
    return roots


def _result(instance_id: str, answer: str) -> TaskResult:
    return TaskResult(
        instance_id=instance_id, arm="echo", status="completed", answer=answer,
        metrics=TaskMetrics(0, 0, 0, 0, 0, 0.0),
    )


@pytest.fixture
def adapter() -> ItBenchAdapter:
    return ItBenchAdapter()


def test_scenario_loader_and_scorers(adapter, tmp_path):
    """PACT anchor (test.dom.itbench): loader + scope report + prompt
    leakage discipline + both deterministic scorers."""
    roots = _write_fixture(tmp_path)
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=roots)

    # -- loader: only recorded scenarios become instances ----------------
    assert [i.instance_id for i in instances] == ["itbench/sre/1", "itbench/sre/2"]
    report = adapter.scope_report
    assert report["scenarios_total"] == 3
    assert report["recorded"] == 2
    assert report["unrecorded"] == 1
    assert report["unrecorded_scenario_numbers"] == [3]
    assert all(i.extra["scope_report"] == report for i in instances)

    # -- prompt: open-book token, alerts in, gold NEVER in ---------------
    one, two = instances
    assert SNAPSHOT_TOKEN in one.prompt
    assert "High error rate on checkout" in one.prompt
    for inst in instances:
        low = inst.prompt.lower()
        for token in GOLD_TOKENS:
            assert token.lower() not in low, (inst.instance_id, token)
        for token in _identifier_tokens(inst.gold["injections"]):
            assert token not in low, (inst.instance_id, token)
        assert "solutions" not in low and "disruption" not in low
    # clean description included; leaking description excluded
    assert one.extra["description_included"] is True
    assert "failing intermittently" in one.prompt
    assert two.extra["description_included"] is False
    assert "stale ads" not in two.prompt

    # -- fault localization: hit / miss / unparseable --------------------
    def score(inst, answer):
        return adapter.score_task(
            run_dir=tmp_path, data_root=tmp_path, instance=inst,
            result=_result(inst.instance_id, answer),
        )

    hit = score(one, (
        "The flag was flipped.\n"
        "FAULT: the payment-flags ConfigMap key paymentServiceFailure\n"
        "REMEDIATION:\n"
        "1. kubectl patch configmap payment-flags -n shop\n"
        "2. kubectl rollout restart deployment/payment-svc\n"
    ))
    assert hit.kind == "fault_localization"
    assert hit.value == 1.0
    assert "payment-flags" in hit.detail["matched_tokens"]
    assert hit.detail["remediation_overlap"] == 1.0

    partial = score(one, (
        "FAULT: paymentServiceFailure flag in payment-flags\n"
        "REMEDIATION:\n"
        "1. kubectl patch configmap payment-flags -n shop\n"
    ))
    assert partial.value == 1.0
    assert partial.detail["remediation_overlap"] == 0.5

    miss = score(one, (
        "FAULT: the frontend deployment is undersized\n"
        "REMEDIATION:\n1. scale up the frontend\n"
    ))
    assert miss.value == 0.0
    assert miss.detail["matched_tokens"] == []
    assert miss.detail["remediation_overlap"] == 0.0

    unparseable = score(one, "I could not determine the cause.")
    assert unparseable.value == 0.0
    assert unparseable.detail["reason"] == "no FAULT: line in answer"
    assert unparseable.detail["remediation_overlap"] == 0.0


def test_zero_recorded_scenarios_raises_naming_the_scope_reduction(
    adapter, tmp_path
):
    roots = _write_fixture(tmp_path, recorded=())
    with pytest.raises(DatasetError, match="scope reduction left zero"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=roots)
    # The counts are still visible for the operator report.
    assert adapter.scope_report["unrecorded"] == 3


def test_real_upstream_nested_solutions_flatten():
    """Upstream ships solutions as groups-of-stages-of-steps (verified
    against itbench-hub scenario 1.json, 2026-07-27); the flat documented
    shape must keep working too."""
    from okg.bench.ab.adapters.itbench import flatten_solution_steps

    nested = [[
        {"steps": [
            {"command": "kubectl -n otel-demo edit configmap flagd-config",
             "text": "Disable the flag"},
            {"command": "kubectl rollout restart deploy/flagd",
             "text": "Restart"},
        ]},
    ]]
    flat = flatten_solution_steps(nested)
    assert [s["command"].split()[0] for s in flat] == ["kubectl", "kubectl"]
    assert flatten_solution_steps([{"command": "c", "text": "t"}]) == [
        {"command": "c", "text": "t"}
    ]
    assert flatten_solution_steps([]) == []


def test_loader_validates_scenarios_loudly(adapter, tmp_path):
    roots = _write_fixture(tmp_path)
    scenarios = tmp_path / "scenarios"

    broken = _scenario_1()
    del broken["solutions"]
    (scenarios / "1.json").write_text(json.dumps(broken))
    with pytest.raises(DatasetError, match=r"missing fields \['solutions'\]"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=roots)

    # Real upstream indexes ship alerts: [] (alerts are runtime artifacts
    # in the recorded telemetry, verified against scenario 1.json
    # 2026-07-27), so an empty list LOADS — the prompt points at the
    # recordings instead. A non-list is still malformed.
    empty_alerts = _scenario_1()
    empty_alerts["alerts"] = []
    (scenarios / "1.json").write_text(json.dumps(empty_alerts))
    loaded = adapter.load_instances(data_root=tmp_path, offline_fixture=roots)
    assert loaded, "empty index alerts must still load when recordings exist"
    assert "recorded telemetry" in loaded[0].prompt

    bad_alerts = _scenario_1()
    bad_alerts["alerts"] = "not-a-list"
    (scenarios / "1.json").write_text(json.dumps(bad_alerts))
    with pytest.raises(DatasetError, match="alerts must be a list"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=roots)
    (scenarios / "1.json").write_text(json.dumps(_scenario_1()))

    no_injections = _scenario_1()
    no_injections["disruptions"] = [{"injections": []}]
    (scenarios / "1.json").write_text(json.dumps(no_injections))
    with pytest.raises(DatasetError, match="no injections"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=roots)


def test_roots_file_and_fetch_are_operator_facing(adapter, tmp_path):
    with pytest.raises(DatasetError, match="operator-supplied roots file"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=None)

    bad = tmp_path / "roots.json"
    bad.write_text(json.dumps({"scenarios_root": str(tmp_path)}))
    with pytest.raises(DatasetError, match=r"missing \['trajectories_root'\]"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)

    # fetch never downloads: it hands back the two-part acquisition
    # instruction (Apache-2.0 clone + CC-BY-NC-4.0 operator copy).
    with pytest.raises(DatasetError) as exc:
        adapter.fetch(data_root=tmp_path)
    message = str(exc.value)
    assert "git clone" in message and "ITBench" in message
    assert "CC-BY-NC-4.0" in message and "never vendored" in message
    ref = adapter.dataset_ref(data_root=tmp_path)
    assert "CC-BY-NC-4.0" in ref["license"]


def test_order_and_pilot_are_by_scenario_number(adapter, tmp_path):
    roots = _write_fixture(tmp_path)
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=roots)
    ordered = adapter.order(list(reversed(instances)))
    assert [i.extra["scenario_number"] for i in ordered] == [1, 2]
    assert adapter.pilot_size == 10
    assert [i.instance_id for i in adapter.pilot_subset(instances)] == [
        "itbench/sre/1", "itbench/sre/2",
    ]


def test_bind_corpus_substitutes_and_mounts(adapter, tmp_path):
    roots = _write_fixture(tmp_path)
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=roots)
    bound = adapter.bind_corpus(instances, {"snapshot_dir": str(snapshot)})
    assert all(SNAPSHOT_TOKEN not in i.prompt for i in bound)
    assert all(str(snapshot) in i.prompt for i in bound)
    assert all(i.extra["add_dirs"] == [str(snapshot)] for i in bound)
    with pytest.raises(DatasetError, match="snapshot_dir"):
        adapter.bind_corpus(instances, {})
