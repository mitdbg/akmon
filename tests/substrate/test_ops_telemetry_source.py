"""OpsTelemetrySource + ops ontology module (PACT okg-corpus-ops).

Synthetic fixture trees only — the adapter has NO live network path. The
fixtures are shaped like the real Apache-2.0 ITBench clone: a scenario index
directory, and a `scenarios/sre` library holding the application indexes, the
applications role's managers file, its install tasks, and the helm values and
Prometheus rule templates those tasks name.

Covered:
- req.ops.topology — the environment block resolves through the application
  library to an application, its component vocabulary, and its alert
  definitions; every service records which artifact named it; the loud
  failures (unknown application, componentless application, absent or
  multi-application environment block).
- req.ops.telemetry — alert DEFINITIONS carry expr / for / severity /
  description; alert names fan out to every definition of that name; a
  scenario naming an undefined alert is reported, never stubbed; targets
  resolve from positive matchers only and an unresolved rule says why; the
  bounded signal index; the trajectory deny-list on both the corpus side and
  the snapshot-mount side.
- req.ops.contract — stripped-gold equivalence, plus the proof that the
  equivalence test is not vacuous.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.ops_telemetry import (
    GROUND_TRUTH_DENY_LIST,
    SNAPSHOT_DENY_LIST,
    TRAJECTORY_DENY_LIST,
    OpsTelemetrySource,
    _find_recordings,
)

REPO = "itbench-hub/ITBench"
APP = "demo-app"

# Distinctive gold content: none of these may reach any emitted attr.
GOLD_STRINGS = (
    "kubectl patch configmap payment-flags --patch-file fix.yaml",
    "Reset the failure flag to off",
    "payment-flags",
    "paymentServiceFailure",
    "the `payment` service failing",
)

_HELM_VALUES = """\
---
{% if applications_cluster.platform == "openshift" %}
serviceAccount:
  create: false
{% endif %}
components:
  checkout:
    resources:
      limits:
        cpu: 500m
  payment:
    resources:
      limits:
        memory: 512Mi
opentelemetry-collector:
  mode: deployment
"""

_APP_RULES = """\
{# Delimiters are changed to keep Prometheus templating. #}
---
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: demo-alerting-rules
  namespace: [[ applications_managers.demo_app.kubernetes.namespace ]]
spec:
  groups:
    - name: demoapp.frontend.goldensignal
      rules:
        - alert: HighErrorRate
          expr: |
            sum (
              rate(
                calls_total{
                  service_name="checkout",
                  namespace="[[ applications_managers.demo_app.kubernetes.namespace ]]"
                }[2m]
              )
            ) > 0
          for: 3m
          labels:
            severity: critical
          annotations:
            description: "Error rate in {{ $labels.service_name }} is above 0"
    - name: demoapp.backend.goldensignal
      rules:
        - alert: HighErrorRate
          expr: |
            sum (
              rate(
                calls_total{
                  service_name!~"frontend|load-generator",
                  namespace="[[ applications_managers.demo_app.kubernetes.namespace ]]"
                }[2m]
              )
            ) > 0
          for: 5m
          labels:
            severity: warning
          annotations:
            description: "Backend error rate is above 0"
"""

_COMMON_RULES = """\
{# Delimiters are changed to keep Prometheus templating. #}
---
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: common-alerting-rules
  namespace: [[ application_namespace ]]
spec:
  groups:
    - name: [[ group_name ]].workloads
      rules:
        - alert: FailedPodsDetected
          expr: |
            sum by (namespace) (
              kube_pod_status_phase{
                namespace="[[ application_namespace ]]",
                phase="Failed"
              }
            ) > 0
          for: 2m
          labels:
            severity: critical
          annotations:
            description: "Pods in failed state is above 0"
"""


def _managers() -> dict:
    return {
        "applications_managers": {
            "demo_app": {
                "helm": {
                    "chart": {"reference": "oci://example/demo",
                              "version": "1.2.3"},
                    "release": {"name": "demo"},
                },
                "kubernetes": {"namespace": "demo-ns"},
            },
        },
    }


def _install_task() -> list:
    return [
        {
            "name": "Install Demo App",
            "kubernetes.core.helm": {
                "chart_ref":
                    "{{ applications_managers.demo_app.helm.chart.reference }}",
                "release_namespace":
                    "{{ applications_managers.demo_app.kubernetes.namespace }}",
                "values":
                    "{{ lookup('ansible.builtin.template', "
                    "'templates/helm/demo_app/values.j2') }}",
            },
        },
        {
            "name": "Create simple load generator",
            "kubernetes.core.k8s": {
                "resource_definition": {
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "metadata": {
                        "labels": {
                            "app.kubernetes.io/component": "load-generator",
                            "app.kubernetes.io/part-of": "demoapp",
                        },
                        "name": "load-generator-demo",
                    },
                },
            },
        },
        {
            "name": "Create Common PrometheusRules",
            "kubernetes.core.k8s": {
                "template": {
                    "path": "templates/kubernetes/common/prometheusrules.j2",
                },
            },
            "vars": {
                "application_namespace":
                    "{{ applications_managers.demo_app.kubernetes.namespace }}",
                "group_name": "demoapp",
            },
        },
        {
            "name": "Create Demo App PrometheusRules",
            "kubernetes.core.k8s": {
                "template": {
                    "path": "templates/kubernetes/demo_app/prometheusrules.j2",
                },
            },
        },
    ]


def _scenario(
    *,
    application_id: str = APP,
    alerts: tuple[str, ...] = ("HighErrorRate", "KubePodNotReady"),
) -> dict:
    return {
        "alerts": list(alerts),
        "category": "sre",
        "complexity": "low",
        "description": "This scenario simulates the `payment` service failing.",
        "disruptions": [{"injections": [{
            "configmap": "payment-flags", "flag": "paymentServiceFailure",
        }]}],
        "environment": {"applications": [{"id": application_id}]},
        "platforms": ["kubernetes", "openshift"],
        "solutions": [[{"steps": [{
            "command": "kubectl patch configmap payment-flags "
                       "--patch-file fix.yaml",
            "text": "Reset the failure flag to off",
        }]}]],
    }


def _write_library(root: Path, *, helm: str | None = _HELM_VALUES) -> Path:
    """Mirror the upstream `<clone>/scenarios/sre` layout."""
    sre = root / "sre"
    role = sre / "project" / "roles" / "applications"
    (sre / "library" / "indexes" / "applications").mkdir(parents=True)
    (sre / "library" / "indexes" / "applications" / "1.json").write_text(
        json.dumps({
            "id": APP, "index": 1, "name": "Demo App",
            "description": "A demo microservice application.",
            "repository": "https://github.com/example/demo",
            "platforms": ["Kubernetes"], "authors": ["Example"],
        })
    )
    (role / "defaults" / "main").mkdir(parents=True)
    (role / "defaults" / "main" / "managers.yaml").write_text(
        yaml.safe_dump(_managers())
    )
    (role / "tasks").mkdir(parents=True)
    (role / "tasks" / "install_demo_app.yaml").write_text(
        yaml.safe_dump(_install_task())
    )
    if helm is not None:
        (role / "templates" / "helm" / "demo_app").mkdir(parents=True)
        (role / "templates" / "helm" / "demo_app" / "values.j2").write_text(helm)
    for name, body in (("demo_app", _APP_RULES), ("common", _COMMON_RULES)):
        target = role / "templates" / "kubernetes" / name
        target.mkdir(parents=True)
        (target / "prometheusrules.j2").write_text(body)
    return sre


def _write_scenarios(root: Path, scenarios: dict[int, dict]) -> Path:
    scenarios_root = root / "scenarios"
    scenarios_root.mkdir(exist_ok=True)
    for number, doc in scenarios.items():
        (scenarios_root / f"{number}.json").write_text(json.dumps(doc))
    return scenarios_root


def _write_trajectories(
    root: Path,
    *,
    recorded: tuple[int, ...] = (1,),
    marker: str = "",
) -> Path:
    trajectories_root = root / "trajectories"
    trajectories_root.mkdir(exist_ok=True)
    for number in recorded:
        run = trajectories_root / "gpt-4o" / "sre" / str(number) / "run-0"
        run.mkdir(parents=True)
        for denied in sorted(TRAJECTORY_DENY_LIST):
            (run / denied).write_text(
                json.dumps({"answer": marker}) if marker else "{}"
            )
        (run / "observability_alerts.json").write_text(json.dumps([
            {"alertname": "HighErrorRate", "service_name": "checkout",
             "timestamp": "2026-07-01T00:00:00Z"},
        ]))
        (run / "observability_pod_logs.jsonl").write_text(
            json.dumps({"service": "payment", "timestamp":
                        "2026-07-01T00:00:05Z", "msg": "boom"}) + "\n"
        )
    return trajectories_root


_DEFAULT = object()


def _build(tmp_path: Path, **kw) -> Path:
    scenarios = kw.pop("scenarios", _DEFAULT)
    if scenarios is _DEFAULT:
        scenarios = {1: _scenario()}
    helm = kw.pop("helm", _HELM_VALUES)
    _write_library(tmp_path, helm=helm)
    _write_scenarios(tmp_path, scenarios)
    return tmp_path


def _source(tmp_path: Path, *, trajectories: Path | None = None,
            scenarios_root: Path | None = None) -> OpsTelemetrySource:
    return OpsTelemetrySource(
        scenarios_root=scenarios_root or (tmp_path / "scenarios"),
        library_root=tmp_path / "sre",
        trajectories_root=trajectories,
    )


def _facts(source: OpsTelemetrySource) -> list:
    return list(source.run("rid", mode="scope_complete").facts)


def _split(facts: list) -> tuple[dict[str, NodeFact], set[tuple[str, ...]]]:
    nodes = {f.node_id: f for f in facts if isinstance(f, NodeFact)}
    edges = {
        (e.src, e.edge_type, e.dst) for e in facts if isinstance(e, EdgeFact)
    }
    return nodes, edges


# ---------- req.ops.topology ----------


def test_topology_services_and_deploys_edges(tmp_path):
    """The environment block is a FOREIGN KEY, not a topology: it resolves to
    one application, which owns the component vocabulary an answer is graded
    against. Every service records which artifact named it."""
    _build(tmp_path)
    facts = _facts(_source(tmp_path))
    nodes, edges = _split(facts)

    app_id = f"ops_application:{REPO}:{APP}"
    app = nodes[app_id]
    assert app.subtype == "ops_application"
    assert app.attrs["namespace"] == "demo-ns"
    assert app.attrs["deploy_kind"] == "helm"
    assert app.attrs["deploy_ref"] == "oci://example/demo"
    assert app.attrs["deploy_version"] == "1.2.3"

    scenario_id = f"ops_scenario:{REPO}:1"
    assert (scenario_id, "deploys", app_id) in edges
    assert nodes[scenario_id].attrs["application_id"] == APP

    services = {
        n.attrs["service_name"]: n.attrs["origin"]
        for n in nodes.values() if n.subtype == "ops_service"
    }
    # helm values components, an install-task component label, and a name
    # only the alert selectors mention — three source-carried origins.
    assert services == {
        "checkout": "helm_values",
        "payment": "helm_values",
        "load-generator": "install_manifest",
        "frontend": "alert_selector",
    }
    for name in services:
        svc_id = f"ops_service:{REPO}:{APP}:{name}"
        assert (app_id, "runs", svc_id) in edges
        assert nodes[svc_id].attrs["namespace"] == "demo-ns"

    # Ids are stable and derivable from (repo, application, name).
    again, _ = _split(_facts(_source(tmp_path)))
    assert set(again) == set(nodes)


def test_scenario_naming_an_undefined_application_raises(tmp_path):
    _build(tmp_path, scenarios={1: _scenario(application_id="ghost-app")})
    with pytest.raises(SubstrateError, match="library does not define"):
        _facts(_source(tmp_path))


def test_absent_or_multi_application_environment_raises(tmp_path):
    empty = _scenario()
    empty["environment"] = {"applications": []}
    _build(tmp_path, scenarios={1: empty})
    with pytest.raises(SubstrateError, match="absent or empty"):
        _facts(_source(tmp_path))

    multi_root = tmp_path / "multi"
    multi_root.mkdir()
    multi = _scenario()
    multi["environment"] = {"applications": [{"id": APP}, {"id": APP}]}
    _build(multi_root, scenarios={1: multi})
    with pytest.raises(SubstrateError, match="declares 2 applications"):
        _facts(_source(multi_root))


def test_componentless_application_raises(tmp_path):
    """An application resolving to zero components makes the answer unit
    unreachable by construction — the defect that produced the 4-node corpus.
    An empty `components:` map is refused before it can become 'no
    components exist'."""
    empty_helm = _HELM_VALUES.replace(
        "components:\n  checkout:", "componentsX:\n  checkout:"
    )
    _build(tmp_path, helm=empty_helm)
    with pytest.raises(SubstrateError, match="declares zero components"):
        _facts(_source(tmp_path))


def test_unknown_rule_template_placeholder_raises(tmp_path):
    """Substituting a blank for an unrecognised placeholder would silently
    corrupt the rule body, so it raises instead."""
    _build(tmp_path)
    rules = (tmp_path / "sre" / "project" / "roles" / "applications"
             / "templates" / "kubernetes" / "common" / "prometheusrules.j2")
    rules.write_text(
        _COMMON_RULES.replace("[[ application_namespace ]]", "[[ mystery ]]")
    )
    with pytest.raises(SubstrateError, match="cannot resolve"):
        _facts(_source(tmp_path))


# ---------- req.ops.telemetry ----------


def test_alerts_signals_and_trajectory_denylist(tmp_path):
    _build(tmp_path)
    trajectories = _write_trajectories(tmp_path, marker="LEAKED-ANSWER-TOKEN")
    source = _source(tmp_path, trajectories=trajectories)
    facts = _facts(source)
    nodes, edges = _split(facts)

    app_id = f"ops_application:{REPO}:{APP}"
    scenario_id = f"ops_scenario:{REPO}:1"

    # -- alerts are DEFINITIONS, keyed by (application, group, name).
    alerts = {n.node_id: n for n in nodes.values() if n.subtype == "ops_alert"}
    assert len(alerts) == 3, sorted(alerts)
    frontend = alerts[
        f"ops_alert:{REPO}:{APP}:demoapp.frontend.goldensignal:HighErrorRate"
    ]
    assert frontend.attrs["for_window"] == "3m"
    assert frontend.attrs["severity"] == "critical"
    assert 'service_name="checkout"' in frontend.attrs["expr"]
    assert frontend.attrs["description"].startswith("Error rate in")
    assert (app_id, "defines", frontend.node_id) in edges

    # The same NAME in two groups is two definitions, not one.
    backend = alerts[
        f"ops_alert:{REPO}:{APP}:demoapp.backend.goldensignal:HighErrorRate"
    ]
    assert backend.attrs["for_window"] == "5m"

    # -- a declared alert name fans out to every definition of that name.
    raises = {e for e in edges if e[1] == "raises"}
    assert raises == {
        (scenario_id, "raises", frontend.node_id),
        (scenario_id, "raises", backend.node_id),
    }
    # -- a name the library does not define is REPORTED, never stubbed.
    assert nodes[scenario_id].attrs["unresolved_alert_names"] == \
        "KubePodNotReady"
    assert source.last_run_report["unresolved_alert_names"] == \
        {"KubePodNotReady": 1}
    assert not any("KubePodNotReady" in nid for nid in nodes)

    # -- targets resolve from POSITIVE matchers only, and an unresolved rule
    #    says why rather than disappearing.
    checkout = f"ops_service:{REPO}:{APP}:checkout"
    assert (frontend.node_id, "targets", checkout) in edges
    assert frontend.attrs["targets_resolved"] is True
    assert backend.attrs["targets_resolved"] is False
    assert backend.attrs["target_selector_kind"] == "negative"
    assert "frontend" in backend.attrs["mentioned_services"]
    assert any("FailedPodsDetected (none)" in entry
               for entry in source.last_run_report["alerts_without_target"])

    # -- the recorded run becomes an INDEX, not a file count.
    run_id = f"telemetry_run:{REPO}:gpt-4o/sre/1/run-0"
    run = nodes[run_id]
    assert (scenario_id, "recorded_by", run_id) in edges
    assert run.attrs["signal_count"] == 2
    signals = [n for n in nodes.values() if n.subtype == "telemetry_signal"]
    kinds = {s.attrs["kind"]: s for s in signals}
    assert set(kinds) == {"event", "log"}
    assert kinds["event"].attrs["record_count"] == 1
    assert kinds["event"].attrs["subject_service"] == "checkout"
    assert kinds["event"].attrs["window_start"] == "2026-07-01T00:00:00Z"
    assert (kinds["event"].node_id, "about", checkout) in edges
    assert (run_id, "observed", frontend.node_id) in edges

    # -- DENY-LIST: the three trajectory files marked the dir as a run and
    #    contributed nothing else. Their marker reaches no attr.
    assert run.attrs["file_count"] == len(TRAJECTORY_DENY_LIST) + 2
    for fact in facts:
        for value in fact.attrs.values():
            assert "LEAKED-ANSWER-TOKEN" not in str(value), fact


def test_denylist_is_one_constant_shared_with_the_snapshot_mount(tmp_path):
    """The corpus side and the prompt side must not be able to drift apart."""
    from okg.bench.ab.adapters import itbench
    from okg.bench.ab.errors import DatasetError

    assert itbench.RECORDING_MARKERS is TRAJECTORY_DENY_LIST
    assert TRAJECTORY_DENY_LIST == {
        "agent_output.json", "session.jsonl", "judge_output.json",
    }
    # the mount path enforces the UNION, not just the trajectory half
    assert itbench.SNAPSHOT_DENY_LIST is SNAPSHOT_DENY_LIST
    assert SNAPSHOT_DENY_LIST == TRAJECTORY_DENY_LIST | GROUND_TRUTH_DENY_LIST

    snapshot = tmp_path / "snap"
    (snapshot / "gpt-4o" / "sre" / "1" / "run-0").mkdir(parents=True)
    (snapshot / "gpt-4o" / "sre" / "1" / "run-0"
     / "observability_alerts.json").write_text("[]")
    itbench._assert_no_denied_files(snapshot)  # clean snapshot passes

    (snapshot / "gpt-4o" / "sre" / "1" / "run-0"
     / "judge_output.json").write_text("{}")
    with pytest.raises(DatasetError, match="deny-listed file"):
        itbench._assert_no_denied_files(snapshot)


def test_ground_truth_is_denied_in_every_domain_tree(tmp_path):
    """`ground_truth.yaml` ships INSIDE the published telemetry.

    Measured on `ibm-research/ITBench-Lite` (2026-07-29): 50 files — 35 under
    `snapshots/sre/` and 15 under `snapshots/finops/`. The finops 15 are the
    regression case: a deny rule written as a path prefix over the SRE tree
    passes this file's SRE arm and still mounts 15 answer keys, so the
    exclusion is asserted per-domain, not once.
    """
    from okg.bench.ab.adapters import itbench
    from okg.bench.ab.errors import DatasetError

    assert "ground_truth.yaml" in GROUND_TRUTH_DENY_LIST

    for domain in ("sre", "finops", "ciso"):
        snapshot = tmp_path / domain
        scen = snapshot / "snapshots" / domain / "v0.2" / "Scenario-1"
        scen.mkdir(parents=True)
        (scen / "k8s_events_raw.tsv").write_text("Timestamp\tBody\n")
        itbench._assert_no_denied_files(snapshot)  # clean: passes

        (scen / "ground_truth.yaml").write_text(
            "fault:\n  - entity:\n      kind: Service\n      name: cart\n"
        )
        with pytest.raises(DatasetError, match="deny-listed file") as exc:
            itbench._assert_no_denied_files(snapshot)
        assert "ground_truth.yaml" in str(exc.value), domain


def test_ground_truth_never_becomes_a_signal_or_a_run_marker(tmp_path):
    """Two distinct properties, both required.

    1. `ground_truth.yaml` supplies no signal and no attribute.
    2. It is NOT a run marker. Letting the answer key decide which
       directories become `telemetry_run` nodes would make the corpus's
       STRUCTURE a function of the gold — a leak no attribute scan sees.
    """
    _build(tmp_path)
    trajectories = tmp_path / "trajectories"

    # (2) a directory holding ONLY the answer key is not a recorded run
    lonely = trajectories / "gpt-4o" / "sre" / "7" / "run-0"
    lonely.mkdir(parents=True)
    (lonely / "ground_truth.yaml").write_text(
        "fault:\n  - entity: {kind: Service, name: LEAKED-GOLD-ENTITY}\n"
    )
    found = _find_recordings(trajectories, adapter="t")
    assert 7 not in found, "the answer key must not mark a directory as a run"

    # (1) alongside a real artifact it is counted and never opened
    (lonely / "observability_alerts.json").write_text(
        '[{"alertname": "FailedPodsDetected", "service_name": "frontend"}]'
    )
    found = _find_recordings(trajectories, adapter="t")
    [rec] = found[7]
    assert rec.file_count == 2, "deny-listed files still count"
    assert [p.name for p in rec.artifacts] == ["observability_alerts.json"]
    assert all(p.name not in SNAPSHOT_DENY_LIST for p in rec.artifacts)


def test_recorded_run_without_a_classifiable_artifact_raises(tmp_path):
    _build(tmp_path)
    trajectories = tmp_path / "trajectories"
    run = trajectories / "gpt-4o" / "sre" / "1" / "run-0"
    run.mkdir(parents=True)
    (run / "agent_output.json").write_text("{}")
    with pytest.raises(SubstrateError, match="no classifiable observability"):
        _facts(_source(tmp_path, trajectories=trajectories))


def _write_subject_run(root: Path, *, unknown: str = "cartservice") -> Path:
    """One recorded run holding one artifact per SUBJECT-DECLARATION state.

    Four distinct signal kinds, because the signal node id is
    `...:<kind>:<subject or _>` — three unsubjected artifacts of the same
    kind would collide on one node id.
    """
    trajectories_root = root / "trajectories"
    run = trajectories_root / "gpt-4o" / "sre" / "1" / "run-0"
    run.mkdir(parents=True)
    (run / "agent_output.json").write_text("{}")
    # (1) the artifact names a service the application defines.
    (run / "observability_alerts.json").write_text(json.dumps([
        {"alertname": "HighErrorRate", "service_name": "checkout",
         "timestamp": "2026-07-01T00:00:00Z"},
    ]))
    # (2) the artifact holds no records at all.
    (run / "observability_metrics.json").write_text("[]")
    # (3) records exist and none carries a subject key.
    (run / "observability_pod_logs.jsonl").write_text(
        json.dumps({"timestamp": "2026-07-01T00:00:05Z", "msg": "boom"}) + "\n"
    )
    # (4) the artifact NAMES a service the application does not define.
    (run / "observability_traces.jsonl").write_text(
        json.dumps({"service": unknown, "timestamp": "2026-07-01T00:00:06Z"})
        + "\n"
    )
    return trajectories_root


def test_subject_stamp_is_read_from_the_artifact_not_from_the_edge(tmp_path):
    """`no_subject_reason` DISCRIMINATES, measured on the complement.

    The predecessor attribute was `subject_resolved = bool(subject)` — the
    same expression as the `about` edge's own gate — so an isolated signal
    carried `false` by construction and the ops isolation probe exempted it
    by construction. Measured on the 2026-07-28 ops generation: 2 of 2
    signals `false`, complement EMPTY. It forgave exactly the
    subject-parsing regression it claimed to catch.

    This test measures the COMPLEMENT: the set of signals carrying a stamp
    must equal the set of signals with no `about` edge, and the signal
    whose artifact named a resolvable service must carry NO stamp. Drop
    the stamp, or force it onto every signal, and one of those two
    equalities breaks.
    """
    _build(tmp_path)
    trajectories = _write_subject_run(tmp_path)
    source = _source(tmp_path, trajectories=trajectories)
    nodes, edges = _split(_facts(source))

    signals = {
        n.attrs["artifact_path"]: n
        for n in nodes.values() if n.subtype == "telemetry_signal"
    }
    assert set(signals) == {
        "observability_alerts.json", "observability_metrics.json",
        "observability_pod_logs.jsonl", "observability_traces.jsonl",
    }

    # -- the three upstream states are three DIFFERENT values, not one
    #    "unresolved" shrug.
    assert {
        path: node.attrs.get("no_subject_reason")
        for path, node in signals.items()
    } == {
        "observability_alerts.json": None,
        "observability_metrics.json": "artifact_empty",
        "observability_pod_logs.jsonl": "no_subject_declared",
        "observability_traces.jsonl": "declared_subject_unknown",
    }
    # An unresolvable name is a FINDING: it is recorded, not shrugged off.
    traces = signals["observability_traces.jsonl"]
    assert traces.attrs["declared_subject_service"] == "cartservice"
    assert traces.attrs["subject_service"] == ""
    assert source.last_run_report["unresolved_subject_services"] == [
        "gpt-4o/sre/1/run-0:observability_traces.jsonl:cartservice"
    ]
    assert source.last_run_report["no_subject_reasons"] == {
        "artifact_empty": 1,
        "no_subject_declared": 1,
        "declared_subject_unknown": 1,
    }

    # -- the circular attribute is GONE, and may not come back unnoticed.
    for node in signals.values():
        assert "subject_resolved" not in node.attrs

    # -- THE COMPLEMENT. Stamped exactly when isolated: an unstamped
    #    isolate is a lost edge and an isolation probe must fire on it, and
    #    a stamped-but-connected signal would mean the stamp forgives a
    #    node the source did in fact relate.
    connected = {e[0] for e in edges} | {e[2] for e in edges}
    isolated = {
        path for path, node in signals.items()
        if node.node_id not in connected
    }
    stamped = {
        path for path, node in signals.items()
        if "no_subject_reason" in node.attrs
    }
    assert stamped == isolated
    assert stamped == {
        "observability_metrics.json", "observability_pod_logs.jsonl",
        "observability_traces.jsonl",
    }
    # ...and the complement is NOT empty: one signal is connected and
    # unstamped, which is what makes the equality above discriminating
    # rather than trivially true.
    assert set(signals) - stamped == {"observability_alerts.json"}
    checkout = f"ops_service:{REPO}:{APP}:checkout"
    assert (
        signals["observability_alerts.json"].node_id, "about", checkout
    ) in edges


def test_a_run_resolving_none_of_its_declared_subjects_raises(tmp_path):
    """"No matches" may not masquerade as "none exist". Without this floor a
    topology regression that emptied the service vocabulary would restamp
    every signal `declared_subject_unknown` and keep building."""
    _build(tmp_path)
    trajectories = tmp_path / "trajectories"
    run = trajectories / "gpt-4o" / "sre" / "1" / "run-0"
    run.mkdir(parents=True)
    for index, kind in enumerate(
        ("logs", "metrics", "traces", "spans", "alerts", "events")
    ):
        (run / f"observability_{kind}.jsonl").write_text(
            json.dumps({
                "service": f"not-a-component-{index}",
                "timestamp": "2026-07-01T00:00:00Z",
            }) + "\n"
        )
    with pytest.raises(SubstrateError, match="resolved ZERO of them"):
        _facts(_source(tmp_path, trajectories=trajectories))


def test_topology_builds_without_any_trajectories_root(tmp_path):
    """The topology and alert families need only the Apache-2.0 clone. An
    absent trajectories_root yields no telemetry families and no error —
    the contract reports those clauses unmet, the source does not invent
    them."""
    _build(tmp_path)
    nodes, _ = _split(_facts(_source(tmp_path)))
    subtypes = {n.subtype for n in nodes.values()}
    assert {"ops_application", "ops_service", "ops_alert",
            "ops_scenario"} <= subtypes
    assert "telemetry_run" not in subtypes
    assert "telemetry_signal" not in subtypes


# ---------- loud failure modes ----------


def test_zero_scenario_files_raises(tmp_path):
    _build(tmp_path, scenarios={})
    with pytest.raises(SubstrateError, match="zero scenario index JSONs"):
        _facts(_source(tmp_path))


def test_non_string_alert_entry_raises(tmp_path):
    """All 33 alert entries across the 66 upstream indexes are bare strings;
    a new shape needs an adapter change, not a silent coercion."""
    doc = _scenario()
    doc["alerts"] = [{"id": "HighErrorRate"}]
    _build(tmp_path, scenarios={1: doc})
    with pytest.raises(SubstrateError, match="must be a non-empty alert NAME"):
        _facts(_source(tmp_path))

    broken_root = tmp_path / "broken"
    broken_root.mkdir()
    broken = _scenario()
    broken["alerts"] = "not-a-list"
    _build(broken_root, scenarios={1: broken})
    with pytest.raises(SubstrateError, match="alerts must be a list"):
        _facts(_source(broken_root))


def test_missing_config_and_missing_dirs_raise(tmp_path):
    with pytest.raises(SubstrateError, match="pass scenarios_root"):
        OpsTelemetrySource().run("rid")
    _build(tmp_path)
    with pytest.raises(SubstrateError, match="is not a directory"):
        _facts(_source(tmp_path, scenarios_root=tmp_path / "nope"))


# ---------- req.ops.contract: gold exclusion ----------


def test_gold_never_reaches_a_per_scenario_attr(tmp_path):
    _build(tmp_path)
    for fact in _facts(_source(tmp_path)):
        for attr_name, value in fact.attrs.items():
            if not isinstance(value, str):
                continue
            for gold in GOLD_STRINGS:
                assert gold.lower() not in value.lower(), (fact, attr_name)


def _strip_gold(src_root: Path, dest_root: Path) -> Path:
    """Rebuild the scenario tree with `disruptions` and `solutions` deleted."""
    dest = dest_root / "scenarios"
    dest.mkdir(parents=True)
    for path in sorted((src_root / "scenarios").glob("*.json")):
        doc = json.loads(path.read_text())
        doc.pop("disruptions", None)
        doc.pop("solutions", None)
        (dest / path.name).write_text(json.dumps(doc))
    return dest


def _stream_signature(facts: list) -> str:
    """A byte-exact rendering of the emitted fact stream, in order."""
    rows = []
    for fact in facts:
        if isinstance(fact, NodeFact):
            rows.append(["N", fact.node_id, fact.subtype, fact.op,
                         json.dumps(fact.attrs, sort_keys=True, default=str),
                         json.dumps(fact.source_record_id, sort_keys=True,
                                    default=str)])
        else:
            rows.append(["E", fact.src, fact.edge_type, fact.dst,
                         fact.provenance,
                         json.dumps(fact.attrs, sort_keys=True, default=str)])
    return json.dumps(rows)


def test_stripped_gold_equivalence(tmp_path):
    """PACT anchor (req.ops.contract). Rebuilding with `disruptions` and
    `solutions` deleted from every scenario JSON must produce a BYTE-IDENTICAL
    fact stream. This now holds by construction — no emission reads either key
    — but construction is an argument and this is the measurement."""
    _build(tmp_path)
    trajectories = _write_trajectories(tmp_path)
    full = _facts(_source(tmp_path, trajectories=trajectories))

    stripped_root = tmp_path / "stripped"
    stripped_scenarios = _strip_gold(tmp_path, stripped_root)
    stripped = _facts(_source(
        tmp_path, trajectories=trajectories,
        scenarios_root=stripped_scenarios,
    ))
    assert _stream_signature(full) == _stream_signature(stripped)


def test_stripped_gold_equivalence_is_not_vacuous(tmp_path):
    """The proof above is worthless if the comparison cannot fail.

    The leak planted here is deliberately of the kind the substring scan
    CANNOT see — an integer counting the injections, carrying no gold text at
    all. That is the whole reason stripped-gold equivalence exists as a
    separate instrument: a corpus can mark which scenario is which without
    ever quoting the answer. The byte comparison must catch it."""

    class _LeakySource(OpsTelemetrySource):
        def _scenario_facts(self, number, scenario, catalog, runs, **kw):
            for fact in super()._scenario_facts(
                number, scenario, catalog, runs, **kw
            ):
                if isinstance(fact, NodeFact) and fact.subtype == "ops_scenario":
                    fact = NodeFact(
                        node_id=fact.node_id,
                        subtype=fact.subtype,
                        attrs={
                            **fact.attrs,
                            "injection_count": len(
                                scenario.get("disruptions") or []
                            ),
                        },
                        source_record_id=fact.source_record_id,
                        source_revision=fact.source_revision,
                        op=fact.op,
                    )
                yield fact

    _build(tmp_path)
    stripped_scenarios = _strip_gold(tmp_path, tmp_path / "stripped")

    def _leaky(scenarios_root: Path) -> list:
        source = _LeakySource(
            scenarios_root=scenarios_root, library_root=tmp_path / "sre",
        )
        return list(source.run("rid", mode="scope_complete").facts)

    full = _leaky(tmp_path / "scenarios")
    stripped = _leaky(stripped_scenarios)
    leaked = next(
        f for f in full
        if isinstance(f, NodeFact) and f.subtype == "ops_scenario"
    )
    assert leaked.attrs["injection_count"] == 1
    for value in leaked.attrs.values():
        for gold in GOLD_STRINGS:
            assert gold.lower() not in str(value).lower(), (
                "the planted leak must be invisible to the substring scan, "
                "otherwise this proves the wrong instrument"
            )
    assert _stream_signature(full) != _stream_signature(stripped), (
        "a deliberately gold-derived attr must make the streams differ, "
        "otherwise the equivalence test proves nothing"
    )


def test_a_gold_quoting_attr_is_caught_by_the_substring_scan(tmp_path):
    """The complementary instrument: a per-scenario attr that QUOTES gold is
    refused at emission, without waiting for a second build to compare
    against."""

    class _QuotingSource(OpsTelemetrySource):
        def _scenario_facts(self, number, scenario, catalog, runs, **kw):
            for fact in super()._scenario_facts(
                number, scenario, catalog, runs, **kw
            ):
                if isinstance(fact, NodeFact) and fact.subtype == "ops_scenario":
                    fact = NodeFact(
                        node_id=fact.node_id,
                        subtype=fact.subtype,
                        attrs={**fact.attrs,
                               "symptom": scenario.get("description", "")},
                        source_record_id=fact.source_record_id,
                        source_revision=fact.source_revision,
                        op=fact.op,
                    )
                yield fact

    _build(tmp_path)
    source = _QuotingSource(
        scenarios_root=tmp_path / "scenarios", library_root=tmp_path / "sre",
    )
    with pytest.raises(SubstrateError, match="gold-exclusion violation"):
        list(source.run("rid", mode="scope_complete").facts)


# ---------- ops module composition ----------


def test_ops_module_composes(tmp_path):
    """A deployment manifest listing module 'ops' composes a catalog admitting
    the seven subtypes and exactly the ten narrowings the source emits."""
    deployment = tmp_path / "scratch"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump(
            {"name": "scratch", "schema_dir": "schemas", "modules": ["ops"]}
        )
    )
    (deployment / "schemas").mkdir()

    composed = compose_catalog(deployment)

    for subtype in ("ops_application", "ops_service", "ops_alert",
                    "ops_scenario", "telemetry_run", "telemetry_signal",
                    "alert_firing"):
        assert subtype in composed.subtypes, sorted(composed.subtypes)
        assert composed.subtype_origin[subtype] == "ops"
    rows = [r for r in composed.narrowings if r.origin == "ops"]
    triples = {
        (r.parent_subtype, r.edge_archetype, r.child_subtype) for r in rows
    }
    assert triples == {
        ("ops_scenario", "deploys", "ops_application"),
        ("ops_application", "runs", "ops_service"),
        ("ops_application", "defines", "ops_alert"),
        ("ops_scenario", "raises", "ops_alert"),
        ("ops_alert", "targets", "ops_service"),
        ("ops_scenario", "recorded_by", "telemetry_run"),
        ("telemetry_run", "observed", "ops_alert"),
        ("telemetry_run", "observed", "alert_firing"),
        ("alert_firing", "targets", "ops_service"),
        ("telemetry_signal", "about", "ops_service"),
    }
    # THE DISJOINTNESS, ENFORCED BY THE CATALOG ITSELF. A firing is an
    # OBSERVATION and a definition is a RULE, and the two vocabularies do not
    # intersect (18 recorded alertnames vs 9 defined, measured on the complete
    # ITBench-Lite release). No narrowing joins them in EITHER direction, so
    # an edge asserting the correspondence is refused at insert rather than
    # published as a fact neither artifact states.
    assert not [
        t for t in triples
        if {t[0], t[2]} == {"alert_firing", "ops_alert"}
    ]


# ---------- published telemetry (ibm-research/ITBench-Lite) ----------
#
# The published-telemetry path had NO test coverage: every assertion about
# firings, the workload roster and the `no_target_reason` stamp rested on a
# scratch script over the 1.576 GB release. These fixtures are shaped like the
# real release so the path is exercised hermetically.

_TELEMETRY_VERSION = "v0.2-B96DF826-4BB2-4B62-97AB-6D84254C53D7"


def _k8s_objects_tsv(path: Path, namespace: str, names: tuple[str, ...],
                     kind: str = "Deployment") -> None:
    """One 17-column OTel export whose `Body` is the Kubernetes object.

    Written with a csv writer, not a join, because that is how the real
    export encodes it: `Body` is CSV-QUOTED inside the tab-delimited file.
    """
    import csv

    from okg.substrate.library.sources.ops_telemetry import _K8S_TSV_HEADER

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter="\t", quotechar='"',
                            quoting=csv.QUOTE_MINIMAL)
        writer.writerow(_K8S_TSV_HEADER)
        for name in names:
            body = json.dumps({
                "kind": kind,
                "metadata": {"namespace": namespace, "name": name},
            })
            writer.writerow(
                ["2025-12-15T17:00:00Z", "2025-12-15T17:00:00", "", "", "",
                 "", "0", "kubernetes", body, "", "{}", "", "", "", "{}",
                 "{}", ""]
            )


def _firing(name: str, *, active_at: str, service: str | None = None,
            namespace: str = "demo-ns", description: str = "") -> dict:
    labels: dict = {"alertname": name, "severity": "critical"}
    if namespace:
        labels["namespace"] = namespace
    if service is not None:
        labels["service_name"] = service
    return {
        "labels": labels,
        "annotations": {"description": description or f"{name} fired"},
        "state": "firing",
        "activeAt": active_at,
    }


def _write_telemetry(root: Path) -> Path:
    """An ITBench-Lite-shaped release covering every branch that matters.

    Scenario-1 is INDEXED and its object dump supplies the workload roster.
    Its four firings are one per `_no_target_reason` outcome, which is what
    makes the stamp's complement measurable rather than assumed:

      RequestErrorRate      service_name=checkout   -> resolves, NO stamp
      Watchdog              no service label        -> no_service_label
      KubeletDown           service_name=prometheus-kubelet
                                                    -> service_outside_topology

    Scenario-99 is PUBLISHED WITH NO INDEX and its firings carry no namespace,
    so no application resolves and they are stamped `application_unresolved` —
    the third reason, which exists because stamping those
    `service_outside_topology` claimed the service was outside a topology
    nothing had consulted.

    The RequestErrorRate description deliberately quotes `payment-flags`, a
    string that also lives inside the scenario's `disruptions`. That is the
    real release's situation (the namespace `otel-demo` appears verbatim in
    scenario 1's disruptions) and it is why the substring exemption exists at
    all — see the equivalence test.
    """
    telemetry = root / "telemetry"
    base = telemetry / "snapshots" / "sre" / _TELEMETRY_VERSION

    one = base / "Scenario-1"
    (one / "alerts").mkdir(parents=True)
    (one / "alerts" / "alerts_at_2025-12-15T17:00:00.json").write_text(
        json.dumps([
            _firing("RequestErrorRate", active_at="2025-12-15T17:00:00Z",
                    service="checkout",
                    description="Error rate for payment-flags is above 0"),
            _firing("Watchdog", active_at="2025-12-15T17:01:00Z"),
        ])
    )
    # the SECOND published pattern, wrapping the same shape
    (one / "alerts"
     / "alerts_in_alerting_state_2025-12-15T17:02:00.json").write_text(
        json.dumps({"data": {"alerts": [
            _firing("KubeletDown", active_at="2025-12-15T17:02:00Z",
                    service="prometheus-kubelet"),
            # a duplicate poll of the SAME firing: dedup keys on
            # (alertname, activeAt), so it must not become a second node
            _firing("Watchdog", active_at="2025-12-15T17:01:00Z"),
        ]}})
    )
    _k8s_objects_tsv(one / "k8s_objects_raw.tsv", "demo-ns",
                     ("checkout", "payment", "frontend", "cart"))
    # the answer key ships inside the release; it is counted, never opened
    (one / "ground_truth.yaml").write_text(
        "fault:\n  - entity: {kind: Service, name: LEAKED-GOLD-ENTITY}\n"
    )

    ninety_nine = base / "Scenario-99"
    (ninety_nine / "alerts").mkdir(parents=True)
    (ninety_nine / "alerts" / "alerts_at_2025-12-15T18:00:00.json").write_text(
        json.dumps([
            _firing("OrphanAlert", active_at="2025-12-15T18:00:00Z",
                    service="checkout", namespace=""),
        ])
    )
    return telemetry


def _telemetry_source(tmp_path: Path, *, telemetry: Path,
                      scenarios_root: Path | None = None,
                      cls=OpsTelemetrySource) -> OpsTelemetrySource:
    return cls(
        scenarios_root=scenarios_root or (tmp_path / "scenarios"),
        library_root=tmp_path / "sre",
        telemetry_root=telemetry,
    )


def test_published_telemetry_becomes_firings_not_definitions(tmp_path):
    """A firing is an OBSERVATION: no expr, no edge to any definition, and a
    dedup key of (alertname, activeAt) rather than one node per polled file."""
    _build(tmp_path)
    telemetry = _write_telemetry(tmp_path)
    source = _telemetry_source(tmp_path, telemetry=telemetry)
    nodes, edges = _split(_facts(source))

    firings = {
        n.attrs["alert_name"]: n
        for n in nodes.values() if n.subtype == "alert_firing"
    }
    assert set(firings) == {
        "RequestErrorRate", "Watchdog", "KubeletDown", "OrphanAlert",
    }
    # the Watchdog duplicate across two polled files is ONE node
    assert sum(1 for n in nodes.values() if n.subtype == "alert_firing") == 4

    # -- an observation carries no rule body, in either direction.
    for firing in firings.values():
        assert "expr" not in firing.attrs, firing.node_id
        assert firing.attrs["alert_name"]
        assert firing.attrs["active_at"]
    definitions = {
        n.node_id for n in nodes.values() if n.subtype == "ops_alert"
    }
    firing_ids = {n.node_id for n in firings.values()}
    assert not [
        e for e in edges
        if ({e[0], e[2]} & definitions) and ({e[0], e[2]} & firing_ids)
    ], "no edge may join a firing to a definition"

    # -- every firing hangs off its run.
    run_one = f"telemetry_run:{REPO}:1"
    assert nodes[run_one].attrs["alert_firing_count"] == 3
    assert nodes[run_one].attrs["has_object_dump"] is True
    assert nodes[run_one].attrs["scenario_indexed"] is True
    for name in ("RequestErrorRate", "Watchdog", "KubeletDown"):
        assert (run_one, "observed", firings[name].node_id) in edges
    assert (f"ops_scenario:{REPO}:1", "recorded_by", run_one) in edges

    # -- a snapshot the 66-index tree does not define is EMITTED and SAID,
    #    never silently subtracted from a floor.
    run_99 = f"telemetry_run:{REPO}:99"
    assert nodes[run_99].attrs["scenario_indexed"] is False
    assert not [e for e in edges if e[1] == "recorded_by" and e[2] == run_99]
    assert source.last_run_report["telemetry_scenarios_without_index"] == [99]

    # -- the deny-listed answer key is counted, never read.
    assert nodes[run_one].attrs["file_count"] == 3  # 2 alert files + the TSV
    for fact in _facts(source):
        for value in fact.attrs.values():
            assert "LEAKED-GOLD-ENTITY" not in str(value), fact


def test_workload_roster_supersedes_the_helm_reconstruction(tmp_path):
    """The telemetry's own object dump is the better source-carried statement
    of the component set, and it is applied PER APPLICATION rather than
    globally — an application with no published telemetry keeps the
    reconstruction rather than losing its topology."""
    _build(tmp_path)
    telemetry = _write_telemetry(tmp_path)
    source = _telemetry_source(tmp_path, telemetry=telemetry)
    nodes, _ = _split(_facts(source))

    services = {
        n.attrs["service_name"]: n.attrs["origin"]
        for n in nodes.values() if n.subtype == "ops_service"
    }
    # `cart` is named by NO library artifact — it exists only because the
    # workload dump names it, which is the whole point of the supersession.
    assert services == {
        "cart": "k8s_workload",
        "checkout": "k8s_workload",
        "frontend": "k8s_workload",
        "payment": "k8s_workload",
        # named only by the install task, absent from the dump: retained,
        # because dropping it would lose a source-carried component
        "load-generator": "alert_selector",
    }
    assert source.last_run_report["roster_source"] == {APP: "k8s_workload"}


# ---------- the no_target_reason stamp, and its complement ----------


def _firing_stamp_invariants(facts: list) -> None:
    """The four properties a firing's `no_target_reason` must satisfy.

    Raises AssertionError on any violation. This is the exact invariant the
    corpus contract's two firing probes gate, hoisted into a function so the
    mutation pairs below can prove each one RED.
    """
    nodes, edges = _split(facts)
    firings = [n for n in nodes.values() if n.subtype == "alert_firing"]
    services = {
        n.attrs["service_name"]
        for n in nodes.values() if n.subtype == "ops_service"
    }
    service_ids = {
        n.node_id for n in nodes.values() if n.subtype == "ops_service"
    }
    targeted = {e[0] for e in edges if e[1] == "targets"}

    stamped = {f.node_id for f in firings if "no_target_reason" in f.attrs}
    untargeted = {f.node_id for f in firings if f.node_id not in targeted}

    # (1) THE BICONDITIONAL. Left-to-right catches a lost edge; right-to-left
    #     catches a stamp forgiving a firing the source did relate.
    assert stamped == untargeted, (
        f"stamped-but-targeted={sorted(stamped - untargeted)}, "
        f"untargeted-but-unstamped={sorted(untargeted - stamped)}"
    )

    # (2) EACH VALUE AGREES WITH THE DECLARATION ON THE NODE. The stamp is
    #     read from the parsed firing, so it is checkable against what the
    #     firing declared — a stamp that were emission-derived would agree
    #     with (1) by construction and say nothing here.
    for firing in firings:
        reason = firing.attrs.get("no_target_reason")
        label = firing.attrs["declared_service_label"]
        where = f"{firing.node_id} declared_service_label={label!r}"
        if reason is None:
            assert label and label in services, (
                f"{where}: NO stamp claims the firing named a service the "
                f"topology defines, but it did not"
            )
        elif reason == "no_service_label":
            assert not label, (
                f"{where}: stamped no_service_label, but the firing DID "
                f"carry a service label"
            )
        elif reason == "service_outside_topology":
            assert label and label not in services, (
                f"{where}: stamped service_outside_topology, but the label "
                f"is {'empty' if not label else 'a defined component'}"
            )
        elif reason == "application_unresolved":
            # no topology was consulted for this snapshot at all
            assert not any(
                e[1] == "targets" and e[0] == firing.node_id for e in edges
            ), firing.node_id
            assert label, (
                "application_unresolved says the APPLICATION did not resolve; "
                "a firing with no service label is no_service_label"
            )
        else:
            raise AssertionError(f"unknown reason {reason!r}")

    # (3) THE COMPLEMENT IS NON-EMPTY. A predicate true of the whole
    #     population discriminates nothing — this is the check that catches a
    #     forced stamp, which (1) cannot see because forcing the stamp also
    #     suppresses the edge.
    unstamped = {f.node_id for f in firings} - stamped
    assert unstamped, (
        "every firing is stamped, so the stamp forgives everything and "
        "measures nothing"
    )

    # (4) NO DANGLING TARGET. Dropping a stamp without dropping its cause
    #     emits a targets edge at a service node that was never created.
    for edge in edges:
        if edge[1] == "targets" and edge[0] in {f.node_id for f in firings}:
            assert edge[2] in service_ids, f"dangling targets edge {edge}"


def test_firing_stamp_discriminates_on_the_real_shape(tmp_path):
    """All three reasons are populated, and the complement is not empty."""
    _build(tmp_path)
    telemetry = _write_telemetry(tmp_path)
    source = _telemetry_source(tmp_path, telemetry=telemetry)
    facts = _facts(source)
    nodes, edges = _split(facts)

    reasons = {
        n.attrs["alert_name"]: n.attrs.get("no_target_reason")
        for n in nodes.values() if n.subtype == "alert_firing"
    }
    assert reasons == {
        "RequestErrorRate": None,            # resolves: NO stamp
        "Watchdog": "no_service_label",
        "KubeletDown": "service_outside_topology",
        "OrphanAlert": "application_unresolved",
    }
    assert source.last_run_report["no_target_reasons"] == {
        "no_service_label": 1,
        "service_outside_topology": 1,
        "application_unresolved": 1,
    }
    assert source.last_run_report["firing_services_outside_topology"] == {
        "prometheus-kubelet": 1,
    }
    # `OrphanAlert` NAMED `checkout`, a real component. It is NOT stamped
    # service_outside_topology, because no topology was ever consulted for
    # its snapshot — conflating the two was a live defect.
    orphan = next(n for n in nodes.values()
                  if n.attrs.get("alert_name") == "OrphanAlert")
    assert orphan.attrs["declared_service_label"] == "checkout"

    checkout = f"ops_service:{REPO}:{APP}:checkout"
    resolved = next(n for n in nodes.values()
                    if n.attrs.get("alert_name") == "RequestErrorRate")
    assert (resolved.node_id, "targets", checkout) in edges

    _firing_stamp_invariants(facts)


#: (label, patch) for each mutation. `patch` replaces `_no_target_reason`.
#: Two per stamp value: DROP it where the input says it applies, and FORCE it
#: onto every firing. Both must turn `_firing_stamp_invariants` RED, or the
#: stamp is decorative.
_STAMP_VALUES = (
    "no_service_label", "service_outside_topology", "application_unresolved",
)


@pytest.mark.parametrize("value", _STAMP_VALUES)
@pytest.mark.parametrize("mode", ("drop", "force"))
def test_no_target_reason_mutations_are_red(tmp_path, monkeypatch, value, mode):
    """SIX MUTATIONS, ALL RED — the program's standard for a stranding stamp.

    A stamp is only worth its probe if perturbing it breaks something. Drop
    the stamp and an untargeted firing goes unexplained; force it and the
    complement empties, so the stamp forgives every firing including the ones
    the source did relate. This standard caught two circular stamps in this
    program, and the third reason (`application_unresolved`) exists because a
    fresh instance of the same defect was committed while fixing it.
    """
    from okg.substrate.library.sources import ops_telemetry

    original = ops_telemetry._no_target_reason

    if mode == "drop":
        def mutant(firing, service_names):
            reason = original(firing, service_names)
            return None if reason == value else reason
    else:
        def mutant(firing, service_names):
            return value

    _build(tmp_path)
    telemetry = _write_telemetry(tmp_path)

    # the UNMUTATED build satisfies the invariants — otherwise a red mutation
    # would prove nothing about the mutation.
    clean = _facts(_telemetry_source(tmp_path, telemetry=telemetry))
    _firing_stamp_invariants(clean)

    monkeypatch.setattr(ops_telemetry, "_no_target_reason", mutant)
    mutated = _facts(_telemetry_source(tmp_path, telemetry=telemetry))
    with pytest.raises(AssertionError):
        _firing_stamp_invariants(mutated)


# ---------- stripped-gold equivalence over the TELEMETRY inputs ----------


def test_a_firing_quotes_gold_so_the_exemption_is_load_bearing(tmp_path):
    """MEASURE the coincidence before trusting the exemption that dodges it.

    `_assert_gold_excluded` exempts `alert_firing` and `telemetry_run` from
    the SUBSTRING scan. An exemption over a population the scan would never
    have flagged costs nothing and proves nothing; this asserts the opposite —
    the scan WOULD refuse this corpus without it, exactly as it does on the
    real release, where the namespace `otel-demo` appears verbatim inside
    scenario 1's `disruptions`.
    """
    _build(tmp_path)
    telemetry = _write_telemetry(tmp_path)
    facts = _facts(_telemetry_source(tmp_path, telemetry=telemetry))

    scenario = json.loads((tmp_path / "scenarios" / "1.json").read_text())
    fragments = {
        value.strip().lower()
        for value in json.dumps(
            [scenario.get(k) for k in ("solutions", "disruptions",
                                       "description")]
        ).split('"')
        if len(value.strip()) >= 8
    }
    quoting = [
        (f.node_id, attr, frag)
        for f in facts if getattr(f, "subtype", None) == "alert_firing"
        for attr, val in f.attrs.items() if isinstance(val, str)
        for frag in fragments if frag in val.lower()
    ]
    assert quoting, (
        "the fixture no longer reproduces the gold/telemetry text "
        "coincidence, so the exemption below is being proven against a "
        "population that never needed it"
    )


def _strip_gold_keys(src_root: Path, dest_root: Path,
                     keys: tuple[str, ...]) -> Path:
    dest = dest_root / "scenarios"
    dest.mkdir(parents=True)
    for path in sorted((src_root / "scenarios").glob("*.json")):
        doc = json.loads(path.read_text())
        for key in keys:
            doc.pop(key, None)
        (dest / path.name).write_text(json.dumps(doc))
    return dest


@pytest.mark.parametrize(
    "keys",
    [
        ("disruptions", "solutions"),
        # the WHOLE fragment set the scan draws on. Stripping only the first
        # two leaves the `description` arm of the exemption unproven, and
        # `description` is the field that names the faulty component in prose
        # on 59 of 66 upstream scenarios.
        ("disruptions", "solutions", "description"),
    ],
)
def test_stripped_gold_equivalence_covers_the_exempt_telemetry_facts(
    tmp_path, keys,
):
    """THE PROOF THE SUBSTRING EXEMPTION RESTS ON.

    `alert_firing` and `telemetry_run` are exempt from the substring scan on
    the argument that they are a pure function of the telemetry artifact and
    never receive a scenario document. That argument is provable directly:
    delete the gold keys from every scenario JSON and rebuild. If a single
    byte of the fact stream moves, the firings ARE derived from the scenario
    document and the exemption is wrong — in which case it must be removed,
    not widened.
    """
    _build(tmp_path)
    telemetry = _write_telemetry(tmp_path)
    full = _facts(_telemetry_source(tmp_path, telemetry=telemetry))

    stripped_scenarios = _strip_gold_keys(
        tmp_path, tmp_path / f"stripped-{len(keys)}", keys,
    )
    stripped = _facts(_telemetry_source(
        tmp_path, telemetry=telemetry, scenarios_root=stripped_scenarios,
    ))
    assert _stream_signature(full) == _stream_signature(stripped)


def test_stripped_gold_equivalence_over_telemetry_is_not_vacuous(tmp_path):
    """The negative control, planted on the EXEMPT subtype.

    A control on a subtype that was never exempt would prove the instrument
    works somewhere else. This one stamps a gold-derived integer onto every
    `alert_firing` — the exact path the exemption covers — and the integer
    carries no gold TEXT, so the substring scan could not catch it even if
    the exemption were lifted. If the streams still match, equivalence is
    measuring nothing and the exemption rests on nothing.
    """

    class _LeakyFiringSource(OpsTelemetrySource):
        def _parse_scenario(self, path, raw):
            number, scenario = super()._parse_scenario(path, raw)
            # read from the document THIS RUN parsed. A first draft of this
            # control read the unstripped file by path, so both arms saw the
            # same gold and the control silently could not fire.
            self.__dict__.setdefault("_leak", {})[number] = len(
                scenario.get("disruptions") or []
            )
            return number, scenario

        def _snapshot_facts(self, snap, scenario_node_id, app, **kw):
            leak = self.__dict__.get("_leak", {}).get(snap.number, -1)
            for fact in super()._snapshot_facts(
                snap, scenario_node_id, app, **kw
            ):
                if isinstance(fact, NodeFact) and fact.subtype == "alert_firing":
                    fact = NodeFact(
                        node_id=fact.node_id,
                        subtype=fact.subtype,
                        attrs={**fact.attrs, "injection_count": leak},
                        source_record_id=fact.source_record_id,
                        source_revision=fact.source_revision,
                        op=fact.op,
                    )
                yield fact

    _build(tmp_path)
    telemetry = _write_telemetry(tmp_path)
    stripped_scenarios = _strip_gold_keys(
        tmp_path, tmp_path / "stripped", ("disruptions", "solutions"),
    )

    full = _facts(_telemetry_source(
        tmp_path, telemetry=telemetry, cls=_LeakyFiringSource,
    ))
    stripped = _facts(_telemetry_source(
        tmp_path, telemetry=telemetry, scenarios_root=stripped_scenarios,
        cls=_LeakyFiringSource,
    ))

    planted = [
        f.attrs["injection_count"] for f in full
        if getattr(f, "subtype", None) == "alert_firing"
    ]
    assert planted and max(planted) == 1
    for value in planted:
        for gold in GOLD_STRINGS:
            assert gold.lower() not in str(value).lower(), (
                "the planted leak must be invisible to the substring scan, "
                "otherwise this proves the wrong instrument"
            )
    assert _stream_signature(full) != _stream_signature(stripped), (
        "a gold-derived attr on the EXEMPT subtype must make the streams "
        "differ, otherwise the exemption is unproven"
    )
