"""Reusable ingest service contracts."""
from __future__ import annotations

import argparse
import inspect
import itertools
import json
import os
import subprocess
import sys
import threading
import time
import types
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg
import pytest
import yaml

import okg.substrate.ingest.service as ingest_service
import okg.substrate.ingest.adapter_factory as adapter_factory
from okg.substrate.catalog.ownership import CatalogIdentity
from okg.substrate.cli.main import _cmd_generation
from okg.substrate.errors import SourceRegistrySelectorAbsent, SubstrateError
from okg.substrate.ingest import (
    IngestOptions,
    IngestPlan,
    plan_ingest_sources,
    resolve_predecessor_deps,
    run_ingest,
    topo_layers,
    topo_sort,
)
from okg.substrate.publisher.repository_provenance import RepositoryProvenance
from okg.substrate.source_sync import SourceSyncEvent
from okg.substrate.sources.registry import (
    load_source_registry,
    load_source_registry_admission,
)
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.pre_3342_freeze import freeze_partial_failures
from tests.pytest.source_jobs_inline import use_inline_source_jobs


@pytest.fixture(autouse=True)
def _dummy_dsn_takes_no_source_sync_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The #3003 run guard needs a real session; the dummy database has none.
    Its behaviour is proven against PostgreSQL in test_publisher.py."""
    real_guard = ingest_service.ingest_run_guard

    def guard(**kwargs: object) -> object:
        if kwargs["dsn"] == "postgres://unused":
            return nullcontext()
        return real_guard(**kwargs)

    monkeypatch.setattr(ingest_service, "ingest_run_guard", guard)


@pytest.fixture(autouse=True)
def _dummy_dsn_has_no_staged_source_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pure service tests use a deliberately nonexistent database.

    Sources run in this process: these tests replace service internals such
    as ``_run_source`` with stubs, which a spawned source child cannot see.
    The process path has its own tests in ``test_source_jobs.py``.
    """
    use_inline_source_jobs(monkeypatch)
    real_probe = ingest_service._staged_source_redrive_state
    real_recovery_admissions = (
        ingest_service._recovery_candidate_admission_ids
    )

    def probe(options: IngestOptions) -> dict[str, object]:
        if options.dsn == "postgres://unused":
            return {
                "staged_work_present": False,
                "latest_checkpoint_id": None,
                "latest_checkpoint_status": None,
                "latest_blocked_reason": None,
                "latest_blocked_receipt": {},
                "latest_published_checkpoint_id": None,
                "branch_mutation_token": "unit-test-branch-token",
            }
        return real_probe(options)

    monkeypatch.setattr(
        ingest_service,
        "_staged_source_redrive_state",
        probe,
    )
    monkeypatch.setattr(
        ingest_service,
        "_recovery_candidate_admission_ids",
        lambda options, **kwargs: (
            []
            if options.dsn == "postgres://unused"
            else real_recovery_admissions(options, **kwargs)
        ),
    )


def test_run_ingest_service_handles_empty_registry(tmp_path: Path) -> None:
    dep = tmp_path / "deployments" / "empty"
    dep.mkdir(parents=True)
    (dep / "source_registry.yaml").write_text("sources: {}\n")

    outcome = run_ingest(
        IngestOptions(
            dsn="postgres://unused",
            deployment="empty",
            deployment_dir=dep,
            publish=False,
        ),
    )

    assert outcome.any_failed is False
    assert {
        key: outcome.report[key]
        for key in ("deployment", "ordered", "results", "publishes", "skipped")
    } == {
        "deployment": "empty",
        "ordered": [],
        "results": [],
        "publishes": [],
        "skipped": [],
    }
    assert outcome.report["timing_status"] == "available"
    assert outcome.report["layer_timings"] == []
    assert outcome.report["timings_ms"]["total"] >= 0


def test_strict_runtime_selector_refuses_mixed_absent_request_before_planning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = tmp_path / "deployments" / "strict"
    dep.mkdir(parents=True)
    (dep / "source_registry.yaml").write_text(
        """
admission_contract: strict_v1
sources:
  dormant:
    module: tests.strict
    class: Adapter
    ownership_id: proof.dormant
    source_class: reference_catalog
    record_identity_kind: domain_key
    record_identity_fields: [id]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
    enabled_by_default: false
    params: {fixture_path: /tmp/dormant.json}
""".lstrip(),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        ingest_service,
        "plan_source_registry_bindings",
        lambda *_args, **_kwargs: pytest.fail(
            "binding planner ran before explicit selectors were validated",
        ),
    )

    with pytest.raises(SourceRegistrySelectorAbsent) as exc:
        run_ingest(
            IngestOptions(
                dsn="postgres://unused",
                deployment="strict",
                deployment_dir=dep,
                include=("proof.dormant", "removed"),
                exclude=("removed",),
                publish=False,
                require_declared_sources=True,
            ),
        )

    assert exc.value.code == "source_registry_selector_absent"
    assert exc.value.data == {
        "deployment": "strict",
        "absent_selectors": ["removed"],
        "declared_source_names": ["dormant"],
        "declared_ownership_ids": ["proof.dormant"],
    }


def test_strict_runtime_selector_rechecks_fresh_registry_before_replan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = tmp_path / "deployments" / "strict"
    dep.mkdir(parents=True)
    registry_path = dep / "source_registry.yaml"
    registry_path.write_text(
        """
admission_contract: strict_v1
sources:
  selected:
    module: tests.strict
    class: Adapter
    ownership_id: proof.selected
    source_class: reference_catalog
    record_identity_kind: domain_key
    record_identity_fields: [id]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
    params: {fixture_path: /tmp/selected.json}
""".lstrip(),
        encoding="utf-8",
    )
    planning_calls = 0

    def stale_plan(*_args, **_kwargs):
        nonlocal planning_calls
        planning_calls += 1
        if planning_calls > 1:
            pytest.fail("fresh absent selector reached binding replanning")
        registry_path.write_text(
            "admission_contract: strict_v1\nsources: {}\n",
            encoding="utf-8",
        )
        raise ValueError("force strict reload")

    monkeypatch.setattr(ingest_service, "plan_source_registry_bindings", stale_plan)
    monkeypatch.setattr(ingest_service, "evict_module_tree", lambda _name: None)

    with pytest.raises(SourceRegistrySelectorAbsent) as exc:
        run_ingest(
            IngestOptions(
                dsn="postgres://unused",
                deployment="strict",
                deployment_dir=dep,
                include=("proof.selected",),
                publish=False,
                require_declared_sources=True,
            ),
        )

    assert exc.value.data["absent_selectors"] == ["proof.selected"]
    assert planning_calls == 1


def test_strict_runtime_selector_rechecks_before_stale_constructor_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = tmp_path / "deployments" / "strict"
    dep.mkdir(parents=True)
    registry_path = dep / "source_registry.yaml"
    registry_path.write_text(
        """
admission_contract: strict_v1
sources:
  selected:
    module: tests.strict
    class: Adapter
    ownership_id: proof.selected
    source_class: reference_catalog
    record_identity_kind: domain_key
    record_identity_fields: [id]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
    params: {fixture_path: /tmp/selected.json}
""".lstrip(),
        encoding="utf-8",
    )
    planning_calls = 0

    def plan_bindings(admission, **_kwargs):
        nonlocal planning_calls
        planning_calls += 1
        if planning_calls > 1:
            pytest.fail("fresh absent selector reached binding replanning")
        return types.SimpleNamespace(plans={"selected": types.SimpleNamespace(
            entry=admission.entries["selected"],
            init_params={},
        )})

    def stale_constructor(_plan):
        registry_path.write_text(
            "admission_contract: strict_v1\nsources: {}\n",
            encoding="utf-8",
        )
        raise TypeError("stale constructor signature")

    monkeypatch.setattr(ingest_service, "plan_source_registry_bindings", plan_bindings)
    monkeypatch.setattr(
        ingest_service,
        "construct_source_adapter_from_plan",
        stale_constructor,
    )
    monkeypatch.setattr(
        ingest_service,
        "classify_source_construction_failure",
        lambda **_kwargs: {"cause": "stale_loaded_code"},
    )
    monkeypatch.setattr(ingest_service, "evict_module_tree", lambda _name: None)

    with pytest.raises(SourceRegistrySelectorAbsent) as exc:
        run_ingest(
            IngestOptions(
                dsn="postgres://unused",
                deployment="strict",
                deployment_dir=dep,
                include=("selected",),
                publish=False,
                require_declared_sources=True,
            ),
        )

    assert exc.value.data["absent_selectors"] == ["selected"]
    assert planning_calls == 1


def test_undeclared_selector_remains_ignored_for_legacy_direct_ingest(
    tmp_path: Path,
) -> None:
    dep = tmp_path / "deployments" / "legacy"
    dep.mkdir(parents=True)
    (dep / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")

    outcome = run_ingest(
        IngestOptions(
            dsn="postgres://unused",
            deployment="legacy",
            deployment_dir=dep,
            include=("undeclared",),
            publish=False,
        ),
    )

    assert outcome.report["ordered"] == []
    assert IngestOptions(
        dsn="postgres://unused",
        deployment="legacy",
        deployment_dir=dep,
    ).require_declared_sources is False


def test_strict_document_refuses_before_constructor_thread_or_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = tmp_path / "deployments" / "strict"
    dep.mkdir(parents=True)
    (dep / "source_registry.yaml").write_text(
        """
admission_contract: strict_v1
sources:
  valid:
    module: tests.strict
    class: Adapter
    ownership_id: proof.valid
    source_class: reference_catalog
    record_identity_kind: domain_key
    record_identity_fields: [id]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
    params: {fixture_path: /tmp/valid.json}
  invalid:
    module: tests.strict
    class: Adapter
    ownership_id: proof.invalid
    source_class: reference_catalog
    record_identity_kind: domain_key
    record_identity_fields: [id]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
    params: {fixture_path: /tmp/invalid.json, unconsumed: true}
""".lstrip(),
        encoding="utf-8",
    )
    constructor_calls: list[str] = []

    class Adapter:
        profile = "reference_catalog"

        def __init__(self, *, fixture_path: str) -> None:
            constructor_calls.append(fixture_path)

    monkeypatch.setattr(
        adapter_factory,
        "import_source_adapter_class",
        lambda _entry: Adapter,
    )
    monkeypatch.setattr(
        ingest_service,
        "_prepare_source_admission",
        lambda **_kwargs: pytest.fail("admission mutated before barrier"),
    )

    with pytest.raises(ValueError, match=r"invalid.*unconsumed"):
        run_ingest(
            IngestOptions(
                dsn="postgres://unused",
                deployment="strict",
                deployment_dir=dep,
                publish=False,
            )
        )

    assert constructor_calls == []


def test_strict_planning_failure_reloads_and_replans_whole_document(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry_path = tmp_path / "source_registry.yaml"
    registry_path.write_text(
        """
admission_contract: strict_v1
sources:
  strict:
    module: tests.strict
    class: Adapter
    ownership_id: proof.strict
    source_class: reference_catalog
    record_identity_kind: domain_key
    record_identity_fields: [id]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
    params: {fixture_path: /tmp/strict.json}
""".lstrip(),
        encoding="utf-8",
    )
    admission = load_source_registry_admission(registry_path)
    calls: list[str] = []

    def plan(_admission, *, dsn, deployment):
        del dsn, deployment
        calls.append("plan")
        if len(calls) == 1:
            raise ValueError("source_param_unconsumed: stale signature")
        return types.SimpleNamespace(plans={"strict": "fresh-plan"})

    monkeypatch.setattr(ingest_service, "plan_source_registry_bindings", plan)
    monkeypatch.setattr(ingest_service, "evict_module_tree", lambda _name: None)

    fresh, plans = ingest_service._plan_strict_admission_with_recovery(
        admission=admission,
        dsn="postgresql://redacted",
        deployment="test",
        registry_path=registry_path,
        repo_root_override=tmp_path,
    )

    assert calls == ["plan", "plan"]
    assert fresh.registry_path == registry_path.resolve()
    assert plans == {"strict": "fresh-plan"}


def test_source_param_dsn_placeholder_uses_deployment_dsn() -> None:
    assert ingest_service._source_param_uses_deployment_dsn_placeholder(
        "${OKG_DSN}",
    ) is True
    assert ingest_service._source_param_uses_deployment_dsn_placeholder(
        "$OKG_DSN",
    ) is True
    assert ingest_service._source_param_uses_deployment_dsn_placeholder(
        "postgres://literal/db",
    ) is False
    assert ingest_service._source_param_uses_deployment_dsn_placeholder(
        "${OTHER_DSN}",
    ) is False
    assert ingest_service._source_param_uses_deployment_dsn_placeholder(
        None,
    ) is False


def test_preimport_failure_does_not_leave_later_modules_unseeded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broken = "broken_preimport_source"
    working = "working_preimport_source"
    (tmp_path / f"{broken}.py").write_text(
        "raise RuntimeError('intentional pre-import failure')\n",
        encoding="utf-8",
    )
    (tmp_path / f"{working}.py").write_text(
        "LOADED = True\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    sys.modules.pop(broken, None)
    sys.modules.pop(working, None)
    registry = _load_registry(tmp_path, {
        "broken": {
            **_source("demo.broken"),
            "module": broken,
            "class": "Adapter",
        },
        "working": {
            **_source("demo.working"),
            "module": working,
            "class": "Adapter",
        },
    })
    plan = plan_ingest_sources(registry)

    ingest_service._preimport_layer_source_modules(
        plan,
        ["broken", "working"],
    )

    assert broken not in sys.modules
    assert getattr(sys.modules[working], "LOADED") is True


def test_service_import_helper_serializes_concurrent_first_imports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_names = [f"deployment_race_source_{index}" for index in range(8)]
    for module_name in module_names:
        (tmp_path / f"{module_name}.py").write_text(
            f"IDENTITY = {module_name!r}\n",
            encoding="utf-8",
        )
        sys.modules.pop(module_name, None)
    monkeypatch.syspath_prepend(str(tmp_path))

    real_import = ingest_service.importlib.import_module
    guard = threading.Lock()
    active = 0
    max_active = 0

    def tracked_import(name: str):
        nonlocal active, max_active
        with guard:
            active += 1
            max_active = max(max_active, active)
        try:
            time.sleep(0.01)
            return real_import(name)
        finally:
            with guard:
                active -= 1

    monkeypatch.setattr(ingest_service.importlib, "import_module", tracked_import)
    with ThreadPoolExecutor(max_workers=len(module_names)) as pool:
        modules = list(pool.map(ingest_service._import_source_module, module_names))

    assert max_active == 1
    assert {module.IDENTITY for module in modules} == set(module_names)


def test_live_planning_characterization_before_move(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Freeze the live service-owned planning contract before extraction."""
    parsed = _source("demo.parsed")
    parsed["predecessor_source"] = ["demo.raw", "side"]
    index = _source("demo.index", predecessor_source="parsed")
    index["sync"] = {
        "triggers": ["fs_event", "manual", "reconcile"],
        "default_event_mode": "scope_incomplete",
        "reconcile_mode": "scope_complete",
        "scope": {
            "accepts": ["path"],
            "unsupported_fallback": "scope_complete",
        },
    }
    registry = _load_registry(tmp_path, {
        "raw": _source("demo.raw", enabled_by_default=False),
        "side": _source("demo.side"),
        "parsed": parsed,
        "index": index,
        "orphan": _source("demo.orphan", predecessor_source="missing"),
    })

    ingest_api = sys.modules["okg.substrate.ingest"]
    planning_exports = {
        "IngestPlan",
        "plan_ingest_sources",
        "resolve_predecessor_deps",
        "topo_layers",
        "topo_sort",
    }
    assert planning_exports <= set(ingest_service.__all__)
    assert planning_exports <= set(ingest_api.__all__)
    assert ingest_api.IngestPlan is ingest_service.IngestPlan is IngestPlan
    assert ingest_api.plan_ingest_sources is ingest_service.plan_ingest_sources
    assert ingest_api.resolve_predecessor_deps is resolve_predecessor_deps
    assert ingest_api.topo_layers is topo_layers
    assert ingest_api.topo_sort is topo_sort
    assert tuple(inspect.signature(plan_ingest_sources).parameters) == (
        "registry",
        "include",
        "exclude",
        "include_predecessors",
        "include_dependents",
        "dependent_fanout",
    )

    plan = plan_ingest_sources(registry, include={"demo.index"})

    assert list(plan.runnable) == ["index", "parsed", "raw", "side"]
    assert plan.runnable["index"] is registry["index"]
    assert plan.deps == {
        "index": {"parsed"},
        "parsed": {"raw", "side"},
        "raw": set(),
        "side": set(),
    }
    assert plan.layers == [["raw", "side"], ["parsed"], ["index"]]
    assert plan.ordered == ("raw", "side", "parsed", "index")
    assert resolve_predecessor_deps(plan.runnable) == plan.deps
    assert topo_layers(plan.deps) == plan.layers
    assert topo_sort(plan.deps) == list(plan.ordered)
    assert IngestPlan(plan.runnable, plan.deps, plan.layers, plan.ordered) == plan

    empty = plan_ingest_sources(registry, include={"missing"})
    assert empty == IngestPlan(runnable={}, deps={}, layers=[], ordered=())
    assert plan_ingest_sources(
        registry,
        include={"demo.index"},
        exclude={"missing"},
    ) == plan
    unresolved_predecessor = plan_ingest_sources(
        registry,
        include={"demo.orphan"},
    )
    assert unresolved_predecessor.deps == {"orphan": set()}
    assert unresolved_predecessor.ordered == ("orphan",)

    probe_call: dict[str, object] = {}

    def valid_probe(name: str, body: object, **kwargs: object) -> list[dict]:
        probe_call.update(name=name, body=body, kwargs=kwargs)
        return []

    monkeypatch.setattr(
        "okg.substrate.deployment_lint.source_activation_probe_findings",
        valid_probe,
    )
    registry_path = tmp_path / "source_registry.yaml"
    ingest_service._validate_deferred_source_activation(
        plan,
        deployment_dir=tmp_path,
        registry_path=registry_path,
    )
    assert probe_call == {
        "name": "raw",
        "body": registry["raw"].raw,
        "kwargs": {
            "deployment_dir": tmp_path,
            "registry_path": registry_path,
        },
    }

    finding = {
        "code": "deployment.source_registry.probe_missing",
        "severity": "blocker",
    }
    monkeypatch.setattr(
        "okg.substrate.deployment_lint.source_activation_probe_findings",
        lambda *_args, **_kwargs: [finding],
    )
    with pytest.raises(SubstrateError) as blocked:
        ingest_service._validate_deferred_source_activation(
            plan,
            deployment_dir=tmp_path,
            registry_path=registry_path,
        )
    assert blocked.value.code == "source_activation_probe_invalid"
    assert blocked.value.data == {"sources": {"raw": [finding]}}

    probe_error = RuntimeError("activation probe unavailable")

    def unavailable_probe(*_args: object, **_kwargs: object) -> list[dict]:
        raise probe_error

    monkeypatch.setattr(
        "okg.substrate.deployment_lint.source_activation_probe_findings",
        unavailable_probe,
    )
    with pytest.raises(RuntimeError) as unavailable:
        ingest_service._validate_deferred_source_activation(
            plan,
            deployment_dir=tmp_path,
            registry_path=registry_path,
        )
    assert unavailable.value is probe_error

    event = SourceSyncEvent(
        deployment="demo",
        source_name="raw",
        trigger_kind="fs_event",
        scope_key="src/raw.py",
        reason="watch:modified",
        event_id="event-1",
    )
    source_events = {"raw": (event,)}
    inherited = ingest_service._source_events_with_inherited_path_scope(
        source_events,
        plan,
    )
    assert inherited is not source_events
    assert inherited["raw"] is source_events["raw"]
    assert set(inherited) == {"raw", "index"}
    inherited_event = inherited["index"][0]
    assert inherited_event.source_name == "index"
    assert inherited_event.scope_key == "src/raw.py"
    assert inherited_event.trigger_kind == "fs_event"
    assert inherited_event.event_id == "event-1:inherited:index"
    assert inherited_event.reason == "inherited:raw:watch:modified"

    plan.runnable["alias"] = plan.runnable["index"]
    removed_alias = plan.runnable.pop("alias")
    assert removed_alias is plan.runnable["index"]
    plan.deps["index"].add("side")
    assert "side" in plan.deps["index"]
    plan.deps["index"].remove("side")
    plan.layers[0].append("sentinel")
    removed_sentinel = plan.layers[0].pop()
    assert removed_sentinel == "sentinel"
    with pytest.raises(FrozenInstanceError):
        plan.ordered = ()

    cycle_registry = _load_registry(tmp_path, {
        "a": _source("demo.a", predecessor_source="b"),
        "b": _source("demo.b", predecessor_source="a"),
    })
    with pytest.raises(SubstrateError) as cycle:
        plan_ingest_sources(cycle_registry)
    assert cycle.value.code == "source_registry_cycle"
    assert str(cycle.value) == "cycle in predecessor_source declarations"
    assert cycle.value.data == {"deps": {"a": ["b"], "b": ["a"]}}


def test_plan_selected_source_includes_predecessor_closure(
    tmp_path: Path,
) -> None:
    registry = _load_registry(tmp_path, {
        "raw": _source("demo.raw"),
        "parsed": _source("demo.parsed", predecessor_source="demo.raw"),
        "index": _source("demo.index", predecessor_source="parsed"),
    })

    plan = plan_ingest_sources(registry, include={"index"})

    assert plan.ordered == ("raw", "parsed", "index")
    assert plan.layers == [["raw"], ["parsed"], ["index"]]


def test_successful_codex_predecessor_unblocks_coordination_packets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lawful codex result lets the named production dependency advance."""
    dep = _write_stub_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    registry_path.write_text(yaml.safe_dump({
        "sources": {
            "codex-sessions": {
                **_source("svc.codex-sessions"),
                "module": "unused",
                "class": "Unused",
                "source_policy": _fixture_source_privacy_policy(),
            },
            "coordination-packets": {
                **_source(
                    "svc.coordination-packets",
                    predecessor_source="svc.codex-sessions",
                ),
                "module": "unused",
                "class": "Unused",
                "source_policy": _fixture_source_privacy_policy(),
            },
        },
    }))
    called: list[str] = []

    def run_source(_options, _entry, name: str) -> dict[str, object]:
        called.append(name)
        return _source_result(name, nodes_appended=1)

    monkeypatch.setattr(ingest_service, "_run_source", run_source)

    outcome = run_ingest(IngestOptions(
        dsn="postgres://unused",
        deployment="svc",
        deployment_dir=dep,
        include={"coordination-packets"},
        publish=False,
    ))

    assert outcome.any_failed is False
    assert called == ["codex-sessions", "coordination-packets"]
    assert [item["status"] for item in outcome.report["results"]] == [
        "OK",
        "OK",
    ]
    assert outcome.report["completeness"][
        "dependency_blocked_sources"
    ] == []


def test_dependency_failure_partition_exhaustive() -> None:
    """Finite model: only declared unavailable predecessors block a source."""
    names = ("a", "b", "c", "d")
    possible_edges = tuple(
        (names[child], names[parent])
        for child in range(len(names))
        for parent in range(child)
    )
    partition = ingest_service._dependency_blocked_sources

    for edge_mask in range(1 << len(possible_edges)):
        deps = {name: set() for name in names}
        for edge_index, (child, parent) in enumerate(possible_edges):
            if edge_mask & (1 << edge_index):
                deps[child].add(parent)
        layers = topo_layers(deps)
        assert layers is not None
        for failed_values in itertools.product((False, True), repeat=len(names)):
            designated_failures = {
                name for name, failed in zip(names, failed_values, strict=True)
                if failed
            }
            unavailable: set[str] = set()
            ran: set[str] = set()
            for layer in layers:
                blocked = partition(layer, deps, unavailable)
                assert blocked == {
                    name: sorted(deps[name] & unavailable)
                    for name in layer
                    if deps[name] & unavailable
                }
                unavailable.update(blocked)
                for name in layer:
                    if name in blocked:
                        continue
                    if name in designated_failures:
                        unavailable.add(name)
                    else:
                        ran.add(name)

            for name in names:
                ancestors = _dependency_ancestors(name, deps)
                if name not in designated_failures and not (
                    ancestors & designated_failures
                ):
                    assert name in ran


def _dependency_ancestors(
    name: str,
    deps: dict[str, set[str]],
) -> set[str]:
    ancestors: set[str] = set()
    pending = list(deps[name])
    while pending:
        predecessor = pending.pop()
        if predecessor in ancestors:
            continue
        ancestors.add(predecessor)
        pending.extend(deps[predecessor] - ancestors)
    return ancestors


def test_plan_can_include_dependent_closure(tmp_path: Path) -> None:
    registry = _load_registry(tmp_path, {
        "raw": _source("demo.raw"),
        "parsed": _source("demo.parsed", predecessor_source="raw"),
        "index": _source("demo.index", predecessor_source="parsed"),
    })

    plan = plan_ingest_sources(
        registry,
        include={"demo.raw"},
        include_predecessors=False,
        include_dependents=True,
    )

    assert plan.ordered == ("raw", "parsed", "index")


def test_plan_can_include_direct_dependents_only(tmp_path: Path) -> None:
    registry = _load_registry(tmp_path, {
        "raw": _source("demo.raw"),
        "parsed": _source("demo.parsed", predecessor_source="raw"),
        "index": _source("demo.index", predecessor_source="parsed"),
    })

    plan = plan_ingest_sources(
        registry,
        include={"demo.raw"},
        include_predecessors=False,
        dependent_fanout="direct",
    )

    assert plan.ordered == ("raw", "parsed")


def test_direct_dependents_do_not_pull_unrelated_predecessor_closure(
    tmp_path: Path,
) -> None:
    registry = _load_registry(tmp_path, {
        "raw": _source("demo.raw"),
        "side": _source("demo.side"),
        "parsed": _source(
            "demo.parsed",
            predecessor_source=["raw", "side"],
        ),
        "index": _source("demo.index", predecessor_source="parsed"),
    })

    plan = plan_ingest_sources(
        registry,
        include={"demo.raw"},
        include_predecessors=True,
        dependent_fanout="direct",
    )

    assert plan.ordered == ("raw", "parsed")
    assert "side" not in plan.runnable
    assert "index" not in plan.runnable


def test_plan_exclude_wins_over_closure(tmp_path: Path) -> None:
    registry = _load_registry(tmp_path, {
        "raw": _source("demo.raw"),
        "parsed": _source("demo.parsed", predecessor_source="raw"),
        "index": _source("demo.index", predecessor_source="parsed"),
    })

    plan = plan_ingest_sources(registry, include={"index"}, exclude={"parsed"})

    assert "parsed" not in plan.runnable
    assert plan.ordered == ("index",)


def test_plan_ignores_registry_only_entries(tmp_path: Path) -> None:
    registry = _load_registry(tmp_path, {
        "raw": _source("demo.raw"),
        "documented-only": {
            "ownership_id": "demo.documented_only",
            "source_class": "registry_only",
        },
    })

    plan = plan_ingest_sources(registry)

    assert plan.ordered == ("raw",)


def test_plan_skips_disabled_sources_unless_included(tmp_path: Path) -> None:
    # Opt-out: bare `okg ingest` (include=None) drives every runnable
    # source EXCEPT those a registry EXPLICITLY sets enabled_by_default:
    # false (e.g. an unconfigured crawl like dblp). A source that leaves
    # the flag UNSET still runs, so a hand-authored registry that never
    # set it keeps its prior all-sources behavior. An explicitly-disabled
    # source runs only when named via --include.
    registry = _load_registry(tmp_path, {
        "kept": _source("demo.kept"),                                  # unset -> runs
        "opted_in": _source("demo.opted_in", enabled_by_default=True),  # true -> runs
        "crawl": _source("demo.crawl", enabled_by_default=False),      # false -> skipped
    })

    assert plan_ingest_sources(registry).ordered == ("kept", "opted_in")
    assert plan_ingest_sources(
        registry, include={"crawl"}
    ).ordered == ("crawl",)


def test_plan_pulls_opted_out_predecessor_via_closure(
    tmp_path: Path,
) -> None:
    # An explicitly-disabled source still runs on a bare ingest when it is
    # the predecessor dependency of a source that does run -- the
    # dependency closure overrides the opt-out.
    registry = _load_registry(tmp_path, {
        "base": _source("demo.base", enabled_by_default=False),
        "app": _source("demo.app", predecessor_source="base"),
    })

    assert plan_ingest_sources(registry).ordered == ("base", "app")


@pytest.mark.parametrize("via_dependency", [False, True])
def test_dormant_probe_contract_is_restored_when_selected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    via_dependency: bool,
) -> None:
    sources = {
        "dormant": _source(
            "demo.dormant",
            enabled_by_default=False,
        ),
    }
    if via_dependency:
        sources["active"] = _source(
            "demo.active",
            predecessor_source="dormant",
        )
        plan = plan_ingest_sources(_load_registry(tmp_path, sources))
    else:
        plan = plan_ingest_sources(
            _load_registry(tmp_path, sources),
            include={"dormant"},
        )
    monkeypatch.setattr(
        "okg.substrate.deployment_lint.source_activation_probe_findings",
        lambda *_args, **_kwargs: [{
            "code": "deployment.source_registry.probe_missing",
            "severity": "blocker",
        }],
    )

    with pytest.raises(SubstrateError) as exc:
        ingest_service._validate_deferred_source_activation(
            plan,
            deployment_dir=tmp_path,
            registry_path=tmp_path / "source_registry.yaml",
        )

    assert exc.value.code == "source_activation_probe_invalid"
    assert "dormant" in exc.value.data["sources"]


def test_plan_raises_on_cycle(tmp_path: Path) -> None:
    registry = _load_registry(tmp_path, {
        "a": _source("demo.a", predecessor_source="b"),
        "b": _source("demo.b", predecessor_source="a"),
    })

    with pytest.raises(SubstrateError) as exc:
        plan_ingest_sources(registry)

    assert exc.value.code == "source_registry_cycle"


def test_run_ingest_skips_no_work_layer_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = _write_stub_deployment(tmp_path)

    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda _options, _entry, name: _source_result(name),
    )
    monkeypatch.setattr(
        ingest_service,
        "_deployment_has_pending_publisher_work",
        lambda _options: False,
    )
    # #3342: a clean no-op still publishes when it clears a recorded source
    # failure; this database-free test has none to clear.
    monkeypatch.setattr(
        ingest_service.published_completeness,
        "run_changes_published_marker",
        lambda **_kwargs: False,
    )

    def fail_publish(*_args, **_kwargs):
        raise AssertionError("Publisher.cycle should not run")

    monkeypatch.setattr(ingest_service, "_publish", fail_publish)

    outcome = run_ingest(
        IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=dep,
        ),
    )

    assert outcome.any_failed is False
    assert outcome.report["publishes"] == [{
        "label": "commit",
        "status": "skipped",
        "publish_skipped": True,
        "reason": "no_source_work",
    }]


def test_run_ingest_force_enrichers_bypasses_no_work_layer_skip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P6: operator force pierces the ingest no_source_work gate."""
    dep = _write_stub_deployment(tmp_path)
    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda _options, _entry, name: _source_result(name),
    )
    monkeypatch.setattr(
        ingest_service,
        "_deployment_has_pending_publisher_work",
        lambda _options: False,
    )
    seen: dict[str, bool] = {}

    def fake_publish(options, *_args, **_kwargs):
        seen["force_enrichers"] = options.force_enrichers
        return {"label": "commit", "status": "published", "generation_id": "g"}

    monkeypatch.setattr(ingest_service, "_publish", fake_publish)

    outcome = run_ingest(
        IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=dep,
            force_enrichers=True,
        ),
    )

    assert outcome.any_failed is False
    assert seen == {"force_enrichers": True}
    assert outcome.report["publishes"][0]["status"] == "published"


def test_ingest_publish_forwards_force_enrichers_to_cycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P6: the ingest publish adapter reaches Publisher.cycle."""
    seen: dict[str, object] = {}
    catalog_identity = object()

    class _Publisher:
        def __init__(self, _dsn, _deployment, **kwargs):
            seen["catalog_identity"] = kwargs.get("catalog_identity")

        def set_owner(self, **_kwargs) -> None:
            return None

        def cycle(self, *, force_enrichers: bool) -> dict[str, object]:
            seen["force_enrichers"] = force_enrichers
            return {"status": "published", "generation_id": "g"}

    monkeypatch.setattr(
        "okg.substrate.publisher.publisher.Publisher",
        _Publisher,
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.deployment_stack."
        "build_deployment_publisher_stack",
        lambda **_kwargs: types.SimpleNamespace(
            deployment_name="svc",
            publisher_kwargs=lambda: {},
            deployment_dir=tmp_path,
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.deployment_stack."
        "build_runtime_catalog_identity",
        lambda **_kwargs: catalog_identity,
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.deployment_stack.publisher_kwargs_from_env",
        lambda: {},
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.repository_provenance.tracking_repository_provenance",
        lambda _path: RepositoryProvenance.exact(
            core_revision="1" * 40,
            deployment_revision="2" * 40,
            deployment_tree_sha="3" * 40,
        ),
    )
    monkeypatch.setattr(
        ingest_service,
        "_apply_publish_admission_feedback",
        lambda _options, _out: None,
    )

    out = ingest_service._publish(
        IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=tmp_path,
            force_enrichers=True,
        ),
        "commit",
    )

    assert seen == {
        "force_enrichers": True,
        "catalog_identity": catalog_identity,
    }
    assert out["status"] == "published"


@pytest.mark.postgres
def test_run_ingest_skips_no_work_layer_publish_with_stack_components(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = _write_stub_deployment(tmp_path)
    _checkpoint_chronos_current_branch(clean_runtime, deployment="svc")
    monkeypatch.setenv("OKG_PERSIST_PUBLISH_TIMINGS", "1")

    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda _options, _entry, name: _source_result(name),
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.deployment_stack."
        "build_deployment_publisher_stack",
        lambda **_kwargs: types.SimpleNamespace(
            materializers=[object()],
            consistency_gates=[object()],
            post_publish_consistency_checks=[object()],
            lenses=[object()],
            enrichers=[],
        ),
    )

    def fail_publish(*_args, **_kwargs):
        raise AssertionError("no-op source-sync layer must not publish")

    monkeypatch.setattr(ingest_service, "_publish", fail_publish)

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
        ),
    )

    assert outcome.any_failed is False
    assert outcome.report["publishes"] == [{
        "label": "commit",
        "status": "skipped",
        "publish_skipped": True,
        "reason": "no_source_work",
    }]
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT value
              FROM okg.source_progress
             WHERE deployment = 'svc'
               AND source_name = '__publisher__'
               AND key LIKE 'sync.publish_stage_timings.run.%%'
            """
        )
        payloads = [json.loads(row[0]) for row in cur.fetchall()]

    assert len(payloads) == 1
    assert payloads[0]["status"] == "skipped"
    assert payloads[0]["publish_skipped"] is True
    assert payloads[0]["reason"] == "no_source_work"


def test_run_ingest_publishes_no_work_layer_when_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = _write_stub_deployment(tmp_path)
    calls: list[str] = []
    hints: list[object] = []

    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda _options, _entry, name: _source_result(name),
    )
    monkeypatch.setattr(
        ingest_service,
        "_deployment_has_pending_publisher_work",
        lambda _options: True,
    )

    def publish(_options, label: str, hint=None, _lineage_hint=None, ingest_completeness=None) -> dict:
        calls.append(label)
        hints.append(hint)
        return {"label": label, "status": "published"}

    monkeypatch.setattr(ingest_service, "_publish", publish)

    outcome = run_ingest(
        IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=dep,
        ),
    )

    assert outcome.any_failed is False
    assert calls == ["commit"]
    assert hints == [({}, {}, True, {})]
    assert outcome.report["publishes"] == [{
        "label": "commit",
        "status": "published",
    }]


def test_run_ingest_staged_probe_failure_precedes_source_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown prior staged state fails closed before a source can append."""
    dep = _write_stub_deployment(tmp_path)
    monkeypatch.setattr(
        ingest_service,
        "_deployment_has_pending_publisher_work",
        lambda _options: True,
    )
    monkeypatch.setattr(
        ingest_service,
        "_staged_source_redrive_state",
        lambda _options: (_ for _ in ()).throw(RuntimeError("probe down")),
    )
    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda *_args, **_kwargs: pytest.fail("source must not run"),
    )

    with pytest.raises(SubstrateError, match="cannot determine whether staged"):
        run_ingest(IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=dep,
        ))


@pytest.mark.parametrize("redrive_status", ["fail", "blocked"])
def test_run_ingest_staged_redrive_failure_precedes_source_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    redrive_status: str,
) -> None:
    """Only a clean staged redrive may be followed by source execution."""
    dep = _write_stub_deployment(tmp_path)
    monkeypatch.setattr(
        ingest_service,
        "_staged_source_redrive_state",
        lambda _options: {
            "staged_work_present": True,
            "latest_checkpoint_id": "published:g1",
            "latest_checkpoint_status": "published",
            "latest_blocked_reason": None,
            "latest_published_checkpoint_id": "published:g1",
            "branch_mutation_token": "sha256:staged",
        },
    )
    monkeypatch.setattr(
        ingest_service,
        "_publish",
        lambda *_args, **_kwargs: {"status": redrive_status},
    )
    monkeypatch.setattr(
        ingest_service,
        "persist_publish_stage_timings",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda *_args, **_kwargs: pytest.fail("source must not run"),
    )

    with pytest.raises(SubstrateError, match="refusing to start new source"):
        run_ingest(IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=dep,
        ))


def test_publish_failure_dict_keeps_the_fix_hint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#1791: `_publish` must not drop the refusing error's repair hint.

    The staged-source redrive guard re-raises from this dict and has no
    other access to the hint, so a dropped `fix_hint` here is what left
    the operator with "resolve the staged-source publish blocker".
    """
    from okg.substrate.publisher import deployment_stack

    def refuse(**_kwargs):
        raise SubstrateError(
            "deployment manifest content differs from the claimed identity",
            data={"observed": {"drifted_blocks": ["nomos"]}},
            fix_hint=(
                "uv run okg catalog ownership claim "
                "--deployment deployments/svc --json"
            ),
        )

    monkeypatch.setattr(
        deployment_stack, "build_deployment_publisher_stack", refuse,
    )
    options = IngestOptions(
        dsn="postgres://unused",
        deployment="svc",
        deployment_dir=tmp_path,
    )

    out = ingest_service._publish(options, "staged-source-redrive")

    assert out["status"] == "fail"
    assert out["error_data"]["observed"]["drifted_blocks"] == ["nomos"]
    assert "okg catalog ownership claim" in out["fix_hint"]


def test_publish_failure_dict_omits_an_absent_fix_hint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hintless refusal adds no key, so the guard keeps its own text."""
    from okg.substrate.publisher import deployment_stack

    def refuse(**_kwargs):
        raise SubstrateError("no hint available", data={})

    monkeypatch.setattr(
        deployment_stack, "build_deployment_publisher_stack", refuse,
    )

    out = ingest_service._publish(
        IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=tmp_path,
        ),
        "staged-source-redrive",
    )

    assert out["status"] == "fail"
    assert "fix_hint" not in out


def test_staged_redrive_guard_forwards_publish_cause_and_hint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#1791: the guard hands over the publish's cause and repair command."""
    dep = _write_stub_deployment(tmp_path)
    monkeypatch.setattr(
        ingest_service,
        "_staged_source_redrive_state",
        lambda _options: {
            "staged_work_present": True,
            "latest_checkpoint_id": "published:g1",
            "latest_checkpoint_status": "published",
            "latest_blocked_reason": None,
            "latest_published_checkpoint_id": "published:g1",
            "branch_mutation_token": "sha256:staged",
        },
    )
    monkeypatch.setattr(
        ingest_service,
        "_publish",
        lambda *_args, **_kwargs: {
            "label": "staged-source-redrive",
            "status": "fail",
            "code": "catalog_manifest_drifted",
            "error": "deployment manifest content differs ... "
                     "drifted_blocks=['nomos']",
            "error_data": {"observed": {"drifted_blocks": ["nomos"]}},
            "fix_hint": (
                "uv run okg catalog ownership claim "
                "--deployment deployments/svc --json"
            ),
        },
    )
    monkeypatch.setattr(
        ingest_service,
        "persist_publish_stage_timings",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda *_args, **_kwargs: pytest.fail("source must not run"),
    )

    with pytest.raises(SubstrateError) as guard:
        run_ingest(IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=dep,
        ))

    # The message operators and tests grep for is unchanged.
    assert "staged source work was not published; refusing to start " \
        "new source writes" in str(guard.value)
    data = guard.value.data
    assert data["publish_code"] == "catalog_manifest_drifted"
    assert data["publish_error_data"]["observed"]["drifted_blocks"] == [
        "nomos",
    ]
    assert "drifted_blocks" in data["publish_error"]
    assert "okg catalog ownership claim" in (guard.value.fix_hint or "")
    # #2213: the refusal must also name the command that DRAINS the staged
    # work. Forwarding only the publish's own hint sent the operator round a
    # claim -> retry -> claim loop that never published the staged rows.
    assert data["drain_command"] == "uv run okg run --once --apply --deployment svc"
    assert data["drain_command"] in (guard.value.fix_hint or "")
    json.dumps({"data": data, "fix_hint": guard.value.fix_hint})


def test_staged_redrive_guard_forwards_a_blocked_publish_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A BLOCKED redrive carries no error or hint; its diagnosis is the
    structured ``blocked_reason`` (failed gates, incomplete sources), and the
    guard forwards it as ``publish_blocked_reason`` (#1791 review)."""
    dep = _write_stub_deployment(tmp_path)
    monkeypatch.setattr(
        ingest_service,
        "_staged_source_redrive_state",
        lambda _options: {
            "staged_work_present": True,
            "latest_checkpoint_id": "published:g1",
            "latest_checkpoint_status": "published",
            "latest_blocked_reason": None,
            "latest_published_checkpoint_id": "published:g1",
            "branch_mutation_token": "sha256:staged",
        },
    )
    monkeypatch.setattr(
        ingest_service,
        "_publish",
        lambda *_args, **_kwargs: {
            "label": "staged-source-redrive",
            "status": "blocked",
            "blocked_reason": {
                "reason": "ingest_source_incomplete",
                "incomplete_sources": ["second"],
            },
        },
    )
    monkeypatch.setattr(
        ingest_service,
        "persist_publish_stage_timings",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda *_args, **_kwargs: pytest.fail("source must not run"),
    )

    with pytest.raises(SubstrateError) as guard:
        run_ingest(IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=dep,
        ))

    data = guard.value.data
    assert data["publish_status"] == "blocked"
    assert data["publish_blocked_reason"] == {
        "reason": "ingest_source_incomplete",
        "incomplete_sources": ["second"],
    }
    assert "publish_error" not in data
    # #2213: a hintless refusal still names the drain command.
    assert guard.value.fix_hint == (
        "publish the already-staged work with: uv run okg run --once "
        "--apply --deployment svc -- then re-run this command"
    )
    json.dumps({"data": data, "fix_hint": guard.value.fix_hint})


def test_staged_redrive_guard_keeps_generic_hint_without_a_publish_hint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hintless refusal (e.g. a blocked publish) keeps the old text."""
    dep = _write_stub_deployment(tmp_path)
    monkeypatch.setattr(
        ingest_service,
        "_staged_source_redrive_state",
        lambda _options: {
            "staged_work_present": True,
            "latest_checkpoint_id": "published:g1",
            "latest_checkpoint_status": "published",
            "latest_blocked_reason": None,
            "latest_published_checkpoint_id": "published:g1",
            "branch_mutation_token": "sha256:staged",
        },
    )
    monkeypatch.setattr(
        ingest_service,
        "_publish",
        lambda *_args, **_kwargs: {"status": "blocked"},
    )
    monkeypatch.setattr(
        ingest_service,
        "persist_publish_stage_timings",
        lambda **_kwargs: None,
    )

    with pytest.raises(SubstrateError) as guard:
        run_ingest(IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=dep,
        ))

    # #2213: a hintless refusal still names the drain command.
    assert guard.value.fix_hint == (
        "publish the already-staged work with: uv run okg run --once "
        "--apply --deployment svc -- then re-run this command"
    )
    assert "publish_error" not in guard.value.data


def test_run_ingest_publishes_layer_with_source_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = _write_stub_deployment(tmp_path)
    calls: list[str] = []

    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda _options, _entry, name: _source_result(
            name,
            nodes_appended=1,
        ),
    )
    monkeypatch.setattr(
        ingest_service,
        "_deployment_has_pending_publisher_work",
        lambda _options: False,
    )

    def publish(_options, label: str, _hint=None, _lineage_hint=None, ingest_completeness=None) -> dict:
        calls.append(label)
        return {"label": label, "status": "published"}

    monkeypatch.setattr(ingest_service, "_publish", publish)

    outcome = run_ingest(
        IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=dep,
        ),
    )

    assert outcome.any_failed is False
    assert calls == ["commit"]


def test_run_ingest_passes_layer_graph_write_set_hint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = _write_stub_deployment(tmp_path)
    hints: list[object] = []

    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda _options, _entry, name: _source_result(
            name,
            nodes_appended=1,
            graph_write_set={
                "schema": "okg.graph_write_set.v1",
                "nodes_by_subtype": {"document": ["n:1"]},
                "node_attr_changes_by_subtype": {
                    "document": {"n:1": ["__new__", "text"]},
                },
                "edges_by_type": {"mentions": [42]},
                "edge_table_available": True,
            },
        ),
    )
    monkeypatch.setattr(
        ingest_service,
        "_deployment_has_pending_publisher_work",
        lambda _options: False,
    )

    def publish(_options, label: str, hint=None, _lineage_hint=None, ingest_completeness=None) -> dict:
        hints.append(hint)
        return {"label": label, "status": "published"}

    monkeypatch.setattr(ingest_service, "_publish", publish)

    outcome = run_ingest(
        IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=dep,
        ),
    )

    assert outcome.any_failed is False
    assert hints == [(
        {"document": ["n:1"]},
        {"mentions": [42]},
        True,
        {"document": {"n:1": ["__new__", "text"]}},
    )]


def test_run_ingest_passes_source_delta_hint_to_enrichers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = _write_stub_deployment(tmp_path)
    lineage_hints: list[object] = []

    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda _options, _entry, name: _source_result(
            name,
            nodes_appended=1,
            graph_write_set={
                "schema": "okg.graph_write_set.v1",
                "nodes_by_subtype": {"document": ["n:1"]},
                "edges_by_type": {},
                "edge_table_available": True,
            },
            source_delta_batch={
                "source_id": "svc.first",
                "contract": "valid",
                "changed_record_ids": ['{"path":"docs/a.md"}'],
            },
        ),
    )
    monkeypatch.setattr(
        ingest_service,
        "_deployment_has_pending_publisher_work",
        lambda _options: False,
    )

    def publish(_options, label: str, _hint=None, lineage_hint=None, ingest_completeness=None) -> dict:
        lineage_hints.append(lineage_hint)
        return {"label": label, "status": "published"}

    monkeypatch.setattr(ingest_service, "_publish", publish)

    outcome = run_ingest(
        IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=dep,
        ),
    )

    assert outcome.any_failed is False
    assert len(lineage_hints) == 1
    hint = lineage_hints[0]
    assert hint is not None
    assert [item.to_dict() for item in hint] == [{
        "dependency_kind": "source_record",
        "dependency_ref": 'svc.first:{"path":"docs/a.md"}',
        "node_subtype": None,
        "edge_type": None,
        "source_id": "svc.first",
        "dirty_reason": "source_record_changed",
    }]


def test_run_ingest_uses_chronos_lineage_when_source_delta_batch_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = _write_stub_deployment(tmp_path)
    lineage_hints: list[object] = []

    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda _options, _entry, name: _source_result(
            name,
            nodes_appended=1,
            graph_write_set={
                "schema": "okg.graph_write_set.v1",
                "nodes_by_subtype": {"document": ["n:1"]},
                "edges_by_type": {},
                "edge_table_available": True,
            },
        ),
    )
    monkeypatch.setattr(
        ingest_service,
        "_deployment_has_pending_publisher_work",
        lambda _options: False,
    )

    def publish(_options, label: str, _hint=None, lineage_hint=None, ingest_completeness=None) -> dict:
        lineage_hints.append(lineage_hint)
        return {"label": label, "status": "published"}

    monkeypatch.setattr(ingest_service, "_publish", publish)

    outcome = run_ingest(
        IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=dep,
        ),
    )

    assert outcome.any_failed is False
    assert lineage_hints == [ingest_service._ENRICHER_LINEAGE_FROM_CHRONOS]


def test_run_ingest_partial_failure_keeps_lineage_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = _write_layered_fixture_deployment(tmp_path)
    lineage_hints: list[object] = []
    publish_blockers: list[object] = []
    markers: list[object] = []

    def run_source(_options, _entry, name: str) -> dict[str, object]:
        if name == "second":
            # Its writes were undone, so the run publishes (#3342).
            return {
                "name": name, "status": "FAIL", "error": "boom",
                "undo_status": "completed",
            }
        return _source_result(
            name,
            nodes_appended=1,
            graph_write_set={
                "schema": "okg.graph_write_set.v1",
                "nodes_by_subtype": {"document": ["n:1"]},
                "edges_by_type": {},
                "edge_table_available": True,
            },
        )

    monkeypatch.setattr(ingest_service, "_run_source", run_source)
    monkeypatch.setattr(
        ingest_service,
        "_deployment_has_pending_publisher_work",
        lambda _options: False,
    )

    def publish(
        _options,
        label: str,
        _hint=None,
        lineage_hint=None,
        publish_blocker=None,
        **_kwargs,
    ) -> dict:
        lineage_hints.append(lineage_hint)
        publish_blockers.append(publish_blocker)
        markers.append(_kwargs.get("ingest_completeness"))
        return {"label": label, "status": "published"}

    monkeypatch.setattr(ingest_service, "_publish", publish)

    outcome = run_ingest(
        IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=dep,
        ),
    )

    assert outcome.any_failed is True
    assert lineage_hints == [None]
    # #3342: no run blocker; the generation's marker names the failure.
    assert publish_blockers == [None]
    assert [item["source"] for item in markers[0]["failed_sources"]] == [
        "second",
    ]


@pytest.mark.postgres
def test_pending_publisher_work_detects_fact_lag(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    dep = _write_stub_deployment(tmp_path)
    _insert_published_generation(clean_runtime, deployment="svc")
    _checkpoint_chronos_current_branch(clean_runtime, deployment="svc")
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES
              ('svc:lag', 'document', '{}'::jsonb, 'svc.first',
               '{}'::jsonb, '{}'::jsonb, now(), 'svc:lag', %s, 'I')
            """,
            (uuid.uuid4(),),
        )

    assert ingest_service._deployment_has_pending_publisher_work(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
        ),
    ) is True


@pytest.mark.postgres
def test_pending_publisher_work_detects_missing_enricher_progress(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = _write_stub_deployment(
        tmp_path,
        enrichers=[{
            "class": (
                "okg.substrate.library.linkers.acceptance_criteria."
                "AcceptanceCriteriaLinker"
            ),
            }],
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.deployment_stack."
        "build_deployment_publisher_stack",
        lambda **_kwargs: types.SimpleNamespace(
            materializers=[],
            consistency_gates=[],
            post_publish_consistency_checks=[],
            lenses=[],
            enrichers=[types.SimpleNamespace(name="acceptance_criteria")],
        ),
    )
    _insert_published_generation(clean_runtime, deployment="svc")
    _checkpoint_chronos_current_branch(clean_runtime, deployment="svc")
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "DELETE FROM okg.enricher_progress WHERE deployment_name = 'svc'"
        )

    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
    )
    assert ingest_service._deployment_has_pending_publisher_work(options) is True

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.enricher_progress
              (deployment_name, enricher_name, fact_watermark, edge_watermark,
               generation_id, last_status, last_run_at)
            SELECT 'svc', 'acceptance_criteria',
                   COALESCE(max(nf.fact_id), 0),
                   COALESCE((SELECT max(ef.fact_id) FROM okg.edge_facts ef), 0),
                   (SELECT max(generation_id)
                      FROM okg.graph_generations
                     WHERE deployment_name = 'svc'),
                   'success', now()
              FROM okg.node_facts nf
            """
        )

    assert ingest_service._deployment_has_pending_publisher_work(options) is False


@pytest.mark.postgres
def test_pending_publisher_work_ignores_clean_stack_components(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = _write_stub_deployment(tmp_path)
    _checkpoint_chronos_current_branch(clean_runtime, deployment="svc")
    monkeypatch.setattr(
        "okg.substrate.publisher.deployment_stack."
        "build_deployment_publisher_stack",
        lambda **_kwargs: types.SimpleNamespace(
            materializers=[object()],
            consistency_gates=[object()],
            post_publish_consistency_checks=[object()],
            lenses=[object()],
            enrichers=[],
        ),
    )

    assert ingest_service._deployment_has_pending_publisher_work(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
        ),
    ) is False


@pytest.mark.postgres
def test_pending_publisher_work_fails_closed_when_quiescence_unavailable(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = _write_stub_deployment(tmp_path)
    _insert_published_generation(clean_runtime, deployment="svc")
    monkeypatch.setattr(
        "okg.substrate.publisher.deployment_stack."
        "build_deployment_publisher_stack",
        lambda **_kwargs: types.SimpleNamespace(
            materializers=[object()],
            consistency_gates=[],
            post_publish_consistency_checks=[],
            lenses=[],
            enrichers=[],
        ),
    )
    monkeypatch.setattr(
        ingest_service,
        "_chronos_publish_inputs_clean",
        lambda *_args, **_kwargs: False,
    )

    assert ingest_service._deployment_has_pending_publisher_work(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
        ),
    ) is True


@pytest.mark.postgres
def test_pending_publisher_work_detects_chronos_branch_lag(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = _write_stub_deployment(tmp_path)
    _checkpoint_chronos_current_branch(clean_runtime, deployment="svc")
    monkeypatch.setattr(
        "okg.substrate.publisher.deployment_stack."
        "build_deployment_publisher_stack",
        lambda **_kwargs: types.SimpleNamespace(
            materializers=[object()],
            consistency_gates=[],
            post_publish_consistency_checks=[],
            lenses=[],
            enrichers=[],
        ),
    )

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts([
            NodeFact("svc:after-checkpoint", "document", {}, {}, {}),
        ])

    assert ingest_service._deployment_has_pending_publisher_work(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
        ),
    ) is True


@pytest.mark.postgres
def test_pending_publisher_work_detects_chronos_non_source_admission(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = _write_stub_deployment(tmp_path)
    _checkpoint_chronos_current_branch(clean_runtime, deployment="svc")
    monkeypatch.setattr(
        "okg.substrate.publisher.deployment_stack."
        "build_deployment_publisher_stack",
        lambda **_kwargs: types.SimpleNamespace(
            materializers=[object()],
            consistency_gates=[],
            post_publish_consistency_checks=[],
            lenses=[],
            enrichers=[],
        ),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.admission_envelopes
              (deployment_name, producer_id, producer_kind, intent,
               admission_mode, status, idempotency_key)
            VALUES
              ('svc', '_lens:test', 'lens', 'promote_lens_overlay',
               'fast_track', 'accepted', %s)
            """,
            (f"lens:{uuid.uuid4().hex}",),
        )

    assert ingest_service._deployment_has_pending_publisher_work(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
        ),
    ) is True


@pytest.mark.postgres
def test_run_ingest_publishes_once_for_all_layers(
    tmp_path: Path,
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    record_property,
) -> None:
    """A two-layer ingest publishes ONCE for the whole commit: a single
    end-of-commit generation lands published and BOTH layers' admission
    envelopes flip to published under it (admission-feedback is
    watermark-coverage based, so one publish covers every layer's facts).

    The legacy ``okg.lease_audit`` owner-lock audit (``acquired`` rows
    carrying ``holder_id`` / ``holder_meta``) was retired alongside the
    ``okg.publisher_leases`` / ``okg.jobs`` runtime — the publisher now
    only retains ``owner_id`` / ``owner_meta`` for advisory-lock error
    payloads and writes no lease-audit rows. So this no longer asserts
    owner recording; it covers the surviving publish behavior.
    """
    # The fixture sources emit `document` nodes; register that subtype
    # in the active catalog so the node-subtype-conformance publish
    # gate admits them. The ingest path reads the applied DB catalog
    # (not the deployment dir), and `clean_runtime` ships an empty
    # subtype set, so without this seed the gate blocks the publish.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(clean_runtime, dep)

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            owner_id="sync-test-owner",
            owner_meta={"ticket": "SYNC-1"},
        ),
    )

    assert outcome.any_failed is False, json.dumps(outcome.report, default=str)
    # One publish for the whole commit (both layers), not one per layer.
    assert [p["status"] for p in outcome.report["publishes"]] == [
        "published"
    ], outcome.report
    assert [p["label"] for p in outcome.report["publishes"]] == ["commit"]
    publish = outcome.report["publishes"][0]
    assert publish["nodes_added"] == 2
    assert publish["edges_added"] == 0
    # The single publish's admission feedback flips BOTH layers' envelopes.
    assert [
        p["admission_feedback"]["updated"]
        for p in outcome.report["publishes"]
    ] == [2]

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        generations = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).list_generations(deployment_name="svc")
        generation = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).checkout_generation(publish["generation_id"])
        live_nodes = int(generation.query(
            "SELECT count(*) AS n FROM okg.graph_nodes"
        )[0]["n"])
        live_edges = int(generation.query(
            "SELECT count(*) AS n FROM okg.graph_edges"
        )[0]["n"])
        cur.execute(
            """
            SELECT status, count(*), count(target_generation_ref)
              FROM okg.admission_envelopes
             WHERE deployment_name = 'svc'
             GROUP BY status
             ORDER BY status
            """,
        )
        admission_statuses = cur.fetchall()
        cur.execute(
            """
            SELECT count(*)
              FROM okg.admission_events
              JOIN okg.admission_envelopes USING (admission_id)
             WHERE event_type = 'published'
               AND deployment_name = 'svc'
            """,
        )
        admission_published_events = int(cur.fetchone()[0])

    # One generation for the whole commit (was one per layer), with both
    # layers' admission envelopes published under that single generation.
    assert len(generations) >= 1
    assert generations[-1].checkpoint_id == publish["generation_id"]
    assert generations[-1].metadata["status"] == "published"
    assert (live_nodes, live_edges) == (2, 0)
    assert admission_statuses == [("published", 2, 2)]
    assert admission_published_events == 2

    from okg.substrate.publish_proof import dlq_count

    current_dlq, queue_depth = dlq_count(clean_runtime)
    assert current_dlq == 0
    assert queue_depth["queued"] == 0
    assert queue_depth["leased"] == 0
    assert queue_depth["dead_letter"] == 0

    rc = _cmd_generation(
        argparse.Namespace(
            command="generation",
            subcmd="describe",
            generation_id=publish["generation_id"],
            dsn=clean_runtime,
            deployment=None,
            json=True,
        ),
        as_json=True,
    )
    generation_description = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert generation_description["status"] == "published"

    from okg.substrate.cli.main import main

    rc = main(["--json", "doctor", "--dsn", clean_runtime])
    doctor = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert rc == 0, doctor
    assert doctor["ok"] is True
    record_property(
        "requirements",
        "req.ingest-planning.publish-parity "
        "graph.ingest-planning.publish-parity",
    )
    record_property(
        "generation_id",
        generation_description["target_generation_id"],
    )
    record_property("chronos_generation_id", publish["generation_id"])
    record_property(
        "graph_report",
        json.dumps({
            "status": generation_description["status"],
            "target_generation_id": generation_description["target_generation_id"],
            "chronos_generation_id": publish["generation_id"],
            "nodes_delta": publish["nodes_added"],
            "edges_delta": publish["edges_added"],
            "live_nodes": live_nodes,
            "live_edges": live_edges,
            "queue_depth": {
                "queued": queue_depth["queued"],
                "leased": queue_depth["leased"],
            },
            "dlq_count": current_dlq,
            "doctor": "clean",
        }, sort_keys=True),
    )


@pytest.mark.postgres
def test_run_ingest_publishes_committed_writes_when_later_source_fails(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """Regression (PR #448 deep review, orphaned-writes-on-layer-failure P1):
    when a LATER layer's source fails mid-commit, the earlier layer's
    already-committed writes must still be checkpointed and gated this run.
    Since #3342 the candidate is published, and its `ingest_completeness`
    marker names the failed source (it was blocked under #987).

    The collapse defers per-layer publishing to one end-of-commit publish; the
    original `... and not any_failed` guard skipped that publish entirely on any
    failure, stranding the successful layer's writes -- a later generation
    captured them in its checkpoint but never invalidated the views/gates that
    read them (the next run's dirty-scope hint omits the now-watermarked sources
    and REPLACES, not supplements, the Chronos scan). The fix checkpoints the
    committed writes with a forced full scan and an explicit run blocker.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(clean_runtime, dep)
    # Make the SECOND (dependent) layer fail; the FIRST layer still commits.
    registry_path = dep / "source_registry.yaml"
    data = yaml.safe_load(registry_path.read_text())
    data["sources"]["second"]["params"]["fixture_path"] = str(
        dep / "missing-second.json",
    )
    registry_path.write_text(yaml.safe_dump(data))
    _claim_layered_fixture_catalog(clean_runtime, dep)

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
        ),
    )

    # The run is FAILED (the second layer's source failed) ...
    assert outcome.any_failed is True
    assert sum(
        1 for r in outcome.report["results"] if r["status"] == "FAIL"
    ) == 1
    # The first layer's committed writes still drive one end-of-commit
    # checkpoint; it is published and marked incomplete (#3342).
    assert [p["status"] for p in outcome.report["publishes"]] == [
        "published"
    ], outcome.report
    assert [p["label"] for p in outcome.report["publishes"]] == ["commit"]
    publish = outcome.report["publishes"][0]
    assert not publish.get("blocked_reason")
    assert publish["nodes_added"] == 1
    assert outcome.report["completeness"]["failed_sources"] == [{
        "source": "second",
        "stage": "run",
    }]

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        generations = store.list_generations(
            deployment_name="svc",
            status=None,
        )
        latest_published = store.latest_generation(
            deployment_name="svc",
            status="published",
        )
        cur.execute(
            """
            SELECT status, count(*)
              FROM okg.admission_envelopes
             WHERE deployment_name = 'svc'
             GROUP BY status
             ORDER BY status
            """
        )
        envelope_statuses = cur.fetchall()

    assert len(generations) >= 1
    assert not [
        generation for generation in generations
        if generation.metadata.get("status") == "blocked"
    ]
    # The successful source's admission is published; the failed source's
    # stays failed, and the generation names it.
    assert envelope_statuses == [("failed", 1), ("published", 1)]
    assert latest_published is not None
    assert latest_published.checkpoint_id == publish["generation_id"]
    marker = latest_published.metadata["ingest_completeness"]
    assert marker["status"] == "incomplete"
    assert marker["failed_sources"] == [{
        "source": "second", "ownership_id": "svc.second", "stage": "run",
    }]


@pytest.mark.postgres
def test_run_ingest_continues_independent_later_source_and_marks_generation(
    tmp_path: Path,
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A failed layer-1 source blocks its dependent, not another lane.

    Since #3342 the generation is published and marked incomplete.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    peer_fixture = dep / "peer.json"
    independent_fixture = dep / "independent.json"
    peer_fixture.write_text(json.dumps({
        "nodes": [{
            "node_id": "svc:peer",
            "subtype": "document",
            "attrs": {"text": "successful layer-1 peer"},
            "source_record_id": {"path": "peer.json"},
        }],
        "edges": [],
    }))
    independent_fixture.write_text(json.dumps({
        "nodes": [{
            "node_id": "svc:independent",
            "subtype": "document",
            "attrs": {"text": "independent layer-2 output"},
            "source_record_id": {"path": "independent.json"},
        }],
        "edges": [],
    }))
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["sources"]["first"]["params"]["fixture_path"] = str(
        dep / "missing-first.json",
    )
    fixture_source = {
        "module": "okg.substrate.library.sources.bootstrap_fixture",
        "class": "BootstrapFixtureSource",
        "source_policy": _fixture_source_privacy_policy(),
        "sync": {
            "triggers": ["manual", "reconcile"],
            "default_event_mode": "scope_complete",
            "reconcile_mode": "scope_complete",
        },
    }
    registry["sources"]["peer"] = {
        **_source("svc.peer"),
        **fixture_source,
        "params": {"fixture_path": str(peer_fixture)},
    }
    registry["sources"]["independent"] = {
        **_source("svc.independent", predecessor_source="peer"),
        **fixture_source,
        "params": {"fixture_path": str(independent_fixture)},
    }
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(clean_runtime, dep)

    outcome = run_ingest(IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
    ))

    assert outcome.any_failed is True
    assert [result["name"] for result in outcome.report["results"]] == [
        "first",
        "peer",
        "independent",
    ]
    assert [result["status"] for result in outcome.report["results"]] == [
        "FAIL",
        "OK",
        "OK",
    ]
    assert outcome.report["skipped"] == [{
        "reason": "upstream_source_failed",
        "after_layer": 1,
        "sources": ["second"],
        "blocked_by": {"second": ["first"]},
    }]
    assert outcome.report["completeness"]["dependency_blocked_sources"] == [{
        "source": "second",
        "blocked_by": ["first"],
    }]
    (publish,) = outcome.report["publishes"]
    assert publish["status"] == "published"
    assert publish["nodes_added"] == 2
    assert publish["node_count"] == 2
    assert not publish.get("blocked_reason")
    generation_id = publish["generation_id"]

    rc = _cmd_generation(
        argparse.Namespace(
            command="generation",
            subcmd="describe",
            generation_id=generation_id,
            dsn=clean_runtime,
            deployment="svc",
            json=True,
        ),
        as_json=True,
    )
    assert rc == 0
    described = json.loads(capsys.readouterr().out)
    assert described["status"] == "published"
    assert described["nodes_live_at_gen"] == 2
    # #3342: the incomplete version names what failed and what it held back.
    described_marker = described["ingest_completeness"]
    assert described_marker["status"] == "incomplete"
    assert described_marker["reason"] == "ingest_source_incomplete"
    assert [item["source"] for item in described_marker["failed_sources"]] == [
        "first",
    ]
    assert described_marker["dependency_blocked_sources"] == [{
        "source": "second", "ownership_id": "svc.second",
        "blocked_by": ["first"],
    }]

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        candidate = store.checkout_generation(generation_id)
        candidate_nodes = int(candidate.query(
            "SELECT count(*) AS n FROM okg.graph_nodes",
        )[0]["n"])
        latest_published = store.latest_generation(
            deployment_name="svc",
            status="published",
        )
        cur.execute(
            "SELECT status, count(*) FROM okg.admission_envelopes "
            "WHERE deployment_name = 'svc' GROUP BY status ORDER BY status"
        )
        admission_statuses = cur.fetchall()

    assert candidate_nodes == 2
    assert latest_published is not None
    assert latest_published.checkpoint_id == generation_id
    marker = latest_published.metadata["ingest_completeness"]
    assert marker["status"] == "incomplete"
    assert [item["source"] for item in marker["failed_sources"]] == ["first"]
    assert marker["dependency_blocked_sources"] == [{
        "source": "second", "ownership_id": "svc.second",
        "blocked_by": ["first"],
    }]
    assert admission_statuses == [("failed", 1), ("published", 2)]


@pytest.mark.postgres
def test_run_ingest_zero_delta_failure_publishes_incomplete_generation(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """An incomplete no-op run must not reuse its prior published generation.

    The prior generation says every source is current; a new one records
    the failure (#3342; it was a blocked checkpoint under #987).
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
    )
    baseline = run_ingest(options)
    (baseline_publish,) = baseline.report["publishes"]
    assert baseline_publish["status"] == "published"

    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["sources"]["second"]["params"]["fixture_path"] = str(
        dep / "missing-second.json",
    )
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(clean_runtime, dep)

    incomplete = run_ingest(replace(options, reset_cursor=True))
    (marked_publish,) = incomplete.report["publishes"]

    assert incomplete.any_failed is True
    assert marked_publish["status"] == "published"
    assert marked_publish["generation_id"] != baseline_publish["generation_id"]
    assert marked_publish["nodes_added"] == 0
    assert marked_publish["nodes_updated"] == 0

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        latest_published = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).latest_generation(
            deployment_name="svc",
            status="published",
        )

    assert latest_published is not None
    assert latest_published.checkpoint_id == marked_publish["generation_id"]
    assert latest_published.metadata["ingest_completeness"][
        "failed_sources"
    ][0]["source"] == "second"


@pytest.mark.postgres
def test_run_ingest_blocked_publish_marks_admission_blocked(
    tmp_path: Path,
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    dep = _write_layered_fixture_deployment(tmp_path)
    deployment_path = dep / "deployment.yaml"
    deployment = yaml.safe_load(deployment_path.read_text())
    deployment["consistency_gates"] = [_blocking_consistency_gate()]
    deployment_path.write_text(yaml.safe_dump(deployment))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    _claim_layered_fixture_catalog(clean_runtime, dep)

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            include_dependents=False,
            admission_observed_at=datetime(
                2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc,
            ),
        ),
    )

    assert outcome.any_failed is True
    result = outcome.report["results"][0]
    publish = outcome.report["publishes"][0]
    assert result["admission_id"]
    assert publish["status"] == "blocked"
    assert publish["admission_feedback"]["updated"] == 1
    generation_id = publish["generation_id"]

    rc = _cmd_generation(
        argparse.Namespace(
            command="generation",
            subcmd="describe",
            generation_id=generation_id,
            dsn=clean_runtime,
            deployment=None,
            json=True,
        ),
        as_json=True,
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert rc == 0
    assert payload["status"] == "blocked"
    assert payload["consistency"]["blocked_gate_count"] == 1
    assert payload["consistency"]["gates"][0]["gate_name"] == (
        "fixture_blocking_gate"
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, target_generation_ref, verdict
              FROM okg.admission_envelopes
             WHERE admission_id = %s::uuid
            """,
            (result["admission_id"],),
        )
        status, target_generation_ref, verdict = cur.fetchone()
        cur.execute(
            """
            SELECT count(*)
              FROM okg.admission_events
             WHERE admission_id = %s::uuid
               AND event_type = 'blocked'
            """,
            (result["admission_id"],),
        )
        blocked_events = int(cur.fetchone()[0])

    assert status == "blocked"
    assert target_generation_ref == generation_id
    assert verdict["blocked_reason"]["reason"] == (
        "consistency_publish_gate_blocked"
    )
    assert blocked_events == 1


@pytest.mark.postgres
def test_run_ingest_uses_safe_run_mode_for_unscoped_dependents(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    dep = _write_layered_fixture_deployment(tmp_path)

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            dependent_fanout="direct",
            source_events={
                "first": (
                    SourceSyncEvent(
                        deployment="svc",
                        source_name="first",
                        trigger_kind="fs_event",
                        scope_key="first.json",
                    ),
                ),
            },
            run_mode="scope_incomplete",
            publish=False,
        ),
    )

    assert outcome.any_failed is False, outcome.report
    by_name = {
        result["name"]: result["run_mode"]
        for result in outcome.report["results"]
    }
    assert by_name == {
        "first": "scope_incomplete",
        "second": "scope_complete",
    }


@pytest.mark.postgres
def test_run_ingest_inherits_path_scope_for_scoped_dependents(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    dep = _write_layered_fixture_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    registry_doc = yaml.safe_load(registry_path.read_text())
    registry_doc["sources"]["second"]["sync"]["scope"] = {
        "accepts": ["path"],
        "unsupported_fallback": "scope_complete",
    }
    registry_path.write_text(yaml.safe_dump(registry_doc))

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            dependent_fanout="direct",
            source_events={
                "first": (
                    SourceSyncEvent(
                        deployment="svc",
                        source_name="first",
                        trigger_kind="fs_event",
                        scope_key="first.json",
                        reason="watch:modified",
                    ),
                ),
            },
            run_mode="scope_incomplete",
            publish=False,
        ),
    )

    assert outcome.any_failed is False
    by_name = {
        result["name"]: result
        for result in outcome.report["results"]
    }
    assert by_name["first"]["run_mode"] == "scope_incomplete"
    assert by_name["first"]["sync_scope"]["scope_keys"] == ["first.json"]
    assert by_name["second"]["run_mode"] == "scope_complete"
    assert by_name["second"]["sync_scope"]["scope_keys"] == ["first.json"]
    assert by_name["second"]["sync_scope"]["trigger_kinds"] == ["fs_event"]


@pytest.mark.postgres
def test_run_ingest_creates_fast_track_admission(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    dep = _write_layered_fixture_deployment(tmp_path)

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            publish=False,
            admission_observed_at=datetime(
                2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc,
            ),
        ),
    )

    result = outcome.report["results"][0]
    assert result["status"] == "OK", outcome.report
    assert result["admission_id"]
    assert result["admission_ref_count"] == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT admission_mode, status, source, run_id::text,
                   authority, check_summary
              FROM okg.admission_envelopes
             WHERE admission_id = %s::uuid
            """,
            (result["admission_id"],),
        )
        envelope = cur.fetchone()
        cur.execute(
            """
            SELECT ref_kind, source, run_id::text, record_count
              FROM okg.admission_record_refs
             WHERE admission_id = %s::uuid
            """,
            (result["admission_id"],),
        )
        refs = cur.fetchall()

    assert envelope is not None
    assert envelope[0:3] == ("fast_track", "accepted", "svc.first")
    assert envelope[4]["policy_declared"] is True
    assert envelope[4]["trust"] == "fixture-trusted"
    assert envelope[5]["admission_policy"]["trust_label"] == "fixture-trusted"
    assert refs == [("payload", "svc.first", envelope[3], 1)]


@pytest.mark.postgres
def test_run_ingest_ok_row_admission_verdict_surface(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """The OK per-source result row surfaces the terminal admission verdict.

    Symmetric with the FAIL row, so a downstream proof-of-life gate can read the
    pipeline's own caught-up-vs-wedged decision
    (``admission_verdict.producer_policy.terminal_state_authorized``) instead of
    re-deriving liveness from the live-row delta. On a caught-up authoritative
    replay this is what lets ``deployment up``'s smoke proof accept a legitimate
    zero-row-delta publish; the finalize verdict content is proven separately by
    the zero-output admission tests, and gate consumption by the caught-up-replay
    tests in test_deployment_upgrade.py.
    """
    dep = _write_layered_fixture_deployment(tmp_path)

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            publish=False,
            admission_observed_at=datetime(
                2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc,
            ),
        ),
    )

    result = outcome.report["results"][0]
    assert result["status"] == "OK", outcome.report
    # The keys must be present on the real OK row (deleting the surfacing lines
    # regresses caught-up deploys back to failing — fail-closed, but still a bug).
    assert "admission_verdict" in result
    assert isinstance(result["admission_verdict"], dict)
    assert result["admission_status"] == "accepted"


@pytest.mark.postgres
def test_run_ingest_failed_source_records_admission_failure(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    dep = _write_layered_fixture_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    data = yaml.safe_load(registry_path.read_text())
    data["sources"]["first"]["params"]["fixture_path"] = str(
        dep / "missing-fixture.json",
    )
    registry_path.write_text(yaml.safe_dump(data))

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            include_dependents=False,
            publish=False,
            admission_observed_at=datetime(
                2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc,
            ),
        ),
    )

    assert outcome.any_failed is True
    result = outcome.report["results"][0]
    assert result["status"] == "FAIL"
    assert result["stage"] == "run"
    assert result["admission_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, verdict
              FROM okg.admission_envelopes
             WHERE admission_id = %s::uuid
            """,
            (result["admission_id"],),
        )
        status, verdict = cur.fetchone()
        cur.execute(
            """
            SELECT event_type, reason
              FROM okg.admission_events
             WHERE admission_id = %s::uuid
             ORDER BY event_id
            """,
            (result["admission_id"],),
        )
        events = cur.fetchall()

    assert status == "failed"
    assert "fixture_path" in verdict["error"]
    assert [event[0] for event in events] == ["created", "failed"]
    assert "fixture_path" in events[-1][1]


@pytest.mark.postgres
def test_run_ingest_publishes_allowed_producer_signature(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    # Register the `document` subtype the fixture emits so the
    # node-subtype-conformance publish gate admits the node (the
    # ingest path reads the applied DB catalog, which `clean_runtime`
    # ships empty).
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(clean_runtime, dep)
    registry_path = dep / "source_registry.yaml"
    data = yaml.safe_load(registry_path.read_text())
    policy = data["sources"]["first"]["admission_policy"]
    policy["allowed_record_kinds"] = ["node"]
    policy["output_signature"] = {
        "nodes": [
            {
                "subtype": "document",
                "clause_id": "allow_document",
            },
        ],
    }
    policy["policy_clauses"] = [
        {
            "id": "allow_document",
            "effect": "allow",
            "record_kind": "node",
            "subtype": "document",
        },
    ]
    registry_path.write_text(yaml.safe_dump(data))
    _claim_layered_fixture_catalog(clean_runtime, dep)

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            include_dependents=False,
            publish=True,
        ),
    )

    assert outcome.any_failed is False, outcome.report
    publish = outcome.report["publishes"][0]
    assert publish["status"] == "published"
    assert publish["nodes_added"] == 1
    result = outcome.report["results"][0]
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, target_generation_ref, verdict
              FROM okg.admission_envelopes
             WHERE admission_id = %s::uuid
            """,
            (result["admission_id"],),
        )
        status, target_generation_ref, verdict = cur.fetchone()
        generation = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).checkout_generation(publish["generation_id"])
        live_count = int(generation.query(
            """
            SELECT count(*) AS n
              FROM okg.graph_nodes
             WHERE node_id = 'svc:first'
            """
        )[0]["n"])

    assert status == "published"
    assert target_generation_ref == publish["generation_id"]
    assert verdict["producer_policy"]["status"] == "matched"
    assert verdict["producer_policy"]["matched_clause_ids"] == [
        "allow_document",
    ]
    assert live_count == 1


# --- Issue #2988: the stream-footprint tail flush is sliced ------------------
#
# A completed-scope source that declares no record set has its fact stream
# taken as its footprint; identities it no longer streams are retracted at the
# END of the run, after the streaming loop. That tail used to put every
# retraction into one buffer and flush it once, so a run owing more than
# MAX_FACTS_PER_CHUNK retractions was refused by `FactChunk` and published
# nothing (example-kb kb76: 21,398 retractions). The tests lower both limits so a
# few dozen retractions cross several slice boundaries.

_TAIL_LIMIT = 20
_TAIL_KEPT = 5
# 60 nodes, 5 kept: 55 retractions, sliced 20/20/15 so the last slice is partial.
_TAIL_TOTAL = 3 * _TAIL_LIMIT

_FOOTPRINT_TAIL_ADAPTER = '''
import json
from pathlib import Path

from okg.substrate.library.sources.base import NodeFact, SourceRun


class FootprintTailSource:
    name = "footprint_tail"
    profile = "discovery_crawl"
    last_run_stats: dict = {}

    def __init__(self, ids_path, **kwargs):
        self.ids_path = Path(ids_path)

    def run(self, run_id, *, mode="scope_complete", cursor=None,
            sync_scope=None, **kwargs):
        ids = json.loads(self.ids_path.read_text())

        def facts():
            for index in ids:
                yield NodeFact(
                    node_id="svc:doc-%d" % index,
                    subtype="document",
                    attrs={"text": "document %d" % index},
                    source_record_id={"id": "doc-%d" % index},
                    source_revision={"v": 1},
                )

        # No record_set: the stream itself is the completed-scope footprint.
        return SourceRun(
            facts=facts(),
            completed_scope=True,
            next_cursor={"ids": len(ids)},
            run_mode=mode,
        )
'''


def _footprint_tail_deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    clean_runtime: str,
    *,
    sync: dict | None = None,
) -> tuple[Path, Path]:
    module_dir = tmp_path / "adapters"
    module_dir.mkdir()
    (module_dir / "footprint_tail_adapter.py").write_text(
        _FOOTPRINT_TAIL_ADAPTER, encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(module_dir))
    sys.modules.pop("footprint_tail_adapter", None)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    # Outside the deployment repository: the publisher refuses uncommitted
    # provenance inputs, and the tests rewrite this file between runs.
    ids_path = tmp_path / "footprint_ids.json"
    ids_path.write_text(json.dumps(list(range(_TAIL_TOTAL))))
    registry_path = dep / "source_registry.yaml"
    data = yaml.safe_load(registry_path.read_text())
    data["sources"] = {
        "footprint": {
            **_source("svc.footprint"),
            "module": "footprint_tail_adapter",
            "class": "FootprintTailSource",
            "params": {"ids_path": str(ids_path)},
            "source_policy": _fixture_source_privacy_policy(),
            **({"sync": sync} if sync is not None else {}),
        },
    }
    registry_path.write_text(yaml.safe_dump(data))
    _claim_layered_fixture_catalog(clean_runtime, dep)
    # Both bounds, lowered together: the runner's flush boundary and the
    # FactChunk refusal it must stay under. Nothing else changes.
    import okg.substrate.fact_batch as fact_batch_module
    import okg.substrate.ingest.runner as runner_module

    monkeypatch.setattr(runner_module, "COPY_CHUNK_ROWS", _TAIL_LIMIT)
    monkeypatch.setattr(fact_batch_module, "MAX_FACTS_PER_CHUNK", _TAIL_LIMIT)
    return dep, ids_path


def _footprint_tail_ingest(
    clean_runtime: str,
    dep: Path,
    *,
    events: tuple[SourceSyncEvent, ...] = (),
):
    return run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"footprint"},
            include_predecessors=False,
            include_dependents=False,
            publish=True,
            # The runtime worker always passes the trigger events of the job
            # it drains (`source_sync_workflow`); `okg ingest --inline` none.
            **({"source_events": {"footprint": events}} if events else {}),
        ),
    )


def _footprint_tail_state(clean_runtime: str) -> dict[str, object]:
    """Published generation, its live nodes, and the branch head's nodes."""
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        latest = store.latest_generation(deployment_name="svc")
        generation_id = latest.checkpoint_id if latest is not None else None
        published = (
            int(store.checkout_generation(generation_id).query(
                "SELECT count(*) AS n FROM okg.graph_nodes"
                " WHERE source = 'svc.footprint'"
            )[0]["n"])
            if generation_id is not None
            else 0
        )
        head = int(store.checkout().query(
            "SELECT count(*) AS n FROM okg.graph_nodes"
            " WHERE source = 'svc.footprint'"
        )[0]["n"])
        with conn.cursor() as cur:
            cur.execute(
                # This database holds only this source's admissions.
                "SELECT state, count(*) FROM okg.fact_batch_receipts"
                " GROUP BY state"
            )
            receipts = {row[0]: row[1] for row in cur.fetchall()}
    return {
        "generation_id": generation_id,
        "published_nodes": published,
        "head_nodes": head,
        "receipts": receipts,
    }


@pytest.mark.postgres
def test_stream_footprint_tail_retracts_more_than_one_chunk(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2988: tail retractions above the chunk limit publish in slices."""
    dep, ids_path = _footprint_tail_deployment(
        tmp_path, monkeypatch, clean_runtime,
    )
    first = _footprint_tail_ingest(clean_runtime, dep)
    assert first.any_failed is False, first.report
    before = _footprint_tail_state(clean_runtime)
    assert before["published_nodes"] == _TAIL_TOTAL, before

    from okg.substrate.ingest.runner import SourceRunner

    tail_slices: list[int] = []
    real_flush = SourceRunner._flush_chunk

    def counting_flush(self, facts, markers, **kwargs):
        if facts and all(getattr(f, "op", "I") == "R" for f in facts):
            tail_slices.append(len(facts))
        return real_flush(self, facts, markers, **kwargs)

    monkeypatch.setattr(SourceRunner, "_flush_chunk", counting_flush)
    ids_path.write_text(json.dumps(list(range(_TAIL_KEPT))))
    second = _footprint_tail_ingest(clean_runtime, dep)

    assert second.any_failed is False, second.report
    result = second.report["results"][0]
    retracted = _TAIL_TOTAL - _TAIL_KEPT
    # Every slice stays under the limit, and the slices add up to the whole
    # retraction set: splitting neither drops nor double-counts a fact.
    assert tail_slices == [_TAIL_LIMIT, _TAIL_LIMIT, retracted - 2 * _TAIL_LIMIT]
    assert result["retract_n"] == retracted, result
    after = _footprint_tail_state(clean_runtime)
    assert after["generation_id"] != before["generation_id"]
    assert after["published_nodes"] == _TAIL_KEPT, after
    assert after["head_nodes"] == _TAIL_KEPT, after


# Issue #3314. The runtime worker runs every source with the trigger events
# of its job. A reconcile or poll wakeup names no scope key, so the source
# reads its whole scope, exactly as an event-less `okg ingest --inline` does.
# The worker run must then retract what left the input, record the record
# set, and accept the next unchanged read as an authoritative replay instead
# of publishing additively and rejecting it `unclassified_empty`.
_WORKER_FOOTPRINT_SYNC = {
    "triggers": ["manual", "poll", "reconcile"],
    "default_event_mode": "scope_complete",
    "reconcile_mode": "scope_complete",
    "poll": {"probe": "always_changed", "interval_seconds": 300},
}


def _worker_wakeup(trigger_kind: str) -> tuple[SourceSyncEvent, ...]:
    # A real poll wakeup always carries the probe fingerprint as its
    # revision hint; a reconcile wakeup carries none.
    return (
        SourceSyncEvent(
            deployment="svc",
            source_name="footprint",
            trigger_kind=trigger_kind,
            revision_hint=(
                "always_changed:footprint" if trigger_kind == "poll" else None
            ),
            reason=f"{trigger_kind}:test",
        ),
    )


def _footprint_record_set_recorded(clean_runtime: str) -> bool:
    from okg.substrate.library.sources.base import (
        cursor_has_record_set_authority,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks"
            " WHERE source = 'svc.footprint'",
        )
        row = cur.fetchone()
    return row is not None and cursor_has_record_set_authority(row[0])


@pytest.mark.postgres
@pytest.mark.parametrize("trigger_kind", ["reconcile", "poll"])
def test_worker_wakeup_of_whole_scope_source_retracts_and_replays(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    trigger_kind: str,
) -> None:
    dep, ids_path = _footprint_tail_deployment(
        tmp_path, monkeypatch, clean_runtime, sync=_WORKER_FOOTPRINT_SYNC,
    )
    events = _worker_wakeup(trigger_kind)

    first = _footprint_tail_ingest(clean_runtime, dep, events=events)
    assert first.any_failed is False, first.report
    seeded = first.report["results"][0]
    assert seeded["sync_scope"]["trigger_kinds"] == [trigger_kind], seeded
    assert seeded["sync_scope"]["scope_keys"] == [], seeded
    assert _footprint_tail_state(clean_runtime)["published_nodes"] == (
        _TAIL_TOTAL
    )
    assert _footprint_record_set_recorded(clean_runtime) is True

    # A record leaves the input: the worker run retracts it.
    ids_path.write_text(json.dumps(list(range(_TAIL_KEPT))))
    second = _footprint_tail_ingest(clean_runtime, dep, events=events)
    assert second.any_failed is False, second.report
    shrunk = second.report["results"][0]
    assert shrunk["retract_n"] == _TAIL_TOTAL - _TAIL_KEPT, shrunk
    after_shrink = _footprint_tail_state(clean_runtime)
    assert after_shrink["published_nodes"] == _TAIL_KEPT, after_shrink
    assert after_shrink["head_nodes"] == _TAIL_KEPT, after_shrink
    assert _footprint_record_set_recorded(clean_runtime) is True

    # The same input again: an accepted authoritative replay, not a
    # rejected `unclassified_empty` run.
    third = _footprint_tail_ingest(clean_runtime, dep, events=events)
    assert third.any_failed is False, third.report
    replay = third.report["results"][0]
    assert replay["retract_n"] == 0, replay
    assert replay["admission_status"] == "accepted", replay
    assert replay["admission_verdict"]["producer_policy"]["reason"] == (
        "authoritative_replay"
    ), replay
    assert _footprint_tail_state(clean_runtime)["published_nodes"] == (
        _TAIL_KEPT
    )


@pytest.mark.postgres
def test_worker_wakeup_naming_a_scope_key_keeps_whole_scope_deletion_off(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#3314 guard: only a wakeup that narrows nothing gains deletion."""
    dep, ids_path = _footprint_tail_deployment(
        tmp_path, monkeypatch, clean_runtime, sync=_WORKER_FOOTPRINT_SYNC,
    )
    first = _footprint_tail_ingest(clean_runtime, dep)
    assert first.any_failed is False, first.report

    ids_path.write_text(json.dumps(list(range(_TAIL_KEPT))))
    narrowed = _footprint_tail_ingest(
        clean_runtime,
        dep,
        events=(
            SourceSyncEvent(
                deployment="svc",
                source_name="footprint",
                trigger_kind="manual",
                scope_key="doc-0",
                reason="manual:test",
            ),
        ),
    )

    assert narrowed.any_failed is False, narrowed.report
    result = narrowed.report["results"][0]
    assert result["sync_scope"]["scope_keys"] == ["doc-0"], result
    # The slice's absence proves nothing about the rest of the source.
    assert result["retract_n"] == 0, result
    assert _footprint_tail_state(clean_runtime)["head_nodes"] == _TAIL_TOTAL


@pytest.mark.postgres
def test_stream_footprint_tail_failure_mid_slices_leaves_publication_unchanged(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2988: a failure on tail slice 2 of 3 publishes nothing.

    Each slice commits its own chunk transaction, as every streaming chunk
    does; the run is not one transaction. Readers read the published
    generation, and the failed source contributes no publish, so the
    generation id and its counts are unchanged. The orchestrator then runs
    #1431 compensation (`_retract_chronos_run`) over the run's receipts.
    """
    dep, ids_path = _footprint_tail_deployment(
        tmp_path, monkeypatch, clean_runtime,
    )
    first = _footprint_tail_ingest(clean_runtime, dep)
    assert first.any_failed is False, first.report
    before = _footprint_tail_state(clean_runtime)
    assert before["published_nodes"] == _TAIL_TOTAL, before
    assert before["head_nodes"] == _TAIL_TOTAL, before

    from okg.substrate.ingest.runner import SourceRunner

    tail_slices: list[int] = []
    real_flush = SourceRunner._flush_chunk

    def failing_flush(self, facts, markers, **kwargs):
        if facts and all(getattr(f, "op", "I") == "R" for f in facts):
            tail_slices.append(len(facts))
            if len(tail_slices) == 2:
                raise RuntimeError("injected failure on tail slice 2")
        return real_flush(self, facts, markers, **kwargs)

    monkeypatch.setattr(SourceRunner, "_flush_chunk", failing_flush)
    ids_path.write_text(json.dumps(list(range(_TAIL_KEPT))))
    second = _footprint_tail_ingest(clean_runtime, dep)

    assert second.any_failed is True, second.report
    result = second.report["results"][0]
    assert result["status"] != "OK", result
    assert "injected failure on tail slice 2" in str(result.get("error")), result
    assert tail_slices == [_TAIL_LIMIT, _TAIL_LIMIT]
    failed = _footprint_tail_state(clean_runtime)
    assert failed["generation_id"] == before["generation_id"], failed
    assert failed["published_nodes"] == before["published_nodes"], failed
    # Slice 1 committed before the failure. #1431 compensation carried its
    # receipt to `compensated`; the first run's receipts stay `applied`.
    assert failed["receipts"].get("applied") == before["receipts"].get("applied")
    assert failed["receipts"].get("compensated") == 1, failed["receipts"]
    assert not failed["receipts"].get("prepared"), failed["receipts"]
    assert not failed["receipts"].get("compensating"), failed["receipts"]
    # Issue #2993: the compensation undoes the run to its run-base
    # checkpoint, so slice 1's committed retractions are restored on the
    # branch head too (before, only rows the run inserted were retracted and
    # the head kept _TAIL_TOTAL - _TAIL_LIMIT).
    assert failed["head_nodes"] == _TAIL_TOTAL, failed

    # A clean rerun converges on the source's real footprint.
    monkeypatch.setattr(SourceRunner, "_flush_chunk", real_flush)
    third = _footprint_tail_ingest(clean_runtime, dep)
    assert third.any_failed is False, third.report
    final = _footprint_tail_state(clean_runtime)
    assert final["generation_id"] != before["generation_id"]
    assert final["published_nodes"] == _TAIL_KEPT, final
    assert final["head_nodes"] == _TAIL_KEPT, final


@pytest.mark.postgres
def test_run_ingest_rejects_out_of_signature_source_output(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    dep = _write_layered_fixture_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    data = yaml.safe_load(registry_path.read_text())
    policy = data["sources"]["first"]["admission_policy"]
    policy["allowed_record_kinds"] = ["node"]
    policy["output_signature"] = {
        "nodes": [
            {
                "subtype": "source_file",
                "clause_id": "allow_source_file",
            },
        ],
    }
    policy["policy_clauses"] = [
        {
            "id": "allow_source_file",
            "effect": "allow",
            "record_kind": "node",
            "subtype": "source_file",
        },
    ]
    registry_path.write_text(yaml.safe_dump(data))

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            include_dependents=False,
            publish=True,
        ),
    )

    result = outcome.report["results"][0]
    assert outcome.any_failed is True
    assert result["status"] == "FAIL"
    assert result["stage"] == "admission_finalize"
    assert result["admission_status"] == "rejected"
    assert result["admission_reject_reason"] == (
        "producer_node_subtype_not_allowed"
    )
    assert outcome.report["publishes"] == []
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, verdict
              FROM okg.admission_envelopes
             WHERE admission_id = %s::uuid
            """,
            (result["admission_id"],),
        )
        status, verdict = cur.fetchone()
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        live_count = int(store.query(
            """
            SELECT count(*) AS n
              FROM okg.graph_nodes
             WHERE node_id = 'svc:first'
            """
        )[0]["n"])

    assert status == "rejected"
    assert verdict["code"] == "producer_node_subtype_not_allowed"
    assert verdict["producer_policy"]["offending_records"][0]["subtype"] == (
        "document"
    )
    assert live_count == 0


@pytest.mark.postgres
def test_accepted_terminal_state_with_no_cursor_preserves_prior_authority(
    clean_runtime: str,
) -> None:
    old_reconcile = datetime(2026, 1, 1, tzinfo=timezone.utc)
    accepted_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES ('svc.no-cursor', '{"cursor": "old", "record_set": {
              "records": {"old": {"node_id": "svc:old"}}
            }}'::jsonb, %s)
            """,
            (old_reconcile,),
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction():
            ingest_service._persist_accepted_source_terminal_state(
                conn,
                source="svc.no-cursor",
                terminal_state={
                    "next_cursor": None,
                    "observed_at": accepted_at.isoformat(),
                    "completed_scope": False,
                    "deployment": "svc",
                    "source_progress": {},
                },
            )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_applied_rev, last_reconcile_at
              FROM okg.source_watermarks
             WHERE source = 'svc.no-cursor'
            """,
        )
        cursor, last_reconcile = cur.fetchone()

    assert cursor["cursor"] == "old"
    assert cursor["record_set"]["records"]["old"]["node_id"] == "svc:old"
    assert last_reconcile == old_reconcile


@pytest.mark.postgres
def test_accepted_terminal_state_rejects_runner_owned_progress_before_cursor(
    clean_runtime: str,
) -> None:
    source = "svc.reserved-progress"
    reserved_key = "sync.bootstrap_identity_authority"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO okg.source_watermarks (source, last_applied_rev)
            VALUES (%s, '{"cursor": "old"}'::jsonb)
            """,
            (source,),
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(SubstrateError) as caught:
            with conn.transaction():
                ingest_service._persist_accepted_source_terminal_state(
                    conn,
                    source=source,
                    terminal_state={
                        "next_cursor": {"cursor": "new"},
                        "observed_at": datetime.now(timezone.utc).isoformat(),
                        "completed_scope": False,
                        "deployment": "svc",
                        "source_progress": {reserved_key: "forged"},
                    },
                )
    assert caught.value.code == "source_progress_reserved_key"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cursor = conn.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks WHERE source = %s",
            (source,),
        ).fetchone()[0]
        ledger_count = conn.execute(
            """
            SELECT count(*) FROM okg.source_progress
             WHERE deployment = 'svc' AND source_name = %s AND key = %s
            """,
            (source, reserved_key),
        ).fetchone()[0]
    assert cursor == {"cursor": "old"}
    assert ledger_count == 0


@pytest.mark.postgres
def test_run_ingest_commits_terminal_state_only_with_admission_acceptance(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "okg_test_accepted_record_set_source"
    module = types.ModuleType(module_name)

    class AcceptedRecordSetSource:
        name = "accepted_record_set"
        profile = "discovery_crawl"
        last_run_stats = {"records": 1}

        def run(self, run_id: str, *, mode: str = "cursor", **_kwargs):
            from okg.substrate.library.sources.base import (
                NodeFact,
                RecordEmission,
                RecordNode,
                RecordSet,
                SourceRun,
            )

            record_id = {"id": "accepted"}
            revision = {"v": 2}
            node = NodeFact(
                node_id="svc:accepted",
                subtype="document",
                attrs={"text": "accepted document"},
                source_record_id=record_id,
                source_revision=revision,
            )
            return SourceRun(
                facts=[node],
                completed_scope=True,
                next_cursor={"cursor": "new"},
                run_mode=mode,
                record_set=RecordSet(records={
                    "accepted": RecordEmission(
                        primary=RecordNode(
                            node_id=node.node_id,
                            subtype=node.subtype,
                            source_record_id=record_id,
                            source_revision=revision,
                        ),
                    ),
                }),
            )

        def progress_after_run(self) -> dict[str, str]:
            return {"record:svc:accepted": "new"}

    module.AcceptedRecordSetSource = AcceptedRecordSetSource
    monkeypatch.setitem(sys.modules, module_name, module)

    dep = _write_stub_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    data = yaml.safe_load(registry_path.read_text())
    data["sources"]["first"]["module"] = module_name
    data["sources"]["first"]["class"] = "AcceptedRecordSetSource"
    policy = data["sources"]["first"]["admission_policy"]
    policy["allowed_record_kinds"] = ["node"]
    policy["output_signature"] = {
        "nodes": [{"subtype": "document", "clause_id": "allow_document"}],
    }
    policy["policy_clauses"] = [{
        "id": "allow_document",
        "effect": "allow",
        "record_kind": "node",
        "subtype": "document",
    }]
    registry_path.write_text(yaml.safe_dump(data))

    old_reconcile = datetime(2026, 1, 1, tzinfo=timezone.utc)
    observed_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES ('svc.first', '{"cursor": "old"}'::jsonb, %s)
            """,
            (old_reconcile,),
        )

    original_finalize = ingest_service._finalize_source_admission
    observed_pre_acceptance: list[dict[str, object]] = []

    def assert_old_state_then_finalize(**kwargs):
        with psycopg.connect(clean_runtime, autocommit=True) as conn, \
             conn.cursor() as cur:
            cur.execute(
                """
                SELECT last_applied_rev
                  FROM okg.source_watermarks
                 WHERE source = 'svc.first'
                """,
            )
            cursor = cur.fetchone()[0]
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.source_progress
                 WHERE deployment = 'svc'
                   AND source_name = 'svc.first'
                   AND key = 'record:svc:accepted'
                """,
            )
            progress_count = int(cur.fetchone()[0])
        observed_pre_acceptance.append({
            "cursor": cursor,
            "progress_count": progress_count,
        })
        return original_finalize(**kwargs)

    monkeypatch.setattr(
        ingest_service,
        "_finalize_source_admission",
        assert_old_state_then_finalize,
    )

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            include_dependents=False,
            publish=False,
            admission_observed_at=observed_at,
        ),
    )

    assert outcome.any_failed is False, outcome.report
    assert observed_pre_acceptance == [{
        "cursor": {"cursor": "old"},
        "progress_count": 0,
    }]
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_applied_rev, last_reconcile_at
              FROM okg.source_watermarks
             WHERE source = 'svc.first'
            """,
        )
        cursor, last_reconcile = cur.fetchone()
        cur.execute(
            """
            SELECT value
              FROM okg.source_progress
             WHERE deployment = 'svc'
               AND source_name = 'svc.first'
               AND key = 'record:svc:accepted'
            """,
        )
        progress = cur.fetchone()

    assert cursor["cursor"] == "new"
    assert cursor["record_set"]["records"]["accepted"]
    assert last_reconcile == observed_at
    assert progress == ("new",)


@pytest.mark.postgres
def test_bootstrap_lease_spans_finalize_failure_compensation_and_restore(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A contender cannot enter while terminal failure restores progress."""
    dep = _write_stub_deployment(tmp_path)
    entry = _load_registry(tmp_path, {
        "bootstrap": {
            **_source("svc.bootstrap"),
            "module": "bootstrap_lease_test_source",
            "class": "BootstrapLeaseTestSource",
        },
    })["bootstrap"]
    module = types.ModuleType("bootstrap_lease_test_source")

    class BootstrapLeaseTestSource:
        profile = "discovery_crawl"
        probe_short_circuit_safe = True
        change_probe = object()
        last_run_stats: dict[str, object] = {}

    module.BootstrapLeaseTestSource = BootstrapLeaseTestSource
    monkeypatch.setitem(sys.modules, module.__name__, module)

    old_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO okg.source_watermarks (source, last_applied_rev)
            VALUES ('svc.bootstrap', '{"cursor":"old"}'::jsonb)
            """,
        )
        conn.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES ('svc', 'svc.bootstrap', 'record:old', 'old', %s)
            """,
            (old_time,),
        )

    lease = threading.Lock()
    restore_started = threading.Event()
    permit_restore = threading.Event()
    restore_finished = threading.Event()
    first_run_id: list[str] = []

    class LeaseHoldingRunner:
        def __init__(self, dsn: str, **_kwargs: object) -> None:
            self.dsn = dsn
            self.held = False

        @staticmethod
        def _adapter_probe_short_circuit_safe(_adapter: object) -> bool:
            return True

        def run(self, _adapter: object, *, run_context, **_kwargs: object):
            if not lease.acquire(blocking=False):
                error = SubstrateError(
                    "bootstrap identity authority is being consumed concurrently"
                )
                error.code = "bootstrap_identity_authority_concurrent"
                raise error
            self.held = True
            first_run_id.append(run_context.run_id)
            with psycopg.connect(self.dsn, autocommit=True) as conn:
                conn.execute(
                    """
                    UPDATE okg.source_watermarks
                       SET replay_required = %s::jsonb
                     WHERE source = 'svc.bootstrap'
                    """,
                    (json.dumps({
                        "obligation": "bootstrap_identity_resume",
                        "run_id": run_context.run_id,
                    }),),
                )
                conn.execute(
                    """
                    INSERT INTO okg.source_progress
                      (deployment, source_name, key, value)
                    VALUES ('svc', 'svc.bootstrap', 'record:new', 'new')
                    """,
                )
            return types.SimpleNamespace(
                run_id=run_context.run_id,
                nodes_appended=0,
                edges_appended=0,
                retractions_node=0,
                retractions_edge=0,
                completed_scope=False,
                source_health=None,
                next_cursor={"cursor": "new"},
                terminal_progress={"record:new": "new"},
                clear_replay_required=False,
                replay_target={
                    "obligation": "bootstrap_identity_resume",
                    "run_id": run_context.run_id,
                },
                expected_last_applied_rev={"cursor": "old"},
                watermark_compare_and_swap=True,
                expected_bootstrap_ledger=None,
                next_bootstrap_ledger={"ledger": "next"},
                rejected_fact_count=0,
                timings_ms={},
            )

        def release_deferred_bootstrap_lock(self) -> None:
            if self.held:
                self.held = False
                lease.release()

    contexts = itertools.count(1)
    first_expected_run_id = str(uuid.UUID(int=1))

    def prepare(**_kwargs: object):
        index = next(contexts)
        return (
            ingest_service.SourceRunContext(
                run_id=str(uuid.UUID(int=index)),
                observed_at=datetime.now(timezone.utc),
                admission_id=str(uuid.uuid4()),
            ),
            {},
            "pending",
        )

    real_restore = ingest_service._restore_source_state

    def blocked_restore(dsn, snapshot, *, expected_replay_run_id=None):
        if expected_replay_run_id != first_expected_run_id:
            real_restore(
                dsn,
                snapshot,
                expected_replay_run_id=expected_replay_run_id,
            )
            return
        restore_started.set()
        assert permit_restore.wait(timeout=10)
        real_restore(
            dsn,
            snapshot,
            expected_replay_run_id=expected_replay_run_id,
        )
        restore_finished.set()

    monkeypatch.setattr(ingest_service, "SourceRunner", LeaseHoldingRunner)
    monkeypatch.setattr(ingest_service, "_prepare_source_admission", prepare)
    monkeypatch.setattr(
        ingest_service,
        "_finalize_source_admission",
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("forced terminal finalize failure")
        ),
    )
    monkeypatch.setattr(ingest_service, "_restore_source_state", blocked_restore)
    monkeypatch.setattr(ingest_service, "_retract_chronos_run", lambda *_a, **_k: None)
    monkeypatch.setattr(ingest_service, "_fail_source_admission", lambda **_k: None)
    monkeypatch.setattr(
        ingest_service,
        "resolve_deployment_source_pii_posture",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        ingest_service,
        "runtime_enforce_mode_dsn_keys",
        lambda _path: set(),
    )

    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        publish=False,
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(
            ingest_service._run_source,
            options,
            entry,
            "bootstrap",
        )
        if not restore_started.wait(timeout=10):
            pytest.fail(
                "first source run ended before restoration started: "
                f"{first.result(timeout=1)!r}"
            )
        contender = executor.submit(
            ingest_service._run_source,
            options,
            entry,
            "bootstrap",
        )
        contender_result = contender.result(timeout=10)
        assert contender_result["status"] == "FAIL"
        assert "consumed concurrently" in contender_result["error"]
        assert restore_finished.is_set() is False
        assert lease.locked() is True
        permit_restore.set()
        first_result = first.result(timeout=10)

    assert first_result["status"] == "FAIL"
    assert first_result["stage"] == "admission_finalize"
    assert restore_finished.is_set() is True
    assert lease.locked() is False
    assert first_run_id == [first_expected_run_id]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        progress = dict(conn.execute(
            """
            SELECT key, value
              FROM okg.source_progress
             WHERE deployment = 'svc'
               AND source_name = 'svc.bootstrap'
               AND key NOT LIKE 'sync.%%'
             ORDER BY key
            """,
        ).fetchall())
    assert progress == {"record:old": "old"}


@pytest.mark.postgres
def test_run_ingest_rejected_admission_restores_source_state(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "okg_test_rejected_record_set_source"
    module = types.ModuleType(module_name)

    class RejectingRecordSetSource:
        name = "rejecting_record_set"
        profile = "discovery_crawl"
        last_run_stats = {"records": 1}

        def run(
            self,
            run_id: str,
            *,
            mode: str = "cursor",
            sync_scope: dict | None = None,
        ):
            from okg.substrate.library.sources.base import (
                NodeFact,
                RecordEmission,
                RecordNode,
                RecordSet,
                SourceRun,
            )

            record_id = {"id": "record"}
            revision = {"v": 2}
            node = NodeFact(
                node_id="svc:record",
                subtype="document",
                attrs={"text": "rejected document"},
                source_record_id=record_id,
                source_revision=revision,
            )
            return SourceRun(
                facts=[node],
                completed_scope=True,
                next_cursor={"cursor": "new"},
                run_mode=mode,
                record_set=RecordSet(records={
                    "record": RecordEmission(
                        primary=RecordNode(
                            node_id=node.node_id,
                            subtype=node.subtype,
                            source_record_id=record_id,
                            source_revision=revision,
                        ),
                    ),
                }),
            )

        def progress_after_run(self) -> dict[str, str]:
            return {"record:svc:record": "new"}

    module.RejectingRecordSetSource = RejectingRecordSetSource
    monkeypatch.setitem(sys.modules, module_name, module)

    dep = _write_stub_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    data = yaml.safe_load(registry_path.read_text())
    data["sources"]["first"]["module"] = module_name
    data["sources"]["first"]["class"] = "RejectingRecordSetSource"
    policy = data["sources"]["first"]["admission_policy"]
    policy["allowed_record_kinds"] = ["node"]
    policy["output_signature"] = {
        "nodes": [
            {
                "subtype": "source_file",
                "clause_id": "allow_source_file",
            },
        ],
    }
    policy["policy_clauses"] = [
        {
            "id": "allow_source_file",
            "effect": "allow",
            "record_kind": "node",
            "subtype": "source_file",
        },
    ]
    registry_path.write_text(yaml.safe_dump(data))

    old_reconcile = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, cutover_watermark, last_applied_rev,
               last_published_rev, projection_cursor, last_reconcile_at)
            VALUES
              ('svc.first', '{"cutover": "old"}'::jsonb,
               '{"cursor": "old"}'::jsonb, '{"published": "old"}'::jsonb,
               42, %s)
            """,
            (old_reconcile,),
        )
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES ('svc', 'svc.first', 'record:svc:old', 'old', %s)
            """,
            (old_reconcile,),
        )
        cur.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES
              ('svc', 'svc.first', 'sync.run.old', '{"old": true}', %s)
            """,
            (old_reconcile,),
        )

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            include_dependents=False,
            publish=True,
        ),
    )

    result = outcome.report["results"][0]
    assert outcome.any_failed is True
    assert result["status"] == "FAIL"
    assert result["admission_status"] == "rejected"
    assert result["admission_reject_reason"] == (
        "producer_node_subtype_not_allowed"
    )
    assert result["source_state_restored"] is True
    assert outcome.report["publishes"] == []
    assert outcome.report["layer_timings"][0]["publish_status"] == (
        "skipped_source_failed"
    )

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        live_count = int(store.query(
            """
            SELECT count(*) AS n
              FROM okg.graph_nodes
             WHERE source = 'svc.first'
            """
        )[0]["n"])
        cur.execute(
            """
            SELECT cutover_watermark, last_applied_rev, last_published_rev,
                   projection_cursor, last_reconcile_at
              FROM okg.source_watermarks
             WHERE source = 'svc.first'
            """
        )
        watermark = cur.fetchone()
        cur.execute(
            """
            SELECT key, value, updated_at
              FROM okg.source_progress
             WHERE deployment = 'svc'
               AND source_name = 'svc.first'
             ORDER BY key
            """
        )
        progress_rows = cur.fetchall()

    assert live_count == 0
    assert watermark[0] == {"cutover": "old"}
    assert watermark[1] == {"cursor": "old"}
    assert watermark[2] == {"published": "old"}
    assert watermark[3] == 42
    assert watermark[4] == old_reconcile
    progress_by_key = {
        key: (value, updated_at)
        for key, value, updated_at in progress_rows
    }
    assert progress_by_key["record:svc:old"] == ("old", old_reconcile)
    assert progress_by_key["sync.run.old"] == ('{"old": true}', old_reconcile)
    assert "record:svc:record" not in progress_by_key


@pytest.mark.postgres
def test_source_state_snapshot_excludes_telemetry_history(
    clean_runtime: str,
) -> None:
    old_reconcile = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, cutover_watermark, last_applied_rev,
               last_published_rev, projection_cursor, last_reconcile_at)
            VALUES
              ('svc.first', '{"cutover": "old"}'::jsonb,
               '{"cursor": "old"}'::jsonb, '{"published": "old"}'::jsonb,
               42, %s)
            """,
            (old_reconcile,),
        )
        cur.executemany(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value, updated_at)
            VALUES ('svc', 'svc.first', %s, %s, %s)
            """,
            [
                ("record:svc:old", "old", old_reconcile),
                ("sync.last_run", '{"run": "old"}', old_reconcile),
                ("sync.run.old", '{"old": true}', old_reconcile),
                (
                    "sync.stage_timings.run.old",
                    '{"stage": "old"}',
                    old_reconcile,
                ),
            ],
        )

    snapshot = ingest_service._snapshot_source_state(
        clean_runtime,
        deployment="svc",
        source="svc.first",
    )

    assert [row["key"] for row in snapshot.progress_rows] == [
        "record:svc:old",
    ]

    newer = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.source_progress
               SET value = 'changed', updated_at = %s
             WHERE deployment = 'svc'
               AND source_name = 'svc.first'
               AND key = 'record:svc:old'
            """,
            (newer,),
        )
        cur.execute(
            """
            UPDATE okg.source_progress
               SET value = '{"run": "new"}', updated_at = %s
             WHERE deployment = 'svc'
               AND source_name = 'svc.first'
               AND key = 'sync.last_run'
            """,
            (newer,),
        )

    ingest_service._restore_source_state(clean_runtime, snapshot)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT key, value, updated_at
              FROM okg.source_progress
             WHERE deployment = 'svc'
               AND source_name = 'svc.first'
             ORDER BY key
            """
        )
        progress = {
            key: (value, updated_at)
            for key, value, updated_at in cur.fetchall()
        }

    assert progress["record:svc:old"] == ("old", old_reconcile)
    assert progress["sync.last_run"] == ('{"run": "new"}', newer)
    assert progress["sync.run.old"] == ('{"old": true}', old_reconcile)
    assert progress["sync.stage_timings.run.old"] == (
        '{"stage": "old"}',
        old_reconcile,
    )


@pytest.mark.postgres
def test_stale_rejected_run_cannot_restore_over_newer_replay_owner(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev)
            VALUES ('svc.concurrent', '{"cursor": "old"}'::jsonb)
            """
        )
        conn.execute(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value)
            VALUES ('svc', 'svc.concurrent', 'record:item', 'old')
            """
        )

    snapshot = ingest_service._snapshot_source_state(
        clean_runtime,
        deployment="svc",
        source="svc.concurrent",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            """
            UPDATE okg.source_watermarks
               SET last_applied_rev = '{"cursor": "newer"}'::jsonb,
                   replay_required = jsonb_build_object(
                     'run_id', 'newer-run',
                     'target_fingerprint', 'newer-target'
                   ),
                   replay_required_at = now()
             WHERE source = 'svc.concurrent'
            """
        )
        conn.execute(
            """
            UPDATE okg.source_progress
               SET value = 'newer', updated_at = now()
             WHERE deployment = 'svc'
               AND source_name = 'svc.concurrent'
               AND key = 'record:item'
            """
        )

    ingest_service._restore_source_state(
        clean_runtime,
        snapshot,
        expected_replay_run_id="stale-run",
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cursor, replay = conn.execute(
            """
            SELECT last_applied_rev, replay_required
              FROM okg.source_watermarks
             WHERE source = 'svc.concurrent'
            """
        ).fetchone()
        progress = conn.execute(
            """
            SELECT value
              FROM okg.source_progress
             WHERE deployment = 'svc'
               AND source_name = 'svc.concurrent'
               AND key = 'record:item'
            """
        ).fetchone()[0]

    assert cursor == {"cursor": "newer"}
    assert replay["run_id"] == "newer-run"
    assert progress == "newer"


@pytest.mark.postgres
def test_unclassified_zero_output_source_sync_is_rejected_without_scan(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _assert_zero_output_source_sync_uses_bounded_admission_path(
        tmp_path=tmp_path,
        clean_runtime=clean_runtime,
        monkeypatch=monkeypatch,
        admission_mode="fast_track",
    )


@pytest.mark.postgres
@pytest.mark.parametrize("admission_mode", ["direct", "review", "quarantine"])
def test_unclassified_zero_output_rejects_non_fast_track_modes(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    admission_mode: str,
) -> None:
    _assert_zero_output_source_sync_uses_bounded_admission_path(
        tmp_path=tmp_path,
        clean_runtime=clean_runtime,
        monkeypatch=monkeypatch,
        admission_mode=admission_mode,
    )


def _source_run_envelope(dsn: str, source: str) -> dict[str, object]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT admission_id::text, status, admission_mode,
                   target_branch_name, run_id::text
              FROM okg.admission_envelopes
             WHERE deployment_name = 'svc'
               AND intent = 'source_sync'
               AND source = %s
            """,
            (source,),
        )
        rows = cur.fetchall()
        assert len(rows) == 1, rows
        admission_id, status, mode, branch, run_id = rows[0]
        cur.execute(
            """
            SELECT event_type
              FROM okg.admission_events
             WHERE admission_id = %s::uuid
             ORDER BY event_id
            """,
            (admission_id,),
        )
        events = [row[0] for row in cur.fetchall()]
    return {
        "admission_id": admission_id,
        "status": status,
        "admission_mode": mode,
        "target_branch_name": branch,
        "run_id": run_id,
        "events": events,
    }


def _branch_source_rows(
    dsn: str, branch: str, source: str
) -> list[tuple[str, str, str]]:
    """(node_id, source, run_id) of every current node `source` wrote."""
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = store.checkout(branch).query(
            "SELECT node_id, source, run_id::text AS run_id "
            "FROM okg.graph_nodes WHERE source = :source "
            "ORDER BY node_id",
            {"source": source},
        )
    finally:
        store.close()
    return [
        (str(row["node_id"]), str(row["source"]), str(row["run_id"]))
        for row in rows
    ]


def _branch_node_ids(dsn: str, branch: str) -> list[str]:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = store.checkout(branch).query(
            "SELECT node_id FROM okg.graph_nodes ORDER BY node_id"
        )
    finally:
        store.close()
    return [str(row["node_id"]) for row in rows]


@pytest.mark.postgres
@pytest.mark.parametrize("admission_mode", ["review", "quarantine"])
def test_held_mode_source_run_waits_for_decide_then_reaches_default(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    admission_mode: str,
) -> None:
    """Issue #1752: a `quarantine`-mode run is held like a `review` run.

    Before the fix, finalize fenced only `review`, so a `quarantine` run
    finalized `accepted` with its rows on `quarantine:review`; `okg
    admission decide --accept` then answered `already_decided` and the rows
    never reached `default`. Both held modes must finalize `pending`, keep
    the rows off `default`, and apply them to `default` on accept.
    """
    from okg.substrate.admission import QUARANTINE_BRANCH
    from okg.substrate.admission_trace import (
        load_admission_traces,
        validate_admission_traces,
    )
    from okg.substrate.runtime import admission_workflows

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    registry_data = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry_data["sources"]["first"]["admission_policy"][
        "admission_mode"
    ] = admission_mode
    registry_path.write_text(yaml.safe_dump(registry_data), encoding="utf-8")
    _claim_layered_fixture_catalog(clean_runtime, dep)

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            publish=False,
        ),
    )
    assert outcome.any_failed is False, outcome.report

    held = _source_run_envelope(clean_runtime, "svc.first")
    assert held["admission_mode"] == admission_mode
    assert held["status"] == "pending", held
    assert "submitted" in held["events"]
    assert "accepted" not in held["events"]
    assert "svc:first" not in _branch_node_ids(clean_runtime, "default")
    assert "svc:first" in _branch_node_ids(clean_runtime, QUARANTINE_BRANCH)
    assert _branch_source_rows(clean_runtime, "default", "svc.first") == []
    held_rows = _branch_source_rows(clean_runtime, QUARANTINE_BRANCH, "svc.first")
    assert held_rows, "the held run wrote nothing to the quarantine branch"
    assert {row[2] for row in held_rows} == {held["run_id"]}

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )

    def decide() -> dict[str, object]:
        return admission_workflows._run_decide(
            deployment="svc",
            admission_id=str(held["admission_id"]),
            decision="accept",
            actor="operator",
            reason="issue-1752",
            verdict=None,
        )

    decided = decide()
    assert decided["status"] == "accepted", decided
    assert not decided.get("already_decided"), decided

    applied = _source_run_envelope(clean_runtime, "svc.first")
    assert applied["status"] == "accepted"
    # Every held row arrives on `default` exactly once, carrying the
    # original source and run as its lineage.
    on_default = _branch_source_rows(clean_runtime, "default", "svc.first")
    assert on_default == held_rows
    assert len({row[0] for row in on_default}) == len(on_default)
    assert "svc:first" in _branch_node_ids(clean_runtime, "default")

    # A second accept is a no-op: already decided, nothing duplicated.
    again = decide()
    assert again["status"] == "accepted", again
    assert again.get("already_decided") is True, again
    assert _branch_source_rows(clean_runtime, "default", "svc.first") == on_default

    # The recorded lifecycle still refines the admission model.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        traces = load_admission_traces(
            conn,
            deployment_name="svc",
            admission_id=str(held["admission_id"]),
        )
    result = validate_admission_traces(traces)
    assert result.ok is True, result


@pytest.mark.postgres
def test_quarantine_mode_source_run_reject_never_reaches_default(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #1752: rejecting a held quarantine-mode run keeps it off
    `default` and retracts its rows from the quarantine branch."""
    from okg.substrate.admission import QUARANTINE_BRANCH
    from okg.substrate.runtime import admission_workflows

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    registry_data = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry_data["sources"]["first"]["admission_policy"][
        "admission_mode"
    ] = "quarantine"
    registry_path.write_text(yaml.safe_dump(registry_data), encoding="utf-8")
    _claim_layered_fixture_catalog(clean_runtime, dep)

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            publish=False,
        ),
    )
    assert outcome.any_failed is False, outcome.report
    held = _source_run_envelope(clean_runtime, "svc.first")
    assert held["status"] == "pending", held
    assert _branch_source_rows(clean_runtime, QUARANTINE_BRANCH, "svc.first")

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    decided = admission_workflows._run_decide(
        deployment="svc",
        admission_id=str(held["admission_id"]),
        decision="reject",
        actor="operator",
        reason="issue-1752",
        verdict=None,
    )
    assert decided["status"] == "rejected", decided

    assert _source_run_envelope(clean_runtime, "svc.first")["status"] == (
        "rejected"
    )
    assert _branch_source_rows(clean_runtime, "default", "svc.first") == []
    assert _branch_source_rows(
        clean_runtime, QUARANTINE_BRANCH, "svc.first"
    ) == []


@pytest.mark.postgres
def test_authorized_shape_zero_output_rejects_disabled_profile(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    dep = _write_layered_fixture_deployment(tmp_path)
    registry = load_source_registry(dep / "source_registry.yaml")
    entry = registry["first"]
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        include={"first"},
        include_predecessors=False,
        publish=False,
        admission_observed_at=datetime(
            2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc,
        ),
    )
    run_context, workflow_ref, envelope_status = (
        ingest_service._prepare_source_admission(
            options=options,
            entry=entry,
            source_registry_name="first",
            run_mode="no_change",
            sync_scope=None,
        )
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE okg.producer_profiles SET status = 'disabled' "
            "WHERE deployment_name = 'svc' AND producer_id = %s",
            (entry.ownership_id,),
        )

    result = ingest_service._finalize_source_admission(
        options=options,
        admission_id=run_context.admission_id,
        source=entry.ownership_id,
        run_id=run_context.run_id,
        workflow_ref=workflow_ref,
        existing_status=envelope_status,
        admission_mode="fast_track",
        observed_output_count=0,
        completed_scope=True,
        source_health_status="ok",
        run_mode="no_change",
    )

    assert result.status == "rejected"
    assert result.reason == "producer_profile_not_writable"
    assert result.verdict["producer_policy"]["terminal_state_authorized"] is False


@pytest.mark.postgres
def test_unhealthy_nonzero_source_run_rejects_without_terminal_advance(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = _write_layered_fixture_deployment(tmp_path)
    registry = load_source_registry(dep / "source_registry.yaml")
    entry = registry["first"]
    observed_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        include={"first"},
        include_predecessors=False,
        publish=False,
        admission_observed_at=observed_at,
    )
    run_context, workflow_ref, envelope_status = (
        ingest_service._prepare_source_admission(
            options=options,
            entry=entry,
            source_registry_name="first",
            run_mode="scope_complete",
            sync_scope=None,
        )
    )
    retracted: list[dict[str, object]] = []
    monkeypatch.setattr(
        ingest_service,
        "_retract_chronos_run",
        lambda *args, **kwargs: retracted.append(
            {"args": args, "kwargs": kwargs}
        ),
    )

    result = ingest_service._finalize_source_admission(
        options=options,
        admission_id=run_context.admission_id,
        source=entry.ownership_id,
        run_id=run_context.run_id,
        workflow_ref=workflow_ref,
        existing_status=envelope_status,
        admission_mode="fast_track",
        observed_output_count=1,
        completed_scope=False,
        source_health_status="acquire_failed",
        run_mode="scope_complete",
        next_cursor={"run_id": "prior-run"},
        observed_at=observed_at,
    )

    assert result.status == "rejected"
    assert result.reason == "source_health_not_healthy"
    assert result.refs == []
    assert retracted
    assert result.verdict["producer_policy"] == {
        "status": "rejected",
        "reason": "failed_or_degraded",
        "source_health_status": "acquire_failed",
        "completed_scope": False,
        "run_mode": "scope_complete",
        "terminal_state_authorized": False,
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        watermark_count = conn.execute(
            "SELECT count(*) FROM okg.source_watermarks WHERE source = %s",
            (entry.ownership_id,),
        ).fetchone()[0]
    assert watermark_count == 0


@pytest.mark.postgres
def test_authorized_shape_zero_output_still_honors_nomos_denial(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.nomos as nomos

    dep = _write_layered_fixture_deployment(tmp_path)
    registry = load_source_registry(dep / "source_registry.yaml")
    entry = registry["first"]
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        include={"first"},
        include_predecessors=False,
        publish=False,
        admission_observed_at=datetime(
            2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc,
        ),
    )
    run_context, workflow_ref, envelope_status = (
        ingest_service._prepare_source_admission(
            options=options,
            entry=entry,
            source_registry_name="first",
            run_mode="no_change",
            sync_scope=None,
        )
    )

    class Denied:
        effect = "deny"
        reason_code = "nomos.test.zero_output_denied"

        def to_dict(self) -> dict[str, str]:
            return {
                "effect": self.effect,
                "reason_code": self.reason_code,
            }

    monkeypatch.setattr(
        nomos,
        "authorize_source_run_outcome_with_active_policy",
        lambda *_args, **_kwargs: types.SimpleNamespace(
            allowed=False, decision=Denied(),
        ),
    )
    result = ingest_service._finalize_source_admission(
        options=options,
        admission_id=run_context.admission_id,
        source=entry.ownership_id,
        run_id=run_context.run_id,
        workflow_ref=workflow_ref,
        existing_status=envelope_status,
        admission_mode="fast_track",
        observed_output_count=0,
        completed_scope=True,
        source_health_status="ok",
        run_mode="no_change",
    )

    assert result.status == "rejected"
    assert result.reason == "nomos.test.zero_output_denied"
    assert result.verdict["nomos"]["effect"] == "deny"
    assert result.verdict["producer_policy"]["terminal_state_authorized"] is False


@pytest.mark.postgres
def test_zero_output_authority_allows_under_audit_mode_outcome(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2123: the zero-output authority path gates on the outcome, not the effect.

    Under `nomos.runtime_enforcement: audit` the boundary is log-and-allow:
    it records the strict deny (`decision.effect == "deny"`, which is the
    audit trail) and returns `allowed` True. The sibling test above covers
    the deny case, which the old and new gates refuse identically, so only
    this arm separates them. Gating on the recorded effect here refused
    every audit-mode run whose source emitted zero facts while the
    fact-bearing finalizer succeeded.
    """
    import okg.substrate.nomos as nomos

    dep = _write_layered_fixture_deployment(tmp_path)
    registry = load_source_registry(dep / "source_registry.yaml")
    entry = registry["first"]
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        include={"first"},
        include_predecessors=False,
        publish=False,
        admission_observed_at=datetime(
            2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc,
        ),
    )
    run_context, workflow_ref, envelope_status = (
        ingest_service._prepare_source_admission(
            options=options,
            entry=entry,
            source_registry_name="first",
            run_mode="no_change",
            sync_scope=None,
        )
    )

    class AuditObservedDeny:
        """The recorded audit verdict an audit-mode outcome carries."""

        effect = "deny"
        reason_code = "nomos.default_deny.no_active_bundle"

        def to_dict(self) -> dict[str, str]:
            return {
                "effect": self.effect,
                "reason_code": self.reason_code,
            }

    monkeypatch.setattr(
        nomos,
        "authorize_source_run_outcome_with_active_policy",
        lambda *_args, **_kwargs: types.SimpleNamespace(
            allowed=True, decision=AuditObservedDeny(),
        ),
    )
    result = ingest_service._finalize_source_admission(
        options=options,
        admission_id=run_context.admission_id,
        source=entry.ownership_id,
        run_id=run_context.run_id,
        workflow_ref=workflow_ref,
        existing_status=envelope_status,
        admission_mode="fast_track",
        observed_output_count=0,
        completed_scope=True,
        source_health_status="ok",
        run_mode="no_change",
    )

    assert result.status != "rejected", result.verdict
    assert result.reason != AuditObservedDeny.reason_code


@pytest.mark.postgres
def test_finalize_accepts_idempotent_replay_zero_output(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#826: a healthy, completed-scope reconcile that emitted a non-empty
    record set but appended zero new facts (every fact deduped to a semantic
    no-op) is ACCEPTED as an idempotent replay, so the admission envelope
    transitions to `accepted` and the publish is not blocked. Authority is
    stubbed allowed here to isolate the structural verdict; the sibling tests
    exercise the real authority checks and the rejected-fact silent-failure
    guard."""
    dep = _write_layered_fixture_deployment(tmp_path)
    registry = load_source_registry(dep / "source_registry.yaml")
    entry = registry["first"]
    observed_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        include={"first"},
        include_predecessors=False,
        publish=False,
        admission_observed_at=observed_at,
    )
    run_context, workflow_ref, envelope_status = (
        ingest_service._prepare_source_admission(
            options=options,
            entry=entry,
            source_registry_name="first",
            run_mode="scope_complete",
            sync_scope=None,
        )
    )
    monkeypatch.setattr(
        ingest_service,
        "_evaluate_zero_output_source_authority",
        lambda *args, **kwargs: (
            True,
            None,
            {
                "source": entry.ownership_id,
                "producer_policy": {"status": "matched"},
            },
        ),
    )

    result = ingest_service._finalize_source_admission(
        options=options,
        admission_id=run_context.admission_id,
        source=entry.ownership_id,
        run_id=run_context.run_id,
        workflow_ref=workflow_ref,
        existing_status=envelope_status,
        admission_mode="fast_track",
        observed_output_count=0,
        completed_scope=True,
        source_health_status="ok",
        run_mode="scope_complete",
        next_cursor={
            "record_set": {"records": {"svc:first": {"node_id": "svc:first"}}},
        },
        observed_at=observed_at,
        rejected_fact_count=0,
    )

    assert result.status == "accepted"
    assert result.reason is None
    assert result.verdict["producer_policy"]["reason"] == "authoritative_replay"
    assert result.verdict["producer_policy"]["terminal_state_authorized"] is True

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM okg.admission_envelopes "
            "WHERE admission_id = %s::uuid",
            (run_context.admission_id,),
        )
        assert cur.fetchone() == ("accepted",)


@pytest.mark.postgres
def test_finalize_rejects_replay_shaped_run_with_rejected_facts(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#826 hardening: a run that looks like a replay (record set present, zero
    appended, healthy, complete) but rejected facts must NOT be accepted — that
    would advance the watermark past lost facts (silent data loss)."""
    dep = _write_layered_fixture_deployment(tmp_path)
    registry = load_source_registry(dep / "source_registry.yaml")
    entry = registry["first"]
    observed_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        include={"first"},
        include_predecessors=False,
        publish=False,
        admission_observed_at=observed_at,
    )
    run_context, workflow_ref, envelope_status = (
        ingest_service._prepare_source_admission(
            options=options,
            entry=entry,
            source_registry_name="first",
            run_mode="scope_complete",
            sync_scope=None,
        )
    )
    monkeypatch.setattr(
        ingest_service,
        "_evaluate_zero_output_source_authority",
        lambda *args, **kwargs: (
            True,
            None,
            {
                "source": entry.ownership_id,
                "producer_policy": {"status": "matched"},
            },
        ),
    )

    result = ingest_service._finalize_source_admission(
        options=options,
        admission_id=run_context.admission_id,
        source=entry.ownership_id,
        run_id=run_context.run_id,
        workflow_ref=workflow_ref,
        existing_status=envelope_status,
        admission_mode="fast_track",
        observed_output_count=0,
        completed_scope=True,
        source_health_status="ok",
        run_mode="scope_complete",
        next_cursor={
            "record_set": {"records": {"svc:first": {"node_id": "svc:first"}}},
        },
        observed_at=observed_at,
        rejected_fact_count=1,
    )

    assert result.status == "rejected"
    # Honest reason: names the rejection, never the generic authority default.
    assert result.reason == "facts_rejected"
    assert result.verdict["producer_policy"]["terminal_state_authorized"] is False


def _assert_zero_output_source_sync_uses_bounded_admission_path(
    *,
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    admission_mode: str,
) -> None:
    dep = _write_layered_fixture_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    registry_data = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry_data["sources"]["first"]["admission_policy"][
        "admission_mode"
    ] = admission_mode
    registry_path.write_text(yaml.safe_dump(registry_data), encoding="utf-8")
    registry = load_source_registry(dep / "source_registry.yaml")
    entry = registry["first"]
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        include={"first"},
        include_predecessors=False,
        publish=False,
        admission_observed_at=datetime(
            2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc,
        ),
    )

    run_context, workflow_ref, envelope_status = (
        ingest_service._prepare_source_admission(
            options=options,
            entry=entry,
            source_registry_name="first",
            run_mode="scope_complete",
            sync_scope=None,
        )
    )

    def fail_unbounded_fact_scan(*_args, **_kwargs):
        raise AssertionError(
            "zero-output finalize should not scan fact ranges",
        )

    monkeypatch.setattr(
        ingest_service,
        "attach_fact_range_refs_for_run",
        fail_unbounded_fact_scan,
    )
    authority_calls = {"profile": 0, "signature": 0}
    original_load_profile = ingest_service.load_profile_by_fingerprint
    original_evaluate_signature = ingest_service.evaluate_source_run_signature

    def load_profile(*args, **kwargs):
        authority_calls["profile"] += 1
        return original_load_profile(*args, **kwargs)

    def evaluate_signature(*args, **kwargs):
        authority_calls["signature"] += 1
        return original_evaluate_signature(*args, **kwargs)

    monkeypatch.setattr(ingest_service, "load_profile_by_fingerprint", load_profile)
    monkeypatch.setattr(
        ingest_service,
        "evaluate_source_run_signature",
        evaluate_signature,
    )

    result = ingest_service._finalize_source_admission(
        options=options,
        admission_id=run_context.admission_id,
        source=entry.ownership_id,
        run_id=run_context.run_id,
        workflow_ref=workflow_ref,
        existing_status=envelope_status,
        admission_mode=admission_mode,
        observed_output_count=0,
    )

    assert result.status == "rejected"
    assert result.refs == []
    assert result.verdict["producer_policy"]["reason"] == "partial"
    assert authority_calls == {"profile": 1, "signature": 1}

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, admission_mode, verdict
              FROM okg.admission_envelopes
             WHERE admission_id = %s::uuid
            """,
            (run_context.admission_id,),
        )
        status, stored_admission_mode, verdict = cur.fetchone()
        cur.execute(
            """
            SELECT count(*)
              FROM okg.admission_record_refs
             WHERE admission_id = %s::uuid
            """,
            (run_context.admission_id,),
        )
        ref_count = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT event_type
              FROM okg.admission_events
             WHERE admission_id = %s::uuid
             ORDER BY event_id
            """,
            (run_context.admission_id,),
        )
        event_types = [row[0] for row in cur.fetchall()]

    assert status == "rejected"
    assert stored_admission_mode == admission_mode
    assert verdict["fact_ref_count"] == 0
    assert verdict["producer_policy"]["record_counts"] == {
        "node": 0,
        "edge": 0,
    }
    assert ref_count == 0
    assert event_types == ["created", "rejected"]


@pytest.mark.postgres
def test_run_ingest_falls_back_when_event_has_no_usable_scope(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    dep = _write_layered_fixture_deployment(tmp_path)

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            source_events={
                "first": (
                    SourceSyncEvent(
                        deployment="svc",
                        source_name="first",
                        trigger_kind="poll",
                        revision_hint="stat-tree:1",
                    ),
                ),
            },
            run_mode="scope_incomplete",
            publish=False,
        ),
    )

    assert outcome.any_failed is False
    by_name = {
        result["name"]: result["run_mode"]
        for result in outcome.report["results"]
    }
    assert by_name == {"first": "scope_complete"}


@pytest.mark.postgres
def test_run_ingest_does_not_scope_reconcile_events(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    dep = _write_layered_fixture_deployment(tmp_path)
    (dep / "first.json").write_text(json.dumps({
        "nodes": [
            {
                "node_id": "svc:first-a",
                "subtype": "document",
                "attrs": {"text": "first a"},
                "source_record_id": {"path": "a.md"},
            },
            {
                "node_id": "svc:first-b",
                "subtype": "document",
                "attrs": {"text": "first b"},
                "source_record_id": {"path": "b.md"},
            },
        ],
        "edges": [],
    }))

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            source_events={
                "first": (
                    SourceSyncEvent(
                        deployment="svc",
                        source_name="first",
                        trigger_kind="reconcile",
                        scope_key="a.md",
                    ),
                ),
            },
            run_mode="scope_complete",
            publish=False,
        ),
    )

    assert outcome.any_failed is False
    first_result = outcome.report["results"][0]
    assert first_result["sync_scope"]["scope_keys"] == []
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        rows = store.query(
            """
            SELECT node_id
              FROM okg.graph_nodes
             WHERE source = 'svc.first'
             ORDER BY node_id
            """
        )
        assert [row["node_id"] for row in rows] == [
            "svc:first-a",
            "svc:first-b",
        ]


@pytest.mark.postgres
def test_run_ingest_uses_safe_run_mode_for_dependent_without_sync_policy(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    dep = _write_layered_fixture_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    data = yaml.safe_load(registry_path.read_text())
    data["sources"]["second"].pop("sync")
    registry_path.write_text(yaml.safe_dump(data))

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            dependent_fanout="direct",
            source_events={
                "first": (
                    SourceSyncEvent(
                        deployment="svc",
                        source_name="first",
                        trigger_kind="fs_event",
                        scope_key="first.json",
                    ),
                ),
            },
            run_mode="scope_incomplete",
            publish=False,
        ),
    )

    assert outcome.any_failed is False
    by_name = {
        result["name"]: result["run_mode"]
        for result in outcome.report["results"]
    }
    assert by_name == {
        "first": "scope_incomplete",
        "second": "scope_complete",
    }


def _blocking_consistency_gate() -> dict:
    return {
        "name": "fixture_blocking_gate",
        "type": "deterministic_sql",
        "check_id": "test.ingest.fixture-blocking-gate",
        "predicate_family": "fixture_block",
        "confidence_type": "deterministic_boolean",
        "candidate_sql": """
            SELECT
              'scope:fixture'::text AS input_scope,
              'block:fixture'::text AS blocking_rule_hash,
              jsonb_build_object('source', 'svc.first')
                AS participant_roles,
              'fixture_publish_block'::text AS predicate_key,
              'stable_source_anchor_v1'::text AS generation_policy,
              true AS emit_observation,
              'fixture_publish_block'::text AS normalized_predicate,
              jsonb_build_object('blocked', true) AS normalized_values,
              'high'::text AS severity,
              1.0::numeric AS confidence,
              '{}'::jsonb AS evidence
        """,
        "coverage_sql": """
            SELECT
              'scope:fixture'::text AS universe_key,
              'exact_postgres'::text AS coverage_basis,
              true AS coverage_complete,
              1::bigint AS eligible_input_count,
              1::bigint AS evaluator_invocation_count,
              1::bigint AS all_pairs_upper_bound
        """,
        "timeout_ms": 3000,
    }


def _source(
    ownership_id: str,
    *,
    predecessor_source: str | None = None,
    admission_policy: bool = True,
    enabled_by_default: bool | None = None,
) -> dict:
    entry = {
        "ownership_id": ownership_id,
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["id"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
    }
    # Leave the key absent when None so fixtures can exercise the
    # "unset reads as run-by-default" opt-out path, distinct from an
    # explicit `enabled_by_default: false`.
    if enabled_by_default is not None:
        entry["enabled_by_default"] = enabled_by_default
    if admission_policy:
        entry["admission_policy"] = {
            "producer_id": ownership_id,
            "producer_kind": "source",
            "trust_label": "fixture-trusted",
            "admission_mode": "fast_track",
            "output_scope_summary": {
                "summary": "fixture source facts",
            },
        }
    if predecessor_source is not None:
        entry["predecessor_source"] = predecessor_source
    return entry


def _load_registry(tmp_path: Path, sources: dict):
    path = tmp_path / "source_registry.yaml"
    path.write_text(yaml.safe_dump({"sources": sources}))
    return load_source_registry(path)


def test_prepare_source_admission_serializes_frozen_policy_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry_path = tmp_path / "source_registry.yaml"
    source = _source("svc.first")
    source["admission_policy"]["producer_id"] = "svc.producer-profile"
    source["admission_policy"]["output_scope_summary"]["details"] = {
        "reviewed_by": ["agent"],
    }
    registry_path.write_text(yaml.safe_dump({
        "admission_contract": "strict_v1",
        "sources": {
            "first": {
                **source,
                "module": "unused",
                "class": "Unused",
            },
        },
    }))
    entry = load_source_registry(registry_path)["first"]
    policy = entry.admission_policy
    assert policy is not None
    assert isinstance(
        policy.output_scope_summary,
        types.MappingProxyType,
    )

    connection = types.SimpleNamespace(transaction=lambda: nullcontext())
    pool = types.SimpleNamespace(
        connection=lambda: nullcontext(connection),
        # `pooled_connection` sizes the general-admission bound from the
        # pool's width, so a stub pool has to declare one.
        max_size=8,
        name="stub-pool",
    )
    profile = types.SimpleNamespace(
        authority_scope={},
        status="compat",
        evidence=lambda: {},
    )

    def ensure_serializable_envelope(_conn, **kwargs):
        authority = json.loads(json.dumps(kwargs["authority"]))
        check_summary = json.loads(json.dumps(kwargs["check_summary"]))
        assert kwargs["producer_id"] == "svc.producer-profile"
        assert kwargs["source"] == "svc.first"
        assert authority["producer_id"] == "svc.producer-profile"
        assert authority["ownership_id"] == "svc.first"
        assert authority["output_scope_summary"] == {
            "summary": "fixture source facts",
            "details": {"reviewed_by": ["agent"]},
        }
        assert check_summary["admission_policy"][
            "output_scope_summary"
        ]["details"] == {"reviewed_by": ["agent"]}
        return types.SimpleNamespace(
            admission_id=str(uuid.uuid4()),
            status="pending",
        )

    monkeypatch.setattr("okg.substrate.db.pool.get_pool", lambda _dsn: pool)
    monkeypatch.setattr(
        ingest_service,
        "_current_catalog_version_id",
        lambda _conn: 1,
    )
    monkeypatch.setattr(
        ingest_service,
        "materialize_producer_profile",
        lambda *_args, **_kwargs: profile,
    )
    monkeypatch.setattr(
        ingest_service,
        "ensure_envelope",
        ensure_serializable_envelope,
    )

    ingest_service._prepare_source_admission(
        options=IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=tmp_path,
            publish=False,
        ),
        entry=entry,
        source_registry_name="first",
        run_mode="scope_complete",
        sync_scope=None,
    )


def _stub_admission_finalize_pool():
    cursor = types.SimpleNamespace(
        execute=lambda *_args, **_kwargs: None,
        fetchone=lambda: (
            "svc.producer-profile",
            {
                "producer_id": "svc.producer-profile",
                "ownership_id": "svc.source",
            },
            {"producer_policy": {"profile_fingerprint": "profile-sha"}},
        ),
    )
    connection = types.SimpleNamespace(
        transaction=lambda: nullcontext(),
        cursor=lambda: nullcontext(cursor),
    )
    return types.SimpleNamespace(
        connection=lambda: nullcontext(connection),
        # `pooled_connection` sizes the general-admission bound from the
        # pool's width, so a stub pool has to declare one.
        max_size=8,
        name="stub-pool",
    )


def _stub_source_admission_authority(
    monkeypatch: pytest.MonkeyPatch,
    *,
    calls: list[dict[str, object]],
) -> None:
    import okg.substrate.nomos as nomos

    profile = types.SimpleNamespace(
        status="active",
        evidence=lambda: {},
    )

    class Denied:
        effect = "deny"
        reason_code = "nomos.test.branch_denied"

        def to_dict(self) -> dict[str, str]:
            return {
                "effect": self.effect,
                "reason_code": self.reason_code,
            }

    def authorize(_conn, **kwargs):
        calls.append(kwargs)
        # `_finalize_source_admission` gates on the enforcement outcome's
        # `allowed`, not on the recorded decision's effect (#2123).
        return types.SimpleNamespace(allowed=False, decision=Denied())

    monkeypatch.setattr(
        ingest_service,
        "load_profile_by_fingerprint",
        lambda *_args, **_kwargs: profile,
    )
    monkeypatch.setattr(
        ingest_service,
        "evaluate_source_run_signature",
        lambda *_args, **_kwargs: {
            "producer_policy": {"status": "matched"},
        },
    )
    monkeypatch.setattr(
        nomos,
        "authorize_source_run_outcome_with_active_policy",
        authorize,
    )


def test_finalize_source_admission_uses_bound_ownership_and_mode_branch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    _stub_source_admission_authority(monkeypatch, calls=calls)
    monkeypatch.setattr(
        "okg.substrate.db.pool.get_pool",
        lambda _dsn: _stub_admission_finalize_pool(),
    )
    monkeypatch.setattr(
        ingest_service,
        "attach_fact_range_refs_for_run",
        lambda *_args, **_kwargs: [types.SimpleNamespace()],
    )
    monkeypatch.setattr(
        ingest_service,
        "_retract_chronos_run",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        ingest_service,
        "transition_envelope",
        lambda *_args, **_kwargs: None,
    )

    result = ingest_service._finalize_source_admission(
        options=IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=tmp_path,
            publish=False,
        ),
        admission_id=str(uuid.uuid4()),
        source="svc.source",
        run_id="run-1",
        workflow_ref={},
        existing_status="pending",
        admission_mode="review",
        observed_output_count=1,
    )

    assert result.status == "rejected"
    assert result.reason == "nomos.test.branch_denied"
    assert len(calls) == 1
    assert calls[0]["producer_id"] == "svc.source"
    assert calls[0]["source"] == "svc.source"
    assert calls[0]["graph_branch"] == "quarantine:review"
    assert calls[0]["fact_ref_count"] == 1


def test_zero_output_finalize_uses_bound_ownership_and_mode_branch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    _stub_source_admission_authority(monkeypatch, calls=calls)
    monkeypatch.setattr(
        "okg.substrate.db.pool.get_pool",
        lambda _dsn: _stub_admission_finalize_pool(),
    )
    monkeypatch.setattr(
        ingest_service,
        "_staged_sweepable_source_admissions",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        ingest_service,
        "transition_envelope",
        lambda *_args, **_kwargs: None,
    )

    result = ingest_service._finalize_source_admission(
        options=IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=tmp_path,
            publish=False,
        ),
        admission_id=str(uuid.uuid4()),
        source="svc.source",
        run_id="run-0",
        workflow_ref={},
        existing_status="pending",
        admission_mode="quarantine",
        observed_output_count=0,
    )

    assert result.status == "rejected"
    assert result.reason == "nomos.test.branch_denied"
    assert len(calls) == 1
    assert calls[0]["producer_id"] == "svc.source"
    assert calls[0]["source"] == "svc.source"
    assert calls[0]["graph_branch"] == "quarantine:review"
    assert calls[0]["fact_ref_count"] == 0


@pytest.mark.parametrize(
    "authority",
    [
        {
            "producer_id": "svc.producer-profile",
            "ownership_id": "svc.other-source",
        },
        {
            "producer_id": "svc.other-profile",
            "ownership_id": "svc.source",
        },
        {"ownership_id": "svc.source"},
    ],
)
def test_source_authorization_identity_refuses_inconsistent_binding(
    authority: dict[str, str],
) -> None:
    from okg.substrate.nomos.store import (
        resolve_source_authorization_identity,
    )

    with pytest.raises(ValueError, match="identity binding"):
        resolve_source_authorization_identity(
            producer_id="svc.producer-profile",
            source="svc.source",
            authority=authority,
        )


@pytest.mark.parametrize(
    ("bootstrap_semantics", "expected_profile"),
    [
        (None, None),
        ("fixture.bootstrap.cursor.v1", "discovery_crawl"),
    ],
)
def test_completed_scope_deletion_service_threads_registry_policy_to_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bootstrap_semantics: str | None,
    expected_profile: str | None,
) -> None:
    dep = tmp_path / "deployments" / "svc"
    dep.mkdir(parents=True)
    (dep / "deployment.yaml").write_text("name: svc\n", encoding="utf-8")
    entry = _load_registry(tmp_path, {
        "crawl": {
            **_source("svc.crawl"),
            "module": "tests.completed_scope_source",
            "class": "CompletedScopeSource",
        },
    })["crawl"]
    captured: dict[str, object] = {}

    class CompletedScopeSource:
        profile = "discovery_crawl"

    if bootstrap_semantics is not None:
        CompletedScopeSource.bootstrap_cursor_semantics = bootstrap_semantics

    class CapturingRunner:
        def __init__(self, _dsn: str, **kwargs: object) -> None:
            captured.update(kwargs)

        def run(self, _adapter: object, **_kwargs: object) -> object:
            from okg.substrate.ingest import runner as ingest_runner

            accumulator = ingest_runner.GraphWriteSetAccumulator(
                max_entries=ingest_runner.MAX_BOUNDED_GRAPH_WRITE_SET_ENTRIES
            )
            accumulator.observe(ingest_runner.GraphWriteSet(
                nodes_by_subtype={"document": [
                    f"n{index}"
                    for index in range(
                        ingest_runner.MAX_BOUNDED_GRAPH_WRITE_SET_ENTRIES + 1
                    )
                ]}
            ))
            assert accumulator.available is False
            now = datetime.now(timezone.utc)
            return ingest_runner.RunResult(
                run_id=str(uuid.uuid4()),
                source_name="svc.crawl",
                mode="scope_complete",
                started_at=now,
                finished_at=now,
                nodes_appended=(
                    ingest_runner.MAX_BOUNDED_GRAPH_WRITE_SET_ENTRIES + 1
                ),
                edges_appended=0,
                retractions_node=0,
                retractions_edge=0,
                completed_scope=True,
                next_cursor={},
                graph_nodes_by_subtype={
                    subtype: tuple(node_ids)
                    for subtype, node_ids in accumulator.nodes_by_subtype.items()
                },
                graph_node_attr_changes_by_subtype={},
                graph_edges_by_type={},
                graph_edge_table_available=accumulator.edge_table_available,
            )

    run_context = ingest_service.SourceRunContext(
        run_id=str(uuid.uuid4()),
        observed_at=datetime.now(timezone.utc),
        admission_id=str(uuid.uuid4()),
    )
    monkeypatch.setattr(ingest_service, "SourceRunner", CapturingRunner)
    monkeypatch.setattr(
        ingest_service,
        "import_source_adapter_class",
        lambda _entry: CompletedScopeSource,
    )
    monkeypatch.setattr(
        ingest_service,
        "_prepare_source_admission",
        lambda **_kwargs: (run_context, {}, "pending"),
    )
    monkeypatch.setattr(
        ingest_service,
        "_snapshot_source_state",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        ingest_service,
        "resolve_deployment_source_pii_posture",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        ingest_service,
        "runtime_enforce_mode_dsn_keys",
        lambda _path: set(),
    )
    monkeypatch.setattr(
        ingest_service,
        "_finalize_source_admission",
        lambda **_kwargs: ingest_service._AdmissionFinalizeResult(),
    )

    result = ingest_service._run_source(
        IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=dep,
            publish=False,
        ),
        entry,
        "crawl",
    )

    assert result["status"] == "OK", result
    assert captured["deletion_semantics"] == "missing_from_completed_scope"
    assert captured["executable_profile"] == expected_profile
    assert captured["registry_name"] == (
        "crawl" if expected_profile is not None else None
    )
    assert captured["ownership_id"] == (
        "svc.crawl" if expected_profile is not None else None
    )
    assert isinstance(captured["registry_binding_fingerprint"], str)
    assert result["admission_status"] == "accepted"
    assert result["nodes_appended"] == 10_001
    assert "graph_write_set" not in result


def test_published_bootstrap_retry_skips_terminal_finalization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep = tmp_path / "deployments" / "svc"
    dep.mkdir(parents=True)
    (dep / "deployment.yaml").write_text("name: svc\n", encoding="utf-8")
    entry = _load_registry(tmp_path, {
        "crawl": {
            **_source("svc.crawl"),
            "module": "tests.published_bootstrap_retry_source",
            "class": "PublishedBootstrapRetrySource",
        },
    })["crawl"]
    released = False

    class PublishedBootstrapRetrySource:
        profile = "discovery_crawl"
        bootstrap_cursor_semantics = "published-retry.cursor.v1"
        last_run_stats: dict[str, object] = {}

    class ReplayRunner:
        def __init__(self, _dsn: str, **_kwargs: object) -> None:
            pass

        @staticmethod
        def _adapter_probe_short_circuit_safe(_adapter: object) -> bool:
            return True

        def run(self, _adapter: object, *, run_context, **_kwargs: object):
            return types.SimpleNamespace(
                run_id=run_context.run_id,
                nodes_appended=0,
                edges_appended=0,
                retractions_node=0,
                retractions_edge=0,
                completed_scope=True,
                source_health=None,
                next_cursor={"cursor": "already-durable"},
                terminal_progress={},
                clear_replay_required=False,
                replay_target=None,
                rejected_fact_count=0,
                timings_ms={},
                idempotent_replay=True,
            )

        def release_deferred_bootstrap_lock(self) -> None:
            nonlocal released
            released = True

    run_context = ingest_service.SourceRunContext(
        run_id=str(uuid.uuid4()),
        observed_at=datetime.now(timezone.utc),
        admission_id=str(uuid.uuid4()),
        idempotency_key="published-retry",
    )
    monkeypatch.setattr(ingest_service, "SourceRunner", ReplayRunner)
    monkeypatch.setattr(
        ingest_service,
        "import_source_adapter_class",
        lambda _entry: PublishedBootstrapRetrySource,
    )
    monkeypatch.setattr(
        ingest_service,
        "_prepare_source_admission",
        lambda **_kwargs: (run_context, {}, "published"),
    )
    monkeypatch.setattr(
        ingest_service,
        "_snapshot_source_state",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        ingest_service,
        "resolve_deployment_source_pii_posture",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        ingest_service,
        "runtime_enforce_mode_dsn_keys",
        lambda _path: set(),
    )
    monkeypatch.setattr(
        ingest_service,
        "_finalize_source_admission",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("idempotent retry re-entered finalization")
        ),
    )

    result = ingest_service._run_source(
        IngestOptions(
            dsn="postgres://unused",
            deployment="svc",
            deployment_dir=dep,
            publish=False,
        ),
        entry,
        "crawl",
    )

    assert result["status"] == "OK", result
    assert result["admission_status"] == "published"
    assert result["admission_verdict"]["idempotent_replay"] is True
    assert released is True


@pytest.mark.postgres
def test_completed_scope_deletion_legacy_service_reconcile_retracts_missing(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact, SourceRun

    module_name = "okg_test_completed_scope_stream_source"
    module = types.ModuleType(module_name)

    class CompletedScopeStreamSource:
        name = "completed-scope-stream"
        profile = "discovery_crawl"
        runs = [
            ["svc:a", "svc:b"],
            ["svc:a"],
        ]

        def run(
            self,
            run_id: str,
            *,
            mode: str = "cursor",
            **_kwargs: object,
        ) -> SourceRun:
            del run_id
            node_ids = type(self).runs.pop(0)
            return SourceRun(
                facts=[
                    NodeFact(
                        node_id=node_id,
                        subtype="document",
                        attrs={"text": node_id},
                        source_record_id={"id": node_id},
                        source_revision={"rev": "stable"},
                    )
                    for node_id in node_ids
                ],
                completed_scope=True,
                next_cursor={},
                run_mode=mode,
            )

    module.CompletedScopeStreamSource = CompletedScopeStreamSource
    monkeypatch.setitem(sys.modules, module_name, module)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_stub_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    source = registry["sources"]["first"]
    source["module"] = module_name
    source["class"] = "CompletedScopeStreamSource"
    policy = source["admission_policy"]
    policy["allowed_record_kinds"] = ["node"]
    policy["output_signature"] = {
        "nodes": [{
            "subtype": "document",
            "clause_id": "allow_document",
        }],
    }
    policy["policy_clauses"] = [{
        "id": "allow_document",
        "effect": "allow",
        "record_kind": "node",
        "subtype": "document",
    }]
    registry_path.write_text(yaml.safe_dump(registry))
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        include={"first"},
        include_predecessors=False,
        include_dependents=False,
        publish=False,
    )

    first = run_ingest(options)
    second = run_ingest(replace(options, reset_cursor=True))

    assert first.any_failed is False, first.report
    assert second.any_failed is False, second.report
    assert second.report["results"][0]["retract_n"] == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(conn).checkout().query(
            """
            SELECT node_id
              FROM okg.graph_nodes
             WHERE source = :source
             ORDER BY node_id
            """,
            {"source": "svc.first"},
        )
    assert [row["node_id"] for row in rows] == ["svc:a"]


def _fixture_source_privacy_policy() -> dict[str, object]:
    return {
        "sensitivity": "local_private",
        "data_classification": "private",
        "credential_ref_policy": "none",
        "live_call_allowed": False,
        "exportability": "no_export",
        "retention": "normal",
        "provenance": "local_filesystem",
        "privacy_obligations": ["audit"],
        "store_raw": True,
    }


def _write_layered_fixture_deployment(tmp_path: Path) -> Path:
    dep = tmp_path / "deployments" / "svc"
    dep.mkdir(parents=True)
    (dep / "schemas").mkdir()
    (dep / "schemas" / "fixture.yaml").write_text(
        "id: https://example.invalid/okg/fixture\n"
        "name: ingest_fixture\n"
        "classes:\n"
        "  Document:\n"
        "    attributes:\n"
        "      text: {}\n"
        "    annotations:\n"
        "      archetype: artifact\n"
        "      transient: 'false'\n"
    )
    first_fixture = dep / "first.json"
    second_fixture = dep / "second.json"
    first_fixture.write_text(json.dumps({
        "nodes": [
            {
                "node_id": "svc:first",
                "subtype": "document",
                "attrs": {"text": "first layer"},
                "source_record_id": {"path": "first.json"},
            },
        ],
        "edges": [],
    }))
    second_fixture.write_text(json.dumps({
        "nodes": [
            {
                "node_id": "svc:second",
                "subtype": "document",
                "attrs": {"text": "second layer"},
                "source_record_id": {"path": "first.json"},
            },
        ],
        "edges": [],
    }))
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "svc",
        "description": "ingest service fixture",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": [],
        "nomos": {
            "deployment_posture": {
                "deployment_class": "dogfood_workspace",
                "read_principals": ["human_operator", "agent"],
                "write_principals": ["human_operator", "source_adapter"],
                "mcp_write_default": "review",
                "export_policy": "internal_only",
                "secret_handling": "secret_ref_required",
                "live_call_default": "review",
                "allowed_source_sensitivities": ["local_private"],
                "allowed_data_classifications": ["private"],
                "allowed_exportability": ["no_export"],
                "privacy_obligations": ["audit"],
                "readiness_profiles": ["dogfood"],
            },
        },
    }))
    (dep / "source_registry.yaml").write_text(yaml.safe_dump({
        "sources": {
            "first": {
                **_source("svc.first"),
                "module": "okg.substrate.library.sources.bootstrap_fixture",
                "class": "BootstrapFixtureSource",
                "params": {"fixture_path": str(first_fixture)},
                "source_policy": _fixture_source_privacy_policy(),
                "sync": {
                    "triggers": ["fs_event", "manual", "reconcile"],
                    "default_event_mode": "scope_incomplete",
                    "reconcile_mode": "scope_complete",
                    "scope": {
                        "accepts": ["path"],
                        "unsupported_fallback": "scope_complete",
                    },
                },
            },
            "second": {
                **_source("svc.second", predecessor_source="first"),
                "module": "okg.substrate.library.sources.bootstrap_fixture",
                "class": "BootstrapFixtureSource",
                "params": {"fixture_path": str(second_fixture)},
                "source_policy": _fixture_source_privacy_policy(),
                "sync": {
                    "triggers": ["fs_event", "manual", "reconcile"],
                    "default_event_mode": "scope_complete",
                    "reconcile_mode": "scope_complete",
                },
            },
        },
    }))
    _commit_layered_fixture_deployment(dep, message="initial fixture")
    return dep


def _commit_layered_fixture_deployment(dep: Path, *, message: str) -> None:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "OKG fixture",
        "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
        "GIT_COMMITTER_NAME": "OKG fixture",
        "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
    }
    if not (dep / ".git").exists():
        subprocess.run(
            ["git", "init", "-q", "-b", "main"],
            cwd=dep,
            env=env,
            check=True,
        )
    subprocess.run(["git", "add", "-A"], cwd=dep, env=env, check=True)
    staged = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        cwd=dep,
        env=env,
        check=False,
    )
    if staged.returncode:
        subprocess.run(
            ["git", "commit", "-q", "-m", message],
            cwd=dep,
            env=env,
            check=True,
        )


def _claim_layered_fixture_catalog(dsn: str, dep: Path) -> CatalogIdentity:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        claim_catalog_ownership,
    )

    _commit_layered_fixture_deployment(dep, message="fixture catalog update")
    composed = compose_catalog(dep)
    identity = catalog_identity_for(
        composed=composed,
        deployment_dir=dep,
        command="test layered ingest fixture",
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        claim_catalog_ownership(conn, identity=identity, force=True)
        apply_composed_catalog(conn, composed, identity=identity)
    return identity


def _write_stub_deployment(
    tmp_path: Path,
    *,
    enrichers: list[dict] | None = None,
) -> Path:
    dep = tmp_path / "deployments" / "svc"
    dep.mkdir(parents=True)
    (dep / "schemas").mkdir()
    deployment = {
        "name": "svc",
        "description": "ingest service stub",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": [],
        "nomos": {
            "deployment_posture": {
                "deployment_class": "dogfood_workspace",
                "read_principals": ["human_operator", "agent"],
                "write_principals": ["human_operator", "source_adapter"],
                "mcp_write_default": "review",
                "export_policy": "internal_only",
                "secret_handling": "secret_ref_required",
                "live_call_default": "review",
                "allowed_source_sensitivities": ["local_private"],
                "allowed_data_classifications": ["private"],
                "allowed_exportability": ["no_export"],
                "privacy_obligations": ["audit"],
                "readiness_profiles": ["dogfood"],
            },
        },
    }
    if enrichers is not None:
        deployment["enrichers"] = enrichers
    (dep / "deployment.yaml").write_text(yaml.safe_dump(deployment))
    (dep / "source_registry.yaml").write_text(yaml.safe_dump({
        "sources": {
            "first": {
                **_source("svc.first"),
                "module": "unused",
                "class": "Unused",
                "source_policy": _fixture_source_privacy_policy(),
            },
        },
    }))
    return dep


def _source_result(
    name: str,
    *,
    nodes_appended: int = 0,
    edges_appended: int = 0,
    retract_n: int = 0,
    retract_e: int = 0,
    graph_write_set: dict[str, object] | None = None,
    source_delta_batch: dict[str, object] | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "name": name,
        "status": "OK",
        "nodes_appended": nodes_appended,
        "edges_appended": edges_appended,
        "retract_n": retract_n,
        "retract_e": retract_e,
        "run_mode": "scope_complete",
        "elapsed_s": 0.0,
        "sync_scope": {},
        "source_stats": {},
    }
    if graph_write_set is not None:
        result["graph_write_set"] = graph_write_set
    if source_delta_batch is not None:
        result["source_delta_batch"] = source_delta_batch
    return result


def _insert_published_generation(
    dsn: str,
    *,
    deployment: str,
) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(catalog_version_id)
              FROM okg.catalog_versions
             WHERE status = 'applied'
            """
        )
        catalog_version_id = cur.fetchone()[0]
        cur.execute("SELECT COALESCE(max(fact_id), 0) FROM okg.node_facts")
        fact_watermark = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT COALESCE(max(fact_id), 0) FROM okg.edge_facts")
        edge_fact_watermark = int(cur.fetchone()[0] or 0)
        cur.execute(
            """
            INSERT INTO okg.graph_generations
              (deployment_name, status, catalog_version_id,
               correlation_id, published_at, fact_watermark,
               edge_fact_watermark)
            VALUES (%s, 'published', %s, %s, now(), %s, %s)
            RETURNING generation_id
            """,
            (
                deployment,
                catalog_version_id,
                f"test:{deployment}:{uuid.uuid4().hex}",
                fact_watermark,
                edge_fact_watermark,
            ),
        )
        return int(cur.fetchone()[0])


def _checkpoint_chronos_current_branch(
    dsn: str,
    *,
    deployment: str,
) -> str:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(catalog_version_id)
              FROM okg.catalog_versions
             WHERE status = 'applied'
            """
        )
        catalog_version_id = cur.fetchone()[0]
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(
            [NodeFact("svc:seed", "document", {}, {}, {})],
            source=f"{deployment}.seed",
        )
        return store.checkpoint_generation(
            None,
            deployment_name=deployment,
            catalog_version_id=catalog_version_id,
            status="published",
        )


# --- close-refused-publish-redrive-gap (#828): unpublished dedupe basis -----


def _insert_staged_source_admission(
    dsn: str,
    *,
    deployment: str,
    source: str,
    status: str = "accepted",
) -> str:
    """A prior source admission with attached refs simulating appended work
    whose publish was refused (the #828 staged state)."""
    admission_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.admission_envelopes
              (admission_id, deployment_name, producer_id, producer_kind,
               intent, admission_mode, status, source, run_id,
               idempotency_key)
            VALUES
              (%s::uuid, %s, %s, 'source', 'append_source_facts',
               'fast_track', %s, %s, %s::uuid, %s)
            """,
            (
                admission_id, deployment, source, status, source, run_id,
                f"staged:{admission_id}",
            ),
        )
        cur.execute(
            """
            INSERT INTO okg.admission_record_refs
              (admission_id, ref_key, ref_kind, source, run_id, payload)
            VALUES
              (%s::uuid, %s, 'payload', %s, %s::uuid, %s::jsonb)
            """,
            (
                admission_id,
                f"chronos_graph_run:{source}:{run_id}",
                source,
                run_id,
                json.dumps({
                    "kind": "chronos_graph_run",
                    "source": source,
                    "run_id": run_id,
                }),
            ),
        )
    return admission_id


@pytest.mark.postgres
def test_zero_output_rejects_when_prior_admission_unpublished(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """close-refused-publish-redrive-gap (#828): a replay-shaped zero-output
    run is REJECTED while the same source has staged appended work that never
    reached a published generation -- accepting would report SUCCESS while
    the stranded facts never re-emit."""
    dep = _write_layered_fixture_deployment(tmp_path)
    registry = load_source_registry(dep / "source_registry.yaml")
    entry = registry["first"]
    observed_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        include={"first"},
        include_predecessors=False,
        publish=False,
        admission_observed_at=observed_at,
    )
    staged_id = _insert_staged_source_admission(
        clean_runtime, deployment="svc", source=entry.ownership_id,
    )
    run_context, workflow_ref, envelope_status = (
        ingest_service._prepare_source_admission(
            options=options,
            entry=entry,
            source_registry_name="first",
            run_mode="scope_complete",
            sync_scope=None,
        )
    )
    monkeypatch.setattr(
        ingest_service,
        "_evaluate_zero_output_source_authority",
        lambda *args, **kwargs: (
            True,
            None,
            {
                "source": entry.ownership_id,
                "producer_policy": {"status": "matched"},
            },
        ),
    )

    result = ingest_service._finalize_source_admission(
        options=options,
        admission_id=run_context.admission_id,
        source=entry.ownership_id,
        run_id=run_context.run_id,
        workflow_ref=workflow_ref,
        existing_status=envelope_status,
        admission_mode="fast_track",
        observed_output_count=0,
        completed_scope=True,
        source_health_status="ok",
        run_mode="scope_complete",
        next_cursor={
            "record_set": {"records": {"svc:first": {"node_id": "svc:first"}}},
        },
        observed_at=observed_at,
        rejected_fact_count=0,
    )

    assert result.status == "rejected"
    assert result.reason == "prior_emission_unpublished"
    policy = result.verdict["producer_policy"]
    assert policy["reason"] == "prior_emission_unpublished"
    assert policy["unpublished_prior_admissions_sample"] == [staged_id]
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM okg.admission_envelopes "
            "WHERE admission_id = %s::uuid",
            (run_context.admission_id,),
        )
        assert cur.fetchone() == ("rejected",)
        # The staged admission itself is untouched by the rejection.
        cur.execute(
            "SELECT status FROM okg.admission_envelopes "
            "WHERE admission_id = %s::uuid",
            (staged_id,),
        )
        assert cur.fetchone() == ("accepted",)


@pytest.mark.postgres
def test_zero_output_accepts_after_staged_admission_published(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Self-heal: once the staged admission is swept to published, the next
    zero-output re-sync is accepted as an idempotent replay again."""
    dep = _write_layered_fixture_deployment(tmp_path)
    registry = load_source_registry(dep / "source_registry.yaml")
    entry = registry["first"]
    observed_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        include={"first"},
        include_predecessors=False,
        publish=False,
        admission_observed_at=observed_at,
    )
    _insert_staged_source_admission(
        clean_runtime,
        deployment="svc",
        source=entry.ownership_id,
        status="published",
    )
    run_context, workflow_ref, envelope_status = (
        ingest_service._prepare_source_admission(
            options=options,
            entry=entry,
            source_registry_name="first",
            run_mode="scope_complete",
            sync_scope=None,
        )
    )
    monkeypatch.setattr(
        ingest_service,
        "_evaluate_zero_output_source_authority",
        lambda *args, **kwargs: (
            True,
            None,
            {
                "source": entry.ownership_id,
                "producer_policy": {"status": "matched"},
            },
        ),
    )

    result = ingest_service._finalize_source_admission(
        options=options,
        admission_id=run_context.admission_id,
        source=entry.ownership_id,
        run_id=run_context.run_id,
        workflow_ref=workflow_ref,
        existing_status=envelope_status,
        admission_mode="fast_track",
        observed_output_count=0,
        completed_scope=True,
        source_health_status="ok",
        run_mode="scope_complete",
        next_cursor={
            "record_set": {"records": {"svc:first": {"node_id": "svc:first"}}},
        },
        observed_at=observed_at,
        rejected_fact_count=0,
    )

    assert result.status == "accepted"
    assert result.verdict["producer_policy"]["reason"] == "authoritative_replay"


@pytest.mark.postgres
def test_pending_work_probe_sees_accepted_source_admission(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#828: an accepted source envelope with attached refs (appended work
    whose publish was refused) counts as pending publisher work, so the
    ingest publish is not skipped. A refs-less accepted source envelope
    (routine zero-output acceptance) does NOT count."""
    dep = _write_stub_deployment(tmp_path)
    _checkpoint_chronos_current_branch(clean_runtime, deployment="svc")
    monkeypatch.setattr(
        "okg.substrate.publisher.deployment_stack."
        "build_deployment_publisher_stack",
        lambda **_kwargs: types.SimpleNamespace(
            materializers=[object()],
            consistency_gates=[],
            post_publish_consistency_checks=[],
            lenses=[],
            enrichers=[],
        ),
    )
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
    )
    # Refs-less accepted source envelope: not sweepable, must not count.
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.admission_envelopes
              (deployment_name, producer_id, producer_kind, intent,
               admission_mode, status, source, idempotency_key)
            VALUES
              ('svc', 'svc.zero', 'source', 'append_source_facts',
               'fast_track', 'accepted', 'svc.zero', %s)
            """,
            (f"zero:{uuid.uuid4().hex}",),
        )
    assert ingest_service._deployment_has_pending_publisher_work(
        options,
    ) is False
    assert ingest_service._staged_source_drive_needed(options) is False

    _insert_staged_source_admission(
        clean_runtime, deployment="svc", source="svc.staged",
    )

    assert ingest_service._deployment_has_pending_publisher_work(
        options,
    ) is True
    assert ingest_service._staged_source_drive_needed(options) is True


@pytest.mark.postgres
def test_rejected_zero_output_run_still_drives_staged_publish(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """#828 end-to-end: the same run publishes staged work before re-sync."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        include={"first"},
        include_predecessors=False,
    )

    first = run_ingest(options)
    assert first.any_failed is False, json.dumps(first.report, default=str)
    assert [p["status"] for p in first.report["publishes"]] == ["published"]

    staged_id = _insert_staged_source_admission(
        clean_runtime, deployment="svc", source="svc.first",
    )

    second = run_ingest(options)
    # The command first publishes the staged basis, then the re-sync can be
    # accepted as an authoritative zero-output replay in the same run.
    assert second.any_failed is False
    (second_result,) = second.report["results"]
    assert second_result["status"] == "OK"
    assert second_result["nodes_appended"] == 0
    # The same run drove a real pre-source publish, which swept the staged
    # admission before the source identity check.
    publish_statuses = [
        p.get("status") for p in second.report["publishes"]
        if not p.get("publish_skipped")
    ]
    assert publish_statuses == ["published"], second.report["publishes"]
    assert second.report["publishes"][0]["label"] == (
        "staged-source-redrive"
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM okg.admission_envelopes "
            "WHERE admission_id = %s::uuid",
            (staged_id,),
        )
        assert cur.fetchone() == ("published",)

    third = run_ingest(options)
    assert third.any_failed is False, json.dumps(third.report, default=str)


@pytest.mark.postgres
def test_staged_redrive_precedes_incomplete_multi_source_checkpoint(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """#828 staged work publishes before the current run's own publish.

    Since #3342 the current run's partial failure publishes too, marked
    incomplete (it was blocked under #987).
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
    )
    baseline = run_ingest(options)
    assert baseline.any_failed is False

    staged_id = _insert_staged_source_admission(
        clean_runtime,
        deployment="svc",
        source="svc.first",
    )
    first_fixture = dep / "first.json"
    first_payload = json.loads(first_fixture.read_text())
    first_payload["nodes"].append({
        "node_id": "svc:first:new",
        "subtype": "document",
        "attrs": {"text": "current successful peer output"},
        "source_record_id": {"path": "first.json", "record": "new"},
    })
    first_fixture.write_text(json.dumps(first_payload))
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["sources"]["second"]["params"]["fixture_path"] = str(
        dep / "missing-second.json",
    )
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(clean_runtime, dep)

    incomplete = run_ingest(replace(options, reset_cursor=True))

    assert incomplete.any_failed is True
    assert [publish["status"] for publish in incomplete.report["publishes"]] == [
        "published",
        "published",
    ]
    redrive, marked = incomplete.report["publishes"]
    assert redrive["label"] == "staged-source-redrive"
    assert not marked.get("blocked_reason")
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        latest_published = store.latest_generation(
            deployment_name="svc",
            status="published",
        )
        cur.execute(
            "SELECT admission_id::text, status FROM okg.admission_envelopes "
            "WHERE admission_id = %s::uuid OR target_generation_ref = %s "
            "ORDER BY created_at",
            (staged_id, marked["generation_id"]),
        )
        statuses = cur.fetchall()

    assert latest_published is not None
    assert latest_published.checkpoint_id == marked["generation_id"]
    assert latest_published.metadata["ingest_completeness"]["status"] == (
        "incomplete"
    )
    assert statuses[0] == (staged_id, "published")
    assert statuses[1:]
    assert all(status == "published" for _, status in statuses[1:])


def _project_node_outside_sources(dsn: str, node_id: str) -> None:
    """Write one node to the branch head the way the views registry does.

    `okg provision` runs `catalog load` before ingest. Once the deployment
    has a published generation, that step upserts `view_definition` nodes
    straight into the branch head (`apply_views_registry`), outside any
    source run, so no source's graph write-set names them.
    """
    from okg.substrate.fact_application import apply_bounded_fact_batch
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            )
            (catalog_version_id,) = cur.fetchone()
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        apply_bounded_fact_batch(
            conn,
            store,
            [
                NodeFact(
                    node_id,
                    "document",
                    {"text": "projected by catalog load"},
                    {"view_def_id": node_id},
                    {"view_def_hash": "fixture"},
                ),
            ],
            deployment_name="svc",
            producer_id="catalog:views_registry",
            producer_kind="system",
            intent="views_registry_projection",
            catalog_version_id=int(catalog_version_id),
            authority_scope={
                "view_definition_ids": [node_id],
                "completed_scope": True,
            },
            source="catalog:views_registry",
            strict_endpoints=True,
        )


def _second_run_with_one_source_node(
    tmp_path: Path,
    dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    before_sources=None,
    during_sources=None,
) -> dict:
    """Publish a baseline, then re-run ingest with one new source node.

    ``before_sources`` runs after the baseline and before the second run;
    ``during_sources`` runs right after the second run's first source
    finishes, i.e. while the run is still going and before its publish.
    Returns the second run's single publish entry.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(dsn, dep)
    options = IngestOptions(dsn=dsn, deployment="svc", deployment_dir=dep)
    (baseline,) = run_ingest(options).report["publishes"]
    assert baseline["status"] == "published", baseline

    if before_sources is not None:
        before_sources()
    _add_source_node(dep)
    if during_sources is not None:
        real_run_source = ingest_service._run_source
        fired: list[bool] = []

        def run_source_then_write(*args, **kwargs):
            result = real_run_source(*args, **kwargs)
            if not fired:
                fired.append(True)
                during_sources()
            return result

        monkeypatch.setattr(ingest_service, "_run_source", run_source_then_write)
    (publish,) = run_ingest(replace(options, reset_cursor=True)).report[
        "publishes"
    ]
    assert publish["status"] == "published", publish
    return publish


def _add_source_node(dep: Path) -> None:
    """Give the `first` fixture source one new node and commit it."""
    first_fixture = dep / "first.json"
    payload = json.loads(first_fixture.read_text())
    payload["nodes"].append({
        "node_id": "svc:added",
        "subtype": "document",
        "attrs": {"text": "added by the second run"},
        "source_record_id": {"path": "first.json", "record": "added"},
    })
    first_fixture.write_text(json.dumps(payload))
    _commit_layered_fixture_deployment(dep, message="add a source node")


def _assert_published_count_matches_visible(
    dsn: str, publish: dict, *expected: str,
) -> None:
    """Stamped node count == visible nodes, and the attribute view has them."""
    from okg.substrate.graph_store import ChronosGraphStore

    published_ids = _published_node_ids(dsn, publish["generation_id"])
    assert set(expected) <= set(published_ids)
    with psycopg.connect(dsn, autocommit=True) as conn:
        latest = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).latest_generation(deployment_name="svc", status="published")
        with conn.cursor() as cur:
            # The attribute read model is what the integrity receipt counts
            # against the checkpoint.
            cur.execute(
                "SELECT count(*) FROM okg.attribute_effective_view_intervals "
                "WHERE target_kind = 'node' AND target_id = ANY(%s) "
                "AND to_ord = 9223372036854775807",
                (list(expected),),
            )
            (attribute_rows,) = cur.fetchone()
    assert latest.checkpoint_id == publish["generation_id"]
    assert int(latest.metadata["node_count"]) == len(published_ids)
    assert attribute_rows == len(expected)
    # The hint decision is kept with the generation, not only in the report.
    assert latest.metadata["dirty_scope_hint"] == publish["dirty_scope_hint"]


@pytest.mark.postgres
def test_branch_write_made_before_sources_is_counted_by_the_publish(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A node written to the branch head before the sources run is counted.

    Reproduces the okg-workspace refusal on a fresh database (lane H,
    2026-09-28): provision `git-files`, then provision `pact`. The second
    provision's catalog step wrote 8 `view_definition` nodes to the branch
    head; the publish took its dirty scope from the sources' write-sets,
    so its running node count missed those 8 while the attribute view
    counted them, and the integrity receipt refused with "incremental
    attribute receipt count delta disagrees with checkpoint" (computed
    53718, checkpoint 53710).
    """
    publish = _second_run_with_one_source_node(
        tmp_path, clean_runtime, monkeypatch,
        before_sources=lambda: _project_node_outside_sources(
            clean_runtime, "svc:projected",
        ),
    )
    _assert_published_count_matches_visible(
        clean_runtime, publish, "svc:projected", "svc:added",
    )
    decision = publish["dirty_scope_hint"]
    assert decision["used"] is False
    assert decision["reason"] == "segment_has_rows_outside_hint"
    assert decision["tables"] == ["nodes"]


@pytest.mark.postgres
def test_branch_write_made_during_the_source_run_is_counted_by_the_publish(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A write by another process while the sources run is counted too.

    The runtime worker writes accepted admissions to the default branch with
    only transaction-scoped locks, so it can land between a source's writes
    and the publish. When the check sat before the sources ran, this
    published a stamped count one below the visible count.
    """
    publish = _second_run_with_one_source_node(
        tmp_path, clean_runtime, monkeypatch,
        during_sources=lambda: _project_node_outside_sources(
            clean_runtime, "svc:during",
        ),
    )
    _assert_published_count_matches_visible(
        clean_runtime, publish, "svc:during", "svc:added",
    )
    decision = publish["dirty_scope_hint"]
    assert decision["used"] is False
    assert decision["tables"] == ["nodes"]


@pytest.mark.postgres
def test_sources_keep_their_dirty_scope_hint_on_a_clean_branch_head(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no write besides the sources', the publish keeps the fast hint."""
    publish = _second_run_with_one_source_node(
        tmp_path, clean_runtime, monkeypatch,
    )
    _assert_published_count_matches_visible(
        clean_runtime, publish, "svc:added",
    )
    assert publish["dirty_scope_hint"]["used"] is True


def _run_with_write_after_materialize(
    tmp_path: Path,
    dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    write,
    *,
    every_attempt: bool = False,
) -> tuple[IngestOptions, dict, dict, list[bool]]:
    """Second ingest run whose publish sees ``write`` land after its reads.

    The effective-view materialize runs after the cycle's counts and last
    read of the branch head and before the checkpoint cut. ``write`` runs
    right after it: on the first publish attempt only, or on every attempt.
    The second run's cycles are full-proof cycles, which check for writes
    at the cut; an incremental cycle holds the branch write lock there, so
    the write would wait for its commit instead (see
    test_write_after_an_incremental_materialize_waits_for_the_commit).
    Returns (options, baseline publish, second run's last publish, fired).
    """
    import okg.substrate.attributes as attributes

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(dsn, dep)
    options = IngestOptions(dsn=dsn, deployment="svc", deployment_dir=dep)
    (baseline,) = run_ingest(options).report["publishes"]
    assert baseline["status"] == "published", baseline
    _add_source_node(dep)
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "1")

    real_materialize = attributes.materialize_effective_attribute_view_for_generation
    fired: list[bool] = []

    def materialize_then_write(*args, **kwargs):
        result = real_materialize(*args, **kwargs)
        if every_attempt or not fired:
            fired.append(True)
            write(len(fired))
        return result

    monkeypatch.setattr(
        attributes,
        "materialize_effective_attribute_view_for_generation",
        materialize_then_write,
    )
    publishes = run_ingest(replace(options, reset_cursor=True)).report[
        "publishes"
    ]
    monkeypatch.setattr(
        attributes,
        "materialize_effective_attribute_view_for_generation",
        real_materialize,
    )
    return options, baseline, publishes[-1], fired


def _latest_published_id(dsn: str) -> str:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        latest = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).latest_generation(deployment_name="svc", status="published")
    return str(latest.checkpoint_id)


@pytest.mark.postgres
def test_branch_write_between_the_scope_read_and_the_cut_is_never_lost(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A write after the cycle's counts refuses that attempt; the retry counts it.

    Cutting anyway would put the row in the checkpoint without counting it,
    and the next cycle's segment would not hold it: a permanent undercount.
    """
    _, _, publish, fired = _run_with_write_after_materialize(
        tmp_path, clean_runtime, monkeypatch,
        lambda _n: _project_node_outside_sources(clean_runtime, "svc:late"),
    )
    assert fired == [True]
    assert publish["status"] == "published", publish
    assert publish["write_race_refusals"] == [
        {"attempt": 1, "stage": "checkpoint_cut"},
    ]
    _assert_published_count_matches_visible(
        clean_runtime, publish, "svc:late", "svc:added",
    )


@pytest.mark.postgres
def test_late_rewrite_of_a_counted_key_is_never_cut_stale(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rewrite of a key the cycle already counted also refuses the cut.

    Chronos rewrites a key already in the current segment in place, so the
    segment's row count does not move. Only the row-version part of the
    fingerprint sees it; without it the checkpoint would carry the new
    attrs while the attribute view kept the old ones.
    """
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    def rewrite_added(_n: int) -> None:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            ChronosGraphStore.from_connection(conn).apply_facts(
                [NodeFact(
                    "svc:added", "document", {"text": "rewritten late"},
                    {"path": "first.json", "record": "added"}, {"v": 2},
                )],
                source="svc.first",
                strict_endpoints=True,
            )

    _, _, publish, fired = _run_with_write_after_materialize(
        tmp_path, clean_runtime, monkeypatch, rewrite_added,
    )
    assert fired == [True]
    assert publish["status"] == "published", publish
    assert publish["write_race_refusals"] == [
        {"attempt": 1, "stage": "checkpoint_cut"},
    ]
    _assert_published_count_matches_visible(
        clean_runtime, publish, "svc:added",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        published = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(publish["generation_id"]).query(
            "SELECT attrs->>'text' AS text FROM okg.graph_nodes "
            "WHERE node_id = 'svc:added'",
        )
        with conn.cursor() as cur:
            # The attribute read model agrees with the graph row.
            cur.execute(
                "SELECT v.attrs->>'text' "
                "  FROM okg.attribute_effective_view AS s "
                "  JOIN okg.attribute_versions AS v "
                "    USING (target_kind, target_id, attrs_hash) "
                " WHERE s.generation_id = %s AND s.target_kind = 'node' "
                "   AND s.target_id = 'svc:added'",
                (publish["generation_id"],),
            )
            view_rows = cur.fetchall()
    assert [row["text"] for row in published] == ["rewritten late"]
    assert view_rows == [("rewritten late",)]


@pytest.mark.postgres
def test_publish_raced_on_every_attempt_fails_loudly_then_recovers(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retry is bounded: a writer that races every attempt fails the run.

    Nothing is published while it races; the next run without the racing
    writer publishes every row it wrote.
    """
    monkeypatch.setenv("OKG_PUBLISH_WRITE_RACE_ATTEMPTS", "2")
    options, baseline, publish, fired = _run_with_write_after_materialize(
        tmp_path, clean_runtime, monkeypatch,
        lambda n: _project_node_outside_sources(clean_runtime, f"svc:race:{n}"),
        every_attempt=True,
    )
    assert fired == [True, True]
    assert publish["status"] == "fail", publish
    assert publish["code"] == "graph_written_during_publish"
    assert publish["error_data"]["write_race_refusals"] == [
        {"attempt": 1, "stage": "checkpoint_cut"},
        {"attempt": 2, "stage": "checkpoint_cut"},
    ]
    assert _latest_published_id(clean_runtime) == baseline["generation_id"]

    publishes = run_ingest(options).report["publishes"]
    (recovered,) = [p for p in publishes if p["status"] == "published"]
    assert recovered["write_race_refusals"] == []
    _assert_published_count_matches_visible(
        clean_runtime, recovered, "svc:race:1", "svc:race:2", "svc:added",
    )


def _baseline_then_one_source_node(
    tmp_path: Path, dsn: str,
) -> tuple[IngestOptions, dict]:
    """Publish a baseline, then give a source one new node (not ingested)."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(dsn, dep)
    options = IngestOptions(dsn=dsn, deployment="svc", deployment_dir=dep)
    (baseline,) = run_ingest(options).report["publishes"]
    assert baseline["status"] == "published", baseline
    _add_source_node(dep)
    return options, baseline


def _generation_metadata(dsn: str, generation_id: str) -> dict:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        generation = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).get_generation(str(generation_id))
    return dict(getattr(generation, "metadata", None) or {})


@pytest.mark.postgres
def test_write_racing_an_incremental_publish_is_refused_before_protected_writes(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One race on an incremental cycle costs one incremental retry.

    The write lands after the counts and just before the cycle takes the
    branch write lock for its first protected attribute write. The attempt
    is refused there (`before_effective_view`) with the attribute integrity
    state still clean, so the retry is incremental -- the count-stamp
    cadence advances instead of resetting -- and counts the row. A refusal
    at the cut would come after the materialize and force a full rebuild
    (1,999 s against 2-4 s at 100k nodes plus 100k edges).
    """
    from okg.substrate.publisher.publisher import Publisher

    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "20")
    options, baseline = _baseline_then_one_source_node(tmp_path, clean_runtime)
    real_fence = Publisher._chronos_fence_protected_writes
    fired: list[bool] = []

    def write_then_fence(self, store, conn, signature_counted):
        if not fired:
            fired.append(True)
            _project_node_outside_sources(clean_runtime, "svc:late")
        return real_fence(self, store, conn, signature_counted)

    monkeypatch.setattr(
        Publisher, "_chronos_fence_protected_writes", write_then_fence,
    )
    (publish,) = run_ingest(replace(options, reset_cursor=True)).report[
        "publishes"
    ]
    assert fired == [True]
    assert publish["status"] == "published", publish
    assert publish["write_race_refusals"] == [
        {"attempt": 1, "stage": "before_effective_view"},
    ]
    before = _generation_metadata(clean_runtime, baseline["generation_id"])
    after = _generation_metadata(clean_runtime, publish["generation_id"])
    assert int(after["count_stamp_cadence"]) == (
        int(before["count_stamp_cadence"]) + 1
    ), after
    _assert_published_count_matches_visible(
        clean_runtime, publish, "svc:late", "svc:added",
    )


def test_write_race_attempts_must_be_a_positive_integer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.publisher import publisher as publisher_module

    monkeypatch.delenv("OKG_PUBLISH_WRITE_RACE_ATTEMPTS", raising=False)
    assert publisher_module._write_race_attempts_allowed() == 3
    monkeypatch.setenv("OKG_PUBLISH_WRITE_RACE_ATTEMPTS", "5")
    assert publisher_module._write_race_attempts_allowed() == 5
    for bad in ("0", "-1", "three"):
        monkeypatch.setenv("OKG_PUBLISH_WRITE_RACE_ATTEMPTS", bad)
        with pytest.raises(SubstrateError, match="positive integer"):
            publisher_module._write_race_attempts_allowed()


@pytest.mark.postgres
def test_dirty_scope_hint_check_failure_fails_the_publish(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hint check that cannot run refuses the publish; it never trusts it."""
    from okg.substrate.graph_store import ChronosGraphStore
    import okg.substrate.publisher.dirty_hint_coverage as coverage

    def broken_check(*_args, **_kwargs):
        raise psycopg.errors.UndefinedTable("relation is gone")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(
        dsn=clean_runtime, deployment="svc", deployment_dir=dep,
    )
    (baseline,) = run_ingest(options).report["publishes"]
    assert baseline["status"] == "published", baseline
    _add_source_node(dep)
    monkeypatch.setattr(coverage, "segment_keys_outside_hint", broken_check)

    (publish,) = run_ingest(replace(options, reset_cursor=True)).report[
        "publishes"
    ]
    assert publish["status"] == "fail", publish
    assert publish["error"] == (
        "cannot check the dirty-scope hint against the branch head"
    )
    assert publish["error_data"]["error_type"] == "UndefinedTable"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        latest = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).latest_generation(deployment_name="svc", status="published")
    assert latest.checkpoint_id == baseline["generation_id"]


def _published_node_ids(dsn: str, generation_id: str) -> list[str]:
    """Node ids visible in one published generation, duplicates included."""
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        generation = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(generation_id)
        rows = generation.query(
            "SELECT node_id FROM okg.graph_nodes ORDER BY node_id",
        )
    return [str(row["node_id"]) for row in rows]


def _retraction_counts(report: dict) -> list[tuple[int, int]]:
    """Per-source retraction counters an ingest run reported."""
    return [
        (int(result.get("retract_n") or 0), int(result.get("retract_e") or 0))
        for result in report["results"]
    ]


@pytest.mark.postgres
def test_staged_redrive_names_manifest_drift_and_recovers_after_claim(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """#1791: the redrive guard must hand over the publish's own diagnosis.

    A manifest edit to a fingerprinted block (`nomos`) after the catalog
    claim refuses the publish with `catalog_manifest_drifted`, which leaves
    staged source work; the next ingest then refuses before any source
    write. The refusal has to name the drifted block and the claim command
    -- previously it carried only `publish_status` / `publish_code` and a
    generic "resolve the blocker" hint, and the operator could not find the
    recovery. After the claim the redrive publishes exactly once.
    """
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        claim_catalog_ownership,
    )
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
    )

    baseline = run_ingest(options)
    assert baseline.any_failed is False, json.dumps(
        baseline.report, default=str,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        baseline_published = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).latest_generation(deployment_name="svc", status="published")
    assert baseline_published is not None
    baseline_ids = _published_node_ids(
        clean_runtime, baseline_published.checkpoint_id,
    )
    assert baseline_ids

    # A real source delta, plus an edit to a fingerprinted manifest block
    # (`nomos`). Committed, so the tree is clean and repository provenance
    # is satisfied; never re-claimed, so the publish identity has drifted.
    first_fixture = dep / "first.json"
    first_payload = json.loads(first_fixture.read_text())
    first_payload["nodes"].append({
        "node_id": "svc:first:drift-delta",
        "subtype": "document",
        "attrs": {"text": "delta emitted while the manifest had drifted"},
        "source_record_id": {"path": "first.json", "record": "drift-delta"},
    })
    first_fixture.write_text(json.dumps(first_payload))
    manifest_path = dep / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text())
    manifest["nomos"]["deployment_posture"]["readiness_profiles"] = [
        "dogfood", "general",
    ]
    manifest_path.write_text(yaml.safe_dump(manifest))
    _commit_layered_fixture_deployment(dep, message="drift the nomos block")

    wedging = run_ingest(replace(options, reset_cursor=True))
    assert wedging.any_failed is True
    failed_publishes = [
        publish for publish in wedging.report["publishes"]
        if publish.get("status") == "fail"
    ]
    assert failed_publishes, json.dumps(
        wedging.report["publishes"], default=str,
    )
    assert failed_publishes[0]["code"] == "catalog_manifest_drifted"
    assert "okg catalog ownership claim" in failed_publishes[0]["fix_hint"]

    # Staged, unpublished source work now precedes any new source write.
    with pytest.raises(SubstrateError) as guard:
        run_ingest(replace(options, reset_cursor=True))

    assert "refusing to start new source writes" in str(guard.value)
    guard_data = guard.value.data
    assert guard_data["publish_status"] == "fail"
    assert guard_data["publish_code"] == "catalog_manifest_drifted"
    observed = guard_data["publish_error_data"]["observed"]
    assert "nomos" in observed["drifted_blocks"]
    assert observed["recorded_manifest_fingerprint"] != (
        observed["live_manifest_fingerprint"]
    )
    assert "drifted_blocks" in guard_data["publish_error"]
    assert "okg catalog ownership claim" in (guard.value.fix_hint or "")
    assert "catalog load --apply" in (guard.value.fix_hint or "")
    # The guard payload still crosses the CLI/DBOS JSON boundary.
    json.dumps({"data": guard_data, "fix_hint": guard.value.fix_hint})

    # Through the wedge, the last known good generation stays the latest
    # published one and stays readable.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        wedged_published = store.latest_generation(
            deployment_name="svc", status="published",
        )
    assert wedged_published is not None
    assert wedged_published.checkpoint_id == baseline_published.checkpoint_id
    assert _published_node_ids(
        clean_runtime, baseline_published.checkpoint_id,
    ) == baseline_ids

    # Recovery: the claim the hint names -- the same call `okg catalog
    # ownership claim` makes -- and nothing else.
    composed = compose_catalog(dep)
    drifted_identity = catalog_identity_for(
        composed=composed,
        deployment_dir=dep,
        command="okg catalog ownership claim",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        claim_catalog_ownership(conn, identity=drifted_identity)

    recovered = run_ingest(replace(options, reset_cursor=True))
    assert recovered.any_failed is False, json.dumps(
        recovered.report, default=str,
    )
    statuses = [
        publish.get("status") for publish in recovered.report["publishes"]
        if not publish.get("publish_skipped")
    ]
    assert "fail" not in statuses, recovered.report["publishes"]
    redrive = recovered.report["publishes"][0]
    assert redrive["label"] == "staged-source-redrive"
    assert redrive["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        recovered_published = store.latest_generation(
            deployment_name="svc", status="published",
        )
    assert recovered_published is not None
    assert recovered_published.checkpoint_id != (
        baseline_published.checkpoint_id
    )

    recovered_ids = _published_node_ids(
        clean_runtime, recovered_published.checkpoint_id,
    )
    # Published once: exactly the source's real delta, no duplicated ids.
    assert len(recovered_ids) == len(set(recovered_ids))
    assert sorted(recovered_ids) == sorted(
        [*baseline_ids, "svc:first:drift-delta"],
    )
    # No run retracted anything on the way through the wedge.
    for report in (baseline.report, wedging.report, recovered.report):
        assert all(
            counts == (0, 0) for counts in _retraction_counts(report)
        ), json.dumps(report["results"], default=str)


@pytest.mark.postgres
def test_staged_redrive_fence_covers_admission_feedback(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A continuation-only late source is not swept by redrive feedback."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher import Publisher
    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_advisory_keys,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
    )
    baseline = run_ingest(options)
    assert baseline.any_failed is False

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact("svc:staged", "document", {"text": "staged"}, {}, {})],
            source="svc.staged",
        )
    staged_id = _insert_staged_source_admission(
        clean_runtime,
        deployment="svc",
        source="svc.staged",
    )
    redrive_state = ingest_service._staged_source_redrive_state(options)

    token_observed = threading.Event()
    writer_ready = threading.Event()
    writer_reached_fence = threading.Event()
    writer_finished = threading.Event()
    feedback_observed = threading.Event()
    writer_errors: list[BaseException] = []
    late_admission_ids: list[str] = []
    original_token = Publisher.chronos_graph_branch_mutation_token
    original_write_lock = ChronosGraphStore._lock_branch_write
    original_feedback = ingest_service._apply_publish_admission_feedback
    late_conn = psycopg.connect(clean_runtime, autocommit=True)
    late_store = ChronosGraphStore.from_connection(late_conn)

    def observed_token(conn, *, branch_name: str) -> str:
        token = original_token(conn, branch_name=branch_name)
        token_observed.set()
        if writer.ident is None:
            writer.start()
        assert writer_reached_fence.wait(timeout=5), writer_errors
        assert not writer_finished.is_set()
        return token

    def observed_write_lock(self, branch: str) -> None:
        if threading.current_thread().name == "late-admission-writer":
            writer_reached_fence.set()
        original_write_lock(self, branch)

    def observed_feedback(
        feedback_options: IngestOptions,
        summary: dict[str, object],
        *,
        conn: psycopg.Connection | None = None,
    ) -> dict[str, object] | None:
        with psycopg.connect(clean_runtime, autocommit=True) as observer, \
                observer.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(hashtext(%s), hashtext(%s))",
                ("okg.chronos_graph", "default"),
            )
            acquired = bool(cur.fetchone()[0])
            if acquired:
                cur.execute(
                    "SELECT pg_advisory_unlock(hashtext(%s), hashtext(%s))",
                    ("okg.chronos_graph", "default"),
                )
            publisher_key = publisher_publish_advisory_keys("svc")
            cur.execute(
                "SELECT pg_try_advisory_lock(%s, %s)",
                publisher_key,
            )
            publisher_acquired = bool(cur.fetchone()[0])
            if publisher_acquired:
                cur.execute(
                    "SELECT pg_advisory_unlock(%s, %s)",
                    publisher_key,
                )
        assert acquired is False
        assert publisher_acquired is False
        assert writer_reached_fence.is_set()
        assert not writer_finished.is_set()
        feedback_observed.set()
        return original_feedback(
            feedback_options,
            summary,
            conn=conn,
        )

    monkeypatch.setattr(
        Publisher,
        "chronos_graph_branch_mutation_token",
        staticmethod(observed_token),
    )
    monkeypatch.setattr(
        ChronosGraphStore,
        "_lock_branch_write",
        observed_write_lock,
    )
    monkeypatch.setattr(
        ingest_service,
        "_apply_publish_admission_feedback",
        observed_feedback,
    )

    def write_late_admission() -> None:
        try:
            writer_ready.set()
            assert token_observed.is_set()
            late_store.apply_facts(
                [NodeFact(
                    "svc:late",
                    "document",
                    {"text": "continuation only"},
                    {},
                    {},
                )],
                source="svc.late",
            )
            late_admission_ids.append(_insert_staged_source_admission(
                clean_runtime,
                deployment="svc",
                source="svc.late",
            ))
        except Exception as exc:  # noqa: BLE001 - surface thread failures
            writer_errors.append(exc)
        finally:
            writer_finished.set()

    writer = threading.Thread(
        target=write_late_admission,
        name="late-admission-writer",
    )
    try:
        redrive = ingest_service._publish(
            options,
            "staged-source-redrive",
            expected_latest_checkpoint_id=redrive_state["latest_checkpoint_id"],
            expected_branch_mutation_token=redrive_state[
                "branch_mutation_token"
            ],
            require_expected_latest_checkpoint=True,
        )
    finally:
        if writer.ident is not None:
            writer.join(timeout=5)
        late_conn.close()

    assert redrive["status"] == "published"
    assert writer_ready.is_set()
    assert feedback_observed.is_set()
    assert writer_finished.is_set()
    assert writer_errors == []
    assert len(late_admission_ids) == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        published_rows = store.checkout_generation(
            redrive["generation_id"],
        ).query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE node_id IN ('svc:staged', 'svc:late')"
        )
        current_rows = store.checkout("default").query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE node_id IN ('svc:staged', 'svc:late')"
        )
        cur.execute(
            "SELECT admission_id::text, status, target_generation_ref "
            "FROM okg.admission_envelopes "
            "WHERE admission_id = ANY(%s::uuid[]) ORDER BY admission_id",
            ([staged_id, late_admission_ids[0]],),
        )
        admission_rows = {
            admission_id: (status, target_generation_ref)
            for admission_id, status, target_generation_ref in cur.fetchall()
        }

    assert {row["node_id"] for row in published_rows} == {"svc:staged"}
    assert {row["node_id"] for row in current_rows} == {
        "svc:staged",
        "svc:late",
    }
    assert admission_rows[staged_id] == (
        "published",
        redrive["generation_id"],
    )
    assert admission_rows[late_admission_ids[0]] == ("accepted", None)


@pytest.mark.postgres
def test_staged_redrive_uses_publisher_then_graph_lock_order(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A normal publisher can emit graph facts while redrive waits its turn."""
    from okg.substrate.enrichers.base import EnrichResult
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher import Publisher

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    catalog_identity = _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
    )
    baseline = run_ingest(options)
    assert baseline.any_failed is False
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact("svc:staged", "document", {"text": "staged"}, {}, {})],
            source="svc.staged",
        )
    _insert_staged_source_admission(
        clean_runtime,
        deployment="svc",
        source="svc.staged",
    )
    redrive_state = ingest_service._staged_source_redrive_state(options)

    redrive_lock_attempted = threading.Event()
    ordinary_enricher_entered = threading.Event()
    ordinary_results: list[dict[str, object]] = []
    ordinary_errors: list[BaseException] = []
    redrive_results: list[dict[str, object]] = []
    redrive_errors: list[BaseException] = []
    original_acquire = Publisher._acquire_dbos_publish_lock

    def observed_acquire(self, conn, *, wait_seconds=None):
        if threading.current_thread().name == "guarded-redrive":
            redrive_lock_attempted.set()
        return original_acquire(
            self,
            conn,
            wait_seconds=wait_seconds,
        )

    monkeypatch.setattr(
        Publisher,
        "_acquire_dbos_publish_lock",
        observed_acquire,
    )

    class _GraphWritingEnricher:
        name = "lock_order_graph_writer"
        publisher_skip_when_clean = False
        reads_subtypes = ("document",)
        reads_edge_types = ()
        emits_edge_types = ()
        requires_narrowings = ()
        write_envelope = {
            "nodes": [{"subtype": "document"}],
            "nondeterministic": False,
        }
        declares_node_provenance = True

        def enrich(self, conn, *, generation_id, incremental=None):
            del generation_id, incremental
            ordinary_enricher_entered.set()
            assert redrive_lock_attempted.wait(timeout=5)
            with conn.cursor() as cur:
                cur.execute("SET lock_timeout = '1500ms'")
            ChronosGraphStore.from_connection(
                conn,
                ensure_schema=False,
            ).apply_facts(
                [NodeFact(
                    "svc:ordinary-derived",
                    "document",
                    {"text": "ordinary publisher graph output"},
                    {},
                    {},
                )],
                source="svc.ordinary-publisher",
            )
            return EnrichResult(self.name, n_edges_emitted=0)

    def run_ordinary_publisher() -> None:
        try:
            ordinary_results.append(
                Publisher(
                    clean_runtime,
                    "svc",
                    enrichers=[_GraphWritingEnricher()],
                    catalog_identity=catalog_identity,
                ).cycle(force_enrichers=True)
            )
        except Exception as exc:  # noqa: BLE001 - surface thread failures
            ordinary_errors.append(exc)

    def run_guarded_redrive() -> None:
        try:
            assert ordinary_enricher_entered.wait(timeout=5)
            redrive_results.append(ingest_service._publish(
                options,
                "staged-source-redrive",
                expected_latest_checkpoint_id=redrive_state[
                    "latest_checkpoint_id"
                ],
                expected_branch_mutation_token=redrive_state[
                    "branch_mutation_token"
                ],
                require_expected_latest_checkpoint=True,
            ))
        except Exception as exc:  # noqa: BLE001 - surface thread failures
            redrive_errors.append(exc)

    ordinary = threading.Thread(
        target=run_ordinary_publisher,
        name="ordinary-publisher",
    )
    redrive = threading.Thread(
        target=run_guarded_redrive,
        name="guarded-redrive",
    )
    ordinary.start()
    redrive.start()
    ordinary.join(timeout=10)
    redrive.join(timeout=10)

    assert not ordinary.is_alive()
    assert not redrive.is_alive()
    assert ordinary_errors == []
    assert redrive_errors == []
    assert ordinary_results[0]["status"] == "published"
    # The ordinary publisher legitimately advanced the predecessor while the
    # redrive waited, so the guarded redrive must refuse instead of checkpointing.
    assert redrive_results[0]["status"] == "fail"
    assert "predecessor changed" in str(redrive_results[0]["error"])


@pytest.mark.postgres
def test_incomplete_repair_freezes_without_promoting_when_source_still_fails(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A still-broken exact repair may advance only to another blocked freeze."""
    # The recovery path under test starts from a pre-#3342 freeze.
    freeze_partial_failures(monkeypatch)
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
    )
    baseline = run_ingest(options)
    (baseline_publish,) = baseline.report["publishes"]
    assert baseline_publish["status"] == "published"

    first_fixture = dep / "first.json"
    first_payload = json.loads(first_fixture.read_text())
    first_payload["nodes"].append({
        "node_id": "svc:partial",
        "subtype": "document",
        "attrs": {"text": "must remain reader-invisible"},
        "source_record_id": {"path": "first.json", "record": "partial"},
    })
    first_fixture.write_text(json.dumps(first_payload))
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["sources"]["second"]["params"]["fixture_path"] = str(
        dep / "missing-second.json",
    )
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(clean_runtime, dep)

    partial = run_ingest(replace(options, reset_cursor=True))
    (blocked,) = partial.report["publishes"]
    assert blocked["status"] == "blocked"
    assert blocked["blocked_reason"]["reason"] == "ingest_source_incomplete"
    staged_id = _insert_staged_source_admission(
        clean_runtime,
        deployment="svc",
        source="svc.first",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        before = store.list_generations(
            deployment_name="svc",
            status="published",
        )

    retried = run_ingest(options)
    assert retried.any_failed is True
    assert [item["status"] for item in retried.report["publishes"]] == [
        "blocked"
    ]
    assert retried.report["publishes"][0]["blocked_reason"]["recovery"][
        "state"
    ] == "repair_required"

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        after = store.list_generations(
            deployment_name="svc",
            status="published",
        )
        latest_published = store.latest_generation(
            deployment_name="svc",
            status="published",
        )
        cur.execute(
            "SELECT status FROM okg.admission_envelopes "
            "WHERE admission_id = %s::uuid",
            (staged_id,),
        )
        staged_status = cur.fetchone()[0]

    assert [generation.checkpoint_id for generation in after] == [
        generation.checkpoint_id for generation in before
    ]
    assert latest_published is not None
    assert latest_published.checkpoint_id == baseline_publish["generation_id"]
    assert staged_status == "blocked"


@pytest.mark.postgres
def test_incomplete_recovery_real_publish(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A repaired branch is blocked once, then its exact freeze is promoted."""
    # The recovery path under test starts from a pre-#3342 freeze.
    freeze_partial_failures(monkeypatch)
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
    )
    baseline = run_ingest(options)
    baseline_generation = baseline.report["publishes"][0]["generation_id"]

    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    healthy_second = Path(
        registry["sources"]["second"]["params"]["fixture_path"]
    )
    repaired_second = dep / "repair-second.json"
    second_payload = healthy_second.read_text()
    registry["sources"]["second"]["params"]["fixture_path"] = str(
        repaired_second
    )
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(clean_runtime, dep)

    incomplete = run_ingest(replace(options, reset_cursor=True))
    (blocked,) = incomplete.report["publishes"]
    assert blocked["status"] == "blocked"
    assert blocked["blocked_reason"]["recovery"]["state"] == (
        "repair_required"
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        latest_before_repair = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).latest_generation(deployment_name="svc", status="published")
    assert latest_before_repair is not None
    assert latest_before_repair.checkpoint_id == baseline_generation

    # Repair the selected binding without changing its registry identity.
    repaired_second.write_text(second_payload)
    _commit_layered_fixture_deployment(dep, message="repair fixture source")
    recovered = run_ingest(replace(options, reset_cursor=True))

    assert recovered.any_failed is False
    assert [item["status"] for item in recovered.report["publishes"]] == [
        "blocked",
        "published",
    ]
    frozen, promoted = recovered.report["publishes"]
    recovery = frozen["blocked_reason"]["recovery"]
    assert recovery["state"] == "frozen"
    assert promoted["label"] == "incomplete-recovery-promotion"
    assert sorted(frozen["admission_feedback"]["admission_ids"]) == (
        recovery["admission_ids"]
    )
    assert promoted["admission_feedback"]["verified_admission_ids"] == (
        recovery["admission_ids"]
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        latest = store.latest_generation(
            deployment_name="svc",
            status="published",
        )
        cur.execute(
            "SELECT admission_id::text, status, target_generation_ref "
            "FROM okg.admission_envelopes "
            "WHERE admission_id = ANY(%s::uuid[]) ORDER BY admission_id",
            (recovery["admission_ids"],),
        )
        admission_rows = list(cur.fetchall())
    assert latest is not None
    assert latest.checkpoint_id == promoted["generation_id"]
    assert admission_rows == [
        (admission_id, "published", promoted["generation_id"])
        for admission_id in recovery["admission_ids"]
    ]


@pytest.mark.postgres
def test_incomplete_recovery_refuses_narrower_scope_before_source(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The recovery path under test starts from a pre-#3342 freeze.
    freeze_partial_failures(monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["sources"]["second"]["params"]["fixture_path"] = str(
        dep / "missing-second.json"
    )
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        reset_cursor=True,
    )
    blocked = run_ingest(options).report["publishes"][0]
    assert blocked["status"] == "blocked"

    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda *_args, **_kwargs: pytest.fail("source must not run"),
    )
    with pytest.raises(SubstrateError) as mismatch:
        run_ingest(replace(
            options,
            include={"first"},
            include_predecessors=False,
        ))

    assert mismatch.value.data["reason"] == (
        "incomplete_recovery_contract_mismatch"
    )


@pytest.mark.postgres
def test_incomplete_recovery_legacy_receipt_fails_closed_before_source(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The recovery path under test starts from a pre-#3342 freeze.
    freeze_partial_failures(monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["sources"]["second"]["params"]["fixture_path"] = str(
        dep / "missing-second.json"
    )
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        reset_cursor=True,
    )
    blocked = run_ingest(options).report["publishes"][0]
    legacy_reason = dict(blocked["blocked_reason"])
    legacy_reason.pop("recovery", None)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "UPDATE _chronos_branch_interval_checkpoints "
            "SET metadata = jsonb_set(metadata::jsonb, "
            "'{blocked_reason}', %s::jsonb) "
            "WHERE checkpoint_id = %s",
            (json.dumps(legacy_reason), blocked["generation_id"]),
        )

    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda *_args, **_kwargs: pytest.fail("source must not run"),
    )
    with pytest.raises(SubstrateError) as legacy:
        run_ingest(options)

    assert legacy.value.data["reason"] == (
        "incomplete_recovery_authority_missing"
    )
    assert "--force-enrichers" in str(legacy.value.fix_hint)


@pytest.mark.postgres
def test_incomplete_recovery_refuses_branch_drift_before_freeze(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A concurrent graph write after the freeze probe cannot be published."""
    # The recovery path under test starts from a pre-#3342 freeze.
    freeze_partial_failures(monkeypatch)
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    healthy_second = Path(
        registry["sources"]["second"]["params"]["fixture_path"]
    )
    repaired_second = dep / "repair-second.json"
    second_payload = healthy_second.read_text()
    registry["sources"]["second"]["params"]["fixture_path"] = str(
        repaired_second
    )
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        reset_cursor=True,
    )
    blocked = run_ingest(options).report["publishes"][0]
    assert blocked["status"] == "blocked"
    repaired_second.write_text(second_payload)
    _commit_layered_fixture_deployment(dep, message="repair fixture source")

    original_publish = ingest_service._publish
    injected = False

    def publish_with_drift(*args, **kwargs):
        nonlocal injected
        if kwargs.get("publish_blocker") is not None and not injected:
            injected = True
            with psycopg.connect(clean_runtime, autocommit=True) as conn:
                ChronosGraphStore.from_connection(
                    conn,
                    ensure_schema=False,
                ).apply_facts(
                    [NodeFact(
                        "svc:concurrent",
                        "document",
                        {"text": "must not enter recovered publish"},
                        {},
                        {},
                    )],
                    source="svc.concurrent",
                )
        return original_publish(*args, **kwargs)

    monkeypatch.setattr(ingest_service, "_publish", publish_with_drift)
    refused = run_ingest(options)

    assert injected is True
    assert [item["status"] for item in refused.report["publishes"]] == [
        "fail"
    ]
    assert refused.report["publishes"][0]["error_data"]["reason"] == (
        "branch_mutation_token_changed"
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        latest = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).latest_generation(deployment_name="svc", status=None)
    assert latest is not None
    assert latest.checkpoint_id == blocked["generation_id"]


@pytest.mark.postgres
def test_incomplete_recovery_refuses_branch_drift_before_promotion(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A writer racing the frozen promotion cannot enter its generation."""
    # The recovery path under test starts from a pre-#3342 freeze.
    freeze_partial_failures(monkeypatch)
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    healthy_second = Path(
        registry["sources"]["second"]["params"]["fixture_path"]
    )
    repaired_second = dep / "repair-second.json"
    second_payload = healthy_second.read_text()
    registry["sources"]["second"]["params"]["fixture_path"] = str(
        repaired_second
    )
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="svc",
        deployment_dir=dep,
        reset_cursor=True,
    )
    blocked = run_ingest(options).report["publishes"][0]
    repaired_second.write_text(second_payload)
    _commit_layered_fixture_deployment(dep, message="repair fixture source")

    original_promote = ingest_service._promote_frozen_recovery
    injected = False

    def promote_with_drift(*args, **kwargs):
        nonlocal injected
        injected = True
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            ChronosGraphStore.from_connection(
                conn,
                ensure_schema=False,
            ).apply_facts(
                [NodeFact(
                    "svc:promotion-race",
                    "document",
                    {"text": "later writer"},
                    {},
                    {},
                )],
                source="svc.promotion-race",
            )
        return original_promote(*args, **kwargs)

    monkeypatch.setattr(
        ingest_service,
        "_promote_frozen_recovery",
        promote_with_drift,
    )
    refused = run_ingest(options)

    assert injected is True
    assert [item["status"] for item in refused.report["publishes"]] == [
        "blocked",
        "fail",
    ]
    frozen, failed_promotion = refused.report["publishes"]
    assert failed_promotion["error_data"]["reason"] == (
        "branch_mutation_token_changed"
    )
    recovery_ids = frozen["blocked_reason"]["recovery"]["admission_ids"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        latest = store.latest_generation(deployment_name="svc", status=None)
        latest_published = store.latest_generation(
            deployment_name="svc",
            status="published",
        )
        cur.execute(
            "SELECT status, target_generation_ref "
            "FROM okg.admission_envelopes "
            "WHERE admission_id = ANY(%s::uuid[]) ORDER BY admission_id",
            (recovery_ids,),
        )
        admission_rows = list(cur.fetchall())
    assert latest is not None
    assert latest.checkpoint_id == frozen["generation_id"]
    assert latest_published is None or (
        latest_published.checkpoint_id != frozen["generation_id"]
    )
    assert admission_rows == [
        ("blocked", frozen["generation_id"])
        for _admission_id in recovery_ids
    ]
    assert frozen["blocked_reason"]["recovery"][
        "predecessor_checkpoint_id"
    ] == blocked["generation_id"]


def _coverage_refusal_for_first_source():
    """A record-set coverage refusal shaped exactly like the runner's."""
    from okg.substrate.errors import SubstrateError
    from okg.substrate.ingest.runner import (
        record_set_coverage_diagnosis,
        record_set_coverage_marker,
    )

    diagnosis = record_set_coverage_diagnosis(
        missing_node_count=1,
        missing_edge_count=0,
        stale_node_count=0,
        stale_edge_count=0,
    )
    return SubstrateError(
        "SourceRunner: record_set coverage check failed; refusing to "
        "advance source watermark " + record_set_coverage_marker(diagnosis),
        data={
            "source_name": "first",
            **{k: v for k, v in diagnosis.items() if k != "fix_hint"},
            "missing_nodes_sample": ["author:shared.author@example.test"],
            "missing_edges_sample": [],
            "stale_nodes_sample": [],
            "stale_edges_sample": [],
        },
        fix_hint=diagnosis["fix_hint"],
    )


def _run_first_source_refusing(tmp_path, dsn, monkeypatch, refusal):
    from okg.substrate.library.sources import bootstrap_fixture

    def _refuse(self, *args, **kwargs):
        raise refusal

    monkeypatch.setattr(
        bootstrap_fixture.BootstrapFixtureSource, "run", _refuse, raising=True
    )
    dep = _write_layered_fixture_deployment(tmp_path)
    return run_ingest(
        IngestOptions(
            dsn=dsn,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            include_dependents=False,
            publish=False,
        ),
    )


@pytest.mark.postgres
def test_source_failure_carries_the_structured_payload_to_the_operator(
    tmp_path,
    clean_runtime: str,
    monkeypatch,
) -> None:
    """A refusal's samples must reach the operator, not just its counts.

    `run_ingest` flattens the exception to `f"{type}: {exc}"` for the report's
    `error` string, so the coverage counts ride out inside the message marker.
    The `data=` payload used to stop there -- and the payload is where
    `missing_nodes_sample` lives, the one field that names WHICH identities
    were absent. Without it, diagnosing a refusal meant reading the runner
    source; a 57 MB ingest log held no copy of it.
    """
    from okg.substrate.runtime.source_sync_workflow import (
        _ingest_failure_details,
    )

    outcome = _run_first_source_refusing(
        tmp_path, clean_runtime, monkeypatch, _coverage_refusal_for_first_source()
    )

    assert outcome.any_failed is True
    result = outcome.report["results"][0]
    assert result["status"] == "FAIL"
    assert result["error_data"]["missing_nodes_sample"] == [
        "author:shared.author@example.test"
    ]

    # And it survives the workflow wrapper the operator actually reads.
    details = _ingest_failure_details(
        failed_results=[result],
        failed_publishes=[],
    )
    assert details["record_set_coverage"]["missing_node_count"] == 1
    assert details["error_data"]["missing_nodes_sample"] == [
        "author:shared.author@example.test"
    ]


@pytest.mark.postgres
def test_source_failure_payload_survives_a_failed_compensation(
    tmp_path,
    clean_runtime: str,
    monkeypatch,
) -> None:
    """A compensation failure rebinds the error; the original payload stays.

    When retracting the failed run also fails, the FAIL record's headline is
    the unresolved compensation, and its `error_data` is that replacement
    error's two strings. The refusal that started it all must still name its
    missing identities, so the original payload rides beside them.
    """
    from okg.substrate.ingest import service as ingest_service

    def _compensation_fails(*args, **kwargs):
        raise RuntimeError("retraction lock unavailable")

    monkeypatch.setattr(
        ingest_service, "_retract_chronos_run", _compensation_fails
    )
    outcome = _run_first_source_refusing(
        tmp_path, clean_runtime, monkeypatch, _coverage_refusal_for_first_source()
    )

    result = outcome.report["results"][0]
    assert result["status"] == "FAIL"
    assert result["error"].startswith(
        "SubstrateError: source failure compensation is unresolved"
    )
    data = result["error_data"]
    assert "record_set coverage check failed" in data["original_error"]
    assert "retraction lock unavailable" in data["compensation_error"]
    assert data["original_error_data"]["missing_nodes_sample"] == [
        "author:shared.author@example.test"
    ]


# --- exercised ingest credential failure (#1794) ----------------------------
#
# `diagnostic_from_source_health` is unit-proven against hand-built health, and
# `test_conformance_diagnostics.py` drives `_run_source` with SourceRunner, both
# admission calls and the state snapshot monkeypatched away and the health a
# literal.  Nothing there resolves a credential.  The tests below run the REAL
# `_run_source` -- real `_prepare_source_admission`, real `SourceRunner.run`,
# real `_finalize_source_admission` -- against a disposable PostgreSQL, with a
# connector whose health comes from `credential_preflight` reading the live
# process environment.  The reference name is derived from that preflight, never
# from a literal in the receipt.

CONFORMANCE_FIXTURES = (
    Path(__file__).parents[1]
    / "fixtures"
    / "deployment_products"
    / "external_conformance_interface"
)
CONFORMANCE_DECLARATION = "archi:cern-team-source-declaration"
CONFORMANCE_ZERO_DIGEST = "sha256:" + "0" * 64
CREDENTIAL_FIXTURE_MODULE = "tests.substrate._credential_fixture_source"
#: Deliberately absent from every environment the suite runs in. The tests
#: assert it is unset rather than assuming it.
UNSET_CREDENTIAL_REF = "OKG_FIXTURE_1794_ABSENT_INGEST_TOKEN"


def _conformance_request(name: str):
    from okg.substrate.contracts.deployment_product.conformance import (
        load_conformance_request,
    )

    return load_conformance_request((CONFORMANCE_FIXTURES / name).read_bytes())


def _seal_conformance_receipt(payload: dict):
    from okg.substrate.contracts.deployment_product.conformance import (
        conformance_receipt_digest,
        load_conformance_receipt,
    )

    payload["receipt_digest"] = CONFORMANCE_ZERO_DIGEST
    provisional = load_conformance_receipt(payload)
    payload["receipt_digest"] = conformance_receipt_digest(provisional)
    return load_conformance_receipt(payload)


def _receipt_carrying(name: str, diagnostic: dict):
    """Seal the reviewed receipt with ONE diagnostic replaced by `diagnostic`."""

    payload = json.loads((CONFORMANCE_FIXTURES / name).read_bytes())
    payload["diagnostics"] = [
        diagnostic if item["code"] == diagnostic["code"] else item
        for item in payload["diagnostics"]
    ]
    assert diagnostic in payload["diagnostics"], (
        f"{name} carries no {diagnostic['code']} diagnostic to replace"
    )
    return _seal_conformance_receipt(payload)


def _write_credential_gated_deployment(
    tmp_path: Path,
    *,
    adapter_class: str,
    params: dict,
    policy_overrides: dict | None = None,
) -> Path:
    """One-source deployment whose adapter is a real importable connector."""

    dep = tmp_path / "deployments" / "svc"
    dep.mkdir(parents=True)
    (dep / "schemas").mkdir()
    (dep / "schemas" / "fixture.yaml").write_text(
        "id: https://example.invalid/okg/fixture\n"
        "name: ingest_fixture\n"
        "classes:\n"
        "  Document:\n"
        "    attributes:\n"
        "      text: {}\n"
        "    annotations:\n"
        "      archetype: artifact\n"
        "      transient: 'false'\n"
    )
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "svc",
        "description": "credential gated ingest fixture",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": [],
        "nomos": {
            "deployment_posture": {
                "deployment_class": "dogfood_workspace",
                "read_principals": ["human_operator", "agent"],
                "write_principals": ["human_operator", "source_adapter"],
                "mcp_write_default": "review",
                "export_policy": "internal_only",
                "secret_handling": "secret_ref_required",
                "live_call_default": "review",
                "allowed_source_sensitivities": ["local_private"],
                "allowed_data_classifications": ["private"],
                "allowed_exportability": ["no_export"],
                "privacy_obligations": ["audit"],
                "readiness_profiles": ["dogfood"],
            },
        },
    }))
    policy = _fixture_source_privacy_policy()
    # The connector consumes one optional environment credential, so the
    # declared policy has to say so; `none` would contradict the run.
    policy["credential_ref_policy"] = "optional"
    policy.update(policy_overrides or {})
    (dep / "source_registry.yaml").write_text(yaml.safe_dump({
        "sources": {
            "gated": {
                **_source("svc.gated"),
                "module": CREDENTIAL_FIXTURE_MODULE,
                "class": adapter_class,
                "params": params,
                "source_policy": policy,
                "sync": {
                    "triggers": ["manual", "reconcile"],
                    "default_event_mode": "scope_complete",
                    "reconcile_mode": "scope_complete",
                },
            },
        },
    }))
    _commit_layered_fixture_deployment(dep, message="credential gated fixture")
    return dep


def _run_credential_gated_ingest(dsn: str, dep: Path):
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    _claim_layered_fixture_catalog(dsn, dep)
    return run_ingest(
        IngestOptions(
            dsn=dsn,
            deployment="svc",
            deployment_dir=dep,
            publish=False,
        ),
    )


@pytest.mark.postgres
def test_real_ingest_credential_failure_carries_the_reference_into_both_profiles(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A REAL ingest of an unset credential feeds both settled profiles.

    The reference name reaches the ingest RECORD -- the recorded health
    carries the NAME and no value -- and stops there: the wire diagnostic
    names only the declaration, because the conformance identifier filter
    refuses any credential, secret or token substring.
    """
    monkeypatch.delenv(UNSET_CREDENTIAL_REF, raising=False)
    # The failure must come from the environment, so prove the environment.
    assert UNSET_CREDENTIAL_REF not in os.environ
    from okg.substrate.distributions.conformance_diagnostics import (
        check_conformance_diagnostic_profiles,
        diagnostic_from_source_health,
    )
    from okg.substrate.contracts.deployment_product.conformance import (
        validate_conformance_exchange,
    )

    dep = _write_credential_gated_deployment(
        tmp_path,
        adapter_class="CredentialGatedFixtureSource",
        params={
            "credential_ref": UNSET_CREDENTIAL_REF,
            "node_id": "svc:gated",
        },
    )
    outcome = _run_credential_gated_ingest(clean_runtime, dep)

    # 1. The real admission gate rejected the run on the observed health.
    assert outcome.any_failed is True
    records = [r for r in outcome.report["results"] if r["name"] == "gated"]
    assert len(records) == 1
    record = records[0]
    assert record["status"] == "FAIL", record
    assert record["stage"] == "admission_finalize", record
    assert record["admission_reject_reason"] == "source_health_not_healthy"
    health = record["source_health"]
    assert health["status"] == "missing_credential"
    assert health["credential_refs"] == [UNSET_CREDENTIAL_REF]
    assert (
        record["admission_verdict"]["producer_policy"]["source_health_status"]
        == "missing_credential"
    )
    # The reference NAME is present; no value is, because none existed.
    assert UNSET_CREDENTIAL_REF in json.dumps(health)

    # 2. Selected WITH a deferred binding: blocked, mutation none.
    deferred_request = _conformance_request(
        "request_archi_cern_team_lifecycle.json"
    )
    binding = deferred_request.inputs.optional_credential_bindings[0]
    assert binding.declaration_ref == CONFORMANCE_DECLARATION
    blocked = diagnostic_from_source_health(
        health,
        declaration_ref=CONFORMANCE_DECLARATION,
        deferred_binding=binding,
    )
    assert blocked is not None
    assert blocked["code"] == (
        "conformance_declared_optional_credential_unavailable"
    )
    assert blocked["optional_credential"]["missing_tooling"] is False
    blocked_receipt = _receipt_carrying(
        "receipt_archi_cern_team_lifecycle_blocked.json", blocked
    )
    validate_conformance_exchange(deferred_request, blocked_receipt)
    check_conformance_diagnostic_profiles(deferred_request, blocked_receipt)
    blocked_arm = {
        arm.arm: arm for arm in blocked_receipt.arms
    }["first_publish"]
    assert (blocked_arm.status, blocked_arm.mutation) == ("blocked", "none")

    # 3. Selected WITHOUT a deferred binding: fail, reference-unsupplied.
    supplied_request = _conformance_request(
        "request_archi_cern_team_lifecycle_supplied.json"
    )
    assert supplied_request.inputs.optional_credential_bindings == ()
    failed = diagnostic_from_source_health(
        health, declaration_ref=CONFORMANCE_DECLARATION
    )
    assert failed is not None
    assert failed["code"] == "conformance_secret_reference_invalid"
    assert failed["observed"] == "reference-unsupplied"
    assert (failed["arm"], failed["phase"]) == ("first_publish", "apply")
    failed_receipt = _receipt_carrying(
        "receipt_archi_cern_team_lifecycle_credential_failed.json", failed
    )
    validate_conformance_exchange(supplied_request, failed_receipt)
    check_conformance_diagnostic_profiles(supplied_request, failed_receipt)
    # The variable name never reaches the wire diagnostic.
    assert UNSET_CREDENTIAL_REF not in json.dumps(failed)


@pytest.mark.postgres
def test_real_ingest_credential_failure_refuses_the_wrong_profile(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The observed health does not license whichever profile a receipt claims."""
    monkeypatch.delenv(UNSET_CREDENTIAL_REF, raising=False)
    assert UNSET_CREDENTIAL_REF not in os.environ
    from okg.substrate.distributions.conformance_diagnostics import (
        ConformanceProfileError,
        check_conformance_diagnostic_profiles,
        diagnostic_from_source_health,
    )
    from okg.substrate.contracts.deployment_product.conformance import (
        validate_conformance_exchange,
    )

    dep = _write_credential_gated_deployment(
        tmp_path,
        adapter_class="CredentialGatedFixtureSource",
        params={
            "credential_ref": UNSET_CREDENTIAL_REF,
            "node_id": "svc:gated",
        },
    )
    outcome = _run_credential_gated_ingest(clean_runtime, dep)
    health = outcome.report["results"][0]["source_health"]
    assert health["status"] == "missing_credential"

    # The declaration DOES carry a deferred binding, so the licensed profile
    # is the blocked one. A receipt reporting the fail profile instead is
    # refused rather than accepted as a second reading of the same health.
    deferred_request = _conformance_request(
        "request_archi_cern_team_lifecycle.json"
    )
    wrong = diagnostic_from_source_health(
        health, declaration_ref=CONFORMANCE_DECLARATION
    )
    assert wrong is not None
    assert wrong["code"] == "conformance_secret_reference_invalid"
    from okg.substrate.contracts.deployment_product.conformance import (
        load_conformance_receipt,
    )
    from okg.substrate.distributions.conformance_diagnostics import (
        failed_arm_result,
    )

    payload = load_conformance_receipt(
        (
            CONFORMANCE_FIXTURES
            / "receipt_archi_cern_team_lifecycle_blocked.json"
        ).read_bytes()
    ).model_dump(mode="json")
    payload["diagnostics"] = [
        wrong
        if item["code"] == (
            "conformance_declared_optional_credential_unavailable"
        )
        else item
        for item in payload["diagnostics"]
    ]
    for arm in payload["arms"]:
        if arm["arm"] == "first_publish":
            arm.update(
                failed_arm_result(
                    "first_publish",
                    ("conformance_secret_reference_invalid",),
                    mutation="none",
                    retry_classification="safe",
                )
            )
    payload["verdict"] = "fail"
    payload["claim"]["completed_arms"] = ["contract", "first_publish"]
    mismatched = _seal_conformance_receipt(payload)
    validate_conformance_exchange(deferred_request, mismatched)
    with pytest.raises(ConformanceProfileError) as refused:
        check_conformance_diagnostic_profiles(deferred_request, mismatched)
    assert refused.value.data["reason"] == "declaration_has_deferred_binding"
    assert refused.value.to_dict()["code"] == "conformance_profile_refused"


@pytest.mark.postgres
def test_real_ingest_runs_healthy_once_the_credential_is_supplied(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The control: the SAME connector is healthy when the variable is set.

    Without this, a `missing_credential` verdict could equally well be a
    fixture that always reports failure.
    """
    monkeypatch.setenv(UNSET_CREDENTIAL_REF, "fixture-token-value")
    from okg.substrate.distributions.conformance_diagnostics import (
        diagnostic_from_source_health,
    )

    dep = _write_credential_gated_deployment(
        tmp_path,
        adapter_class="CredentialGatedFixtureSource",
        params={
            "credential_ref": UNSET_CREDENTIAL_REF,
            "node_id": "svc:gated",
        },
    )
    outcome = _run_credential_gated_ingest(clean_runtime, dep)

    assert outcome.any_failed is False
    record = outcome.report["results"][0]
    assert record["status"] == "OK", record
    assert record["source_health"]["status"] == "ok"
    assert record["nodes_appended"] == 1
    # A healthy run raises no conformance diagnostic at all.
    assert diagnostic_from_source_health(
        record["source_health"], declaration_ref=CONFORMANCE_DECLARATION
    ) is None
    # The supplied value never reaches the per-source record -- the whole
    # record, not just its health, so a leak into any other field is caught.
    assert "fixture-token-value" not in json.dumps(record)


class _RejectingAuthorityHandler(BaseHTTPRequestHandler):
    """Answers any bearer token but one with a login page, like a real portal."""

    expected_token = "the-only-accepted-token"

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract.
        presented = self.headers.get("Authorization", "")
        if presented == f"Bearer {self.expected_token}":
            body = b"accepted"
            self.send_response(200)
        else:
            body = (
                b"<html><body><form method='post'>"
                b"<input name='password' type='password'></form>"
                b"</body></html>"
            )
            self.send_response(401)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        return


@pytest.mark.postgres
def test_real_ingest_rejected_token_maps_onto_authentication_failed(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A supplied-but-refused token is `auth_failed`, not `missing_credential`.

    The token is supplied from the environment and rejected by a real
    authority over a real request, so the two credential failures are
    distinguished by what actually happened rather than by a fixture switch.
    """
    from okg.substrate.distributions.conformance_diagnostics import (
        check_conformance_diagnostic_profiles,
        diagnostic_from_source_health,
    )
    from okg.substrate.contracts.deployment_product.conformance import (
        validate_conformance_exchange,
    )

    rejected_value = "a-token-this-authority-refuses"
    monkeypatch.setenv(UNSET_CREDENTIAL_REF, rejected_value)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _RejectingAuthorityHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        endpoint = f"http://127.0.0.1:{server.server_address[1]}/records"
        dep = _write_credential_gated_deployment(
            tmp_path,
            adapter_class="TokenRejectedFixtureSource",
            params={
                "credential_ref": UNSET_CREDENTIAL_REF,
                "endpoint": endpoint,
                "node_id": "svc:gated",
            },
            policy_overrides={"live_call_allowed": True},
        )
        outcome = _run_credential_gated_ingest(clean_runtime, dep)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)

    assert outcome.any_failed is True
    record = outcome.report["results"][0]
    assert record["status"] == "FAIL", record
    assert record["admission_reject_reason"] == "source_health_not_healthy"
    health = record["source_health"]
    assert health["status"] == "auth_failed", health
    # The credential was supplied, so no reference is reported as absent, and
    # the refused value never reaches the record.
    assert health["credential_refs"] == []
    assert rejected_value not in json.dumps(record)

    request = _conformance_request(
        "request_archi_cern_team_lifecycle_supplied.json"
    )
    diagnostic = diagnostic_from_source_health(
        health, declaration_ref=CONFORMANCE_DECLARATION
    )
    assert diagnostic is not None
    assert diagnostic["code"] == "conformance_secret_reference_invalid"
    assert diagnostic["observed"] == "authentication-failed"
    receipt = _receipt_carrying(
        "receipt_archi_cern_team_lifecycle_credential_failed.json", diagnostic
    )
    validate_conformance_exchange(request, receipt)
    check_conformance_diagnostic_profiles(request, receipt)


# ---------------------------------------------------------------------------
# #3927: a health rejection names the connector's reason and the other sources
# ---------------------------------------------------------------------------


def _write_mixed_health_deployment(tmp_path: Path) -> Path:
    """Three sources: `gated` is unhealthy (its credential is unset), `steady`
    is an independent healthy peer, and `after_gated` depends on `gated`."""

    dep = _write_credential_gated_deployment(
        tmp_path,
        adapter_class="CredentialGatedFixtureSource",
        params={
            "credential_ref": UNSET_CREDENTIAL_REF,
            "node_id": "svc:gated",
        },
    )
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    for name, node_id, predecessor in (
        ("steady", "svc:steady", None),
        ("after_gated", "svc:after-gated", "gated"),
    ):
        fixture = dep / f"{name}.json"
        fixture.write_text(json.dumps({
            "nodes": [{
                "node_id": node_id,
                "subtype": "document",
                "attrs": {"text": name},
                "source_record_id": {"path": fixture.name},
            }],
            "edges": [],
        }))
        registry["sources"][name] = {
            **_source(f"svc.{name}", predecessor_source=predecessor),
            "module": "okg.substrate.library.sources.bootstrap_fixture",
            "class": "BootstrapFixtureSource",
            "params": {"fixture_path": str(fixture)},
            "source_policy": _fixture_source_privacy_policy(),
            "sync": {
                "triggers": ["manual", "reconcile"],
                "default_event_mode": "scope_complete",
                "reconcile_mode": "scope_complete",
            },
        }
    registry_path.write_text(yaml.safe_dump(registry))
    _commit_layered_fixture_deployment(dep, message="mixed health fixture")
    return dep


def _run_mixed_health_ingest(dsn: str, dep: Path, *, publish: bool):
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    _claim_layered_fixture_catalog(dsn, dep)
    return run_ingest(
        IngestOptions(
            dsn=dsn,
            deployment="svc",
            deployment_dir=dep,
            publish=publish,
        ),
    )


def _mixed_health_ingest_cli(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    dsn: str,
    dep: Path,
    *flags: str,
    as_json: bool,
):
    """Run the real `okg ingest` command function on the mixed deployment."""
    from okg.substrate.cli import main as cli_main

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    _claim_layered_fixture_catalog(dsn, dep)
    monkeypatch.setattr(cli_main, "_resolve_dsn", lambda args: dsn)
    args = cli_main._build_parser().parse_args(
        ["ingest", "--deployment", str(dep), *flags],
    )
    rc = cli_main._cmd_ingest(args, as_json=as_json)
    captured = capsys.readouterr()
    return rc, captured.out, captured.err


@pytest.mark.postgres
@pytest.mark.parametrize("publish", [True, False])
def test_health_rejection_names_the_reason_and_the_other_sources(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    publish: bool,
) -> None:
    monkeypatch.delenv(UNSET_CREDENTIAL_REF, raising=False)
    assert UNSET_CREDENTIAL_REF not in os.environ
    dep = _write_mixed_health_deployment(tmp_path)

    outcome = _run_mixed_health_ingest(clean_runtime, dep, publish=publish)

    assert outcome.any_failed is True
    by_name = {r["name"]: r for r in outcome.report["results"]}
    gated = by_name["gated"]
    assert gated["status"] == "FAIL", gated
    # The refusal carries the connector's own reason, not only the code.
    said = gated["source_health"]["reason"]
    assert said
    assert gated["error"].startswith(
        "source admission rejected: source_health_not_healthy ("
    )
    assert "missing_credential" in gated["error"]
    assert said in gated["error"]
    assert by_name["steady"]["status"] == "OK"

    # ... and the note built from this run's own report says what became of
    # the other sources.
    from okg.substrate.ingest.failure_followup import (
        other_sources_after_failure,
    )

    others = other_sources_after_failure(
        deployment="svc",
        completeness=outcome.report["completeness"],
        publishes=outcome.report["publishes"],
    )
    assert others is not None
    assert others["failed"] == ["gated"]
    assert others["not_run_depends_on_failed"] == ["after_gated"]
    if publish:
        assert others["published_without_failed"] == ["steady"], (
            outcome.report["publishes"], outcome.report["completeness"],
        )
        assert others["ingested_not_published"] == []
        assert "1 other source was published without gated: steady." in (
            others["message"]
        )
    else:
        assert others["published_without_failed"] == []
        assert others["ingested_not_published"] == ["steady"]
        assert "1 other source was ingested but not published: steady." in (
            others["message"]
        )
    assert (
        "1 source that depends on gated did not run: after_gated."
        in others["message"]
    )
    assert "okg ingest --deployment svc --exclude gated" in others["next_step"]
    assert "one run" in others["next_step"]


@pytest.mark.postgres
def test_okg_ingest_text_prints_the_other_sources_note(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv(UNSET_CREDENTIAL_REF, raising=False)
    dep = _write_mixed_health_deployment(tmp_path)

    rc, out, _err = _mixed_health_ingest_cli(
        monkeypatch, capsys, clean_runtime, dep, "--inline", as_json=False,
    )

    assert rc == 1
    lines = out.splitlines()
    # The two note lines come after the report, and name the connector's
    # reason in the report above them.
    assert "1 other source was published without gated: steady. " in lines[-2]
    assert "1 source that depends on gated did not run: after_gated." in lines[-2]
    assert lines[-1].startswith("To go on, fix gated and run `okg ingest --inline")
    assert "--exclude gated`" in lines[-1]
    assert "missing_credential" in out


@pytest.mark.postgres
def test_okg_ingest_json_carries_the_other_sources_note(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv(UNSET_CREDENTIAL_REF, raising=False)
    dep = _write_mixed_health_deployment(tmp_path)

    rc, out, _err = _mixed_health_ingest_cli(
        monkeypatch, capsys, clean_runtime, dep, as_json=True,
    )

    assert rc == 1
    report = json.loads(out.strip().splitlines()[-1])
    others = report["other_sources"]
    assert others["published_without_failed"] == ["steady"]
    assert others["not_run_depends_on_failed"] == ["after_gated"]
    # Not asked to run inline: the suggested command does not add the flag.
    assert "--inline" not in others["next_step"]
    gated = {r["name"]: r for r in report["results"]}["gated"]
    assert "missing_credential" in gated["error"]


@pytest.mark.postgres
def test_healthy_okg_ingest_has_no_other_sources_note(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # The same deployment with the credential supplied: nothing failed, so
    # there is no failure note and no `other_sources` key.
    monkeypatch.setenv(UNSET_CREDENTIAL_REF, "supplied-for-this-test")
    dep = _write_mixed_health_deployment(tmp_path)

    rc, out, _err = _mixed_health_ingest_cli(
        monkeypatch, capsys, clean_runtime, dep, "--no-publish", as_json=True,
    )

    assert rc == 0
    assert "other_sources" not in json.loads(out.strip().splitlines()[-1])


def test_admission_rejection_error_without_a_reason_says_so() -> None:
    from okg.deployment import ConnectorHealth

    bare = ConnectorHealth(status="endpoint_failed")
    assert ingest_service._admission_rejection_error(
        "source_health_not_healthy", bare,
    ) == (
        "source admission rejected: source_health_not_healthy "
        "(the connector reported endpoint_failed and gave no reason)"
    )
    # Other rejection codes stay bare.
    assert ingest_service._admission_rejection_error(
        "producer_profile_not_writable", bare,
    ) == "source admission rejected: producer_profile_not_writable"
    assert ingest_service._admission_rejection_error(
        "source_health_not_healthy", None,
    ) == "source admission rejected: source_health_not_healthy"


def test_other_sources_note_is_absent_when_there_is_nothing_to_say() -> None:
    from okg.substrate.ingest.failure_followup import (
        other_sources_after_failure,
        text_lines,
    )

    def note(completeness):
        return other_sources_after_failure(
            deployment="svc", completeness=completeness, publishes=[],
        )

    assert note(None) is None
    assert note({"failed_sources": [], "completed_sources": ["a"]}) is None
    # The failed source was the only one selected: no other source to name.
    assert note({
        "failed_sources": [{"source": "a"}],
        "completed_sources": [],
        "dependency_blocked_sources": [],
    }) is None
    assert text_lines({"results": []}) == []


def test_other_sources_note_uses_plural_wording() -> None:
    from okg.substrate.ingest.failure_followup import (
        other_sources_after_failure,
        text_lines,
    )

    result = other_sources_after_failure(
        deployment="svc",
        completeness={
            "failed_sources": [{"source": "cds"}],
            "completed_sources": ["a", "b"],
            "dependency_blocked_sources": [
                {"source": "x", "blocked_by": ["cds"]},
                {"source": "y", "blocked_by": ["cds"]},
            ],
        },
        publishes=[{"label": "commit", "status": "published"}],
    )

    assert result is not None
    assert result["message"] == (
        "2 other sources were published without cds: a, b. "
        "2 sources that depend on cds did not run: x, y."
    )
    assert text_lines({"other_sources": result}) == [
        result["message"], result["next_step"],
    ]


_NOTE_COMPLETENESS = {
    "failed_sources": [{"source": "cds"}],
    "completed_sources": ["notes"],
    "dependency_blocked_sources": [],
}


def _note_for(publishes, **kwargs):
    from okg.substrate.ingest.failure_followup import (
        other_sources_after_failure,
    )

    return other_sources_after_failure(
        deployment="svc",
        completeness=_NOTE_COMPLETENESS,
        publishes=publishes,
        **kwargs,
    )


@pytest.mark.parametrize(
    ("final", "published"),
    [
        pytest.param(
            {"label": "commit", "status": "published",
             "generation_id": "g2", "predecessor_generation_id": "g1"},
            True, id="published",
        ),
        pytest.param(
            {"label": "commit", "status": "published",
             "generation_id": "g1", "predecessor_generation_id": "g1"},
            True, id="no-change",
        ),
        pytest.param(
            {"label": "commit", "status": "published",
             "generation_id": "g2", "phase_b_blocked": True},
            False, id="phase-b-blocked",
        ),
        pytest.param(
            {"label": "commit", "status": "published",
             "generation_id": "g2",
             "blocked_reason": {"reason": "gate failed"}},
            False, id="blocked-reason",
        ),
        pytest.param(
            {"label": "commit", "status": "fail", "error": "boom"},
            False, id="failed",
        ),
        pytest.param(
            {"label": "commit", "status": "skipped"},
            False, id="skipped",
        ),
    ],
)
def test_other_sources_note_judges_the_final_publish_by_its_outcome(
    final: dict, published: bool,
) -> None:
    note = _note_for([final])

    assert note is not None
    assert (note["published_without_failed"] == ["notes"]) is published
    assert (note["ingested_not_published"] == ["notes"]) is not published


def test_other_sources_note_ignores_the_resume_publishes_before_the_run() -> None:
    # A staged-source redrive or a recovery promotion published, but they
    # say nothing about the sources of THIS run: its own last publish
    # (label commit) was blocked.
    note = _note_for([
        {"label": "staged-source-redrive", "status": "published",
         "generation_id": "g2", "predecessor_generation_id": "g1"},
        {"label": "recovery-promotion", "status": "published",
         "generation_id": "g3", "predecessor_generation_id": "g2"},
        {"label": "commit", "status": "published", "generation_id": "g4",
         "phase_b_blocked": True},
    ])
    assert note is not None
    assert note["ingested_not_published"] == ["notes"]
    assert note["published_without_failed"] == []
    # With no commit publish at all, nothing of this run was published.
    only_resume = _note_for([
        {"label": "staged-source-redrive", "status": "published",
         "generation_id": "g2", "predecessor_generation_id": "g1"},
    ])
    assert only_resume is not None
    assert only_resume["ingested_not_published"] == ["notes"]


def test_other_sources_note_advises_inline_only_when_run_inline() -> None:
    publishes = [{"label": "commit", "status": "published",
                  "generation_id": "g2", "predecessor_generation_id": "g1"}]
    plain = _note_for(publishes)
    inline = _note_for(publishes, inline_flag=True)
    assert plain is not None and inline is not None
    assert "okg ingest --deployment svc --exclude cds" in plain["next_step"]
    assert "--inline" not in plain["next_step"]
    assert "okg ingest --inline --deployment svc --exclude cds" in inline["next_step"]
    assert "later run includes it again" in inline["next_step"]
