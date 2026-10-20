from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any, get_type_hints

import pytest

import okg.substrate.ingest as ingest_api
import okg.substrate.ingest.planning as planning
import okg.substrate.ingest.service as ingest_service
from okg.substrate.errors import SourceActivationProbeInvalid, SubstrateError
from okg.substrate.source_sync import (
    SourceSyncCapabilities,
    SourceSyncEvent,
    SourceSyncScope,
)
from okg.substrate.sources.registry import SourceEntry


def _entry(
    name: str,
    *,
    ownership_id: str | None = None,
    predecessor_source: str | list[str] | tuple[str, ...] | None = None,
    enabled_by_default: bool | None = None,
    accepts_path_scope: bool = False,
) -> SourceEntry:
    owner = ownership_id or f"demo.{name}"
    raw: dict[str, object] = {
        "ownership_id": owner,
        "source_class": "discovery_crawl",
    }
    if predecessor_source is not None:
        raw["predecessor_source"] = predecessor_source
    if enabled_by_default is not None:
        raw["enabled_by_default"] = enabled_by_default
    sync = (
        SourceSyncCapabilities(scope=SourceSyncScope(accepts=("path",)))
        if accepts_path_scope
        else None
    )
    return SourceEntry(
        name=name,
        ownership_id=owner,
        source_class="discovery_crawl",
        record_identity_kind="scoped_locator",
        record_identity_fields=["id"],
        source_revision_kind="content_hash",
        deletion_semantics="missing_from_completed_scope",
        publication_mode="published_generation",
        credential_refs=[],
        credential_aliases={},
        required_for_baseline=False,
        raw=raw,
        sync=sync,
        enabled_by_default=bool(enabled_by_default),
    )


def _registry() -> dict[str, SourceEntry]:
    return {
        "raw": _entry("raw", enabled_by_default=False),
        "side": _entry("side"),
        "parsed": _entry(
            "parsed",
            predecessor_source=("demo.raw", "side"),
        ),
        "index": _entry(
            "index",
            predecessor_source="parsed",
            accepts_path_scope=True,
        ),
    }


def test_service_facade_matches_planning_owner() -> None:
    assert planning.IngestPlan.__module__ == "okg.substrate.ingest.planning"
    assert planning.plan_ingest_sources.__module__ == "okg.substrate.ingest.planning"
    assert ingest_service.IngestPlan is planning.IngestPlan
    assert ingest_service.plan_ingest_sources is planning.plan_ingest_sources
    assert ingest_service.resolve_predecessor_deps is planning.resolve_predecessor_deps
    assert ingest_service.topo_layers is planning.topo_layers
    assert ingest_service.topo_sort is planning.topo_sort
    assert ingest_api.IngestPlan is planning.IngestPlan
    assert ingest_api.plan_ingest_sources is planning.plan_ingest_sources
    assert ingest_api.resolve_predecessor_deps is planning.resolve_predecessor_deps
    assert ingest_api.topo_layers is planning.topo_layers
    assert ingest_api.topo_sort is planning.topo_sort
    assert (
        ingest_service._source_events_with_inherited_path_scope
        is planning.source_events_with_inherited_path_scope
    )
    assert get_type_hints(ingest_service.IngestPlan)["runnable"] == dict[
        str, Any
    ]
    assert "registry" in get_type_hints(planning.plan_ingest_sources)


def test_planning_owner_preserves_selection_order_and_mutable_plan_fields() -> None:
    registry = _registry()

    plan = planning.plan_ingest_sources(registry, include={"demo.index"})

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
    assert planning.topo_sort(plan.deps) == list(plan.ordered)
    assert planning.topo_layers(plan.deps) == plan.layers

    plan.runnable["alias"] = plan.runnable["index"]
    removed_alias = plan.runnable.pop("alias")
    assert removed_alias is registry["index"]
    plan.deps["index"].add("side")
    assert "side" in plan.deps["index"]
    plan.deps["index"].remove("side")
    plan.layers[0].append("sentinel")
    removed_sentinel = plan.layers[0].pop()
    assert removed_sentinel == "sentinel"
    with pytest.raises(FrozenInstanceError):
        plan.ordered = ()


def test_unknown_selectors_and_predecessors_retain_legacy_ignore_behavior() -> None:
    registry = {
        **_registry(),
        "orphan": _entry("orphan", predecessor_source="missing"),
    }

    empty = planning.plan_ingest_sources(registry, include={"missing"})
    orphan = planning.plan_ingest_sources(registry, include={"demo.orphan"})

    assert empty == planning.IngestPlan({}, {}, [], ())
    assert orphan.deps == {"orphan": set()}
    assert orphan.ordered == ("orphan",)


def test_closure_fanout_exclusion_and_params_predecessor_stay_exact() -> None:
    registry = _registry()
    params_child = _entry("params_child")
    params_child.raw["params"] = {"predecessor_source": "demo.raw"}
    registry["params_child"] = params_child

    direct = planning.plan_ingest_sources(
        registry,
        include={"demo.raw"},
        include_predecessors=False,
        dependent_fanout="direct",
    )
    closure = planning.plan_ingest_sources(
        registry,
        include={"demo.raw"},
        include_predecessors=False,
        dependent_fanout="closure",
    )
    explicit_none = planning.plan_ingest_sources(
        registry,
        include={"demo.raw"},
        include_predecessors=False,
        include_dependents=True,
        dependent_fanout="none",
    )
    excluded = planning.plan_ingest_sources(
        registry,
        include={"demo.index"},
        exclude={"parsed"},
    )

    assert direct.ordered == ("raw", "params_child", "parsed")
    assert closure.ordered == ("raw", "params_child", "parsed", "index")
    assert explicit_none.ordered == ("raw",)
    assert excluded.ordered == ("index",)
    assert planning.resolve_predecessor_deps({
        "raw": registry["raw"],
        "params_child": params_child,
    }) == {
        "raw": set(),
        "params_child": {"raw"},
    }


def test_pure_activation_validation_preserves_blocking_error_shape() -> None:
    registry = {
        "first": _entry("first", enabled_by_default=False),
        "second": _entry("second", enabled_by_default=False),
        "active": _entry(
            "active",
            predecessor_source=("first", "second"),
        ),
    }
    plan = planning.plan_ingest_sources(registry, include={"active"})
    valid = planning.ActivationProbeResult(status="valid", findings=())
    first_finding = {
        "code": "deployment.source_registry.probe_missing",
        "severity": "blocker",
    }
    second_finding = {
        "code": "deployment.source_registry.probe_unsound",
        "severity": "blocker",
    }
    first_invalid = planning.ActivationProbeResult(
        status="invalid",
        findings=(first_finding,),
    )
    second_invalid = planning.ActivationProbeResult(
        status="invalid",
        findings=(second_finding,),
    )

    planning.validate_deferred_source_activation(
        plan,
        {"first": valid, "second": valid},
    )
    with pytest.raises(SourceActivationProbeInvalid) as blocked:
        planning.validate_deferred_source_activation(
            plan,
            {"first": first_invalid, "second": second_invalid},
        )

    assert blocked.value.code == "source_activation_probe_invalid"
    assert blocked.value.data == {
        "sources": {
            "first": [first_finding],
            "second": [second_finding],
        },
    }
    with pytest.raises(ValueError):
        planning.ActivationProbeResult(status="valid", findings=(first_finding,))
    with pytest.raises(ValueError):
        planning.ActivationProbeResult(status="invalid", findings=())


def test_path_scope_inheritance_remains_pure_and_exact() -> None:
    plan = planning.plan_ingest_sources(_registry(), include={"demo.index"})
    event = SourceSyncEvent(
        deployment="demo",
        source_name="raw",
        trigger_kind="fs_event",
        scope_key="src/raw.py",
        reason="watch:modified",
        event_id="event-1",
    )
    original = {"raw": (event,)}

    inherited = planning.source_events_with_inherited_path_scope(original, plan)

    assert inherited is not original
    assert inherited["raw"] is original["raw"]
    assert set(inherited) == {"raw", "index"}
    assert inherited["index"] == (
        SourceSyncEvent(
            deployment="demo",
            source_name="index",
            trigger_kind="fs_event",
            scope_key="src/raw.py",
            reason="inherited:raw:watch:modified",
            event_id="event-1:inherited:index",
        ),
    )

    empty: dict[str, tuple[SourceSyncEvent, ...]] = {}
    assert planning.source_events_with_inherited_path_scope(empty, plan) is empty
    own_event = SourceSyncEvent(
        deployment="demo",
        source_name="index",
        trigger_kind="manual",
        scope_key="src/index.py",
    )
    own_events = {"raw": (event,), "index": (own_event,)}
    assert (
        planning.source_events_with_inherited_path_scope(own_events, plan)
        is own_events
    )
    reconcile = SourceSyncEvent(
        deployment="demo",
        source_name="raw",
        trigger_kind="reconcile",
        scope_key="src/raw.py",
    )
    scopeless = SourceSyncEvent(
        deployment="demo",
        source_name="raw",
        trigger_kind="fs_event",
    )
    unusable = {"raw": (reconcile, scopeless)}
    assert (
        planning.source_events_with_inherited_path_scope(unusable, plan)
        is unusable
    )


def test_cycles_remain_loud_and_topology_helpers_return_none() -> None:
    registry = {
        "a": _entry("a", predecessor_source="b"),
        "b": _entry("b", predecessor_source="a"),
    }
    deps = {"a": {"b"}, "b": {"a"}}

    assert planning.topo_layers(deps) is None
    assert planning.topo_sort(deps) is None
    with pytest.raises(SubstrateError) as cycle:
        planning.plan_ingest_sources(registry)

    assert getattr(cycle.value, "code", None) == "source_registry_cycle"
    assert str(cycle.value) == "cycle in predecessor_source declarations"
    assert cycle.value.data == {"deps": {"a": ["b"], "b": ["a"]}}
