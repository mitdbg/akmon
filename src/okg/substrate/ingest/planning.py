"""Pure source-selection and dependency planning for substrate ingest."""
from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Literal

from okg.substrate.errors import SourceActivationProbeInvalid, SubstrateError
from okg.substrate.source_sync import (
    SourceSyncDependentFanout,
    SourceSyncEvent,
)

if TYPE_CHECKING:
    from okg.substrate.sources.registry import SourceEntry
else:
    # Preserve runtime annotation resolution without importing the registry's
    # database-backed producer-authority dependencies into this pure module.
    SourceEntry = Any


@dataclass(frozen=True)
class IngestPlan:
    """Resolved source subset and dependency layers."""

    runnable: dict[str, SourceEntry]
    deps: dict[str, set[str]]
    layers: list[list[str]]
    ordered: tuple[str, ...]


@dataclass(frozen=True)
class ActivationProbeResult:
    """Successfully acquired activation-probe findings for one source."""

    status: Literal["valid", "invalid"]
    findings: tuple[dict[str, Any], ...]

    def __post_init__(self) -> None:
        if self.status not in ("valid", "invalid"):
            raise ValueError(f"unknown activation probe status: {self.status!r}")
        if self.status == "valid" and self.findings:
            raise ValueError("a valid activation probe cannot contain findings")
        if self.status == "invalid" and not self.findings:
            raise ValueError("an invalid activation probe must contain findings")

    @classmethod
    def from_findings(
        cls,
        findings: list[dict[str, Any]],
    ) -> "ActivationProbeResult":
        """Classify a completed probe without hiding acquisition failures."""
        return cls(
            status="invalid" if findings else "valid",
            findings=tuple(findings),
        )


def plan_ingest_sources(
    registry: Mapping[str, SourceEntry],
    *,
    include: Collection[str] | None = None,
    exclude: Collection[str] = (),
    include_predecessors: bool = True,
    include_dependents: bool = False,
    dependent_fanout: SourceSyncDependentFanout | None = None,
) -> IngestPlan:
    """Resolve runnable sources, dependency closure, and topo layers.

    `include` entries may name registry keys or ownership IDs.
    `exclude` always wins over closure expansion.
    """
    excluded = _resolve_source_names(registry, set(exclude))
    candidates = {
        name: entry
        for name, entry in registry.items()
        if not entry.legacy
        and not entry.is_registry_only
        and name not in excluded
    }
    all_deps = resolve_predecessor_deps(candidates)
    if include is None:
        # An absent flag preserves the historical runnable-by-default behavior;
        # only an explicit false opts a source out of a bare ingest.
        selected = {
            name
            for name, entry in candidates.items()
            if entry.raw.get("enabled_by_default") is not False
        }
    else:
        selected = _resolve_source_names(candidates, set(include))

    if include_predecessors:
        selected = _with_predecessor_closure(selected, all_deps)
    fanout = _resolve_dependent_fanout(
        include_dependents=include_dependents,
        dependent_fanout=dependent_fanout,
    )
    if fanout == "direct":
        selected = _with_direct_dependents(selected, all_deps)
    elif fanout == "closure":
        selected = _with_dependent_closure(selected, all_deps)
        if include_predecessors:
            selected = _with_predecessor_closure(selected, all_deps)

    selected -= excluded
    runnable = {
        name: candidates[name]
        for name in sorted(selected)
        if name in candidates
    }
    deps = resolve_predecessor_deps(runnable)
    layers = topo_layers(deps)
    if layers is None:
        raise _source_registry_cycle(deps)
    return IngestPlan(
        runnable=runnable,
        deps=deps,
        layers=layers,
        ordered=tuple(name for layer in layers for name in layer),
    )


def validate_deferred_source_activation(
    plan: IngestPlan,
    probe_results: Mapping[str, ActivationProbeResult],
) -> None:
    """Validate completed dormant-source probes without acquiring effects."""
    blocked: dict[str, list[dict[str, Any]]] = {}
    for name, entry in plan.runnable.items():
        if entry.raw.get("enabled_by_default") is not False:
            continue
        result = probe_results[name]
        if result.status == "invalid":
            blocked[name] = list(result.findings)
    if not blocked:
        return
    raise SourceActivationProbeInvalid(
        "explicitly selected dormant sources do not satisfy the activation "
        "probe contract",
        data={"sources": blocked},
        fix_hint=(
            "add a sound source-owned runtime change probe and supported poll "
            "selector before enabling or explicitly ingesting these sources"
        ),
    )


def source_events_with_inherited_path_scope(
    source_events: Mapping[str, tuple[SourceSyncEvent, ...]],
    plan: IngestPlan,
) -> Mapping[str, tuple[SourceSyncEvent, ...]]:
    """Let path-scoped dependents consume predecessor file-event scope."""
    if not source_events:
        return source_events

    original = {
        name: tuple(events)
        for name, events in source_events.items()
    }
    merged: dict[str, tuple[SourceSyncEvent, ...]] = dict(original)
    changed = False
    for name in plan.ordered:
        if merged.get(name):
            continue
        entry = plan.runnable.get(name)
        if entry is None or not _source_accepts_path_scope(entry):
            continue
        inherited: list[SourceSyncEvent] = []
        for predecessor in _predecessor_closure_names(name, plan.deps):
            for event in original.get(predecessor, ()):
                if event.trigger_kind == "reconcile" or not event.scope_key:
                    continue
                inherited.append(_inherit_source_sync_event(event, name))
        if inherited:
            merged[name] = tuple(inherited)
            changed = True

    return merged if changed else source_events


def _source_accepts_path_scope(entry: SourceEntry) -> bool:
    sync = entry.sync
    scope = getattr(sync, "scope", None) if sync is not None else None
    accepts = set(getattr(scope, "accepts", ()) or ())
    return "path" in accepts


def _predecessor_closure_names(
    name: str,
    deps: Mapping[str, set[str]],
) -> tuple[str, ...]:
    seen: set[str] = set()
    queue = list(sorted(deps.get(name, set())))
    while queue:
        predecessor = queue.pop(0)
        if predecessor in seen:
            continue
        seen.add(predecessor)
        queue.extend(sorted(deps.get(predecessor, set()) - seen))
    return tuple(sorted(seen))


def _inherit_source_sync_event(
    event: SourceSyncEvent,
    target_source_name: str,
) -> SourceSyncEvent:
    reason = event.reason or event.trigger_kind
    event_id = (
        f"{event.event_id}:inherited:{target_source_name}"
        if event.event_id
        else None
    )
    return replace(
        event,
        source_name=target_source_name,
        event_id=event_id,
        reason=f"inherited:{event.source_name}:{reason}",
    )


def topo_sort(deps: dict[str, set[str]]) -> list[str] | None:
    """Standard Kahn topo-sort. Returns ordered list, or None on cycle."""
    layers = topo_layers(deps)
    if layers is None:
        return None
    return [name for layer in layers for name in layer]


def topo_layers(deps: dict[str, set[str]]) -> list[list[str]] | None:
    """Kahn topo-sort grouped by dependency depth."""
    indegree = {name: len(d) for name, d in deps.items()}
    rev: dict[str, set[str]] = {name: set() for name in deps}
    for name, dep_names in deps.items():
        for dep_name in dep_names:
            rev.setdefault(dep_name, set()).add(name)
    queue = sorted([n for n, deg in indegree.items() if deg == 0])
    out: list[str] = []
    layers: list[list[str]] = []
    while queue:
        layer = queue
        queue = []
        layers.append(layer)
        for name in layer:
            out.append(name)
            for child in sorted(rev.get(name, ())):
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)
        queue.sort()
    if len(out) != len(deps):
        return None
    return layers


def resolve_predecessor_deps(
    runnable: dict[str, SourceEntry],
) -> dict[str, set[str]]:
    """Resolve source-registry predecessor declarations."""
    by_ownership: dict[str, str] = {
        entry.ownership_id: name
        for name, entry in runnable.items()
        if getattr(entry, "ownership_id", None)
    }
    deps: dict[str, set[str]] = {}
    for name, entry in runnable.items():
        raw = entry.raw
        pred_raw = raw.get("predecessor_source")
        if pred_raw is None:
            pred_raw = (raw.get("params") or {}).get("predecessor_source")
        if isinstance(pred_raw, str):
            pred_list = [pred_raw]
        elif isinstance(pred_raw, (list, tuple)):
            pred_list = [p for p in pred_raw if isinstance(p, str)]
        else:
            pred_list = []
        resolved: set[str] = set()
        for pred in pred_list:
            if pred in runnable:
                resolved.add(pred)
            elif pred in by_ownership:
                resolved.add(by_ownership[pred])
        deps[name] = resolved
    return deps


def _resolve_source_names(
    registry: Mapping[str, SourceEntry],
    requested: set[str],
) -> set[str]:
    by_ownership = {
        entry.ownership_id: name
        for name, entry in registry.items()
        if getattr(entry, "ownership_id", None)
    }
    resolved: set[str] = set()
    for item in requested:
        if item in registry:
            resolved.add(item)
        elif item in by_ownership:
            resolved.add(by_ownership[item])
    return resolved


def _with_predecessor_closure(
    selected: set[str],
    deps: Mapping[str, set[str]],
) -> set[str]:
    out = set(selected)
    queue = list(selected)
    while queue:
        name = queue.pop()
        for dep_name in deps.get(name, set()):
            if dep_name not in out:
                out.add(dep_name)
                queue.append(dep_name)
    return out


def _with_dependent_closure(
    selected: set[str],
    deps: Mapping[str, set[str]],
) -> set[str]:
    reverse: dict[str, set[str]] = {name: set() for name in deps}
    for name, dep_names in deps.items():
        for dep_name in dep_names:
            reverse.setdefault(dep_name, set()).add(name)
    out = set(selected)
    queue = list(selected)
    while queue:
        name = queue.pop()
        for dependent in reverse.get(name, set()):
            if dependent not in out:
                out.add(dependent)
                queue.append(dependent)
    return out


def _with_direct_dependents(
    selected: set[str],
    deps: Mapping[str, set[str]],
) -> set[str]:
    reverse: dict[str, set[str]] = {name: set() for name in deps}
    for name, dep_names in deps.items():
        for dep_name in dep_names:
            reverse.setdefault(dep_name, set()).add(name)
    out = set(selected)
    for name in selected:
        out.update(reverse.get(name, set()))
    return out


def _resolve_dependent_fanout(
    *,
    include_dependents: bool,
    dependent_fanout: SourceSyncDependentFanout | None,
) -> SourceSyncDependentFanout:
    if dependent_fanout is not None:
        return dependent_fanout
    return "closure" if include_dependents else "none"


def _source_registry_cycle(deps: Mapping[str, set[str]]) -> SubstrateError:
    err = SubstrateError(
        "cycle in predecessor_source declarations",
        data={"deps": {k: list(v) for k, v in deps.items()}},
    )
    err.code = "source_registry_cycle"
    return err


__all__ = [
    "ActivationProbeResult",
    "IngestPlan",
    "plan_ingest_sources",
    "resolve_predecessor_deps",
    "source_events_with_inherited_path_scope",
    "topo_layers",
    "topo_sort",
    "validate_deferred_source_activation",
]
