"""Effect-free dependency planning for the derived-artifact scheduler.

This module owns the scheduler's dependency *rules*: which tasks are
admissible, which dependency edges are known, what deterministic order
they run in, and what canonical payload identifies the resulting graph.
It is given already-loaded dependency records and returns plain data or
raises :class:`DerivedArtifactScheduleError`. It never loads, writes, or
opens a connection.

It therefore imports no database, publisher, or scheduler module, which
is what lets a maintainer change a rule here and prove it with a
database-free test. ``scheduler`` keeps SQL loading, the lineage query,
plan creation, transactions and timing, and re-exports the names defined
here so no caller sees a new type.

Timing is a caller-supplied callback, not a dependency: pass
``record_timing`` and this module will report the same labels the
scheduler has always reported, at the same boundaries.
"""
from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Generic, Protocol, TypeVar

from okg.substrate.derived_artifacts.hashing import (
    RELATION_SLOT_NAME,
    stable_hash,
)


class DerivedArtifactScheduleError(Exception):
    """Raised when a derived-artifact plan is invalid or cannot bind."""


@dataclass(frozen=True)
class DerivedArtifactDependency:
    """Logical dependency between two derived artifact definitions."""

    downstream_definition_id: str
    upstream_definition_id: str
    upstream_slot_name: str = RELATION_SLOT_NAME
    dependency_kind: str = "manifest_slot"
    scope_key: str | None = None
    required_compute_hash: str | None = None
    required_output_schema_hash: str | None = None
    required_renderer_hash: str | None = None
    required_dirty_rule_hash: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


class _DefinitionIdentity(Protocol):
    """The only thing planning reads from a task's definition."""

    @property
    def definition_id(self) -> str:
        """Identify the derived artifact this task produces."""


class PlannableTask(Protocol):
    """A schedule task, seen through the one field planning needs.

    Structural on purpose: the concrete task types stay in ``scheduler``
    because planning never needs their rows, outputs, dirty scopes or
    metrics.
    """

    @property
    def definition(self) -> _DefinitionIdentity:
        """Carry the definition whose id orders and hashes this task."""


TaskT = TypeVar("TaskT", bound=PlannableTask)


@dataclass(frozen=True)
class DependencyPlan(Generic[TaskT]):
    """The plain result of planning one dependency graph."""

    task_by_id: Mapping[str, TaskT]
    topological_order: list[str]
    graph_hash: str


def build_task_map(tasks: Sequence[TaskT]) -> dict[str, TaskT]:
    """Index admitted tasks by definition id.

    Separate from :func:`plan_dependency_graph` so the scheduler can
    refuse a duplicate or empty task set *before* it writes any registry
    row.
    """
    task_by_id: dict[str, TaskT] = {}
    for task in tasks:
        definition_id = task.definition.definition_id
        if definition_id in task_by_id:
            raise DerivedArtifactScheduleError(
                f"duplicate schedule task for {definition_id!r}"
            )
        task_by_id[definition_id] = task
    if not task_by_id:
        raise DerivedArtifactScheduleError("schedule requires at least one task")
    return task_by_id


def require_known_dependencies(
    task_by_id: Mapping[str, TaskT],
    dependencies: Sequence[DerivedArtifactDependency],
) -> None:
    """Refuse a graph whose upstream tasks are not all admitted."""
    missing = sorted(
        {
            dep.upstream_definition_id
            for dep in dependencies
            if dep.upstream_definition_id not in task_by_id
        }
    )
    if missing:
        raise DerivedArtifactScheduleError(
            "schedule is missing upstream tasks: " + ", ".join(missing)
        )


def topological_order(
    definition_ids: set[str],
    dependencies: Sequence[DerivedArtifactDependency],
) -> list[str]:
    """Order definitions upstream-first, breaking ties alphabetically."""
    outgoing: dict[str, set[str]] = {
        definition_id: set() for definition_id in definition_ids
    }
    indegree: dict[str, int] = {
        definition_id: 0 for definition_id in definition_ids
    }
    for dep in dependencies:
        if dep.upstream_definition_id not in definition_ids:
            continue
        if dep.downstream_definition_id not in definition_ids:
            continue
        if dep.downstream_definition_id not in outgoing[dep.upstream_definition_id]:
            outgoing[dep.upstream_definition_id].add(dep.downstream_definition_id)
            indegree[dep.downstream_definition_id] += 1

    ready = sorted(
        definition_id for definition_id, count in indegree.items()
        if count == 0
    )
    order: list[str] = []
    while ready:
        current = ready.pop(0)
        order.append(current)
        for downstream in sorted(outgoing[current]):
            indegree[downstream] -= 1
            if indegree[downstream] == 0:
                ready.append(downstream)
                ready.sort()

    if len(order) != len(definition_ids):
        cycle_nodes = sorted(
            definition_id for definition_id, count in indegree.items()
            if count > 0
        )
        raise DerivedArtifactScheduleError(
            "derived artifact dependency cycle: " + " -> ".join(cycle_nodes)
        )
    return order


def graph_hash(
    *,
    deployment_name: str,
    execution_phase: str,
    catalog_version_id: int | None,
    definition_ids: Sequence[str],
    dependencies: Sequence[DerivedArtifactDependency],
) -> str:
    """Hash the canonical schedule-graph payload.

    Sequence-sensitive by design: ``dependencies`` is hashed in the order
    given, which is the order the scheduler's SQL read returns, so plan
    identity tracks the loaded edge sequence. ``metadata`` is deliberately
    absent from the payload.
    """
    return stable_hash(
        {
            "kind": "derived_artifact_schedule_graph_v1",
            "deployment_name": deployment_name,
            "execution_phase": execution_phase,
            "catalog_version_id": catalog_version_id,
            "definition_ids": list(definition_ids),
            "dependencies": [
                {
                    "downstream_definition_id": dep.downstream_definition_id,
                    "upstream_definition_id": dep.upstream_definition_id,
                    "upstream_slot_name": dep.upstream_slot_name,
                    "dependency_kind": dep.dependency_kind,
                    "scope_key": dep.scope_key,
                    "required_compute_hash": dep.required_compute_hash,
                    "required_output_schema_hash": (
                        dep.required_output_schema_hash
                    ),
                    "required_renderer_hash": dep.required_renderer_hash,
                    "required_dirty_rule_hash": dep.required_dirty_rule_hash,
                }
                for dep in dependencies
            ],
        }
    )


def plan_dependency_graph(
    *,
    deployment_name: str,
    execution_phase: str,
    catalog_version_id: int | None,
    tasks: Sequence[TaskT],
    dependencies: Sequence[DerivedArtifactDependency],
    record_timing: Callable[[str, float], None] | None = None,
) -> DependencyPlan[TaskT]:
    """Validate, order and hash one dependency graph.

    ``dependencies`` must already be loaded; this function never reads
    them. ``record_timing`` is called as ``(label, started)`` where
    ``started`` is a :func:`time.perf_counter` reading, matching the
    scheduler's own ``add_timing`` so the reported labels and their
    accumulation are unchanged.
    """
    task_by_id = build_task_map(tasks)

    started = time.perf_counter()
    require_known_dependencies(task_by_id, dependencies)
    order = topological_order(set(task_by_id), dependencies)
    if record_timing is not None:
        record_timing("topological_order", started)

    started = time.perf_counter()
    computed_graph_hash = graph_hash(
        deployment_name=deployment_name,
        execution_phase=execution_phase,
        catalog_version_id=catalog_version_id,
        definition_ids=order,
        dependencies=dependencies,
    )
    if record_timing is not None:
        record_timing("graph_hash", started)

    return DependencyPlan(
        task_by_id=task_by_id,
        topological_order=order,
        graph_hash=computed_graph_hash,
    )
