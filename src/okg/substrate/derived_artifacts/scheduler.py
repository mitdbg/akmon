"""Deterministic scheduler for relation and view derived artifacts.

The scheduler owns dependency validation, topological order,
immutable upstream manifest binding, carry-forward, and dirty-scope
refresh. It publishes canonical manifests and never projects legacy
graph nodes for view outputs.
"""
from __future__ import annotations

from contextlib import nullcontext
import time
from dataclasses import dataclass, field, replace
from typing import Any, Mapping, Sequence
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.derived_artifacts.dependency_index import (
    INDEXED_DEPENDENCY_KINDS,
    DependencyLookupKey,
    lookup_dirty_outputs,
)
from okg.substrate.derived_artifacts.derivation_registry import (
    DeclaredDeps,
    DerivationRegistry,
    DerivationRow,
    DeterminismClass,
    ExecutionPhase,
    FailureMode,
    GovernanceProfile,
    Materialization,
    OverridePath,
)
from okg.substrate.derived_artifacts.maintained_derivation import (
    ChronosRegisteredTable,
    CorrectnessOracle,
    DerivationSpec,
    MaintainResult,
    ProjectionParityOracle,
    SentinelSpec,
)
from okg.substrate.derived_artifacts.hashing import RELATION_SLOT_NAME
from okg.substrate.derived_artifacts.dependency_planning import (
    DerivedArtifactDependency,
    DerivedArtifactScheduleError,
    build_task_map,
    plan_dependency_graph,
)
from okg.substrate.derived_artifacts.relations import (
    DirtyScope,
    PublishedArtifact,
    RelationArtifactDefinition,
    RelationRow,
    carry_forward_relation_artifact,
    enqueue_dirty_scope,
    pending_dirty_scope_keys,
    publish_relation_artifact,
    refresh_relation_artifact,
    register_jsonb_relation,
    relation_dirty_rows_match_manifest,
    _physical_relation_name,
)
from okg.substrate.derived_artifacts.read_views import (
    ViewSlotNotFound,
    get_view_slot,
)
from okg.substrate.derived_artifacts.views import (
    InlineViewOutput,
    PublishedViewArtifact,
    ViewArtifactDefinition,
    carry_forward_view_artifact,
    outputs_match_manifest_content,
    publish_view_artifact,
    register_view_artifact_definition,
)


# DerivedArtifactScheduleError and DerivedArtifactDependency are defined
# in `dependency_planning`, which owns the rules that raise and inspect
# them, and are imported above. They are re-exported here unchanged: the
# objects are identical, so `except DerivedArtifactScheduleError` and
# `isinstance` keep working for every existing caller.


@dataclass(frozen=True)
class RelationScheduleTask:
    """One relation artifact task admitted to the scheduler."""

    definition: RelationArtifactDefinition
    rows: Sequence[RelationRow]
    dirty_scope_keys: Sequence[str] | None = None
    dirty_rows_known_changed: bool = False
    consume_dirty_scopes: bool = True
    metrics: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ViewScheduleTask:
    """One native view artifact task admitted to the scheduler."""

    definition: ViewArtifactDefinition
    outputs: Sequence[InlineViewOutput]
    dirty_scope_keys: Sequence[str] | None = None
    consume_dirty_scopes: bool = True
    metrics: Mapping[str, Any] = field(default_factory=dict)
    #: Stored on the view's manifest (``inputs_valid_through_generation``,
    #: ``external_inputs_state``, ``inputs_as_of``). The same record is written
    #: whether the scheduler publishes the outputs or stores them as a carry.
    manifest_metadata: Mapping[str, Any] = field(default_factory=dict)


type DerivedScheduleTask = RelationScheduleTask | ViewScheduleTask


@dataclass(frozen=True)
class _ScheduleDerivationRow:
    """A dynamic registry row for one scheduler task.

    Relations/views are already manifest-backed publishers, so this row is the
    governance/dispatch envelope around that existing path rather than a request to
    write through the kit's side-table ``maintain()`` driver.
    """

    task: DerivedScheduleTask
    row: DerivationRow
    task_kind: str
    raw_dirty_rule_deps: tuple[str, ...] = ()
    unsupported_dirty_rule_deps: tuple[str, ...] = ()
    dependency_fallback_used: bool = False


@dataclass(frozen=True)
class LineageInvalidation:
    """One reverse-index invalidation input for scheduler planning."""

    dependency_kind: str
    dependency_ref: str
    dirty_reason: str = "lineage_dependency_changed"
    dependency_hash: str | None = None

    def lookup_key(self) -> DependencyLookupKey:
        return DependencyLookupKey(
            dependency_kind=self.dependency_kind,
            dependency_ref=self.dependency_ref,
        )


@dataclass(frozen=True)
class LineageDirtyPlanningResult:
    """Reverse-index dirty/reuse plan for one scheduler run."""

    status: str
    queried_key_count: int
    dirty_output_count: int
    reusable_output_count: int
    unavailable_dependency_kinds: tuple[str, ...]
    dirty_scope_keys_by_definition: Mapping[str, tuple[str, ...]]
    dirty_output_ids: tuple[str, ...]
    reusable_definition_ids: tuple[str, ...]
    fallback_definition_ids: tuple[str, ...] = ()

    @property
    def available(self) -> bool:
        return self.status == "available"

    def metrics_for_definition(self, definition_id: str) -> dict[str, Any]:
        return {
            "status": self.status,
            "queried_key_count": self.queried_key_count,
            "dirty_output_count": self.dirty_output_count,
            "reusable_output_count": self.reusable_output_count,
            "unavailable_dependency_kinds": list(
                self.unavailable_dependency_kinds
            ),
            "dirty_scope_keys": list(
                self.dirty_scope_keys_by_definition.get(definition_id, ())
            ),
            "force_full": definition_id in self.fallback_definition_ids,
        }


@dataclass(frozen=True)
class ResolvedManifestBinding:
    """Concrete upstream manifest resolved before a downstream run."""

    dependency: DerivedArtifactDependency
    upstream_manifest_id: UUID
    upstream_target_generation_id: int
    bound_compute_hash: str
    bound_output_schema_hash: str
    bound_renderer_hash: str
    bound_dirty_rule_hash: str
    upstream_slot: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class ScheduledNodeResult:
    definition_id: str
    topo_index: int
    status: str
    run_id: UUID
    manifest_id: UUID
    dirty_scope_count: int
    upstream_binding_count: int


@dataclass(frozen=True)
class _ScheduleMetadataCompletion:
    run_id: UUID
    downstream_definition_id: str
    bindings: Sequence[ResolvedManifestBinding]
    definition_id: str
    status: str
    dirty_scope_count: int
    upstream_binding_count: int


@dataclass(frozen=True)
class ScheduleResult:
    plan_id: UUID
    graph_hash: str
    target_generation_id: int
    execution_phase: str
    nodes: list[ScheduledNodeResult]
    lineage_plan: LineageDirtyPlanningResult | None = None
    timings_ms: dict[str, float] = field(default_factory=dict)


_SCHEDULER_DEFAULT_DEPS = frozenset({
    "component",
    "component_hash",
    "graph_node",
    "source_content_hash",
    "source_record",
})
_SCHEDULER_JUDGMENT_DEPS = _SCHEDULER_DEFAULT_DEPS | frozenset({
    "decision",
    "evidence",
    "proposal",
})
_SCHEDULER_SENTINEL = SentinelSpec(
    pk_columns=["__scheduler_row__"],
    cadence_column="__cadence__",
)


@dataclass(frozen=True)
class _DeclaredDependencyResolution:
    kinds: frozenset[str]
    raw: tuple[str, ...]
    unsupported: tuple[str, ...]
    fallback_used: bool


def _noop_maintain(_ctx: Any) -> MaintainResult:
    return MaintainResult([], [], 0, None, "full_recount")


def _declared_deps_from_dirty_rule(
    dirty_rule: Mapping[str, Any],
    *,
    fallback: frozenset[str],
) -> _DeclaredDependencyResolution:
    declared = dirty_rule.get("dependencies", ())
    if isinstance(declared, str):
        declared = (declared,)
    if not isinstance(declared, Sequence):
        declared = ()
    raw = tuple(str(kind) for kind in declared if isinstance(kind, str))
    allowed = frozenset(kind for kind in raw if kind in INDEXED_DEPENDENCY_KINDS)
    unsupported = tuple(kind for kind in raw if kind not in INDEXED_DEPENDENCY_KINDS)
    if allowed:
        return _DeclaredDependencyResolution(
            kinds=allowed,
            raw=raw,
            unsupported=unsupported,
            fallback_used=False,
        )
    return _DeclaredDependencyResolution(
        kinds=fallback,
        raw=raw,
        unsupported=unsupported,
        fallback_used=True,
    )


def _relation_recompute_sql(definition: RelationArtifactDefinition) -> str:
    physical_name = _physical_relation_name(definition)
    definition_id = definition.definition_id.replace("'", "''")
    return (
        "SELECT scope_key, row_key, row_hash, dependency_hash "
        f"FROM okg_derived.{physical_name} "
        f"WHERE definition_id = '{definition_id}'"
    )


def _relation_schedule_row(task: RelationScheduleTask) -> _ScheduleDerivationRow:
    definition = task.definition
    physical_name = _physical_relation_name(definition)
    deps = _declared_deps_from_dirty_rule(
        definition.dirty_rule,
        fallback=_SCHEDULER_DEFAULT_DEPS,
    )
    spec = DerivationSpec(
        derivation_id=definition.definition_id,
        sentinel=_SCHEDULER_SENTINEL,
        oracle=CorrectnessOracle(
            _relation_recompute_sql(definition),
            ["scope_key", "row_key"],
            ["row_hash", "dependency_hash"],
        ),
        force_full_predicates=[],
        full_recount=_noop_maintain,
        incremental=_noop_maintain,
        self_heal_every=25,
    )
    row = DerivationRow(
        spec=spec,
        declared_deps=DeclaredDeps(kinds=deps.kinds),
        materializations=[
            Materialization(
                kind="side_table",
                storage=ChronosRegisteredTable(
                    table=f"okg_derived.{physical_name}",
                    ensure_fn=lambda _store: None,
                ),
                sentinel=_SCHEDULER_SENTINEL,
            )
        ],
        governance=GovernanceProfile(
            determinism=DeterminismClass.DETERMINISTIC,
        ),
        execution_phase=ExecutionPhase.POST_PUBLISH,
        failure_mode=FailureMode.GATING,
        timing_key="relation",
        scheduler_kind="relation",
    )
    return _ScheduleDerivationRow(
        task=task,
        row=row,
        task_kind="relation",
        raw_dirty_rule_deps=deps.raw,
        unsupported_dirty_rule_deps=deps.unsupported,
        dependency_fallback_used=deps.fallback_used,
    )


def _view_projection_parity_oracle() -> ProjectionParityOracle:
    return ProjectionParityOracle(lambda _frozen, _checkout: {}, ["slot_name"])


def _view_override_path() -> OverridePath:
    from okg.substrate.lenses.promote import promote_lens_overlay
    from okg.substrate.lenses.putback import (
        commit_lens_overlay,
        prepare_lens_putback,
    )

    return OverridePath(
        prepare=prepare_lens_putback,
        commit=commit_lens_overlay,
        promote=promote_lens_overlay,
    )


def _view_schedule_row(task: ViewScheduleTask) -> _ScheduleDerivationRow:
    definition = task.definition
    deps = _declared_deps_from_dirty_rule(
        definition.dirty_rule,
        fallback=_SCHEDULER_JUDGMENT_DEPS,
    )
    spec = DerivationSpec(
        derivation_id=definition.definition_id,
        sentinel=_SCHEDULER_SENTINEL,
        oracle=_view_projection_parity_oracle(),
        force_full_predicates=[],
        full_recount=_noop_maintain,
        incremental=_noop_maintain,
        self_heal_every=25,
    )
    row = DerivationRow(
        spec=spec,
        declared_deps=DeclaredDeps(kinds=deps.kinds),
        materializations=[
            Materialization(
                kind="rendered_artifact",
                storage=ChronosRegisteredTable(
                    table="okg.derived_output_slots",
                    ensure_fn=lambda _store: None,
                ),
                sentinel=_SCHEDULER_SENTINEL,
            )
        ],
        governance=GovernanceProfile(
            determinism=DeterminismClass.JUDGMENT,
            override=_view_override_path(),
        ),
        execution_phase=ExecutionPhase.POST_PUBLISH,
        failure_mode=FailureMode.GATING,
        timing_key="view",
        scheduler_kind="view",
    )
    return _ScheduleDerivationRow(
        task=task,
        row=row,
        task_kind="view",
        raw_dirty_rule_deps=deps.raw,
        unsupported_dirty_rule_deps=deps.unsupported,
        dependency_fallback_used=deps.fallback_used,
    )


def _schedule_derivation_row(task: DerivedScheduleTask) -> _ScheduleDerivationRow:
    if isinstance(task, RelationScheduleTask):
        return _relation_schedule_row(task)
    return _view_schedule_row(task)


def _registry_row_metrics(scheduled: _ScheduleDerivationRow) -> dict[str, Any]:
    row = scheduled.row
    return {
        "derivation_id": row.spec.derivation_id,
        "scheduler_kind": row.scheduler_kind,
        "determinism": row.governance.determinism.value,
        "oracle": type(row.spec.oracle).__name__,
        "declared_deps": sorted(row.declared_deps.kinds),
        "materializations": [
            {
                "kind": materialization.kind,
                "storage": materialization.storage.table,
            }
            for materialization in row.materializations
        ],
        "override": row.governance.override is not None,
        "lineage_required": row.governance.requires_lineage_envelope,
        "raw_dirty_rule_deps": list(scheduled.raw_dirty_rule_deps),
        "unsupported_dirty_rule_deps": list(
            scheduled.unsupported_dirty_rule_deps
        ),
        "dependency_fallback_used": scheduled.dependency_fallback_used,
    }


def _task_with_registry_metrics(
    scheduled: _ScheduleDerivationRow,
) -> DerivedScheduleTask:
    return replace(
        scheduled.task,
        metrics={
            **dict(scheduled.task.metrics),
            "registry_row": _registry_row_metrics(scheduled),
        },
    )


def _schedule_registry(
    tasks: Sequence[DerivedScheduleTask],
) -> dict[str, _ScheduleDerivationRow]:
    registry = DerivationRegistry()
    rows: dict[str, _ScheduleDerivationRow] = {}
    for task in tasks:
        scheduled = _schedule_derivation_row(task)
        registry.register(scheduled.row)
        rows[scheduled.row.spec.derivation_id] = scheduled
    return rows


def _register_schedule_row(
    conn: psycopg.Connection[Any],
    scheduled: _ScheduleDerivationRow,
) -> None:
    if scheduled.task_kind == "relation":
        register_jsonb_relation(conn, scheduled.task.definition)
        return
    register_view_artifact_definition(conn, scheduled.task.definition)


def register_dependency(
    conn: psycopg.Connection[Any],
    dependency: DerivedArtifactDependency,
) -> UUID:
    """Persist or update one logical dependency edge."""

    if dependency.downstream_definition_id == dependency.upstream_definition_id:
        raise DerivedArtifactScheduleError(
            "a derived artifact cannot depend on itself"
        )
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.derived_artifact_dependencies (
                  downstream_definition_id, upstream_definition_id,
                  upstream_slot_name, dependency_kind, scope_key,
                  required_compute_hash, required_output_schema_hash,
                  required_renderer_hash, required_dirty_rule_hash,
                  metadata
                ) VALUES (
                  %(downstream_definition_id)s,
                  %(upstream_definition_id)s,
                  %(upstream_slot_name)s,
                  %(dependency_kind)s,
                  %(scope_key)s,
                  %(required_compute_hash)s,
                  %(required_output_schema_hash)s,
                  %(required_renderer_hash)s,
                  %(required_dirty_rule_hash)s,
                  %(metadata)s
                )
                ON CONFLICT (
                  downstream_definition_id, upstream_definition_id,
                  upstream_slot_name, (COALESCE(scope_key, ''))
                ) DO UPDATE SET
                  dependency_kind = EXCLUDED.dependency_kind,
                  required_compute_hash = EXCLUDED.required_compute_hash,
                  required_output_schema_hash =
                    EXCLUDED.required_output_schema_hash,
                  required_renderer_hash = EXCLUDED.required_renderer_hash,
                  required_dirty_rule_hash =
                    EXCLUDED.required_dirty_rule_hash,
                  metadata = EXCLUDED.metadata
                RETURNING dependency_id
                """,
                {
                    "downstream_definition_id": (
                        dependency.downstream_definition_id
                    ),
                    "upstream_definition_id": (
                        dependency.upstream_definition_id
                    ),
                    "upstream_slot_name": dependency.upstream_slot_name,
                    "dependency_kind": dependency.dependency_kind,
                    "scope_key": dependency.scope_key,
                    "required_compute_hash": dependency.required_compute_hash,
                    "required_output_schema_hash": (
                        dependency.required_output_schema_hash
                    ),
                    "required_renderer_hash": (
                        dependency.required_renderer_hash
                    ),
                    "required_dirty_rule_hash": (
                        dependency.required_dirty_rule_hash
                    ),
                    "metadata": Jsonb(dict(dependency.metadata)),
                },
            )
            (dependency_id,) = cur.fetchone()
            return dependency_id


def schedule_relation_artifacts(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    target_generation_id: int,
    tasks: Sequence[RelationScheduleTask],
    execution_phase: str = "post_publish",
    catalog_version_id: int | None = None,
    lineage_invalidations: Sequence[LineageInvalidation] = (),
) -> ScheduleResult:
    """Run relation artifact tasks in deterministic dependency order."""
    return schedule_derived_artifacts(
        conn,
        deployment_name=deployment_name,
        target_generation_id=target_generation_id,
        tasks=tasks,
        execution_phase=execution_phase,
        catalog_version_id=catalog_version_id,
        lineage_invalidations=lineage_invalidations,
    )


def schedule_derived_artifacts(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    target_generation_id: int,
    tasks: Sequence[DerivedScheduleTask],
    execution_phase: str = "post_publish",
    catalog_version_id: int | None = None,
    lineage_invalidations: Sequence[LineageInvalidation] = (),
    register_tasks: bool = True,
    trusted_previous_manifest_ids: Mapping[str, UUID] | None = None,
    coalesce_task_transactions: bool = False,
) -> ScheduleResult:
    """Run derived artifact tasks in deterministic dependency order."""

    timings_ms: dict[str, float] = {}
    total_started = time.perf_counter()

    def add_timing(label: str, started: float) -> None:
        elapsed = round((time.perf_counter() - started) * 1000, 3)
        timings_ms[label] = round(timings_ms.get(label, 0.0) + elapsed, 3)

    step_started = time.perf_counter()
    # Refuse a duplicate or empty task set before any registry row is
    # written; `plan_dependency_graph` composes the same step for
    # database-free callers.
    task_by_id = build_task_map(tasks)
    schedule_rows = _schedule_registry(tasks)
    if register_tasks:
        for scheduled in sorted(
            schedule_rows.values(),
            key=lambda item: item.row.spec.derivation_id,
        ):
            _register_schedule_row(conn, scheduled)
    else:
        timings_ms["register_tasks_skipped"] = float(len(tasks))
    add_timing("register_tasks", step_started)

    step_started = time.perf_counter()
    dependencies = _load_dependencies(conn, set(task_by_id))
    add_timing("load_dependencies", step_started)
    # The pure rules live in `dependency_planning`. It is handed the
    # already-loaded dependency records and the scheduler's own
    # `add_timing`, so it reports the same `topological_order` and
    # `graph_hash` labels, at the same boundaries, as the inline code it
    # replaced.
    plan = plan_dependency_graph(
        deployment_name=deployment_name,
        execution_phase=execution_phase,
        catalog_version_id=catalog_version_id,
        tasks=tasks,
        dependencies=dependencies,
        record_timing=add_timing,
    )
    topo_order = plan.topological_order
    plan_graph_hash = plan.graph_hash
    step_started = time.perf_counter()
    lineage_plan = (
        _plan_lineage_dirty_outputs(
            conn,
            task_by_id=task_by_id,
            target_generation_id=target_generation_id,
            invalidations=lineage_invalidations,
        )
        if lineage_invalidations
        else None
    )
    add_timing("lineage_plan", step_started)
    step_started = time.perf_counter()
    plan_id = _create_plan(
        conn,
        deployment_name=deployment_name,
        target_generation_id=target_generation_id,
        execution_phase=execution_phase,
        catalog_version_id=catalog_version_id,
        graph_hash=plan_graph_hash,
        topo_order=topo_order,
        timing_sink=timings_ms,
        use_transaction=not coalesce_task_transactions,
    )
    add_timing("create_plan", step_started)

    results: list[ScheduledNodeResult] = []
    local_relation_outputs: dict[
        str,
        tuple[PublishedArtifact, RelationArtifactDefinition],
    ] = {}
    pending_metadata: list[_ScheduleMetadataCompletion] = []

    def flush_pending_metadata(*, complete_plan: bool) -> None:
        if not pending_metadata:
            return
        step_started = time.perf_counter()
        metadata_timings = _persist_schedule_metadata_batch(
            conn,
            plan_id=plan_id,
            completions=pending_metadata,
            complete_plan=complete_plan,
            use_transaction=not coalesce_task_transactions,
        )
        pending_metadata.clear()
        for label, elapsed in metadata_timings.items():
            timings_ms[label] = round(
                timings_ms.get(label, 0.0) + elapsed,
                3,
            )
        add_timing("task.complete_metadata_flush", step_started)

    try:
        for topo_index, definition_id in enumerate(topo_order):
            task = task_by_id[definition_id]
            scheduled = schedule_rows[definition_id]
            started = time.monotonic()
            try:
                step_started = time.perf_counter()
                upstream_bindings = _resolve_bindings(
                    conn,
                    dependencies=[
                        dep for dep in dependencies
                        if dep.downstream_definition_id == definition_id
                    ],
                    target_generation_id=target_generation_id,
                    local_relation_outputs=local_relation_outputs,
                )
                add_timing("task.resolve_bindings", step_started)
                step_started = time.perf_counter()
                dirty_scope_keys = _dirty_scope_keys(
                    conn,
                    task,
                    target_generation_id=target_generation_id,
                    lineage_scope_keys=(
                        ()
                        if lineage_plan is None
                        else lineage_plan.dirty_scope_keys_by_definition.get(
                            definition_id,
                            (),
                        )
                    ),
                )
                add_timing("task.dirty_scope_keys", step_started)
                step_started = time.perf_counter()
                published, status = _run_schedule_row(
                    conn,
                    scheduled,
                    target_generation_id=target_generation_id,
                    dirty_scope_keys=dirty_scope_keys,
                    upstream_bindings=upstream_bindings,
                    force_full=(
                        lineage_plan is not None
                        and definition_id in lineage_plan.fallback_definition_ids
                    ),
                    trusted_previous_manifest_id=(
                        None
                        if trusted_previous_manifest_ids is None
                        else trusted_previous_manifest_ids.get(definition_id)
                    ),
                    use_task_transactions=not coalesce_task_transactions,
                    lineage_planning_metrics=(
                        None
                        if lineage_plan is None
                        else lineage_plan.metrics_for_definition(definition_id)
                    ),
                    elapsed_ms=lambda: int((time.monotonic() - started) * 1000),
                )
                task_timing_label = f"task.run_{scheduled.task_kind}"
                add_timing(task_timing_label, step_started)
                for label, elapsed in getattr(
                    published,
                    "timings_ms",
                    {},
                ).items():
                    timings_ms[f"{task_timing_label}.{label}"] = round(
                        timings_ms.get(f"{task_timing_label}.{label}", 0.0)
                        + float(elapsed),
                        3,
                    )
                if scheduled.task_kind == "relation":
                    local_relation_outputs[definition_id] = (
                        published,
                        task.definition,
                    )
                if coalesce_task_transactions:
                    pending_metadata.append(
                        _ScheduleMetadataCompletion(
                            run_id=published.run_id,
                            downstream_definition_id=definition_id,
                            bindings=tuple(upstream_bindings),
                            definition_id=definition_id,
                            status=status,
                            dirty_scope_count=len(dirty_scope_keys),
                            upstream_binding_count=len(upstream_bindings),
                        )
                    )
                else:
                    metadata_timings = _persist_bindings_and_mark_node_completed(
                        conn,
                        run_id=published.run_id,
                        downstream_definition_id=definition_id,
                        bindings=upstream_bindings,
                        plan_id=plan_id,
                        definition_id=definition_id,
                        status=status,
                        dirty_scope_count=len(dirty_scope_keys),
                        upstream_binding_count=len(upstream_bindings),
                        complete_plan=topo_index == len(topo_order) - 1,
                        use_transaction=True,
                    )
                    for label, elapsed in metadata_timings.items():
                        timings_ms[label] = round(
                            timings_ms.get(label, 0.0) + elapsed,
                            3,
                        )
                results.append(
                    ScheduledNodeResult(
                        definition_id=definition_id,
                        topo_index=topo_index,
                        status=status,
                        run_id=published.run_id,
                        manifest_id=published.manifest_id,
                        dirty_scope_count=len(dirty_scope_keys),
                        upstream_binding_count=len(upstream_bindings),
                    )
                )
            except Exception as exc:
                if coalesce_task_transactions:
                    try:
                        flush_pending_metadata(complete_plan=False)
                    except Exception:
                        # Preserve the original task exception; the failed flush
                        # is secondary cleanup during error handling.
                        pass
                step_started = time.perf_counter()
                _mark_node_failed(conn, plan_id, definition_id, exc)
                add_timing("task.mark_failed", step_started)
                raise
    except Exception as exc:
        step_started = time.perf_counter()
        _fail_plan(conn, plan_id, exc)
        add_timing("fail_plan", step_started)
        raise
    if coalesce_task_transactions:
        flush_pending_metadata(complete_plan=True)
    add_timing("total", total_started)

    return ScheduleResult(
        plan_id=plan_id,
        graph_hash=plan_graph_hash,
        target_generation_id=target_generation_id,
        execution_phase=execution_phase,
        nodes=results,
        lineage_plan=lineage_plan,
        timings_ms=timings_ms,
    )


def plan_lineage_invalidations(
    conn: psycopg.Connection[Any],
    *,
    tasks: Sequence[DerivedScheduleTask],
    target_generation_id: int,
    invalidations: Sequence[LineageInvalidation],
) -> LineageDirtyPlanningResult:
    """Plan dirty/reusable scheduler outputs from reverse lineage keys."""

    return _plan_lineage_dirty_outputs(
        conn,
        task_by_id=build_task_map(tasks),
        target_generation_id=target_generation_id,
        invalidations=invalidations,
    )


def _plan_lineage_dirty_outputs(
    conn: psycopg.Connection[Any],
    *,
    task_by_id: Mapping[str, DerivedScheduleTask],
    target_generation_id: int,
    invalidations: Sequence[LineageInvalidation],
) -> LineageDirtyPlanningResult:
    keys = tuple(invalidation.lookup_key() for invalidation in invalidations)
    if not keys:
        return LineageDirtyPlanningResult(
            status="available",
            queried_key_count=0,
            dirty_output_count=0,
            reusable_output_count=0,
            unavailable_dependency_kinds=(),
            dirty_scope_keys_by_definition={},
            dirty_output_ids=(),
            reusable_definition_ids=(),
        )

    lookup = lookup_dirty_outputs(conn, dependency_keys=keys)
    dirty_scope_keys: dict[str, set[str]] = {}
    dirty_output_ids: list[str] = []
    for output in lookup.dirty_outputs:
        if output.definition_id not in task_by_id:
            continue
        dirty_output_ids.append(output.output_id)
        dirty_scope_keys.setdefault(output.definition_id, set()).add(
            _dirty_scope_key_for_output(
                output_id=output.output_id,
                invalidation_scope=output.invalidation_scope,
            )
        )

    fallback_definition_ids = (
        tuple(sorted(task_by_id))
        if lookup.missing_dependency_kinds
        else ()
    )
    reusable_definition_ids: list[str] = []
    if not fallback_definition_ids:
        for definition_id, task in sorted(task_by_id.items()):
            if definition_id in dirty_scope_keys:
                continue
            previous_manifest_id = _latest_completed_manifest(
                conn,
                definition_id=definition_id,
                before_generation_id=target_generation_id,
            )
            if (
                previous_manifest_id is not None
                and _manifest_hashes_match(
                    conn,
                    definition=task.definition,
                    manifest_id=previous_manifest_id,
                )
            ):
                reusable_definition_ids.append(definition_id)

    return LineageDirtyPlanningResult(
        status=lookup.status,
        queried_key_count=len(keys),
        dirty_output_count=len(dirty_output_ids),
        reusable_output_count=len(reusable_definition_ids),
        unavailable_dependency_kinds=lookup.missing_dependency_kinds,
        dirty_scope_keys_by_definition={
            definition_id: tuple(sorted(scope_keys))
            for definition_id, scope_keys in sorted(dirty_scope_keys.items())
        },
        dirty_output_ids=tuple(sorted(dirty_output_ids)),
        reusable_definition_ids=tuple(reusable_definition_ids),
        fallback_definition_ids=fallback_definition_ids,
    )


def _dirty_scope_key_for_output(
    *,
    output_id: str,
    invalidation_scope: str,
) -> str:
    if invalidation_scope in {"global", "per-index", "per-component"}:
        return "__all__"
    return output_id


def enqueue_dependency_dirty_scope(
    conn: psycopg.Connection[Any],
    definition: RelationArtifactDefinition,
    *,
    target_generation_id: int,
    scope_key: str,
    dependency_ref: str,
    dependency_hash: str | None = None,
) -> None:
    """Convenience wrapper for scheduler tests and callers."""

    enqueue_dirty_scope(
        conn,
        definition,
        target_generation_id=target_generation_id,
        dirty_scope=DirtyScope(
            scope_key=scope_key,
            dirty_reason="dependency_changed",
            dependency_kind="derived_artifact",
            dependency_ref=dependency_ref,
            dependency_hash=dependency_hash,
        ),
    )


# The pure rule steps now live in `dependency_planning`. Call them by
# their public names there; this module no longer keeps private aliases
# for them, because nothing imported those aliases.


def _load_dependencies(
    conn: psycopg.Connection[Any],
    definition_ids: set[str],
) -> list[DerivedArtifactDependency]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT downstream_definition_id, upstream_definition_id,
                   upstream_slot_name, dependency_kind, scope_key,
                   required_compute_hash, required_output_schema_hash,
                   required_renderer_hash, required_dirty_rule_hash,
                   metadata
              FROM okg.derived_artifact_dependencies
             WHERE downstream_definition_id = ANY(%(definition_ids)s)
             ORDER BY downstream_definition_id, upstream_definition_id,
                      upstream_slot_name, scope_key NULLS FIRST
            """,
            {"definition_ids": sorted(definition_ids)},
        )
        return [
            DerivedArtifactDependency(
                downstream_definition_id=row[0],
                upstream_definition_id=row[1],
                upstream_slot_name=row[2],
                dependency_kind=row[3],
                scope_key=row[4],
                required_compute_hash=row[5],
                required_output_schema_hash=row[6],
                required_renderer_hash=row[7],
                required_dirty_rule_hash=row[8],
                metadata=dict(row[9] or {}),
            )
            for row in cur.fetchall()
        ]


def _create_plan(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    target_generation_id: int,
    execution_phase: str,
    catalog_version_id: int | None,
    graph_hash: str,
    topo_order: Sequence[str],
    timing_sink: dict[str, float] | None = None,
    use_transaction: bool = True,
) -> UUID:
    def record(label: str, started: float) -> None:
        if timing_sink is None:
            return
        timing_sink[label] = round(
            timing_sink.get(label, 0.0)
            + ((time.perf_counter() - started) * 1000.0),
            3,
        )

    plan_nodes = [
        {"definition_id": definition_id, "topo_index": topo_index}
        for topo_index, definition_id in enumerate(topo_order)
    ]
    transaction_context = conn.transaction() if use_transaction else nullcontext()
    with transaction_context:
        with conn.cursor() as cur:
            started = time.perf_counter()
            cur.execute(
                """
                WITH plan AS (
                  INSERT INTO okg.derived_schedule_plans (
                    deployment_name, target_generation_id,
                    execution_phase, catalog_version_id, graph_hash,
                    status
                  ) VALUES (
                    %(deployment_name)s, %(target_generation_id)s,
                    %(execution_phase)s, %(catalog_version_id)s,
                    %(graph_hash)s, 'running'
                  )
                  RETURNING plan_id
                ),
                nodes AS (
                  INSERT INTO okg.derived_schedule_nodes (
                    plan_id, definition_id, topo_index, status, started_at
                  )
                  SELECT plan.plan_id, node.definition_id,
                         node.topo_index, 'planned', now()
                    FROM plan
                    CROSS JOIN jsonb_to_recordset(%(plan_nodes)s::jsonb)
                      AS node(definition_id text, topo_index int)
                  RETURNING 1
                )
                SELECT plan.plan_id,
                       (SELECT count(*) FROM nodes) AS node_count
                  FROM plan
                """,
                {
                    "deployment_name": deployment_name,
                    "target_generation_id": target_generation_id,
                    "execution_phase": execution_phase,
                    "catalog_version_id": catalog_version_id,
                    "graph_hash": graph_hash,
                    "plan_nodes": Jsonb(plan_nodes),
                },
            )
            plan_id, node_count = cur.fetchone()
            record("create_plan.insert_plan_and_nodes", started)
            if timing_sink is not None:
                timing_sink["create_plan.node_count"] = float(node_count)
            return plan_id


def _dirty_scope_keys(
    conn: psycopg.Connection[Any],
    task: DerivedScheduleTask,
    *,
    target_generation_id: int,
    lineage_scope_keys: Sequence[str] = (),
) -> list[str]:
    lineage_keys = set(lineage_scope_keys)
    if task.dirty_scope_keys is not None:
        return sorted(set(task.dirty_scope_keys) | lineage_keys)
    return sorted(
        set(pending_dirty_scope_keys(
            conn,
            task.definition,
            target_generation_id=target_generation_id,
        ))
        | lineage_keys
    )


def _run_schedule_row(
    conn: psycopg.Connection[Any],
    scheduled: _ScheduleDerivationRow,
    *,
    target_generation_id: int,
    dirty_scope_keys: Sequence[str],
    upstream_bindings: Sequence[ResolvedManifestBinding],
    force_full: bool,
    trusted_previous_manifest_id: UUID | None,
    lineage_planning_metrics: Mapping[str, Any] | None,
    use_task_transactions: bool,
    elapsed_ms,
) -> tuple[PublishedArtifact | PublishedViewArtifact, str]:
    task = _task_with_registry_metrics(scheduled)
    if scheduled.task_kind == "relation":
        return _run_relation_task(
            conn,
            task,
            target_generation_id=target_generation_id,
            dirty_scope_keys=dirty_scope_keys,
            upstream_bindings=upstream_bindings,
            force_full=force_full,
            trusted_previous_manifest_id=trusted_previous_manifest_id,
            lineage_planning_metrics=lineage_planning_metrics,
            use_task_transactions=use_task_transactions,
            elapsed_ms=elapsed_ms,
        )
    return _run_view_task(
        conn,
        task,
        target_generation_id=target_generation_id,
        dirty_scope_keys=dirty_scope_keys,
        upstream_bindings=upstream_bindings,
        force_full=force_full,
        trusted_previous_manifest_id=trusted_previous_manifest_id,
        lineage_planning_metrics=lineage_planning_metrics,
        use_task_transactions=use_task_transactions,
        elapsed_ms=elapsed_ms,
    )


def _run_relation_task(
    conn: psycopg.Connection[Any],
    task: RelationScheduleTask,
    *,
    target_generation_id: int,
    dirty_scope_keys: Sequence[str],
    upstream_bindings: Sequence[ResolvedManifestBinding],
    force_full: bool,
    trusted_previous_manifest_id: UUID | None,
    lineage_planning_metrics: Mapping[str, Any] | None,
    use_task_transactions: bool,
    elapsed_ms,
) -> tuple[PublishedArtifact, str]:
    timings_ms: dict[str, float] = {}

    def add_timing(label: str, started: float) -> None:
        timings_ms[label] = round(
            timings_ms.get(label, 0.0)
            + ((time.perf_counter() - started) * 1000.0),
            3,
        )

    if trusted_previous_manifest_id is not None:
        previous_manifest_id = trusted_previous_manifest_id
        timings_ms["scheduler.latest_completed_manifest.trusted"] = 1.0
    else:
        step_started = time.perf_counter()
        previous_manifest_id = _latest_completed_manifest(
            conn,
            definition_id=task.definition.definition_id,
            before_generation_id=target_generation_id,
        )
        add_timing("scheduler.latest_completed_manifest", step_started)
    dependency_manifest_ids = [
        binding.upstream_manifest_id for binding in upstream_bindings
    ]
    if trusted_previous_manifest_id is not None:
        previous_hashes_match = True
        timings_ms["scheduler.manifest_hashes_match.trusted"] = 1.0
    else:
        step_started = time.perf_counter()
        previous_hashes_match = (
            previous_manifest_id is not None
            and _manifest_hashes_match(
                conn,
                definition=task.definition,
                manifest_id=previous_manifest_id,
            )
        )
        add_timing("scheduler.manifest_hashes_match", step_started)
    metrics = {
        **dict(task.metrics),
        "scheduler_elapsed_ms": elapsed_ms(),
        "upstream_binding_count": len(upstream_bindings),
    }
    if lineage_planning_metrics is not None:
        metrics["lineage_planning"] = dict(lineage_planning_metrics)
    if (
        not force_full
        and
        previous_manifest_id is not None
        and previous_hashes_match
        and not dirty_scope_keys
    ):
        step_started = time.perf_counter()
        published = carry_forward_relation_artifact(
            conn,
            task.definition,
            source_manifest_id=previous_manifest_id,
            target_generation_id=target_generation_id,
            metrics=metrics,
            definition_registered=True,
        )
        add_timing("scheduler.carry_forward_relation_artifact", step_started)
        return (
            replace(
                published,
                timings_ms={
                    **dict(published.timings_ms),
                    **timings_ms,
                },
            ),
            "carried_forward",
        )
    if (
        not force_full
        and
        previous_manifest_id is not None
        and previous_hashes_match
        and dirty_scope_keys
    ):
        step_started = time.perf_counter()
        dirty_set = set(dirty_scope_keys)
        dirty_rows = [row for row in task.rows if row.scope_key in dirty_set]
        add_timing("scheduler.filter_dirty_rows", step_started)
        if task.dirty_rows_known_changed:
            timings_ms["scheduler.dirty_rows_match_manifest.skipped_known_changed"] = 1.0
            dirty_match_source_manifest = None
            dirty_match_previous_stats = None
            dirty_rows_match = False
        else:
            step_started = time.perf_counter()
            dirty_match = relation_dirty_rows_match_manifest(
                conn,
                task.definition,
                source_manifest_id=previous_manifest_id,
                dirty_scope_keys=dirty_scope_keys,
                dirty_rows=dirty_rows,
                timing_sink=timings_ms,
            )
            add_timing("scheduler.dirty_rows_match_manifest", step_started)
            dirty_match_source_manifest = dirty_match.source_manifest
            dirty_match_previous_stats = dirty_match.previous_dirty_stats
            dirty_rows_match = dirty_match.matches
        if dirty_rows_match:
            carry_metrics = {
                **metrics,
                "dirty_scope_count": len(dirty_scope_keys),
                "dirty_rows_compared": len(dirty_rows),
                "dirty_rows_unchanged": True,
            }
            step_started = time.perf_counter()
            published = carry_forward_relation_artifact(
                conn,
                task.definition,
                source_manifest_id=previous_manifest_id,
                target_generation_id=target_generation_id,
                metrics=carry_metrics,
                definition_registered=True,
            )
            add_timing("scheduler.carry_forward_relation_artifact", step_started)
            if task.consume_dirty_scopes:
                step_started = time.perf_counter()
                _consume_dirty_scopes(
                    conn,
                    definition_id=task.definition.definition_id,
                    target_generation_id=target_generation_id,
                    dirty_scope_keys=dirty_scope_keys,
                    run_id=published.run_id,
                )
                add_timing("scheduler.consume_dirty_scopes", step_started)
            else:
                timings_ms[
                    "scheduler.consume_dirty_scopes.skipped_direct_delta"
                ] = 1.0
            return (
                replace(
                    published,
                    timings_ms={
                        **dict(published.timings_ms),
                        **timings_ms,
                    },
                ),
                "carried_forward",
            )
        step_started = time.perf_counter()
        published = refresh_relation_artifact(
            conn,
            task.definition,
            source_manifest_id=previous_manifest_id,
            target_generation_id=target_generation_id,
            dirty_scope_keys=dirty_scope_keys,
            dirty_rows=dirty_rows,
            metrics=metrics,
            use_transaction=use_task_transactions,
            definition_registered=True,
            source_manifest=dirty_match_source_manifest,
            previous_dirty_stats=dirty_match_previous_stats,
            consume_dirty_scopes=task.consume_dirty_scopes,
        )
        add_timing("scheduler.refresh_relation_artifact", step_started)
        return (
            replace(
                published,
                timings_ms={
                    **dict(published.timings_ms),
                    **timings_ms,
                },
            ),
            "completed",
        )
    step_started = time.perf_counter()
    published = publish_relation_artifact(
        conn,
        task.definition,
        target_generation_id=target_generation_id,
        rows=task.rows,
        trigger_kind="dirty" if dirty_scope_keys and not force_full else "full",
        dependency_manifest_ids=dependency_manifest_ids,
        metrics=metrics,
    )
    add_timing("scheduler.publish_relation_artifact", step_started)
    if task.consume_dirty_scopes:
        step_started = time.perf_counter()
        _consume_dirty_scopes(
            conn,
            definition_id=task.definition.definition_id,
            target_generation_id=target_generation_id,
            dirty_scope_keys=dirty_scope_keys,
            run_id=published.run_id,
        )
        add_timing("scheduler.consume_dirty_scopes", step_started)
    else:
        timings_ms["scheduler.consume_dirty_scopes.skipped_direct_delta"] = 1.0
    return (
        replace(
            published,
            timings_ms={
                **dict(published.timings_ms),
                **timings_ms,
            },
        ),
        "completed",
    )


def _run_view_task(
    conn: psycopg.Connection[Any],
    task: ViewScheduleTask,
    *,
    target_generation_id: int,
    dirty_scope_keys: Sequence[str],
    upstream_bindings: Sequence[ResolvedManifestBinding],
    force_full: bool,
    trusted_previous_manifest_id: UUID | None,
    lineage_planning_metrics: Mapping[str, Any] | None,
    use_task_transactions: bool,
    elapsed_ms,
) -> tuple[PublishedViewArtifact, str]:
    timings_ms: dict[str, float] = {}

    def add_timing(label: str, started: float) -> None:
        timings_ms[label] = round(
            timings_ms.get(label, 0.0)
            + ((time.perf_counter() - started) * 1000.0),
            3,
        )

    if trusted_previous_manifest_id is not None:
        previous_manifest_id = trusted_previous_manifest_id
        timings_ms["scheduler.latest_completed_manifest.trusted"] = 1.0
    else:
        step_started = time.perf_counter()
        previous_manifest_id = _latest_completed_manifest(
            conn,
            definition_id=task.definition.definition_id,
            before_generation_id=target_generation_id,
        )
        add_timing("scheduler.latest_completed_manifest", step_started)
    dependency_manifest_ids = [
        binding.upstream_manifest_id for binding in upstream_bindings
    ]
    if trusted_previous_manifest_id is not None:
        previous_hashes_match = True
        timings_ms["scheduler.manifest_hashes_match.trusted"] = 1.0
    else:
        step_started = time.perf_counter()
        previous_hashes_match = (
            previous_manifest_id is not None
            and _manifest_hashes_match(
                conn,
                definition=task.definition,
                manifest_id=previous_manifest_id,
            )
        )
        add_timing("scheduler.manifest_hashes_match", step_started)
    metrics = {
        **dict(task.metrics),
        "scheduler_elapsed_ms": elapsed_ms(),
        "upstream_binding_count": len(upstream_bindings),
        "dirty_scope_count": len(dirty_scope_keys),
    }
    if lineage_planning_metrics is not None:
        metrics["lineage_planning"] = dict(lineage_planning_metrics)
    # A relation-backed view has to bind the current upstream manifests even
    # when its own dirty scope is empty. Carrying forward here would reuse the
    # previous relation reference and discard the outputs the caller just
    # rendered from the current corpus.
    #
    # An empty dirty-scope set is not evidence that a view's output is
    # unchanged: the native view stage passes none for a full rebuild. A task
    # that holds the outputs it just rendered is carried only when each slot's
    # content hash (generation-dependent fields excluded) equals the previous
    # manifest's; otherwise the outputs are published.
    may_carry = (
        not force_full
        and previous_manifest_id is not None
        and previous_hashes_match
        and not dirty_scope_keys
        and not upstream_bindings
    )
    if may_carry and task.outputs:
        step_started = time.perf_counter()
        may_carry = outputs_match_manifest_content(
            conn,
            previous_manifest_id=previous_manifest_id,
            outputs=task.outputs,
        )
        add_timing("scheduler.view_content_hash_compare", step_started)
        timings_ms["scheduler.view_content_unchanged"] = 1.0 if may_carry else 0.0
    if may_carry:
        step_started = time.perf_counter()
        published = carry_forward_view_artifact(
            conn,
            task.definition,
            source_manifest_id=previous_manifest_id,
            target_generation_id=target_generation_id,
            metrics=metrics,
            manifest_metadata=task.manifest_metadata,
        )
        add_timing("scheduler.carry_forward_view_artifact", step_started)
        status = "carried_forward"
    else:
        step_started = time.perf_counter()
        outputs = _bind_view_relation_outputs(
            conn,
            outputs=task.outputs,
            upstream_bindings=upstream_bindings,
        )
        add_timing("scheduler.bind_view_relation_outputs", step_started)
        step_started = time.perf_counter()
        published = publish_view_artifact(
            conn,
            task.definition,
            target_generation_id=target_generation_id,
            outputs=outputs,
            trigger_kind=(
                "dirty" if dirty_scope_keys and not force_full else "full"
            ),
            dependency_manifest_ids=dependency_manifest_ids,
            metrics=metrics,
            use_transaction=use_task_transactions,
            definition_registered=(
                previous_manifest_id is not None and previous_hashes_match
            ),
            manifest_metadata=task.manifest_metadata,
        )
        add_timing("scheduler.publish_view_artifact", step_started)
        status = "completed"
        if task.consume_dirty_scopes:
            step_started = time.perf_counter()
            _consume_dirty_scopes(
                conn,
                definition_id=task.definition.definition_id,
                target_generation_id=target_generation_id,
                dirty_scope_keys=dirty_scope_keys,
                run_id=published.run_id,
            )
            add_timing("scheduler.consume_dirty_scopes", step_started)
        else:
            timings_ms[
                "scheduler.consume_dirty_scopes.skipped_direct_delta"
            ] = 1.0

    return (
        replace(
            published,
            timings_ms={
                **dict(published.timings_ms),
                **timings_ms,
            },
        ),
        status,
    )


def _bind_view_relation_outputs(
    conn: psycopg.Connection[Any],
    *,
    outputs: Sequence[InlineViewOutput],
    upstream_bindings: Sequence[ResolvedManifestBinding],
) -> list[InlineViewOutput]:
    if not upstream_bindings:
        return list(outputs)

    bindings_by_key = {
        (
            binding.dependency.upstream_definition_id,
            binding.dependency.upstream_slot_name,
        ): binding
        for binding in upstream_bindings
    }
    bound_outputs: list[InlineViewOutput] = []
    for output in outputs:
        if output.storage_kind != "relation":
            bound_outputs.append(output)
            continue
        storage_ref = dict(output.storage_ref or {})
        upstream_definition_id = str(
            storage_ref.get("upstream_definition_id") or ""
        )
        upstream_slot_name = str(
            storage_ref.get("upstream_slot_name") or RELATION_SLOT_NAME
        )
        binding = bindings_by_key.get(
            (upstream_definition_id, upstream_slot_name)
        )
        if binding is None:
            raise DerivedArtifactScheduleError(
                "relation view output has no resolved upstream binding: "
                f"slot={output.slot_name!r} "
                f"upstream_definition_id={upstream_definition_id!r} "
                f"upstream_slot_name={upstream_slot_name!r}"
            )
        upstream_slot = (
            dict(binding.upstream_slot)
            if binding.upstream_slot is not None
            else _load_bound_output_slot(
                conn,
                manifest_id=binding.upstream_manifest_id,
                slot_name=upstream_slot_name,
            )
        )
        bound_outputs.append(
            replace(
                output,
                storage_ref={
                    **storage_ref,
                    **dict(upstream_slot["storage_ref"]),
                    "upstream_manifest_id": str(binding.upstream_manifest_id),
                    "upstream_target_generation_id": (
                        binding.upstream_target_generation_id
                    ),
                    "bound_compute_hash": binding.bound_compute_hash,
                    "bound_output_schema_hash": (
                        binding.bound_output_schema_hash
                    ),
                    "bound_renderer_hash": binding.bound_renderer_hash,
                    "bound_dirty_rule_hash": binding.bound_dirty_rule_hash,
                },
                row_count=(
                    output.row_count
                    if output.row_count is not None
                    else upstream_slot["row_count"]
                ),
                byte_count=(
                    output.byte_count
                    if output.byte_count is not None
                    else upstream_slot["byte_count"]
                ),
            )
        )
    return bound_outputs


def _load_bound_output_slot(
    conn: psycopg.Connection[Any],
    *,
    manifest_id: UUID,
    slot_name: str,
) -> dict[str, Any]:
    try:
        slot = get_view_slot(
            conn,
            manifest_id=manifest_id,
            slot_name=slot_name,
        )
    except ViewSlotNotFound as exc:
        raise DerivedArtifactScheduleError(
            f"bound upstream slot {slot_name!r} not found on manifest "
            f"{manifest_id}"
        ) from exc
    return {
        "storage_ref": dict(slot.storage_ref or {}),
        "row_count": slot.row_count,
        "byte_count": slot.byte_count,
    }


def _consume_dirty_scopes(
    conn: psycopg.Connection[Any],
    *,
    definition_id: str,
    target_generation_id: int,
    dirty_scope_keys: Sequence[str],
    run_id: UUID,
) -> None:
    if not dirty_scope_keys:
        return
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.derived_dirty_scopes
                   SET consumed_run_id = %(run_id)s,
                       consumed_at = now()
                 WHERE definition_id = %(definition_id)s
                   AND target_generation_id <= %(target_generation_id)s
                   AND scope_key = ANY(%(dirty_scope_keys)s)
                   AND consumed_run_id IS NULL
                """,
                {
                    "run_id": run_id,
                    "definition_id": definition_id,
                    "target_generation_id": target_generation_id,
                    "dirty_scope_keys": sorted(set(dirty_scope_keys)),
                },
            )


def _latest_completed_manifest(
    conn: psycopg.Connection[Any],
    *,
    definition_id: str,
    before_generation_id: int,
) -> UUID | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT manifest_id
              FROM okg.derived_output_manifests
             WHERE definition_id = %(definition_id)s
               AND target_generation_id < %(before_generation_id)s
               AND status = 'completed'
             ORDER BY target_generation_id DESC, promoted_at DESC
             LIMIT 1
            """,
            {
                "definition_id": definition_id,
                "before_generation_id": before_generation_id,
            },
        )
        row = cur.fetchone()
    return None if row is None else row[0]


def _manifest_hashes_match(
    conn: psycopg.Connection[Any],
    *,
    definition: RelationArtifactDefinition | ViewArtifactDefinition,
    manifest_id: UUID,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.compute_hash, r.output_schema_hash,
                   r.renderer_hash, r.dirty_rule_hash
              FROM okg.derived_output_manifests m
              JOIN okg.derived_artifact_runs r
                ON r.run_id = m.run_id
             WHERE m.manifest_id = %(manifest_id)s
               AND m.definition_id = %(definition_id)s
               AND m.status = 'completed'
            """,
            {
                "manifest_id": manifest_id,
                "definition_id": definition.definition_id,
            },
        )
        row = cur.fetchone()
    if row is None:
        return False
    return row == (
        definition.compute_hash,
        definition.output_schema_hash,
        definition.renderer_hash,
        definition.dirty_rule_hash,
    )


def _resolve_bindings(
    conn: psycopg.Connection[Any],
    *,
    dependencies: Sequence[DerivedArtifactDependency],
    target_generation_id: int,
    local_relation_outputs: (
        Mapping[str, tuple[PublishedArtifact, RelationArtifactDefinition]]
        | None
    ) = None,
) -> list[ResolvedManifestBinding]:
    bindings: list[ResolvedManifestBinding] = []
    for dependency in dependencies:
        local_relation_entry = (
            None
            if local_relation_outputs is None
            else local_relation_outputs.get(dependency.upstream_definition_id)
        )
        if (
            local_relation_entry is not None
            and dependency.upstream_slot_name == RELATION_SLOT_NAME
        ):
            local_relation, local_definition = local_relation_entry
            binding = ResolvedManifestBinding(
                dependency=dependency,
                upstream_manifest_id=local_relation.manifest_id,
                upstream_target_generation_id=target_generation_id,
                bound_compute_hash=local_definition.compute_hash,
                bound_output_schema_hash=local_definition.output_schema_hash,
                bound_renderer_hash=local_definition.renderer_hash,
                bound_dirty_rule_hash=local_definition.dirty_rule_hash,
                upstream_slot={
                    "storage_ref": dict(local_relation.storage_ref),
                    "row_count": local_relation.row_count,
                    "byte_count": local_relation.byte_count,
                    "payload_hash": local_relation.payload_hash,
                },
            )
            _require_compatible(binding)
            bindings.append(binding)
            continue
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT m.manifest_id, m.target_generation_id,
                       r.compute_hash, r.output_schema_hash,
                       r.renderer_hash, r.dirty_rule_hash
                  FROM okg.derived_output_manifests m
                  JOIN okg.derived_artifact_runs r
                    ON r.run_id = m.run_id
                 WHERE m.definition_id = %(definition_id)s
                   AND m.target_generation_id = %(target_generation_id)s
                   AND m.status = 'completed'
                   AND EXISTS (
                         WITH RECURSIVE manifest_chain AS (
                           SELECT source.manifest_id,
                                  source.reused_from_manifest_id,
                                  0 AS depth
                             FROM okg.derived_output_manifests source
                            WHERE source.manifest_id = m.manifest_id
                              AND source.status = 'completed'
                           UNION ALL
                           SELECT parent.manifest_id,
                                  parent.reused_from_manifest_id,
                                  manifest_chain.depth + 1
                             FROM okg.derived_output_manifests parent
                             JOIN manifest_chain
                               ON parent.manifest_id =
                                  manifest_chain.reused_from_manifest_id
                            WHERE parent.status = 'completed'
                              AND manifest_chain.depth < 64
                         )
                         SELECT 1
                           FROM manifest_chain
                           JOIN okg.derived_output_slots s
                             ON s.manifest_id = manifest_chain.manifest_id
                            AND s.slot_name = %(slot_name)s
                          LIMIT 1
                       )
                 ORDER BY m.promoted_at DESC
                 LIMIT 1
                """,
                {
                    "slot_name": dependency.upstream_slot_name,
                    "definition_id": dependency.upstream_definition_id,
                    "target_generation_id": target_generation_id,
                },
            )
            row = cur.fetchone()
        if row is None:
            raise DerivedArtifactScheduleError(
                "missing completed upstream manifest for "
                f"{dependency.upstream_definition_id!r} at generation "
                f"{target_generation_id}"
            )
        binding = ResolvedManifestBinding(
            dependency=dependency,
            upstream_manifest_id=row[0],
            upstream_target_generation_id=int(row[1]),
            bound_compute_hash=row[2],
            bound_output_schema_hash=row[3],
            bound_renderer_hash=row[4],
            bound_dirty_rule_hash=row[5],
        )
        _require_compatible(binding)
        bindings.append(binding)
    return bindings


def _require_compatible(binding: ResolvedManifestBinding) -> None:
    checks = [
        (
            "compute_hash",
            binding.dependency.required_compute_hash,
            binding.bound_compute_hash,
        ),
        (
            "output_schema_hash",
            binding.dependency.required_output_schema_hash,
            binding.bound_output_schema_hash,
        ),
        (
            "renderer_hash",
            binding.dependency.required_renderer_hash,
            binding.bound_renderer_hash,
        ),
        (
            "dirty_rule_hash",
            binding.dependency.required_dirty_rule_hash,
            binding.bound_dirty_rule_hash,
        ),
    ]
    for label, required, bound in checks:
        if required is not None and required != bound:
            raise DerivedArtifactScheduleError(
                f"upstream {label} mismatch for "
                f"{binding.dependency.upstream_definition_id!r}: "
                f"required {required}, bound {bound}"
            )


def _persist_bindings(
    conn: psycopg.Connection[Any],
    *,
    run_id: UUID,
    downstream_definition_id: str,
    bindings: Sequence[ResolvedManifestBinding],
) -> None:
    if not bindings:
        return
    with conn.transaction():
        with conn.cursor() as cur:
            _persist_bindings_in_cursor(
                cur,
                run_id=run_id,
                downstream_definition_id=downstream_definition_id,
                bindings=bindings,
            )


def _persist_bindings_and_mark_node_completed(
    conn: psycopg.Connection[Any],
    *,
    run_id: UUID,
    downstream_definition_id: str,
    bindings: Sequence[ResolvedManifestBinding],
    plan_id: UUID,
    definition_id: str,
    status: str,
    dirty_scope_count: int,
    upstream_binding_count: int,
    complete_plan: bool,
    use_transaction: bool = True,
) -> dict[str, float]:
    timings_ms: dict[str, float] = {}

    def record(label: str, started: float) -> None:
        timings_ms[label] = round((time.perf_counter() - started) * 1000.0, 3)

    transaction_context = conn.transaction() if use_transaction else nullcontext()
    with transaction_context:
        with conn.cursor() as cur:
            started = time.perf_counter()
            binding_count, node_count, plan_count = (
                _persist_bindings_and_mark_node_completed_in_cursor(
                    cur,
                    run_id=run_id,
                    downstream_definition_id=downstream_definition_id,
                    bindings=bindings,
                    plan_id=plan_id,
                    definition_id=definition_id,
                    status=status,
                    dirty_scope_count=dirty_scope_count,
                    upstream_binding_count=upstream_binding_count,
                    complete_plan=complete_plan,
                )
            )
            record("task.complete_metadata", started)
            timings_ms["task.complete_metadata.binding_count"] = float(
                binding_count
            )
            timings_ms["task.complete_metadata.node_count"] = float(node_count)
            timings_ms["task.complete_metadata.plan_count"] = float(plan_count)

    return timings_ms


def _persist_schedule_metadata_batch(
    conn: psycopg.Connection[Any],
    *,
    plan_id: UUID,
    completions: Sequence[_ScheduleMetadataCompletion],
    complete_plan: bool,
    use_transaction: bool = True,
) -> dict[str, float]:
    timings_ms: dict[str, float] = {}

    if not completions:
        return timings_ms

    node_rows = [
        {
            "run_id": str(completion.run_id),
            "definition_id": completion.definition_id,
            "status": completion.status,
            "dirty_scope_count": completion.dirty_scope_count,
            "upstream_binding_count": completion.upstream_binding_count,
        }
        for completion in completions
    ]
    binding_rows = [
        {
            "run_id": str(completion.run_id),
            "downstream_definition_id": completion.downstream_definition_id,
            "upstream_definition_id": binding.dependency.upstream_definition_id,
            "upstream_manifest_id": str(binding.upstream_manifest_id),
            "upstream_slot_name": binding.dependency.upstream_slot_name,
            "upstream_target_generation_id": (
                binding.upstream_target_generation_id
            ),
            "scope_key": binding.dependency.scope_key,
            "required_compute_hash": binding.dependency.required_compute_hash,
            "required_output_schema_hash": (
                binding.dependency.required_output_schema_hash
            ),
            "required_renderer_hash": binding.dependency.required_renderer_hash,
            "required_dirty_rule_hash": (
                binding.dependency.required_dirty_rule_hash
            ),
            "bound_compute_hash": binding.bound_compute_hash,
            "bound_output_schema_hash": binding.bound_output_schema_hash,
            "bound_renderer_hash": binding.bound_renderer_hash,
            "bound_dirty_rule_hash": binding.bound_dirty_rule_hash,
            "metadata": dict(binding.dependency.metadata),
        }
        for completion in completions
        for binding in completion.bindings
    ]

    transaction_context = conn.transaction() if use_transaction else nullcontext()
    with transaction_context:
        with conn.cursor() as cur:
            started = time.perf_counter()
            cur.execute(
                """
                WITH node_input AS (
                  SELECT *
                    FROM jsonb_to_recordset(%(nodes)s::jsonb) AS row(
                      run_id UUID,
                      definition_id TEXT,
                      status TEXT,
                      dirty_scope_count BIGINT,
                      upstream_binding_count BIGINT
                    )
                ),
                binding_input AS (
                  SELECT *
                    FROM jsonb_to_recordset(%(bindings)s::jsonb) AS row(
                      run_id UUID,
                      downstream_definition_id TEXT,
                      upstream_definition_id TEXT,
                      upstream_manifest_id UUID,
                      upstream_slot_name TEXT,
                      upstream_target_generation_id BIGINT,
                      scope_key TEXT,
                      required_compute_hash TEXT,
                      required_output_schema_hash TEXT,
                      required_renderer_hash TEXT,
                      required_dirty_rule_hash TEXT,
                      bound_compute_hash TEXT,
                      bound_output_schema_hash TEXT,
                      bound_renderer_hash TEXT,
                      bound_dirty_rule_hash TEXT,
                      metadata JSONB
                    )
                ),
                persisted_bindings AS (
                  INSERT INTO okg.derived_run_dependency_bindings (
                    run_id, downstream_definition_id,
                    upstream_definition_id, upstream_manifest_id,
                    upstream_slot_name, upstream_target_generation_id,
                    scope_key, required_compute_hash,
                    required_output_schema_hash, required_renderer_hash,
                    required_dirty_rule_hash, bound_compute_hash,
                    bound_output_schema_hash, bound_renderer_hash,
                    bound_dirty_rule_hash, metadata
                  )
                  SELECT
                    run_id,
                    downstream_definition_id,
                    upstream_definition_id,
                    upstream_manifest_id,
                    upstream_slot_name,
                    upstream_target_generation_id,
                    scope_key,
                    required_compute_hash,
                    required_output_schema_hash,
                    required_renderer_hash,
                    required_dirty_rule_hash,
                    bound_compute_hash,
                    bound_output_schema_hash,
                    bound_renderer_hash,
                    bound_dirty_rule_hash,
                    metadata
                  FROM binding_input
                  ON CONFLICT (
                    run_id, upstream_definition_id, upstream_slot_name,
                    (COALESCE(scope_key, ''))
                  ) DO UPDATE SET
                    upstream_manifest_id = EXCLUDED.upstream_manifest_id,
                    bound_compute_hash = EXCLUDED.bound_compute_hash,
                    bound_output_schema_hash = EXCLUDED.bound_output_schema_hash,
                    bound_renderer_hash = EXCLUDED.bound_renderer_hash,
                    bound_dirty_rule_hash = EXCLUDED.bound_dirty_rule_hash,
                    metadata = EXCLUDED.metadata
                  RETURNING 1
                ),
                completed_nodes AS (
                  UPDATE okg.derived_schedule_nodes AS node
                     SET status = node_input.status,
                         run_id = node_input.run_id,
                         dirty_scope_count = node_input.dirty_scope_count,
                         upstream_binding_count =
                           node_input.upstream_binding_count,
                         completed_at = now()
                    FROM node_input
                   WHERE node.plan_id = %(plan_id)s
                     AND node.definition_id = node_input.definition_id
                  RETURNING 1
                ),
                completed_plan AS (
                  UPDATE okg.derived_schedule_plans
                     SET status = 'completed',
                         completed_at = now()
                   WHERE plan_id = %(plan_id)s
                     AND %(complete_plan)s
                  RETURNING 1
                )
                SELECT
                  (SELECT count(*) FROM persisted_bindings)::int,
                  (SELECT count(*) FROM completed_nodes)::int,
                  (SELECT count(*) FROM completed_plan)::int
                """,
                {
                    "nodes": Jsonb(node_rows),
                    "bindings": Jsonb(binding_rows),
                    "plan_id": plan_id,
                    "complete_plan": complete_plan,
                },
            )
            row = cur.fetchone()
            elapsed = round((time.perf_counter() - started) * 1000.0, 3)
    if row is None:
        binding_count = node_count = plan_count = 0
    else:
        binding_count, node_count, plan_count = (
            int(row[0]),
            int(row[1]),
            int(row[2]),
        )
    timings_ms["task.complete_metadata"] = elapsed
    timings_ms["task.complete_metadata.binding_count"] = float(binding_count)
    timings_ms["task.complete_metadata.node_count"] = float(node_count)
    timings_ms["task.complete_metadata.plan_count"] = float(plan_count)
    timings_ms["task.complete_metadata.batch_size"] = float(len(completions))
    return timings_ms


def _persist_bindings_and_mark_node_completed_in_cursor(
    cur: psycopg.Cursor[Any],
    *,
    run_id: UUID,
    downstream_definition_id: str,
    bindings: Sequence[ResolvedManifestBinding],
    plan_id: UUID,
    definition_id: str,
    status: str,
    dirty_scope_count: int,
    upstream_binding_count: int,
    complete_plan: bool,
) -> tuple[int, int, int]:
    binding_rows = [
        {
            "upstream_definition_id": binding.dependency.upstream_definition_id,
            "upstream_manifest_id": str(binding.upstream_manifest_id),
            "upstream_slot_name": binding.dependency.upstream_slot_name,
            "upstream_target_generation_id": (
                binding.upstream_target_generation_id
            ),
            "scope_key": binding.dependency.scope_key,
            "required_compute_hash": binding.dependency.required_compute_hash,
            "required_output_schema_hash": (
                binding.dependency.required_output_schema_hash
            ),
            "required_renderer_hash": binding.dependency.required_renderer_hash,
            "required_dirty_rule_hash": (
                binding.dependency.required_dirty_rule_hash
            ),
            "bound_compute_hash": binding.bound_compute_hash,
            "bound_output_schema_hash": binding.bound_output_schema_hash,
            "bound_renderer_hash": binding.bound_renderer_hash,
            "bound_dirty_rule_hash": binding.bound_dirty_rule_hash,
            "metadata": dict(binding.dependency.metadata),
        }
        for binding in bindings
    ]
    if not binding_rows:
        cur.execute(
            """
            WITH completed_node AS (
              UPDATE okg.derived_schedule_nodes
                 SET status = %(status)s,
                     run_id = %(run_id)s,
                     dirty_scope_count = %(dirty_scope_count)s,
                     upstream_binding_count = %(upstream_binding_count)s,
                     completed_at = now()
               WHERE plan_id = %(plan_id)s
                 AND definition_id = %(definition_id)s
              RETURNING 1
            ),
            completed_plan AS (
              UPDATE okg.derived_schedule_plans
                 SET status = 'completed',
                     completed_at = now()
               WHERE plan_id = %(plan_id)s
                 AND %(complete_plan)s
              RETURNING 1
            )
            SELECT
              0,
              (SELECT count(*) FROM completed_node)::int,
              (SELECT count(*) FROM completed_plan)::int
            """,
            {
                "run_id": run_id,
                "plan_id": plan_id,
                "definition_id": definition_id,
                "status": status,
                "dirty_scope_count": dirty_scope_count,
                "upstream_binding_count": upstream_binding_count,
                "complete_plan": complete_plan,
            },
        )
        row = cur.fetchone()
        if row is None:
            return 0, 0, 0
        return int(row[0]), int(row[1]), int(row[2])

    cur.execute(
        """
        WITH binding_input AS (
          SELECT *
            FROM jsonb_to_recordset(%(bindings)s::jsonb) AS row(
              upstream_definition_id TEXT,
              upstream_manifest_id UUID,
              upstream_slot_name TEXT,
              upstream_target_generation_id BIGINT,
              scope_key TEXT,
              required_compute_hash TEXT,
              required_output_schema_hash TEXT,
              required_renderer_hash TEXT,
              required_dirty_rule_hash TEXT,
              bound_compute_hash TEXT,
              bound_output_schema_hash TEXT,
              bound_renderer_hash TEXT,
              bound_dirty_rule_hash TEXT,
              metadata JSONB
            )
        ),
        persisted_bindings AS (
          INSERT INTO okg.derived_run_dependency_bindings (
            run_id, downstream_definition_id,
            upstream_definition_id, upstream_manifest_id,
            upstream_slot_name, upstream_target_generation_id,
            scope_key, required_compute_hash,
            required_output_schema_hash, required_renderer_hash,
            required_dirty_rule_hash, bound_compute_hash,
            bound_output_schema_hash, bound_renderer_hash,
            bound_dirty_rule_hash, metadata
          )
          SELECT
            %(run_id)s,
            %(downstream_definition_id)s,
            upstream_definition_id,
            upstream_manifest_id,
            upstream_slot_name,
            upstream_target_generation_id,
            scope_key,
            required_compute_hash,
            required_output_schema_hash,
            required_renderer_hash,
            required_dirty_rule_hash,
            bound_compute_hash,
            bound_output_schema_hash,
            bound_renderer_hash,
            bound_dirty_rule_hash,
            metadata
          FROM binding_input
          ON CONFLICT (
            run_id, upstream_definition_id, upstream_slot_name,
            (COALESCE(scope_key, ''))
          ) DO UPDATE SET
            upstream_manifest_id = EXCLUDED.upstream_manifest_id,
            bound_compute_hash = EXCLUDED.bound_compute_hash,
            bound_output_schema_hash = EXCLUDED.bound_output_schema_hash,
            bound_renderer_hash = EXCLUDED.bound_renderer_hash,
            bound_dirty_rule_hash = EXCLUDED.bound_dirty_rule_hash,
            metadata = EXCLUDED.metadata
          RETURNING 1
        ),
        completed_node AS (
          UPDATE okg.derived_schedule_nodes
             SET status = %(status)s,
                 run_id = %(run_id)s,
                 dirty_scope_count = %(dirty_scope_count)s,
                 upstream_binding_count = %(upstream_binding_count)s,
                 completed_at = now()
           WHERE plan_id = %(plan_id)s
             AND definition_id = %(definition_id)s
          RETURNING 1
        ),
        completed_plan AS (
          UPDATE okg.derived_schedule_plans
             SET status = 'completed',
                 completed_at = now()
           WHERE plan_id = %(plan_id)s
             AND %(complete_plan)s
          RETURNING 1
        )
        SELECT
          (SELECT count(*) FROM persisted_bindings)::int,
          (SELECT count(*) FROM completed_node)::int,
          (SELECT count(*) FROM completed_plan)::int
        """,
        {
            "bindings": Jsonb(binding_rows),
            "run_id": run_id,
            "downstream_definition_id": downstream_definition_id,
            "plan_id": plan_id,
            "definition_id": definition_id,
            "status": status,
            "dirty_scope_count": dirty_scope_count,
            "upstream_binding_count": upstream_binding_count,
            "complete_plan": complete_plan,
        },
    )
    row = cur.fetchone()
    if row is None:
        return 0, 0, 0
    return int(row[0]), int(row[1]), int(row[2])


def _persist_bindings_in_cursor(
    cur: psycopg.Cursor[Any],
    *,
    run_id: UUID,
    downstream_definition_id: str,
    bindings: Sequence[ResolvedManifestBinding],
) -> None:
    for binding in bindings:
        dep = binding.dependency
        cur.execute(
            """
            INSERT INTO okg.derived_run_dependency_bindings (
              run_id, downstream_definition_id,
              upstream_definition_id, upstream_manifest_id,
              upstream_slot_name, upstream_target_generation_id,
              scope_key, required_compute_hash,
              required_output_schema_hash, required_renderer_hash,
              required_dirty_rule_hash, bound_compute_hash,
              bound_output_schema_hash, bound_renderer_hash,
              bound_dirty_rule_hash, metadata
            ) VALUES (
              %(run_id)s, %(downstream_definition_id)s,
              %(upstream_definition_id)s, %(upstream_manifest_id)s,
              %(upstream_slot_name)s,
              %(upstream_target_generation_id)s, %(scope_key)s,
              %(required_compute_hash)s,
              %(required_output_schema_hash)s,
              %(required_renderer_hash)s,
              %(required_dirty_rule_hash)s,
              %(bound_compute_hash)s,
              %(bound_output_schema_hash)s,
              %(bound_renderer_hash)s,
              %(bound_dirty_rule_hash)s,
              %(metadata)s
            )
            ON CONFLICT (
              run_id, upstream_definition_id, upstream_slot_name,
              (COALESCE(scope_key, ''))
            ) DO UPDATE SET
              upstream_manifest_id = EXCLUDED.upstream_manifest_id,
              bound_compute_hash = EXCLUDED.bound_compute_hash,
              bound_output_schema_hash = EXCLUDED.bound_output_schema_hash,
              bound_renderer_hash = EXCLUDED.bound_renderer_hash,
              bound_dirty_rule_hash = EXCLUDED.bound_dirty_rule_hash,
              metadata = EXCLUDED.metadata
            """,
            {
                "run_id": run_id,
                "downstream_definition_id": downstream_definition_id,
                "upstream_definition_id": dep.upstream_definition_id,
                "upstream_manifest_id": binding.upstream_manifest_id,
                "upstream_slot_name": dep.upstream_slot_name,
                "upstream_target_generation_id": (
                    binding.upstream_target_generation_id
                ),
                "scope_key": dep.scope_key,
                "required_compute_hash": dep.required_compute_hash,
                "required_output_schema_hash": dep.required_output_schema_hash,
                "required_renderer_hash": dep.required_renderer_hash,
                "required_dirty_rule_hash": dep.required_dirty_rule_hash,
                "bound_compute_hash": binding.bound_compute_hash,
                "bound_output_schema_hash": binding.bound_output_schema_hash,
                "bound_renderer_hash": binding.bound_renderer_hash,
                "bound_dirty_rule_hash": binding.bound_dirty_rule_hash,
                "metadata": Jsonb(dict(dep.metadata)),
            },
        )


def _mark_node_running(
    conn: psycopg.Connection[Any],
    plan_id: UUID,
    definition_id: str,
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.derived_schedule_nodes
                   SET status = 'running',
                       started_at = now()
                 WHERE plan_id = %(plan_id)s
                   AND definition_id = %(definition_id)s
                """,
                {"plan_id": plan_id, "definition_id": definition_id},
            )


def _mark_node_completed(
    conn: psycopg.Connection[Any],
    *,
    plan_id: UUID,
    definition_id: str,
    run_id: UUID,
    status: str,
    dirty_scope_count: int,
    upstream_binding_count: int,
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.derived_schedule_nodes
                   SET status = %(status)s,
                       run_id = %(run_id)s,
                       dirty_scope_count = %(dirty_scope_count)s,
                       upstream_binding_count =
                         %(upstream_binding_count)s,
                       completed_at = now()
                 WHERE plan_id = %(plan_id)s
                   AND definition_id = %(definition_id)s
                """,
                {
                    "plan_id": plan_id,
                    "definition_id": definition_id,
                    "run_id": run_id,
                    "status": status,
                    "dirty_scope_count": dirty_scope_count,
                    "upstream_binding_count": upstream_binding_count,
                },
            )


def _mark_node_failed(
    conn: psycopg.Connection[Any],
    plan_id: UUID,
    definition_id: str,
    exc: Exception,
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.derived_schedule_nodes
                   SET status = 'failed',
                       error = %(error)s,
                       completed_at = now()
                 WHERE plan_id = %(plan_id)s
                   AND definition_id = %(definition_id)s
                """,
                {
                    "plan_id": plan_id,
                    "definition_id": definition_id,
                    "error": Jsonb(
                        {
                            "type": type(exc).__name__,
                            "message": str(exc),
                        }
                    ),
                },
            )


def _complete_plan(conn: psycopg.Connection[Any], plan_id: UUID) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.derived_schedule_plans
                   SET status = 'completed',
                       completed_at = now()
                 WHERE plan_id = %(plan_id)s
                """,
                {"plan_id": plan_id},
            )


def _fail_plan(
    conn: psycopg.Connection[Any],
    plan_id: UUID,
    exc: Exception,
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.derived_schedule_plans
                   SET status = 'failed',
                       error = %(error)s,
                       completed_at = now()
                 WHERE plan_id = %(plan_id)s
                """,
                {
                    "plan_id": plan_id,
                    "error": Jsonb(
                        {
                            "type": type(exc).__name__,
                            "message": str(exc),
                        }
                    ),
                },
            )
