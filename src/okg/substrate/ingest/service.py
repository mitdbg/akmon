"""Reusable substrate ingest orchestration.

The CLI and worker both need the same source-running and inter-layer
publish behavior. This module owns that orchestration without depending
on CLI parsing or rendering.
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import time
import uuid
import contextvars
from contextlib import nullcontext
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable

import psycopg

from okg.substrate import fastyaml
from okg.substrate.admission import (
    admission_mode_awaits_decision,
    apply_generation_feedback,
    attach_fact_range_refs_for_run,
    ensure_envelope,
    graph_branch_for_admission_mode,
    record_held_run_submitted as _record_review_submitted,
    stable_idempotency_key,
    transition_envelope,
    workflow_ref_from_dbos,
)
from okg.substrate.commit_scope import commit_scope, db_scope_key, memoized
from okg.substrate.ingest.layer_write_sets import (
    _ENRICHER_LINEAGE_FROM_CHRONOS,
    _compact_large_graph_write_set_result,
    _compact_large_graph_write_sets,
    _int_result_count,
    _merge_layer_chronos_dirty_scope_hint,
    _merge_layer_enricher_lineage_invalidations_hint,
)
from okg.substrate.errors import (
    SourceProcessBindingChanged,
    SourceRegistrySelectorAbsent,
    SourceRunAlreadyActive,
    SubstrateError,
)
from okg.substrate.images.render import images_config_for_deployment
from okg.substrate.ingest.planning import (
    ActivationProbeResult,
    IngestPlan,
    plan_ingest_sources,
    resolve_predecessor_deps,
    source_events_with_inherited_path_scope,
    topo_layers,
    topo_sort,
    validate_deferred_source_activation,
)
from okg.substrate.ingest import full_retraction_guard as _retraction_guard
from okg.substrate.ingest.runner import (
    dedupe_report_fields,
    image_report_fields,
    sanitize_report_fields as _sanitize_report_fields,
    SourceRunner,
    SourceRunContext,
    profile_native_mode,
    upsert_source_progress,
)
from okg.substrate.ingest.zero_output_admission import (  # noqa: F401  (re-exported for callers and tests)
    _source_history_kwargs,
    _source_health_is_healthy,
    _zero_output_admission_verdict,
    _zero_output_rejection_reason,
)
from okg.substrate.ingest.record_authority_service import (
    PrivateAuthorityCandidate,
    private_authority_candidate_from_result,
    record_pending_authority_admission,
    resolve_record_authority_format,
    seal_bound_authority,
)
from okg.substrate.ingest.source_terminal_state import (
    persist_accepted_source_terminal_state as _persist_accepted_source_terminal_state,
    source_terminal_state_evidence as _source_terminal_state_evidence,
)
from okg.substrate.ingest.adapter_factory import (
    ConstructedSourceAdapter,
    SourceAdapterPlan,
    bind_source_adapter_identity,
    construct_source_adapter_from_plan,
    import_source_adapter_class,
    plan_source_registry_bindings,
    source_adapter_graph_branch,
    source_adapter_init_params,
    source_param_uses_deployment_dsn_placeholder,
)
from okg.substrate.ingest.source_construction import (
    classify_source_construction_failure,
    evict_module_tree,
    retry_construction_after_eviction,
)
from okg.substrate.ingest.source_params import (
    explain_legacy_construct_failure,
)
from okg.substrate.producer_authority import (
    ProducerPolicyViolation,
    evaluate_source_run_signature,
    load_profile_by_fingerprint,
    materialize_producer_profile,
    merge_policy_evidence,
)
from okg.substrate.profiling import profile_stage, profiling_context
from okg.substrate.profiling.policy import ProfilingPolicy, resolve_policy
from okg.substrate.runtime.source_sync_constants import SOURCE_SYNC_WORKFLOW_NAME
from okg.substrate.runtime.source_sync_priority import ingest_run_guard

# Hoisted to module level so the publisher.py module's ~44 ms cold-
# start import happens once per process (at worker boot / first
# `from okg.substrate.ingest.service import run_ingest`) rather than
# lazily inside `_should_skip_layer_publish` on the critical path
# between ingest layers. The previous lazy import showed up as a 45
# ms inter-layer gap on the palimpzest-tier1-100 timeline.
from okg.substrate.publisher.deployment_stack import (
    build_deployment_publisher_stack as _build_deployment_publisher_stack,
)
from okg.substrate.source_run_modes import accepts as accepts_run_mode
from okg.substrate.source_sync import (
    SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES,
    SourceSyncDependentFanout,
    SourceSyncEvent,
    # Called through this module by ingest/source_job_runtime.py, so a test
    # that patches it here patches the moved code too.
    source_sync_scope_payload,  # noqa: F401
)
from okg.substrate.ingest.pii_posture import resolve_deployment_source_pii_posture
from okg.substrate.ingest import incomplete_recovery, published_completeness
from okg.substrate.ingest import source_job_runtime, source_jobs
from okg.substrate.ingest import record_set_coverage_state
from okg.substrate.nomos.rollout import runtime_enforce_mode_dsn_keys
from okg.substrate.source_sync_convergence import READY_SOURCES
from okg.substrate.sources.registry import (
    STRICT_ADMISSION_CONTRACT,
    SourceEntry,
    SourceRegistryAdmission,
    load_source_registry_admission,
)
from okg.substrate.telemetry import current_trace_identity, use_trace
from okg.substrate.support.user_hints import okg_prefix


log = logging.getLogger(__name__)

# Temporary private compatibility alias while callers migrate to the owner.
_source_events_with_inherited_path_scope = source_events_with_inherited_path_scope


# Max source child processes one source-sync step runs at once. The width
# lives on the `ingest.source_layer` term in db/concurrency_budget.py; each
# child's own connections are admitted against the live server count by
# `ingest/source_jobs.py` before it starts.
def _layer_max_concurrency() -> int:
    from okg.substrate.db.concurrency_budget import width

    return width("ingest.source_layer")


@dataclass(frozen=True)
class IngestOptions:
    """Options for one deployment ingest orchestration run."""

    dsn: str
    deployment: str
    deployment_dir: Path
    include: Collection[str] | None = None
    exclude: Collection[str] = field(default_factory=set)
    include_predecessors: bool = True
    include_dependents: bool = False
    dependent_fanout: SourceSyncDependentFanout | None = None
    source_events: Mapping[str, tuple[SourceSyncEvent, ...]] = field(
        default_factory=dict,
    )
    run_mode: str = "scope_complete"
    #: True when the operator typed `--mode`. An explicit mode is used as
    #: given; only an omitted one is translated per strict source profile.
    run_mode_explicit: bool = False
    publish: bool = True
    redrive_staged_before_sources: bool = False  # publish=False callers that publish after
    owner_id: str | None = None
    owner_meta: Mapping[str, Any] = field(default_factory=dict)
    profiling_policy: ProfilingPolicy | None = None
    profile_force_deep: bool = False
    command: str | None = None
    admission_observed_at: datetime | None = None
    max_source_workers: int | None = None
    reset_cursor: bool = False
    allow_full_retraction: Collection[str] = ()  # issue #2818 backstop
    force_enrichers: bool = False
    source_registry_repo_root_override: Path | str | None = None
    source_registry_admission: SourceRegistryAdmission | None = None
    source_binding_plans: Mapping[str, SourceAdapterPlan] = field(
        default_factory=dict,
    )
    constructed_source_bindings: Mapping[str, ConstructedSourceAdapter] = field(
        default_factory=dict,
    )
    require_declared_sources: bool = False
    run_modes_by_source: Mapping[str, str] = field(default_factory=dict)
    #: Hold the deployment's source-sync signal for the whole run (#3003).
    #: Only the runtime worker, serialized by its own queue, passes False.
    hold_source_sync_signal: bool = True
    sync_wait_seconds: float | None = None  # None: env or 600 s default

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_modes_by_source", MappingProxyType(dict(self.run_modes_by_source)))


@dataclass(frozen=True)
class IngestRunResult:
    """Structured result returned by the ingest service."""

    report: dict[str, Any]
    any_failed: bool


@dataclass(frozen=True)
class _AdmissionFinalizeResult:
    """Result of post-run source admission evaluation."""

    refs: list[Any] = field(default_factory=list)
    status: str = "accepted"
    reason: str | None = None
    verdict: Mapping[str, Any] | None = None

    @property
    def rejected(self) -> bool:
        return self.status == "rejected"


def run_ingest(options: IngestOptions) -> IngestRunResult:
    """Run registry sources in dependency layers, publishing per layer.

    Opens a per-commit memo scope (:func:`okg.substrate.commit_scope`) so the
    catalog version and Nomos policy bundle — invariant within one run — are
    read at most once per commit instead of once per dependency layer.
    """
    from okg.substrate.deployments.admission import require_active_deployment
    require_active_deployment(options.deployment_dir)
    telemetry_context = (
        nullcontext() if current_trace_identity().is_contextual else use_trace()
    )
    run_guard = ingest_run_guard(
        dsn=options.dsn,
        deployment=options.deployment,
        deployment_dir=options.deployment_dir,
        hold=options.hold_source_sync_signal,
        wait_seconds=options.sync_wait_seconds,
    )
    with telemetry_context, run_guard:
        with commit_scope():
            result = _run_ingest_inner(options)
    return importlib.import_module("okg.substrate.db.index_reclaim").rebuild_indexes_after_ingest(options, result)


def _reload_and_plan_strict_admission(
    *,
    admission: SourceRegistryAdmission,
    dsn: str,
    deployment: str,
    registry_path: Path,
    repo_root_override: Path | str | None,
    required_source_selectors: tuple[str, ...] = (),
) -> tuple[SourceRegistryAdmission, Mapping[str, SourceAdapterPlan]]:
    """Evict every strict class, reload the document, and replan it once."""
    from okg.substrate.deployments import deployment_module_import_lock

    with deployment_module_import_lock():
        for entry in admission.entries.values():
            module_name = str(entry.raw.get("module") or "")
            if module_name:
                evict_module_tree(module_name)
        importlib.invalidate_caches()
        fresh = load_source_registry_admission(
            registry_path,
            repo_root_override=repo_root_override,
            force_reload=True,
        )
        _require_declared_source_selectors(
            fresh.entries,
            selectors=required_source_selectors,
            deployment=deployment,
        )
        plans = plan_source_registry_bindings(
            fresh,
            dsn=dsn,
            deployment=deployment,
        ).plans
    return fresh, plans


def _plan_strict_admission_with_recovery(
    *,
    admission: SourceRegistryAdmission,
    dsn: str,
    deployment: str,
    registry_path: Path,
    repo_root_override: Path | str | None,
    required_source_selectors: tuple[str, ...] = (),
) -> tuple[SourceRegistryAdmission, Mapping[str, SourceAdapterPlan]]:
    """Retry a pre-construction strict plan once against fresh module bytes."""
    try:
        plans = plan_source_registry_bindings(
            admission,
            dsn=dsn,
            deployment=deployment,
        ).plans
    except (AttributeError, ImportError, TypeError, ValueError):
        if not admission.strict:
            raise
        return _reload_and_plan_strict_admission(
            admission=admission,
            dsn=dsn,
            deployment=deployment,
            registry_path=registry_path,
            repo_root_override=repo_root_override,
            required_source_selectors=required_source_selectors,
        )
    return admission, plans


def _construct_strict_selected_bindings(
    *,
    admission: SourceRegistryAdmission,
    plans: Mapping[str, SourceAdapterPlan],
    selected_names: Iterable[str],
    dsn: str,
    deployment: str,
    registry_path: Path,
    repo_root_override: Path | str | None,
    reselect_names: Callable[[Mapping[str, SourceEntry]], Iterable[str]],
    required_source_selectors: tuple[str, ...] = (),
    allow_stale_retry: bool = True,
) -> tuple[
    SourceRegistryAdmission,
    Mapping[str, SourceAdapterPlan],
    Mapping[str, ConstructedSourceAdapter],
]:
    """Construct a strict selected set only after its whole document plans.

    A proven stale class invalidates every plan, not just the failing source.
    The registry is parsed again and all classes are replanned before any retry.
    """
    constructed: dict[str, ConstructedSourceAdapter] = {}
    for name in sorted(selected_names):
        plan = plans.get(name)
        if plan is None:
            continue
        try:
            constructed[name] = construct_source_adapter_from_plan(plan)
        except TypeError as exc:
            diagnosis = classify_source_construction_failure(
                exc=exc,
                stage="construct",
                module_name=str(plan.entry.raw.get("module") or ""),
                class_name=str(plan.entry.raw.get("class") or ""),
                params=plan.init_params,
            )
            if (
                not allow_stale_retry
                or diagnosis is None
                or diagnosis.get("cause") != "stale_loaded_code"
            ):
                raise
            fresh_admission, fresh_plans = _reload_and_plan_strict_admission(
                admission=admission,
                dsn=dsn,
                deployment=deployment,
                registry_path=registry_path,
                repo_root_override=repo_root_override,
                required_source_selectors=required_source_selectors,
            )
            return _construct_strict_selected_bindings(
                admission=fresh_admission,
                plans=fresh_plans,
                selected_names=reselect_names(fresh_admission.entries),
                dsn=dsn,
                deployment=deployment,
                registry_path=registry_path,
                repo_root_override=repo_root_override,
                reselect_names=reselect_names,
                required_source_selectors=required_source_selectors,
                allow_stale_retry=False,
            )
    return admission, plans, MappingProxyType(constructed)


def _require_declared_source_selectors(
    entries: Mapping[str, SourceEntry],
    *,
    selectors: Collection[str],
    deployment: str,
) -> None:
    """Refuse explicit selectors absent from current registry authority."""
    if not selectors:
        return
    declared_names = set(entries)
    declared_ownership_ids = {
        entry.ownership_id
        for entry in entries.values()
        if entry.ownership_id
    }
    absent = sorted(
        set(selectors) - declared_names - declared_ownership_ids,
    )
    if absent:
        raise SourceRegistrySelectorAbsent(
            "explicit source selector is absent from the current source registry",
            data={
                "deployment": deployment,
                "absent_selectors": absent,
                "declared_source_names": sorted(declared_names),
                "declared_ownership_ids": sorted(declared_ownership_ids),
            },
            fix_hint=(
                "remove the stale selector from queued work or restore its "
                "source_registry.yaml declaration"
            ),
        )


def _validate_run_modes_by_source(options: IngestOptions, plan: Any) -> None:
    if not options.run_modes_by_source:
        return
    if set(options.run_modes_by_source) != set(plan.ordered):
        raise SubstrateError(
            "per-source run modes must cover the exact selected source closure",
            data={"missing": sorted(set(plan.ordered) - set(options.run_modes_by_source)),
                  "unknown": sorted(set(options.run_modes_by_source) - set(plan.ordered))},
        )
    for name in plan.ordered:
        entry = plan.runnable[name]
        if not accepts_run_mode(entry.source_class, options.run_modes_by_source[name]):
            raise SubstrateError("per-source run mode is incompatible with its source profile",
                                 data={"source": name, "source_class": entry.source_class})


def _run_ingest_inner(options: IngestOptions) -> IngestRunResult:
    deployment_dir = Path(options.deployment_dir)
    if options.max_source_workers is not None and options.max_source_workers < 1:
        raise SubstrateError(
            "max_source_workers must be at least 1",
            data={"max_source_workers": options.max_source_workers},
        )
    from okg.substrate.deployments.sync_root import (
        require_usable_source_configuration_authority,
    )

    canonical_source_root = require_usable_source_configuration_authority(
        options.source_registry_repo_root_override,
        command=options.command or f"ingest for {options.deployment}",
    )
    if canonical_source_root is not options.source_registry_repo_root_override:
        options = replace(
            options,
            source_registry_repo_root_override=canonical_source_root,
        )
    from okg.substrate.deployments import resolve_source_registry_path

    registry_path = resolve_source_registry_path(deployment_dir)
    if not registry_path.is_file():
        raise SubstrateError(
            f"{registry_path} does not exist",
            fix_hint="declare a valid source_registry path in deployment.yaml",
        )

    try:
        admission = load_source_registry_admission(
            registry_path,
            repo_root_override=options.source_registry_repo_root_override,
        )
    except (ValueError, OSError) as e:
        err = SubstrateError(f"source_registry.yaml is invalid: {e}")
        err.code = "source_registry_invalid"
        raise err from None

    required_source_selectors = (
        tuple(options.include or ())
        if options.require_declared_sources
        else ()
    )
    _require_declared_source_selectors(
        admission.entries,
        selectors=required_source_selectors,
        deployment=options.deployment,
    )

    admission, binding_plans = _plan_strict_admission_with_recovery(
        admission=admission,
        dsn=options.dsn,
        deployment=options.deployment,
        registry_path=registry_path,
        repo_root_override=options.source_registry_repo_root_override,
        required_source_selectors=required_source_selectors,
    )
    plan = plan_ingest_sources(
        admission.entries,
        include=options.include,
        exclude=options.exclude,
        include_predecessors=options.include_predecessors,
        include_dependents=options.include_dependents,
        dependent_fanout=options.dependent_fanout,
    )
    _validate_run_modes_by_source(options, plan)
    _retraction_guard.validate_full_retraction_override_names(options.allow_full_retraction, admission.entries, plan.ordered)
    _validate_deferred_source_activation(
        plan,
        deployment_dir=deployment_dir,
        registry_path=registry_path,
    )
    if admission.strict:

        def reselect_strict_names(
            entries: Mapping[str, SourceEntry],
        ) -> Iterable[str]:
            fresh_plan = plan_ingest_sources(
                entries,
                include=options.include,
                exclude=options.exclude,
                include_predecessors=options.include_predecessors,
                include_dependents=options.include_dependents,
                dependent_fanout=options.dependent_fanout,
            )
            _validate_run_modes_by_source(options, fresh_plan)
            _validate_deferred_source_activation(
                fresh_plan,
                deployment_dir=deployment_dir,
                registry_path=registry_path,
            )
            return tuple(fresh_plan.ordered)

        admission, binding_plans, constructed_bindings = (
            _construct_strict_selected_bindings(
                admission=admission,
                plans=binding_plans,
                selected_names=plan.ordered,
                dsn=options.dsn,
                deployment=options.deployment,
                registry_path=registry_path,
                repo_root_override=options.source_registry_repo_root_override,
                reselect_names=reselect_strict_names,
                required_source_selectors=required_source_selectors,
            )
        )
        # A stale-code retry creates a fresh frozen entry set. Rebuild source
        # selection against that exact authority before any mutation starts.
        plan = plan_ingest_sources(
            admission.entries,
            include=options.include,
            exclude=options.exclude,
            include_predecessors=options.include_predecessors,
            include_dependents=options.include_dependents,
            dependent_fanout=options.dependent_fanout,
        )
        options = replace(
            options,
            source_registry_admission=admission,
            source_binding_plans=binding_plans,
            constructed_source_bindings=constructed_bindings,
        )

    # Pre-warm only after strict schema, semantic, class/signature, and
    # selected-construction barriers. The stack builder can touch runtime/DB
    # authority and therefore must never race ahead of a refused document.
    import threading as _threading

    def _prewarm_stack() -> None:
        try:
            _build_deployment_publisher_stack(
                dsn=options.dsn,
                deployment_name=options.deployment,
                deployment_dir=Path(options.deployment_dir),
            )
        except Exception:  # noqa: BLE001 - best-effort prewarm only
            pass

    if options.publish:
        # Run in a copy of this command's context so deployment lookups by
        # name see a folder the command resolved (#2951 binding).
        _prewarm_thread = _threading.Thread(
            target=contextvars.copy_context().run,
            args=(_prewarm_stack,),
            daemon=True,
            name="okg-ingest-prewarm-stack",
        )
        _prewarm_thread.start()
    source_events = _source_events_with_inherited_path_scope(
        options.source_events,
        plan,
    )
    if source_events is not options.source_events:
        options = replace(options, source_events=source_events)
    layers = plan.layers
    report: dict[str, Any] = {
        "deployment": options.deployment,
        "ordered": list(plan.ordered),
        "results": [],
        "publishes": [],
        "skipped": [],
    }
    run_started = time.perf_counter()
    layer_timings: list[dict[str, Any]] = []
    any_failed = False
    unavailable_sources: set[str] = set()
    failed_source_details: list[dict[str, Any]] = []
    dependency_blocked_sources: dict[str, list[str]] = {}
    # Per-layer source write-sets, accumulated across the commit's layers so the
    # single end-of-commit publish receives the UNION dirty-scope hint.
    accumulated_layer_results: list[dict[str, Any]] = []
    policy = options.profiling_policy or resolve_policy(None)
    command = options.command or f"okg ingest --deployment {options.deployment}"
    recovery_source_contract = incomplete_recovery.build_source_contract(
        ordered=plan.ordered,
        runnable=plan.runnable,
        deps=plan.deps,
        source_events=options.source_events,
        source_binding_plans=options.source_binding_plans,
        run_mode_for_source=lambda entry, name, events, sync_scope: (
            _run_mode_for_source(
                options,
                entry,
                name,
                events,
                sync_scope,
            )
        ),
    )
    recovery_authority: dict[str, Any] | None = None

    with profiling_context(
        deployment=options.deployment,
        target_kind="ingest",
        command=command,
        policy=policy,
        force_deep=options.profile_force_deep,
    ) as profile_ctx:
        # Heal work staged by an earlier refused publish or crash (#1889: also
        # for a caller that publishes once after ingest) before new admissions.
        # Critical on multi-source deployments: if a current source later fails,
        # its run-scoped blocker must not terminally block older accepted work
        # that #828 requires the same ingest command to sweep to published.
        staged_redrive_needed = False
        if options.publish or options.redrive_staged_before_sources:
            try:
                redrive_state = _staged_source_redrive_state(options)
                staged_redrive_needed = bool(redrive_state["staged_work_present"])
                report["staged_source_redrive_probe"] = (
                    incomplete_recovery.redrive_probe_report(redrive_state)
                )
            except Exception as exc:  # noqa: BLE001 - fail before source writes
                raise SubstrateError(
                    "cannot determine whether staged source work requires "
                    "a pre-ingest publish",
                    data={
                        "deployment": options.deployment,
                        "error_type": type(exc).__name__,
                    },
                    fix_hint=(
                        "restore admission-ledger access, then retry ingest; "
                        "new source writes were not started"
                    ),
                ) from exc
            recovery_resolution = incomplete_recovery.resume_before_source_writes(
                redrive_state=redrive_state,
                deployment=options.deployment,
                source_contract=recovery_source_contract,
                ensure_feedback=lambda checkpoint_id, recovery: (
                    incomplete_recovery.ensure_frozen_feedback(
                        dsn=options.dsn,
                        deployment=options.deployment,
                        checkpoint_id=checkpoint_id,
                        recovery=recovery,
                    )
                ),
                load_state=lambda: _staged_source_redrive_state(options),
                promote=lambda state, recovery: _promote_frozen_recovery(
                    options,
                    redrive_state=state,
                    recovery=recovery,
                    ingest_completeness=published_completeness.deferred_run_marker(
                        state["latest_blocked_receipt"], admission.entries,
                    ),  # #3375: the frozen repair run's own marker
                ),
                source_profiles={
                    name: plan.runnable[name].source_class
                    for name in plan.ordered
                },
            )
            recovery_authority = recovery_resolution["authority"]
            redrive_state = recovery_resolution["redrive_state"]
            staged_redrive_needed = recovery_resolution["staged_redrive_needed"]
            if recovery_resolution["report"] is not None:
                report["incomplete_recovery"] = recovery_resolution["report"]
            if recovery_resolution["promotion"] is not None:
                report["publishes"].append(recovery_resolution["promotion"])
        if staged_redrive_needed:
            redrive_started = time.perf_counter()
            with profile_stage("ingest.publish.staged_source_redrive"):
                redrive = _publish(
                    options,
                    "staged-source-redrive",
                    expected_latest_checkpoint_id=redrive_state["latest_checkpoint_id"],
                    expected_branch_mutation_token=redrive_state[
                        "branch_mutation_token"
                    ],
                    require_expected_latest_checkpoint=True,
                )
            report["publishes"].append(redrive)
            persist_publish_stage_timings(
                dsn=options.dsn,
                deployment=options.deployment,
                published=redrive,
            )
            layer_timings.append(
                {
                    "index": "staged-source-redrive",
                    "sources": [],
                    "source_wall_ms": 0.0,
                    "source_reported_total_ms": 0.0,
                    "publish_wall_ms": _elapsed_ms(redrive_started),
                    "publish_reported_ms": _publish_reported_ms(redrive),
                    "publish_status": str(redrive.get("status") or "unknown"),
                    "staged_source_drive": True,
                }
            )
            if redrive.get("status") != "published" and not _run_base.rerun_may_proceed(
                redrive, {plan.runnable[n].ownership_id for n in plan.ordered}
            ):
                # Forward the refusing publish's own diagnosis. Before #1791
                # this guard kept only `status` and `code`, so the operator
                # saw a generic "resolve the blocker" hint and never the
                # cause (e.g. `drifted_blocks`) or the repair command.
                guard_data: dict[str, Any] = {
                    "deployment": options.deployment,
                    "publish_status": redrive.get("status"),
                    "publish_code": redrive.get("code"),
                }
                publish_error = redrive.get("error")
                if publish_error is not None:
                    guard_data["publish_error"] = str(publish_error)
                publish_error_data = redrive.get("error_data")
                if isinstance(publish_error_data, Mapping):
                    guard_data["publish_error_data"] = dict(publish_error_data)
                elif publish_error_data is not None:
                    guard_data["publish_error_data"] = publish_error_data
                # A BLOCKED publish carries no error/fix_hint; its diagnosis
                # is the structured `blocked_reason` (failed gates, incomplete
                # sources), so forward that too.
                publish_blocked_reason = redrive.get("blocked_reason")
                if isinstance(publish_blocked_reason, Mapping):
                    guard_data["publish_blocked_reason"] = dict(
                        publish_blocked_reason
                    )
                elif publish_blocked_reason:
                    guard_data["publish_blocked_reason"] = str(
                        publish_blocked_reason
                    )
                publish_fix_hint = redrive.get("fix_hint")
                # Name the command that DRAINS the staged work. Before #2213
                # this hint forwarded only the refusing publish's own hint, so
                # the guided add-source flow told the operator to re-claim
                # ownership and never that the staged rows still had to be
                # published; re-claiming alone left the same refusal in place.
                drain_command = (
                    f"{okg_prefix()} run --once --apply --deployment "
                    f"{options.deployment}"
                )
                drain_hint = (
                    "publish the already-staged work with: "
                    f"{drain_command} -- then re-run this command"
                )
                guard_data["drain_command"] = drain_command
                if redrive.get("code") in _run_base.UNDO_BLOCK_CODES:
                    # #2993: the drain publish is refused too until the undo is
                    # finished, so the undo's own commands come first.
                    drain_hint = f"{publish_fix_hint}; then run {drain_command}"
                    publish_fix_hint = None
                raise SubstrateError(
                    "staged source work was not published; refusing to start "
                    "new source writes",
                    data=guard_data,
                    fix_hint=(
                        f"{drain_hint}. That publish was refused: "
                        f"{publish_fix_hint}"
                        if publish_fix_hint
                        else drain_hint
                    ),
                )

        # Lane H stage 1 (PR #2917 section 3): every source runs in its own
        # `spawn` child process and starts as soon as its predecessors
        # finish, instead of as a thread in this process layer by layer. The
        # threads shared one interpreter lock: one process used 0.97 cores
        # while 79% of source-thread samples waited on that lock. Graph
        # writes still serialize on the branch lock; reading, parsing,
        # cleaning and serialization now run in parallel.
        planned_sources = [name for layer in layers for name in layer]
        # Resolve every module name before any child starts, so a bad name
        # fails at the child's import stage before it writes anything.
        _preimport_layer_source_modules(plan, planned_sources)
        max_children = _layer_max_concurrency()
        if options.max_source_workers is not None:
            max_children = min(max_children, options.max_source_workers)
        max_children = max(1, max_children)
        with source_job_runtime.open_source_job_runtime(options, plan) as job_runtime:
            job_outcome = source_jobs.run_source_jobs(
                ordered=planned_sources,
                deps=plan.deps,
                runtime=job_runtime,
                max_concurrency=max_children,
            )
            # Merge the spill files while the step directory still exists.
            for result in job_outcome.results.values():
                source_job_runtime.merge_source_job_spill(result)
        report["source_jobs"] = {
            "max_concurrency": max_children,
            "start_order": list(job_outcome.start_order),
            "admission_waits": dict(job_outcome.admission_waits),
        }
        for idx, layer in enumerate(layers, start=1):
            layer_failed = False
            layer_results: list[dict[str, Any]] = []
            successful_layer_results: list[dict[str, Any]] = []
            blocked_by = {
                name: list(job_outcome.skipped[name])
                for name in layer
                if name in job_outcome.skipped
            }
            if blocked_by:
                dependency_blocked_sources.update(blocked_by)
                unavailable_sources.update(blocked_by)
                report["skipped"].append(
                    {
                        "reason": "upstream_source_failed",
                        "after_layer": idx - 1,
                        "sources": list(blocked_by),
                        "blocked_by": blocked_by,
                    }
                )
            runnable_layer = [name for name in layer if name not in blocked_by]
            layer_timing: dict[str, Any] = {
                "index": idx,
                "sources": list(layer),
                "scheduled_sources": list(runnable_layer),
                "dependency_blocked_sources": list(blocked_by),
                "source_wall_ms": 0.0,
                "source_reported_total_ms": 0.0,
                "publish_wall_ms": 0.0,
                "publish_reported_ms": 0.0,
                "publish_status": "not_run",
            }
            if not runnable_layer:
                layer_timing["publish_status"] = (
                    "skipped_dependency_failed" if options.publish else "disabled"
                )
                layer_timings.append(layer_timing)
                continue
            for name in runnable_layer:
                result = job_outcome.results[name]
                source_job_runtime.record_source_job_result(
                    options=options,
                    entry=plan.runnable[name],
                    name=name,
                    result=result,
                    started_at=job_outcome.started_at.get(name),
                    finished_at=job_outcome.finished_at.get(name),
                    profile_ctx=profile_ctx,
                )
                report["results"].append(result)
                layer_results.append(result)
                if result["status"] != "OK":
                    any_failed = True
                    layer_failed = True
                    unavailable_sources.add(name)
                    failed_source_details.append(
                        {
                            "source": name,
                            "stage": result.get("stage"),
                            "code": result.get("code"),
                        }
                    )
                else:
                    successful_layer_results.append(result)
                if not options.publish:
                    # No publish will read this write-set, so the exact
                    # ID lists are dead weight in the report. When a
                    # publish IS pending, the dirty-scope hint still
                    # needs them; they are compacted after the last
                    # publish instead.
                    _compact_large_graph_write_set_result(result)
            layer_starts = [
                job_outcome.started_at[name]
                for name in runnable_layer
                if name in job_outcome.started_at
            ]
            layer_ends = [
                job_outcome.finished_at[name]
                for name in runnable_layer
                if name in job_outcome.finished_at
            ]
            if layer_starts and layer_ends:
                layer_timing["source_wall_ms"] = round(
                    max(0.0, (max(layer_ends) - min(layer_starts)) * 1000), 3
                )
            layer_timing["source_reported_total_ms"] = _source_reported_total_ms(
                layer_results
            )
            if options.publish and successful_layer_results:
                # Defer the publish. Every layer in a commit writes to the same
                # branch, and a later layer reads the prior layer's COMMITTED
                # writes via the branch head (runner._checkout_graph_branch), not
                # via a published generation -- so publishing per layer is
                # redundant for cross-layer visibility. Accumulate each layer's
                # graph write-set and publish ONCE after the last layer with the
                # union dirty-scope hint. Admission-feedback and the consistency
                # gates are watermark-/branch-head-cumulative, so one
                # end-of-commit publish covers the whole delta. If a later layer
                # fails, the post-loop block still publishes the committed writes
                # with a forced full scan (see below), so they are not orphaned.
                accumulated_layer_results.extend(successful_layer_results)
                layer_timing["publish_status"] = (
                    "deferred_incomplete" if any_failed else "deferred_to_commit"
                )
            elif options.publish and layer_failed:
                layer_timing["publish_status"] = "skipped_source_failed"
            else:
                layer_timing["publish_status"] = "disabled"
            layer_timings.append(layer_timing)

        completeness = published_completeness.ingest_completeness(
            plan=plan,
            results=report["results"],
            failed_source_details=failed_source_details,
            dependency_blocked_sources=dependency_blocked_sources,
        )
        report["completeness"] = completeness
        source_plan_incomplete = completeness["status"] == "incomplete"

        # One publish for the whole commit: all layers' writes are already on
        # the branch head, so a single cycle covers the union delta. A clean run
        # trusts the union dirty-scope hint (the perf path) and is skipped only
        # when no layer appended work and no prior publisher work is pending.
        #
        # A PARTIAL FAILURE where one or more sources committed still publishes
        # those writes (#3342), with a forced full scan. They
        # would otherwise be ORPHANED: a later generation's checkpoint captures
        # them, but they are never gated or view-invalidated, because the next
        # run's dirty-scope hint omits them (those sources no-op once their
        # watermark advanced) and the hint REPLACES, not supplements, the Chronos
        # scan. So when any_failed we force a full scan (hint=None) so the whole
        # branch head -- every earlier layer AND the failed layer's successful
        # sources -- is gated + invalidated this run; the source failure is
        # already recorded above and any_failed stays set (the run still reports
        # failed). Gated on accumulated_layer_results so a commit that fails with
        # NO earlier committed layer (e.g. a single rejected source) publishes
        # nothing -- nothing to orphan, and no phantom generation.
        # close-refused-publish-redrive-gap (#828): a run that accumulated
        # nothing publishable (all layers failed -- e.g. a zero-output
        # rejection -- or produced nothing) must still publish when staged
        # sweepable source admissions exist. Those are prior runs' appended
        # facts whose publish was refused; only a real checkpoint drains
        # them, and on a single-source deployment THIS run is the only
        # driver. Without this, a prior_emission_unpublished rejection
        # would suppress the very publish that heals it.
        staged_source_drive = bool(
            options.publish
            and not accumulated_layer_results
            and _staged_source_drive_needed(options)
        )
        recovery_repair_run = bool(
            recovery_authority is not None
            and recovery_authority.get("state") == incomplete_recovery.REPAIR_REQUIRED
        )
        # #3342 reverses #987's req.publish.incomplete-blocked: an incomplete
        # run whose failed sources were all undone publishes, and the
        # generation's `ingest_completeness` marker names what failed. A repair
        # run freezes `frozen` and promotes in this run; only an unfinished
        # undo still freezes `repair_required` (published_completeness).
        publishes_incomplete = (
            source_plan_incomplete
            and not recovery_repair_run
            and published_completeness.partial_failure_may_publish(report["results"])
        )
        # A run that records a failure publishes through the accumulated path
        # below (a completed source is in it); one whose every source failed
        # publishes nothing.
        completeness_run = published_completeness.run_marker(
            completeness, plan.runnable, admission.entries,
        )
        changes_published_marker = lambda: published_completeness.run_changes_published_marker(  # noqa: E731
            dsn=options.dsn, deployment=options.deployment, run=completeness_run,
        )
        if options.publish and (accumulated_layer_results or staged_source_drive):
            label = "commit"
            commit_timing: dict[str, Any] = {
                "index": "commit",
                "sources": [],
                "source_wall_ms": 0.0,
                "source_reported_total_ms": 0.0,
                "publish_wall_ms": 0.0,
                "publish_reported_ms": 0.0,
                "publish_status": "not_run",
            }
            if staged_source_drive:
                # Auditable: this publish ran to drain staged
                # admissions, not because this run produced work.
                commit_timing["staged_source_drive"] = True
            if (
                not any_failed
                and not recovery_repair_run
                and _should_skip_layer_publish(options, accumulated_layer_results)
                and not changes_published_marker()
            ):
                skipped_publish = {
                    "label": label,
                    "status": "skipped",
                    "publish_skipped": True,
                    "reason": "no_source_work",
                }
                report["publishes"].append(skipped_publish)
                persist_publish_stage_timings(
                    dsn=options.dsn,
                    deployment=options.deployment,
                    published=skipped_publish,
                )
                commit_timing["publish_status"] = "skipped_no_source_work"
                layer_timings.append(commit_timing)
            else:
                recovery_freeze = (
                    source_plan_incomplete and not publishes_incomplete
                ) or recovery_repair_run
                freeze_state, publish_blocker = incomplete_recovery.prepare_freeze(
                    enabled=recovery_freeze,
                    deployment=options.deployment,
                    completeness=completeness,
                    state=published_completeness.freeze_state(
                        incomplete=source_plan_incomplete, results=report["results"],
                    ),
                    source_contract=recovery_source_contract,
                    load_state=lambda: _staged_source_redrive_state(options),
                    admission_ids_for=lambda predecessor: (
                        _recovery_candidate_admission_ids(
                            options,
                            predecessor_checkpoint_id=predecessor,
                        )
                    ),
                )
                # #3342: mark a freeze cut because an undo did not finish.
                published_completeness.stamp_freeze(publish_blocker, report["results"])
                # On a partial failure the union hint is incomplete (it lacks the
                # failed layer's committed sources), so a full Chronos scan is the
                # only complete dirty scope -- trust nothing, scan everything.
                dirty_hint = (
                    None
                    if recovery_freeze or source_plan_incomplete
                    else _merge_layer_chronos_dirty_scope_hint(
                        accumulated_layer_results
                    )
                )
                enricher_lineage_hint = (
                    None
                    if recovery_freeze or source_plan_incomplete
                    else _merge_layer_enricher_lineage_invalidations_hint(
                        accumulated_layer_results,
                    )
                )
                publish_started = time.perf_counter()
                with profile_stage(f"ingest.publish.{label}"):
                    trace_identity = _trace_identity_from_layer_results(
                        accumulated_layer_results,
                    )
                    active_trace_identity = current_trace_identity()
                    trace_context = nullcontext()
                    if (
                        not active_trace_identity.is_contextual
                        and trace_identity is not None
                    ):
                        trace_id, span_id = trace_identity
                        trace_context = (
                            use_trace(trace_id, span_id=span_id)
                            if span_id is not None
                            else use_trace(trace_id)
                        )
                    with trace_context:
                        publish_span_context = nullcontext(None)
                        if options.owner_meta.get("command") == "source_sync_workflow":
                            from okg.substrate.runtime.workflow_telemetry import (
                                source_sync_publish_span,
                            )

                            publish_span_context = source_sync_publish_span(
                                dsn=options.dsn,
                                deployment_name=options.deployment,
                                workflow_id=options.owner_meta.get("workflow_id"),
                            )
                        own_ids = [r.get("run_id") for r in report["results"]]
                        _run_base.INGEST_OWN_RUN_IDS.set(tuple(filter(None, own_ids)))
                        with publish_span_context as publish_span:
                            published = _publish(
                                options,
                                label,
                                dirty_hint,
                                enricher_lineage_hint,
                                ingest_completeness=completeness_run,
                                **incomplete_recovery.freeze_publish_kwargs(
                                    freeze_state=freeze_state,
                                    blocker=publish_blocker,
                                ),
                            )
                            if publish_span is not None:
                                publish_span.observe_result(published)
                published, frozen_publish, recovery_report = (
                    incomplete_recovery.promote_blocked_freeze(
                        published=published,
                        blocker=publish_blocker,
                        load_state=lambda: _staged_source_redrive_state(options),
                        promote=lambda state, recovery: _promote_frozen_recovery(
                            options,
                            redrive_state=state,
                            recovery=recovery,
                            ingest_completeness=completeness_run,
                        ),
                    )
                )
                if frozen_publish is not None:
                    report["publishes"].append(frozen_publish)
                    persist_publish_stage_timings(
                        dsn=options.dsn,
                        deployment=options.deployment,
                        published=frozen_publish,
                    )
                if recovery_report is not None:
                    report["incomplete_recovery"] = recovery_report
                commit_timing["publish_wall_ms"] = _elapsed_ms(publish_started)
                commit_timing["publish_reported_ms"] = _publish_reported_ms(
                    published,
                )
                commit_timing["publish_status"] = str(
                    published.get("status") or "unknown",
                )
                report["publishes"].append(published)
                persist_publish_stage_timings(
                    dsn=options.dsn,
                    deployment=options.deployment,
                    published=published,
                )
                layer_timings.append(commit_timing)
                # A non-published status is a real publish failure only on the
                # clean path; on the partial-failure path the run is already
                # failed and a no-op/blocked publish there must not double-record.
                if not any_failed and published.get("status") != "published":
                    any_failed = True
                    _append_skipped(
                        report,
                        reason="publish_failed",
                        after_layer=len(layers),
                        remaining_layers=[],
                    )

    report["timing_status"] = "available"
    report["layer_timings"] = layer_timings
    report["timings_ms"] = _ingest_timings_ms(
        run_started=run_started,
        layer_timings=layer_timings,
    )
    # Deterministic count of DB round-trips for this ingest run (the whole
    # per-commit workflow). Read from the active profiling context's always-on
    # counter, which observes every psycopg execute. Pure instrumentation: it
    # surfaces a count the substrate already records, changing no engine
    # behavior. count-based-perf-gate / req.counts.emit.
    report["query_count"] = getattr(profile_ctx, "query_count", None)
    _compact_large_graph_write_sets(report)
    return IngestRunResult(report=report, any_failed=any_failed)


def _admission_rejection_error(reason: str, source_health: Any) -> str:
    """The `error` text of a source the admission gate rejected (#3927).

    A run rejected as `source_health_not_healthy` carries the connector's own
    account of why (`ConnectorHealth.reason`, redacted by `as_dict`). The text
    states it, so a reader does not have to open the JSON report to learn it.
    Other rejection reasons keep the bare code.
    """
    message = f"source admission rejected: {reason}"
    if reason != "source_health_not_healthy" or source_health is None:
        return message
    health = source_health.as_dict()
    said = health.get("reason")
    if said:
        return f"{message} (the connector reported {health['status']}: {said})"
    return (
        f"{message} (the connector reported {health['status']} "
        "and gave no reason)"
    )


def _elapsed_ms(started: float) -> float:
    return round(max(0.0, (time.perf_counter() - started) * 1000), 3)


def _source_reported_total_ms(
    layer_results: list[dict[str, Any]],
) -> float:
    total = 0.0
    for result in layer_results:
        timings = result.get("timings_ms")
        if isinstance(timings, Mapping):
            total += float(timings.get("total") or 0.0)
    return round(total, 3)


def _publish_reported_ms(publish: Mapping[str, Any]) -> float:
    return round(float(publish.get("total_duration_ms") or 0.0), 3)


def _ingest_timings_ms(
    *,
    run_started: float,
    layer_timings: list[dict[str, Any]],
) -> dict[str, float]:
    source_wall = sum(float(row.get("source_wall_ms") or 0.0) for row in layer_timings)
    source_reported = sum(
        float(row.get("source_reported_total_ms") or 0.0) for row in layer_timings
    )
    publish_wall = sum(
        float(row.get("publish_wall_ms") or 0.0) for row in layer_timings
    )
    publish_reported = sum(
        float(row.get("publish_reported_ms") or 0.0) for row in layer_timings
    )
    total = _elapsed_ms(run_started)
    accounted_wall = source_wall + publish_wall
    return {
        "total": total,
        "source_wall": round(source_wall, 3),
        "source_reported_total": round(source_reported, 3),
        "publish_wall": round(publish_wall, 3),
        "publish_reported_total": round(publish_reported, 3),
        "orchestration_overhead": round(max(0.0, total - accounted_wall), 3),
    }


def _should_skip_layer_publish(
    options: IngestOptions,
    layer_results: list[dict[str, Any]],
) -> bool:
    """Return True only for a proven no-work ingest layer.

    Source-level zeroes are not enough. A prior failed publish or a newly
    registered enricher can leave publisher work pending even when the
    just-run layer appended no facts.
    """
    if options.force_enrichers:
        return False
    if not layer_results:
        return False
    if _layer_results_have_source_work(layer_results):
        return False
    if os.environ.get("OKG_SOURCE_SYNC_SKIP_NO_WORK_PUBLISH") in {
        "0",
        "false",
        "FALSE",
        "no",
        "NO",
    }:
        return False
    try:
        return not _deployment_has_pending_publisher_work(options)
    except Exception:  # noqa: BLE001 — fail closed: if no-work proof is unavailable, publish
        return False


def _layer_results_have_source_work(
    layer_results: list[dict[str, Any]],
) -> bool:
    for result in layer_results:
        for key in (
            "nodes_appended",
            "edges_appended",
            "retract_n",
            "retract_e",
        ):
            if int(result.get(key) or 0) != 0:
                return True
    return False


def _deployment_has_pending_publisher_work(
    options: IngestOptions,
) -> bool:
    """Detect whether skipping an ingest-layer publish is unsafe."""
    from okg.substrate.publisher.deployment_stack import (
        build_deployment_publisher_stack,
    )

    stack = build_deployment_publisher_stack(
        dsn=options.dsn,
        deployment_name=options.deployment,
        deployment_dir=options.deployment_dir,
    )
    if getattr(stack, "checkpoint_attestors", ()):
        return True
    # Use the process-shared pool instead of `psycopg.connect`. Saves
    # ~10 ms TCP+TLS+auth handshake per `_should_skip_layer_publish`
    # call. The pool is the same one source-sync uses; this function
    # is gated by source-side has-no-source-work, so concurrent
    # ingest-side pool pressure is bounded.
    from okg.substrate.db.pool import GENERAL, pooled_connection

    with pooled_connection(options.dsn, purpose=GENERAL) as conn:
        if _has_pending_chronos_derived_work(
            conn,
            deployment=options.deployment,
            branch_name="default",
        ):
            return True
        current_catalog_version_id = _current_catalog_version_id(conn)
        if not _chronos_publish_inputs_clean(
            conn,
            options=options,
            current_catalog_version_id=current_catalog_version_id,
        ):
            return True
        with conn.cursor() as cur:
            if _stack_enrichers_have_pending_work(
                cur,
                deployment=options.deployment,
                enricher_names=[
                    str(getattr(e, "name", e.__class__.__name__))
                    for e in stack.enrichers
                ],
            ):
                return True

            # Combined query: prior published watermarks + current
            # node_facts max + current edge_facts max in one round trip.
            # The previous shape issued 3 separate queries (15-25 ms
            # cumulative on the flamegraph).
            cur.execute(
                """
                WITH prior_pub AS (
                  SELECT fact_watermark, edge_fact_watermark
                    FROM okg.graph_generations
                   WHERE deployment_name = %s
                     AND status = 'published'
                   ORDER BY generation_id DESC
                   LIMIT 1
                )
                SELECT
                  (SELECT fact_watermark      FROM prior_pub),
                  (SELECT edge_fact_watermark FROM prior_pub),
                  COALESCE((SELECT max(fact_id) FROM okg.node_facts), 0),
                  COALESCE((SELECT max(fact_id) FROM okg.edge_facts), 0)
                """,
                (options.deployment,),
            )
            row = cur.fetchone()
            if row is not None and row[0] is not None:
                fact_watermark = int(row[0] or 0)
                edge_fact_watermark = int(row[1] or 0)
                if int(row[2] or 0) > fact_watermark:
                    return True
                if int(row[3] or 0) > edge_fact_watermark:
                    return True

            return False


def _chronos_publish_inputs_clean(
    conn: psycopg.Connection,
    *,
    options: IngestOptions,
    current_catalog_version_id: int | None,
) -> bool:
    if current_catalog_version_id is None:
        return False
    try:
        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        previous = store.latest_generation(
            deployment_name=options.deployment,
            branch="default",
            status="published",
        )
        if previous is None:
            return False
        previous_catalog_version_id = _checkpoint_catalog_version_id(
            previous,
        )
        if previous_catalog_version_id != int(current_catalog_version_id):
            return False
        return _chronos_checkpoint_visible_equal(
            conn,
            branch_name="default",
            checkpoint_id=str(previous.checkpoint_id),
        )
    except Exception:  # noqa: BLE001 - publish-skip proof must fail closed
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
        return False


def _checkpoint_catalog_version_id(checkpoint: Any) -> int | None:
    metadata = getattr(checkpoint, "metadata", None) or {}
    if not isinstance(metadata, Mapping):
        return None
    raw = metadata.get("catalog_version_id")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _chronos_checkpoint_visible_equal(
    conn: psycopg.Connection,
    *,
    branch_name: str,
    checkpoint_id: str,
) -> bool:
    """Return True when branch and checkpoint expose identical graph rows."""
    from psycopg import sql

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT current_segment.branch_point,
                   checkpoint_segment.branch_point
              FROM _chronos_branch_interval_branches branch
              JOIN _chronos_branch_interval_segments current_segment
                ON current_segment.segment_id = branch.current_segment_id
              JOIN _chronos_branch_interval_checkpoints checkpoint
                ON checkpoint.checkpoint_id = %s
              JOIN _chronos_branch_interval_segments checkpoint_segment
                ON checkpoint_segment.segment_id = checkpoint.segment_id
             WHERE branch.branch_id = %s
               AND checkpoint.branch_id = %s
            """,
            (checkpoint_id, branch_name, branch_name),
        )
        row = cur.fetchone()
        if row is None:
            return False
        current_point = int(row[0])
        checkpoint_point = int(row[1])
        if current_point == checkpoint_point:
            return True
        cur.execute(
            """
            SELECT physical_table
              FROM _chronos_branch_tables
             WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')
               AND backend = 'interval'
            """
        )
        physical_tables = [str(table) for (table,) in cur.fetchall()]
        if len(physical_tables) < 2:
            return False
        for physical_table in physical_tables:
            query = sql.SQL(
                """
                SELECT EXISTS (
                    SELECT 1
                      FROM public.{physical_table}
                     WHERE (
                           live_lo <= %s
                       AND %s < live_hi
                       AND deleted = FALSE
                     ) IS DISTINCT FROM (
                           live_lo <= %s
                       AND %s < live_hi
                       AND deleted = FALSE
                     )
                     LIMIT 1
                )
                """
            ).format(physical_table=sql.Identifier(physical_table))
            cur.execute(
                query,
                (
                    current_point,
                    current_point,
                    checkpoint_point,
                    checkpoint_point,
                ),
            )
            if bool(cur.fetchone()[0]):
                return False
    return True


def _has_pending_chronos_derived_work(
    conn: psycopg.Connection,
    *,
    deployment: str,
    branch_name: str,
) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.admission_envelopes')")
            if cur.fetchone()[0] is not None:
                # close-refused-publish-redrive-gap (#828): source envelopes
                # are counted too. The refs arm below mirrors the feedback
                # sweep's candidate set (_mark_generation_published:
                # pending/accepted, attached refs, non-review/quarantine
                # pendings), so probe-true implies one real published cycle
                # clears at least one counted envelope -- an accepted source
                # admission whose publish was refused stays visible until a
                # real checkpoint sweeps it, instead of being skipped
                # forever. Routine runs never self-count: accepted
                # zero-output envelopes carry no refs, and work-bearing runs
                # bypass the skip via layer results.
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                          FROM okg.admission_envelopes e
                         WHERE e.deployment_name = %s
                           AND e.status IN ('pending', 'accepted')
                           AND COALESCE(e.target_branch_name, 'default') = %s
                           AND (
                             e.producer_kind = 'lens'
                             OR (
                               (
                                 e.status = 'accepted'
                                 OR e.admission_mode IN (
                                   'direct',
                                   'fast_track'
                                 )
                               )
                               AND EXISTS (
                                 SELECT 1
                                   FROM okg.admission_record_refs r
                                  WHERE r.admission_id = e.admission_id
                                    AND r.ref_kind IN (
                                      'node_fact_range',
                                      'edge_fact_range',
                                      'payload'
                                    )
                                    AND (
                                      r.ref_kind != 'payload'
                                      OR r.payload->>'kind' =
                                         'chronos_graph_run'
                                    )
                               )
                             )
                           )
                         LIMIT 1
                    )
                    """,
                    (deployment, branch_name),
                )
                if bool(cur.fetchone()[0]):
                    return True
            cur.execute("SELECT to_regclass('okg.derived_dirty_scopes')")
            dirty_scopes_exists = cur.fetchone()[0] is not None
            cur.execute("SELECT to_regclass('okg.derived_artifact_definitions')")
            definitions_exists = cur.fetchone()[0] is not None
            if dirty_scopes_exists and definitions_exists:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                          FROM okg.derived_dirty_scopes s
                          JOIN okg.derived_artifact_definitions d
                            ON d.definition_id = s.definition_id
                         WHERE d.deployment_name = %s
                           AND s.consumed_run_id IS NULL
                         LIMIT 1
                    )
                    """,
                    (deployment,),
                )
                if bool(cur.fetchone()[0]):
                    return True
    except Exception:  # noqa: BLE001 - publish-skip proof must fail closed
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
        return True
    return False


def _staged_sweepable_source_admissions(
    conn: psycopg.Connection,
    *,
    deployment: str,
    branch: str,
    source: str | None = None,
    exclude_admission_id: str | None = None,
    limit: int = 5,
) -> list[str]:
    """Admission ids of source runs that appended work never published.

    Mirrors the feedback sweep's candidate set (_mark_generation_published):
    producer_kind='source', status pending/accepted (review/quarantine
    pendings that only `okg admission decide` can clear are excluded via
    the accepted-or-direct/fast_track qualification), attached fact refs,
    target branch. A non-empty result means the rows a zero-output re-sync
    deduped against never reached a published generation -- the #828
    re-drive gap. Scoped to one source for the admission verdict; the
    deployment-wide (source=None) form drives the commit-gate publish.
    Raises on query failure: callers decide the fail-closed posture.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.admission_envelopes')")
        if cur.fetchone()[0] is None:
            # No admission schema means no staged admissions can exist --
            # a factual empty, not a fallback.
            return []
        cur.execute(
            """
            SELECT e.admission_id::text
              FROM okg.admission_envelopes e
             WHERE e.deployment_name = %s
               AND e.producer_kind = 'source'
               AND e.status IN ('pending', 'accepted')
               AND COALESCE(e.target_branch_name, 'default') = %s
               AND (
                 e.status = 'accepted'
                 OR e.admission_mode IN ('direct', 'fast_track')
               )
               AND (%s::text IS NULL OR e.source = %s)
               AND (%s::text IS NULL OR e.admission_id::text <> %s)
               AND EXISTS (
                 SELECT 1
                   FROM okg.admission_record_refs r
                  WHERE r.admission_id = e.admission_id
                    AND r.ref_kind IN (
                      'node_fact_range', 'edge_fact_range', 'payload'
                    )
                    AND (
                      r.ref_kind != 'payload'
                      OR r.payload->>'kind' = 'chronos_graph_run'
                    )
               )
             ORDER BY e.created_at
             LIMIT %s
            """,
            (
                deployment,
                branch,
                source,
                source,
                exclude_admission_id,
                exclude_admission_id,
                limit,
            ),
        )
        return [str(row[0]) for row in cur.fetchall()]


def _staged_source_drive_needed(options: IngestOptions) -> bool:
    """Deployment-wide: does staged sweepable source work require a publish?

    Consulted by the commit gate when a run accumulated no publishable
    layer results (all layers failed or produced nothing) -- a zero-output
    rejection must not suppress the publish that heals it (#828 re-drive
    gap). Fail-closed: an errored probe drives the publish rather than
    silently skipping it.
    """
    try:
        return _staged_source_admissions_present(options)
    except Exception:  # noqa: BLE001 - drive-needed proof must fail closed
        return True


def _staged_source_admissions_present(options: IngestOptions) -> bool:
    """Return an authoritative staged-work observation or raise.

    Pre-source repair may run only after a successful database observation.
    The end-of-run commit gate wraps this helper with its stronger fail-closed
    policy so probe failure still cannot silently strand publisher work.
    """
    return bool(_staged_source_redrive_state(options)["staged_work_present"])


def _staged_source_redrive_state(options: IngestOptions) -> dict[str, Any]:
    """Atomically observe staged work and its checkpoint predecessor."""
    return incomplete_recovery.staged_source_redrive_state(
        dsn=options.dsn, deployment=options.deployment,
    )


def _recovery_candidate_admission_ids(
    options: IngestOptions,
    *,
    predecessor_checkpoint_id: str,
) -> list[str]:
    return incomplete_recovery.recovery_admission_ids(
        dsn=options.dsn,
        deployment=options.deployment,
        predecessor_checkpoint_id=predecessor_checkpoint_id,
    )


def _promote_frozen_recovery(
    options: IngestOptions,
    *,
    redrive_state: Mapping[str, Any],
    recovery: Mapping[str, Any],
    ingest_completeness: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Promote the exact frozen checkpoint without running another source."""
    return _publish(
        replace(options, force_enrichers=True),
        "incomplete-recovery-promotion",
        ingest_completeness=ingest_completeness,
        **incomplete_recovery.promotion_publish_kwargs(
            redrive_state=redrive_state,
            recovery=recovery,
            deployment=options.deployment,
        ),
    )


def _stack_enrichers_have_pending_work(
    cur: psycopg.Cursor,
    *,
    deployment: str,
    enricher_names: list[str],
) -> bool:
    if not enricher_names:
        return False
    cur.execute(
        """
        SELECT enricher_name, last_status
          FROM okg.enricher_progress
         WHERE deployment_name = %s
           AND enricher_name = ANY(%s)
        """,
        (deployment, enricher_names),
    )
    progress = {str(name): str(status or "") for name, status in cur.fetchall()}
    for name in enricher_names:
        if progress.get(name) != "success":
            return True
    return False


def _validate_deferred_source_activation(
    plan: IngestPlan,
    *,
    deployment_dir: Path,
    registry_path: Path,
) -> None:
    """Acquire dormant-source probes, then delegate pure validation."""
    from okg.substrate.deployment_lint import (
        source_activation_probe_findings,
    )

    probe_results: dict[str, ActivationProbeResult] = {}
    for name, entry in plan.runnable.items():
        if entry.raw.get("enabled_by_default") is not False:
            continue
        findings = source_activation_probe_findings(
            name,
            entry.raw,
            deployment_dir=deployment_dir,
            registry_path=registry_path,
        )
        probe_results[name] = ActivationProbeResult.from_findings(findings)
    validate_deferred_source_activation(
        plan,
        probe_results,
    )


def _dependency_blocked_sources(
    layer: Collection[str],
    deps: Mapping[str, set[str]],
    unavailable_sources: Collection[str],
) -> dict[str, list[str]]:
    """Map only sources with declared unavailable predecessors to blockers."""
    unavailable = set(unavailable_sources)
    return {
        name: sorted(set(deps.get(name, set())) & unavailable)
        for name in layer
        if set(deps.get(name, set())) & unavailable
    }


def _upsert_source_record_set_coverage_state(
    *,
    options: IngestOptions,
    entry: SourceEntry,
    result: Mapping[str, Any],
) -> None:
    """Record whether this run failed its record-set coverage check.

    Written for every outcome, not only failures: a source that recovers
    clears its own flag on the next successful run, so the watcher's
    exclusion cannot outlive the problem it was protecting against.
    """
    if not options.deployment or not entry.ownership_id:
        return
    payload = record_set_coverage_state.coverage_state_payload(
        deployment=options.deployment,
        source_name=entry.ownership_id,
        result=result,
    )
    if payload is None:
        return
    try:
        upsert_source_progress(
            dsn=options.dsn,
            deployment=options.deployment,
            source_name=entry.ownership_id,
            progress={
                record_set_coverage_state.COVERAGE_STATE_KEY: json.dumps(
                    payload, sort_keys=True, default=str,
                ),
            },
        )
    except Exception:  # noqa: BLE001 - diagnostics must not fail ingest.
        log.exception(
            "failed to upsert record-set coverage state for source=%s",
            entry.ownership_id,
        )


def _annotate_source_stage_attrs(
    attrs: dict[str, Any],
    result: Mapping[str, Any],
) -> None:
    timings = result.get("timings_ms")
    timing_available = isinstance(timings, dict) and bool(timings)
    nodes_appended = _int_result_count(result, "nodes_appended")
    edges_appended = _int_result_count(result, "edges_appended")
    retract_n = _int_result_count(result, "retract_n")
    retract_e = _int_result_count(result, "retract_e")
    unavailable_reason = str(result.get("error")) if result.get("error") else None
    attrs.update(
        {
            "status": result.get("status"),
            "run_mode": result.get("run_mode"),
            "timing_status": result.get("timing_status"),
            "timing_unavailable_reason": (
                None
                if timing_available
                else unavailable_reason or "source_result_missing_timings"
            ),
            "unavailable_reason": unavailable_reason,
            "timings_ms": timings if isinstance(timings, dict) else {},
            "nodes_appended": nodes_appended,
            "edges_appended": edges_appended,
            "retract_n": retract_n,
            "retract_e": retract_e,
            "output_count": (nodes_appended + edges_appended + retract_n + retract_e),
            "source_stats": (
                result.get("source_stats")
                if isinstance(result.get("source_stats"), dict)
                else {}
            ),
        }
    )
    if result.get("admission_ref_count") is not None:
        attrs["admission_ref_count"] = result.get("admission_ref_count")


def _source_param_uses_deployment_dsn_placeholder(value: Any) -> bool:
    return source_param_uses_deployment_dsn_placeholder(value)


def _preimport_layer_source_modules(plan: Any, layer: "Iterable[str]") -> None:
    """Sequentially pre-import the layer's source modules in the main thread.

    Delegates to ``deployments.preimport_source_modules`` (which holds the
    process-wide deployment-module lock) so the source fan-out only ever hits
    the module cache — the evict/inject/first-import race is closed before any
    worker thread runs. A module that fails to import is NOT raised here:
    remaining modules are still seeded independently, and the per-source
    runner re-imports the failed module under the same lock so it can report
    the failure with full source context.
    """
    from okg.substrate.deployments import preimport_source_modules

    module_names: list[str] = []
    for name in layer:
        entry = plan.runnable.get(name)
        if entry is None:
            continue
        module_name = entry.raw.get("module")
        if module_name:
            module_names.append(str(module_name))
    for module_name in dict.fromkeys(module_names):
        try:
            preimport_source_modules([module_name])
        except Exception:  # noqa: BLE001 — source runner reports with context
            continue


def _import_source_module(module_name: Any) -> Any:
    """Backward-compatible import seam used by concurrency tests."""
    from okg.substrate.deployments import deployment_module_import_lock

    with deployment_module_import_lock():
        return importlib.import_module(module_name)


def _run_source(
    options: IngestOptions,
    entry: SourceEntry,
    name: str,
    *,
    prepared: source_job_runtime.PreparedSourceRun | None = None,
    job_hooks: Any | None = None,
) -> dict[str, Any]:
    """Run one source and admit its result.

    Production runs this inside a source child process
    (:mod:`okg.substrate.ingest.source_jobs`) with ``prepared`` created by the
    parent and ``job_hooks`` connecting it to the coordinator: the hooks take
    the per-source run lock before the first write and report the pre-run
    state snapshot the parent needs to compensate a crash. Called without
    them (tests and direct callers) it prepares its own admission and runs
    in the calling process.
    """
    started = time.time()
    timings_started = time.perf_counter()
    timings_last = timings_started
    timings_ms: dict[str, float] = {}

    def mark_timing(label: str) -> None:
        nonlocal timings_last
        now = time.perf_counter()
        timings_ms[label] = round((now - timings_last) * 1000, 3)
        timings_last = now

    def timing_payload(
        runner_timings: Mapping[str, Any] | None = None,
    ) -> dict[str, float]:
        payload = {
            **timings_ms,
            "total": round((time.perf_counter() - timings_started) * 1000, 3),
        }
        if runner_timings:
            for key, value in runner_timings.items():
                if not isinstance(value, (int, float)):
                    continue
                payload[f"source_runner.{key}"] = round(float(value), 3)
        return payload

    run_context: SourceRunContext | None = None
    construct_diagnosis: dict[str, Any] | None = None
    stale_module_recovery: dict[str, Any] | None = None
    workflow_ref: dict[str, Any] | None = None
    envelope_status: str | None = None
    admission_final_status: str | None = None
    source_state_snapshot: _SourceStateSnapshot | None = None
    source_runner: SourceRunner | None = None
    managed_change_identity = False
    refs: list[Any] = []
    # The admission boundary runs before adapter import/construction. Keep the
    # diagnostic inputs initialized for that earlier failure path so reporting
    # the original admission error cannot replace it with UnboundLocalError.
    module_name = str(entry.raw.get("module") or "")
    class_name = str(entry.raw.get("class") or "")
    params: dict[str, Any] = {}
    admission_contract = getattr(entry, "admission_contract", None)
    stage = "admission"
    try:
        if prepared is None:
            prepared = source_job_runtime.prepare_source_run(options, entry, name)
        run_mode = prepared.run_mode
        sync_scope = prepared.sync_scope
        run_context = prepared.run_context
        workflow_ref = prepared.workflow_ref
        envelope_status = prepared.envelope_status
        timings_ms.update(prepared.timings_ms)
        timings_last = time.perf_counter()
        strict_binding: ConstructedSourceAdapter | None = None
        if admission_contract == STRICT_ADMISSION_CONTRACT:
            if job_hooks is None:
                strict_binding = options.constructed_source_bindings[name]
            else:
                stage = "construct"
                strict_binding = source_job_runtime.rebuild_strict_binding(
                    options,
                    entry,
                    name,
                    prepared.strict_fingerprint,
                )
                mark_timing("construct")
            adapter = strict_binding.adapter
            graph_branch = strict_binding.graph_branch
            registry_binding_fingerprint = strict_binding.registry_binding_fingerprint

        if admission_contract != STRICT_ADMISSION_CONTRACT:
            stage = "import"
            cls = import_source_adapter_class(entry)
            mark_timing("import")
            graph_branch = source_adapter_graph_branch(entry)
            params = source_adapter_init_params(
                entry,
                dsn=options.dsn,
                deployment=options.deployment,
                graph_branch=graph_branch,
                adapter_class=cls,
            )
            stage = "construct"
            try:
                adapter = cls(**params)
            except TypeError as construct_error:
                # Issue #945: a long-lived worker holds the module objects it
                # imported. An adapter that rejects a registry-declared
                # argument is usually not a bug — it is a definition the
                # process loaded before the argument existed. Prove which,
                # and if the loaded definition really is out of date,
                # re-import it and try once more rather than failing this
                # cycle (and the next 176) on code that no longer exists.
                construct_diagnosis = classify_source_construction_failure(
                    exc=construct_error,
                    stage=stage,
                    module_name=module_name,
                    class_name=class_name,
                    params=params,
                )
                if construct_diagnosis is None:
                    raise
                if construct_diagnosis["cause"] != "stale_loaded_code":
                    explain_legacy_construct_failure(
                        construct_diagnosis,
                        source=name,
                        params=entry.raw.get("params") or {},
                        adapter_class=cls,
                    )
                    raise
                recovery = retry_construction_after_eviction(
                    module_name=module_name,
                    class_name=class_name,
                    params=params,
                    # Substrate-supplied arguments are injected from the
                    # TARGET class's signature, so they must be recomputed
                    # against the re-imported class: the whole premise is
                    # that the fresh definition declares parameters the
                    # stale one did not.
                    prepare_params=lambda fresh_cls, _current: (
                        source_adapter_init_params(
                            entry,
                            dsn=options.dsn,
                            deployment=options.deployment,
                            graph_branch=graph_branch,
                            adapter_class=fresh_cls,
                        )
                    ),
                )
                construct_diagnosis["recovery"] = {
                    key: value for key, value in recovery.items() if key != "adapter"
                }
                if not recovery.get("recovered"):
                    raise
                adapter = recovery["adapter"]
                stale_module_recovery = construct_diagnosis
                log.warning(
                    "source %s: re-imported stale module %s after %s; "
                    "construction succeeded on retry. This process is "
                    "serving code older than the checkout — restart the "
                    "runtime worker.",
                    name,
                    module_name,
                    construct_diagnosis["error_kind"],
                )
            registry_binding_fingerprint = bind_source_adapter_identity(adapter, entry)
            mark_timing("construct")
        managed_change_identity = getattr(
            adapter, "change_probe", None
        ) is not None and SourceRunner._adapter_probe_short_circuit_safe(adapter)
        if job_hooks is not None:
            # A child that outlived a crashed parent must never write beside
            # the recovered run of the same source.
            stage = "source_run_lock"
            job_hooks.acquire_run_lock(entry.ownership_id or name)
            mark_timing("source_run_lock")
        stage = "source_state_snapshot"
        source_state_snapshot = _snapshot_source_state(
            options.dsn,
            deployment=options.deployment,
            source=entry.ownership_id,
        )
        mark_timing("source_state_snapshot")
        if job_hooks is not None:
            job_hooks.started(
                source_state_snapshot=source_state_snapshot,
                graph_branch=graph_branch,
                managed_change_identity=managed_change_identity,
            )
        stage = "run"
        # `bind-at-rest-anonymization`: resolve this source's at-rest privacy
        # posture from its source_policy. An explicitly store_raw source gets a
        # cheap no-op posture; a not-store-raw source loads the vault LOUD (a
        # missing key fails the run, never a silent raw store) and its facts are
        # pseudonymized / masked at admission.
        manifest = (
            fastyaml.safe_load(
                (Path(options.deployment_dir) / "deployment.yaml").read_text(
                    encoding="utf-8"
                )
            )
            or {}
        )
        pii_posture = resolve_deployment_source_pii_posture(
            manifest=manifest,
            source_name=name,
            source_entry=entry.raw,
            enforce_dsn_keys=runtime_enforce_mode_dsn_keys(
                Path(options.deployment_dir)
            ),
        )
        images_config = images_config_for_deployment(
            Path(options.deployment_dir), manifest,
        )
        bootstrap_cursor_semantics = getattr(
            adapter,
            "bootstrap_cursor_semantics",
            None,
        )
        runner_executable_profile = (
            strict_binding.executable_profile
            if strict_binding is not None
            else (
                entry.source_class
                if isinstance(bootstrap_cursor_semantics, str)
                and bootstrap_cursor_semantics
                else None
            )
        )
        runner_registry_name = (
            strict_binding.registry_name
            if strict_binding is not None
            else (name if runner_executable_profile is not None else None)
        )
        runner_ownership_id = (
            strict_binding.ownership_id
            if strict_binding is not None
            else (entry.ownership_id if runner_executable_profile is not None else None)
        )
        source_runner = SourceRunner(
            options.dsn,
            source_name=entry.ownership_id,
            deployment_name=options.deployment,
            graph_branch=graph_branch,
            pii_posture=pii_posture,
            deletion_semantics=getattr(entry, "deletion_semantics", None),
            record_authority_format=resolve_record_authority_format(entry),
            executable_profile=runner_executable_profile,
            source_revision_kind=getattr(entry, "source_revision_kind", None),
            registry_name=runner_registry_name,
            ownership_id=runner_ownership_id,
            registry_binding_fingerprint=registry_binding_fingerprint,
            # The watermark is admitted below, after ``run()`` returns. Keep
            # the bootstrap lease through that CAS so no concurrent consumer
            # can apply facts against the same authority generation.
            defer_bootstrap_lock_release=True,
            # This function already compensates a failed run (see the `_retract_chronos_run` +
            # `_restore_source_state` pair below) and restores watermarks the runner cannot see, so
            # the runner must not compensate the same run a second time (issue #1431).
            compensate_on_failure=False,
            images_config=images_config,
            full_retraction_override=_retraction_guard.full_retraction_override_for(options, name, entry.ownership_id),
        )
        result = source_runner.run(
            adapter,
            mode=run_mode,
            sync_scope=sync_scope,
            run_context=run_context,
            reset_cursor=options.reset_cursor,
            # `deltaflow-source-contracts` (2026-06-13): readiness-critical
            # sources emit validated SourceDeltaBatch evidence so publish/
            # projection and readiness gates can reason about delta-
            # proportional work. Other sources stay on the existing path
            # until their per-adapter migration lands.
            emit_delta_contract=entry.ownership_id in READY_SOURCES,
        )
        mark_timing("source_runner")
        runner_timings = getattr(result, "timings_ms", {}) or {}
        stage = "admission_finalize"
        if bool(getattr(result, "idempotent_replay", False)):
            if envelope_status not in {"accepted", "published", "blocked"}:
                raise SubstrateError(
                    "durable bootstrap transition has no terminal admission",
                    data={
                        "admission_id": run_context.admission_id,
                        "envelope_status": envelope_status,
                    },
                )
            finalize_result = _AdmissionFinalizeResult(
                refs=[],
                status=envelope_status,
                verdict={
                    "source": entry.ownership_id,
                    "run_id": run_context.run_id,
                    "idempotent_replay": True,
                    "terminal_state_authorized": True,
                },
            )
        else:
            finalize_result = _coerce_admission_finalize_result(
                _finalize_source_admission(
                    options=options,
                    admission_id=run_context.admission_id,
                    source=entry.ownership_id,
                    run_id=run_context.run_id,
                    workflow_ref=workflow_ref,
                    existing_status=envelope_status,
                    admission_mode=(
                        entry.admission_policy.admission_mode
                        if entry.admission_policy is not None
                        else "fast_track"
                    ),
                    observed_output_count=(
                        result.nodes_appended
                        + result.edges_appended
                        + result.retractions_node
                        + result.retractions_edge
                    ),
                    completed_scope=bool(getattr(result, "completed_scope", False)),
                    source_health_status=(
                        result.source_health.status
                        if getattr(result, "source_health", None) is not None
                        else "ok"
                    ),
                    # A runner-owned probe short-circuit reports ``no_change``.
                    # Normal results retain the requested mode separately while
                    # admission consumes the adapter's authorized structural
                    # outcome (for example an append feed's completed-scope
                    # replay).
                    run_mode=_admission_run_mode(result, fallback=run_mode),
                    next_cursor=getattr(result, "next_cursor", None),
                    observed_at=run_context.observed_at,
                    source_progress=getattr(result, "terminal_progress", None),
                    clear_replay_required=bool(
                        getattr(result, "clear_replay_required", False)
                    ),
                    replay_target=getattr(result, "replay_target", None),
                    expected_last_applied_rev=getattr(
                        result,
                        "expected_last_applied_rev",
                        None,
                    ),
                    watermark_compare_and_swap=bool(
                        getattr(
                            result,
                            "watermark_compare_and_swap",
                            False,
                        )
                    ),
                    expected_bootstrap_ledger=getattr(
                        result,
                        "expected_bootstrap_ledger",
                        None,
                    ),
                    next_bootstrap_ledger=getattr(
                        result,
                        "next_bootstrap_ledger",
                        None,
                    ),
                    # Direct attribute access (not a defaulted getattr) on purpose:
                    # this feeds the silent-data-loss guard, so a future RunResult
                    # field rename must fail loudly here rather than silently default
                    # to 0 and re-open #826. Matches observed_output_count above.
                    rejected_fact_count=result.rejected_fact_count,
                    private_authority_candidate=(
                        private_authority_candidate_from_result(result)
                    ),
                ),
            )
        admission_final_status = finalize_result.status
        # #2993: the admission is final; an accepted run lifts its rerun block.
        _run_base.finish_run_admission(
            options.dsn, branch=graph_branch, run_id=run_context.run_id,
            deployment=options.deployment, source=entry.ownership_id,
            admission_status=admission_final_status,
        )
        refs = finalize_result.refs
        mark_timing("admission_finalize")
        policy = (finalize_result.verdict or {}).get("producer_policy") or {}
        # #2074 (a): an accepted no-op keeps prior state too, run markers included.
        if source_state_snapshot is not None and (
            finalize_result.rejected or policy.get("keeps_source_state")
        ):
            stage = "source_state_restore"
            replay_run_id = (getattr(result, "replay_target", None) or {}).get("run_id")
            _restore_source_state(
                options.dsn,
                source_state_snapshot,
                expected_replay_run_id=str(replay_run_id) if replay_run_id else None,
            )
            mark_timing("source_state_restore")
        if finalize_result.rejected:
            reason = finalize_result.reason or "source_admission_rejected"
            return {
                "name": name,
                "status": "FAIL",
                "stage": "admission_finalize",
                "error": _admission_rejection_error(
                    reason, getattr(result, "source_health", None),
                ),
                "nodes_appended": result.nodes_appended,
                "edges_appended": result.edges_appended,
                "retract_n": result.retractions_node,
                "retract_e": result.retractions_edge,
                "run_mode": run_mode,
                "elapsed_s": round(time.time() - started, 3),
                "timing_status": "available",
                "timings_ms": timing_payload(runner_timings),
                "sync_scope": sync_scope or {},
                "source_stats": getattr(adapter, "last_run_stats", {}) or {},
                "admission_id": run_context.admission_id,
                "admission_ref_count": len(refs),
                "admission_status": finalize_result.status,
                "admission_reject_reason": reason,
                "admission_verdict": finalize_result.verdict or {},
                "source_state_restored": source_state_snapshot is not None,
                # Finalize retracted the rejected run before it returned.
                "undo_status": published_completeness.UNDO_COMPLETED,
                # The health that rejected the run stays observable here as on
                # an OK record (redacted; credential NAMES only), so a harness
                # can tell an absent credential from missing tooling (#1794).
                **(
                    {"source_health": result.source_health.as_dict()}
                    if getattr(result, "source_health", None) is not None
                    else {}
                ),
                **_sanitize_report_fields(result),
                **image_report_fields(result),
            }
    except Exception as e:
        # The runner already flushed this sanitized record before unwinding into
        # compensation. Preserve it even if compensation replaces the exception.
        tail_failure_diagnostic = getattr(source_runner, "tail_failure_diagnostic", None)
        tail_observer_failures = getattr(source_runner, "_tail_observer_failures", 0)
        # Issue #945: an adapter that rejects a registry-declared
        # argument is almost never a run-time bug — it is either a
        # process serving stale code or a real registry/adapter
        # disagreement. Say which, measured, instead of recording an
        # opaque TypeError whose premise contradicts the file on disk.
        # Classified HERE, before the compensation below can replace `e`
        # with a compensation error, so the diagnosis describes the
        # original failure. Reused from the construct stage only for a
        # construct-stage failure: a recovered construct followed by a
        # run-stage error must not be relabelled as a signature mismatch.
        classification = (construct_diagnosis if stage == "construct" else None) or (
            classify_source_construction_failure(
                exc=e,
                stage=stage,
                module_name=module_name,
                class_name=class_name,
                params=params,
            )
        )
        # Capture the failure's own payload BEFORE compensation may rebind
        # `e`: a refusal that also fails restore must still name its samples.
        original_error_data = (
            dict(e.data) if isinstance(e, SubstrateError) and e.data else {}
        )
        recovery_blocked = False
        # Nothing is written before the run lock is held, so a failure at
        # that stage has nothing to retract -- and retracting this run id
        # could undo the writes of an orphan that shares it.
        before_first_write = stage == "source_run_lock"
        # Only a REFUSED lock leaves the admission alone: another run of this
        # source (possibly that orphan, with this same admission) still owns
        # it. Any other failure to take the lock (pool refusal, database
        # error) fails the admission as usual, so it is not left pending for
        # the next publish sweep.
        lock_refused = isinstance(e, SourceRunAlreadyActive)
        if run_context is not None and not before_first_write:
            try:
                # No `graph_conn` here on purpose: this handler runs after
                # `_finalize_source_admission`'s transaction has ended, so
                # the branch advisory lock that transaction held is already
                # released and compensation's own connection cannot wait on
                # it (#2123).
                _retract_chronos_run(
                    options.dsn,
                    admission_id=run_context.admission_id,
                    source=entry.ownership_id,
                    run_id=run_context.run_id,
                    branch=(graph_branch if "graph_branch" in locals() else "default"),
                )
            except Exception as compensation_error:  # noqa: BLE001
                recovery_blocked = True
                e = SubstrateError(
                    "source failure compensation is unresolved",
                    data={
                        "original_error": f"{type(e).__name__}: {e}",
                        "compensation_error": (
                            f"{type(compensation_error).__name__}: {compensation_error}"
                        ),
                    },
                )
        if source_state_snapshot is not None:
            try:
                _restore_source_state(
                    options.dsn,
                    source_state_snapshot,
                    expected_replay_run_id=(
                        run_context.run_id
                        if managed_change_identity and run_context is not None
                        else None
                    ),
                )
            except Exception as restore_error:  # noqa: BLE001
                recovery_blocked = True
                e = SubstrateError(
                    "source failure state restoration is unresolved",
                    data={
                        "original_error": f"{type(e).__name__}: {e}",
                        "restore_error": (
                            f"{type(restore_error).__name__}: {restore_error}"
                        ),
                    },
                )
        # An unresolved compensation is the more urgent fact, so it keeps
        # its own error text; the diagnosis still rides along on the
        # admission verdict.
        error_text = (
            str(classification["message"])
            if classification is not None and not recovery_blocked
            else f"{type(e).__name__}: {e}"
        )
        if (
            run_context is not None
            and workflow_ref is not None
            and envelope_status not in {"published", "blocked"}
            and admission_final_status != "rejected"
            and not recovery_blocked
            and not lock_refused
        ):
            _fail_source_admission(
                dsn=options.dsn,
                admission_id=run_context.admission_id,
                error=error_text,
                workflow_ref=workflow_ref,
                diagnosis=classification,
            )
        return {
            "name": name,
            "status": "FAIL",
            "stage": stage,
            "error": error_text,
            "undo_status": published_completeness.undo_status(
                wrote=run_context is not None and not before_first_write,
                recovery_blocked=recovery_blocked,
            ),
            # The source-job refusals carry their registered code so the
            # completeness receipt names the cause, not only the stage.
            **(
                {"code": e.code}
                if isinstance(e, (SourceRunAlreadyActive, SourceProcessBindingChanged))
                else {}
            ),
            "elapsed_s": round(time.time() - started, 3),
            "timing_status": "partial",
            "timings_ms": timing_payload(),
            **({"source_tail_failure": tail_failure_diagnostic}
               if isinstance(tail_failure_diagnostic, dict) else {}),
            **({"source_tail_observer_failures": tail_observer_failures}
               if isinstance(tail_observer_failures, int)
               and tail_observer_failures > 0 else {}),
            **(
                {
                    "error_kind": classification["error_kind"],
                    "error_diagnosis": classification,
                }
                if classification is not None
                else {}
            ),
            # A SubstrateError's structured payload used to die here: only
            # `error_text` survived, so a record-set coverage refusal reached
            # the operator as counts with no `missing_nodes_sample` naming
            # WHICH identities were absent. Diagnosing one then meant reading
            # the runner source. Carry the payload the way a failed publish
            # already does (`_publish_once`), so the samples ride out to
            # `_ingest_failure_details` and the workflow error.
            **(
                {
                    "error_data": {
                        **(
                            dict(e.data)
                            if isinstance(e, SubstrateError)
                            else {}
                        ),
                        # When compensation rebound `e`, its data holds only
                        # the two error strings; keep the original payload
                        # beside them under its own key.
                        **(
                            {"original_error_data": original_error_data}
                            if recovery_blocked and original_error_data
                            else {}
                        ),
                    }
                }
                if (isinstance(e, SubstrateError) and e.data)
                or (recovery_blocked and original_error_data)
                else {}
            ),
            # The repair instruction rides out with the failure, as it does
            # for a failed publish (#1791); a runner mode refusal names the
            # profile's accepted modes here (#2476).
            **(
                {"fix_hint": str(e.fix_hint)}
                if isinstance(e, SubstrateError) and e.fix_hint
                else {}
            ),
            **(
                {"admission_id": run_context.admission_id}
                if run_context is not None
                else {}
            ),
        }
    finally:
        if source_runner is not None:
            release_bootstrap_lock = getattr(
                source_runner,
                "release_deferred_bootstrap_lock",
                None,
            )
            if callable(release_bootstrap_lock):
                release_bootstrap_lock()
    output = {
        "name": name,
        "status": "OK",
        "run_id": getattr(result, "run_id", run_context.run_id),
        "nodes_appended": result.nodes_appended,
        "edges_appended": result.edges_appended,
        "retract_n": result.retractions_node,
        "retract_e": result.retractions_edge,
        "run_mode": run_mode,
        "elapsed_s": round(time.time() - started, 3),
        "timing_status": "available",
        "timings_ms": timing_payload(runner_timings),
        "sync_scope": sync_scope or {},
        "source_stats": getattr(adapter, "last_run_stats", {}) or {},
        "admission_id": run_context.admission_id,
        "admission_ref_count": len(refs),
        # Surface the per-source terminal admission verdict on the OK row (symmetric with the FAIL
        # row above) so downstream proof-of-life gates can consume the pipeline's own
        # caught-up-vs-wedged decision (producer_policy.terminal_state_authorized / reason /
        # completed_scope) instead of re-deriving liveness from the live-row delta alone. A source
        # that produced real appended output carries no zero-output verdict here ({}), so a
        # caught-up gate accepts only when EVERY requested source reached an authorized zero-output
        # terminal state. ``admission_status`` distinguishes an accepted terminal state from a
        # review-mode "pending" one that is authorized but NOT yet published, so the gate never
        # treats a queued-for-review source as caught up.
        "admission_status": finalize_result.status,
        "admission_verdict": finalize_result.verdict or {},
        # PACT sanitize-fact-text-at-admission: NUL scrub/reject counts, present only when nonzero.
        **_sanitize_report_fields(result),
        **dedupe_report_fields(result),
        # PACT first-class-images: images rendered, reused and in error, and
        # bytes stored; present only when the run saw a file image value.
        **image_report_fields(result),
        **_retraction_guard.full_retraction_report_fields(result),
    }
    if stale_module_recovery is not None:
        # The run succeeded, but only because the adapter's module had to be re-imported. Report it
        # under its own key: the process is still serving stale code for everything else it loaded.
        # Not `error_kind` — this result is an OK, and a consumer treating the presence of
        # `error_kind` as failure must not misread it.
        output["stale_module_recovery"] = stale_module_recovery
    if getattr(result, "trace_id", None):
        output["trace_id"] = result.trace_id
    if getattr(result, "telemetry_span_id", None):
        output["telemetry_span_id"] = result.telemetry_span_id
    # Surface source health (e.g. a degraded checkout acquisition:
    # acquire_stale / acquire_failed) so it is observable in the per-source
    # ingest run report rather than being dropped between the runner and here.
    if getattr(result, "source_health", None) is not None:
        output["source_health"] = result.source_health.as_dict()
    graph_write_set_fn = getattr(result, "graph_write_set_as_dict", None)
    graph_write_set = (
        graph_write_set_fn()
        if callable(graph_write_set_fn)
        else getattr(result, "graph_write_set", None)
    )
    if graph_write_set is not None:
        output["graph_write_set"] = graph_write_set
    source_delta_batch = getattr(result, "source_delta_batch", None)
    if source_delta_batch is not None:
        output["source_delta_batch"] = source_delta_batch
    fact_batch_summary = getattr(result, "fact_batch_summary", None)
    if fact_batch_summary is not None:
        output["fact_batch_summary"] = fact_batch_summary
    return output


def _trace_identity_from_layer_results(
    layer_results: list[dict[str, Any]],
) -> tuple[str, str | None] | None:
    for result in reversed(layer_results):
        trace_id = result.get("trace_id")
        if trace_id:
            span_id = result.get("telemetry_span_id")
            return str(trace_id), str(span_id) if span_id else None
    return None


def _upsert_source_stage_timing_progress(
    *,
    options: IngestOptions,
    entry: SourceEntry,
    source_registry_name: str,
    result: Mapping[str, Any],
    input_count: int,
) -> None:
    if not options.deployment or not entry.ownership_id:
        return
    timings = result.get("timings_ms")
    if not isinstance(timings, Mapping) or not timings:
        return
    run_id = str(result.get("run_id") or "")
    payload = {
        "schema": "okg.source_sync.stage_timings.last_run.v1",
        "deployment": options.deployment,
        "source_name": entry.ownership_id,
        "registry_source_name": source_registry_name,
        "run_id": run_id or None,
        "status": result.get("status"),
        "timing_status": result.get("timing_status"),
        "run_mode": result.get("run_mode"),
        "timings_ms": {str(k): v for k, v in timings.items()},
        "nodes_appended": _int_result_count(result, "nodes_appended"),
        "edges_appended": _int_result_count(result, "edges_appended"),
        "nodes_retracted": _int_result_count(result, "retract_n"),
        "edges_retracted": _int_result_count(result, "retract_e"),
        "input_count": int(input_count or 0),
        "sync_scope": (
            result.get("sync_scope")
            if isinstance(result.get("sync_scope"), Mapping)
            else {}
        ),
        "output_count": (
            _int_result_count(result, "nodes_appended")
            + _int_result_count(result, "edges_appended")
            + _int_result_count(result, "retract_n")
            + _int_result_count(result, "retract_e")
        ),
        "source_stats": (
            result.get("source_stats")
            if isinstance(result.get("source_stats"), Mapping)
            else {}
        ),
    }
    progress = {
        "sync.stage_timings.last_run": json.dumps(
            payload,
            sort_keys=True,
            default=str,
        )
    }
    if run_id:
        progress[f"sync.stage_timings.run.{run_id}"] = progress[
            "sync.stage_timings.last_run"
        ]
    try:
        upsert_source_progress(
            dsn=options.dsn,
            deployment=options.deployment,
            source_name=entry.ownership_id,
            progress=progress,
        )
    except Exception:  # noqa: BLE001 - diagnostics must not fail ingest.
        log.exception(
            "failed to upsert stage timing progress for source=%s",
            entry.ownership_id,
        )


# Pseudo-source under which publish-cycle timings are recorded. The publish
# cycle is not owned by any single source, so its diagnostics live under a
# reserved name (the source_progress UPSERT has no FK on source_name).
_PUBLISH_TIMINGS_PSEUDO_SOURCE = "__publisher__"


def persist_publish_stage_timings(
    *,
    dsn: str,
    deployment: str | None,
    published: Mapping[str, Any],
) -> None:
    """Persist source-sync publish decisions for replay attribution.

    OFF by default: only runs when ``OKG_PERSIST_PUBLISH_TIMINGS`` is set in
    the environment, so production deployments are unaffected and do not
    accumulate per-generation diagnostic rows. The bench worker sets it.
    Called from BOTH publish paths (inter-layer publish in ``run_ingest`` and
    the scheduled ``okg_publisher_cycle`` workflow) so the timings are
    captured regardless of which path produced the generation. Like the
    source-side timing upsert, failures here never fail the publish.
    """
    if not os.environ.get("OKG_PERSIST_PUBLISH_TIMINGS"):
        return
    if not deployment:
        return
    label = str(published.get("label") or "publish")
    status = str(published.get("status") or "")
    publish_skipped = bool(published.get("publish_skipped"))
    timings = published.get("timings_ms")
    generation_id = published.get("generation_id")

    if status == "published":
        if not generation_id:
            return
        key_suffix = str(generation_id)
    elif publish_skipped:
        key_suffix = f"{label}.skipped.{uuid.uuid4().hex}"
    else:
        return

    payload = {
        "schema": "okg.publish.stage_timings.run.v1",
        "deployment": deployment,
        "generation_id": str(generation_id) if generation_id else None,
        "status": status,
        "label": label,
        "publish_skipped": publish_skipped,
        "reason": published.get("reason"),
        "timings_ms": (
            {str(k): v for k, v in timings.items()}
            if isinstance(timings, Mapping)
            else {}
        ),
        "total_duration_ms": published.get("total_duration_ms"),
    }
    value = json.dumps(payload, sort_keys=True, default=str)
    progress = {
        f"sync.publish_stage_timings.run.{key_suffix}": value,
        "sync.publish_stage_timings.last_run": value,
    }
    try:
        SourceRunner(
            dsn,
            source_name=_PUBLISH_TIMINGS_PSEUDO_SOURCE,
            deployment_name=deployment,
        )._upsert_progress(progress)
    except Exception:  # noqa: BLE001 - diagnostics must not fail ingest.
        log.exception(
            "SourceRunner: failed to upsert publish timing progress "
            "for deployment=%s label=%s",
            deployment,
            label,
        )


def _coerce_admission_finalize_result(
    value: _AdmissionFinalizeResult | list[Any],
) -> _AdmissionFinalizeResult:
    """Accept legacy test doubles that still return the old refs list."""

    if isinstance(value, _AdmissionFinalizeResult):
        return value
    return _AdmissionFinalizeResult(refs=list(value))


# Snapshot/restore of a source's pre-run watermark and progress now lives in
# `ingest.compensation` so `SourceRunner` can undo a failed run for callers that
# have no orchestrator (issue #1431). The module-level names are kept because
# the failure paths below - and the tests that pin them - resolve them here.
from okg.substrate.ingest.compensation import (  # noqa: E402
    SourceStateSnapshot as _SourceStateSnapshot,
    restore_source_state as _restore_source_state,
    snapshot_source_state as _snapshot_source_state,
)
from okg.substrate.ingest import run_base as _run_base  # noqa: E402


def _prepare_source_admission(
    *,
    options: IngestOptions,
    entry: SourceEntry,
    source_registry_name: str,
    run_mode: str,
    sync_scope: dict[str, Any] | None,
) -> tuple[SourceRunContext, dict[str, Any], str]:
    """Create/coalesce the fast-track envelope before facts are written."""
    observed_at = options.admission_observed_at or datetime.now(timezone.utc)
    workflow_ref = workflow_ref_from_dbos(SOURCE_SYNC_WORKFLOW_NAME)
    policy = entry.admission_policy
    producer_id = (
        policy.producer_id
        if policy is not None
        else entry.ownership_id or source_registry_name
    )
    producer_kind = policy.producer_kind if policy is not None else "source"
    admission_mode = policy.admission_mode if policy is not None else "fast_track"
    replay_key = workflow_ref.get("workflow_id") or f"inline:{observed_at.isoformat()}"
    idempotency_key = stable_idempotency_key(
        "source_sync",
        options.deployment,
        source_registry_name,
        producer_id,
        run_mode,
        sync_scope or {},
        replay_key,
    )
    run_id = uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"okg:source-run:{options.deployment}:{producer_id}:{idempotency_key}",
    )
    from okg.substrate.db.pool import GENERAL, pooled_connection

    with pooled_connection(options.dsn, purpose=GENERAL) as conn:
        catalog_version_id = _current_catalog_version_id(conn)
        with conn.transaction():
            output_scope_summary = (
                policy.as_dict()["output_scope_summary"]
                if policy is not None
                else {
                    "summary": (
                        "implicit source-registry ownership; add "
                        "admission_policy to make trust explicit"
                    )
                }
            )
            profile = materialize_producer_profile(
                conn,
                deployment_name=options.deployment,
                producer_id=producer_id,
                producer_kind=producer_kind,
                trust_label=(
                    policy.trust_label
                    if policy is not None
                    else "implicit_legacy_trusted"
                ),
                admission_mode=admission_mode,
                authority_scope=(policy.authority_scope if policy is not None else {}),
                allowed_record_kinds=(
                    policy.allowed_record_kinds if policy is not None else ()
                ),
                output_signature=(
                    policy.output_signature if policy is not None else {}
                ),
                policy_clauses=(policy.policy_clauses if policy is not None else ()),
                output_scope_summary=output_scope_summary,
                source_ref={
                    "source_registry_name": source_registry_name,
                    "ownership_id": entry.ownership_id,
                },
                catalog_version_id=catalog_version_id,
            )
            producer_policy_summary = {
                **profile.evidence(),
                **(
                    {"warnings": ["producer_signature_missing"]}
                    if profile.status == "compat"
                    else {}
                ),
            }
            envelope = ensure_envelope(
                conn,
                deployment_name=options.deployment,
                producer_id=producer_id,
                producer_kind=producer_kind,
                intent="source_sync",
                admission_mode=admission_mode,
                status="pending",
                idempotency_key=idempotency_key,
                catalog_version_id=catalog_version_id,
                source=entry.ownership_id,
                run_id=str(run_id),
                authority={
                    "source_registry_name": source_registry_name,
                    "producer_id": producer_id,
                    "ownership_id": entry.ownership_id,
                    "trust": (
                        policy.trust_label
                        if policy is not None
                        else "implicit_legacy_trusted"
                    ),
                    "policy_declared": policy is not None,
                    "authority_scope": profile.authority_scope,
                    "output_scope_summary": output_scope_summary,
                    "producer_policy": producer_policy_summary,
                },
                check_summary={
                    "run_mode": run_mode,
                    "sync_scope": sync_scope or {},
                    "replay_key_kind": (
                        "dbos_workflow" if workflow_ref.get("workflow_id") else "inline"
                    ),
                    "admission_policy": (
                        policy.as_dict()
                        if policy is not None
                        else {
                            "declared": False,
                            "warning": "implicit_admission_policy",
                        }
                    ),
                    "producer_policy": producer_policy_summary,
                },
                workflow_ref=workflow_ref,
            )
    return (
        SourceRunContext(
            run_id=str(run_id),
            observed_at=observed_at,
            admission_id=envelope.admission_id,
            idempotency_key=idempotency_key,
            workflow_id=(
                str(workflow_ref.get("workflow_id"))
                if workflow_ref.get("workflow_id")
                else None
            ),
            producer_profile=profile,
        ),
        workflow_ref,
        envelope.status,
    )


def _admission_run_mode(result: Any, *, fallback: str) -> str:
    effective = getattr(result, "effective_mode", None)
    requested = getattr(result, "mode", None)
    requested_mode = requested if isinstance(requested, str) and requested else fallback
    if not isinstance(effective, str) or not effective:
        return requested_mode
    if effective == requested_mode:
        return requested_mode
    next_cursor = getattr(result, "next_cursor", None)
    has_inline_record_set = isinstance(next_cursor, Mapping) and isinstance(
        next_cursor.get("record_set"), Mapping
    )
    has_external_record_set = (
        isinstance(next_cursor, Mapping)
        and isinstance(next_cursor.get("record_set_summary"), Mapping)
        and bool(next_cursor.get("record_set_ref"))
    )
    if (
        bool(getattr(result, "completed_scope", False))
        and effective in SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES
        and (has_inline_record_set or has_external_record_set)
    ):
        return effective
    if (
        effective == "no_change"
        and bool(getattr(result, "completed_scope", False))
        and bool(getattr(result, "runner_proven_no_change", False))
    ):
        return effective
    return requested_mode


def _finalize_source_admission(
    *,
    options: IngestOptions,
    admission_id: str,
    source: str,
    run_id: str,
    workflow_ref: dict[str, Any],
    existing_status: str | None,
    admission_mode: str,
    observed_output_count: int | None = None,
    completed_scope: bool = False,
    source_health_status: str = "ok",
    run_mode: str = "cursor",
    next_cursor: Mapping[str, Any] | None = None,
    observed_at: datetime | None = None,
    source_progress: Mapping[str, str] | None = None,
    clear_replay_required: bool = False,
    replay_target: Mapping[str, Any] | None = None,
    expected_last_applied_rev: Mapping[str, Any] | None = None,
    watermark_compare_and_swap: bool = False,
    expected_bootstrap_ledger: Mapping[str, Any] | None = None,
    next_bootstrap_ledger: Mapping[str, Any] | None = None,
    rejected_fact_count: int = 0,
    private_authority_candidate: PrivateAuthorityCandidate | None = None,
) -> _AdmissionFinalizeResult:
    from okg.substrate.db.pool import GENERAL, pooled_connection

    with pooled_connection(options.dsn, purpose=GENERAL) as conn:
        with conn.transaction():
            terminal_state = _source_terminal_state_evidence(
                next_cursor=next_cursor,
                observed_at=observed_at,
                completed_scope=completed_scope,
                deployment=options.deployment,
                source_progress=source_progress,
                clear_replay_required=clear_replay_required,
                replay_target=replay_target,
                expected_last_applied_rev=expected_last_applied_rev,
                watermark_compare_and_swap=watermark_compare_and_swap,
                expected_bootstrap_ledger=expected_bootstrap_ledger,
                next_bootstrap_ledger=next_bootstrap_ledger,
            )
            if existing_status not in {
                "published",
                "blocked",
            } and not _source_health_is_healthy(source_health_status):
                # Health is a run-level integrity signal, not merely metadata.
                # A partially readable producer may have staged valid-looking
                # facts before discovering a malformed record; reject and
                # retract that entire run so neither facts nor terminal state
                # can advance from incomplete input.
                _retract_chronos_run(
                    options.dsn,
                    graph_conn=conn,
                    admission_id=admission_id,
                    source=source,
                    run_id=run_id,
                    branch=graph_branch_for_admission_mode(admission_mode),
                )
                verdict = {
                    "source": source,
                    "run_id": run_id,
                    "fact_ref_count": 0,
                    "admission_mode": admission_mode,
                    "producer_policy": {
                        "status": "rejected",
                        "reason": "failed_or_degraded",
                        "source_health_status": source_health_status,
                        "completed_scope": completed_scope,
                        "run_mode": run_mode,
                        "terminal_state_authorized": False,
                    },
                }
                if terminal_state is not None:
                    verdict["source_terminal_state"] = terminal_state
                transition_envelope(
                    conn,
                    admission_id=admission_id,
                    status="rejected",
                    actor=source,
                    reason="source_health_not_healthy",
                    verdict=verdict,
                    workflow_ref=workflow_ref,
                )
                return _AdmissionFinalizeResult(
                    refs=[],
                    status="rejected",
                    reason="source_health_not_healthy",
                    verdict=verdict,
                )
            if observed_output_count == 0 and existing_status not in {
                "published",
                "blocked",
            }:
                authority_allowed, authority_reason, authority_evidence = (
                    _evaluate_zero_output_source_authority(
                        conn,
                        options=options,
                        admission_id=admission_id,
                        source=source,
                        run_id=run_id,
                        admission_mode=admission_mode,
                    )
                )
                # close-refused-publish-redrive-gap (#828): a zero-output
                # run is only an idempotent no-op if the rows it deduped
                # against reached a PUBLISHED generation. Prior staged
                # admissions of this source (accepted, refs attached, never
                # swept) mean the dedupe basis is unpublished branch-head
                # state from a refused publish -- accepting would convert
                # stranded work into a silent SUCCESS/chunks=0 loop.
                # Fail-closed: a detection error rejects with its own
                # reason, never defaults to "no unpublished work".
                prior_unpublished_check_failed = False
                try:
                    prior_unpublished_admissions = _staged_sweepable_source_admissions(
                        conn,
                        deployment=options.deployment,
                        branch=graph_branch_for_admission_mode(
                            admission_mode,
                        ),
                        source=source,
                        exclude_admission_id=admission_id,
                    )
                except Exception:  # noqa: BLE001 - fail closed, never accept blind
                    log.exception(
                        "unpublished-prior-admission detection failed for "
                        "source=%s run=%s; rejecting zero-output run "
                        "fail-closed",
                        source,
                        run_id,
                    )
                    prior_unpublished_admissions = []
                    prior_unpublished_check_failed = True
                structural_verdict = _zero_output_admission_verdict(
                    source=source,
                    run_id=run_id,
                    admission_mode=admission_mode,
                    completed_scope=completed_scope,
                    source_health_status=source_health_status,
                    run_mode=run_mode,
                    next_cursor=next_cursor,
                    rejected_fact_count=rejected_fact_count,
                    prior_unpublished_admissions=prior_unpublished_admissions,
                    prior_unpublished_check_failed=(prior_unpublished_check_failed),
                    **_source_history_kwargs(
                        conn, source=source, deployment=options.deployment,
                        completed_scope=completed_scope,
                    ),
                )
                if structural_verdict["producer_policy"].get("keeps_source_state"):
                    terminal_state = None  # #2074 (a): no cursor, progress or reconcile saved
                verdict = merge_policy_evidence(
                    structural_verdict,
                    authority_evidence,
                )
                structural_allowed = structural_verdict["producer_policy"][
                    "terminal_state_authorized"
                ]
                accepted_zero = structural_allowed and authority_allowed
                rejection_reason = (
                    None
                    if accepted_zero
                    else _zero_output_rejection_reason(
                        authority_reason=authority_reason,
                        structural_allowed=structural_allowed,
                        structural_kind=structural_verdict["producer_policy"].get(
                            "reason"
                        ),
                    )
                )
                producer_policy = dict(verdict.get("producer_policy") or {})
                producer_policy["authority_status"] = producer_policy.get("status")
                producer_policy["status"] = "matched" if accepted_zero else "rejected"
                producer_policy["terminal_state_authorized"] = accepted_zero
                verdict["producer_policy"] = producer_policy
                if terminal_state is not None:
                    verdict["source_terminal_state"] = terminal_state
                if admission_mode_awaits_decision(admission_mode) and accepted_zero:
                    # Same private handoff as the non-zero held exit below.
                    record_pending_authority_admission(
                        conn,
                        admission_id=admission_id,
                        candidate=private_authority_candidate,
                    )
                    _record_review_submitted(
                        conn,
                        admission_id=admission_id,
                        actor=source,
                        verdict=verdict,
                        workflow_ref=workflow_ref,
                    )
                    return _AdmissionFinalizeResult(
                        refs=[],
                        status="pending",
                        verdict=verdict,
                    )
                if accepted_zero:
                    seal_bound_authority(conn, private_authority_candidate)
                    _persist_accepted_source_terminal_state(
                        conn,
                        source=source,
                        terminal_state=terminal_state,
                    )
                transition_envelope(
                    conn,
                    admission_id=admission_id,
                    status=("accepted" if accepted_zero else "rejected"),
                    actor=source,
                    reason=rejection_reason,
                    verdict=verdict,
                    workflow_ref=workflow_ref,
                )
                return _AdmissionFinalizeResult(
                    refs=[],
                    status=("accepted" if accepted_zero else "rejected"),
                    reason=rejection_reason,
                    verdict=verdict,
                )
            refs = attach_fact_range_refs_for_run(
                conn,
                admission_id=admission_id,
                source=source,
                run_id=run_id,
                workflow_ref=workflow_ref,
            )
            if existing_status not in {"published", "blocked"}:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT producer_id, authority, check_summary
                          FROM okg.admission_envelopes
                         WHERE admission_id = %s::uuid
                        """,
                        (admission_id,),
                    )
                    row = cur.fetchone()
                producer_id = str(row[0]) if row else source
                authority = dict(row[1] or {}) if row else {}
                check_summary = dict(row[2] or {}) if row else {}
                policy_summary = dict(
                    check_summary.get("producer_policy") or {},
                )
                profile = load_profile_by_fingerprint(
                    conn,
                    deployment_name=options.deployment,
                    producer_id=producer_id,
                    profile_fingerprint=policy_summary.get(
                        "profile_fingerprint",
                    ),
                )
                try:
                    policy_evidence = evaluate_source_run_signature(
                        conn,
                        profile=profile,
                        source=source,
                        run_id=run_id,
                    )
                except ProducerPolicyViolation as exc:
                    _retract_chronos_run(
                        options.dsn,
                        graph_conn=conn,
                        admission_id=admission_id,
                        source=source,
                        run_id=run_id,
                        branch=graph_branch_for_admission_mode(
                            admission_mode,
                        ),
                    )
                    verdict = merge_policy_evidence(
                        {
                            "source": source,
                            "run_id": run_id,
                            "fact_ref_count": len(refs),
                            "admission_mode": admission_mode,
                        },
                        exc.verdict,
                    )
                    transition_envelope(
                        conn,
                        admission_id=admission_id,
                        status="rejected",
                        actor=source,
                        reason=exc.code,
                        verdict=verdict,
                        workflow_ref=workflow_ref,
                    )
                    return _AdmissionFinalizeResult(
                        refs=refs,
                        status="rejected",
                        reason=exc.code,
                        verdict=verdict,
                    )
                try:
                    from okg.substrate.nomos import (
                        authorize_source_run_outcome_with_active_policy,
                    )
                    from okg.substrate.nomos.store import (
                        resolve_source_authorization_identity,
                    )

                    authorization_source = resolve_source_authorization_identity(
                        producer_id=producer_id,
                        source=source,
                        authority=authority,
                    )
                    # `allowed` is the authorization answer; the recorded
                    # decision's effect keeps the strict verdict, which an
                    # audit-mode deployment has chosen not to act on (#2123).
                    nomos_outcome = authorize_source_run_outcome_with_active_policy(
                        conn,
                        deployment_name=options.deployment,
                        scope=f"deployment:{options.deployment}",
                        producer_id=authorization_source,
                        source=source,
                        run_id=run_id,
                        fact_ref_count=len(refs),
                        admission_mode=admission_mode,
                        graph_branch=graph_branch_for_admission_mode(
                            admission_mode,
                        ),
                    )
                    nomos_decision = (
                        None if nomos_outcome is None else nomos_outcome.decision
                    )
                except Exception as exc:  # noqa: BLE001
                    _retract_chronos_run(
                        options.dsn,
                        graph_conn=conn,
                        admission_id=admission_id,
                        source=source,
                        run_id=run_id,
                        branch=graph_branch_for_admission_mode(
                            admission_mode,
                        ),
                    )
                    verdict = merge_policy_evidence(
                        {
                            "source": source,
                            "run_id": run_id,
                            "fact_ref_count": len(refs),
                            "admission_mode": admission_mode,
                        },
                        {
                            "nomos": {
                                "effect": "deny",
                                "reason_code": ("nomos.source_admission_error"),
                                "error": f"{type(exc).__name__}: {exc}",
                            },
                        },
                    )
                    transition_envelope(
                        conn,
                        admission_id=admission_id,
                        status="rejected",
                        actor=source,
                        reason="nomos_source_admission_failed",
                        verdict=verdict,
                        workflow_ref=workflow_ref,
                    )
                    return _AdmissionFinalizeResult(
                        refs=refs,
                        status="rejected",
                        reason="nomos_source_admission_failed",
                        verdict=verdict,
                    )
                if nomos_outcome is not None and not nomos_outcome.allowed:
                    _retract_chronos_run(
                        options.dsn,
                        graph_conn=conn,
                        admission_id=admission_id,
                        source=source,
                        run_id=run_id,
                        branch=graph_branch_for_admission_mode(
                            admission_mode,
                        ),
                    )
                    verdict = merge_policy_evidence(
                        {
                            "source": source,
                            "run_id": run_id,
                            "fact_ref_count": len(refs),
                            "admission_mode": admission_mode,
                        },
                        {
                            "nomos": nomos_decision.to_dict(),
                        },
                    )
                    transition_envelope(
                        conn,
                        admission_id=admission_id,
                        status="rejected",
                        actor=source,
                        reason=nomos_decision.reason_code,
                        verdict=verdict,
                        workflow_ref=workflow_ref,
                    )
                    return _AdmissionFinalizeResult(
                        refs=refs,
                        status="rejected",
                        reason=nomos_decision.reason_code,
                        verdict=verdict,
                    )
                verdict = merge_policy_evidence(
                    {
                        "source": source,
                        "run_id": run_id,
                        "fact_ref_count": len(refs),
                        "admission_mode": admission_mode,
                    },
                    merge_policy_evidence(
                        policy_evidence,
                        (
                            {"nomos": nomos_decision.to_dict()}
                            if nomos_decision is not None
                            else {}
                        ),
                    ),
                )
                if terminal_state is not None:
                    verdict["source_terminal_state"] = terminal_state
                if admission_mode_awaits_decision(admission_mode):
                    # Review- and quarantine-mode runs hold on the
                    # quarantine branch until `okg admission decide`
                    # applies them onto `default`. Record the passing
                    # policy verdict and signal readiness for review, but
                    # do NOT accept: an `accepted` envelope here would
                    # report rows as admitted that never reach `default`
                    # (issue #1752).
                    record_pending_authority_admission(
                        conn,
                        admission_id=admission_id,
                        candidate=private_authority_candidate,
                    )
                    _record_review_submitted(
                        conn,
                        admission_id=admission_id,
                        actor=source,
                        verdict=verdict,
                        workflow_ref=workflow_ref,
                    )
                    return _AdmissionFinalizeResult(
                        refs=refs,
                        status="pending",
                        verdict=verdict,
                    )
                seal_bound_authority(conn, private_authority_candidate)
                _persist_accepted_source_terminal_state(
                    conn,
                    source=source,
                    terminal_state=terminal_state,
                )
                transition_envelope(
                    conn,
                    admission_id=admission_id,
                    status="accepted",
                    actor=source,
                    verdict=verdict,
                    workflow_ref=workflow_ref,
                )
            else:
                return _AdmissionFinalizeResult(
                    refs=refs,
                    status=existing_status or "accepted",
                )
    return _AdmissionFinalizeResult(refs=refs, status="accepted")


def _evaluate_zero_output_source_authority(
    conn: psycopg.Connection,
    *,
    options: IngestOptions,
    admission_id: str,
    source: str,
    run_id: str,
    admission_mode: str,
) -> tuple[bool, str | None, dict[str, Any]]:
    """Evaluate profile/signature/Nomos even when a source emitted no facts."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT producer_id, authority, check_summary
              FROM okg.admission_envelopes
             WHERE admission_id = %s::uuid
            """,
            (admission_id,),
        )
        row = cur.fetchone()
    producer_id = str(row[0]) if row else source
    authority = dict(row[1] or {}) if row else {}
    check_summary = dict(row[2] or {}) if row else {}
    policy_summary = dict(check_summary.get("producer_policy") or {})
    base = {
        "source": source,
        "run_id": run_id,
        "fact_ref_count": 0,
        "admission_mode": admission_mode,
    }
    try:
        profile = load_profile_by_fingerprint(
            conn,
            deployment_name=options.deployment,
            producer_id=producer_id,
            profile_fingerprint=policy_summary.get("profile_fingerprint"),
        )
        if profile.status not in {"active", "compat"}:
            return (
                False,
                "producer_profile_not_writable",
                merge_policy_evidence(
                    base,
                    {
                        "producer_policy": {
                            **profile.evidence(),
                            "status": "rejected",
                            "reason": "producer_profile_not_writable",
                        },
                    },
                ),
            )
        policy_evidence = evaluate_source_run_signature(
            conn,
            profile=profile,
            source=source,
            run_id=run_id,
        )
    except ProducerPolicyViolation as exc:
        return False, exc.code, merge_policy_evidence(base, exc.verdict)

    try:
        from okg.substrate.nomos import (
            authorize_source_run_outcome_with_active_policy,
        )
        from okg.substrate.nomos.store import (
            resolve_source_authorization_identity,
        )

        authorization_source = resolve_source_authorization_identity(
            producer_id=producer_id,
            source=source,
            authority=authority,
        )
        nomos_outcome = authorize_source_run_outcome_with_active_policy(
            conn,
            deployment_name=options.deployment,
            scope=f"deployment:{options.deployment}",
            producer_id=authorization_source,
            source=source,
            run_id=run_id,
            fact_ref_count=0,
            admission_mode=admission_mode,
            graph_branch=graph_branch_for_admission_mode(admission_mode),
        )
        nomos_decision = (
            None if nomos_outcome is None else nomos_outcome.decision
        )
    except Exception as exc:  # noqa: BLE001 - policy failures deny zero-output
        return (
            False,
            "nomos_source_admission_failed",
            merge_policy_evidence(
                base,
                policy_evidence,
                {
                    "nomos": {
                        "effect": "deny",
                        "reason_code": "nomos.source_admission_error",
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                },
            ),
        )
    if nomos_outcome is not None and not nomos_outcome.allowed:
        return (
            False,
            nomos_decision.reason_code,
            merge_policy_evidence(
                base,
                policy_evidence,
                {"nomos": nomos_decision.to_dict()},
            ),
        )
    authority_status = str(
        (policy_evidence.get("producer_policy") or {}).get("status") or ""
    )
    return (
        authority_status in {"matched", "compat"},
        None,
        merge_policy_evidence(
            base,
            policy_evidence,
            ({"nomos": nomos_decision.to_dict()} if nomos_decision is not None else {}),
        ),
    )


def _retract_chronos_run(
    dsn: str,
    *,
    admission_id: str | None,
    source: str,
    run_id: str,
    branch: str = "default",
    graph_conn: Any | None = None,
) -> None:
    """Compensate one rejected run (see `ingest.compensation`).

    Kept as a module-level name because the orchestrator's failure paths —
    and the tests that pin them — resolve it here. `SourceRunner` calls the
    same implementation directly when it has no orchestrator to compensate
    for it (issue #1431).
    """
    from okg.substrate.ingest.compensation import retract_chronos_run

    retract_chronos_run(
        dsn,
        admission_id=admission_id,
        source=source,
        run_id=run_id,
        branch=branch,
        graph_conn=graph_conn,
    )


def _fail_source_admission(
    *,
    dsn: str,
    admission_id: str,
    error: str,
    workflow_ref: dict[str, Any],
    diagnosis: Mapping[str, Any] | None = None,
) -> None:
    # Issue #945: the classified diagnosis rides in the envelope verdict
    # (`error_kind` + the evidence that produced it) so an operator
    # querying okg.admission_events can filter on the kind instead of
    # reading 177 identical TypeError strings and guessing.
    verdict: dict[str, Any] = {"error": error}
    if diagnosis is not None:
        verdict["error_kind"] = diagnosis.get("error_kind")
        verdict["error_diagnosis"] = dict(diagnosis)
    try:
        from okg.substrate.db.pool import GENERAL, pooled_connection

        with pooled_connection(dsn, purpose=GENERAL) as conn:
            with conn.transaction():
                transition_envelope(
                    conn,
                    admission_id=admission_id,
                    status="failed",
                    reason=error,
                    verdict=verdict,
                    workflow_ref=workflow_ref,
                )
    except Exception:  # noqa: BLE001 — best-effort failure-record; logged, never re-raised
        log.exception(
            "failed to mark source admission failed: admission_id=%s",
            admission_id,
        )


def _current_catalog_version_id(conn: psycopg.Connection) -> int | None:
    # The active applied catalog version is global to the database and is never
    # written during an ingest run (catalog application is a separate admin
    # path), so resolve it at most once per commit. Outside a commit scope this
    # falls through to a live read.
    def _read() -> int | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT max(catalog_version_id)
                  FROM okg.catalog_versions
                 WHERE status = 'applied'
                """
            )
            row = cur.fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])

    return memoized(("catalog_version_id", db_scope_key(conn)), _read)


def _run_mode_for_source(
    options: IngestOptions,
    entry: SourceEntry,
    name: str,
    events: tuple[SourceSyncEvent, ...],
    sync_scope: dict[str, Any] | None = None,
) -> str:
    if options.run_modes_by_source:
        return options.run_modes_by_source[name]
    requested = _requested_run_mode_for_source(
        options, entry, name, events, sync_scope,
    )
    if options.run_mode_explicit:
        return requested
    return profile_native_mode(entry, requested)


def _requested_run_mode_for_source(
    options: IngestOptions,
    entry: SourceEntry,
    name: str,
    events: tuple[SourceSyncEvent, ...],
    sync_scope: dict[str, Any] | None = None,
) -> str:
    if not options.source_events:
        return options.run_mode
    sync = entry.sync
    if events:
        event_mode = _source_event_run_mode(
            sync,
            events,
            fallback_run_mode=options.run_mode,
        )
        if _event_scope_is_usable(sync, event_mode, sync_scope):
            return event_mode
    if sync is None:
        if options.run_mode in {"scope_incomplete", "scope_complete"}:
            return "scope_complete"
        return options.run_mode
    mode = sync.default_event_mode
    if mode in {"scope_incomplete", "scope_complete"}:
        return (
            sync.scope.unsupported_fallback
            if sync.scope is not None and sync.scope.unsupported_fallback
            else sync.reconcile_mode or "scope_complete"
        )
    return mode


def _source_event_run_mode(
    sync: Any,
    events: tuple[SourceSyncEvent, ...],
    *,
    fallback_run_mode: str,
) -> str:
    if sync is None:
        return fallback_run_mode
    if any(event.trigger_kind == "reconcile" for event in events):
        return sync.reconcile_mode or "scope_complete"
    has_delete = any(
        event.reason in {"webhook:removed", "webhook:deleted"}
        or str(event.reason or "").startswith("watch:deleted")
        or str(event.reason or "").startswith("watch:moved")
        or ":watch:deleted" in str(event.reason or "")
        or ":watch:moved" in str(event.reason or "")
        for event in events
    )
    if has_delete and sync.scoped_delete_mode is not None:
        return sync.scoped_delete_mode
    if has_delete and sync.deletes_require_reconcile:
        return sync.reconcile_mode or "scope_complete"
    return sync.default_event_mode or fallback_run_mode


def _event_scope_is_usable(
    sync: Any,
    run_mode: str,
    sync_scope: dict[str, Any] | None,
) -> bool:
    if run_mode not in {"scope_incomplete", "scope_complete"}:
        return True
    if sync is None or sync.scope is None:
        return False
    accepts = set(sync.scope.accepts)
    scope = sync_scope or {}
    if "path" in accepts and scope.get("scope_keys"):
        return True
    if "revision_hint" in accepts and scope.get("revision_hints"):
        return True
    return False


def _publish(
    options: IngestOptions,
    label: str,
    chronos_dirty_scope_hint: (
        tuple[
            dict[str, list[str]],
            dict[str, list[int]],
            bool,
            dict[str, dict[str, list[str]]] | None,
        ]
        | None
    ) = None,
    enricher_lineage_invalidations_hint: Any = _ENRICHER_LINEAGE_FROM_CHRONOS,
    publish_blocker: Mapping[str, Any] | None = None,
    expected_latest_checkpoint_id: str | None = None,
    expected_branch_mutation_token: str | None = None,
    require_expected_latest_checkpoint: bool = False,
    expected_admission_ids: Iterable[str] | None = None,
    recovery_from_generation_id: str | None = None,
    admission_feedback_expected_status: str | None = None,
    expected_install_stage: Any = None,
    ingest_completeness: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    from okg.substrate.publisher.deployment_stack import (
        build_deployment_publisher_stack,
    )
    from okg.substrate.publisher.factory import build_deployment_publisher

    try:
        if expected_install_stage is not None and not require_expected_latest_checkpoint:
            raise SubstrateError("installer stage requires guarded publication")
        stack = build_deployment_publisher_stack(
            dsn=options.dsn,
            deployment_name=options.deployment,
            deployment_dir=options.deployment_dir,
        )
        publisher_overrides = {
            "chronos_dirty_scope_hint": chronos_dirty_scope_hint,
            "own_source_run_ids": _run_base.INGEST_OWN_RUN_IDS.get(),  # #2993
        }
        if options.owner_meta.get("command") == "source_sync_workflow":
            publisher_overrides["source_sync_publish_context"] = True
        if ingest_completeness is not None:  # #3342: the generation's marker
            publisher_overrides["ingest_completeness"] = ingest_completeness
        if enricher_lineage_invalidations_hint is not _ENRICHER_LINEAGE_FROM_CHRONOS:
            publisher_overrides["enricher_lineage_invalidations_hint"] = (
                enricher_lineage_invalidations_hint
            )
        publisher = build_deployment_publisher(
            dsn=options.dsn,
            deployment_name=options.deployment,
            deployment_dir=options.deployment_dir,
            command=f"okg ingest publish:{label}",
            stack=stack,
            publisher_overrides=publisher_overrides,
        )
        owner_meta = dict(options.owner_meta)
        owner_meta.setdefault(
            "command",
            f"okg ingest --deployment {options.deployment} (publish:{label})",
        )
        publisher.set_owner(
            owner_id=options.owner_id,
            owner_meta=owner_meta,
        )
        cycle_kwargs: dict[str, Any] = {
            "force_enrichers": options.force_enrichers,
        }
        if publish_blocker is not None:
            cycle_kwargs["publish_blocker"] = publish_blocker
        if require_expected_latest_checkpoint:
            cycle_kwargs["expected_latest_checkpoint_id"] = (
                expected_latest_checkpoint_id
            )
            cycle_kwargs["expected_branch_mutation_token"] = (
                expected_branch_mutation_token
            )
            cycle_kwargs["require_expected_latest_checkpoint"] = True

        def cycle_and_feedback(
            conn: psycopg.Connection | None = None,
        ) -> dict[str, Any]:
            feedback_kwargs: dict[str, Any] = {}
            if expected_admission_ids is not None:
                feedback_kwargs["expected_admission_ids"] = expected_admission_ids
            if recovery_from_generation_id is not None:
                feedback_kwargs["recovery_from_generation_id"] = (
                    recovery_from_generation_id
                )
            if admission_feedback_expected_status is not None:
                feedback_kwargs["expected_status"] = admission_feedback_expected_status
            if conn is None:
                out = publisher.cycle(**cycle_kwargs)
                feedback = _apply_publish_admission_feedback(
                    options,
                    out,
                    **feedback_kwargs,
                )
            else:
                out = publisher.cycle(conn=conn, **cycle_kwargs)
                feedback = _apply_publish_admission_feedback(
                    options,
                    out,
                    conn=conn,
                    **feedback_kwargs,
                )
            if feedback is not None:
                out["admission_feedback"] = feedback
            return out

        if require_expected_latest_checkpoint:
            # Keep the SAME graph-write session fence through admission
            # feedback. Otherwise a source blocked behind Publisher.cycle can
            # append only after its checkpoint, then race the deployment-wide
            # feedback sweep and be mislabeled as published by a generation
            # that does not contain its row.
            # RESERVED: this connection holds the deployment publisher mutex
            # and the branch write fence around the whole publish cycle. It is
            # the publish path, not a lane borrowing beside it — classifying it
            # GENERAL would let the reservation refuse the very publish it
            # exists to protect, and would park a hours-long hold in a general
            # slot.
            from okg.substrate.db.pool import RESERVED, pooled_connection
            from okg.substrate.graph_store import ChronosGraphStore

            with pooled_connection(
                options.dsn, purpose=RESERVED,
            ) as guarded_conn:
                graph = ChronosGraphStore.from_connection(
                    guarded_conn,
                    ensure_schema=False,
                )
                # Match ordinary publisher lock order: deployment publisher
                # mutex FIRST, graph branch fence SECOND. Both session locks
                # are reentrant on this connection, so Publisher.cycle can
                # enter its normal locking path without an inversion against
                # another publisher emitting derived graph facts.
                with publisher._publish_lock_on_connection(
                    guarded_conn,
                    mode=publisher._resolved_coordination_mode(),
                ):
                    if not graph._lock_branch_write_session(publisher.branch_name):
                        raise SubstrateError(
                            "guarded redrive requires PostgreSQL branch fencing",
                            data={"branch": publisher.branch_name},
                        )
                    try:
                        if expected_install_stage is not None:
                            from okg.substrate.distributions.install_stage import require_stage
                            if expected_install_stage.deployment != options.deployment or expected_install_stage.branch != publisher.branch_name:
                                raise SubstrateError("installer stage publication identity mismatch")
                            require_stage(guarded_conn, expected_install_stage)
                        out = cycle_and_feedback(guarded_conn)
                    finally:
                        graph._unlock_branch_write_session(publisher.branch_name)
        else:
            out = cycle_and_feedback()
        return {"label": label, **out}
    except SubstrateError as e:
        # `fix_hint` is the only field carrying the repair instruction. The
        # staged-source redrive guard re-raises from this dict, so dropping
        # the hint here destroys the recovery command one layer below the
        # operator who needs it (#1791).
        failed: dict[str, Any] = {
            "label": label,
            "status": "fail",
            "code": e.code,
            "error": str(e),
            "error_data": dict(e.data),
        }
        if e.fix_hint:
            failed["fix_hint"] = str(e.fix_hint)
        return failed


def _apply_publish_admission_feedback(
    options: IngestOptions,
    summary: Mapping[str, Any],
    *,
    conn: psycopg.Connection | None = None,
    expected_admission_ids: Iterable[str] | None = None,
    recovery_from_generation_id: str | None = None,
    expected_status: str | None = None,
) -> dict[str, Any] | None:
    generation_id = summary.get("generation_id")
    status = summary.get("status")
    if generation_id is None or status not in {"published", "blocked"}:
        return None
    if expected_status is not None and status != expected_status:
        return None

    workflow_ref = workflow_ref_from_dbos("okg_source_sync")
    # Borrow a warm connection from the process-shared pool instead of opening
    # a fresh psycopg connection for the commit publish. The pool's connection
    # is autocommit=True, matching the previous connect(); the explicit
    # conn.transaction() keeps the feedback write atomic.
    # RESERVED: this runs after a successful publish and marks the admissions
    # that generation contains. Being refused here would leave a published
    # generation with unapplied feedback.
    from okg.substrate.db.pool import RESERVED, pooled_connection

    connection_scope = (
        nullcontext(conn)
        if conn is not None
        else pooled_connection(options.dsn, purpose=RESERVED)
    )
    with connection_scope as feedback_conn:
        with feedback_conn.transaction():
            return apply_generation_feedback(
                feedback_conn,
                deployment_name=options.deployment,
                generation_id=str(generation_id),
                workflow_ref=workflow_ref,
                expected_admission_ids=expected_admission_ids,
                recovery_from_generation_id=recovery_from_generation_id,
            )


def _append_skipped(
    report: dict[str, Any],
    *,
    reason: str,
    after_layer: int,
    remaining_layers: list[list[str]],
) -> None:
    remaining = [name for layer in remaining_layers for name in layer]
    if remaining:
        report["skipped"].append(
            {
                "reason": reason,
                "after_layer": after_layer,
                "sources": remaining,
            }
        )


__all__ = [
    "IngestOptions",
    "IngestPlan",
    "IngestRunResult",
    "plan_ingest_sources",
    "resolve_predecessor_deps",
    "run_ingest",
    "topo_layers",
    "topo_sort",
]
