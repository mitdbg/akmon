"""Source-sync ingest as a DBOS workflow (migrate-substrate-runtime-
to-dbos Phase 2 §2.1).

Replaces the legacy ``okg.jobs`` ``ingest`` job kind with a
``@DBOS.workflow``-decorated wrapper around the existing
``run_ingest(IngestOptions)`` callable. Per-source orchestration —
dependency-layer ordering, watermark handling, source-event passing,
inter-layer publish — stays inside ``run_ingest`` unchanged; only the
queue-and-dispatch shell flips from ``okg.jobs`` to DBOS.

The workflow:

  * Runs on the deployment's source-sync queue
    (``okg-<deployment>-source-sync``). This keeps replaceable source
    wakeups from starving control-plane workflows on the default queue.
  * Takes JSON-serializable args (deployment, sources, run_mode, etc.)
    so DBOS workflow recovery can re-serialize the input on resume.
  * Resolves the deployment dir + DSN from the manifest at workflow
    body time, not at enqueue time. This keeps the args list small
    and avoids capturing transient paths.

The ingest/projection/publish cycle is a durable ``@DBOS.step``. Once that
cycle completes, workflow recovery reads its recorded result instead of
executing the heavy body again. The step does not retry after a reported
failure because the body performs externally visible database work; a later
source-sync wakeup is the safe retry boundary.
"""
from __future__ import annotations

import logging
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from okg.substrate.runtime.source_sync_constants import SOURCE_SYNC_WORKFLOW_NAME


__all__ = [
    "SOURCE_SYNC_WORKFLOW_NAME",
    "register_source_sync_workflow",
    "source_sync_workflow",
]


log = logging.getLogger(__name__)


_DECORATED: dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Deployment-context cache (perf §2)
# ---------------------------------------------------------------------------
#
# `_run_one_ingest` is called once per source-sync workflow invocation,
# which during a high-frequency event stream (commit-replay bench, busy
# repo) is once per commit. Re-parsing the YAML manifest, extracting
# the DSN, and walking `find_deployment_dir` on every commit costs
# ~250-350ms per call — frequently larger than the actual fact
# emission. Cache the resolved bundle at module scope (one process =
# one worker) and invalidate on manifest mtime change so a live edit
# of the manifest still takes effect within the next workflow.
#
# Key: deployment name (every workflow is bound to one deployment at
# `register_source_sync_workflow` time, but a worker could in principle
# host multiple, so we key by name).
# Value: (manifest_mtime_ns, manifest, dsn, dep_dir).
#
# Concurrency: the worker may run multiple source-sync workflows in
# parallel (different sources on different partition queues), but each
# entry is read+stat+possibly-write under a single lock. Lock contention
# is negligible compared to the YAML parse this protects.
_CTX_CACHE: dict[str, tuple[int, Any, str, Path]] = {}
_CTX_LOCK = threading.Lock()


def _stat_manifest_mtime_ns(dep_dir: Path) -> int:
    """Return manifest.yaml's mtime in nanoseconds, or 0 if missing.

    Used as the cache-invalidation token. Stat is ~50µs vs ~200ms for
    a full re-resolve, so the check pays for itself almost immediately.
    """
    try:
        return os.stat(dep_dir / "deployment.yaml").st_mtime_ns
    except OSError:
        return 0


def _resolve_context_cached(deployment: str):
    """Resolved (manifest, dsn, dep_dir) tuple — cached by mtime.

    On a cache hit (very common: every workflow after the first for a
    given deployment, assuming the manifest hasn't changed) this skips
    the YAML parse + DSN extract + dir lookup that dominated profiled
    source-sync runs. The saved work is roughly hundreds of ms per hit.
    """
    from okg.substrate.deployments import (
        find_deployment_dir,
        inject_deployment_path,
        require_postgres_dsn,
        resolve_deployment,
    )
    from okg.substrate.deployments.sync_root import require_resolved_sync_root

    # Fast path: cache hit + mtime unchanged.
    with _CTX_LOCK:
        hit = _CTX_CACHE.get(deployment)
    if hit is not None:
        cached_mtime, manifest, dsn, dep_dir = hit
        if cached_mtime == _stat_manifest_mtime_ns(dep_dir):
            # Still inject the path: sys.path edits are process-global
            # and *should* persist, but cheap to re-apply (idempotent).
            inject_deployment_path(dep_dir)
            return manifest, dsn, dep_dir

    # Slow path: resolve from disk + populate cache under lock.
    manifest = resolve_deployment(deployment)
    # This lane feeds `manifest.sync_root` straight into the source registry's
    # repo_root_override below, where None silently means "some default root".
    # Checked on the SLOW path only, which is sufficient and deliberate: the
    # check runs before `_CTX_CACHE` is written, so no cache hit can ever serve
    # a manifest that did not pass it (issue #1014).
    require_resolved_sync_root(manifest, command="source sync")
    dsn = require_postgres_dsn(manifest, deployment=deployment)
    dep_dir = find_deployment_dir(deployment)
    if dep_dir is None:
        raise RuntimeError(
            f"deployment dir for {deployment!r} not found; "
            "check OKG_DEPLOYMENTS_DIR and deployments/<name>/"
        )
    inject_deployment_path(dep_dir)
    with _CTX_LOCK:
        _CTX_CACHE[deployment] = (
            _stat_manifest_mtime_ns(dep_dir),
            manifest,
            dsn,
            dep_dir,
        )
    return manifest, dsn, dep_dir


def register_source_sync_workflow(*, deployment: str) -> None:
    """Decorate the source-sync workflow on the live DBOS singleton.

    Called from the worker entry point AFTER ``DBOS()`` and BEFORE
    ``DBOS.launch()``. Idempotent: clears the module-level cache on
    entry so a fresh worker boot picks up the current closure.
    """
    from dbos import DBOS
    from okg.substrate.runtime.workflow_operational_outcomes import (
        define_operational_transactions,
        participant_for,
    )

    _DECORATED.clear()
    operational = define_operational_transactions(
        participant=participant_for(
            deployment=deployment,
            workflow_name=SOURCE_SYNC_WORKFLOW_NAME,
        ),
    )

    # Probe step: a no-op marker that records "deployment context is
    # resolved, ingest is about to start" in `dbos.operation_outputs`.
    # Gives the bench (and any future profiler) a clean boundary that
    # separates A1.setup from A1.ingest without parsing application
    # logs. Without it, the workflow has only ONE step (DBOS.setEvent
    # at the very end), so "A1" collapses into a single 250-350ms bar
    # that misleadingly looks like pure overhead but actually contains
    # the entire run_ingest call. See bench/plot.py phase_breakdown.
    @DBOS.step(name="okg_source_sync_setup_done")
    def _record_setup_done() -> int:
        return int(time.monotonic() * 1000)

    @DBOS.step(name="okg_source_sync_observed_at")
    def _capture_observed_at() -> str:
        return datetime.now(timezone.utc).isoformat()

    @DBOS.step(
        name="okg_source_sync_ingest_cycle",
        retries_allowed=False,
    )
    def _run_durable_ingest_cycle(payload: dict[str, Any]) -> dict:
        return _run_one_ingest(
            deployment=deployment,
            sources=payload.get("sources"),
            run_mode=str(payload.get("run_mode") or "scope_complete"),
            include_dependents=bool(payload.get("include_dependents")),
            dependent_fanout=payload.get("dependent_fanout"),
            events=payload.get("events"),
            publish=bool(payload.get("publish")),
            include_predecessors=bool(payload.get("include_predecessors")),
            profile_force_deep=bool(payload.get("profile_force_deep")),
            profile_collectors=tuple(payload.get("profile_collectors") or ()),
            force_enrichers=bool(payload.get("force_enrichers")),
            observed_at_iso=payload.get("observed_at_iso"),
            workflow_telemetry=payload.get("workflow_telemetry"),
            run_mode_explicit=bool(payload.get("run_mode_explicit")),
        )

    @DBOS.workflow(
        name=SOURCE_SYNC_WORKFLOW_NAME,
        max_recovery_attempts=10,
    )
    def source_sync_workflow_impl(
        sources: list[str] | None = None,
        run_mode: str = "scope_complete",
        include_dependents: bool = True,
        dependent_fanout: str | None = None,
        events: list[dict] | None = None,
        publish: bool = True,
        include_predecessors: bool = True,
        profile_force_deep: bool = False,
        profile_collectors: list[str] | None = None,
        supersedes_workflow_id: str | None = None,
        force_enrichers: bool = False,
        workflow_telemetry: dict[str, Any] | None = None,
        run_mode_explicit: bool = False,
    ) -> dict:
        """Drive one source-sync ingest cycle for the bound deployment.

        Args mirror ``SourceSyncJob``'s shape so an operator-side
        enqueue and a scheduled enqueue share a stable contract.
        Returns the resulting ``IngestRunResult.report`` so operators
        can inspect outcomes via ``okg runtime debug-workflow``.

        ``run_mode_explicit`` is True only when the operator typed
        ``okg ingest --mode``; that mode then reaches the runner as typed,
        as on the inline path (issue #2486). A workflow enqueued before the
        argument existed recovers with the default: mode not typed.
        """
        observed_at_iso = _capture_observed_at()
        # The value is durable DBOS input used by current-DLQ accounting. The
        # workflow body does not otherwise alter execution for a supersession.
        del supersedes_workflow_id
        _record_setup_done()
        operational.register_execution_tx()
        result = _run_durable_ingest_cycle({
            "sources": sources,
            "run_mode": run_mode,
            "include_dependents": include_dependents,
            "dependent_fanout": dependent_fanout,
            "events": events,
            "publish": publish,
            "include_predecessors": include_predecessors,
            "profile_force_deep": profile_force_deep,
            "profile_collectors": list(profile_collectors or ()),
            "force_enrichers": force_enrichers,
            "observed_at_iso": observed_at_iso,
            "workflow_telemetry": workflow_telemetry,
            "run_mode_explicit": run_mode_explicit,
        })
        operational.record_outcome_tx(_source_sync_operational_outcome(result))
        return result

    _DECORATED[SOURCE_SYNC_WORKFLOW_NAME] = source_sync_workflow_impl
    log.info(
        "registered source-sync workflow: deployment=%s name=%s",
        deployment, SOURCE_SYNC_WORKFLOW_NAME,
    )


def source_sync_workflow() -> Any:
    """Public accessor for the decorated workflow function.

    Used by tests + admin tooling that want a direct handle.
    """
    if SOURCE_SYNC_WORKFLOW_NAME not in _DECORATED:
        raise RuntimeError(
            "source-sync workflow not registered; call "
            "register_source_sync_workflow() before DBOS.launch()"
        )
    return _DECORATED[SOURCE_SYNC_WORKFLOW_NAME]


# ---------------------------------------------------------------------------
# Body — delegates to the existing run_ingest callable
# ---------------------------------------------------------------------------


def _run_one_ingest(
    *,
    deployment: str,
    sources: list[str] | None,
    run_mode: str,
    include_dependents: bool,
    dependent_fanout: str | None,
    events: list[dict] | None,
    publish: bool,
    include_predecessors: bool = True,
    profile_force_deep: bool = False,
    profile_collectors: tuple[str, ...] = (),
    force_enrichers: bool = False,
    setup_done_probe: Any = None,
    observed_at_iso: str | None = None,
    workflow_telemetry: dict[str, Any] | None = None,
    run_mode_explicit: bool = False,
) -> dict:
    """Resolve the deployment manifest, build IngestOptions, run.

    Uses the same ingest path as direct source-sync execution, with two
    workflow-specific differences:

      1. The manifest is resolved fresh at workflow execution time —
         but cached at worker scope, invalidated on manifest mtime
         change. The legacy comment "manifest is resolved fresh ...
         so a worker restart with a manifest update picks up the
         change" still holds: restart purges the cache, mtime-bump
         purges entries selectively without restart.
      2. Profiling is enabled with the runtime's policy; the workflow
         summary surfaces the profiler's outputs.

    The optional ``setup_done_probe`` is a @DBOS.step the caller may
    pass to mark "deployment context resolved, run_ingest about to
    start". We invoke it inside the workflow body so the
    operation_outputs table records two timestamps (probe + final
    setEvent), letting the bench split A1 into setup + ingest.
    """
    from okg.substrate.runtime.workflow_telemetry import dbos_workflow_trace
    from okg.substrate.source_sync import SourceSyncEvent
    from okg.substrate.deployments.sync_root import (
        require_usable_source_configuration_authority,
    )

    manifest, dsn, dep_dir = _resolve_context_cached(deployment)
    canonical_source_root = require_usable_source_configuration_authority(
        manifest.sync_root,
        command="source sync",
    )
    admission_observed_at = (
        datetime.fromisoformat(observed_at_iso)
        if observed_at_iso else None
    )

    # Reconstruct SourceSyncEvent tuples from their JSON dict form.
    parsed_events: list[SourceSyncEvent] = []
    for raw in (events or []):
        try:
            parsed_events.append(SourceSyncEvent.from_dict(raw))
        except Exception as e:  # noqa: BLE001
            log.warning(
                "source_sync_workflow: dropping malformed event %r: %s",
                raw, e,
            )

    workflow_id = _current_dbos_workflow_id()
    workflow_started_at = datetime.now(timezone.utc)
    with dbos_workflow_trace(
        dsn=dsn,
        deployment_name=deployment,
        workflow_name=SOURCE_SYNC_WORKFLOW_NAME,
        workflow_id=workflow_id,
        queue_name=f"okg-{deployment}-source-sync",
        started_at=workflow_started_at,
        attributes={
            "okg.target.kind": "source_sync",
            "okg.run.mode": run_mode,
            "okg.source_sync.include_dependents": include_dependents,
            "okg.source_sync.include_predecessors": include_predecessors,
            "okg.source_sync.publish": publish,
            "okg.source_sync.event_count": len(parsed_events),
            "okg.source_sync.source_count": len(sources or ()),
        },
        source_sync_trace_handoff=workflow_telemetry,
    ):
        return _run_one_ingest_with_trace(
            manifest=manifest,
            source_root=canonical_source_root,
            dsn=dsn,
            dep_dir=dep_dir,
            admission_observed_at=admission_observed_at,
            parsed_events=parsed_events,
            workflow_id=workflow_id,
            deployment=deployment,
            sources=sources,
            run_mode=run_mode,
            include_dependents=include_dependents,
            dependent_fanout=dependent_fanout,
            publish=publish,
            include_predecessors=include_predecessors,
            profile_force_deep=profile_force_deep,
            profile_collectors=profile_collectors,
            force_enrichers=force_enrichers,
            setup_done_probe=setup_done_probe,
            run_mode_explicit=run_mode_explicit,
        )


def _run_one_ingest_with_trace(
    *,
    manifest: Any,
    source_root: Path | None,
    dsn: str,
    dep_dir: Path,
    admission_observed_at: datetime | None,
    parsed_events: list[Any],
    workflow_id: str | None,
    deployment: str,
    sources: list[str] | None,
    run_mode: str,
    include_dependents: bool,
    dependent_fanout: str | None,
    publish: bool,
    include_predecessors: bool,
    profile_force_deep: bool,
    profile_collectors: tuple[str, ...],
    force_enrichers: bool,
    setup_done_probe: Any,
    run_mode_explicit: bool = False,
) -> dict:
    """Run and classify one ingest while its workflow trace stays bound."""
    from okg.substrate.ingest.service import IngestOptions, run_ingest
    from okg.substrate.source_sync import source_sync_events_by_source

    usefulness = _source_sync_start_usefulness(
        dsn=dsn,
        deployment=deployment,
        workflow_id=workflow_id,
        sources=sources,
    )
    # Operator-forced recompute intent is independent of source-frontier
    # freshness. A newer ordinary source scan may supersede this workflow's
    # scan, but it does not subsume the explicit request to rerun enrichers,
    # gates, and checkpoint creation.
    if not force_enrichers and not usefulness["useful"]:
        log.info(
            "source-sync workflow superseded before source scan: "
            "deployment=%s workflow_id=%s sources=%s reason=%s",
            deployment,
            workflow_id,
            sources if sources else "<all>",
            usefulness["reason"],
        )
        return {
            "all_failed": False,
            "any_failed": False,
            "superseded": True,
            "malformed_event_units": [],
            "report": {
                "status": "superseded",
                "reason": "source_sync_workflow_superseded",
                "source_sync_usefulness": usefulness,
                "results": [],
                "publishes": [],
            },
        }

    options = IngestOptions(
        dsn=dsn,
        deployment=deployment,
        deployment_dir=dep_dir,
        include=tuple(sources) if sources else None,
        include_predecessors=include_predecessors,
        include_dependents=include_dependents,
        dependent_fanout=dependent_fanout,
        source_events=source_sync_events_by_source(
            tuple(parsed_events),
        ),
        run_mode=run_mode,
        run_mode_explicit=run_mode_explicit,
        publish=publish,
        admission_observed_at=admission_observed_at,
        owner_id=f"workflow:source_sync:{deployment}",
        owner_meta={
            "command": "source_sync_workflow",
            "workflow_id": workflow_id,
            "events": [e.to_dict() for e in parsed_events],
            "sources": list(sources or ()),
            "run_mode": run_mode,
            "profile_force_deep": profile_force_deep,
            "profile_collectors": list(profile_collectors),
        },
        profile_force_deep=profile_force_deep,
        force_enrichers=force_enrichers,
        source_registry_repo_root_override=source_root,
        require_declared_sources=True,
        # The source-sync queue runs one sync at a time per deployment, and a
        # command-line run checks that queue before it starts (#3003). Holding
        # the admission-intent lock across a worker run would instead stall
        # every source-sync enqueue (pollers, watchers) for the whole run.
        hold_source_sync_signal=False,
    )
    runtime_sources = tuple(
        sources
        or _source_sync_all_registry_sources(
            dep_dir,
            repo_root_override=source_root,
        )
    )

    log.info(
        "source-sync workflow start: deployment=%s sources=%s "
        "run_mode=%s publish=%s",
        deployment,
        sources if sources else "<all>",
        run_mode, publish,
    )

    # Probe step: marks "setup done, ingest beginning" in
    # operation_outputs. The bench reads the two step rows from
    # operation_outputs to compute (setup_ms, ingest_ms) separately
    # instead of one opaque "A1" bar. Safe no-op if absent (e.g., when
    # called directly outside a registered workflow).
    if setup_done_probe is not None:
        setup_done_probe()

    # `restore-publisher-perf-and-profiling` §B: profile the ingest call.
    from okg.substrate.runtime.profiling_bridge import (
        workflow_profiling_context,
    )
    with workflow_profiling_context(
        manifest=manifest,
        deployment=deployment,
        target_kind="source_sync",
        command="okg_source_sync",
        profile=True if profile_force_deep else None,
        collectors=profile_collectors,
        force_deep=profile_force_deep,
    ) as prof:
        prof.owner_id = options.owner_id
        prof.owner_meta = dict(options.owner_meta)
        prof.source_sync = {
            "sources": list(sources or ()),
            "run_mode": run_mode,
            "event_count": len(parsed_events),
            "include_dependents": include_dependents,
            "include_predecessors": include_predecessors,
            "dependent_fanout": dependent_fanout,
            "publish": publish,
        }
        result = run_ingest(options)
    results = result.report.get("results", [])
    publishes = result.report.get("publishes", [])
    failed_results = [r for r in results if r.get("status") != "OK"]
    failed_publishes = [
        p for p in publishes
        if p.get("status") != "published"
        and not bool(p.get("publish_skipped"))
    ]
    first_err = _first_ingest_failure_error(
        failed_results=failed_results,
        failed_publishes=failed_publishes,
    )
    all_sources_failed = bool(results) and len(failed_results) == len(results)
    publish_failed = bool(failed_publishes)
    silent_failed_result = (
        result.any_failed
        and not failed_results
        and not failed_publishes
    )

    if result.any_failed:
        # Log at ERROR with the failed stage counts and first concrete
        # error so the worker log surfaces the root cause, not just
        # "ended with failures".
        # The bug this caught (2026-05-11): a worker started with
        # `--dsn ...` but missing `OKG_DSN` env var was silently
        # returning SUCCESS while every source died with
        # `missing "=" after "${OKG_DSN}"`. The warning at the
        # original log level was easy to scroll past.
        log.error(
            "source-sync workflow had failures: deployment=%s "
            "failed_sources=%d/%d failed_publishes=%d/%d "
            "first_error=%s",
            deployment,
            len(failed_results), len(results),
            len(failed_publishes), len(publishes),
            (first_err or "")[:300],
        )
    else:
        log.info(
            "source-sync workflow complete: deployment=%s "
            "n_results=%d n_publishes=%d",
            deployment,
            len(results),
            len(result.report.get("publishes", [])),
        )

    # If EVERY source failed, the workflow has produced no useful
    # output — surface this as a workflow ERROR so DBOS marks the
    # row as failed (status='ERROR') instead of SUCCESS with a
    # any_failed=true flag buried in the output payload.
    # Partial failures still return SUCCESS: the workflow did its
    # job for the sources that worked, and the report records the
    # failed ones for an operator to investigate.
    workflow_failure_error: str | None = None
    if all_sources_failed:
        workflow_failure_error = (
            f"source-sync workflow: all {len(results)} sources failed "
            f"for deployment {deployment!r}. First error: "
            f"{(first_err or '')[:300]}"
        )
    elif publish_failed:
        workflow_failure_error = (
            f"source-sync workflow: publish failed for deployment "
            f"{deployment!r}. First error: {(first_err or '')[:300]}"
        )
    elif silent_failed_result:
        workflow_failure_error = (
            f"source-sync workflow: ingest reported failure for deployment "
            f"{deployment!r} but no failed source or publish entry was "
            "present in the report"
        )
    if workflow_failure_error is not None:
        # Carry the structured payload the substrate already computed.
        # Truncating `first_err` to 300 characters used to drop the
        # record-set coverage counts, leaving an operator unable to tell
        # a missing-coverage failure (a cursor reset repairs it) from a
        # stale-coverage failure (a cursor reset does not, and can make
        # a backlogged source worse).
        failure_details = _ingest_failure_details(
            failed_results=failed_results,
            failed_publishes=failed_publishes,
        )
        coverage = failure_details.get("record_set_coverage")
        if coverage:
            workflow_failure_error = (
                f"{workflow_failure_error} "
                f"record_set_coverage: missing_nodes="
                f"{coverage['missing_node_count']} missing_edges="
                f"{coverage['missing_edge_count']} stale_nodes="
                f"{coverage['stale_node_count']} stale_edges="
                f"{coverage['stale_edge_count']} class="
                f"{coverage['coverage_class']} dominant="
                f"{coverage['dominant']}"
            )
        raise SourceSyncWorkflowError(
            workflow_failure_error,
            data={
                "deployment": deployment,
                "workflow_id": workflow_id,
                "failed_source_count": len(failed_results),
                "source_count": len(results),
                "failed_publish_count": len(failed_publishes),
                **failure_details,
            },
        )
    runtime_completion = _source_sync_runtime_payload(
        deployment=deployment,
        workflow_id=workflow_id,
        source_count=len(runtime_sources),
        report=result.report,
    )

    # Return the report + flags as a flat dict so DBOS can serialize
    # cleanly. The full IngestRunResult dataclass isn't JSON-safe.
    # `all_failed` is always False here (otherwise we raised above);
    # exposing it is part of the fix-substrate-silent-failures
    # convention so operators / tooling can scan SUCCESS workflow
    # outputs for the `all_failed: true` invariant violation.
    return {
        "all_failed": False,
        "any_failed": result.any_failed,
        "superseded": False,
        "malformed_event_units": [],
        "report": result.report,
        "source_sync_runtime": runtime_completion,
    }


def _source_sync_all_registry_sources(
    dep_dir: Path,
    *,
    repo_root_override: Path | None = None,
) -> list[str]:
    from okg.substrate.sources.registry import load_source_registry

    registry = load_source_registry(
        dep_dir / "source_registry.yaml",
        **(
            {"repo_root_override": repo_root_override}
            if repo_root_override is not None
            else {}
        ),
    )
    return sorted(registry)


def _source_sync_runtime_payload(
    *,
    deployment: str,
    workflow_id: str | None,
    source_count: int,
    report: Mapping[str, Any],
) -> dict[str, Any]:
    results = report.get("results", [])
    publishes = report.get("publishes", [])
    return {
        "schema": "okg.source_sync.runtime.v1",
        "status": "completed",
        "deployment": deployment,
        "dbos_workflow_id": workflow_id,
        "authority": "dbos.workflow_status",
        "source_progress_authority": "okg.source_sync_progress_and_watermarks",
        "source_count": source_count,
        "result_count": len(results) if isinstance(results, list) else None,
        "publish_count": len(publishes) if isinstance(publishes, list) else None,
    }


def _source_sync_operational_outcome(result: Mapping[str, Any]) -> dict[str, Any]:
    """Map a successful ingest-cycle result to stable typed evidence."""
    from okg.substrate.runtime.workflow_operational_outcomes import (
        WorkflowOperationalEvidenceError,
    )
    if not isinstance(result, Mapping):
        raise WorkflowOperationalEvidenceError("source-sync result must be a mapping")
    if type(result.get("any_failed")) is not bool or type(result.get("superseded")) is not bool:
        raise WorkflowOperationalEvidenceError("source-sync result lacks boolean failure flags")
    report = result.get("report")
    if not isinstance(report, Mapping):
        raise WorkflowOperationalEvidenceError(
            "source-sync result requires a report mapping"
        )
    source_results = report.get("results")
    publishes = report.get("publishes")
    if not isinstance(source_results, list) or not isinstance(publishes, list):
        raise WorkflowOperationalEvidenceError(
            "source-sync report requires results and publishes lists"
        )
    normalized_results: list[tuple[str, Mapping[str, Any]]] = []
    for item in source_results:
        if not isinstance(item, Mapping):
            raise WorkflowOperationalEvidenceError(
                "source-sync results entries must be mappings"
            )
        source = str(item.get("source") or item.get("name") or "").strip()
        if not source:
            raise WorkflowOperationalEvidenceError(
                "source-sync result entry requires a source name"
            )
        normalized_results.append((source, item))
    succeeded_sources = sorted(
        source for source, item in normalized_results if item.get("status") == "OK"
    )
    failed_units: list[str] = []
    failure_details: list[dict[str, Any]] = []
    for source, item in normalized_results:
        if item.get("status") == "OK":
            continue
        code = str(item.get("code") or "source_failure")
        failed_units.append(f"{source}|{code}")
        failure_details.append({"source": source, "code": code, "error": str(item.get("error") or "")[:300]})
    for item in publishes:
        if not isinstance(item, Mapping):
            raise WorkflowOperationalEvidenceError(
                "source-sync publishes entries must be mappings"
            )
        if item.get("status") == "published" or item.get("publish_skipped"):
            continue
        failed_units.append(f"publish|{item.get('status') or 'failure'}")
        failure_details.append({"stage": "publish", "status": item.get("status"), "error": str(item.get("error") or "")[:300]})
    failed_units = sorted(set(failed_units))
    malformed_units = result.get("malformed_event_units")
    if not isinstance(malformed_units, list) or any(
        not isinstance(value, str) or not value for value in malformed_units
    ):
        raise WorkflowOperationalEvidenceError(
            "malformed_event_units must be a list of non-empty strings"
        )
    failed_units.extend(f"event|{value}" for value in malformed_units)
    failed_units = sorted(set(failed_units))
    if result["superseded"] and not failed_units:
        outcome = "skipped"
    elif failed_units and succeeded_sources:
        outcome = "partial_failure"
    elif failed_units:
        outcome = "retained_failure"
    else:
        outcome = "clean"
    if bool(result["any_failed"]) != bool(failed_units):
        raise WorkflowOperationalEvidenceError("source-sync failure flag disagrees with report")
    return {
        "outcome": outcome,
        "succeeded_count": len(succeeded_sources),
        "failed_count": len(failed_units),
        "semantic_units": {
            "succeeded_sources": succeeded_sources,
            "failed_units": failed_units,
        },
        "details": {"failures": failure_details[:10]},
    }


def _current_dbos_workflow_id() -> str | None:
    try:
        from dbos import DBOS
        workflow_id_attr = getattr(DBOS, "workflow_id", None)
        workflow_id = (
            workflow_id_attr()
            if callable(workflow_id_attr) else workflow_id_attr
        )
    except Exception:  # noqa: BLE001
        return None
    return workflow_id if isinstance(workflow_id, str) and workflow_id else None


def _source_sync_start_usefulness(
    *,
    dsn: str,
    deployment: str,
    workflow_id: str | None,
    sources: list[str] | None,
) -> dict[str, Any]:
    """Return whether the starting workflow should still scan sources.

    This is deliberately conservative. A workflow exits only when the active
    frontier projection can see this workflow for every requested source and
    has selected a different workflow as the useful frontier for every one of
    them. Missing DBOS state, an all-sources run, or any source still selecting
    this workflow keeps the scan live.
    """
    requested_sources = [str(source) for source in (sources or ()) if source]
    if not workflow_id:
        return {
            "useful": True,
            "reason": "no_dbos_workflow_context",
            "workflow_id": workflow_id,
            "sources": requested_sources,
        }
    if not requested_sources:
        return {
            "useful": True,
            "reason": "all_sources_workflow_not_bounded",
            "workflow_id": workflow_id,
            "sources": requested_sources,
        }

    try:
        from okg.substrate.source_sync_enqueue import (
            active_dbos_source_sync_workflows_by_source,
        )
        active_by_source = active_dbos_source_sync_workflows_by_source(
            dsn=dsn,
            deployment=deployment,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "source-sync start usefulness check failed; running workflow: %s",
            exc,
            exc_info=True,
        )
        return {
            "useful": True,
            "reason": "active_frontier_lookup_failed",
            "workflow_id": workflow_id,
            "sources": requested_sources,
            "error": str(exc),
        }

    decisions: list[dict[str, Any]] = []
    for source_name in requested_sources:
        active = active_by_source.get(source_name)
        if not active:
            decisions.append({
                "source_name": source_name,
                "useful": True,
                "reason": "source_has_no_active_frontier_record",
            })
            continue
        selected_workflow_id = str(active.get("workflow_id") or "")
        candidate_ids = {
            str(candidate_id)
            for candidate_id in (
                active.get("candidate_workflow_ids") or ()
            )
        }
        if workflow_id not in candidate_ids and selected_workflow_id != workflow_id:
            decisions.append({
                "source_name": source_name,
                "useful": True,
                "reason": "workflow_not_visible_in_active_candidates",
                "selected_workflow_id": selected_workflow_id,
                "candidate_workflow_ids": sorted(candidate_ids),
            })
            continue
        selected = selected_workflow_id == workflow_id
        decisions.append({
            "source_name": source_name,
            "useful": selected,
            "reason": (
                "selected_frontier"
                if selected else "superseded_by_selected_frontier"
            ),
            "selected_workflow_id": selected_workflow_id,
            "candidate_workflow_ids": sorted(candidate_ids),
            "selection_reason": active.get("selection_reason"),
            "desired_frontier": dict(active.get("desired_frontier") or {}),
        })

    useful = any(bool(decision["useful"]) for decision in decisions)
    return {
        "useful": useful,
        "reason": (
            "at_least_one_source_still_selects_workflow"
            if useful else "all_sources_superseded_by_selected_frontiers"
        ),
        "workflow_id": workflow_id,
        "sources": requested_sources,
        "decisions": decisions,
    }


class SourceSyncWorkflowError(RuntimeError):
    """A source-sync workflow failure that keeps its structured payload.

    The workflow used to raise a bare ``RuntimeError`` whose only content
    was a 300-character slice of the first error string. That threw away
    the structured payload the substrate had already computed — most
    painfully the record-set coverage counts, where ``missing_*`` and
    ``stale_*`` call for opposite repairs and the operator could no
    longer tell which one they were looking at. Subclassing
    ``RuntimeError`` keeps every existing catch site and
    ``pytest.raises(RuntimeError)`` working.
    """

    def __init__(self, message: str, *, data: dict[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.data = data or {}

    def __reduce__(self) -> tuple[Any, ...]:
        # DBOS stores terminal errors as pickles. The default
        # `Exception.__reduce__` replays only `args`, which would drop
        # `data` on the way back out of `dbos.workflow_status` — the same
        # loss this class exists to prevent.
        return (_rebuild_source_sync_workflow_error, (self.message, self.data))


def _rebuild_source_sync_workflow_error(
    message: str,
    data: dict[str, Any],
) -> SourceSyncWorkflowError:
    return SourceSyncWorkflowError(message, data=data)


_RECORD_SET_COVERAGE_RE = re.compile(
    r"\[record_set_coverage\s+"
    r"missing_nodes=(?P<missing_nodes>\d+)\s+"
    r"missing_edges=(?P<missing_edges>\d+)\s+"
    r"stale_nodes=(?P<stale_nodes>\d+)\s+"
    r"stale_edges=(?P<stale_edges>\d+)\s+"
    r"class=(?P<coverage_class>[a-z_]+)\s+"
    r"dominant=(?P<dominant>[a-z_]+)\]"
)


def _record_set_coverage_fields(error_text: str) -> dict[str, Any]:
    """Lift the record-set coverage counts back out of an error string.

    ``run_ingest`` flattens a ``SubstrateError`` to ``f"{type}: {exc}"``
    when it records a failed source in the report, so the ``data=``
    payload is gone by the time this module sees it. The runner writes
    the counts into the message text as a stable marker
    (``okg.substrate.ingest.runner.record_set_coverage_marker``); this
    reads them back so the workflow error carries structured counts
    instead of a truncated sentence.
    """
    match = _RECORD_SET_COVERAGE_RE.search(error_text or "")
    if match is None:
        return {}
    fields = match.groupdict()
    return {
        "missing_node_count": int(fields["missing_nodes"]),
        "missing_edge_count": int(fields["missing_edges"]),
        "stale_node_count": int(fields["stale_nodes"]),
        "stale_edge_count": int(fields["stale_edges"]),
        "coverage_class": fields["coverage_class"],
        "dominant": fields["dominant"],
    }


def _ingest_failure_details(
    *,
    failed_results: list[dict[str, Any]],
    failed_publishes: list[dict[str, Any]],
) -> dict[str, Any]:
    """Collect the structured payload behind the first ingest failure.

    Preserves whatever the report already carries (``error_kind``,
    ``error_diagnosis``, and any ``error_code`` / ``error_data`` /
    ``error_fix_hint`` a caller adds later) plus the record-set coverage
    counts parsed out of the error text.
    """
    details: dict[str, Any] = {}
    for result in failed_results:
        error = str(result.get("error") or result.get("code") or "")
        if not error:
            continue
        details["failed_source"] = result.get("name")
        details["failed_stage"] = result.get("stage")
        for key in (
            "error_kind",
            "error_code",
            "error_data",
            "error_fix_hint",
            "error_diagnosis",
        ):
            if result.get(key):
                details[key] = result[key]
        coverage = _record_set_coverage_fields(error)
        if coverage:
            details["record_set_coverage"] = coverage
        break
    if not details:
        for publish in failed_publishes:
            error = str(
                publish.get("error")
                or publish.get("code")
                or publish.get("reason")
                or publish.get("blocked_reason")
                or ""
            )
            details["failed_publish"] = publish.get("label")
            details["publish_status"] = publish.get("status")
            coverage = _record_set_coverage_fields(error)
            if coverage:
                details["record_set_coverage"] = coverage
            break
    return {k: v for k, v in details.items() if v not in (None, "", {})}


def _first_ingest_failure_error(
    *,
    failed_results: list[dict[str, Any]],
    failed_publishes: list[dict[str, Any]],
) -> str:
    """Return a stable, operator-facing failure summary."""
    for result in failed_results:
        error = result.get("error")
        if error:
            return str(error)
        code = result.get("code")
        if code:
            return str(code)
    for publish in failed_publishes:
        label = publish.get("label") or "<unknown>"
        status = publish.get("status") or "<missing-status>"
        error = (
            publish.get("error")
            or publish.get("code")
            or publish.get("reason")
            or publish.get("blocked_reason")
        )
        if error:
            return f"publish {label} status={status}: {error}"
        return f"publish {label} status={status}"
    return "(no error message)"
