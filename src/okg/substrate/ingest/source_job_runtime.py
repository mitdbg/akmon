"""The parent side of one-process-per-source ingest (lane H stage 1).

``ingest/source_jobs.py`` holds the coordinator and the child entry point;
``ingest/service.py`` holds the per-source work (``_run_source``). This module
holds what the parent does around each child, moved out of the service so it
stays within its line budget:

* the production runtime (:class:`ProcessSourceJobRuntime`) that prepares a
  source's admission in the parent and starts its ``spawn`` child;
* the child's start payload (:func:`source_child_payload`), which carries only
  plain, picklable values -- the child rebuilds the registry entry from them;
* the failure results and the compensation a crashed child needs
  (:func:`fail_source_job`);
* the parent-side bookkeeping after a child finishes (spill merge, profile
  stage, progress rows, recorded peaks).

Service helpers are looked up on the service module at call time, never
imported by name: the service imports this module, and tests that replace a
service helper must see their replacement used here too.
"""
from __future__ import annotations

import logging
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any, Iterator, Mapping

from okg.substrate.errors import (
    SourceConnectionBudgetExhausted,
    SourceMemoryLimitExceeded,
    SourceProcessBindingChanged,
    SubstrateError,
)
from okg.substrate.ingest import source_jobs
from okg.substrate.ingest.published_completeness import (
    UNDO_COMPLETED,
    UNDO_NOTHING_WRITTEN,
    UNDO_UNRESOLVED,
)
from okg.substrate.ingest.runner import SourceRunContext
from okg.substrate.runtime.source_sync_priority import run_guard_handoff
from okg.substrate.sources.registry import (
    STRICT_ADMISSION_CONTRACT,
    SourceEntry,
    source_entry_description,
    source_entry_from_description,
)

if TYPE_CHECKING:
    from okg.substrate.ingest.adapter_factory import ConstructedSourceAdapter
    from okg.substrate.ingest.planning import IngestPlan
    from okg.substrate.ingest.service import IngestOptions

log = logging.getLogger(__name__)


def _svc() -> ModuleType:
    from okg.substrate.ingest import service

    return service


def source_child_payload(
    options: IngestOptions,
    entry: SourceEntry,
    name: str,
    prepared: PreparedSourceRun,
    *,
    spill_path: Path,
    lock_wait_seconds: float,
) -> dict[str, Any]:
    """The start payload of one source child: plain, picklable values only.

    A ``spawn`` child receives its arguments pickled. A strict registry entry
    is frozen into read-only mapping views that cannot be pickled, so the
    entry travels as :func:`source_entry_description` and the child rebuilds
    it. The rebuild is checked here, before any child starts: an entry that
    would not come back equal is refused by name instead of running the child
    on a different entry.
    """
    description = source_entry_description(entry)
    if source_entry_from_description(description) != entry:
        raise SubstrateError(
            "source child not started: its registry entry does not rebuild "
            "equal from the description the child receives",
            data={"source": name, "ownership_id": entry.ownership_id},
        )
    from okg.substrate import deployments

    # The command's folder-resolved deployments (#2951) live in a context
    # variable, which a ``spawn`` child does not inherit; the child binds
    # these again (`source_jobs.prepare_child_process`). None: no scope.
    bindings = deployments._RESOLVED_DEPLOYMENT_BINDINGS.get()
    identity = _svc().current_trace_identity()
    return {
        "name": name,
        "entry": description,
        "prepared": prepared,
        "options": {
            "dsn": options.dsn,
            "deployment": options.deployment,
            "deployment_dir": Path(options.deployment_dir),
            "reset_cursor": options.reset_cursor,
            "run_mode": options.run_mode,
            "publish": options.publish,
            # Issue #2818 backstop: the child's runner applies the operator's
            # `--allow-full-retraction` and records who asked for it.
            "allow_full_retraction": tuple(
                str(source) for source in options.allow_full_retraction
            ),
            "owner_id": options.owner_id,
        },
        "deployment_bindings": None if bindings is None else {
            bound_name: str(bound_dir) for bound_name, bound_dir in bindings.items()
        },
        "spill_path": str(spill_path),
        "lock_wait_seconds": lock_wait_seconds,
        # #3003: the child checks this run's source-sync hold before each
        # chunk (a spawn child does not inherit the guard's context).
        "run_guard": run_guard_handoff(options.deployment),
        "trace": {
            "trace_id": identity.trace_id,
            "span_id": identity.span_id,
            "parent_span_id": identity.parent_span_id,
        },
    }


@dataclass(frozen=True)
class PreparedSourceRun:
    """Admission state the parent creates before a source child starts.

    The parent allocates the run identity and the pending admission envelope
    (it holds the DBOS workflow context the envelope's replay key comes
    from), so it can still compensate and fail the run when the child dies
    without reporting.
    """

    run_mode: str
    sync_scope: dict[str, Any] | None
    run_context: SourceRunContext
    workflow_ref: dict[str, Any]
    envelope_status: str
    #: The binding fingerprint the parent admitted for a strict source; the
    #: child refuses to run an adapter it rebuilt to a different binding.
    strict_fingerprint: str | None
    timings_ms: Mapping[str, float]


def prepare_source_run(
    options: IngestOptions,
    entry: SourceEntry,
    name: str,
) -> PreparedSourceRun:
    """Resolve the run mode and create the source's pending admission."""
    started = time.perf_counter()
    events = tuple(options.source_events.get(name, ()))
    sync_scope = _svc().source_sync_scope_payload(name, events)
    run_mode = _svc()._run_mode_for_source(
        options,
        entry,
        name,
        events,
        sync_scope,
    )
    timings_ms: dict[str, float] = {}
    strict_fingerprint: str | None = None
    if getattr(entry, "admission_contract", None) == STRICT_ADMISSION_CONTRACT:
        strict_binding = options.constructed_source_bindings.get(name)
        if strict_binding is None:
            raise SubstrateError(
                "strict source has no frozen document binding",
                data={"source": name},
            )
        strict_fingerprint = strict_binding.registry_binding_fingerprint
        timings_ms["construct"] = round((time.perf_counter() - started) * 1000, 3)
    admission_started = time.perf_counter()
    # Strict construction and its post-profile check completed for every
    # selected source before any source started. Only now may
    # producer/envelope admission or database state be created.
    run_context, workflow_ref, envelope_status = _svc()._prepare_source_admission(
        options=options,
        entry=entry,
        source_registry_name=name,
        run_mode=run_mode,
        sync_scope=sync_scope,
    )
    timings_ms["admission_prepare"] = round(
        (time.perf_counter() - admission_started) * 1000, 3
    )
    return PreparedSourceRun(
        run_mode=run_mode,
        sync_scope=sync_scope,
        run_context=run_context,
        workflow_ref=workflow_ref,
        envelope_status=envelope_status,
        strict_fingerprint=strict_fingerprint,
        timings_ms=timings_ms,
    )


def rebuild_strict_binding(
    options: IngestOptions,
    entry: SourceEntry,
    name: str,
    parent_fingerprint: str | None,
) -> ConstructedSourceAdapter:
    """Construct a strict adapter in the source child from its registry entry.

    An adapter object cannot cross a process boundary, so the child plans and
    constructs its own. It must bind to exactly what the parent admitted.
    """
    from okg.substrate.ingest.adapter_factory import plan_source_adapter

    plan = plan_source_adapter(
        entry,
        dsn=options.dsn,
        deployment=options.deployment,
    )
    if plan.registry_binding_fingerprint != parent_fingerprint:
        raise SourceProcessBindingChanged(
            "the source child resolved a different adapter binding than the "
            "one admitted for this step",
            data={
                "source": name,
                "parent_fingerprint": parent_fingerprint,
                "child_fingerprint": plan.registry_binding_fingerprint,
            },
            fix_hint=(
                "the adapter code or registry changed while the step ran; "
                "re-run the source so parent and child load the same binding"
            ),
        )
    return _svc().construct_source_adapter_from_plan(plan)


class ProcessSourceJobRuntime:
    """Production environment of the source-job coordinator.

    Each launch prepares the source's admission here, in the parent, then
    starts a ``spawn`` child that runs :func:`_run_source`.
    """

    def __init__(
        self,
        options: IngestOptions,
        plan: IngestPlan,
        step_dir: Path,
    ) -> None:
        self._options = options
        self._plan = plan
        self._step_dir = step_dir
        self._lock_wait_seconds = source_jobs.run_lock_wait_seconds()
        self._budget = source_jobs.memory_budget_bytes()
        self._peaks = recorded_source_peaks(options, plan)

    def _entry(self, name: str) -> SourceEntry:
        return self._plan.runnable[name]

    def launch(self, name: str) -> Any:
        entry = self._entry(name)
        started = time.time()
        try:
            drain_parent_schema_index_jobs()
            prepared = prepare_source_run(self._options, entry, name)
        except Exception as exc:  # noqa: BLE001 - reported as this source's failure
            return source_prepare_failure(name, entry, exc, started)
        try:
            payload = source_child_payload(
                self._options,
                entry,
                name,
                prepared,
                spill_path=self._step_dir / f"{uuid.uuid4().hex}.jsonl",
                lock_wait_seconds=self._lock_wait_seconds,
            )
            return source_jobs.SpawnedSourceJob(
                name=name,
                payload=payload,
                memory_limit=self.memory_limit(name),
                on_crash=lambda job: fail_source_job(
                    self._options,
                    entry,
                    name,
                    prepared,
                    job=job,
                    started=started,
                ),
            )
        except Exception as exc:  # noqa: BLE001 - the child never started
            return fail_source_job(
                self._options,
                entry,
                name,
                prepared,
                job=None,
                started=started,
                start_error=exc,
            )

    def connection_state(self) -> tuple[int, int]:
        from okg.substrate.db.pool import GENERAL, pooled_connection

        with pooled_connection(self._options.dsn, purpose=GENERAL) as conn:
            row = conn.execute(
                "SELECT count(*) FILTER (WHERE backend_type = 'client backend'),"
                "       current_setting('max_connections')::int"
                "  FROM pg_stat_activity"
            ).fetchone()
        return int(row[0]), int(row[1])

    def recorded_peak(self, name: str) -> int | None:
        return self._peaks.get(name)

    def memory_limit(self, name: str) -> int:
        return source_jobs.memory_limit_bytes()

    def is_heavy(self, name: str) -> bool:
        return source_jobs.is_heavy_source(name, self._entry(name).ownership_id)

    def sample_memory(self, pid: int) -> int | None:
        return source_jobs.process_footprint_bytes(pid)

    def memory_budget(self) -> int:
        return self._budget

    def start_failed(self, name: str, *, stage: str, error: str) -> dict[str, Any]:
        return source_start_failed_result(name, stage=stage, error=error)

    def connection_budget_exhausted(
        self,
        name: str,
        *,
        live: int,
        needed: int,
        allowed: int,
        waited: float,
    ) -> dict[str, Any]:
        return connection_budget_exhausted_result(
            name, live=live, needed=needed, allowed=allowed, waited=waited,
        )


def source_start_failed_result(
    name: str,
    *,
    stage: str,
    error: str,
) -> dict[str, Any]:
    """The failed result of a source whose start check kept raising.

    No child ran and no admission was prepared, so there is nothing to
    compensate; the other sources are unaffected.
    """
    return {
        "name": name,
        "status": "FAIL",
        "stage": stage,
        "error": f"source child not started: {error}",
        "undo_status": UNDO_NOTHING_WRITTEN,
        "elapsed_s": 0.0,
        "timing_status": "partial",
        "timings_ms": {},
    }


def connection_budget_exhausted_result(
    name: str,
    *,
    live: int,
    needed: int,
    allowed: int,
    waited: float,
) -> dict[str, Any]:
    """The failed result of a source whose child never got its connections."""
    err = SourceConnectionBudgetExhausted(
        "source child not started: database connections stayed over "
        "budget for the whole wait",
        data={
            "source": name,
            "live": live,
            "needed": needed,
            "allowed": allowed,
            "waited_seconds": round(waited, 3),
        },
        fix_hint=(
            "stop idle database clients or raise max_connections, then "
            "re-run the source"
        ),
    )
    return {
        "name": name,
        "status": "FAIL",
        "stage": "connection_admission",
        "undo_status": UNDO_NOTHING_WRITTEN,
        "code": err.code,
        "error": f"{type(err).__name__}: {err}",
        "error_data": dict(err.data),
        "fix_hint": err.fix_hint,
        "elapsed_s": round(waited, 3),
        "timing_status": "partial",
        "timings_ms": {},
    }


@contextmanager
def open_source_job_runtime(
    options: IngestOptions,
    plan: IngestPlan,
) -> Iterator[Any]:
    """The coordinator's environment for one source-sync step.

    The one production runtime starts a child process per source. Tests that
    replace ``_run_source`` with a stub replace this seam with an in-process
    runtime, because a stub cannot cross into a spawned interpreter.
    """
    with source_jobs.source_job_step_dir() as step_dir:
        yield ProcessSourceJobRuntime(options, plan, step_dir)


def recorded_source_peaks(
    options: IngestOptions,
    plan: IngestPlan,
) -> dict[str, int]:
    """Last recorded child peak memory per registry name."""
    by_owner = {
        entry.ownership_id: name
        for name, entry in plan.runnable.items()
        if entry.ownership_id
    }
    if not by_owner:
        return {}
    from okg.substrate.db.pool import GENERAL, pooled_connection

    with pooled_connection(options.dsn, purpose=GENERAL) as conn:
        rows = conn.execute(
            "SELECT source_name, value FROM okg.source_progress"
            " WHERE deployment = %s AND key = %s AND source_name = ANY(%s)",
            (
                options.deployment,
                source_jobs.PEAK_MEMORY_PROGRESS_KEY,
                list(by_owner),
            ),
        ).fetchall()
    peaks: dict[str, int] = {}
    for source_name, value in rows:
        try:
            peak = int(value)
        except (TypeError, ValueError):
            log.warning(
                "ignoring malformed recorded peak %r for source %s; it "
                "counts at its full memory limit",
                value,
                source_name,
            )
            continue
        if peak > 0:
            peaks[by_owner[source_name]] = peak
    return peaks


def drain_parent_schema_index_jobs() -> None:
    """Finish this process's asynchronous schema-index work before a spawn.

    Children create any index work left inline
    (``CHRONOS_ASYNC_SCHEMA_INDEXES=0``); nothing the parent queued may still
    be building when a child starts writing.
    """
    from chronos_core.branching._interval_backend import (
        _wait_for_all_async_schema_index_jobs,
    )

    _wait_for_all_async_schema_index_jobs()


def source_prepare_failure(
    name: str,
    entry: SourceEntry,
    exc: Exception,
    started: float,
) -> dict[str, Any]:
    """The failed result of a source whose admission could not be prepared.

    Mirrors ``_run_source``'s failure record for its admission stage: no run
    identity exists yet, so there is nothing to compensate.
    """
    classification = _svc().classify_source_construction_failure(
        exc=exc,
        stage="admission",
        module_name=str(entry.raw.get("module") or ""),
        class_name=str(entry.raw.get("class") or ""),
        params={},
    )
    error_text = (
        str(classification["message"])
        if classification is not None
        else f"{type(exc).__name__}: {exc}"
    )
    return {
        "name": name,
        "status": "FAIL",
        "stage": "admission",
        "error": error_text,
        "undo_status": UNDO_NOTHING_WRITTEN,
        "elapsed_s": round(time.time() - started, 3),
        "timing_status": "partial",
        "timings_ms": {},
        **(
            {
                "error_kind": classification["error_kind"],
                "error_diagnosis": classification,
            }
            if classification is not None
            else {}
        ),
        **(
            {"error_data": dict(exc.data)}
            if isinstance(exc, SubstrateError) and exc.data
            else {}
        ),
        **(
            {"fix_hint": str(exc.fix_hint)}
            if isinstance(exc, SubstrateError) and exc.fix_hint
            else {}
        ),
    }


def fail_source_job(
    options: IngestOptions,
    entry: SourceEntry,
    name: str,
    prepared: PreparedSourceRun,
    *,
    job: Any,
    started: float,
    start_error: Exception | None = None,
) -> dict[str, Any]:
    """Fail a source whose child ended without a result, compensation included.

    This is the failure path ``_run_source`` runs for its own exceptions,
    run from the parent: retract the run's graph writes, restore the source's
    pre-run state when the child reported its snapshot, and mark the
    admission failed.
    """
    code: str | None = None
    error_data: dict[str, Any] = {}
    started_payload = (getattr(job, "started", None) or {}) if job is not None else {}
    if start_error is not None:
        stage = "child_start"
        error_text = (
            "source child could not start: "
            f"{type(start_error).__name__}: {start_error}"
        )
    elif job is not None and getattr(job, "abort_reason", None):
        stage = "child_exit"
        error_text = (
            "source child stopped because the source-job coordinator "
            f"failed: {job.abort_reason}"
        )
        error_data = dict(job.describe_exit())
    elif job is not None and job.memory_kill is not None:
        stage = "child_exit"
        limit = int(job.memory_kill["limit_bytes"])
        hint = (
            f"raise {source_jobs.SOURCE_MEMORY_LIMIT_ENV} (whole bytes, "
            "one limit for all sources) or narrow this source's scope, "
            "then re-run the source"
        )
        err = SourceMemoryLimitExceeded(
            "source child killed: memory over its limit of "
            f"{limit / 1024 ** 3:.1f} GiB ({limit} bytes); {hint}",
            data={"source": name, **job.memory_kill},
            fix_hint=hint,
        )
        code = err.code
        error_text = f"{type(err).__name__}: {err}"
        error_data = {**dict(err.data), **job.describe_exit()}
    else:
        stage = "child_exit"
        exit_info = job.describe_exit() if job is not None else {}
        error_data = dict(exit_info)
        if job is not None and job.result is None and exit_info.get("exit_code") == 0:
            error_text = "source child exited without reporting a result"
        elif "signal" in exit_info:
            error_text = f"source child killed by signal {exit_info['signal']}"
            if exit_info["signal"] == "SIGKILL" and sys.platform == "darwin":
                # No memory kill was recorded (that branch is above), so the
                # kernel or another process killed it. macOS writes a crash
                # report for a kernel kill such as a guarded-descriptor
                # violation.
                error_text = (
                    f"{error_text} (no memory kill was recorded; look for a "
                    "crash report from this time in "
                    "~/Library/Logs/DiagnosticReports)"
                )
        else:
            error_text = (
                f"source child exited with status {exit_info.get('exit_code')}"
            )
        if exit_info.get("child_error"):
            error_text = f"{error_text}: {exit_info['child_error']}"
    run_context = prepared.run_context
    recovery_blocked = False
    try:
        _svc()._retract_chronos_run(
            options.dsn,
            admission_id=run_context.admission_id,
            source=entry.ownership_id,
            run_id=run_context.run_id,
            branch=str(
                started_payload.get("graph_branch")
                or _svc().source_adapter_graph_branch(entry)
            ),
        )
    except Exception as compensation_error:  # noqa: BLE001
        recovery_blocked = True
        error_data = {
            "original_error": error_text,
            "compensation_error": (
                f"{type(compensation_error).__name__}: {compensation_error}"
            ),
            "original_error_data": error_data,
        }
        error_text = "SubstrateError: source failure compensation is unresolved"
    snapshot = started_payload.get("source_state_snapshot")
    if snapshot is not None:
        try:
            _svc()._restore_source_state(
                options.dsn,
                snapshot,
                expected_replay_run_id=(
                    run_context.run_id
                    if started_payload.get("managed_change_identity")
                    else None
                ),
            )
        except Exception as restore_error:  # noqa: BLE001
            recovery_blocked = True
            error_data = {
                "original_error": error_text,
                "restore_error": f"{type(restore_error).__name__}: {restore_error}",
                "original_error_data": error_data,
            }
            error_text = "SubstrateError: source failure state restoration is unresolved"
    if (
        prepared.envelope_status not in {"published", "blocked"}
        and not recovery_blocked
    ):
        _svc()._fail_source_admission(
            dsn=options.dsn,
            admission_id=run_context.admission_id,
            error=error_text,
            workflow_ref=prepared.workflow_ref,
        )
    return {
        "name": name,
        "status": "FAIL",
        "stage": stage,
        "error": error_text,
        **({"code": code} if code is not None and not recovery_blocked else {}),
        "elapsed_s": round(time.time() - started, 3),
        "timing_status": "partial",
        "timings_ms": dict(prepared.timings_ms),
        **({"error_data": error_data} if error_data else {}),
        "admission_id": run_context.admission_id,
        "source_state_restored": snapshot is not None and not recovery_blocked,
        "undo_status": UNDO_UNRESOLVED if recovery_blocked else UNDO_COMPLETED,
    }


def merge_source_job_spill(result: dict[str, Any]) -> None:
    """Put a child's spilled write-set back on its result.

    A missing or unreadable spill file leaves the write-set off the result.
    The publisher then has no hint for this source and takes its full-scope
    path -- a spill that cannot be read is never taken to mean "no work".
    """
    spill = result.get("spill")
    if not isinstance(spill, dict) or spill.get("status") != "written":
        return
    path = Path(str(spill.get("path")))
    try:
        payload = source_jobs.read_spill(path)
    except source_jobs.SpillUnreadable as exc:
        log.error(
            "source %s: spill file unreadable; its publish uses no dirty-scope "
            "hint: %s",
            result.get("name"),
            exc,
        )
        result["spill"] = {
            "status": "unreadable",
            "error": str(exc),
            "keys": list(spill.get("keys") or ()),
        }
        return
    for key, value in payload.items():
        if key in source_jobs.SPILLED_RESULT_KEYS:
            result[key] = value
    result["spill"] = {
        "status": "merged",
        "keys": sorted(payload),
        "lines": spill.get("lines"),
    }


def record_source_job_result(
    *,
    options: IngestOptions,
    entry: SourceEntry,
    name: str,
    result: dict[str, Any],
    started_at: float | None,
    finished_at: float | None,
    profile_ctx: Any,
) -> None:
    """Parent-side bookkeeping for one finished source job.

    Records the per-source profile stage and the child's query count on this
    run's profile, the stage-timing and record-set-coverage progress rows,
    and the child's peak memory for the next run's memory admission.
    """
    input_count = len(tuple(options.source_events.get(name, ())))
    stage_attrs: dict[str, Any] = {
        "source_name": name,
        "ownership_id": entry.ownership_id,
        "deployment": options.deployment,
        "input_count": input_count,
    }
    _svc()._annotate_source_stage_attrs(stage_attrs, result)
    process = result.get("source_process")
    if profile_ctx is not None:
        if isinstance(process, dict) and isinstance(process.get("query_count"), int):
            profile_ctx.query_count += int(process["query_count"])
        if (
            started_at is not None
            and finished_at is not None
            and (profile_ctx.policy.lightweight.enabled or profile_ctx.collect_detail)
        ):
            profile_ctx.record_stage(
                f"source.{name}.run",
                int(started_at * 1_000_000_000),
                int(max(0.0, finished_at - started_at) * 1_000_000_000),
                None,
                stage_attrs,
            )
    _svc()._upsert_source_stage_timing_progress(
        options=options,
        entry=entry,
        source_registry_name=name,
        result=result,
        input_count=input_count,
    )
    _svc()._upsert_source_record_set_coverage_state(
        options=options,
        entry=entry,
        result=result,
    )
    process = process if isinstance(process, dict) else {}
    progress: dict[str, str] = {}
    peak = process.get("peak_memory_bytes")
    if isinstance(peak, int) and peak > 0:
        progress[source_jobs.PEAK_MEMORY_PROGRESS_KEY] = str(peak)
    peak_connections = process.get("peak_connections")
    if isinstance(peak_connections, int) and peak_connections > 0:
        progress[source_jobs.PEAK_CONNECTIONS_PROGRESS_KEY] = str(peak_connections)
    if progress and entry.ownership_id:
        try:
            _svc().upsert_source_progress(
                dsn=options.dsn,
                deployment=options.deployment,
                source_name=entry.ownership_id,
                progress=progress,
            )
        except Exception:  # noqa: BLE001 - the next run counts it at its limit
            log.exception(
                "failed to record child peaks for source=%s", entry.ownership_id
            )
