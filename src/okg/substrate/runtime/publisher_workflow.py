"""Publisher.cycle as a DBOS workflow (migrate-substrate-runtime-to-dbos Phase 1).

Replaces the legacy ``okg.jobs`` ``publish`` job-kind dispatch with a
``@DBOS.workflow``-decorated wrapper around ``Publisher.cycle()``. The
workflow runs on the deployment's existing partition queue
(``okg-<deployment>``, concurrency=1); the queue's concurrency=1 is
the new mutual-exclusion mechanism, replacing
``coordination/lease.py``.

The workflow body is intentionally thin: build the deployment's
publisher stack (existing helper), construct a ``Publisher`` with
``coordination_mode='dbos'`` so it skips the lease, and call
``cycle()``. The cycle's existing semantics (Phase A projection,
Phase B gates, Phase C commit, post-publish hooks) are unchanged.

The scheduled enqueue lives in :mod:`publisher_schedule`. Worker
boot calls :func:`register_publisher_workflow` before
``DBOS.launch()`` so the @DBOS.workflow decorator runs at the right
point in the singleton lifecycle.

## Recovery contract

A worker killed mid-cycle leaves the workflow in DBOS's pending state
with whatever steps committed before the kill. DBOS recovery (Phase 1
§2.7 measured 0.32s on the dev rig) detects the abandoned workflow
within ~1 polling interval and another worker picks it up. The
workflow resumes — Publisher.cycle is already idempotent against the
generation ledger (catalog_version_id + content hash dedupes a
redundant publish), so a re-run after partial progress is safe.

``max_recovery_attempts=10`` gives the workflow ten distinct kill+
resume rounds before being marked ``MAX_RECOVERY_ATTEMPTS_EXCEEDED``;
that's a fail-loud signal that something is wrong beyond transient
worker death (a poison-pill catalog version, a corrupt manifest,
etc.).
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Any


__all__ = [
    "PUBLISHER_WORKFLOW_NAME",
    "publisher_cycle_workflow",
    "register_publisher_workflow",
]


log = logging.getLogger(__name__)


# Stable name used by the scheduled enqueue + by operator tooling
# (`okg runtime debug-workflow <id>` filters on this).
PUBLISHER_WORKFLOW_NAME = "okg_publisher_cycle"


# Module-level handle to the decorated workflow so the scheduled
# enqueue can find it. Populated by register_publisher_workflow().
_DECORATED: dict[str, Any] = {}

# The blocking sync a no_run_base_guard skip last warned about, per deployment
# (this process only): the skip repeats every tick for as long as a first
# bootstrap runs, so it warns once per sync and then logs at INFO.
_GUARD_BLIND_WARNED: dict[str, str] = {}


def register_publisher_workflow(*, deployment: str) -> None:
    """Decorate the publisher workflow.

    Called from the worker entry point AFTER ``DBOS()`` is constructed
    but BEFORE ``DBOS.launch()``. Idempotent: clears the cache on
    entry so a fresh worker boot doesn't pick up a stale closure.

    The deployment name is bound into the workflow's closure because
    DBOS workflow function arguments must be JSON-serializable;
    closing over `deployment` keeps the cycle's caller-side state out
    of the workflow's input row.
    """
    from dbos import DBOS
    from okg.substrate.runtime.workflow_operational_outcomes import (
        define_operational_transactions,
        participant_for,
    )

    _DECORATED.clear()
    # PACT publisher-starvation-4039: every cycle records a typed outcome, so a
    # skipped cycle (a DBOS SUCCESS) is told apart from a publish.
    operational = define_operational_transactions(
        participant=participant_for(
            deployment=deployment, workflow_name=PUBLISHER_WORKFLOW_NAME,
        ),
    )

    @DBOS.workflow(
        name=PUBLISHER_WORKFLOW_NAME,
        max_recovery_attempts=10,
    )
    def publisher_cycle_workflow(
        branch_name: str = "default",
        force_enrichers: bool = False,
    ) -> dict:
        """Run one Publisher.cycle() for the bound deployment on `branch_name`.

        Returns the cycle's summary dict. DBOS persists this as the
        workflow's output; operators read it via
        ``okg runtime debug-workflow <id>``. `branch_name` defaults to
        `default` so the scheduled enqueue (arg-less) is unchanged; a
        per-worktree fork publish passes its experiment branch.
        """
        operational.register_execution_tx()
        summary = _run_one_cycle(
            deployment=deployment,
            branch_name=branch_name,
            force_enrichers=force_enrichers,
        )
        # Keep this as plain workflow code, not a DBOS step. In live
        # shutdowns a signal can arrive after Publisher.cycle() returns
        # but before this post-cycle feedback step records itself; the
        # DBOS step wrapper then tries to touch the system DB after
        # teardown has started. The feedback remains persisted through
        # its own SQL writes and is represented in the workflow output.
        feedback = _apply_admission_feedback_safely(
            deployment=deployment,
            summary=summary,
        )
        if isinstance(summary, dict):
            summary["admission_feedback"] = feedback
        # Recorded last, so the outcome's time (the publisher's freshness
        # clock) is the end of the whole cycle, feedback included.
        operational.record_outcome_tx(
            _publisher_operational_outcome(summary, branch_name=branch_name)
        )
        return summary

    _DECORATED[PUBLISHER_WORKFLOW_NAME] = publisher_cycle_workflow
    log.info(
        "registered publisher workflow: deployment=%s name=%s",
        deployment, PUBLISHER_WORKFLOW_NAME,
    )


def publisher_cycle_workflow() -> dict:
    """Public accessor for the decorated workflow function.

    Used by the scheduled enqueue and by tests that want to drive
    one cycle directly via the DBOS workflow path.
    """
    if PUBLISHER_WORKFLOW_NAME not in _DECORATED:
        raise RuntimeError(
            "publisher workflow not registered; call "
            "register_publisher_workflow() before DBOS.launch()"
        )
    return _DECORATED[PUBLISHER_WORKFLOW_NAME]


# ---------------------------------------------------------------------------
# Cycle runner
# ---------------------------------------------------------------------------


def _run_one_cycle(
    *,
    deployment: str,
    branch_name: str = "default",
    force_enrichers: bool = False,
    scheduled: bool | None = None,
) -> dict:
    """Build the deployment publisher stack and run one cycle on `branch_name`.

    Mirrors the normal ``okg run --once`` publisher path, with two
    workflow-specific differences:

      1. ``coordination_mode='dbos'`` is set so no lease is acquired
         — the surrounding DBOS workflow's partition queue is the
         mutual exclusion.
      2. The DSN is resolved from the deployment manifest rather than
         taken as a positional arg (the workflow signature is
         arg-less so DBOS recovery doesn't need to re-serialize it).

    ``scheduled`` marks a cycle the cron tick enqueued (None: read it from the
    running workflow's deduplication id; False outside a workflow). Only a
    scheduled cycle can be *overdue* (PACT publisher-starvation-4039): when the
    newest of its last non-skipped cycle and the branch's newest published
    generation is older than ``publisher.max_source_sync_yield_seconds``, it
    ignores source-sync workflow rows at the start check and in the in-lock
    probe, live or stranded. It keeps the admission intent, the publish lock,
    the run-base guard and the write-race refusal, and never waits for a run
    inside the control queue: a run still writing ends the cycle as a recorded
    skip. Where the run-base guard is blind (no published generation yet) the
    row yield stays on. Manual publishes keep today's behaviour.
    """
    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.publisher.deployment_stack import (
        build_deployment_publisher_stack,
    )
    from okg.substrate.publisher.factory import build_deployment_publisher
    from okg.substrate.runtime.profiling_bridge import (
        workflow_profiling_context,
    )
    from okg.substrate.runtime.workflow_telemetry import dbos_workflow_trace
    from okg.substrate.errors import PublisherLeaseHeld, SourceRunInProgress

    manifest = resolve_deployment(deployment)
    from okg.substrate.deployments import require_postgres_dsn
    dsn = require_postgres_dsn(manifest, deployment=deployment)

    # PACT bound-host-storage-pressure / req.host.staging-governance: on a
    # staging-tier worker the publisher cycle is the replay driver. Gate it at
    # the workflow's entry -- it publishes only under an explicit, unexpired,
    # budgeted arm on the same Postgres server, and halts (skips the publish)
    # loudly otherwise, so a daemon/container restart alone can never resume an
    # unbounded replay. No-op on every other tier (the arm gate short-circuits
    # unless OKG_ENVIRONMENT_TIER == "staging").
    staging_halt = _staging_replay_guard(deployment=deployment, dsn=dsn)
    if staging_halt is not None:
        log.warning(
            "publisher cycle halted by staging replay governance: "
            "deployment=%s reason=%s | %s",
            deployment,
            staging_halt.reason,
            staging_halt.message,
        )
        return _staging_replay_skip_summary(
            deployment=deployment,
            branch_name=branch_name,
            verdict=staging_halt,
        )

    if scheduled is None:
        scheduled = _is_scheduled_cycle(deployment=deployment, dsn=dsn)
    freshness = None
    overdue = False
    if scheduled:
        from okg.substrate.runtime.publisher_schedule import (
            read_publisher_freshness,
        )

        freshness = read_publisher_freshness(
            deployment=deployment,
            dsn=dsn,
            branch_name=branch_name,
            bound_seconds=manifest.publisher.max_source_sync_yield_seconds,
        )
        overdue = freshness.overdue
    # Where the run-base guard is blind (the branch has no published
    # generation, so a run cuts no run base) the row yield is the only thing
    # that sees a writing sync: it stays on, without bound.
    guard_blind = overdue and not freshness.has_generation
    rows_yield = not overdue or guard_blind
    skip_reason = "no_run_base_guard" if guard_blind else "source_sync_active"

    def annotate(summary: dict) -> dict:
        if freshness is not None and isinstance(summary, dict):
            summary["scheduled"] = {
                "overdue": overdue,
                "freshness_age_seconds": freshness.age_seconds,
                "bound_seconds": freshness.bound_seconds,
            }
        return summary

    row_or_intent_blocker = _active_source_sync_workflow_id_for_publisher(
        deployment=deployment,
        dsn=dsn,
    )
    if rows_yield:
        blocking_source_sync = row_or_intent_blocker
    else:
        blocking_source_sync = _source_sync_admission_intent_id_for_publisher(
            deployment=deployment,
            dsn=dsn,
        )
    if blocking_source_sync is not None:
        if freshness is None:
            log.info(
                "publisher cycle skipped: deployment=%s branch_name=%s "
                "blocking_source_sync_workflow_id=%s",
                deployment, branch_name, blocking_source_sync,
            )
        else:
            warn = overdue
            if guard_blind:
                # A first bootstrap can hold this skip for days: warn once per
                # blocking sync, then INFO.
                warn = _GUARD_BLIND_WARNED.get(deployment) != blocking_source_sync
                _GUARD_BLIND_WARNED[deployment] = blocking_source_sync
            (log.warning if warn else log.info)(
                "publisher cycle skipped (%s): deployment=%s branch_name=%s "
                "blocking_source_sync_workflow_id=%s %s %s",
                skip_reason, deployment, branch_name, blocking_source_sync,
                _describe_workflow_row(dsn, blocking_source_sync),
                freshness.describe(),
            )
        return annotate(_source_sync_active_skip_summary(
            deployment=deployment,
            branch_name=branch_name,
            blocking_workflow_id=blocking_source_sync,
            reason=skip_reason,
        ))
    if overdue and row_or_intent_blocker is not None:
        log.warning(
            "publisher cycle overdue: publishing past a source-sync "
            "workflow row: deployment=%s branch_name=%s "
            "source_sync_workflow_id=%s %s %s",
            deployment, branch_name, row_or_intent_blocker,
            _describe_workflow_row(dsn, row_or_intent_blocker),
            freshness.describe(),
        )

    def source_sync_priority_probe() -> str | None:
        if not rows_yield:
            return _source_sync_admission_intent_id_for_publisher(
                deployment=deployment,
                dsn=dsn,
            )
        return _active_source_sync_workflow_id_for_publisher(
            deployment=deployment,
            dsn=dsn,
        )

    stack = build_deployment_publisher_stack(
        dsn=dsn,
        deployment_name=deployment,
        deployment_dir=getattr(manifest, "deployment_dir", None),
    )
    pub = build_deployment_publisher(
        dsn=dsn,
        deployment_name=deployment,
        deployment_dir=getattr(manifest, "deployment_dir", None),
        command="okg publisher cycle",
        branch_name=branch_name,
        stack=stack,
        publisher_overrides={
            "coordination_mode": "dbos",
            "source_sync_priority_probe": source_sync_priority_probe,
        },
    )
    # okg#3112: the worker cycle runs the post-publish index rebuild after
    # the publish released its lock (opt-in per caller; time-budgeted).
    pub.post_publish_index_rebuild_in_cycle = True
    # `restore-publisher-perf-and-profiling` §B: wrap pub.cycle() so
    # the 24 `profile_stage(...)` hooks inside publisher.py emit
    # telemetry on the DBOS-driven path (today they're no-ops because
    # no context is active outside the CLI).
    with dbos_workflow_trace(
        dsn=dsn,
        deployment_name=deployment,
        workflow_name=PUBLISHER_WORKFLOW_NAME,
        workflow_id=_current_dbos_workflow_id(),
        branch_name=branch_name,
        queue_name=f"okg-{deployment}",
        attributes={
            "okg.target.kind": "publish",
            "okg.branch.name": branch_name,
        },
    ):
        with workflow_profiling_context(
            manifest=manifest,
            deployment=deployment,
            target_kind="publish",
            command="okg_publisher_cycle",
        ) as prof:
            try:
                summary = pub.cycle(force_enrichers=force_enrichers)
            except SourceRunInProgress as exc:
                # Issue #2993: another source run is still writing. A
                # scheduled tick yields to it, as it yields to an active
                # source sync: not a failed publish. The next tick after the
                # run ends publishes its work. `okg run --once` and
                # merge-back, which an operator runs, still refuse loudly.
                skipped = _source_run_in_progress_skip_summary(
                    deployment=deployment,
                    branch_name=branch_name,
                    error=exc,
                )
                run_ids = ",".join(
                    str(item.get("run_id"))
                    for item in skipped["in_progress"]
                    if isinstance(item, dict)
                ) or "unknown"
                (log.warning if overdue else log.info)(
                    "publisher cycle skipped: deployment=%s branch_name=%s "
                    "source run in progress, run_ids=%s %s: %s",
                    deployment, branch_name, run_ids,
                    freshness.describe() if freshness is not None else "",
                    exc,
                )
                return annotate(skipped)
            except PublisherLeaseHeld as exc:
                if exc.data.get("reason") != "source_sync_active":
                    raise
                return annotate(_source_sync_active_skip_summary(
                    deployment=deployment,
                    branch_name=branch_name,
                    blocking_workflow_id=str(
                        exc.data.get("blocking_workflow_id") or ""
                    ),
                    reason=skip_reason,
                ))
            if isinstance(summary, dict) and summary.get("generation_id"):
                # prof.generation_id is an int telemetry token. Under chronos_native the
                # canonical generation_id is a Chronos checkpoint STRING; the int token is
                # carried as compat_generation_id. Prefer that; fall back to the legacy int.
                gen_token = summary.get("compat_generation_id")
                if gen_token is None:
                    gen_token = summary.get("generation_id")
                try:
                    prof.generation_id = int(gen_token)
                except (TypeError, ValueError):
                    # Profiling metadata is advisory; keep the timing row without a generation id.
                    pass
    # bench-okg-internals: persist the publish cycle's per-phase timings
    # (env-gated by OKG_PERSIST_PUBLISH_TIMINGS; no-op in production). This
    # covers the scheduled publisher path; run_ingest's inter-layer publish
    # persists too, so either path that produces a generation is captured.
    from okg.substrate.ingest.service import persist_publish_stage_timings
    persist_publish_stage_timings(
        dsn=dsn, deployment=deployment, published=summary,
    )
    # Opt-in resolution-aware reuse pass (PACT consistency-resolution-aware-reuse,
    # task.reuse.scheduler): only deployments that set
    # consistency.resolution_aware_reuse run it; every other cycle is unchanged.
    # Post-publish maintenance state, isolated so a reuse-pass failure never turns
    # a successful publish into a crash.
    if isinstance(summary, dict) and getattr(
        stack, "resolution_aware_reuse_enabled", False
    ):
        summary["reuse_pass"] = _run_reuse_pass_safely(
            dsn=dsn,
            deployment=deployment,
            summary=summary,
            deployment_dir=stack.deployment_dir,
        )
    if isinstance(summary, dict):
        # okg#3112 step 2: ask for a declared-attribute index run when a
        # declaration changed or catalog apply asked. Enqueue only: the build
        # and the drift checks run on the index-maintenance queue, never here.
        summary["declared_attr_index_maintenance"] = (
            _enqueue_declared_attr_index_maintenance_after_cycle(
                dsn=dsn, deployment=deployment
            )
        )
    log.info(
        "publisher cycle complete: deployment=%s generation_id=%s "
        "status=%s nodes_added=%s edges_added=%s",
        deployment,
        summary.get("generation_id"),
        summary.get("status"),
        summary.get("nodes_added"),
        summary.get("edges_added"),
    )
    return annotate(summary)


def _enqueue_declared_attr_index_maintenance_after_cycle(
    *, dsn: str, deployment: str
) -> dict:
    """Enqueue the declared-attribute index workflow when it has work.

    Runs no index DDL and no drift check (design section 4.4). A failure is
    logged with its traceback and returned; the five-minute schedule runs
    the workflow anyway.
    """
    try:
        from okg.substrate.db.connection import connect as db_connect
        from okg.substrate.declared_attr_indexes import maintenance_wanted
        from okg.substrate.runtime.declared_attr_index_workflow import (
            enqueue_declared_attr_index_maintenance,
        )

        with db_connect(dsn, autocommit=True) as conn:
            wanted, reason = maintenance_wanted(conn)
        if not wanted:
            return {"status": "not_needed", "reason": reason}
        return enqueue_declared_attr_index_maintenance(
            deployment=deployment, dsn=dsn, reason=reason
        )
    except Exception as exc:  # noqa: BLE001 - post-publish maintenance is isolated
        log.exception(
            "declared-attribute index enqueue failed after publish: deployment=%s",
            deployment,
        )
        return {"status": "error", "error": f"{type(exc).__name__}: {exc}"}


def _run_reuse_pass_safely(
    *,
    dsn: str,
    deployment: str,
    summary: dict,
    deployment_dir: Any,
) -> dict:
    """Run the opt-in reuse pass without letting it break a successful publish.

    Delegates to the reuse-pass workflow's post-cycle hook (which itself re-checks
    the opt-in flag). Any failure is caught and reported in the summary rather than
    propagated — the certificates are post-publish maintenance state, never a gate
    on the published generation."""
    try:
        from okg.substrate.runtime.reuse_pass_workflow import (
            run_post_cycle_reuse_pass,
        )

        return run_post_cycle_reuse_pass(
            dsn=dsn,
            deployment=deployment,
            summary=summary,
            deployment_dir=deployment_dir,
        )
    except Exception as exc:  # noqa: BLE001 - post-publish maintenance is isolated
        log.exception(
            "reuse pass failed after publish: deployment=%s generation_id=%s",
            deployment,
            summary.get("generation_id") if isinstance(summary, dict) else None,
        )
        return {
            "reuse_pass": "error",
            "deployment": deployment,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _staging_replay_guard(*, deployment: str, dsn: str):
    """Return a halt verdict when a staging-tier replay must be skipped.

    Returns ``None`` (cycle proceeds) on every non-staging tier and when the
    staging session is armed and within bounds. Fails closed: any guard error
    surfaces as a halt rather than an unguarded publish.
    """
    if os.environ.get("OKG_ENVIRONMENT_TIER") != "staging":
        return None
    from okg.substrate.runtime.staging_replay import guard_publisher_cycle

    verdict = guard_publisher_cycle(deployment=deployment, dsn=dsn)
    return verdict if verdict.halted else None


def _staging_replay_skip_summary(
    *,
    deployment: str,
    branch_name: str,
    verdict: Any,
) -> dict[str, Any]:
    return {
        "deployment": deployment,
        "branch_name": branch_name,
        "status": "skipped",
        "publish_skipped": True,
        "skipped": "staging_replay_unarmed",
        "staging_replay_halt": verdict.to_dict(),
    }


def _source_sync_active_skip_summary(
    *,
    deployment: str,
    branch_name: str,
    blocking_workflow_id: str,
    reason: str = "source_sync_active",
) -> dict[str, Any]:
    return {
        "deployment": deployment,
        "branch_name": branch_name,
        "status": "skipped",
        "publish_skipped": True,
        "skipped": reason,
        "blocking_workflow_id": blocking_workflow_id,
    }


def _source_run_in_progress_skip_summary(
    *,
    deployment: str,
    branch_name: str,
    error: Any,
) -> dict[str, Any]:
    data = dict(getattr(error, "data", None) or {})
    return {
        "deployment": deployment,
        "branch_name": branch_name,
        "status": "skipped",
        "publish_skipped": True,
        "skipped": "source_run_in_progress",
        "reason": str(error),
        "in_progress": list(data.get("in_progress") or []),
        "fix_hint": getattr(error, "fix_hint", None),
    }


def _current_dbos_workflow_id() -> str | None:
    dbos_module = sys.modules.get("dbos")
    if dbos_module is None:
        return None
    DBOS = getattr(dbos_module, "DBOS", None)
    if DBOS is None:
        return None
    workflow_id = getattr(DBOS, "workflow_id", None)
    if callable(workflow_id):
        try:
            workflow_id = workflow_id()
        except Exception:  # noqa: BLE001 - observability must stay best-effort.
            return None
    if isinstance(workflow_id, str) and workflow_id:
        return workflow_id
    return None


def _active_source_sync_workflow_id_for_publisher(
    *,
    deployment: str,
    dsn: str,
) -> str | None:
    """Return active source-sync work that should run before cron publish."""
    try:
        from okg.substrate.runtime.dbos_enqueue import (
            active_workflow_id_for_name,
        )
        from okg.substrate.runtime.partition_queue import (
            source_sync_queue_name,
        )
        from okg.substrate.runtime.source_sync_constants import (
            SOURCE_SYNC_WORKFLOW_NAME,
        )
        from okg.substrate.runtime.source_sync_priority import (
            source_sync_admission_intent_workflow_id,
        )

        intent_id = source_sync_admission_intent_workflow_id(
            dsn=dsn,
            deployment=deployment,
        )
        if intent_id is not None:
            return intent_id
        return active_workflow_id_for_name(
            dsn=dsn,
            queue_name=source_sync_queue_name(deployment),
            workflow_name=SOURCE_SYNC_WORKFLOW_NAME,
        )
    except Exception as exc:  # noqa: BLE001
        log.exception(
            "publisher source-sync active-workflow probe failed: "
            "deployment=%s error=%s; publisher cycle blocked",
            deployment, exc,
        )
        raise RuntimeError(
            "publisher source-sync active-workflow probe failed; "
            "publish admission is unavailable"
        ) from exc


def _source_sync_admission_intent_id_for_publisher(
    *,
    deployment: str,
    dsn: str,
) -> str | None:
    """Return the source-sync admission intent that must still be honoured.

    The intent is a live session lock (a runtime producer committing an
    enqueue, or a command-line run for its whole run), never a stale workflow
    row, so an overdue cycle keeps yielding to it. A failed probe stops the
    cycle, as the full probe does.
    """
    try:
        from okg.substrate.runtime.source_sync_priority import (
            source_sync_admission_intent_workflow_id,
        )

        return source_sync_admission_intent_workflow_id(
            dsn=dsn,
            deployment=deployment,
        )
    except Exception as exc:  # noqa: BLE001
        log.exception(
            "publisher source-sync admission-intent probe failed: "
            "deployment=%s error=%s; publisher cycle blocked",
            deployment, exc,
        )
        raise RuntimeError(
            "publisher source-sync admission-intent probe failed; "
            "publish admission is unavailable"
        ) from exc


def _describe_workflow_row(dsn: str, workflow_id: str) -> str:
    """Status and age of a blocking workflow row, for a log line."""
    from okg.substrate.runtime.publisher_schedule import source_sync_workflow_row

    try:
        status, age = source_sync_workflow_row(dsn=dsn, workflow_id=workflow_id)
    except Exception as exc:  # noqa: BLE001 - decoration of a log line only
        return f"source_sync_status=unreadable({type(exc).__name__})"
    return (
        f"source_sync_status={status} source_sync_age_seconds="
        f"{'unknown' if age is None else f'{age:.0f}'}"
    )


def _is_scheduled_cycle(*, deployment: str, dsn: str) -> bool:
    """Did the cron tick enqueue the running workflow?

    The tick enqueues with the bucketed deduplication id of
    ``publisher_deduplication_id``; a manual publish
    (``okg publisher cycle``, republish requests, fork publishes) carries none.
    False outside a DBOS workflow.
    """
    workflow_id = _current_dbos_workflow_id()
    if workflow_id is None:
        return False
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.runtime.publisher_schedule import (
        publisher_deduplication_id,
    )

    with db_connect(dsn, connect_timeout=5, autocommit=True) as conn:
        row = conn.execute(
            "SELECT deduplication_id FROM dbos.workflow_status"
            " WHERE workflow_uuid = %s",
            (workflow_id,),
        ).fetchone()
    prefix = f"{publisher_deduplication_id(deployment)}:"
    return bool(
        row is not None and isinstance(row[0], str) and row[0].startswith(prefix)
    )


def _publisher_operational_outcome(
    summary: Any, *, branch_name: str,
) -> dict[str, Any]:
    """Map a finished cycle's summary to its typed operational outcome.

    ``clean`` for a publish, a blocked checkpoint or a no-op; ``skipped`` for
    every skip path, with the reason and its subject in ``details``
    (``source_sync_active``, ``source_run_in_progress``,
    ``no_run_base_guard``, ``staging_replay_unarmed``).
    """
    from okg.substrate.runtime.workflow_operational_outcomes import (
        WorkflowOperationalEvidenceError,
    )

    if not isinstance(summary, dict):
        raise WorkflowOperationalEvidenceError(
            "publisher cycle summary must be a mapping"
        )
    status = summary.get("status")
    details: dict[str, Any] = {"branch_name": branch_name}
    scheduled = summary.get("scheduled")
    if isinstance(scheduled, dict):
        details["overdue"] = bool(scheduled.get("overdue"))
    if status == "skipped":
        reason = str(summary.get("skipped") or "").strip()
        if not reason:
            raise WorkflowOperationalEvidenceError(
                "skipped publisher cycle names no reason"
            )
        details["reason"] = reason
        details["subject"] = _skip_subject(summary)
        return {
            "outcome": "skipped",
            "succeeded_count": 0,
            "failed_count": 0,
            "semantic_units": {},
            "details": details,
        }
    if not isinstance(status, str) or not status:
        raise WorkflowOperationalEvidenceError(
            "publisher cycle summary lacks a status"
        )
    details["status"] = status
    generation_id = summary.get("generation_id")
    units: dict[str, list[str]] = {}
    if generation_id is not None and str(generation_id):
        details["generation_id"] = str(generation_id)
        units["generation_ids"] = [str(generation_id)]
    return {
        "outcome": "clean",
        "succeeded_count": 1,
        "failed_count": 0,
        "semantic_units": units,
        "details": details,
    }


def _skip_subject(summary: dict[str, Any]) -> str:
    """What a skip names: the sync, the run(s), or the staging verdict."""
    blocking = summary.get("blocking_workflow_id")
    if blocking:
        return str(blocking)
    run_ids = [
        str(item.get("run_id"))
        for item in summary.get("in_progress") or []
        if isinstance(item, dict) and item.get("run_id")
    ]
    if run_ids:
        return ",".join(run_ids[:3])
    halt = summary.get("staging_replay_halt")
    if isinstance(halt, dict) and halt.get("reason"):
        return str(halt["reason"])
    return "unspecified"


def _apply_admission_feedback_safely(
    *,
    deployment: str,
    summary: dict,
) -> dict:
    """Apply admission feedback without turning publish success into a race."""
    try:
        return _apply_admission_feedback(
            deployment=deployment,
            summary=summary,
        )
    except Exception as e:  # noqa: BLE001
        log.exception(
            "publisher admission feedback failed: deployment=%s "
            "generation_id=%s",
            deployment,
            summary.get("generation_id") if isinstance(summary, dict) else None,
        )
        return {
            "deployment": deployment,
            "status": (
                summary.get("status")
                if isinstance(summary, dict) else None
            ),
            "updated": 0,
            "skipped": "admission_feedback_failed",
            "error": f"{type(e).__name__}: {e}",
        }


def _apply_admission_feedback(
    *,
    deployment: str,
    summary: dict,
) -> dict:
    """Apply admission lifecycle feedback for the published generation."""
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.admission import (
        apply_generation_feedback,
        workflow_ref_from_dbos,
    )
    from okg.substrate.deployments import (
        require_postgres_dsn,
        resolve_deployment,
    )

    generation_id = summary.get("generation_id")
    status = summary.get("status")
    if generation_id is None or status not in {"published", "blocked"}:
        return {
            "deployment": deployment,
            "status": status,
            "updated": 0,
            "skipped": "no_generation_feedback_needed",
        }

    manifest = resolve_deployment(deployment)
    dsn = require_postgres_dsn(manifest, deployment=deployment)
    workflow_ref = workflow_ref_from_dbos(PUBLISHER_WORKFLOW_NAME)
    with db_connect(dsn, autocommit=True) as conn:
        with conn.transaction():
            # Pass generation_id through WITHOUT int() coercion: under chronos_native it
            # is a Chronos checkpoint string (apply_generation_feedback ->
            # _load_generation_feedback reads the checkpoint metadata for it); int() would
            # crash on the string. Legacy int ids still route to graph_generations.
            feedback = apply_generation_feedback(
                conn,
                deployment_name=deployment,
                generation_id=generation_id,
                workflow_ref=workflow_ref,
            )
    return feedback
