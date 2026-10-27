"""`okg runtime` subcommand handlers.

Extracted from ``cli/main.py`` by ``extract-cli-runtime``. The two
``okg runtime`` handlers (``_cmd_runtime`` and
``_cmd_runtime_escalation``) used to live inline in ``main.py``;
moving them here shrinks ``main.py`` and lets the runtime work
evolve in isolation.

The handlers defer their imports of ``cli.main`` helpers
(``_emit_error``, ``_emit_ok``, ``_resolve_deployment_dsn``) to call
time to avoid a top-level circular import.
"""
from __future__ import annotations

import argparse
import os
from typing import Any
from okg.substrate.support.user_hints import okg_prefix


def _complete_runtime_worker_process(
    returncode: int,
    *,
    operator: bool,
) -> int:
    """Own the final process boundary of a supervised worker.

    Supervised mode hard-exits (``os._exit``) for two codes: the attested
    controlled-recovery handoff, and the unattested code 7 where a workflow is
    still running. Operator mode and every other code return normally.
    """
    from okg.substrate.runtime.recovery_handoff import (
        CONTROLLED_RECOVERY_EXIT_CODE,
    )
    from okg.substrate.runtime.worker import WORKER_RECOVERY_UNATTESTED_EXIT_CODE

    if returncode == CONTROLLED_RECOVERY_EXIT_CODE and not operator:
        os._exit(returncode)
    if returncode == WORKER_RECOVERY_UNATTESTED_EXIT_CODE and not operator:
        # The worker already stopped dispatch and waited out its bounded grace
        # period; a workflow (for example a publish) is still running on an
        # executor thread. A normal return would make the interpreter join that
        # thread, so the stop would last until its statement timeout (issue
        # #3861). Leave now; DBOS recovers the workflow on the next start.
        # The exit closes the client's sockets but Postgres does not drop the
        # session at once: it notices the closed socket only through
        # `client_connection_check_interval` (set on every okg session, 5 s by
        # default), then cancels the running statement and frees the session
        # advisory publish lock. Anything already committed stays committed.
        import logging
        import sys

        logging.getLogger(__name__).error(
            "worker exiting with code %d while workflow(s) are still running; "
            "Postgres ends their in-flight statements once it notices the "
            "closed connection (client_connection_check_interval) and DBOS "
            "recovers them on the next start",
            returncode,
        )
        logging.shutdown()
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(returncode)
    return returncode


def _cmd_runtime(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg runtime ...` — DBOS runtime layer commands.

    Dispatches to per-subcommand handlers. Bootstrap is implemented in
    Phase 1 §2.2; smoke / worker / reset / list-schedules /
    trigger-schedule / get-event / measure-recovery are stubs that
    fill in with §2.3, §2.4, §2.6, §2.7 respectively.
    """
    import json

    from okg.substrate.cli.main import (
        _coerce_dt,
        _emit_error,
        _find_profile_for_workflow,
        _read_publisher_phase_events,
        _resolve_deployment_dsn,
        _summarize_phase_event_payload,
    )
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )
    from okg.substrate.errors import SubstrateError
    from okg.substrate.runtime import bootstrap_dbos_schema

    from okg.substrate.deployments.admission import require_active_deployment
    if args.rt_subcmd == "worker":
        try:
            require_active_deployment(args.deployment)
        except SubstrateError as exc:
            return _emit_error(exc, as_json=as_json)

    # If --dsn is supplied, propagate it as OKG_DSN BEFORE resolving the
    # manifest. The manifest's `postgres.dsn: ${OKG_DSN}` is expanded at
    # resolve time against os.environ, and any DBOS workflow body that
    # later re-resolves the manifest (e.g. okg_source_sync) needs to see
    # the same value. Without this, --dsn covered only the DBOS-side
    # connection while source connections silently failed with
    # `missing "=" after "${OKG_DSN}"` (2026-05-11).
    if hasattr(args, "dsn") and args.dsn:
        os.environ["OKG_DSN"] = args.dsn

    try:
        manifest = resolve_deployment(args.deployment)
    except DeploymentResolutionError as e:
        return _emit_error(SubstrateError(str(e)), as_json=as_json)

    if (
        not manifest.runtime.enabled
        and args.rt_subcmd not in {
            "bootstrap", "openshell-policy", "supersede-checkpoint",
            "finish-undo", "deferred-edges",
        }
    ):
        # Bootstrap is allowed even when runtime is disabled — it sets
        # up the substrate for the operator to flip the manifest later.
        # supersede-checkpoint only writes the checkpoint's recovery
        # record; an inline `okg ingest` blocks a checkpoint on an install
        # with no runtime too, so the way out cannot require one (#2476).
        # Other subcommands need an enabled runtime.
        return _emit_error(
            SubstrateError(
                f"runtime is not enabled for deployment "
                f"{args.deployment!r}; set runtime.enabled: true in "
                f"deployment.yaml first",
            ),
            as_json=as_json,
        )

    dsn = args.dsn if hasattr(args, "dsn") and args.dsn else (
        _resolve_deployment_dsn(args)
    )

    if args.rt_subcmd == "openshell-policy":
        from pathlib import Path

        import yaml

        from okg.substrate.runtime.openshell_policy import (
            generate_openshell_policy,
            render_openshell_policy_yaml,
        )
        try:
            policy = generate_openshell_policy(
                manifest,
                postgres_dsn=dsn,
            )
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)
        if args.output:
            Path(args.output).write_text(
                render_openshell_policy_yaml(policy),
                encoding="utf-8",
            )
        if as_json:
            print(json.dumps(policy, indent=2))
        else:
            print(yaml.safe_dump(policy, sort_keys=False), end="")
        return 0

    if args.rt_subcmd == "bootstrap":
        try:
            result = bootstrap_dbos_schema(
                deployment=args.deployment,
                dsn=dsn,
                runtime=manifest.runtime,
            )
        except Exception as e:
            return _emit_error(SubstrateError(str(e)), as_json=as_json)
        payload = {
            "deployment": result.deployment,
            "schema": result.schema_name,
            "already_bootstrapped": result.already_bootstrapped,
            "role_split_applied": result.role_split_applied,
            "mcp_login": result.mcp_login,
        }
        if as_json:
            print(json.dumps(payload, indent=2))
        else:
            status = (
                "no-op (already bootstrapped)"
                if result.already_bootstrapped else "complete"
            )
            print(f"runtime bootstrap: {result.deployment} — {status}")
            print(f"  schema: {result.schema_name}")
            print(f"  role_split_applied: {result.role_split_applied}")
            mcp_login = result.mcp_login or {}
            print(
                "  mcp_login: "
                f"reconciled={mcp_login.get('reconciled')} "
                f"reason={mcp_login.get('reason')}"
            )
            if mcp_login.get("fix_hint"):
                print(f"    fix: {mcp_login['fix_hint']}")
        return 0

    if args.rt_subcmd == "smoke":
        from okg.substrate.runtime.smoke import run_smoke
        result = run_smoke(
            deployment=args.deployment,
            dsn=dsn,
            runtime=manifest.runtime,
        )
        payload = {
            "deployment": result.deployment,
            "listen_notify_ok": result.listen_notify_ok,
            "round_trip_ms": result.round_trip_ms,
            "error": result.error,
        }
        if as_json:
            print(json.dumps(payload, indent=2))
        else:
            status = "OK" if result.listen_notify_ok else "FAILED"
            print(
                f"runtime smoke: {result.deployment} — {status} "
                f"(round_trip_ms={result.round_trip_ms})"
            )
            if result.error:
                print(f"  error: {result.error}")
        return 0 if result.listen_notify_ok else 1

    if args.rt_subcmd == "worker":
        # The same one readiness answer the other entry commands give
        # (issue #2128). Without it this branch walks straight into
        # `enforce_release_contract_if_declared`, whose release read
        # queries `okg.catalog_versions` unconditionally, so a
        # pre-migration worker launch answered a raw psycopg
        # `UndefinedTable` traceback while `okg mcp-serve` two frames
        # away named `okg migrate --apply`.
        from okg.substrate.cli.main import _report_database_behind
        from okg.substrate.db.readiness import require_database_ready

        try:
            readiness = require_database_ready(
                dsn,
                surface="okg runtime worker",
                deployment=args.deployment,
            )
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)
        try:
            from okg.substrate.deployment_release import (
                enforce_release_contract_if_declared,
            )

            release_report = enforce_release_contract_if_declared(
                deployment=args.deployment,
                dsn=dsn,
                process_role="runtime-worker",
            )
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)
        # Reported only after the release verdict is known, so the worker
        # never says "continuing" and then refuses (PACT
        # unknown-s7-release-verdict).
        _report_database_behind(
            readiness,
            release_report,
            surface="okg runtime worker",
            deployment=args.deployment,
        )
        from okg.substrate.runtime.worker import run_worker

        operator = bool(getattr(args, "operator", False))
        returncode = run_worker(
            deployment=args.deployment,
            dsn=dsn,
            runtime=manifest.runtime,
            operator=operator,
            max_wait_seconds=getattr(args, "max_wait_seconds", 600.0),
        )
        return _complete_runtime_worker_process(
            returncode,
            operator=operator,
        )

    if args.rt_subcmd == "list-schedules":
        from okg.substrate.runtime.admin import client_list_schedules
        entries = client_list_schedules(
            deployment=args.deployment,
            dsn=dsn,
            runtime=manifest.runtime,
        )
        payload = [
            {
                "schedule_id": e.schedule_id,
                "cron": e.cron,
                "workflow_name": e.workflow_name,
                "paused": e.paused,
            }
            for e in entries
        ]
        if as_json:
            print(json.dumps(payload, indent=2))
        else:
            for e in entries:
                print(
                    f"  {e.schedule_id:30s}  cron={e.cron!r:18s}  "
                    f"workflow={e.workflow_name}"
                )
        return 0

    if args.rt_subcmd == "enqueue-fenced-recovery":
        from okg.substrate.runtime.admin import client_enqueue_fenced_recovery

        try:
            workflow_id = client_enqueue_fenced_recovery(
                deployment=args.deployment,
                dsn=dsn,
                workflow_name=args.workflow_name,
                promotion_id=args.promotion_id,
                deduplication_id=args.deduplication_id,
            )
        except Exception as e:
            return _emit_error(SubstrateError(str(e)), as_json=as_json)
        payload = {
            "deployment": args.deployment,
            "workflow_name": args.workflow_name,
            "workflow_id": workflow_id,
            "promotion_id": args.promotion_id,
            "deduplication_id": args.deduplication_id,
        }
        if as_json:
            print(json.dumps(payload, indent=2))
        else:
            print(
                f"enqueued fenced recovery {args.workflow_name} "
                f"→ workflow_id={workflow_id}"
            )
        return 0

    if args.rt_subcmd == "trigger-schedule":
        from okg.substrate.runtime.admin import client_trigger_schedule
        try:
            workflow_id = client_trigger_schedule(
                deployment=args.deployment,
                dsn=dsn,
                runtime=manifest.runtime,
                schedule_id=args.schedule_id,
            )
        except Exception as e:
            return _emit_error(
                SubstrateError(str(e)), as_json=as_json,
            )
        if as_json:
            print(json.dumps({"workflow_id": workflow_id}, indent=2))
        else:
            print(
                f"triggered {args.schedule_id} → "
                f"workflow_id={workflow_id}"
            )
        return 0

    if args.rt_subcmd == "pause-schedule":
        from okg.substrate.runtime.admin import client_pause_schedule
        try:
            result = client_pause_schedule(
                deployment=args.deployment,
                dsn=dsn,
                schedule_id=args.schedule_id,
            )
        except Exception as e:
            return _emit_error(
                SubstrateError(str(e)), as_json=as_json,
            )
        payload = {
            "deployment": args.deployment,
            "schedule_id": result.schedule_id,
            "paused": True,
            "persisted_paused": result.persisted_paused,
            "scheduler_fenced": result.scheduler_fenced,
            "active_scheduled_workflows_at_timeout": (
                result.active_scheduled_workflows_at_timeout
            ),
            "elapsed_seconds": result.elapsed_seconds,
        }
        if as_json:
            print(json.dumps(payload, indent=2))
        else:
            print(f"paused {args.schedule_id}")
        return 0

    if args.rt_subcmd == "resume-schedule":
        from okg.substrate.runtime.admin import client_resume_schedule
        try:
            client_resume_schedule(
                deployment=args.deployment,
                dsn=dsn,
                schedule_id=args.schedule_id,
            )
        except Exception as e:
            return _emit_error(
                SubstrateError(str(e)), as_json=as_json,
            )
        payload = {
            "deployment": args.deployment,
            "schedule_id": args.schedule_id,
            "active": True,
        }
        if as_json:
            print(json.dumps(payload, indent=2))
        else:
            print(f"resumed {args.schedule_id}")
        return 0

    if args.rt_subcmd == "reset":
        from okg.substrate.runtime.dev import (
            DevResetRefused, reset_dbos,
        )
        try:
            result = reset_dbos(
                deployment=args.deployment,
                dsn=dsn,
                runtime=manifest.runtime,
                confirm=args.confirm,
                # Resolve the target environment from the manifest's
                # `runtime.environment` marker (default "development" only
                # when genuinely absent). A `runtime.environment: production`
                # manifest fails closed via DevResetRefused (#682).
                environment=manifest.runtime.environment,
            )
        except DevResetRefused as e:
            return _emit_error(SubstrateError(str(e)), as_json=as_json)
        if as_json:
            print(json.dumps(
                {
                    "deployment": result.deployment,
                    "cleared": result.cleared,
                    "schema": result.schema_name,
                },
                indent=2,
            ))
        else:
            print(f"runtime reset: {result.deployment} — cleared")
            print(f"  schema: {result.schema_name}")
        return 0

    if args.rt_subcmd == "get-event":
        from okg.substrate.runtime.admin import client_get_event
        try:
            value = client_get_event(
                dsn=dsn,
                workflow_id=args.workflow_id,
                key=args.key,
            )
        except Exception as e:
            return _emit_error(
                SubstrateError(str(e)), as_json=as_json,
            )
        if as_json:
            print(json.dumps(
                {"workflow_id": args.workflow_id,
                 "key": args.key, "value": value},
                indent=2, default=str,
            ))
        else:
            print(f"{args.key}: {value}")
        return 0

    if args.rt_subcmd == "debug-workflow":
        from okg.substrate.runtime.admin import client_debug_workflow
        try:
            report = client_debug_workflow(
                dsn=dsn, workflow_id=args.workflow_id,
            )
        except Exception as e:
            return _emit_error(SubstrateError(str(e)), as_json=as_json)

        # Per `decompose-publisher-cycle-into-dbos-steps`
        # (deepen-dbos-integration Phase 1): the publisher cycle
        # emits 8 normative phase events. Read them in spec-order
        # and render as a timeline. Only the publisher workflow
        # has these events; other workflows skip this section.
        phase_events: list[dict] = []
        if report.name == "okg_publisher_cycle":
            phase_events = _read_publisher_phase_events(
                dsn=dsn, workflow_id=report.workflow_id,
            )

        # `restore-publisher-perf-and-profiling` §C: derive lifetime,
        # work, queue_wait. `work_ms` = sum of per-phase durations;
        # `lifetime_ms` = completed_at − started_at (when present);
        # `queue_wait_ms` = lifetime − work. For in-flight workflows
        # (completed_at None) we report lifetime-so-far but leave
        # queue_wait_ms None to avoid a misleading value.
        work_ms = sum(
            int(evt.get("payload", {}).get("duration_ms", 0) or 0)
            for evt in phase_events
        )
        lifetime_ms: int | None = None
        queue_wait_ms: int | None = None
        if report.started_at and report.completed_at:
            try:
                _s = _coerce_dt(report.started_at)
                _c = _coerce_dt(report.completed_at)
                if _s is not None and _c is not None:
                    lifetime_ms = int(
                        (_c - _s).total_seconds() * 1000
                    )
                    if phase_events:
                        queue_wait_ms = max(0, lifetime_ms - work_ms)
            except Exception:  # noqa: BLE001
                lifetime_ms = None
                queue_wait_ms = None
        elif report.started_at:
            try:
                from datetime import datetime as _dt
                from datetime import timezone as _tz
                _s = _coerce_dt(report.started_at)
                if _s is not None:
                    lifetime_ms = int(
                        (
                            _dt.now(_tz.utc)
                            - _s.astimezone(_tz.utc)
                        ).total_seconds() * 1000
                    )
            except Exception:  # noqa: BLE001
                lifetime_ms = None

        # Profile artifact link: look up the most-recent profile run
        # whose request_path matches the workflow_uuid. The link is
        # written by the workflow body itself (per
        # restore-publisher-perf-and-profiling §B).
        profile_path: str | None = _find_profile_for_workflow(
            workflow_id=report.workflow_id,
        )
        source_sync_debug = None
        if report.name == "okg_source_sync":
            source_sync_debug = _source_sync_debug_summary(
                report=report,
                deployment=args.deployment,
                profile_path=profile_path,
                coerce_dt=_coerce_dt,
            )

        payload = {
            "workflow_id": report.workflow_id,
            "name": report.name,
            "status": report.status,
            "created_at": getattr(report, "created_at", None),
            "updated_at": getattr(report, "updated_at", None),
            "started_at": report.started_at,
            "completed_at": report.completed_at,
            "recovery_attempts": report.recovery_attempts,
            "queue_name": report.queue_name,
            "dbos": {
                "deduplication_id": getattr(
                    report, "deduplication_id", None,
                ),
                "executor_id": getattr(report, "executor_id", None),
                "application_version": getattr(
                    report, "application_version", None,
                ),
                "serialization": getattr(report, "serialization", None),
                "inputs_present": bool(getattr(report, "inputs", None)),
                "output_present": getattr(report, "output", None) is not None,
            },
            "steps": report.steps,
            "error": report.error,
            "phase_events": phase_events,
            "work_ms": work_ms if phase_events else None,
            "lifetime_ms": lifetime_ms,
            "queue_wait_ms": queue_wait_ms,
            "profile_path": profile_path,
            "source_sync": source_sync_debug,
        }
        if as_json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            print(f"workflow {report.workflow_id}")
            print(f"  name: {report.name}")
            print(f"  status: {report.status}")
            print(f"  recovery_attempts: {report.recovery_attempts}")
            if report.queue_name:
                print(f"  queue: {report.queue_name}")
            print(f"  started_at: {report.started_at}")
            if report.completed_at:
                print(f"  completed_at: {report.completed_at}")
            if report.error:
                print(f"  error: {report.error}")
            # Render the three perf-derivation lines before phase
            # events. These answer "is this slow because of work or
            # queue wait?" at a glance.
            if lifetime_ms is not None:
                lifetime_s = lifetime_ms / 1000.0
                print(f"  lifetime:   {lifetime_s:8.1f} s")
                if phase_events:
                    work_s = work_ms / 1000.0
                    if queue_wait_ms is not None:
                        qw_s = queue_wait_ms / 1000.0
                        pct = (
                            int(100 * queue_wait_ms / lifetime_ms)
                            if lifetime_ms > 0 else 0
                        )
                        print(
                            f"  work:       {work_s:8.1f} s"
                            f"   (queue_wait {qw_s:.1f} s = {pct}%)"
                        )
                    else:
                        print(f"  work:       {work_s:8.1f} s   (queue_wait <in flight>)")
            if profile_path:
                print(f"  profile:    {profile_path}")
            if source_sync_debug:
                print("  source_sync:")
                print(
                    "    decode: "
                    f"{source_sync_debug.get('decode_status')}"
                )
                print(
                    "    sources: "
                    f"{', '.join(source_sync_debug.get('sources') or []) or '<all>'}"
                )
                print(
                    "    run_mode: "
                    f"{source_sync_debug.get('run_mode')}"
                )
                stage_timing = source_sync_debug.get("stage_timing") or {}
                print(
                    "    timing: "
                    f"{stage_timing.get('status')} "
                    f"({stage_timing.get('timed_source_count', 0)}/"
                    f"{stage_timing.get('source_count', 0)} sources)"
                )
            if phase_events:
                print(f"  phase_events ({len(phase_events)}):")
                cum_ms = 0
                for evt in phase_events:
                    duration = evt.get("payload", {}).get(
                        "duration_ms", 0,
                    )
                    cum_ms += duration
                    summary = _summarize_phase_event_payload(
                        evt.get("payload", {}),
                    )
                    print(
                        f"    +{cum_ms / 1000:6.1f}s  "
                        f"{evt['name']:<40s}  {summary}"
                    )
            if report.steps:
                print(f"  steps ({len(report.steps)}):")
                for s in report.steps:
                    started = s.get("started_at")
                    completed = s.get("completed_at")
                    name = s.get("name") or "<unknown>"
                    print(
                        f"    [{s.get('step_id')}] {name}  "
                        f"started={started} completed={completed}"
                    )
                    if s.get("error"):
                        print(f"      error: {s['error']}")
        return 0 if not report.error else 1

    if args.rt_subcmd == "cancel-workflow":
        # Issues #1257 / #1254: the scoped, non-destructive remedy for a
        # publish queue wedged by one workflow that can never finish. It
        # clears exactly one row on a queue this deployment owns; every
        # other target is refused rather than silently skipped, so a
        # zero-effect run can never read as a successful clear.
        from okg.substrate.runtime.workflow_cancel import (
            cancel_deployment_workflow,
        )
        try:
            cancel_result = cancel_deployment_workflow(
                dsn=dsn,
                deployment=args.deployment,
                workflow_id=args.workflow_id,
            )
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)
        except Exception as e:  # noqa: BLE001
            return _emit_error(SubstrateError(str(e)), as_json=as_json)
        payload = cancel_result.to_dict()
        if as_json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            print(f"cancelled workflow {cancel_result.workflow_id}")
            print(f"  name: {cancel_result.name}")
            print(f"  queue: {cancel_result.queue_name}")
            print(f"  status: {cancel_result.previous_status} -> "
                  f"{cancel_result.status}")
            if cancel_result.queue_slot_released:
                print(
                    "  the queue slot this workflow held is released; the "
                    "next scheduled cycle can start"
                )
            else:
                print(
                    "  this workflow held no queue slot; cancelling it clears "
                    "it as a blocker, it does not free capacity"
                )
        return 0

    if args.rt_subcmd == "supersede-checkpoint":
        # Issue #1599: an incomplete checkpoint requires the next run to
        # reproduce its exact repair contract. When a source in that contract
        # cannot be made to pass, the contract is unsatisfiable and every
        # queued workflow refuses. This is the recorded way out.
        from okg.substrate.ingest.incomplete_recovery import (
            supersede_incomplete_checkpoint,
        )
        try:
            payload = supersede_incomplete_checkpoint(
                dsn=dsn,
                deployment=args.deployment,
                reason=args.reason,
                actor=args.actor,
                checkpoint_id=args.checkpoint_id,
                apply=bool(args.apply),
            )
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)
        except Exception as e:  # noqa: BLE001
            return _emit_error(SubstrateError(str(e)), as_json=as_json)
        if as_json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            verb = "superseded" if payload["applied"] else "would supersede"
            print(f"{verb} checkpoint {payload['checkpoint_id']}")
            print(f"  deployment: {payload['deployment']}")
            print(f"  recovery state: {payload['prior_state']} -> superseded")
            print(
                "  repair contract retired: "
                f"{', '.join(payload['required_sources']) or '(none)'}"
            )
            print(f"  reason: {payload['reason']} (by {payload['actor']})")
            if payload["applied"]:
                print(
                    "  the checkpoint stays blocked and nothing was deleted; "
                    "its sources resume from their own watermarks"
                )
            else:
                print(f"  nothing changed. To apply:\n    "
                      f"{payload['next_command']}")
        return 0

    if args.rt_subcmd == "deferred-edges":
        # Cross-source edges waiting for another source's record
        # (cross-source-endpoint-wedge). Read-only, operator-only: the id
        # sample is not served by any read operator.
        from okg.substrate.db.connection import connect as db_connect
        from okg.substrate.graph_store_internal.edge_deferrals import (
            deferred_edges_report,
        )
        from okg.substrate.deployments import find_deployment_dir
        from okg.substrate.publisher.deployment_stack import (
            load_deferral_policy,
        )

        deployment_dir = find_deployment_dir(args.deployment)
        warn_after = (
            load_deferral_policy(deployment_dir).warn_after_seconds
            if deployment_dir is not None else 24 * 3600
        )
        try:
            with db_connect(
                dsn, deployment=args.deployment, autocommit=True,
            ) as conn:
                payload = deferred_edges_report(
                    conn, warn_after_seconds=warn_after,
                )
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)
        payload["deployment"] = args.deployment
        if as_json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            print(f"deployment: {args.deployment}")
            print(f"waiting edges: {payload['total']}")
            for group in payload["groups"]:
                print(
                    f"  {group['producer']} {group['edge_type']} -> "
                    f"{group['endpoint_subtype']} [{group['state']}]: "
                    f"{group['count']}"
                )
            if payload["oldest_deferred_at"]:
                print(f"oldest deferred at: {payload['oldest_deferred_at']}")
            if payload["warn"]:
                print("warning: the oldest waiting edge is past the warning age")
            for item in payload.get("sample", []):
                print(
                    f"  {item['src']} {item['edge_type']} {item['dst']} "
                    f"({item['producer']}, {item['state']})"
                )
        return 0

    if args.rt_subcmd == "finish-undo":
        # Issue #2993: a failed source run whose undo did not finish blocks
        # every publish (`source_run_undo_unfinished`); this is the way out.
        # Like supersede-checkpoint it needs no runtime worker, since an
        # inline `okg ingest` can leave an undo unfinished too.
        from okg.substrate.ingest.run_base import finish_pending_undos
        try:
            payload = finish_pending_undos(
                dsn=dsn,
                deployment=args.deployment,
                run_id=args.run_id,
                accept_current=bool(args.accept_current),
                reason=args.reason,
                actor=args.actor,
                apply=bool(args.apply),
            )
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)
        if as_json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            print(f"unfinished undos: {len(payload['pending'])} "
                  f"(mode: {payload['mode']})")
            for item in payload["pending"]:
                print(f"  run {item['run_id']} of {item['source']} "
                      f"on {item['branch']}")
            for result in payload["results"]:
                print(f"  {result['run_id']}: {result['status']}"
                      + (f" ({result['error']['code']})"
                         if result.get("error") else ""))
            for skipped in payload.get("skipped") or ():
                print(f"  {skipped['run_id']}: skipped: {skipped['why']}")
            if not payload["applied"]:
                print(f"  nothing changed. To apply:\n    "
                      f"{payload['next_command']}")
            else:
                print(f"  still unfinished: {len(payload['still_pending'])}")
        still = payload.get("still_pending") or []
        return 1 if payload["applied"] and still else 0

    if args.rt_subcmd == "drain":
        from okg.substrate.runtime.admin import client_drain
        try:
            result = client_drain(
                deployment=args.deployment,
                dsn=dsn,
                max_wait_seconds=args.max_wait_seconds,
                # Operator-facing drain: report the queue as soon as it reads
                # empty. The settle window belongs to the deployment gate
                # (`runtime quiesce`), which passes its own.
                stable_zero_seconds=0.0,
            )
        except Exception as e:
            return _emit_error(SubstrateError(str(e)), as_json=as_json)
        payload = {
            "deployment": result.deployment,
            "drained": result.drained,
            "in_flight_at_timeout": result.in_flight_at_timeout,
            "elapsed_seconds": result.elapsed_seconds,
            "pending_workflows": result.pending_workflows,
        }
        if as_json:
            print(json.dumps(payload, indent=2))
        else:
            status = "drained" if result.drained else "TIMEOUT"
            print(
                f"runtime drain: {result.deployment} — {status} "
                f"(elapsed={result.elapsed_seconds:.1f}s)"
            )
            if not result.drained:
                print(
                    f"  in_flight_at_timeout="
                    f"{result.in_flight_at_timeout}"
                )
                for wf in result.pending_workflows:
                    print(
                        "  pending_workflow: "
                        f"{wf.get('workflow_id')} "
                        f"name={wf.get('name')} "
                        f"status={wf.get('status')}"
                    )
        return 0 if result.drained else 1

    if args.rt_subcmd == "quiesce":
        from pathlib import Path

        from okg.substrate.errors import SubstrateError
        from okg.substrate.runtime.quiescence import (
            quiesce_runtime,
            write_receipt,
        )

        try:
            payload = quiesce_runtime(
                deployment=args.deployment,
                dsn=dsn,
                owner=args.owner,
                candidate_revision=args.candidate_revision,
                lifecycle_plan_digest=args.lifecycle_plan_digest,
                lifecycle_contract_digest=args.lifecycle_contract_digest,
                max_wait_seconds=args.max_wait_seconds,
                poll_interval_seconds=args.poll_interval_seconds,
                stable_zero_seconds=args.stable_zero_seconds,
                bootstrap_quiescence=bool(args.bootstrap_quiescence),
            )
            if args.receipt:
                write_receipt(Path(args.receipt), payload)
        except Exception as e:
            return _emit_error(SubstrateError(str(e)), as_json=as_json)
        if as_json:
            print(json.dumps(payload, indent=2))
        else:
            print(
                f"runtime quiesce: {payload['deployment']} — "
                f"{payload['status']} owner={payload['owner']}"
            )
        return 0 if payload["ok"] else 1

    if args.rt_subcmd == "fence-status":
        from okg.substrate.errors import SubstrateError
        from okg.substrate.runtime.promotion_fence import (
            promotion_fence_status,
            promotion_schedule_snapshot_digest,
        )

        try:
            status = promotion_fence_status(
                deployment=args.deployment,
                dsn=dsn,
            )
        except Exception as e:
            return _emit_error(SubstrateError(str(e)), as_json=as_json)
        payload: dict[str, Any] = {
            "ok": True,
            "deployment": args.deployment,
            "fence": None,
        }
        if isinstance(status, dict):
            snapshot = status.pop("schedule_snapshot", [])
            status["schedule_snapshot_digest"] = (
                promotion_schedule_snapshot_digest(snapshot)
            )
            payload["fence"] = status
        if as_json:
            print(json.dumps(payload, indent=2))
        else:
            state = (
                payload["fence"].get("status")
                if isinstance(payload["fence"], dict)
                else "absent"
            )
            print(f"runtime fence: {args.deployment} — {state}")
        return 0

    if args.rt_subcmd == "measure-recovery":
        from okg.substrate.runtime.recovery_test import (
            measure_kill9_recovery,
            measure_sigstop_behavior,
            measure_split_brain,
        )
        if args.mode == "kill9":
            r = measure_kill9_recovery(
                deployment=args.deployment, dsn=dsn,
                runtime=manifest.runtime,
            )
        elif args.mode == "sigstop":
            r = measure_sigstop_behavior(
                deployment=args.deployment, dsn=dsn,
                runtime=manifest.runtime,
            )
        else:  # split-brain
            r = measure_split_brain(
                deployment=args.deployment, dsn=dsn,
                runtime=manifest.runtime,
            )
        payload = {
            "mode": r.mode,
            "deployment": r.deployment,
            "ok": r.ok,
            "recovery_seconds": r.recovery_seconds,
            "notes": r.notes,
            "raw": r.raw,
        }
        if as_json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            status = "PASS" if r.ok else "FAIL"
            recovery = (
                f"{r.recovery_seconds:.1f}s"
                if r.recovery_seconds is not None else "n/a"
            )
            print(
                f"measure-recovery [{r.mode}] {r.deployment} — "
                f"{status} (recovery={recovery})"
            )
            print(f"  {r.notes}")
        return 0 if r.ok else 1

    if args.rt_subcmd == "escalation":
        return _cmd_runtime_escalation(args, dsn=dsn, as_json=as_json)

    return _emit_error(
        SubstrateError(
            f"okg runtime {args.rt_subcmd!r} unrecognized",
        ),
        as_json=as_json,
    )


def _source_sync_debug_summary(
    *,
    report,
    deployment: str,
    profile_path: str | None,
    coerce_dt,
) -> dict:
    from okg.substrate.source_sync_enqueue import (
        source_sync_payload_from_dbos_inputs,
    )

    raw_inputs = getattr(report, "inputs", None)
    serialization = getattr(report, "serialization", None)
    decoded = None
    if raw_inputs:
        decoded = source_sync_payload_from_dbos_inputs(
            deployment=deployment,
            inputs=raw_inputs,
            serialization=serialization,
        )
    decode_status = (
        "decoded"
        if decoded is not None else
        "missing_inputs"
        if not raw_inputs else
        "decode_failed"
    )
    decoded = decoded or {}
    events = decoded.get("events")
    if not isinstance(events, list):
        events = []
    output = getattr(report, "output", None)
    ingest_report = (
        output.get("report")
        if isinstance(output, dict) and isinstance(output.get("report"), dict)
        else {}
    )
    source_results = (
        ingest_report.get("results")
        if isinstance(ingest_report.get("results"), list) else []
    )
    source_timings = []
    for result in source_results:
        if not isinstance(result, dict):
            continue
        timings = result.get("timings_ms")
        timing_available = isinstance(timings, dict) and bool(timings)
        timing_unavailable_reason = None
        if not timing_available:
            timing_unavailable_reason = (
                str(result.get("error"))
                if result.get("error") else
                "source_result_missing_timings"
            )
        source_timings.append({
            "name": result.get("name"),
            "status": result.get("status"),
            "run_mode": result.get("run_mode"),
            "elapsed_s": result.get("elapsed_s"),
            "timing_status": result.get("timing_status"),
            "timings_ms": timings if isinstance(timings, dict) else {},
            "timing_unavailable_reason": timing_unavailable_reason,
            "nodes_appended": result.get("nodes_appended"),
            "edges_appended": result.get("edges_appended"),
            "source_stats": (
                result.get("source_stats")
                if isinstance(result.get("source_stats"), dict) else {}
            ),
        })
    timed_count = sum(
        1 for row in source_timings
        if isinstance(row.get("timings_ms"), dict) and row["timings_ms"]
    )
    stage_timing_status = "available" if timed_count else "missing"
    stage_timing_reason = None
    if not timed_count:
        status = str(getattr(report, "status", "") or "").upper()
        if status in {"ENQUEUED", "PENDING"}:
            stage_timing_reason = "workflow_not_finished"
        elif not source_timings:
            stage_timing_reason = "workflow_output_missing_source_results"
        else:
            stage_timing_reason = "source_results_missing_timings"
    requested_profile = bool(
        decoded.get("profile_force_deep")
        or decoded.get("profile_collectors")
    )
    return {
        "decode_status": decode_status,
        "deployment": decoded.get("deployment") or deployment,
        "sources": decoded.get("sources") or [],
        "run_mode": decoded.get("run_mode"),
        "publish": decoded.get("publish"),
        "include_predecessors": decoded.get("include_predecessors"),
        "include_dependents": decoded.get("include_dependents"),
        "dependent_fanout": decoded.get("dependent_fanout"),
        "profile_force_deep": decoded.get("profile_force_deep"),
        "profile_collectors": decoded.get("profile_collectors") or [],
        "event_count": len(events),
        "event_sample": _source_sync_event_sample(events),
        "queue_dwell_ms": _queue_dwell_ms(report, coerce_dt),
        "stage_timing": {
            "status": stage_timing_status,
            "unavailable_reason": stage_timing_reason,
            "source_count": len(source_timings),
            "timed_source_count": timed_count,
            "sources": source_timings,
            "publishes": (
                ingest_report.get("publishes")
                if isinstance(ingest_report.get("publishes"), list)
                else []
            ),
        },
        "profile_availability": {
            "status": (
                "available"
                if profile_path else
                "requested_missing"
                if requested_profile else
                "not_requested"
            ),
            "path": profile_path,
        },
        "dbos": {
            "deduplication_id": getattr(report, "deduplication_id", None),
            "executor_id": getattr(report, "executor_id", None),
            "application_version": getattr(
                report, "application_version", None,
            ),
            "queue_name": getattr(report, "queue_name", None),
        },
        "safe_recovery": _source_sync_recovery_commands(
            deployment=deployment,
            workflow_id=report.workflow_id,
            queue_name=getattr(report, "queue_name", None),
        ),
    }


def _source_sync_event_sample(events: list[dict]) -> list[dict]:
    sample: list[dict] = []
    for event in events[:5]:
        if not isinstance(event, dict):
            continue
        scope = event.get("scope")
        scope_keys = []
        if isinstance(scope, dict):
            for key in ("path", "record_id", "revision_hint"):
                value = scope.get(key)
                if isinstance(value, list):
                    scope_keys.extend(str(item) for item in value[:3])
                elif value:
                    scope_keys.append(str(value))
        scope_key = event.get("scope_key")
        if scope_key:
            scope_keys.append(str(scope_key))
        revision_hint = event.get("revision_hint")
        if revision_hint:
            scope_keys.append(str(revision_hint))
        sample.append({
            "event_id": event.get("event_id"),
            "source": event.get("source_name") or event.get("source"),
            "trigger_kind": event.get("trigger_kind"),
            "trigger_reason": (
                event.get("reason") or event.get("trigger_reason")
            ),
            "observed_at": event.get("observed_at"),
            "scope_sample": scope_keys[:5],
        })
    return sample


def _queue_dwell_ms(report, coerce_dt) -> int | None:
    created_at = coerce_dt(getattr(report, "created_at", None))
    if created_at is None:
        created_at = coerce_dt(getattr(report, "started_at", None))
    starts = []
    for step in getattr(report, "steps", []) or []:
        if not isinstance(step, dict):
            continue
        started = coerce_dt(step.get("started_at"))
        if started is not None:
            starts.append(started)
    if created_at is None:
        return None
    if not starts:
        status = str(getattr(report, "status", "") or "").upper()
        if status in {"ENQUEUED", "PENDING"}:
            from datetime import datetime, timezone
            dwell = (
                datetime.now(timezone.utc) - created_at.astimezone(timezone.utc)
            ).total_seconds() * 1000
            return max(0, int(dwell))
        return None
    dwell = (min(starts) - created_at).total_seconds() * 1000
    return max(0, int(dwell))


def _source_sync_recovery_commands(
    *,
    deployment: str,
    workflow_id: str,
    queue_name: str | None,
) -> dict:
    commands = {
        "debug_workflow": (
            f"{okg_prefix()} runtime debug-workflow "
            f"--deployment {deployment} {workflow_id} --json"
        ),
        "source_sync_health": (
            f"{okg_prefix()} sync health --deployment {deployment} --json"
        ),
    }
    if queue_name:
        commands["dbos_gc_dry_run"] = (
            f"{okg_prefix()} db dbos-gc "
            f"--deployment {deployment} "
            "--stale-after-seconds 600 "
            f"--queue-name {queue_name} "
            "--workflow-name okg_source_sync --json"
        )
    return commands


def _cmd_runtime_escalation(
    args: argparse.Namespace, *, dsn: str, as_json: bool,
) -> int:
    """`okg runtime escalation ...` dispatcher.

    Phase 2 §1.4.2 ships ``respond``. Subsequent phases add ``list``
    and ``show`` for operator triage.
    """
    import json

    from okg.substrate.cli.main import _current_user, _emit_error
    from okg.substrate.errors import SubstrateError

    if args.esc_subcmd == "respond":
        from okg.substrate.db.connection import connect as db_connect
        from okg.substrate.runtime.escalation import (
            EscalationNotFoundError,
            EscalationStateError,
        )
        from okg.substrate.runtime.escalation_response import (
            InvalidDecisionPayload,
            respond_to_escalation,
        )

        # Parse the --decision payload (JSON string).
        try:
            decision = json.loads(args.decision)
        except json.JSONDecodeError as e:
            return _emit_error(
                SubstrateError(
                    f"--decision is not valid JSON: {e.msg} (at "
                    f"char {e.pos})",
                    fix_hint="e.g. --decision '{\"label\": \"stale\"}'",
                ),
                as_json=as_json,
            )
        if not isinstance(decision, dict):
            return _emit_error(
                SubstrateError(
                    "--decision must be a JSON object (got "
                    f"{type(decision).__name__})",
                ),
                as_json=as_json,
            )

        decided_by = args.decided_by or f"cli:{_current_user()}"

        try:
            with db_connect(dsn) as conn:
                result = respond_to_escalation(
                    conn,
                    deployment=args.deployment,
                    escalation_id=args.escalation_id,
                    decision=decision,
                    decided_by=decided_by,
                    send_wake=not args.no_send_wake,
                )
        except EscalationNotFoundError as e:
            return _emit_error(
                SubstrateError(
                    f"no escalation with id {e.args[0]!r} in deployment "
                    f"{args.deployment!r} at status pending or routed",
                ),
                as_json=as_json,
            )
        except EscalationStateError as e:
            return _emit_error(SubstrateError(str(e)), as_json=as_json)
        except InvalidDecisionPayload as e:
            return _emit_error(
                SubstrateError(
                    f"decision rejected: {e}",
                    fix_hint=(
                        "match the decision to the schema named by the "
                        "escalation's decision_schema_ref (its "
                        "okg.escalation_live row; no okg command shows "
                        "an escalation yet)"
                    ),
                ),
                as_json=as_json,
            )

        payload = {
            "escalation_id": result.escalation_id,
            "workflow_id": result.workflow_id,
            "status": result.new_status,
            "decided_at": result.decided_at,
            "decided_by": result.decided_by,
            "wake_sent": result.wake_sent,
        }
        if as_json:
            print(json.dumps(payload, indent=2))
        else:
            print(
                f"escalation respond: {result.escalation_id} → "
                f"{result.new_status} "
                f"(wake_sent={result.wake_sent})"
            )
            print(f"  workflow_id: {result.workflow_id}")
            print(f"  decided_by:  {result.decided_by}")
            print(f"  decided_at:  {result.decided_at}")
        return 0

    return _emit_error(
        SubstrateError(
            f"okg runtime escalation {args.esc_subcmd!r} unrecognized",
        ),
        as_json=as_json,
    )



__all__ = ["_cmd_runtime", "_cmd_runtime_escalation"]
