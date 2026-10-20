"""DBOS runtime status and source-sync enqueue surfaces."""
from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from typing import Any, Mapping

import psycopg

from okg.substrate.cli.dsn_authority import resolve_read_dsn
from okg.substrate.db.management import (
    JOB_RUNTIME_PROCEDURES,
    JOB_RUNTIME_RELATIONS,
    job_runtime_cron_schedules,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.db.connection import connect as db_connect
from okg.substrate.support.user_hints import okg_prefix


_DBOS_OKG_WORKFLOWS = {
    "okg_publisher_cycle",
    "okg_source_sync",
    "okg_embed_batch",
}


def _emit(payload: Mapping[str, Any], *, as_json: bool) -> None:
    if as_json or os.environ.get("OKG_AGENT") == "1":
        print(json.dumps(payload, sort_keys=True, default=str))
        return
    print(f"jobs {payload.get('surface')}: {payload.get('status')}")


def _emit_error(err: SubstrateError, *, as_json: bool) -> int:
    if as_json or os.environ.get("OKG_AGENT") == "1":
        print(json.dumps(err.to_json(), sort_keys=True), file=sys.stderr)
        return 1
    fix = f"\n  fix_hint: {err.fix_hint}" if err.fix_hint else ""
    print(f"error [{err.code}]: {err.message}{fix}", file=sys.stderr)
    if err.data:
        print(f"  data: {err.data}", file=sys.stderr)
    return 1


def _relation_exists(cur: psycopg.Cursor, relation: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (relation,))
    return cur.fetchone()[0] is not None


def _procedure_exists(cur: psycopg.Cursor, procedure: str) -> bool:
    cur.execute("SELECT to_regprocedure(%s)", (procedure,))
    return cur.fetchone()[0] is not None


def _dbos_workflow_status_report(cur: psycopg.Cursor) -> dict[str, Any]:
    if not _relation_exists(cur, "dbos.workflow_status"):
        return {
            "dbos_workflow_status_installed": False,
            "dbos_workflow_counts": [],
            "dbos_source_sync_counts": [],
            "dbos_publisher_counts": [],
            "dbos_embed_counts": [],
        }

    cur.execute(
        """
        SELECT name, status, count(*)
          FROM dbos.workflow_status
         WHERE name = ANY(%s)
            OR name LIKE %s ESCAPE '\\'
            OR name LIKE %s ESCAPE '\\'
            OR name LIKE %s ESCAPE '\\'
            OR name LIKE %s ESCAPE '\\'
         GROUP BY name, status
         ORDER BY name, status
        """,
        (
            sorted(_DBOS_OKG_WORKFLOWS),
            r"okg\_source\_poll\_%",
            r"okg\_admission\_%",
            r"okg\_escalation\_%",
            r"okg\_lens\_%",
        ),
    )
    workflow_counts = [
        {"workflow_name": row[0], "status": row[1], "count": int(row[2])}
        for row in cur.fetchall()
    ]

    def counts_for(workflow_name: str) -> list[dict[str, Any]]:
        return [
            {"status": row["status"], "count": row["count"]}
            for row in workflow_counts
            if row["workflow_name"] == workflow_name
        ]

    return {
        "dbos_workflow_status_installed": True,
        "dbos_workflow_counts": workflow_counts,
        "dbos_source_sync_counts": counts_for("okg_source_sync"),
        "dbos_publisher_counts": counts_for("okg_publisher_cycle"),
        "dbos_embed_counts": counts_for("okg_embed_batch"),
    }


#: Plain-language reading of each `okg jobs status` verdict, and in
#: particular whether it blocks the operator. A newcomer met `status:
#: unavailable` with exit 1 on a perfectly working deployment and could not
#: tell whether it mattered (issue #2128, finding F16).
_STATUS_MEANINGS = {
    "unavailable": (
        "no background worker runtime is installed on this deployment "
        "(DBOS workflow tables are absent). This does NOT block install, "
        "search or chat, and it does not block ingest or publish: "
        "`okg ingest` and `okg run --once --apply` run synchronously in "
        "your own process. Install a worker only if you want scheduled "
        "background jobs -- `okg runtime bootstrap --deployment <name>`, "
        "then `okg runtime worker --deployment <name>`"
    ),
    "cleanup_required": (
        "the background worker runtime is installed, but objects from the "
        "retired in-database job runtime are still present; they are "
        "listed under obsolete_objects_present and "
        "obsolete_cron_schedules_present"
    ),
    "clean": (
        "the background worker runtime is installed and carries no "
        "retired job-runtime objects"
    ),
}


def _status_meaning(status: str) -> str:
    """Plain reading of `status`, including whether it blocks anything."""
    return _STATUS_MEANINGS.get(
        status, f"unrecognized jobs-status verdict {status!r}"
    )


def _status_report(dsn: str, *, deployment: str | None) -> dict[str, Any]:
    with db_connect(dsn) as conn, conn.cursor() as cur:
        dbos_status = _dbos_workflow_status_report(cur)
        obsolete_objects_present = [
            relation
            for relation in JOB_RUNTIME_RELATIONS
            if _relation_exists(cur, relation)
        ]
        obsolete_objects_present.extend(
            procedure
            for procedure in JOB_RUNTIME_PROCEDURES
            if _procedure_exists(cur, procedure)
        )
        obsolete_cron_schedules_present = job_runtime_cron_schedules(cur)
    dbos_available = bool(dbos_status["dbos_workflow_status_installed"])
    cleanup_required = bool(
        obsolete_objects_present or obsolete_cron_schedules_present
    )
    status = (
        "unavailable"
        if not dbos_available
        else "cleanup_required"
        if cleanup_required
        else "clean"
    )
    return {
        "schema": "okg.dbos_runtime.status.v1",
        "surface": "status",
        "status": status,
        "status_meaning": _status_meaning(status),
        "ok": dbos_available and not cleanup_required,
        "deployment": deployment,
        "execution_authority": "dbos.workflow_status",
        "source_progress_authority": "okg.source_sync_progress_and_watermarks",
        "okg_job_runtime_subsystem": (
            "cleanup_required" if cleanup_required else "absent"
        ),
        "obsolete_objects_present": obsolete_objects_present,
        "obsolete_cron_schedules_present": obsolete_cron_schedules_present,
        **dbos_status,
    }


def _cmd_status(args: argparse.Namespace, *, as_json: bool) -> int:
    resolution = resolve_read_dsn(args, surface="jobs.status")
    payload = _status_report(
        resolution.dsn,
        deployment=resolution.deployment,
    )
    payload["authority"] = resolution.authority
    payload["target"] = resolution.target
    _emit(payload, as_json=as_json)
    return 0 if payload["ok"] else 1


def _runtime_deployment(args: argparse.Namespace, resolution, *, command: str) -> str:
    """The runtime deployment whose queue a job command addresses.

    ``--runtime-deployment``, then the deployment the DSN was resolved for,
    then the shared default rule (``OKG_DEPLOYMENT``, then the checkout's
    configured ``graph_projection.deployment``); never a hard-coded name. With
    a bare ``--dsn`` the configured default still names the queue, as before;
    outside a configured checkout this now refuses instead.
    """
    from okg.substrate.deployment_default import resolve_command_deployment

    return resolve_command_deployment(
        args.runtime_deployment or resolution.deployment,
        command=command,
    )


def _cmd_enqueue_source(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.cli.apply_boundary import missing_apply_error

    if err := missing_apply_error(
        args,
        command="jobs enqueue-source",
        fix_command="okg jobs enqueue-source --apply ...",
    ):
        return _emit_error(err, as_json=as_json)

    from okg.substrate.source_sync import SourceSyncEvent, SourceSyncJob
    from okg.substrate.source_sync_enqueue import enqueue_source_sync_via_dbos

    resolution = resolve_read_dsn(args, surface="jobs.enqueue-source")
    try:
        runtime_deployment = _runtime_deployment(
            args, resolution, command="okg jobs enqueue-source"
        )
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    event = SourceSyncEvent(
        deployment=runtime_deployment,
        source_name=args.source,
        trigger_kind="manual",
        scope_key=args.scope_key,
        reason="operator_scope",
    )
    job = SourceSyncJob(
        deployment=runtime_deployment,
        sources=(args.source,),
        run_mode=args.run_mode,
        events=(event,),
        dependent_fanout=args.dependent_fanout,
        publish=not args.no_publish,
        supersedes_workflow_id=getattr(args, "supersedes_workflow_id", None),
    )
    deduplication_id = (
        args.run_key or f"source-sync:{runtime_deployment}:{uuid.uuid4().hex}"
    )
    try:
        workflow_id = enqueue_source_sync_via_dbos(
            deployment=runtime_deployment,
            dsn=resolution.dsn,
            job=job,
            deduplication_id=deduplication_id,
            promotion_id=getattr(args, "promotion_id", None),
        )
    except Exception as exc:  # noqa: BLE001
        return _emit_error(
            SubstrateError(
                f"DBOS-path source-sync enqueue failed: {exc}",
                data={
                    "deployment": resolution.deployment,
                    "runtime_deployment": runtime_deployment,
                    "source": args.source,
                    "deduplication_id": deduplication_id,
                },
                fix_hint=(
                    f"check the DBOS runtime worker with `{okg_prefix()} dogfood status "
                    f"--deployment {runtime_deployment} --json`; this command "
                    "does not fall back to an OKG job executor"
                ),
            ),
            as_json=as_json,
        )
    payload = {
        "schema": "okg.source_sync.enqueue.v1",
        "surface": "enqueue-source",
        "status": "ok",
        "ok": True,
        "deployment": resolution.deployment,
        "runtime_deployment": runtime_deployment,
        "source": args.source,
        "deduplication_id": deduplication_id,
        "dbos_workflow_id": workflow_id,
        "supersedes_workflow_id": getattr(args, "supersedes_workflow_id", None),
        "promotion_id": getattr(args, "promotion_id", None),
        "execution_authority": "dbos.workflow_status",
        "authority": resolution.authority,
        "target": resolution.target,
    }
    _emit(payload, as_json=as_json)
    return 0


def _cmd_find_source_run(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.runtime.dbos_enqueue import workflow_id_for_deduplication
    from okg.substrate.runtime.partition_queue import source_sync_queue_name

    resolution = resolve_read_dsn(args, surface="jobs.find-source-run")
    try:
        runtime_deployment = _runtime_deployment(
            args, resolution, command="okg jobs find-source-run"
        )
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    workflow_id = workflow_id_for_deduplication(
        dsn=resolution.dsn,
        queue_name=source_sync_queue_name(runtime_deployment),
        deduplication_id=args.run_key,
    )
    payload = {
        "schema": "okg.source_sync.find_run.v1",
        "ok": True,
        "runtime_deployment": runtime_deployment,
        "run_key": args.run_key,
        "dbos_workflow_id": workflow_id,
    }
    _emit(payload, as_json=as_json)
    return 0


def _cmd_jobs(args: argparse.Namespace, *, as_json: bool) -> int:
    if args.jobs_subcmd == "status":
        return _cmd_status(args, as_json=as_json)
    if args.jobs_subcmd == "enqueue-source":
        return _cmd_enqueue_source(args, as_json=as_json)
    if args.jobs_subcmd == "find-source-run":
        return _cmd_find_source_run(args, as_json=as_json)
    raise SystemExit(f"unknown jobs subcommand: {args.jobs_subcmd!r}")


__all__ = ["_cmd_jobs"]
