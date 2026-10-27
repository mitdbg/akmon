"""Dogfood runtime control-plane CLI.

This module is the operator/agent front door for using an OKG deployment as
the development coordination source of truth. It deliberately composes the
existing readiness and source-sync contracts instead of creating a second
health model.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shlex
import signal
import shutil
import subprocess
import time
from html import escape as _xml_escape
from pathlib import Path
from typing import Any, Callable, Mapping

from okg.substrate.cli._options import add_deployment_arg
from okg.substrate.cli._output import emit_result
from okg.substrate.cli.deployment import (
    _READY_DOGFOOD_MAX_PRODUCER_AGE_SECONDS,
    _deployment_ready_env_status,
    _deployment_ready_live_checks,
    _dogfood_local_profile_command_prefix,
    _ready_load_yaml_mapping,
    _ready_nomos_active_state_message,
    _ready_nomos_active_state_status,
)
from okg.substrate.cli.deployment_identifier import (
    IDENTIFIER_KIND_MISMATCH,
    require_graph_deployment_name as _require_graph_deployment_name,
)
from okg.substrate.db.wal_settings import GRAPH_WAL_SETTINGS, graph_wal_command_args
from okg.substrate.deployments.readiness_profile import MAX_SOURCE_AGE_SECONDS
from okg.substrate.errors import SubstrateError
from okg.substrate.runtime.host_headroom import (
    RECLAIM_RUNBOOK as _HOST_STORAGE_RECLAIM_RUNBOOK,
)
from okg.substrate.source_sync_recovery import (
    plan_stale_source_sync_work_recovery as _plan_stale_source_sync_work_recovery,
    source_sync_dbos_workflows as _source_sync_dbos_workflows,
    source_sync_recovery_queue_name as _source_sync_recovery_queue_name,
)
from okg.substrate.support.user_hints import okg_prefix


JsonFlagAdder = Callable[[argparse.ArgumentParser], None]

DOGFOOD_STATUS_SCHEMA = "okg.dogfood.runtime_status.v1"
DOGFOOD_CONTROL_SCHEMA = "okg.dogfood.runtime_control.v1"
DOGFOOD_SERVICE_ADAPTER_SCHEMA = "okg.dogfood.service_adapter.v1"
DOGFOOD_DEPLOYMENT_HELP = (
    "Dogfood deployment name (default: OKG_DEPLOYMENT, else "
    "graph_projection.deployment in the checkout's .okg/coordination.yaml; "
    "refuses when none is set)"
)


def _dogfood_deployment(args: argparse.Namespace) -> str:
    """The deployment a dogfood command acts on; never a hard-coded name."""
    from okg.substrate.deployment_default import resolve_command_deployment

    subcmd = str(getattr(args, "dogfood_subcmd", "") or "").strip()
    return resolve_command_deployment(
        getattr(args, "deployment", None),
        command=f"okg dogfood {subcmd}".strip(),
    )
SERVICE_ADAPTERS = ("auto", "launchd", "systemd", "docker-compose", "foreground")
RENDERABLE_SERVICE_ADAPTERS = ("launchd", "systemd", "docker-compose")
# Supervised shutdown has sequential producer joins, stale-work cleanup, a 30s
# active-DBOS wait, and a 5s destroy wait. Keep one conservative budget across
# adapters and orphan cleanup so normal SIGTERM handling is not cut short.
WORKER_GRACEFUL_SHUTDOWN_SECONDS = 120.0


def register_subparsers(
    sub: argparse._SubParsersAction,
    *,
    add_json_flag: JsonFlagAdder,
) -> None:
    p_dogfood = sub.add_parser(
        "dogfood",
        help="Inspect OKG dogfood runtime authority",
    )
    add_json_flag(p_dogfood)
    p_sub = p_dogfood.add_subparsers(dest="dogfood_subcmd", required=True)

    p_status = p_sub.add_parser(
        "status",
        help="Report graph, runtime-worker, source-sync, and queue trust state",
    )
    add_json_flag(p_status)
    _add_status_args(p_status)

    p_doctor = p_sub.add_parser(
        "doctor",
        help="Run the dogfood runtime status contract with full details",
    )
    add_json_flag(p_doctor)
    _add_status_args(p_doctor)

    for action in ("up", "down", "restart"):
        p_action = p_sub.add_parser(
            action,
            help=f"Plan or execute dogfood runtime worker {action}",
        )
        add_json_flag(p_action)
        _add_control_args(p_action, action=action)

    p_service = p_sub.add_parser(
        "service",
        help="Render or install runtime-worker supervisor adapters",
    )
    add_json_flag(p_service)
    p_service_sub = p_service.add_subparsers(
        dest="dogfood_service_subcmd",
        required=True,
    )
    p_render = p_service_sub.add_parser(
        "render",
        help="Render a service adapter without writing it",
    )
    add_json_flag(p_render)
    _add_service_adapter_args(p_render, include_output=False)
    p_install = p_service_sub.add_parser(
        "install",
        help="Write a rendered service adapter when --apply is supplied",
    )
    add_json_flag(p_install)
    _add_service_adapter_args(p_install, include_output=True)


def _add_status_args(parser: argparse.ArgumentParser) -> None:
    add_deployment_arg(
        parser,
        default=None,
        help=DOGFOOD_DEPLOYMENT_HELP,
    )
    parser.add_argument(
        "--max-active-queue-depth",
        type=int,
        default=0,
        help="Maximum non-DLQ substrate queue depth accepted as ready",
    )
    parser.add_argument(
        "--max-source-age-seconds",
        type=int,
        default=None,
        help=(
            "Maximum age for required source watermarks (default: the "
            "deployment's readiness profile limit: dogfood 1 h, ci 6 h, "
            "standard 14 days)"
        ),
    )
    parser.add_argument(
        "--max-producer-age-seconds",
        type=int,
        default=_READY_DOGFOOD_MAX_PRODUCER_AGE_SECONDS,
        help="Maximum age for required source-sync producer heartbeats",
    )
    parser.add_argument(
        "--skip-doctor",
        action="store_true",
        help="Skip the deployment doctor subprocess",
    )
    parser.add_argument(
        "--show-values",
        action="store_true",
        help="Show resolved env values instead of redacting them",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Include the full underlying dogfood readiness payload",
    )


def _add_control_args(parser: argparse.ArgumentParser, *, action: str) -> None:
    add_deployment_arg(
        parser,
        default=None,
        help=DOGFOOD_DEPLOYMENT_HELP,
    )
    parser.add_argument(
        "--adapter",
        choices=SERVICE_ADAPTERS,
        default="auto",
        help="Supervisor adapter to target (default: auto)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=f"Execute the planned {action} command(s)",
    )
    parser.add_argument(
        "--launchd-plist",
        default=None,
        help="launchd plist path override",
    )
    parser.add_argument(
        "--systemd-scope",
        choices=("system", "user"),
        default="system",
        help="systemd scope for generated commands",
    )


def _add_service_adapter_args(
    parser: argparse.ArgumentParser,
    *,
    include_output: bool,
) -> None:
    add_deployment_arg(
        parser,
        default=None,
        help=DOGFOOD_DEPLOYMENT_HELP,
    )
    parser.add_argument(
        "--adapter",
        choices=RENDERABLE_SERVICE_ADAPTERS,
        required=True,
        help="Service adapter to render",
    )
    parser.add_argument(
        "--repo-root",
        default=str(Path.cwd()),
        help="Repository root used as the worker working directory",
    )
    parser.add_argument(
        "--okg-bin",
        # Executed, not a hint: it becomes the command in the rendered compose
        # (the repo-mounted okg-runtime image), systemd and launchd services,
        # which run the checkout's uv project however okg is installed here.
        default="uv run okg",
        help="Command used to invoke okg in the service adapter",
    )
    parser.add_argument(
        "--dsn",
        default="${OKG_DSN}",
        help="DSN value or placeholder for adapters that need one",
    )
    parser.add_argument(
        "--postgres-image",
        default="okg-pg17:local",
        help="Docker image for the dedicated dogfood Postgres service",
    )
    parser.add_argument(
        "--postgres-host-port",
        type=int,
        default=55434,
        help="Host port for the dedicated dogfood Postgres service",
    )
    parser.add_argument(
        "--postgres-db",
        default=None,
        help="Database name for the dedicated dogfood Postgres service",
    )
    parser.add_argument(
        "--postgres-password",
        default="okg",
        help="Postgres superuser password for local dogfood compose",
    )
    parser.add_argument(
        "--postgres-memory-limit",
        default="6g",
        help="Docker memory limit for the dedicated dogfood Postgres service",
    )
    parser.add_argument(
        "--postgres-shm-size",
        default="2gb",
        help="Docker /dev/shm size for the dedicated dogfood Postgres service",
    )
    parser.add_argument(
        "--postgres-max-connections",
        type=int,
        default=200,
        help=(
            "Postgres max_connections for the dedicated dogfood service. The "
            "ingest worker opens up to 52 connections and each agent-session "
            "tool server up to 16, so the stock 100 runs out with a few "
            "agent sessions open."
        ),
    )
    parser.add_argument(
        "--postgres-max-worker-processes",
        type=int,
        default=32,
        help="Postgres max_worker_processes for the dedicated dogfood service",
    )
    parser.add_argument(
        "--postgres-timescaledb-workers",
        type=int,
        default=8,
        help="TimescaleDB background workers for the dedicated dogfood service",
    )
    parser.add_argument(
        "--postgres-shared-buffers",
        default="512MB",
        help=(
            "Postgres shared_buffers for the dedicated dogfood service. The "
            "initdb default (128MB) starves a large graph DB; size this to the "
            "container memory limit."
        ),
    )
    parser.add_argument(
        "--postgres-max-locks-per-transaction",
        type=int,
        default=1024,
        help=(
            "Postgres max_locks_per_transaction for the dedicated dogfood "
            "service. The shared lock table holds max_locks_per_transaction * "
            "(max_connections + max_prepared_transactions) entries; the 64 "
            "default exhausts under large concurrent publishes ('out of shared "
            "memory')."
        ),
    )
    parser.add_argument(
        "--postgres-maintenance-work-mem",
        default="256MB",
        help=(
            "Postgres maintenance_work_mem for the dedicated dogfood service "
            "(vacuum / index build / TimescaleDB compression)."
        ),
    )
    parser.add_argument(
        "--env-file",
        default="/etc/okg/runtime-worker.env",
        help="systemd EnvironmentFile path",
    )
    parser.add_argument(
        "--user",
        default="okg",
        help="systemd service user",
    )
    parser.add_argument(
        "--group",
        default="okg",
        help="systemd service group",
    )
    if include_output:
        parser.add_argument(
            "--output",
            default=None,
            help="Output path for install; defaults to adapter destination",
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Write the rendered service file",
        )


def cmd_dogfood(args: argparse.Namespace, *, as_json: bool) -> int:
    if args.dogfood_subcmd == "status":
        return _cmd_dogfood_status(args, as_json=as_json)
    if args.dogfood_subcmd == "doctor":
        args.details = True
        return _cmd_dogfood_status(args, as_json=as_json)
    if args.dogfood_subcmd in {"up", "down", "restart"}:
        return _cmd_dogfood_control(args, as_json=as_json)
    if args.dogfood_subcmd == "service":
        return _cmd_dogfood_service(args, as_json=as_json)
    raise AssertionError(f"unhandled dogfood subcommand: {args.dogfood_subcmd}")


def _cmd_dogfood_status(args: argparse.Namespace, *, as_json: bool) -> int:
    try:
        deployment = _dogfood_deployment(args)
    except SubstrateError as exc:
        from okg.substrate.cli.main import _emit_error

        return _emit_error(exc, as_json=as_json)
    # Type the identifier BEFORE any probe (#1289). Forwarding an
    # environment id into the live checks fabricated five blockers against
    # a healthy deployment; substituting the manifest name silently would
    # hide which identity actually ran. Refuse and name both.
    try:
        _require_graph_deployment_name(
            deployment,
            correct_command=(
                f"env -u OKG_DSN {okg_prefix()} dogfood status "
                "--deployment {deployment} --json"
            ),
        )
    except SubstrateError as exc:
        payload = _unavailable_payload(
            deployment=deployment,
            reason=IDENTIFIER_KIND_MISMATCH,
            message=str(exc),
        )
        # Bind the two identities separately; never merge them.
        data = dict(getattr(exc, "data", None) or {})
        payload.update(data)
        correct = str(data.get("graph_deployment_name") or "")
        correct_command = (
            f"env -u OKG_DSN {okg_prefix()} dogfood status "
            f"--deployment {correct} --json"
        )
        # `_unavailable_payload` renders its recovery block from the
        # identifier it was given. On THIS path that identifier is the
        # wrong kind, so every rendered command would carry the wrong
        # identity — handing the operator a page of commands that repeat
        # the mistake. Replace it with the one command that is correct.
        payload["recovery_commands"] = {"dogfood_status": correct_command}
        payload["next_action"] = {
            "code": IDENTIFIER_KIND_MISMATCH,
            "command": correct_command,
            "owner": "operator",
            "why": (
                f"{deployment!r} names an environment; its graph deployment "
                f"is {correct!r}"
            ),
        }
        _emit_status(payload, as_json=as_json)
        return 1
    try:
        env_payload, resolved_dsn = _deployment_ready_env_status(
            args,
            show_values=bool(getattr(args, "show_values", False)),
        )
    except (OSError, ValueError, SubstrateError) as exc:
        payload = _unavailable_payload(
            deployment=deployment,
            reason="environment_unavailable",
            message=str(exc),
        )
        _emit_status(payload, as_json=as_json)
        return 1

    # Issue #3958: the profile this deployment is judged by comes from its
    # own declaration (readiness.profile, else a composed environment
    # policy, else standard), never from a hard-coded dogfood.
    profile = (
        _deployment_readiness_profile(Path(str(env_payload["deployment_dir"])))
        if env_payload.get("deployment_dir")
        else None
    )
    if not env_payload.get("ok") or not resolved_dsn:
        payload = _status_payload(
            deployment=deployment,
            environment=env_payload,
            live={},
            resolved_dsn=False,
            include_details=bool(getattr(args, "details", False)),
            **_profile_payload_kwargs(profile),
        )
        _emit_status(payload, as_json=as_json)
        return 1

    deployment_dir = Path(str(env_payload["deployment_dir"]))
    assert profile is not None  # deployment_dir is set on this path.
    if profile.get("problem"):
        # A profile that cannot be resolved runs NO profile's checks: any
        # verdict would be a guess about which contract applies.
        payload = _profile_problem_payload(
            deployment=deployment,
            environment=env_payload,
            profile=profile,
        )
        _emit_status(payload, as_json=as_json)
        return 1
    profile_name = str(profile["name"])
    run_doctor = (
        str(getattr(args, "dogfood_subcmd", "")) == "doctor"
        or bool(getattr(args, "details", False))
    )
    live_kwargs = {
        "deployment": deployment,
        "deployment_dir": deployment_dir,
        "dsn": resolved_dsn,
        "max_active_queue_depth": int(
            getattr(args, "max_active_queue_depth", 0) or 0
        ),
        "max_source_age_seconds": int(
            getattr(args, "max_source_age_seconds", None)
            or MAX_SOURCE_AGE_SECONDS[profile_name]
        ),
        "readiness_profile": profile_name,
        "max_producer_age_seconds": int(
            getattr(args, "max_producer_age_seconds", None)
            or _READY_DOGFOOD_MAX_PRODUCER_AGE_SECONDS
        ),
        "skip_doctor": (
            bool(getattr(args, "skip_doctor", False)) or not run_doctor
        ),
    }
    live = _deployment_ready_live_checks(**live_kwargs)
    recovery_plan = _plan_stale_source_sync_work_recovery(
        deployment=deployment,
        dsn=resolved_dsn,
        source_sync_status=_mapping(live.get("source_sync_status")),
    )
    if recovery_plan:
        live = dict(live)
        live["source_sync_recovery_plan"] = recovery_plan
    live = _attach_nomos_active_state(
        live,
        deployment=deployment,
        deployment_dir=deployment_dir,
        dsn=resolved_dsn,
        readiness_profile=profile_name,
    )
    from okg.substrate.deployment_apply import deployment_apply_health

    apply_health = deployment_apply_health(
        deployment=deployment,
        deployment_dir=deployment_dir,
        dsn=resolved_dsn,
    )
    live = dict(live)
    live["deployment_apply"] = apply_health
    apply_check = {
        "id": "deployment.apply",
        "status": "pass" if apply_health.get("ok") else "blocker",
        "ok": bool(apply_health.get("ok")),
        "message": (
            "deployment apply state is converged"
            if apply_health.get("ok")
            else apply_health.get("message")
            or "deployment apply is incomplete, failed, or stale"
        ),
        "details": apply_health,
    }
    live.setdefault("checks", []).append(apply_check)
    if not apply_health.get("ok"):
        live.setdefault("blockers", []).append(apply_check)
    live = _attach_worker_code_identity(
        live,
        deployment=deployment,
        dsn=resolved_dsn,
    )
    live["runtime_config"] = _declared_runtime_config(deployment_dir)
    defaulted = _profile_defaulted_warning(profile, deployment_dir)
    if defaulted is not None:
        live.setdefault("warnings", []).append(defaulted)
    payload = _status_payload(
        deployment=deployment,
        environment=env_payload,
        live=live,
        resolved_dsn=True,
        include_details=bool(getattr(args, "details", False)),
        **_profile_payload_kwargs(profile),
    )
    _emit_status(payload, as_json=as_json)
    return 0 if payload["ok"] else 1


def _deployment_readiness_profile(deployment_dir: Path) -> dict[str, Any]:
    """The resolver's answer for this deployment directory (#3958).

    A manifest that cannot be read for its profile is a problem like any
    other: no profile's checks run, and the payload names why.
    """
    from okg.substrate.cli.deployment import _ready_resolved_readiness_profile

    try:
        return _ready_resolved_readiness_profile(deployment_dir)
    except Exception as exc:  # noqa: BLE001 - reported as a named problem.
        return {
            "name": None,
            "source": None,
            "manifest_value": None,
            "environment_policy": None,
            "problem": {
                "code": "deployment.readiness.profile_unreadable",
                "message": (
                    "the manifest could not be read for readiness.profile: "
                    f"{type(exc).__name__}: {exc}"
                ),
                "field": "readiness.profile",
                "files": [str(deployment_dir / "deployment.yaml")],
            },
        }


def _profile_payload_kwargs(profile: Mapping[str, Any] | None) -> dict[str, Any]:
    """`_status_payload` keyword arguments for a resolved profile.

    With no resolution (no deployment directory was resolved) or with a
    problem, the profile is ``None`` and no printed command names one: a
    `--checks` value nobody resolved would be a guess.
    """
    if profile is None:
        return {"readiness_profile": None}
    return {
        "readiness_profile": (
            None if profile.get("problem") else str(profile.get("name"))
        ),
        "profile_resolution": profile,
    }


def _profile_defaulted_warning(
    profile: Mapping[str, Any],
    deployment_dir: Path,
) -> dict[str, Any] | None:
    """Warn when a deployment that looks like a dogfood one declared nothing.

    The profile stays ``standard`` and trust is unchanged: a signal never
    chooses the profile. It only tells the operator that the stricter
    contract they may expect is not the one being applied.
    """
    from okg.substrate.cli.deployment import _ready_manifest_and_origin
    from okg.substrate.deployments.readiness_profile import (
        SOURCE_DEFAULT,
        dogfood_signals,
    )

    if profile.get("source") != SOURCE_DEFAULT:
        return None
    manifest, _composed = _ready_manifest_and_origin(deployment_dir)
    signals = dogfood_signals(manifest)
    if not signals:
        return None
    return {
        "id": "readiness.profile_defaulted",
        "status": "warning",
        "ok": None,
        "message": (
            "this deployment declares no readiness.profile and is judged as "
            "standard, but it carries dogfood signals ("
            + ", ".join(signals)
            + "); declare readiness.profile in deployment.yaml"
        ),
        "details": {"profile": dict(profile), "signals": signals},
    }


def _profile_problem_payload(
    *,
    deployment: str,
    environment: Mapping[str, Any],
    profile: Mapping[str, Any],
) -> dict[str, Any]:
    """The status payload when the readiness profile cannot be resolved.

    No profile's live checks ran, so the payload claims nothing about the
    graph: trust is ``unavailable``, one blocker names both values and both
    files, and ``next_action`` names the field to edit. No printed command
    carries a ``--checks`` value, because none is known to apply.
    """
    problem = _mapping(profile.get("problem"))
    files = [str(path) for path in problem.get("files") or []]
    message = (
        f"{problem.get('message')}. readiness.profile: "
        f"{profile.get('manifest_value')!r}; environment policy: "
        f"{profile.get('environment_policy')!r}; files: "
        + (", ".join(files) or "none")
    )
    recovery_commands = _default_recovery_commands(
        deployment, readiness_profile=None,
    )
    quoted = shlex.quote(deployment)
    payload: dict[str, Any] = {
        "schema": DOGFOOD_STATUS_SCHEMA,
        "deployment": deployment,
        "ok": False,
        "trust_level": "unavailable",
        "readiness_profile": dict(profile),
        "environment": dict(environment),
        "read_authority": {
            "ok": False,
            "reason": "readiness_profile_unresolved",
        },
        "runtime_config": {},
        "runtime_worker": {
            "ok": False,
            "status": "unknown",
            "code_identity": {
                "status": "not_observed",
                "ok": None,
                "reasons": ["readiness_profile_unresolved"],
            },
        },
        "blockers": [{
            "id": "readiness.profile.unavailable",
            "status": "blocker",
            "ok": False,
            "message": message,
            "details": {
                "code": problem.get("code"),
                "field": problem.get("field"),
                "files": files,
                "manifest_value": profile.get("manifest_value"),
                "environment_policy": profile.get("environment_policy"),
            },
        }],
        "warnings": [],
        "unknowns": [],
        "recovery_commands": recovery_commands,
        "next_action": {
            "code": "fix_readiness_profile",
            "owner": "operator",
            "field": problem.get("field"),
            "files": files,
            "why": str(problem.get("message") or ""),
            "command": f"{okg_prefix()} deployment lint {quoted} --json",
        },
    }
    return payload


def _cmd_dogfood_control(args: argparse.Namespace, *, as_json: bool) -> int:
    try:
        deployment = _dogfood_deployment(args)
    except SubstrateError as exc:
        from okg.substrate.cli.main import _emit_error

        return _emit_error(exc, as_json=as_json)
    action = str(args.dogfood_subcmd)
    # Type the identifier here TOO, and for a sharper reason than on the
    # status path: up / down / restart MUTATE. A wrong-kind identifier on
    # a read-only surface produces a misleading report; here it would act
    # on the wrong identity — rendering and running service commands built
    # from a name that owns nothing.
    try:
        _require_graph_deployment_name(
            deployment,
            correct_command=(
                f"env -u OKG_DSN {okg_prefix()} dogfood {action} "
                "--deployment {deployment} --json"
            ),
        )
    except SubstrateError as exc:
        data = dict(getattr(exc, "data", None) or {})
        _emit_control(
            {
                "schema": DOGFOOD_CONTROL_SCHEMA,
                "action": action,
                "ok": False,
                "applied": False,
                "status": IDENTIFIER_KIND_MISMATCH,
                "error": str(exc),
                **data,
            },
            as_json=as_json,
        )
        return 1
    plan = _control_plan(
        deployment=deployment,
        action=action,
        adapter=str(getattr(args, "adapter", "auto") or "auto"),
        launchd_plist=getattr(args, "launchd_plist", None),
        systemd_scope=str(getattr(args, "systemd_scope", "system") or "system"),
    )
    requested_apply = bool(getattr(args, "apply", False))
    payload: dict[str, Any] = {
        "schema": DOGFOOD_CONTROL_SCHEMA,
        "deployment": deployment,
        "action": action,
        "adapter": plan["adapter"],
        "apply": requested_apply,
        # `apply` records what was *asked for*; `applied`/`status` record what
        # actually happened. Without them a plan-only run emits
        # {"action": "up", "ok": true} and reads as a completed start
        # (issue #944).
        "applied": False,
        "status": "planned",
        "apply_command": _control_apply_command(deployment, action),
        "ok": True,
        "supported": plan["supported"],
        "commands": plan["commands"],
        "status_command": plan["status_command"],
        "service_adapter": plan.get("service_adapter"),
    }
    if requested_apply:
        already_absent = False
        if not plan["supported"]:
            payload["ok"] = False
            payload["status"] = "unsupported"
            payload["error"] = plan["reason"]
            _emit_control(payload, as_json=as_json)
            return 1
        if action == "restart" and plan.get("restart_down_commands"):
            already_absent, absence_probe = _launchd_service_not_loaded(plan)
            if absence_probe is not None:
                payload["service_absence_probe"] = absence_probe
            if already_absent:
                down_results = []
                down_command_ok = True
                payload["restart_start_mode"] = "bootstrap_from_absent"
            else:
                down_results = _run_control_commands(
                    plan["restart_down_commands"]
                )
                down_command_ok = all(
                    row["returncode"] == 0 for row in down_results
                )
            process_audit = _runtime_worker_process_audit(deployment)
            down_state_ok = _runtime_worker_absence_proven(process_audit)
            if _int_value(process_audit.get("process_count")):
                payload["orphan_recovery"] = _terminate_runtime_worker_roots(
                    process_audit,
                )
                process_audit = _runtime_worker_process_audit(deployment)
                down_state_ok = _runtime_worker_absence_proven(process_audit)
            if down_state_ok and down_command_ok:
                up_results = _run_control_commands(plan["restart_up_commands"])
            else:
                up_results = []
                absence_unproven = (
                    process_audit.get("status") == "unavailable"
                )
                payload["bootstrap_skipped"] = (
                    "stop_command_failed"
                    if not down_command_ok
                    else (
                        "runtime_worker_absence_unproven"
                        if absence_unproven
                        else "runtime_worker_still_observed"
                    )
                )
                payload["process_state"] = (
                    "stop_command_failed"
                    if not down_command_ok
                    else (
                        "runtime_worker_absence_unproven"
                        if absence_unproven
                        else "old_runtime_worker_still_observed"
                    )
                )
            results = [*down_results, *up_results]
            command_ok = down_state_ok and down_command_ok and all(
                row["returncode"] == 0 for row in up_results
            )
        else:
            absence_probe = None
            if action == "down":
                already_absent, absence_probe = _launchd_target_is_absent(plan)
                if absence_probe is not None:
                    payload["service_absence_probe"] = absence_probe
            if already_absent:
                results = []
                command_ok = True
            else:
                results = _run_control_commands(plan["commands"])
                command_ok = all(row["returncode"] == 0 for row in results)
            process_audit = _runtime_worker_process_audit(deployment)
            if (
                action == "down"
                and not already_absent
                and _int_value(process_audit.get("process_count"))
            ):
                payload["orphan_recovery"] = _terminate_runtime_worker_roots(
                    process_audit,
                )
                process_audit = _runtime_worker_process_audit(deployment)
        payload["results"] = results
        payload["ok"] = command_ok
        if action == "restart" and plan.get("restart_down_commands"):
            process_audit = _runtime_worker_process_audit(deployment)
        payload["process_audit"] = process_audit
        process_state_ok = _control_process_state_ok(action, process_audit)
        if action == "down" and process_state_ok:
            if already_absent:
                payload["final_state"] = "already_absent"
            elif not command_ok:
                payload["final_state"] = "stopped_after_failed_command"
                # A failed stop command must not be masked by a clean process
                # audit alone: with the service still loaded, launchd can
                # respawn the worker later. But when a post-failure probe
                # proves the service is absent from the service manager,
                # there is nothing left to respawn and the stop contract is
                # met — bootout of a never-loaded service returns exit 5 on
                # some macOS versions (2026-07-22 deployment-up stop phase).
                absent_after, post_probe = _launchd_service_not_loaded(plan)
                if post_probe is not None:
                    payload["service_absence_probe_after_failure"] = post_probe
                if absent_after:
                    payload["final_state"] = "absent_after_failed_command"
                    payload["ok"] = True
        if not process_state_ok:
            payload["ok"] = False
            payload.setdefault(
                "process_state",
                "unexpected_runtime_worker_processes",
            )
            payload["recovery_command"] = _runtime_process_recovery_command(
                deployment,
                process_audit,
            )
        # `applied` must mean "control commands actually ran". The
        # already-absent short-circuits above execute nothing, and reporting
        # those as `applied` would be the mirror of the bug being fixed: a
        # no-op reading as an execution.
        executed = bool(results)
        payload["applied"] = executed
        if not payload["ok"]:
            payload["status"] = "failed"
        elif executed:
            payload["status"] = "applied"
        else:
            payload["status"] = "already_satisfied"
    _emit_control(payload, as_json=as_json)
    return 0 if payload["ok"] else 1


def _cmd_dogfood_service(args: argparse.Namespace, *, as_json: bool) -> int:
    try:
        deployment = _dogfood_deployment(args)
    except SubstrateError as exc:
        from okg.substrate.cli.main import _emit_error

        return _emit_error(exc, as_json=as_json)
    adapter = str(args.adapter)
    subcmd = str(args.dogfood_service_subcmd)
    dsn = _resolve_service_install_dsn(
        str(args.dsn),
        apply=(
            subcmd == "install"
            and bool(getattr(args, "apply", False))
        ),
        deployment=deployment,
    )
    try:
        rendered = _render_service_adapter(
            deployment=deployment,
            adapter=adapter,
            repo_root=Path(str(args.repo_root)).expanduser(),
            okg_bin=str(args.okg_bin),
            dsn=dsn,
            postgres_image=str(args.postgres_image),
            postgres_host_port=int(args.postgres_host_port),
            postgres_db=(
                str(args.postgres_db)
                if getattr(args, "postgres_db", None)
                else None
            ),
            postgres_password=str(args.postgres_password),
            postgres_memory_limit=str(args.postgres_memory_limit),
            postgres_shm_size=str(args.postgres_shm_size),
            postgres_max_connections=int(args.postgres_max_connections),
            postgres_max_worker_processes=int(args.postgres_max_worker_processes),
            postgres_timescaledb_workers=int(args.postgres_timescaledb_workers),
            postgres_shared_buffers=str(args.postgres_shared_buffers),
            postgres_max_locks_per_transaction=int(
                args.postgres_max_locks_per_transaction
            ),
            postgres_maintenance_work_mem=str(args.postgres_maintenance_work_mem),
            env_file=str(args.env_file),
            user=str(args.user),
            group=str(args.group),
            output_path=(
                Path(str(args.output)).expanduser()
                if getattr(args, "output", None)
                else None
            ),
        )
    except SubstrateError as exc:
        from okg.substrate.cli.main import _emit_error

        return _emit_error(exc, as_json=as_json)
    output = getattr(args, "output", None) or rendered["destination"]
    payload: dict[str, Any] = {
        "schema": DOGFOOD_SERVICE_ADAPTER_SCHEMA,
        "deployment": deployment,
        "adapter": adapter,
        "action": subcmd,
        "ok": True,
        "filename": rendered["filename"],
        "destination": rendered["destination"],
        "output": output,
        "install_commands": rendered["install_commands"],
        "control_commands": rendered["control_commands"],
        "content_sha256": (
            "sha256:"
            + hashlib.sha256(
                str(rendered["content"]).encode("utf-8"),
            ).hexdigest()
        ),
    }
    if subcmd == "render":
        payload["content"] = rendered["content"]
    if rendered.get("postgres"):
        payload["postgres"] = rendered["postgres"]
    if adapter == "docker-compose" and rendered.get("postgres"):
        postgres = rendered["postgres"]
        identity = _compose_identity_verdict(
            expected_project=str(postgres["compose_project"]),
            container_name=str(postgres["container_name"]),
            volume_name=str(postgres["volume_name"]),
            descriptor=str(output),
            container_probe=_docker_inspect_labels(
                kind="container", name=str(postgres["container_name"]),
            ),
            volume_probe=_docker_inspect_labels(
                kind="volume", name=str(postgres["volume_name"]),
            ),
        )
        payload["compose_identity"] = identity
        if not identity["ok"]:
            # Do not hand over `up`/`down`/`down -v`/`restart` under a label
            # that Compose will not match. Refuse by name (issue #1602).
            payload["ok"] = False
            payload["status"] = identity["status"]
            payload["error"] = identity["error"]
            payload["reconcile_commands"] = identity["reconcile_commands"]
            payload["install_commands"] = []
            payload["control_commands"] = {}
            payload["written"] = None
            if subcmd == "install":
                payload["apply"] = bool(getattr(args, "apply", False))
            _emit_service(payload, as_json=as_json)
            return 1
    if subcmd == "install":
        payload["apply"] = bool(getattr(args, "apply", False))
        if getattr(args, "apply", False):
            if adapter == "launchd" and _is_unresolved_dsn_placeholder(dsn):
                payload["ok"] = False
                payload["error"] = (
                    "launchd does not expand DSN placeholders; pass --dsn with "
                    "the concrete deployment DSN before installing"
                )
                payload["written"] = None
                _emit_service(payload, as_json=as_json)
                return 1
            target = Path(str(output)).expanduser()
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(str(rendered["content"]), encoding="utf-8")
            payload["written"] = str(target)
            if adapter == "docker-compose" and rendered.get("postgres"):
                authority = _publish_dogfood_authority(
                    deployment=deployment,
                    repo_root=Path(str(args.repo_root)).expanduser().resolve(),
                    rendered=rendered,
                )
                payload["authority"] = authority
        else:
            payload["written"] = None
    _emit_service(payload, as_json=as_json)
    return 0


def _publish_dogfood_authority(
    *,
    deployment: str,
    repo_root: Path,
    rendered: Mapping[str, Any],
) -> dict[str, Any]:
    """Publish the compose endpoint and project it to the main checkout."""
    from urllib.parse import urlsplit

    from okg.substrate.cli._env_helpers import _write_local_env_value
    from okg.substrate.deployment_authority import worktree_main_local_env_path
    from okg.substrate.dogfood_authority import write_authority

    postgres = rendered.get("postgres")
    if not isinstance(postgres, Mapping):
        raise SubstrateError("docker-compose render did not return Postgres authority")
    dsn = str(postgres["host_dsn"])
    parts = urlsplit(dsn)
    database = str(postgres["database"])
    host = str(parts.hostname or "127.0.0.1")
    port = int(parts.port or postgres["host_port"])
    descriptor_sha256 = hashlib.sha256(
        str(rendered["content"]).encode("utf-8"),
    ).hexdigest()
    runtime_instance = {
        "container_name": str(postgres["container_name"]),
        "compose_project": str(postgres["compose_project"]),
    }
    record_path = write_authority(
        deployment,
        dsn=dsn,
        database=database,
        host=host,
        port=port,
        runtime_instance=runtime_instance,
        descriptor_sha256=descriptor_sha256,
    )
    local_env_path = worktree_main_local_env_path()
    if local_env_path is None:
        local_env_path = repo_root / ".okg" / "local.env"
    _write_local_env_value(local_env_path, "OKG_DSN", dsn)
    return {
        "schema": "okg.dogfood.authority.publish.v1",
        "mode": "runtime_record_with_main_env_projection",
        "record_path": str(record_path),
        "local_env_path": str(local_env_path),
        "deployment": deployment,
        "database": database,
        "host": host,
        "port": port,
        "runtime_instance": runtime_instance,
        "credential_source": "protected_record",
    }


def _control_plan(
    *,
    deployment: str,
    action: str,
    adapter: str,
    launchd_plist: str | None,
    systemd_scope: str,
) -> dict[str, Any]:
    resolved_adapter = _resolve_adapter(adapter)
    if resolved_adapter == "launchd":
        label = f"org.okg.runtime-worker.{deployment}"
        plist = (
            Path(launchd_plist).expanduser()
            if launchd_plist
            else Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"
        )
        service_target = f"gui/{os.getuid()}/{label}"
        status = _command_row(
            ["launchctl", "print", service_target],
            f"launchctl print {shlex.quote(service_target)}",
        )
        if action == "up":
            commands = [
                _command_row(
                    ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(plist)],
                    (
                        "launchctl bootstrap "
                        f"gui/$(id -u) {shlex.quote(str(plist))}"
                    ),
                ),
                _command_row(
                    ["launchctl", "enable", service_target],
                    f"launchctl enable {shlex.quote(service_target)}",
                ),
            ]
        elif action == "down":
            commands = [
                _command_row(
                    ["launchctl", "bootout", f"gui/{os.getuid()}", str(plist)],
                    (
                        "launchctl bootout "
                        f"gui/$(id -u) {shlex.quote(str(plist))}"
                    ),
                )
            ]
        else:
            restart_down_commands = [
                _command_row(
                    ["launchctl", "bootout", f"gui/{os.getuid()}", str(plist)],
                    (
                        "launchctl bootout "
                        f"gui/$(id -u) {shlex.quote(str(plist))}"
                    ),
                )
            ]
            restart_up_commands = [
                _command_row(
                    ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(plist)],
                    (
                        "launchctl bootstrap "
                        f"gui/$(id -u) {shlex.quote(str(plist))}"
                    ),
                ),
                _command_row(
                    ["launchctl", "enable", service_target],
                    f"launchctl enable {shlex.quote(service_target)}",
                ),
            ]
            commands = [*restart_down_commands, *restart_up_commands]
        plan = {
            "adapter": resolved_adapter,
            "supported": True,
            "commands": commands,
            "status_command": status,
            "service_adapter": {
                "render": (
                    f"{okg_prefix()} dogfood service render "
                    f"--deployment {shlex.quote(deployment)} "
                    "--adapter launchd --json"
                ),
                "install": (
                    f"{okg_prefix()} dogfood service install "
                    f"--deployment {shlex.quote(deployment)} "
                    "--adapter launchd --apply --json"
                ),
            },
        }
        if action == "restart":
            plan["restart_down_commands"] = restart_down_commands
            plan["restart_up_commands"] = restart_up_commands
        return plan
    if resolved_adapter == "systemd":
        unit = f"okg-runtime-worker@{deployment}.service"
        scope = "--user " if systemd_scope == "user" else ""
        argv_prefix = ["systemctl"]
        if systemd_scope == "user":
            argv_prefix.append("--user")
        display_prefix = f"systemctl {scope}".rstrip()
        systemd_action = {
            "up": "start",
            "down": "stop",
        }.get(action, action)
        commands = [_command_row(
            [*argv_prefix, systemd_action, unit],
            (
                f"{display_prefix} "
                f"{systemd_action} "
                f"{shlex.quote(unit)}"
            ),
        )]
        status = _command_row(
            [*argv_prefix, "status", unit],
            f"{display_prefix} status {shlex.quote(unit)}",
        )
        return {
            "adapter": resolved_adapter,
            "supported": True,
            "commands": commands,
            "status_command": status,
            "service_adapter": {
                "render": (
                    f"{okg_prefix()} dogfood service render "
                    f"--deployment {shlex.quote(deployment)} "
                    "--adapter systemd --json"
                ),
                "install": (
                    f"{okg_prefix()} dogfood service install "
                    f"--deployment {shlex.quote(deployment)} "
                    "--adapter systemd --apply --json"
                ),
            },
        }
    if resolved_adapter == "foreground" and action == "up":
        command = [
            "uv",
            "run",
            "okg",
            "runtime",
            "worker",
            "--deployment",
            deployment,
        ]
        return {
            "adapter": resolved_adapter,
            "supported": True,
            "commands": [_command_row(command, " ".join(map(shlex.quote, command)))],
            "status_command": _command_row(
                [
                    "uv",
                    "run",
                    "okg",
                    "dogfood",
                    "status",
                    "--deployment",
                    deployment,
                    "--json",
                ],
                (
                    f"{okg_prefix()} dogfood status "
                    f"--deployment {shlex.quote(deployment)} --json"
                ),
            ),
            "service_adapter": None,
        }
    return {
        "adapter": resolved_adapter,
        "supported": False,
        "reason": (
            f"{action} requires a supervised adapter; use launchd or systemd"
        ),
        "commands": [],
        "status_command": _command_row(
            [
                "uv",
                "run",
                "okg",
                "dogfood",
                "status",
                "--deployment",
                deployment,
                "--json",
            ],
            (
                f"{okg_prefix()} dogfood status "
                f"--deployment {shlex.quote(deployment)} --json"
            ),
        ),
        "service_adapter": None,
    }


def _resolve_adapter(adapter: str) -> str:
    if adapter != "auto":
        return adapter
    system = platform.system().lower()
    if system == "darwin":
        return "launchd"
    if system == "linux":
        return "systemd"
    return "foreground"


def _command_row(argv: list[str], display: str) -> dict[str, Any]:
    return {"argv": argv, "display": display}


def _run_control_commands(commands: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for command in commands:
        argv = [str(part) for part in command.get("argv") or []]
        proc = subprocess.run(  # noqa: S603 - argv comes from fixed templates.
            argv,
            check=False,
            capture_output=True,
            text=True,
        )
        results.append({
            "command": command.get("display"),
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        })
    return results


def _launchd_target_is_absent(
    plan: Mapping[str, Any],
) -> tuple[bool, dict[str, Any] | None]:
    """Prove a launchd target was never installed/loaded before skipping stop.

    A missing plist alone is not evidence that the job is unloaded, and an
    arbitrary ``launchctl`` error is not absence.  Require both the missing
    artifact and launchctl's exact service-not-found signature.  The caller's
    post-probe process audit is the third leg of the success condition.
    """
    if plan.get("adapter") != "launchd":
        return False, None
    commands = plan.get("commands") or []
    if len(commands) != 1:
        return False, None
    argv = [str(part) for part in commands[0].get("argv") or []]
    if len(argv) != 4 or Path(argv[0]).name != "launchctl" or argv[1] != "bootout":
        return False, None
    if Path(argv[-1]).exists():
        return False, None
    probe_rows = _run_control_commands([dict(plan.get("status_command") or {})])
    probe = probe_rows[0] if probe_rows else None
    if probe is None:
        return False, None
    absent = (
        probe.get("returncode") == 113
        and "Could not find service" in str(probe.get("stderr") or "")
    )
    return absent, probe


def _launchd_service_not_loaded(
    plan: Mapping[str, Any],
) -> tuple[bool, dict[str, Any] | None]:
    """Prove a launchd service is not loaded after a failed stop command.

    Unlike ``_launchd_target_is_absent`` this deliberately ignores whether
    the plist file exists on disk: a plist that is present but not loaded
    cannot respawn anything in this login session, so the stop contract is
    met. Only launchctl's exact service-not-found signature counts as
    proof; any other error keeps the stop failed.
    """
    if plan.get("adapter") != "launchd":
        return False, None
    probe_rows = _run_control_commands([dict(plan.get("status_command") or {})])
    probe = probe_rows[0] if probe_rows else None
    if probe is None:
        return False, None
    absent = (
        probe.get("returncode") == 113
        and "Could not find service" in str(probe.get("stderr") or "")
    )
    return absent, probe


def _render_service_adapter(
    *,
    deployment: str,
    adapter: str,
    repo_root: Path,
    okg_bin: str,
    dsn: str,
    postgres_image: str,
    postgres_host_port: int,
    postgres_db: str | None,
    postgres_password: str,
    postgres_memory_limit: str,
    postgres_shm_size: str,
    postgres_max_connections: int,
    postgres_max_worker_processes: int,
    postgres_timescaledb_workers: int,
    postgres_shared_buffers: str,
    postgres_max_locks_per_transaction: int,
    postgres_maintenance_work_mem: str,
    env_file: str,
    user: str,
    group: str,
    output_path: Path | None = None,
) -> dict[str, Any]:
    repo_root = repo_root.expanduser().resolve()
    if adapter == "launchd":
        return _render_launchd_adapter(
            deployment=deployment,
            repo_root=repo_root,
            okg_bin=okg_bin,
            dsn=dsn,
        )
    if adapter == "systemd":
        return _render_systemd_adapter(
            deployment=deployment,
            repo_root=repo_root,
            okg_bin=okg_bin,
            env_file=env_file,
            user=user,
            group=group,
        )
    if adapter == "docker-compose":
        return _render_compose_adapter(
            deployment=deployment,
            repo_root=repo_root,
            okg_bin=okg_bin,
            postgres_image=postgres_image,
            postgres_host_port=postgres_host_port,
            postgres_db=postgres_db,
            postgres_password=postgres_password,
            postgres_memory_limit=postgres_memory_limit,
            postgres_shm_size=postgres_shm_size,
            postgres_max_connections=postgres_max_connections,
            postgres_max_worker_processes=postgres_max_worker_processes,
            postgres_timescaledb_workers=postgres_timescaledb_workers,
            postgres_shared_buffers=postgres_shared_buffers,
            postgres_max_locks_per_transaction=postgres_max_locks_per_transaction,
            postgres_maintenance_work_mem=postgres_maintenance_work_mem,
            output_path=output_path,
        )
    raise AssertionError(f"unsupported service adapter: {adapter}")


def _render_launchd_adapter(
    *,
    deployment: str,
    repo_root: Path,
    okg_bin: str,
    dsn: str,
) -> dict[str, Any]:
    label = f"org.okg.runtime-worker.{deployment}"
    filename = f"{label}.plist"
    destination = str(Path.home() / "Library" / "LaunchAgents" / filename)
    argv = [
        *_launchd_okg_argv(okg_bin),
        "runtime",
        "worker",
        "--deployment",
        deployment,
    ]
    argv_xml = "\n".join(
        f"    <string>{_xml_escape(part)}</string>" for part in argv
    )
    dsn_env_name = _launchd_dsn_env_name(
        repo_root=repo_root,
        deployment=deployment,
    )
    dsn_xml = (
        f"    <key>{_xml_escape(dsn_env_name)}</key>\n"
        f"    <string>{_xml_escape(dsn)}</string>\n"
        if dsn
        else ""
    )
    content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{_xml_escape(label)}</string>
  <key>ProgramArguments</key>
  <array>
{argv_xml}
  </array>
  <key>WorkingDirectory</key>
  <string>{_xml_escape(str(repo_root))}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONUNBUFFERED</key>
    <string>1</string>
    <key>OKG_IVM_CONSOLE_COUNT_ROLLUP</key>
    <string>1</string>
{dsn_xml.rstrip()}
  </dict>
  <key>KeepAlive</key>
  <true/>
  <key>RunAtLoad</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>10</integer>
  <key>ExitTimeOut</key>
  <integer>{int(WORKER_GRACEFUL_SHUTDOWN_SECONDS)}</integer>
  <key>StandardOutPath</key>
  <string>/tmp/okg-runtime-worker.{_xml_escape(deployment)}.log</string>
  <key>StandardErrorPath</key>
  <string>/tmp/okg-runtime-worker.{_xml_escape(deployment)}.log</string>
</dict>
</plist>
"""
    return {
        "filename": filename,
        "destination": destination,
        "content": content,
        "install_commands": [
            f"mkdir -p {shlex.quote(str(Path(destination).parent))}",
            (
                f"{okg_prefix()} dogfood service install "
                f"--deployment {shlex.quote(deployment)} "
                f"--adapter launchd --output {shlex.quote(destination)} "
                "--apply --json"
            ),
            f"launchctl bootstrap gui/$(id -u) {shlex.quote(destination)}",
            f"launchctl enable gui/$(id -u)/{shlex.quote(label)}",
        ],
        # These are handed to an operator who just installed the service, so
        # they must act rather than print a plan (issue #944).
        "control_commands": {
            action: (
                f"{okg_prefix()} dogfood {action} "
                f"--deployment {shlex.quote(deployment)} "
                "--adapter launchd --apply --json"
            )
            for action in ("up", "down", "restart")
        },
    }


_WHOLE_DSN_ENV_REF_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _launchd_dsn_env_name(*, repo_root: Path, deployment: str) -> str:
    """Return the one environment variable a launchd worker must receive.

    launchd does not run a shell, so the adapter must bind the resolved DSN
    under the exact whole-value placeholder declared by the deployment.  A
    literal DSN (or an older checkout without an accessible manifest) keeps the
    historical ``OKG_DSN`` binding.  Embedded/multiple references are refused:
    assigning the complete DSN to each component variable would silently
    synthesize a broken connection string.
    """
    manifest_path = repo_root / "deployments" / deployment / "deployment.yaml"
    if not manifest_path.is_file():
        from okg.substrate.deployments import find_deployment_dir

        deployment_dir = find_deployment_dir(deployment)
        if deployment_dir is not None:
            manifest_path = deployment_dir / "deployment.yaml"
    if not manifest_path.is_file():
        return "OKG_DSN"

    from okg.substrate import fastyaml

    try:
        raw = fastyaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # noqa: BLE001 - normalize parser failures.
        raise SubstrateError(
            f"deployment manifest for {deployment!r} is unreadable",
            data={"deployment": deployment, "manifest_path": str(manifest_path)},
            fix_hint="repair the deployment manifest before rendering a service",
        ) from exc
    postgres = raw.get("postgres") if isinstance(raw, dict) else None
    raw_dsn = postgres.get("dsn") if isinstance(postgres, dict) else None
    if not isinstance(raw_dsn, str) or not raw_dsn.strip():
        return "OKG_DSN"
    refs = _WHOLE_DSN_ENV_REF_RE.findall(raw_dsn)
    whole = _WHOLE_DSN_ENV_REF_RE.fullmatch(raw_dsn.strip())
    if whole is not None and len(refs) == 1:
        return whole.group(1)
    if refs:
        raise SubstrateError(
            "launchd requires postgres.dsn to be a single whole-value "
            "${VAR} reference",
            data={
                "deployment": deployment,
                "manifest_path": str(manifest_path),
                "referenced_env": sorted(set(refs)),
            },
            fix_hint=(
                "declare postgres.dsn as one ${DEPLOYMENT_DSN_VAR} reference "
                "before rendering the launchd adapter"
            ),
        )
    return "OKG_DSN"


def _launchd_okg_argv(okg_bin: str) -> list[str]:
    argv = shlex.split(okg_bin)
    if not argv:
        return argv
    executable = argv[0]
    if "/" not in executable:
        resolved = shutil.which(executable)
        if resolved:
            argv[0] = resolved
    if Path(argv[0]).name == "uv":
        for index, part in enumerate(argv[1:], start=1):
            if part == "okg":
                return [
                    *argv[:index],
                    "--module",
                    "okg.substrate.cli.main",
                    *argv[index + 1:],
                ]
    return argv


def _is_unresolved_dsn_placeholder(dsn: str) -> bool:
    value = dsn.strip()
    return bool(
        re.search(r"\$\{[A-Za-z_][A-Za-z0-9_]*\}", value)
        or re.fullmatch(r"\$[A-Za-z_][A-Za-z0-9_]*", value)
    )


def _resolve_service_install_dsn(
    value: str,
    *,
    apply: bool,
    deployment: str | None = None,
) -> str:
    """Resolve a whole-value DSN env reference only for explicit install.

    Deployment apply must pass the selected DSN to the service renderer
    without exposing credentials in the child process argv. Render/dry-run
    keeps placeholders intact, and a missing install-time variable remains
    unresolved so the existing launchd fail-closed guard rejects it.
    """
    if not apply:
        return value
    match = _WHOLE_DSN_ENV_REF_RE.fullmatch(value.strip())
    if match is None:
        return value
    # An explicit deployment authority may resolve the canonical OKG_DSN
    # placeholder, but it must not silently satisfy an unrelated placeholder
    # (for example ``${COOPER_OKG_DSN}``) from another deployment's profile.
    if deployment and match.group(1) == "OKG_DSN":
        from okg.substrate.deployment_authority import (
            deployment_self_hosted_authoritative,
            resolve_deployment_dsn_authority,
        )

        authority = resolve_deployment_dsn_authority(
            deployment,
            local_profile_authoritative=deployment_self_hosted_authoritative(
                deployment
            ),
        )
        resolved = authority.get("dsn")
        if isinstance(resolved, str) and resolved:
            return resolved
    resolved = os.environ.get(match.group(1))
    if resolved:
        return resolved
    return value


def _render_systemd_adapter(
    *,
    deployment: str,
    repo_root: Path,
    okg_bin: str,
    env_file: str,
    user: str,
    group: str,
) -> dict[str, Any]:
    filename = "okg-runtime-worker@.service"
    destination = "/etc/systemd/system/okg-runtime-worker@.service"
    argv = [*shlex.split(okg_bin), "runtime", "worker", "--deployment", "%i"]
    content = f"""[Unit]
Description=OKG runtime worker (deployment %i)
After=postgresql.service network.target
Wants=postgresql.service

[Service]
Type=simple
User={user}
Group={group}
WorkingDirectory={repo_root}
Environment="PYTHONUNBUFFERED=1"
EnvironmentFile={env_file}
Environment="OKG_IVM_CONSOLE_COUNT_ROLLUP=1"
ExecStart=/usr/bin/env OKG_IVM_CONSOLE_COUNT_ROLLUP=1 {' '.join(shlex.quote(part) for part in argv)}
KillSignal=SIGTERM
TimeoutStopSec={int(WORKER_GRACEFUL_SHUTDOWN_SECONDS)}
Restart=on-failure
RestartSec=5
StartLimitIntervalSec=300
StartLimitBurst=5
MemoryMax=2G
TasksMax=256
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"""
    return {
        "filename": filename,
        "destination": destination,
        "content": content,
        "install_commands": [
            (
                f"sudo {okg_prefix()} dogfood service install "
                f"--deployment {shlex.quote(deployment)} "
                "--adapter systemd --output "
                f"{shlex.quote(destination)} --apply --json"
            ),
            "sudo systemctl daemon-reload",
            (
                "sudo systemctl enable --now "
                f"okg-runtime-worker@{shlex.quote(deployment)}.service"
            ),
        ],
        "control_commands": {
            "up": (
                "systemctl start "
                f"okg-runtime-worker@{shlex.quote(deployment)}.service"
            ),
            "down": (
                "systemctl stop "
                f"okg-runtime-worker@{shlex.quote(deployment)}.service"
            ),
            "restart": (
                "systemctl restart "
                f"okg-runtime-worker@{shlex.quote(deployment)}.service"
            ),
        },
    }


def _render_compose_adapter(
    *,
    deployment: str,
    repo_root: Path,
    okg_bin: str,
    postgres_image: str,
    postgres_host_port: int,
    postgres_db: str | None,
    postgres_password: str,
    postgres_memory_limit: str,
    postgres_shm_size: str,
    postgres_max_connections: int,
    postgres_max_worker_processes: int,
    postgres_timescaledb_workers: int,
    postgres_shared_buffers: str,
    postgres_max_locks_per_transaction: int,
    postgres_maintenance_work_mem: str,
    output_path: Path | None = None,
) -> dict[str, Any]:
    if postgres_host_port <= 0:
        raise SubstrateError("--postgres-host-port must be positive")
    if postgres_max_connections <= 0:
        raise SubstrateError("--postgres-max-connections must be positive")
    if postgres_max_worker_processes <= 0:
        raise SubstrateError("--postgres-max-worker-processes must be positive")
    if postgres_timescaledb_workers < 0:
        raise SubstrateError("--postgres-timescaledb-workers must be non-negative")
    if postgres_max_locks_per_transaction < 64:
        raise SubstrateError(
            "--postgres-max-locks-per-transaction must be >= 64 (the shared "
            "lock table must not shrink below the Postgres default; a large "
            "graph DB needs it well above 64 to survive concurrent publishes)"
        )
    if postgres_timescaledb_workers >= postgres_max_worker_processes:
        raise SubstrateError(
            "--postgres-timescaledb-workers must be lower than "
            "--postgres-max-worker-processes so normal Postgres workers have headroom"
        )

    safe_deployment = _compose_safe_token(deployment, replacement="-")
    declared_db = _dogfood_postgres_db_name(deployment, repo_root=repo_root)
    if postgres_db and declared_db and postgres_db != declared_db:
        raise SubstrateError(
            "dogfood Postgres database identity mismatch",
            data={
                "deployment": deployment,
                "expected_database": declared_db,
                "requested_database": postgres_db,
            },
            fix_hint=(
                "omit --postgres-db so the dogfood descriptor uses the "
                "deployment manifest database"
            ),
        )
    db_name = postgres_db or declared_db
    if not db_name:
        raise SubstrateError(
            f"no database is declared for deployment {deployment!r}",
            data={
                "deployment": deployment,
                "manifest_path": str(
                    repo_root.expanduser().resolve()
                    / "deployments"
                    / deployment
                    / "deployment.yaml"
                ),
            },
            fix_hint=(
                "declare postgres.database in the deployment manifest, or pass "
                "--postgres-db <name> to name the dogfood database explicitly"
            ),
        )
    container_name = f"okg-{safe_deployment}-dogfood-postgres"
    volume_name = f"okg-{safe_deployment}-dogfood-postgres-data"
    # Compose adopts containers by project/service labels, not by container
    # name, so the project label IS the stack's identity (issue #1602: a
    # foreign container labelled `okg-dogfood-okg-workspace` was adopted and
    # removed).  Key that label on what the stack owns -- its data volume and
    # its host port -- and never on the checkout that rendered the file.  Two
    # checkouts of one deployment share one volume and one port, so they are
    # one stack and must render one project; two stacks that own different
    # data render different projects.  A path-derived label would instead give
    # every worktree its own project for the same running container, turning
    # `down`/`restart` into a silent no-op and `up` into a name collision, and
    # would put an absolute path digest into the tracked descriptor.
    stack_digest = _compose_stack_digest(
        volume_name=volume_name,
        host_port=postgres_host_port,
    )
    project_name = f"okg-dogfood-{safe_deployment}-{stack_digest}"
    worker_service = f"okg-runtime-worker-{safe_deployment}"
    host_dsn = (
        f"postgres://postgres:{postgres_password}@localhost:"
        f"{postgres_host_port}/{db_name}"
    )
    service_dsn = (
        f"postgres://postgres:{postgres_password}@postgres:5432/{db_name}"
    )

    filename = f"docker-compose.{deployment}.dogfood.yaml"
    # The descriptor lives with the deployment it runs, not at the repository
    # root.  Its relative paths (`ops/pg`, the `/workspace` mount) are computed
    # below from wherever it is written, so any destination stays correct.
    destination_path = output_path.expanduser().resolve() if output_path else (
        repo_root / "deployments" / deployment / filename
    )
    destination = str(destination_path)
    compose_root = destination_path.parent
    relative_repo_root = Path(os.path.relpath(repo_root, compose_root)).as_posix()
    if relative_repo_root == ".":
        relative_repo_root = "."
    pg_context = f"{relative_repo_root}/ops/pg"
    pg_initdb = f"{relative_repo_root}/ops/pg/initdb"
    command = [*shlex.split(okg_bin), "runtime", "worker", "--deployment", deployment]
    command_yaml = ", ".join(_yaml_quote(part) for part in command)
    postgres_command = [
        "postgres",
        # No pg_cron: substrate pg_cron schedules are retired (DBOS owns
        # runtime schedules). ops/pg/initdb creates pg_cron only on a server
        # that preloads it, so this server's first-time setup skips it.
        "-c",
        "shared_preload_libraries=timescaledb,pg_textsearch",
        "-c",
        f"max_connections={postgres_max_connections}",
        "-c",
        f"max_worker_processes={postgres_max_worker_processes}",
        "-c",
        f"timescaledb.max_background_workers={postgres_timescaledb_workers}",
        # Size the shared lock table and buffer cache for a large graph DB.
        # The initdb defaults (128MB shared_buffers, 64 max_locks_per_transaction)
        # starve a multi-GB deployment: the 6400-slot lock table exhausts under
        # large concurrent publishes ('out of shared memory'), which crashed the
        # okg-workspace dogfood DB on 2026-07-23
        # (fix-dogfood-postgres-lock-and-memory-sizing).
        "-c",
        f"shared_buffers={postgres_shared_buffers}",
        "-c",
        f"max_locks_per_transaction={postgres_max_locks_per_transaction}",
        "-c",
        f"maintenance_work_mem={postgres_maintenance_work_mem}",
        # Checkpoint and WAL sizing for graph ingest (lane-h-pg-wal-settings):
        # the 1GB initdb max_wal_size forced a checkpoint every gigabyte and
        # made a first code-structure ingest write 22 GB of full-page images.
        *graph_wal_command_args(),
    ]
    postgres_command_yaml = "\n".join(
        f"      - {_yaml_quote(part)}" for part in postgres_command
    )
    content = f"""name: {project_name}

services:
  postgres:
    build:
      context: {pg_context}
    image: {postgres_image}
    container_name: {container_name}
    init: true
    shm_size: {postgres_shm_size}
    mem_limit: {postgres_memory_limit}
    environment:
      POSTGRES_PASSWORD: {_yaml_quote(postgres_password)}
      POSTGRES_DB: {_yaml_quote(db_name)}
      POSTGRES_INITDB_ARGS: "--locale=C --encoding=UTF8"
    command:
{postgres_command_yaml}
    ports:
      - "{postgres_host_port}:5432"
    volumes:
      - postgres-data:/home/postgres/pgdata/data
      - {pg_initdb}:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d {db_name}"]
      interval: 5s
      timeout: 3s
      retries: 40
    restart: unless-stopped

  {worker_service}:
    image: okg-runtime:local
    working_dir: /workspace
    command: [{command_yaml}]
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      PYTHONUNBUFFERED: "1"
      OKG_IVM_CONSOLE_COUNT_ROLLUP: "1"
      OKG_DSN: {_yaml_quote(service_dsn)}
    volumes:
      - {relative_repo_root}:/workspace
    restart: unless-stopped
    stop_grace_period: {int(WORKER_GRACEFUL_SHUTDOWN_SECONDS)}s

volumes:
  postgres-data:
    name: {volume_name}
"""
    return {
        "filename": filename,
        "destination": destination,
        "content": content,
        "postgres": {
            "dedicated": True,
            "compose_project": project_name,
            "container_name": container_name,
            "volume_name": volume_name,
            "host_port": postgres_host_port,
            "database": db_name,
            "host_dsn": host_dsn,
            "service_dsn": service_dsn,
            "memory_limit": postgres_memory_limit,
            "shm_size": postgres_shm_size,
            "max_connections": postgres_max_connections,
            "max_worker_processes": postgres_max_worker_processes,
            "timescaledb_max_background_workers": postgres_timescaledb_workers,
            "shared_buffers": postgres_shared_buffers,
            "max_locks_per_transaction": postgres_max_locks_per_transaction,
            "maintenance_work_mem": postgres_maintenance_work_mem,
            "wal_settings": dict(GRAPH_WAL_SETTINGS),
        },
        "install_commands": [
            (
                f"{okg_prefix()} dogfood service install "
                f"--deployment {shlex.quote(deployment)} "
                f"--adapter docker-compose --output {shlex.quote(str(destination_path))} "
                "--apply --json"
            ),
            f"docker compose -f {shlex.quote(str(destination_path))} up -d",
        ],
        "control_commands": {
            "up": f"docker compose -f {shlex.quote(str(destination_path))} up -d",
            "down": f"docker compose -f {shlex.quote(str(destination_path))} down",
            "restart": f"docker compose -f {shlex.quote(str(destination_path))} restart",
        },
    }


COMPOSE_PROJECT_LABEL = "com.docker.compose.project"
COMPOSE_IDENTITY_CONFLICT = "compose_project_label_conflict"


def _compose_stack_digest(*, volume_name: str, host_port: int) -> str:
    """Digest the stack's own data identity: its volume and its host port.

    Deliberately path-free.  See `_render_compose_adapter` for why the
    rendering checkout must not enter this value.
    """
    material = f"volume={volume_name}\nhost_port={int(host_port)}\n"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _docker_inspect_labels(*, kind: str, name: str) -> dict[str, Any]:
    """Read one Docker object's labels.

    Returns an explicit probe verdict; absence of Docker is reported, never
    silently treated as "no conflict".
    """
    if kind not in {"container", "volume"}:
        raise AssertionError(f"unsupported docker object kind: {kind}")
    binary = shutil.which("docker")
    if binary is None:
        return {"probe": "docker_unavailable", "present": None, "labels": {}}
    argv = (
        [
            binary, "inspect", "--type", "container",
            "--format", "{{json .Config.Labels}}", name,
        ]
        if kind == "container"
        else [binary, "volume", "inspect", "--format", "{{json .Labels}}", name]
    )
    try:
        proc = subprocess.run(  # noqa: S603 - argv is a fixed template.
            argv,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:  # noqa: BLE001
        return {
            "probe": "docker_error", "present": None, "labels": {},
            "detail": str(exc),
        }
    if proc.returncode != 0:
        detail = (proc.stderr or "").strip()
        if "No such" in detail or "no such" in detail:
            return {"probe": "ok", "present": False, "labels": {}}
        return {
            "probe": "docker_error", "present": None, "labels": {},
            "detail": detail,
        }
    raw = (proc.stdout or "").strip()
    try:
        labels = json.loads(raw) if raw and raw != "null" else {}
    except json.JSONDecodeError:
        return {"probe": "docker_error", "present": None, "labels": {}, "detail": raw}
    if not isinstance(labels, dict):
        labels = {}
    return {
        "probe": "ok",
        "present": True,
        "labels": {str(key): str(value) for key, value in labels.items()},
    }


def _compose_identity_verdict(
    *,
    expected_project: str,
    container_name: str,
    volume_name: str,
    descriptor: str,
    container_probe: Mapping[str, Any],
    volume_probe: Mapping[str, Any],
) -> dict[str, Any]:
    """Decide whether the mutating compose verbs are safe to hand over.

    Compose selects by project/service label.  If this deployment's explicitly
    named container or volume already carries a DIFFERENT project label, then
    `up` collides on the container name, and `down`, `down -v` and `restart`
    match nothing and exit 0 -- a silent no-op against a live database.  Refuse
    by name instead, and name the exact reconciliation commands.
    """
    conflicts: list[dict[str, Any]] = []
    for kind, name, probe in (
        ("container", container_name, container_probe),
        ("volume", volume_name, volume_probe),
    ):
        if probe.get("probe") != "ok" or not probe.get("present"):
            continue
        observed = str((probe.get("labels") or {}).get(COMPOSE_PROJECT_LABEL, ""))
        if observed and observed != expected_project:
            conflicts.append({
                "kind": kind,
                "name": name,
                "observed_project": observed,
                "expected_project": expected_project,
            })
    verdict: dict[str, Any] = {
        "expected_project": expected_project,
        "container": dict(container_probe),
        "volume": dict(volume_probe),
        "conflicts": conflicts,
        "ok": not conflicts,
    }
    if not conflicts:
        return verdict
    named = ", ".join(
        f"{row['kind']} {row['name']} carries {COMPOSE_PROJECT_LABEL}="
        f"{row['observed_project']}"
        for row in conflicts
    )
    verdict["status"] = COMPOSE_IDENTITY_CONFLICT
    verdict["error"] = (
        f"refusing to emit mutating compose commands for {expected_project}: "
        f"{named}. `up` would collide on the container name and "
        "`down`/`down -v`/`restart` would match nothing and exit 0."
    )
    reconcile: list[str] = []
    if any(row["kind"] == "container" for row in conflicts):
        reconcile.append(f"docker rm -f {shlex.quote(container_name)}")
    if any(row["kind"] == "volume" for row in conflicts):
        # Never scripted away: removing a volume destroys data, so the
        # operator confirms ownership before anything touches it.
        reconcile.append(
            "docker volume inspect --format "
            f"'{{{{index .Labels \"{COMPOSE_PROJECT_LABEL}\"}}}}' "
            f"{shlex.quote(volume_name)}"
        )
    reconcile.append(
        f"docker compose -f {shlex.quote(descriptor)} up -d --no-deps postgres"
    )
    verdict["reconcile_commands"] = reconcile
    return verdict


def _compose_safe_token(value: str, *, replacement: str) -> str:
    chars: list[str] = []
    previous_replacement = False
    for char in value.lower():
        if char.isalnum():
            chars.append(char)
            previous_replacement = False
            continue
        if not previous_replacement:
            chars.append(replacement)
            previous_replacement = True
    token = "".join(chars).strip(replacement)
    return token or "default"


def _dogfood_postgres_db_name(
    deployment: str,
    *,
    repo_root: Path | None = None,
) -> str | None:
    """The database the deployment manifest declares, or ``None``.

    Nothing is derived from the deployment name: a render with no declared
    database and no ``--postgres-db`` refuses (see the caller) rather than
    inventing ``okg_<name>`` and reporting success.
    """
    from okg.substrate import fastyaml

    if repo_root is not None:
        manifest_path = (
            repo_root.expanduser().resolve()
            / "deployments"
            / deployment
            / "deployment.yaml"
        )
        if manifest_path.is_file():
            try:
                raw = fastyaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
            except Exception as exc:  # noqa: BLE001 - render fails closed
                raise SubstrateError(
                    f"deployment manifest for {deployment!r} is unreadable",
                    data={"manifest_path": str(manifest_path)},
                    fix_hint="repair the deployment manifest before rendering dogfood",
                ) from exc
            postgres = raw.get("postgres") if isinstance(raw, dict) else None
            database = postgres.get("database") if isinstance(postgres, dict) else None
            if isinstance(database, str) and database:
                return database
    from okg.substrate.deployment_authority import _declared_database

    declared = _declared_database(deployment)
    return declared or None


def _yaml_quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _unavailable_payload(
    *,
    deployment: str,
    reason: str,
    message: str,
) -> dict[str, Any]:
    quoted = shlex.quote(deployment)
    return {
        "schema": DOGFOOD_STATUS_SCHEMA,
        "deployment": deployment,
        "ok": False,
        "trust_level": "unavailable",
        "environment": {
            "ok": False,
            "error": reason,
            "message": message,
        },
        "read_authority": {
            "ok": False,
            "reason": reason,
        },
        "runtime_config": {},
        "runtime_worker": {
            "ok": False,
            "status": "unknown",
            # Present in EVERY payload shape (issue #945): a consumer
            # testing `code_identity.status != "diverged"` must not read
            # a missing key as "no divergence".
            "code_identity": {
                "status": "not_observed",
                "ok": None,
                "reasons": [reason],
            },
        },
        "blockers": [{
            "id": reason,
            "status": "blocker",
            "ok": False,
            "message": message,
        }],
        "warnings": [],
        # Present in EVERY payload shape: a consumer must not read a
        # missing key as "nothing was left unmeasured" (issue #1290).
        "unknowns": [],
        "next_action": {
            "code": "fix_environment",
            "owner": "operator",
            "command": (
                f"{okg_prefix()} deployment env "
                f"{quoted} --json"
            ),
        },
        # No profile was resolved on this path (#3958), so no printed
        # command names one: no `--checks`, no dogfood service render.
        "recovery_commands": _default_recovery_commands(
            deployment, readiness_profile=None,
        ),
    }


def _status_payload(
    *,
    deployment: str,
    environment: Mapping[str, Any],
    live: Mapping[str, Any],
    resolved_dsn: bool,
    include_details: bool,
    readiness_profile: str | None = "dogfood",
    profile_resolution: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The status payload for one readiness run.

    ``readiness_profile`` is the profile the live checks ran under (#3958);
    ``None`` means it could not be resolved, and no printed command then
    names one. ``profile_resolution`` is the resolver's answer, echoed as the
    payload's ``readiness_profile`` block when given.
    """
    raw_blockers = _list_of_mappings(live.get("blockers"))
    raw_warnings = _list_of_mappings(live.get("warnings"))
    # Issue #1290: probes that were never measured. They are not blockers
    # (nothing is proven broken) and they are not passes (nothing is proven
    # working), so they travel in their own list and deny `ready`.
    raw_unknowns = _list_of_mappings(live.get("unknowns"))
    # Per `reduce-agent-friction` req.friction.environment-truth: a resolved
    # DSN whose database identity does not match the manifest's declared
    # database must never be presented unlabeled (the 2026-06-10 stale-clone
    # incident shape). `mismatch`/`unresolved` is a blocker; `isolated` is
    # a labeled warning (worktree isolation is a legitimate mode, but the
    # dogfood runtime should normally run the declared database).
    target = _mapping(_mapping(environment.get("authority")).get("target"))
    identity = str(target.get("identity") or "")
    if identity in {"mismatch", "unresolved"}:
        raw_blockers.append({
            "id": "database.identity",
            "status": "blocker",
            "ok": False,
            "message": (
                f"resolved DSN database identity is {identity}: "
                f"{target.get('database')!r} vs declared "
                f"{target.get('expected_database')!r} — do not trust "
                "recovery hints against this database"
            ),
            "details": target,
        })
    elif identity == "isolated":
        raw_warnings.append({
            "id": "database.identity",
            "status": "warning",
            "ok": False,
            "message": (
                f"resolved DSN targets an ISOLATED database "
                f"{target.get('database')!r} (declared "
                f"{target.get('expected_database')!r}); recovery hints "
                "apply to the isolated copy, not the deployment's live "
                "database"
            ),
            "details": target,
        })
    source_sync_status = _mapping(live.get("source_sync_status"))
    process_audit = (
        _mapping(live.get("runtime_processes"))
        or _runtime_worker_process_audit(deployment)
    )
    if (
        source_sync_status.get("ok") is False
        and not any(
            str(row.get("id") or "") == "sources.sync_liveness"
            for row in raw_blockers
        )
    ):
        raw_blockers.append({
            "id": "sources.sync_liveness",
            "status": "blocker",
            "ok": False,
            "message": "source-sync status is blocked",
            "details": source_sync_status,
        })
    # Issue #2987 / #2502: a deployment that declares `runtime.enabled:
    # false` runs no worker on purpose. The checks that exist only because a
    # worker should be running are not applicable to it; every other check
    # still counts.
    runtime_config = _mapping(live.get("runtime_config"))
    # Issue #3958: under `standard` only, an undeclared runtime reads as the
    # runtime's own default, disabled. Safe because `okg runtime worker`
    # refuses to start unless the runtime is enabled, so no okg-started
    # worker can exist; one observed anyway is a stale-context blocker.
    runtime_undeclared_default = (
        readiness_profile == "standard"
        and runtime_config.get("status") == "not_declared"
    )
    runtime_disabled = (
        runtime_config.get("status") == "disabled" or runtime_undeclared_default
    )
    not_applicable: list[str] = []
    if runtime_disabled:
        raw_blockers, raw_warnings, not_applicable = _runtime_disabled_checks(
            raw_blockers,
            raw_warnings,
            process_audit=process_audit,
            undeclared=runtime_undeclared_default,
        )
    if process_audit.get("ok") is not True and not any(
        str(row.get("id") or "") == "runtime.worker_processes"
        for row in raw_blockers
    ) and (not runtime_disabled or process_audit.get("split_executor")):
        process_status = str(process_audit.get("status") or "unavailable")
        process_message = {
            "split_executor": (
                "multiple independent runtime worker process roots are active"
            ),
            "not_observed": "no runtime worker process root is active",
            "unavailable": (
                "runtime worker process authority could not be measured"
            ),
        }.get(
            process_status,
            "runtime worker process authority is not exactly one root",
        )
        raw_blockers.append({
            "id": "runtime.worker_processes",
            "status": "blocker",
            "ok": False,
            "message": process_message,
            "details": process_audit,
        })
    code_identity = _code_identity_block(
        live,
        deployment=deployment,
        process_audit=process_audit,
    )
    if code_identity["status"] == "diverged" and not any(
        str(row.get("id") or "") == "runtime.code_revision"
        for row in raw_blockers
    ):
        raw_blockers.append({
            "id": "runtime.code_revision",
            "status": "blocker",
            "ok": False,
            "message": _code_divergence_message(code_identity),
            "details": code_identity,
        })
    generation = _mapping(live.get("generation"))
    mcp = _mapping(live.get("mcp"))
    runtime_worker = _runtime_worker_status(source_sync_status, process_audit)
    if runtime_disabled:
        runtime_worker = _runtime_disabled_worker_status(
            runtime_worker,
            process_audit=process_audit,
            not_applicable=not_applicable,
            undeclared=runtime_undeclared_default,
        )
    runtime_worker["code_identity"] = code_identity
    recovery_plan = _mapping(live.get("source_sync_recovery_plan"))
    workflow_recovery = _workflow_recovery_summary(
        deployment,
        source_sync_status,
    )
    if recovery_plan:
        workflow_recovery["recovery_plan"] = recovery_plan
    trust_level = _trust_level(
        environment=environment,
        resolved_dsn=resolved_dsn,
        live=live,
        blockers=raw_blockers,
        unknowns=raw_unknowns,
        readiness_profile=readiness_profile,
    )
    blockers = _summarize_checks(raw_blockers)
    warnings = _summarize_checks(raw_warnings)
    unknowns = _summarize_checks(raw_unknowns)
    read_authority_ok = trust_level in {"ready", "stale_context"}
    recovery_commands = _recovery_commands(
        deployment,
        source_sync_status=source_sync_status,
        environment=environment,
        readiness_profile=readiness_profile,
    )
    payload: dict[str, Any] = {
        "schema": DOGFOOD_STATUS_SCHEMA,
        "deployment": deployment,
        "ok": trust_level == "ready",
        "trust_level": trust_level,
    }
    if profile_resolution is not None:
        payload["readiness_profile"] = dict(profile_resolution)
    payload.update({
        "environment": dict(environment),
        "read_authority": {
            "ok": read_authority_ok,
            "generation_id": generation.get("generation_id"),
            "generation_status": generation.get("status"),
            "mcp_graph_read": bool(mcp.get("ok")) and read_authority_ok,
            "mcp_pinned_generation_id": mcp.get("pinned_generation_id"),
        },
        "runtime_worker": runtime_worker,
        "runtime_processes": process_audit,
        "runtime_config": dict(runtime_config),
        "source_freshness": _source_freshness_summary(
            _mapping(live.get("source_freshness"))
        ),
        "source_sync_status": _source_sync_summary(source_sync_status),
        "source_sync_convergence": _source_sync_convergence_summary(
            _mapping(source_sync_status.get("convergence"))
        ),
        "workflow_recovery": workflow_recovery,
        "queues": {
            "substrate": _mapping(live.get("queues")),
            "source_sync": _mapping(source_sync_status.get("wakeups")),
        },
        "generation": generation,
        "mcp": mcp,
        "nomos_runtime": _mapping(live.get("nomos_runtime")),
        "nomos_source_lineage": _mapping(live.get("nomos_source_lineage")),
        "deployment_apply": _mapping(live.get("deployment_apply")),
        "code_drift": _mapping(live.get("code_drift")),
        "doctor": _mapping(live.get("doctor")),
        "database": _mapping(live.get("database")),
        # Host-filesystem headroom block (PACT bound-host-storage-pressure):
        # real bytes/percent per filesystem + the declared thresholds, so
        # `okg dogfood status` shows host pressure before Postgres PANICs.
        "host_storage": _mapping(live.get("host_storage")),
        "blockers": blockers,
        "warnings": warnings,
        "unknowns": unknowns,
        "recovery_commands": recovery_commands,
        "runtime_health_policy": _runtime_health_policy_payload(
            recovery_commands,
        ),
    })
    if recovery_plan:
        payload["source_sync_recovery_plan"] = recovery_plan
    if include_details:
        payload["details"] = {
            "readiness": dict(live),
        }
    payload["next_action"] = _next_action(payload)
    return payload


def _trust_level(
    *,
    environment: Mapping[str, Any],
    resolved_dsn: bool,
    live: Mapping[str, Any],
    blockers: list[dict[str, Any]],
    unknowns: list[dict[str, Any]] | None = None,
    readiness_profile: str | None = "dogfood",
) -> str:
    if not environment.get("ok") or not resolved_dsn:
        return "unavailable"
    if unknowns:
        # A probe that timed out was not measured. Reporting `ready` on
        # that evidence is the defect (#1290): unknown rendered as known.
        generation = _mapping(live.get("generation"))
        mcp = _mapping(live.get("mcp"))
        graph_readable = (
            generation.get("status") == "published" and mcp.get("ok") is True
        )
        return "stale_context" if graph_readable else "unavailable"
    blocker_ids = {str(row.get("id") or "") for row in blockers}
    if any(_is_unavailable_dependency_blocker(row) for row in blockers):
        return "unavailable"
    if (
        "database.connectivity" in blocker_ids
        or "database.migrations" in blocker_ids
        or "generation.latest_published" in blocker_ids
        or "mcp.graph_read" in blocker_ids
        or "nomos.active_state" in blocker_ids
        or "deployment.apply" in blocker_ids
        or "runtime.worker_processes" in blocker_ids
        # A healthy-but-WRONG database must never read as "usable stale
        # context" (reduce-agent-friction req.friction.environment-truth:
        # the 2026-06-10 stale-clone shape).
        or "database.identity" in blocker_ids
    ):
        return "unavailable"
    generation = _mapping(live.get("generation"))
    mcp = _mapping(live.get("mcp"))
    graph_readable = (
        generation.get("status") == "published"
        and mcp.get("ok") is True
    )
    if not graph_readable:
        return "unavailable"
    if readiness_profile != "dogfood" and "deployment.code_drift" not in blocker_ids:
        # Issue #3958: only dogfood makes code drift a blocker; under every
        # other profile drift is a warning, and a warning does not move
        # trust. (Under dogfood the rule below is unchanged: each drift state
        # it reads is exactly one the drift check reports as a blocker.)
        return "stale_context" if blockers else "ready"
    code_drift = _mapping(live.get("code_drift"))
    code_drift_status = str(code_drift.get("status") or "")
    if code_drift_status in {"error", "misconfigured"}:
        return "unavailable"
    commits_behind = code_drift.get("commits_behind")
    if (
        code_drift.get("ok") is False
        or (
            isinstance(commits_behind, int)
            and commits_behind > 0
        )
    ):
        return "stale_context"
    if blockers:
        return "stale_context" if graph_readable else "unavailable"
    return "ready"


def _is_unavailable_dependency_blocker(row: Mapping[str, Any]) -> bool:
    check_id = str(row.get("id") or "")
    if check_id.endswith(".unavailable"):
        return True
    details = _mapping(row.get("details"))
    status = str(details.get("status") or "")
    reason = str(details.get("reason") or "")
    if (
        check_id == "deployment.code_drift"
        and status in {"error", "misconfigured"}
    ):
        return True
    return status in {"unavailable", "command_failed"} or (
        reason == "readiness_dependency_failed"
    )


# Heartbeat statuses that mean "no measurement was taken", not "the worker is
# down". Reading these as a down-state is what made `dogfood status` recommend
# restarting a worker that had been up for 27 hours (issue #944).
_UNMEASURED_WORKER_STATUSES = frozenset({
    "",
    "not_observed",
    "not_directly_observed",
})

# `_runtime_worker_contract` collapses *any* probe exception into
# `ok: False` / `unhealthy`, which looks like a measured down-state but is
# not: the probe threw before observing anything. A stopped Postgres — the
# actual blocker in issue #944 — reaches exactly this code, so classifying it
# by status string alone reintroduces the bug. Classify by code as well.
_UNMEASURED_WORKER_CODES = frozenset({
    "runtime_services_probe_failed",
    "runtime_services_not_observed",
    "runtime_services_other_deployment",
    "runtime_services_not_requested",
    "runtime_services_unknown",
})

_WORKER_OBSERVATION_REQUIRES = (
    "a runtime-services overlay row for this deployment, read from the "
    "deployment database; an unreachable database, a failed probe, or a row "
    "owned by another deployment yields no measurement, not a down worker"
)


def _runtime_worker_status(
    source_sync_status: Mapping[str, Any],
    process_audit: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    runtime = _mapping(source_sync_status.get("runtime_liveness"))
    worker = _mapping(runtime.get("worker"))
    if not worker:
        worker = {"ok": None, "status": "not_observed"}
    else:
        worker = dict(worker)
    status = str(worker.get("status") or "")
    code = str(worker.get("code") or "")
    measured = (
        status not in _UNMEASURED_WORKER_STATUSES
        and code not in _UNMEASURED_WORKER_CODES
    )
    worker["measured"] = measured
    if measured:
        return worker
    worker["requires"] = _WORKER_OBSERVATION_REQUIRES
    audit = _mapping(process_audit)
    if audit.get("ok") is True:
        # The OS process audit in this same payload has proven exactly one
        # live worker root. An absent heartbeat next to that evidence is a
        # telemetry gap, and must not be presented as a down worker.
        worker["ok"] = None
        worker["status"] = "process_observed_no_heartbeat"
        worker["heartbeat_status"] = status or "not_observed"
        worker["process_evidence"] = {
            "status": audit.get("status"),
            "root_count": audit.get("root_count"),
            "root_pids": [
                row.get("pid") for row in _list_of_mappings(audit.get("roots"))
            ],
        }
    return worker


_RUNTIME_DISABLED_NOTE = (
    "the runtime is disabled by configuration (runtime.enabled is false in "
    "deployment.yaml): ingest runs by hand or by reconcile, so no runtime "
    "worker process or DBOS queue history is expected"
)

_RUNTIME_UNDECLARED_NOTE = (
    "the runtime is not declared (no runtime.enabled in deployment.yaml); "
    "under the standard readiness profile that reads as the runtime's own "
    "default, disabled, so no runtime worker process or DBOS queue history "
    "is expected"
)

# The one queue shape a runtime that never ran produces: the DBOS workflow
# table does not exist. The okg job queues are still measured beside it.
_DBOS_HISTORY_ABSENT_REASONS = frozenset({"dbos.workflow_status missing"})


def _declared_runtime_config(deployment_dir: Path) -> dict[str, Any]:
    """Read `runtime.enabled` through the runtime's own config reader.

    Only an explicit `runtime: {enabled: false}` reads as `disabled`. An
    absent block or key is `not_declared` and an invalid block is
    `unreadable`; both keep the worker requirement, because a status check
    must not relax on a declaration it could not read.

    One exception (issue #3958): under the `standard` readiness profile,
    `not_declared` reads as the runtime's own default, disabled
    (`_status_payload`). That is safe because `okg runtime worker` refuses to
    start unless the runtime is enabled, so no okg-started worker can exist;
    a worker observed anyway is the `runtime.undeclared_but_running`
    blocker. Under dogfood and ci, and for `unreadable` in every profile, the
    worker requirement stays.
    """
    from okg.substrate.runtime.config import RuntimeConfig

    manifest_path = deployment_dir / "deployment.yaml"
    try:
        manifest = _ready_load_yaml_mapping(manifest_path)
        block = manifest.get("runtime")
        config = RuntimeConfig.from_manifest_block(block)
    except Exception as exc:  # noqa: BLE001 - reported, never relaxed.
        return {
            "status": "unreadable",
            "enabled": None,
            "manifest_path": str(manifest_path),
            "error": f"{type(exc).__name__}: {exc}",
        }
    if not isinstance(block, Mapping) or "enabled" not in block:
        return {
            "status": "not_declared",
            "enabled": None,
            "manifest_path": str(manifest_path),
        }
    return {
        "status": "enabled" if config.enabled else "disabled",
        "enabled": bool(config.enabled),
        "manifest_path": str(manifest_path),
    }


def _runtime_disabled_checks(
    blockers: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
    *,
    process_audit: Mapping[str, Any],
    undeclared: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Reclassify the runtime-only checks for a runtime disabled by config.

    Returns the new blockers, the new warnings, and the ids that were set
    aside as not applicable. The `queues.active_depth` arm is a guard only:
    today every queue depth comes from DBOS rows, so a missing workflow
    table always reports depth 0. If a future queue view reports queued work
    beside missing DBOS history, it stays a (stale-context) blocker rather
    than disappearing with the not-applicable row.

    ``undeclared`` is the `standard` reading of a missing runtime block as
    disabled (#3958). It adds the ``runtime.enabled_not_declared`` warning,
    and a worker observed running is a ``runtime.undeclared_but_running``
    blocker (stale context) whose text names the missing block, since the
    ``runtime.enabled is false`` wording would be untrue.
    """
    off_phrase = (
        "the runtime block is not declared (read as disabled)"
        if undeclared
        else "runtime.enabled is false"
    )
    split = bool(process_audit.get("split_executor"))
    kept: list[dict[str, Any]] = []
    # Several worker roots at once is a hazard whatever the configuration
    # says, so a split executor keeps its blocker.
    not_applicable: list[str] = [] if split else ["runtime.worker_processes"]
    for row in blockers:
        check_id = str(row.get("id") or "")
        details = _mapping(row.get("details"))
        if check_id == "runtime.worker_processes" and not split:
            continue
        if (
            check_id == "queues.unavailable"
            and details.get("queue_health_measured") is True
            and str(details.get("reason") or "") in _DBOS_HISTORY_ABSENT_REASONS
        ):
            not_applicable.append(check_id)
            depth = details.get("active_queue_depth")
            bound = details.get("max_active_queue_depth")
            if isinstance(depth, int) and depth > int(bound or 0):
                kept.append({
                    "id": "queues.active_depth",
                    "status": "blocker",
                    "ok": False,
                    "message": (
                        f"{depth} queued job(s) are waiting and "
                        f"{off_phrase}, so no runtime worker "
                        "will drain them; run the ingest by hand"
                    ),
                    # Not the queue row's own `status: unavailable`: the
                    # depth WAS measured, so this is stale context, not an
                    # unreadable graph.
                    "details": {
                        "active_queue_depth": depth,
                        "max_active_queue_depth": bound,
                        "queue_depth": details.get("queue_depth"),
                        "runtime": "disabled_by_config",
                    },
                })
            continue
        kept.append(row)
    if undeclared:
        warnings = [*warnings, {
            "id": "runtime.enabled_not_declared",
            "status": "warning",
            "ok": None,
            "message": (
                "deployment.yaml declares no runtime.enabled; under the "
                "standard profile that reads as the runtime's own default, "
                "disabled, so no runtime worker is required. Declare "
                "runtime.enabled to make the intent explicit"
            ),
        }]
    if process_audit.get("ok") is True and undeclared:
        kept.append({
            "id": "runtime.undeclared_but_running",
            "status": "blocker",
            "ok": False,
            "message": (
                "the runtime block is not declared in deployment.yaml (read "
                "as disabled), but a runtime worker process is running for "
                "this deployment; declare runtime.enabled: true to run it, "
                "or stop the worker"
            ),
            "details": dict(process_audit),
        })
    elif process_audit.get("ok") is True:
        warnings = [*warnings, {
            "id": "runtime.disabled_but_running",
            "status": "warning",
            "ok": False,
            "message": (
                "runtime.enabled is false, but a runtime worker process is "
                "running for this deployment; it may drain queues and "
                "publish outside the declared configuration"
            ),
            "details": dict(process_audit),
        }]
    elif process_audit.get("ok") is None:
        # The OS process listing itself failed, so a stray worker would go
        # unseen. The graph is still readable: a warning, not a blocker.
        warnings = [*warnings, {
            "id": "runtime.process_audit_unavailable",
            "status": "warning",
            "ok": None,
            "message": (
                f"{off_phrase}, and the check for a stray "
                "runtime worker process could not run: "
                f"{process_audit.get('reason') or 'process listing failed'}"
            ),
            "details": dict(process_audit),
        }]
    return kept, warnings, not_applicable


def _runtime_disabled_worker_status(
    worker: Mapping[str, Any],
    *,
    process_audit: Mapping[str, Any],
    not_applicable: list[str],
    undeclared: bool = False,
) -> dict[str, Any]:
    out = dict(worker)
    out["observed_status"] = worker.get("status")
    out["status"] = (
        "not_declared_default_disabled" if undeclared else "disabled_by_config"
    )
    out["ok"] = None
    out["measured"] = True
    out["note"] = (
        _RUNTIME_UNDECLARED_NOTE if undeclared else _RUNTIME_DISABLED_NOTE
    )
    out["not_applicable_checks"] = list(not_applicable)
    if process_audit.get("ok") is True:
        out["note"] += (
            "; a runtime worker process is nevertheless running "
            + (
                "(see blocker runtime.undeclared_but_running)"
                if undeclared
                else "(see warning runtime.disabled_but_running)"
            )
        )
    return out


def _attach_nomos_active_state(
    live: Mapping[str, Any],
    *,
    deployment: str,
    deployment_dir: Path,
    dsn: str,
    readiness_profile: str = "dogfood",
) -> dict[str, Any]:
    manifest_path = deployment_dir / "deployment.yaml"
    if not manifest_path.exists():
        return dict(live)
    manifest = _ready_load_yaml_mapping(manifest_path)
    nomos_runtime = _ready_nomos_active_state_status(
        deployment=deployment,
        dsn=dsn,
        manifest=manifest,
        deployment_dir=deployment_dir,
        readiness_profile=readiness_profile,
    )
    enriched = dict(live)
    enriched["nomos_runtime"] = nomos_runtime
    if str(nomos_runtime.get("status") or "") == "skipped":
        return enriched
    ok = bool(nomos_runtime.get("ok"))
    check = {
        "id": "nomos.active_state",
        "status": "pass" if ok else "blocker",
        "ok": ok,
        "message": _ready_nomos_active_state_message(nomos_runtime),
        "details": nomos_runtime,
    }
    enriched["checks"] = [*_list_of_mappings(enriched.get("checks")), check]
    if not ok:
        enriched["blockers"] = [
            *_list_of_mappings(enriched.get("blockers")),
            check,
        ]
    return enriched


def _code_divergence_message(block: Mapping[str, Any]) -> str:
    reasons = [str(row) for row in block.get("reasons") or []]
    parts: list[str] = []
    if "code_revision_mismatch" in reasons:
        parts.append(
            f"the running worker loaded revision "
            f"{block.get('worker_code_revision')} but this checkout is at "
            f"{block.get('checkout_code_revision')}"
        )
    if "loaded_modules_modified_since_import" in reasons:
        modules = [
            str(row.get("module"))
            for row in block.get("modified_modules") or []
            if isinstance(row, Mapping) and row.get("module")
        ]
        sample = ", ".join(modules[:3])
        parts.append(
            f"{block.get('modified_module_count')} module(s) the worker "
            f"already imported have been edited since it loaded them"
            + (f" (e.g. {sample})" if sample else "")
        )
    detail = "; ".join(parts) or "worker code diverges from this checkout"
    return (
        f"runtime worker is serving stale code: {detail} — until it is "
        "restarted it will keep running the code it loaded at start"
    )


def _attach_worker_code_identity(
    live: Mapping[str, Any],
    *,
    deployment: str,
    dsn: str,
) -> dict[str, Any]:
    """Attach the worker's boot-time code identity to the live checks.

    Issue #945. Only the worker process can observe which code it
    loaded, so the identity rides the scheduled doctor's verdict event.
    A read failure attaches nothing; `_code_identity_block` then reports
    `not_observed`, which is a distinct state from `matched`.
    """
    enriched = dict(live)
    try:
        from okg.substrate.runtime.publisher_health import (
            read_worker_code_identity_overlay,
        )

        observed = read_worker_code_identity_overlay(
            deployment=deployment,
            dsn=dsn,
        )
    except Exception as exc:  # noqa: BLE001 - status stays readable.
        enriched["worker_code_identity"] = {
            "status": "probe_failed",
            "reason": f"{type(exc).__name__}: {exc}",
        }
        return enriched
    if observed is not None:
        enriched["worker_code_identity"] = observed
    return enriched


def _observed_worker_root_count(
    process_audit: Mapping[str, Any] | None,
) -> int | None:
    """Live worker process roots, or ``None`` when they were not counted.

    An unavailable process audit must not be read as "one worker": with
    two workers at different revisions the reported identity belongs to
    whichever wrote the last verdict, and calling that "the worker" would
    be a claim the data does not support.
    """
    audit = _mapping(process_audit)
    if not audit or str(audit.get("status") or "") == "unavailable":
        return None
    raw = audit.get("root_count")
    return int(raw) if isinstance(raw, (int, float)) else None


def _code_identity_block(
    live: Mapping[str, Any],
    *,
    deployment: str,
    process_audit: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Compare the worker's loaded code to the checkout IT is serving.

    Four states, per issue #945: `matched`, `diverged`,
    `stale_observation`, `not_observed`. Only `matched` is reassuring —
    an unreported, stale, or ambiguous identity means the check did not
    run, never that it passed.

    The checkout revision is re-read from the WORKER's own recorded root
    rather than from this process's tree. `okg dogfood status` is
    routinely run from a different worktree than the worker's sync root,
    and comparing across them would report drift that does not exist.
    """
    from okg.substrate.runtime.code_identity import (
        checkout_code_revision,
        evaluate_code_divergence,
    )

    observed = live.get("worker_code_identity")
    observed = observed if isinstance(observed, Mapping) else None
    worker_root = None
    if observed is not None:
        worker_root = (
            observed.get("checkout_root") or observed.get("package_root")
        )
    block = evaluate_code_divergence(
        observed=observed,
        checkout_revision=(
            checkout_code_revision(worker_root)
            if worker_root else None
        ),
        observed_age_minutes=(
            _float_value(observed.get("observed_minutes_ago"))
            if observed is not None else None
        ),
        worker_root_count=_observed_worker_root_count(process_audit),
    )
    if observed is not None:
        block["observed_minutes_ago"] = observed.get("observed_minutes_ago")
        block["last_observed_at"] = observed.get("last_observed_at")
        block["source"] = observed.get("source")
    if block["status"] == "diverged":
        block["fix_hint"] = _restart_worker_hint(deployment)
    return block


def _restart_worker_hint(deployment: str) -> str:
    """The supervisor-level restart for this host, or the fallback.

    `okg dogfood restart` is deliberately NOT the primary hint: the
    incident in #945 was prolonged partly because it does not restart a
    supervised worker. Render the adapter's own restart command.
    """
    try:
        plan = _control_plan(
            deployment=deployment,
            action="restart",
            adapter="auto",
            launchd_plist=None,
            systemd_scope="system",
        )
        if plan.get("supported"):
            commands = [
                str(row.get("display"))
                for row in plan.get("commands") or []
                if row.get("display")
            ]
            if commands:
                return "; ".join(commands)
    except Exception:  # noqa: BLE001 - fall back to the generic hint.
        pass
    prefix = _dogfood_local_profile_command_prefix()
    quoted = shlex.quote(deployment)
    return (
        f"restart the runtime worker for {quoted} "
        f"({prefix}{okg_prefix()} dogfood restart --deployment {quoted} --apply)"
    )


def _source_sync_summary(status: Mapping[str, Any]) -> dict[str, Any]:
    if not status:
        return {}
    desired_plan = _mapping(status.get("desired_plan"))
    runtime = _mapping(status.get("runtime_liveness"))
    return {
        "schema": status.get("schema"),
        "deployment": status.get("deployment"),
        "ok": status.get("ok"),
        "status": status.get("status"),
        "blockers": _list_of_mappings(status.get("blockers")),
        "desired_plan": {
            "expected_producer_count": desired_plan.get(
                "expected_producer_count",
            ),
            "disabled_producer_count": desired_plan.get(
                "disabled_producer_count",
            ),
            "config_warning_count": len(
                desired_plan.get("config_warnings") or []
            ),
        },
        "runtime_liveness": _runtime_liveness_summary(runtime),
        "backlog": _mapping(status.get("backlog")),
        "wakeups": _mapping(status.get("wakeups")),
        "convergence": _source_sync_convergence_summary(
            _mapping(status.get("convergence"))
        ),
    }


def _source_sync_convergence_summary(
    convergence: Mapping[str, Any],
) -> dict[str, Any]:
    if not convergence:
        return {}
    recovery_plan = _mapping(convergence.get("recovery_plan"))
    actions = _list_of_mappings(recovery_plan.get("actions"))
    blockers = _list_of_mappings(convergence.get("blockers"))
    return {
        "schema": convergence.get("schema"),
        "deployment": convergence.get("deployment"),
        "ok": convergence.get("ok"),
        "state": convergence.get("state"),
        "source_count": convergence.get("source_count"),
        "readiness_required_source_count": convergence.get(
            "readiness_required_source_count"
        ),
        "state_counts": _mapping(convergence.get("state_counts")),
        "blockers": blockers,
        "recovery_plan": {
            "action_count": recovery_plan.get("action_count"),
            "actions": actions,
        },
        "next_action": _mapping(convergence.get("next_action")),
    }


def _runtime_liveness_summary(runtime: Mapping[str, Any]) -> dict[str, Any]:
    if not runtime:
        return {}
    return {
        "ok": runtime.get("ok"),
        "status": runtime.get("status"),
        "worker": _mapping(runtime.get("worker")),
        "services": _service_status_summary(_mapping(runtime.get("services"))),
        "poll_probes": _single_service_summary(
            _mapping(runtime.get("poll_probes"))
        ),
        "source_status_counts": _source_status_counts(
            _mapping(runtime.get("sources"))
        ),
        "missing_producer_count": len(
            _list_of_mappings(runtime.get("missing_producers"))
        ),
        "stale_producer_count": len(
            _list_of_mappings(runtime.get("stale_producers"))
        ),
        "config_warning_count": len(
            _list_of_mappings(runtime.get("config_warnings"))
        ),
    }


def _service_status_summary(
    services: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    return {
        str(name): _single_service_summary(_mapping(service))
        for name, service in services.items()
    }


def _single_service_summary(service: Mapping[str, Any]) -> dict[str, Any]:
    runtime_service = _mapping(service.get("runtime_service"))
    return {
        "ok": service.get("ok"),
        "status": service.get("status"),
        "expected_source_count": service.get("expected_source_count"),
        "enabled": runtime_service.get("enabled"),
        "alive": runtime_service.get("alive"),
        "last_error": runtime_service.get("last_error"),
    }


def _source_status_counts(sources: Mapping[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for source in sources.values():
        if not isinstance(source, Mapping):
            continue
        status = str(source.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    return counts


def _source_freshness_summary(freshness: Mapping[str, Any]) -> dict[str, Any]:
    if not freshness:
        return {}
    if freshness.get("status") == "not_checked":
        # Issue #3958: a standard deployment with no baseline sources. Not
        # `ok: True` (nothing was found fresh) and not `ok: False` (nothing
        # was required): the reason says which.
        return {
            "status": "not_checked",
            "reason": freshness.get("reason"),
        }
    groups = _mapping(freshness.get("groups"))
    group_summary: dict[str, dict[str, Any]] = {}
    for name, group in groups.items():
        group_map = _mapping(group)
        sources = _list_of_mappings(group_map.get("sources"))
        group_summary[str(name)] = {
            "ok": group_map.get("ok"),
            "source_count": len(sources),
            "stale_count": sum(
                1 for source in sources if source.get("status") == "stale"
            ),
            "missing_count": sum(
                1 for source in sources if source.get("status") == "missing"
            ),
        }
    return {
        "ok": freshness.get("ok"),
        "max_source_age_seconds": freshness.get("max_source_age_seconds"),
        "missing": list(freshness.get("missing") or []),
        "stale": list(freshness.get("stale") or []),
        "undeclared": list(freshness.get("undeclared") or []),
        "groups": group_summary,
    }


def _summarize_checks(checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_summarize_check(check) for check in checks]


def _summarize_check(check: Mapping[str, Any]) -> dict[str, Any]:
    out = {
        key: check.get(key)
        for key in ("id", "status", "ok", "message")
        if key in check
    }
    details = _mapping(check.get("details"))
    if not details:
        return out
    check_id = str(check.get("id") or "")
    if check_id == "sources.freshness":
        out["details"] = _source_freshness_summary(details)
    elif check_id == "sources.sync_liveness":
        if details.get("schema") == "okg.source_sync.status.v1":
            out["details"] = _source_sync_summary(details)
        else:
            out["details"] = {
                "ok": details.get("ok"),
                "producer_count": details.get("producer_count"),
                "expected_producer_count": details.get(
                    "expected_producer_count"
                ),
                "missing_producer_count": len(
                    _list_of_mappings(details.get("missing_producers"))
                ),
                "stale_producer_count": len(
                    _list_of_mappings(details.get("stale_producers"))
                ),
                "dlq_count": details.get("dlq_count"),
            }
    elif check_id == "runtime.worker_processes":
        out["details"] = _runtime_process_summary(details)
    else:
        out["details"] = details
    return out


def _next_action(payload: Mapping[str, Any]) -> dict[str, Any]:
    deployment = str(payload.get("deployment") or "<deployment>")
    commands = _mapping(payload.get("recovery_commands"))
    env = _mapping(payload.get("environment"))
    if payload.get("trust_level") == "ready":
        return {
            "code": "none",
            "owner": "agent",
            "command": commands.get("dogfood_status"),
        }
    if not env.get("ok"):
        recommended = list(env.get("recommended_commands") or [])
        return {
            "code": "configure_environment",
            "owner": "operator",
            "command": (
                recommended[0]
                if recommended
                else commands.get("deployment_env")
            ),
        }
    convergence = _mapping(payload.get("source_sync_convergence"))
    convergence_next = _mapping(convergence.get("next_action"))
    if (
        convergence
        and convergence.get("state") not in {None, "", "ready"}
        and convergence_next.get("code") not in {None, "", "none"}
    ):
        return {
            "code": convergence_next.get("code"),
            "owner": convergence_next.get("owner") or "runtime-worker",
            "source_name": convergence_next.get("source_name"),
            "command": (
                convergence_next.get("command")
                or commands.get("refresh_poll_backstop")
            ),
        }

    blocker_ids = {
        str(row.get("id") or "") for row in _list_of_mappings(
            payload.get("blockers")
        )
    }
    # Host storage at the blocker floor outranks every other recovery:
    # a full disk is the root cause of the worker/database symptoms that
    # follow (2026-07-17 double-ENOSPC), so the reclaim runbook comes
    # first (PACT bound-host-storage-pressure).
    if "host.storage_headroom" in blocker_ids:
        return {
            "code": "reclaim_host_storage",
            "owner": "operator",
            "runbook": _HOST_STORAGE_RECLAIM_RUNBOOK,
            "command": f"cat {_HOST_STORAGE_RECLAIM_RUNBOOK}",
        }
    runtime_processes = _mapping(payload.get("runtime_processes"))
    runtime_worker = _mapping(payload.get("runtime_worker"))
    worker_status = str(runtime_worker.get("status") or "")
    # A runtime disabled by configuration has no worker to start (#2987),
    # nor has an undeclared one under the standard profile (#3958).
    runtime_disabled = worker_status in {
        "disabled_by_config",
        "not_declared_default_disabled",
    }
    if runtime_processes.get("ok") is not True and (
        not runtime_disabled or runtime_processes.get("split_executor")
    ):
        if runtime_processes.get("split_executor"):
            return {
                "code": "resolve_split_runtime_worker",
                "owner": "operator",
                "command": _runtime_process_recovery_command(
                    deployment,
                    runtime_processes,
                ),
            }
        return {
            "code": "start_or_restart_worker",
            "owner": "operator",
            "command": (
                commands.get("dogfood_restart")
                or commands.get("dogfood_up")
                or commands.get("start_supervised_worker")
                or commands.get("runtime_worker_foreground")
            ),
        }
    # Reaching here means the process audit proved exactly one live worker
    # root. An unmeasured heartbeat therefore cannot justify a restart: the
    # worker is demonstrably running and the gap is in telemetry, not
    # liveness (issue #944). Only a *measured* bad state restarts the worker.
    if runtime_worker.get("measured") is False:
        # Every unmeasured shape lands here, not just
        # `process_observed_no_heartbeat`: falling through on an unrecognized
        # unmeasured status would silently drop the recommendation.
        return {
            "code": "diagnose_runtime_worker_telemetry",
            "owner": "operator",
            "detail": (
                "the runtime worker process is running but its heartbeat "
                "could not be read; check database reachability and the "
                "runtime-services overlay before restarting anything"
            ),
            "requires": runtime_worker.get("requires"),
            "heartbeat_status": runtime_worker.get("heartbeat_status"),
            "command": (
                commands.get("dogfood_doctor")
                or commands.get("source_sync_health")
                or commands.get("dogfood_status")
            ),
        }
    if worker_status in {
        "down",
        "stale",
        "unhealthy",
        "missing",
    } or runtime_worker.get("ok") is False:
        return {
            "code": "start_or_restart_worker",
            "owner": "operator",
            "command": (
                commands.get("dogfood_up")
                or commands.get("start_supervised_worker")
                or commands.get("runtime_worker_foreground")
            ),
        }
    if "runtime.code_revision" in blocker_ids:
        code_identity = _mapping(runtime_worker.get("code_identity"))
        return {
            "code": "restart_stale_runtime_worker",
            "owner": "operator",
            "command": (
                code_identity.get("fix_hint")
                or commands.get("dogfood_restart")
            ),
        }
    workflow_recovery = _mapping(payload.get("workflow_recovery"))
    if workflow_recovery.get("ok") is False:
        return {
            "code": "inspect_or_recover_source_sync_work",
            "owner": "runtime-operator",
            "command": (
                workflow_recovery.get("dry_run_command")
                or commands.get("source_sync_dbos_gc_dry_run")
                or commands.get("source_sync_health")
            ),
        }
    if "sources.freshness" in blocker_ids:
        return {
            "code": "refresh_stale_sources",
            "owner": "runtime-worker",
            "command": commands.get("refresh_poll_backstop"),
        }
    if "database.connectivity" in blocker_ids:
        return {
            "code": "fix_database_connectivity",
            "owner": "operator",
            "command": commands.get("dogfood_ready_gate"),
        }
    if "database.migrations" in blocker_ids:
        return {
            "code": "run_migrations",
            "owner": "operator",
            "command": f"{okg_prefix()} migrate --deployment {shlex.quote(deployment)}",
        }
    if "generation.latest_published" in blocker_ids:
        return {
            "code": "publish_generation",
            "owner": "operator",
            "command": commands.get("dogfood_ready_gate"),
        }
    if "mcp.graph_read" in blocker_ids:
        return {
            "code": "fix_mcp_graph_read",
            "owner": "operator",
            "command": commands.get("dogfood_ready_gate"),
        }
    if "doctor.okg_workspace" in blocker_ids:
        return {
            "code": "run_doctor",
            "owner": "operator",
            "command": commands.get("dogfood_ready_gate"),
        }
    return {
        "code": "inspect_readiness_blockers",
        "owner": "operator",
        "command": commands.get("dogfood_ready_gate"),
    }


def _recovery_commands(
    deployment: str,
    *,
    source_sync_status: Mapping[str, Any],
    environment: Mapping[str, Any],
    readiness_profile: str | None = "dogfood",
) -> dict[str, Any]:
    commands = _default_recovery_commands(
        deployment, readiness_profile=readiness_profile,
    )
    source_commands = _mapping(source_sync_status.get("recovery_commands"))
    commands.update(source_commands)
    commands.update(
        _source_sync_dbos_recovery_commands(
            deployment,
            source_sync_status,
        )
    )
    recommended = list(environment.get("recommended_commands") or [])
    if recommended:
        commands["environment_recommended"] = recommended
    return commands


def _runtime_health_policy_payload(
    commands: Mapping[str, str],
) -> dict[str, Any]:
    from okg.substrate.runtime.health_policy import runtime_health_policy

    return runtime_health_policy(commands=commands)


def _workflow_recovery_summary(
    deployment: str,
    source_sync_status: Mapping[str, Any],
) -> dict[str, Any]:
    dbos = _source_sync_dbos_workflows(source_sync_status)
    if not dbos:
        return {
            "ok": None,
            "status": "unavailable",
            "reason": "dbos_workflow_status_unavailable",
        }
    commands = _source_sync_dbos_recovery_commands(
        deployment,
        source_sync_status,
    )
    active_count = _int_value(dbos.get("active_count"))
    active_rows = _list_of_mappings(dbos.get("active_rows"))
    stale_replaceable = bool(
        dbos.get("stale_replaceable_work_present")
        or _mapping(source_sync_status.get("backlog")).get(
            "stale_replaceable_work_present"
        )
    )
    recovery_queue_name = _source_sync_recovery_queue_name(
        deployment,
        dbos,
    )
    return {
        "ok": active_count == 0 or not stale_replaceable,
        "status": (
            "clear"
            if active_count == 0
            else (
                "stale_replaceable_work_present"
                if stale_replaceable else "active_transient_work_present"
            )
        ),
        "workflow_name": "okg_source_sync",
        "queue_name": recovery_queue_name,
        "active_queue_names": list(dbos.get("active_queue_names") or []),
        "active_count": active_count,
        "active_rows": active_rows,
        "terminal_history_count": _int_value(
            dbos.get("terminal_history_count")
        ),
        "pending_count": _int_value(dbos.get("pending_count")),
        "waiting_count": _int_value(dbos.get("waiting_count")),
        "fresh_pending_count": _int_value(dbos.get("fresh_pending_count")),
        "running_pending_count": _int_value(
            dbos.get("running_pending_count")
        ),
        "replaceable_waiting_count": _int_value(
            dbos.get("replaceable_waiting_count")
        ),
        "oldest_active_age_seconds": dbos.get("oldest_active_age_seconds"),
        "newest_active_age_seconds": dbos.get("newest_active_age_seconds"),
        "oldest_pending_update_age_seconds": dbos.get(
            "oldest_pending_update_age_seconds"
        ),
        "oldest_waiting_age_seconds": dbos.get(
            "oldest_waiting_age_seconds"
        ),
        "oldest_replaceable_waiting_age_seconds": dbos.get(
            "oldest_replaceable_waiting_age_seconds"
        ),
        "stale_pending_work_present": bool(
            dbos.get("stale_pending_work_present")
        ),
        "stale_waiting_work_present": bool(
            dbos.get("stale_waiting_work_present")
        ),
        "stale_replaceable_work_present": stale_replaceable,
        "stale_after_seconds": 600,
        "safe_recovery": "dry_run_then_confirm_apply",
        "dry_run_command": commands.get("source_sync_dbos_gc_dry_run"),
        "apply_command": commands.get("source_sync_dbos_gc_apply"),
    }


def _runtime_worker_deployment_matches(
    tokens: list[str], deployment: str
) -> bool:
    """Return whether ``tokens`` select ``deployment`` via --deployment."""
    return any(
        token == f"--deployment={deployment}"
        or (
            token == "--deployment"
            and offset + 1 < len(tokens)
            and tokens[offset + 1] == deployment
        )
        for offset, token in enumerate(tokens)
    )


_RUNTIME_WORKER_MODULE = "okg.substrate.runtime.worker"


def _runtime_worker_module_invocation(
    argv: list[str], program: str, python_program: bool, deployment: str
) -> bool:
    """Match the worker module run directly, e.g. ``-m okg.substrate.runtime.worker``.

    The interpreter may carry flags before ``-m`` (the sealed-release adapter
    passes ``-I``), so the module selector is located rather than assumed to sit
    at a fixed index.
    """
    if not (python_program or program == "uv"):
        return False
    for index in range(len(argv) - 1):
        selector = argv[index]
        if selector not in {"-m", "--module"}:
            continue
        if argv[index + 1] != _RUNTIME_WORKER_MODULE:
            continue
        return _runtime_worker_deployment_matches(argv[index + 2 :], deployment)
    return False


def _runtime_worker_command_matches(command: str, deployment: str) -> bool:
    """Return whether ``command`` is a real worker invocation for deployment.

    Service adapters launch the worker in one of two argv SHAPES, and both must
    be recognised:

    * a CLI subcommand, where ``runtime worker`` appears as two adjacent argv
      tokens (the ``okg`` console script, ``okg.substrate.cli.main``, ``uv``);
    * the worker module directly, ``-m okg.substrate.runtime.worker``, which is
      what the sealed-release service adapter runs. Here ``runtime`` and
      ``worker`` are components of ONE dotted token, so a scan for the adjacent
      pair never fires (issue #1327). That made every sealed-release deployment
      report ``process_count: 0``/``not_observed`` while its worker was running,
      and — because ``split_executor`` is derived from the matched set — made
      the two-workers-on-one-database hazard undetectable rather than merely
      unreported.

    Match parsed argv, not a substring, so process-list readers see every form
    without mistaking an ``rg``/``grep`` command containing the same text for a
    worker.
    """
    try:
        argv = shlex.split(command)
    except ValueError:
        return False
    if not argv:
        return False

    program = Path(argv[0]).name
    python_program = program.lower().startswith("python")

    if _runtime_worker_module_invocation(argv, program, python_program, deployment):
        return True

    for index in range(len(argv) - 1):
        if argv[index : index + 2] != ["runtime", "worker"]:
            continue
        prefix = argv[:index]
        tail = argv[index + 2 :]
        deployment_matches = _runtime_worker_deployment_matches(tail, deployment)
        if not deployment_matches or not prefix:
            continue

        direct_console_script = (
            len(prefix) == 1 and program == "okg"
        )
        python_console_script = (
            python_program
            and len(prefix) == 2
            and Path(prefix[1]).name == "okg"
            and "/" in prefix[1]
        )
        python_module_entrypoint = (
            python_program
            and len(prefix) == 3
            and prefix[1:] == ["-m", "okg.substrate.cli.main"]
        )
        uv_console_script = (
            program == "uv"
            and prefix[1:] == ["run", "okg"]
        )
        uv_module_entrypoint = (
            program == "uv"
            and prefix[1:] == [
                "run",
                "--module",
                "okg.substrate.cli.main",
            ]
        )
        if (
            direct_console_script
            or python_console_script
            or python_module_entrypoint
            or uv_console_script
            or uv_module_entrypoint
        ):
            return True
    return False


def _runtime_worker_process_audit(deployment: str) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            ["ps", "-axo", "pid=,ppid=,command="],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        return {
            "ok": None,
            "status": "unavailable",
            "reason": str(exc),
            "process_count": 0,
            "root_count": 0,
            "split_executor": False,
            "processes": [],
            "roots": [],
        }
    if proc.returncode != 0:
        return {
            "ok": None,
            "status": "unavailable",
            "reason": proc.stderr.strip() or f"ps returned {proc.returncode}",
            "process_count": 0,
            "root_count": 0,
            "split_executor": False,
            "processes": [],
            "roots": [],
        }
    rows: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        parts = line.strip().split(maxsplit=2)
        if len(parts) != 3:
            continue
        pid_raw, ppid_raw, command = parts
        if not _runtime_worker_command_matches(command, deployment):
            continue
        try:
            pid = int(pid_raw)
            ppid = int(ppid_raw)
        except ValueError:
            continue
        rows.append({"pid": pid, "ppid": ppid, "command": command})
    pid_set = {int(row["pid"]) for row in rows}
    roots = [row for row in rows if int(row["ppid"]) not in pid_set]
    split_executor = len(roots) > 1
    status = (
        "split_executor"
        if split_executor
        else ("single_executor" if roots else "not_observed")
    )
    return {
        "ok": len(roots) == 1,
        "status": status,
        "process_count": len(rows),
        "root_count": len(roots),
        "split_executor": split_executor,
        "processes": rows,
        "roots": roots,
    }


def _runtime_process_summary(audit: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ok": audit.get("ok"),
        "status": audit.get("status"),
        "process_count": audit.get("process_count"),
        "root_count": audit.get("root_count"),
        "split_executor": audit.get("split_executor"),
        "roots": _list_of_mappings(audit.get("roots")),
    }


def _control_process_state_ok(
    action: str,
    audit: Mapping[str, Any],
) -> bool:
    if audit.get("split_executor"):
        return False
    if action == "down":
        return _runtime_worker_absence_proven(audit)
    return audit.get("ok") is True


def _runtime_worker_absence_proven(audit: Mapping[str, Any]) -> bool:
    return (
        audit.get("status") == "not_observed"
        and _int_value(audit.get("process_count")) == 0
    )


def _terminate_runtime_worker_roots(
    audit: Mapping[str, Any],
    *,
    grace_seconds: float = WORKER_GRACEFUL_SHUTDOWN_SECONDS,
) -> dict[str, Any]:
    """Terminate audited runtime-worker roots left behind by supervisor down.

    The process audit only includes commands matching the specific deployment's
    runtime-worker invocation. `down --apply` owns those processes after it has
    asked the supervisor to stop, so terminating them is the recovery contract
    for orphaned launchd/uv children.
    """
    rows = _list_of_mappings(audit.get("roots")) or _list_of_mappings(
        audit.get("processes")
    )
    rows_by_pid: dict[int, dict[str, Any]] = {}
    for row in rows:
        pid = _int_value(row.get("pid"))
        if pid and pid != os.getpid():
            rows_by_pid[pid] = row
    pids = sorted(rows_by_pid)
    result: dict[str, Any] = {
        "attempted": bool(pids),
        "signal": "SIGTERM",
        "pids": pids,
        "terminated": [],
        "still_running": [],
        "identity_mismatches": [],
        "errors": [],
    }
    if not pids:
        return result

    expected_identities: dict[int, dict[str, str | None]] = {}
    for pid in pids:
        expected_command = str(rows_by_pid[pid].get("command") or "")
        observed = _runtime_worker_process_identity(pid)
        if (
            observed.get("alive") is not True
            or observed.get("available") is not True
            or observed.get("command") != expected_command
        ):
            result["terminated"].append(pid)
            result["identity_mismatches"].append(
                {
                    "pid": pid,
                    "reason": observed.get("reason") or "command_changed",
                }
            )
            continue
        expected_identities[pid] = {
            "command": expected_command,
            "start_token": str(observed.get("start_token") or "") or None,
        }
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            result["terminated"].append(pid)
        except OSError as exc:
            result["errors"].append({"pid": pid, "error": str(exc)})

    def original_process_alive(pid: int) -> bool:
        identity = expected_identities.get(pid) or {}
        return _process_alive(
            pid,
            expected_command=identity.get("command"),
            expected_start_token=identity.get("start_token"),
        )

    deadline = time.monotonic() + max(0.0, grace_seconds)
    pending = [pid for pid in pids if pid not in result["terminated"]]
    while pending and time.monotonic() < deadline:
        pending = [pid for pid in pending if original_process_alive(pid)]
        if pending:
            time.sleep(0.1)

    still_running = [pid for pid in pending if original_process_alive(pid)]
    result["terminated"].extend(pid for pid in pids if pid not in still_running)
    result["still_running"] = still_running
    if still_running:
        result["signal"] = "SIGKILL"
        for pid in still_running:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                continue
            except OSError as exc:
                result["errors"].append({"pid": pid, "error": str(exc)})
        time.sleep(0.2)
        result["still_running"] = [
            pid for pid in still_running if original_process_alive(pid)
        ]
        result["terminated"].extend(
            pid for pid in still_running if pid not in result["still_running"]
        )
    result["terminated"] = sorted(set(result["terminated"]))
    return result


def _runtime_worker_process_identity(pid: int) -> dict[str, Any]:
    try:
        observed = subprocess.run(
            [
                "ps",
                "-p",
                str(pid),
                "-o",
                "stat=",
                "-o",
                "lstart=",
                "-o",
                "command=",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {
            "available": False,
            "alive": True,
            "reason": type(exc).__name__,
        }
    raw = observed.stdout.strip()
    if observed.returncode != 0 or not raw:
        return {
            "available": True,
            "alive": False,
            "reason": "not_observed",
        }
    parts = raw.split(maxsplit=6)
    state = parts[0]
    if state.upper().startswith("Z"):
        return {
            "available": True,
            "alive": False,
            "state": state,
            "reason": "zombie",
        }
    if len(parts) != 7:
        return {
            "available": False,
            "alive": True,
            "state": state,
            "reason": "identity_unparseable",
        }
    return {
        "available": True,
        "alive": True,
        "state": state,
        "start_token": " ".join(parts[1:6]),
        "command": parts[6],
    }


def _process_alive(
    pid: int,
    *,
    expected_command: str | None = None,
    expected_start_token: str | None = None,
) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        # With an expected identity, inability to probe must suppress
        # escalation: we cannot prove this is still the audited worker.
        return expected_command is None and expected_start_token is None
    identity = _runtime_worker_process_identity(pid)
    if identity.get("alive") is not True:
        return False
    if expected_command is not None:
        if identity.get("available") is not True:
            return False
        if identity.get("command") != expected_command:
            return False
    if (
        expected_start_token is not None
        and identity.get("start_token") != expected_start_token
    ):
        return False
    return True


def _runtime_process_recovery_command(
    deployment: str,
    audit: Mapping[str, Any],
) -> str:
    roots = _list_of_mappings(audit.get("roots"))
    pids = [
        str(row.get("pid"))
        for row in roots
        if row.get("pid") is not None
    ]
    quoted = shlex.quote(deployment)
    if pids:
        return f"{okg_prefix()} dogfood down --deployment {quoted} --apply --json"
    return f"{okg_prefix()} dogfood restart --deployment {quoted} --apply --json"


def _source_sync_dbos_recovery_commands(
    deployment: str,
    source_sync_status: Mapping[str, Any],
) -> dict[str, str]:
    dbos = _source_sync_dbos_workflows(source_sync_status)
    if dbos and dbos.get("available") is False:
        return {}
    queue_name = _source_sync_recovery_queue_name(deployment, dbos)
    quoted_deployment = shlex.quote(deployment)
    quoted_queue = shlex.quote(queue_name)
    prefix = _dogfood_local_profile_command_prefix()
    base = (
        f"{prefix}{okg_prefix()} db dbos-gc "
        f"--deployment {quoted_deployment} "
        "--stale-after-seconds 600 "
        f"--queue-name {quoted_queue} "
        "--workflow-name okg_source_sync "
        "--status ENQUEUED"
    )
    return {
        "source_sync_dbos_gc_dry_run": f"{base} --json",
        "source_sync_dbos_gc_apply": f"{base} --confirm --apply --json",
    }


def _control_apply_command(deployment: str, action: str) -> str:
    """Return the command that actually performs ``action``.

    A plan-only control run advertises this so the operator never has to infer
    that the flagless form they just ran did nothing (issue #944).
    """
    quoted = shlex.quote(deployment)
    prefix = _dogfood_local_profile_command_prefix()
    return (
        f"{prefix}{okg_prefix()} dogfood {action} --deployment {quoted} "
        "--apply --json"
    )


def _default_recovery_commands(
    deployment: str,
    *,
    readiness_profile: str | None = "dogfood",
) -> dict[str, str]:
    """The recovery commands a status payload offers.

    Issue #3958: the readiness gate re-runs the checks of the profile this
    deployment is judged by, and the service-render entries (the dogfood
    worker supervisor) are offered only under dogfood. With no resolved
    profile (``None``) no command names one.
    """
    quoted = shlex.quote(deployment)
    prefix = _dogfood_local_profile_command_prefix()
    commands = _all_default_recovery_commands(quoted, prefix)
    if readiness_profile is None:
        commands.pop("dogfood_ready_gate")
    else:
        commands["dogfood_ready_gate"] = (
            f"{prefix}{okg_prefix()} deployment ready {quoted} "
            f"--checks {shlex.quote(readiness_profile)} --json"
        )
    if readiness_profile != "dogfood":
        for key in (
            "service_render_launchd",
            "service_render_systemd",
            "service_render_docker_compose",
        ):
            commands.pop(key)
    return commands


def _all_default_recovery_commands(quoted: str, prefix: str) -> dict[str, str]:
    return {
        "dogfood_status": (
            f"{prefix}{okg_prefix()} dogfood status --deployment {quoted} --json"
        ),
        "dogfood_doctor": (
            f"{prefix}{okg_prefix()} dogfood doctor --deployment {quoted} --json"
        ),
        # Supervisor control subcommands only act under `--apply`; without it
        # they print a plan and exit. A recommended recovery command that
        # cannot change anything is worse than no recommendation, because the
        # operator reads the unchanged status as "the fix did not work"
        # (issue #944). Recommended entries therefore always carry `--apply`;
        # the `*_plan` keys expose the plan-only form for review.
        "dogfood_up": (
            f"{prefix}{okg_prefix()} dogfood up --deployment {quoted} "
            "--apply --json"
        ),
        "dogfood_up_plan": (
            f"{prefix}{okg_prefix()} dogfood up --deployment {quoted} --json"
        ),
        "dogfood_down": (
            f"{prefix}{okg_prefix()} dogfood down --deployment {quoted} "
            "--apply --json"
        ),
        "dogfood_down_plan": (
            f"{prefix}{okg_prefix()} dogfood down --deployment {quoted} --json"
        ),
        "dogfood_restart": (
            f"{prefix}{okg_prefix()} dogfood restart --deployment {quoted} "
            "--apply --json"
        ),
        "dogfood_restart_plan": (
            f"{prefix}{okg_prefix()} dogfood restart --deployment {quoted} --json"
        ),
        "service_render_launchd": (
            f"{prefix}{okg_prefix()} dogfood service render "
            f"--deployment {quoted} --adapter launchd --json"
        ),
        "service_render_systemd": (
            f"{prefix}{okg_prefix()} dogfood service render "
            f"--deployment {quoted} --adapter systemd --json"
        ),
        "service_render_docker_compose": (
            f"{prefix}{okg_prefix()} dogfood service render "
            f"--deployment {quoted} --adapter docker-compose --json"
        ),
        "dogfood_ready_gate": (
            f"{prefix}{okg_prefix()} deployment ready {quoted} --checks dogfood --json"
        ),
        "deployment_env": (
            f"{prefix}{okg_prefix()} deployment env {quoted} --json"
        ),
        "runtime_worker_foreground": (
            f"{prefix}{okg_prefix()} runtime worker --deployment {quoted}"
        ),
        "source_sync_health": (
            f"{prefix}{okg_prefix()} sync health --deployment {quoted} --json"
        ),
        "runtime_service_docs": "docs/operate/deployments/runtime.md",
    }


def _emit_status(payload: Mapping[str, Any], *, as_json: bool) -> None:
    if as_json:
        emit_result(payload, as_json=True)
        return
    print(f"dogfood deployment: {payload.get('deployment')}")
    print(f"trust_level: {payload.get('trust_level')}")
    print(f"ok: {payload.get('ok')}")
    read_authority = _mapping(payload.get("read_authority"))
    if read_authority:
        print(f"generation: {read_authority.get('generation_id')}")
        print(f"mcp_graph_read: {read_authority.get('mcp_graph_read')}")
    runtime_worker = _mapping(payload.get("runtime_worker"))
    if runtime_worker:
        print(f"runtime_worker: {runtime_worker.get('status')}")
        if runtime_worker.get("note"):
            print(f"  note: {runtime_worker.get('note')}")
    blockers = _list_of_mappings(payload.get("blockers"))
    print(f"blockers: {len(blockers)}")
    for blocker in blockers[:5]:
        print(f"  - {blocker.get('id')}: {blocker.get('message')}")
    next_action = _mapping(payload.get("next_action"))
    if next_action:
        print(f"next_action: {next_action.get('code')}")
        if next_action.get("command"):
            print(f"command: {next_action.get('command')}")


def _emit_control(payload: Mapping[str, Any], *, as_json: bool) -> None:
    if as_json:
        emit_result(payload, as_json=True)
        return
    print(f"dogfood deployment: {payload.get('deployment')}")
    print(f"action: {payload.get('action')}")
    print(f"adapter: {payload.get('adapter')}")
    print(f"apply: {payload.get('apply')}")
    print(f"applied: {payload.get('applied')}")
    print(f"result: {payload.get('status')}")
    if payload.get("status") == "planned":
        print(
            "NOTHING WAS EXECUTED: this was a plan only. Re-run with --apply "
            "to perform the action."
        )
        apply_command = payload.get("apply_command")
        if apply_command:
            print(f"apply command: {apply_command}")
    if not payload.get("supported", True):
        print(f"unsupported: {payload.get('error')}")
    for command in _list_of_mappings(payload.get("commands")):
        print(f"command: {command.get('display')}")
    status_command = _mapping(payload.get("status_command"))
    if status_command:
        print(f"status: {status_command.get('display')}")


def _emit_service(payload: Mapping[str, Any], *, as_json: bool) -> None:
    if as_json:
        emit_result(payload, as_json=True)
        return
    if payload.get("action") == "render":
        print(str(payload.get("content") or ""), end="")
        return
    print(f"dogfood deployment: {payload.get('deployment')}")
    print(f"adapter: {payload.get('adapter')}")
    print(f"output: {payload.get('output')}")
    print(f"written: {payload.get('written')}")


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list_of_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _float_value(value: Any) -> float | None:
    """Coerce to float, or ``None`` when there is no usable number.

    Unlike :func:`_int_value` this never substitutes a zero: callers use
    the ``None`` to mean "not measured", which must stay distinguishable
    from "measured as zero".
    """
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "DOGFOOD_STATUS_SCHEMA",
    "cmd_dogfood",
    "register_subparsers",
]
