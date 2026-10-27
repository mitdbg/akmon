"""Independent readiness checks for one sealed deployment environment."""

from __future__ import annotations

import json
import ipaddress
import os
import socket
import subprocess
import sys
import stat
import threading
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from okg.substrate.deployments import DeploymentConfigError
from okg.substrate.deployments.environment import DeploymentEnvironment
from okg.substrate.environment_services import (
    environment_process_authority_status,
    environment_release_handoff_status,
    environment_service_installation_status,
)
from okg.substrate.library.sources._url_safety import open_validated_url
from okg.substrate.release_activation import environment_release_status
from okg.substrate.runtime.release_authority import process_start_identity


def environment_readiness_report(
    environment: DeploymentEnvironment,
    plan: Mapping[str, Any],
    *,
    timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    """Probe environment endpoints, graph state, policy, and stdio MCP separately."""
    release = environment_release_status(environment)
    processes = environment_process_authority_status(environment)
    installation = environment_service_installation_status(environment, plan)
    handoff = environment_release_handoff_status(
        environment,
        expected_artifact_id=release.get("artifact_id"),
        expected_definition_digest=release.get("definition_digest"),
        expected_pointer_digest=release.get("active_pointer_digest"),
    )
    database = _probe_database_readiness(environment)
    operational = _probe_operational_readiness(environment, release=release)
    prerequisites = _probe_pact_prerequisites(environment, release=release)
    generation = database["generation"]
    nomos = database["nomos_read_path"]
    ui = _probe_ui_http(
        environment,
        processes=processes,
        timeout_seconds=timeout_seconds,
    )
    webhook = _probe_webhook_owner(
        environment,
        processes=processes,
        timeout_seconds=timeout_seconds,
    )
    registration = _load_mcp_registration(
        environment,
        plan,
    )
    mcp = _probe_mcp_registration(
        environment,
        registration=registration,
        expected_release=release,
        expected_generation_id=generation.get("generation_id"),
        timeout_seconds=timeout_seconds,
    )
    checks: list[dict[str, Any]] = []
    profile = _readiness_profile_resolution(environment)
    problem = profile.get("problem")
    if isinstance(problem, Mapping):
        # Fail closed (#3958): a conflicting, invalid or authored profile
        # blocks promotion with the same code `okg dogfood status` reports.
        _check(
            checks,
            "readiness.profile",
            False,
            "",
            (
                "the readiness profile cannot be resolved "
                f"({problem.get('code')}): {problem.get('message')}"
            ),
            dict(profile),
        )
    _check(
        checks,
        "release.active",
        bool(release.get("ok")),
        "active sealed release matches the environment",
        "active sealed release is missing or invalid",
        release,
    )
    _check(
        checks,
        "processes.authority",
        bool(processes.get("ok")),
        "worker and UI processes match sealed authority",
        "worker or UI process authority is missing, stale, or foreign",
        processes,
    )
    _check(
        checks,
        "services.installation",
        bool(installation.get("ok")),
        "installed service artifacts match the current environment plan",
        "installed service artifacts are missing, stale, or modified",
        installation,
    )
    _check(
        checks,
        "release.handoff",
        bool(handoff.get("ok")),
        "active release handoff stopped its predecessor and converged",
        "active release handoff is incomplete or an obsolete service returned",
        handoff,
    )
    _check(
        checks,
        "generation.latest_published",
        generation.get("status") == "published",
        "latest published generation resolved",
        "no published generation is available",
        generation,
    )
    checks.extend(
        dict(row)
        for row in operational.get("checks") or ()
        if str(row.get("id") or "").startswith(
            ("database.", "queues.", "sources.", "doctor.")
        )
    )
    _check(
        checks,
        "pact.prerequisites",
        bool(prerequisites.get("ok")),
        "required prerequisite PACTs are complete with accepted evidence",
        "one or more prerequisite PACTs are incomplete or invalid",
        prerequisites,
    )
    _check(
        checks,
        "ui.http",
        bool(ui.get("ok")),
        "UI health endpoint matches the environment and generation",
        "UI health endpoint is unavailable or serves foreign state",
        ui,
    )
    _check(
        checks,
        "webhook.owner",
        bool(webhook.get("ok")),
        "webhook endpoint is reachable and owned by the sealed worker",
        "webhook endpoint is unavailable or lacks worker ownership",
        webhook,
    )
    _check(
        checks,
        "nomos.read_path",
        bool(nomos.get("ok")),
        "standard read subjects are allowed by the active Nomos bundle",
        "Nomos read subjects are denied or policy state is unavailable",
        nomos,
    )
    _check(
        checks,
        "mcp.registration",
        bool(registration.get("ok")),
        "environment stdio MCP registration is installed and current",
        "environment stdio MCP registration is missing, stale, or foreign",
        registration,
    )
    _check(
        checks,
        "mcp.release_authority",
        bool(mcp.get("release_authority_ok")),
        "MCP process entered the environment's sealed release",
        "MCP process release root does not match the active environment",
        mcp.get("surface") or mcp,
    )
    _check(
        checks,
        "mcp.inspect",
        bool(mcp.get("inspect_ok")),
        "MCP inspect read succeeded",
        "MCP inspect read failed",
        mcp.get("inspect") or mcp,
    )
    _check(
        checks,
        "mcp.search",
        bool(mcp.get("search_ok")),
        "MCP search read succeeded",
        "MCP search read failed or was denied",
        mcp.get("search") or mcp,
    )
    _check(
        checks,
        "mcp.generation_alignment",
        bool(
            mcp.get("generation_aligned")
            and generation.get("generation_id")
            and ui.get("generation_id") == generation.get("generation_id")
        ),
        "UI and MCP reads pin the latest published generation",
        "UI or MCP is pinned to a stale or foreign generation",
        {
            "expected_generation_id": generation.get("generation_id"),
            "ui_generation_id": ui.get("generation_id"),
            "mcp_generation_ids": mcp.get("generation_ids") or [],
        },
    )
    blockers = [row for row in checks if row["status"] == "blocker"]
    # Issue #1290: an operational probe that exceeded its deadline arrives
    # as `unknown`. It is not a blocker -- nothing was proven broken -- but
    # it is not a pass either, and `ok` computed from blockers alone would
    # report a ready environment on evidence that was never collected.
    unknowns = [row for row in checks if row["status"] == "unknown"]
    return {
        "schema": "okg.environment_readiness/v1",
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "ok": not blockers and not unknowns,
        "status": (
            "blocked" if blockers
            else "unknown" if unknowns
            else "ready"
        ),
        "environment_id": environment.id,
        "tier": environment.tier,
        "deployment": environment.graph_deployment_name,
        "artifact_id": release.get("artifact_id"),
        "checks": checks,
        "blockers": blockers,
        "unknowns": unknowns,
        "release": release,
        "processes": processes,
        "generation": generation,
        "ui": ui,
        "webhook": webhook,
        "nomos_read_path": nomos,
        "mcp_registration": registration,
        "service_installation": installation,
        "release_handoff": handoff,
        "mcp": mcp,
        "database_readiness": operational.get("database"),
        "queues": operational.get("queues"),
        "source_freshness": operational.get("source_freshness"),
        "source_sync_status": operational.get("source_sync_status"),
        "source_sync_liveness": operational.get("source_sync_liveness"),
        "doctor": operational.get("doctor"),
        "prerequisite_pacts": prerequisites,
    }


def _readiness_profile_resolution(
    environment: DeploymentEnvironment,
) -> dict[str, Any]:
    """The readiness-profile resolver's answer for this environment (#3958).

    The same resolver `okg dogfood status` uses, over the composed manifest,
    so a definition whose ``readiness.profile`` disagrees with the
    environment's ``readiness.policy`` fails this gate too and cannot be
    promoted. The gate's own checks keep running under ``readiness.policy``,
    as before: when there is no problem and the policy chooses a profile, the
    resolver's answer IS that policy; a non-choosing policy (``org-private``)
    keeps its own source groups and Nomos posture here.
    """
    from okg.substrate.deployments.readiness_profile import (
        declared_readiness_profile,
    )

    resolved = declared_readiness_profile(
        environment.manifest.effective_raw, composed=True,
    )
    resolved["composed"] = True
    if isinstance(resolved.get("problem"), dict):
        resolved["problem"]["files"] = [
            str(environment.manifest.deployment_dir / "definition.yaml"),
            str(environment.descriptor_path),
        ]
    return resolved


def _probe_database_readiness(
    environment: DeploymentEnvironment,
) -> dict[str, Any]:
    try:
        from okg.substrate.db.connection import connect as db_connect

        from okg.substrate.cli.deployment_readiness import (
            _ready_generation_status,
            _ready_nomos_read_probe_status,
        )

        with db_connect(
            environment.database.dsn,
            connect_timeout=5,
            autocommit=True,
        ) as conn:
            try:
                generation = _ready_generation_status(
                    conn,
                    environment.graph_deployment_name,
                )
            except Exception as exc:  # noqa: BLE001 - independent diagnosis.
                generation = _unavailable_probe(exc)
            manifest = {"nomos": dict(environment.manifest.nomos or {})}
            try:
                nomos = _ready_nomos_read_probe_status(
                    conn,
                    deployment_name=environment.graph_deployment_name,
                    deployment_dir=environment.manifest.deployment_dir,
                    readiness_profile=environment.readiness.policy,
                    manifest_override=manifest,
                )
            except Exception as exc:  # noqa: BLE001 - independent diagnosis.
                nomos = _unavailable_probe(exc)
        return {"generation": generation, "nomos_read_path": nomos}
    except Exception as exc:  # noqa: BLE001 - readiness preserves blockers.
        failure = _unavailable_probe(exc)
        return {"generation": dict(failure), "nomos_read_path": dict(failure)}


def _probe_operational_readiness(
    environment: DeploymentEnvironment,
    *,
    release: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        from okg.substrate.cli.deployment import (
            _READY_DOGFOOD_MAX_PRODUCER_AGE_SECONDS,
            _READY_DOGFOOD_MAX_SOURCE_AGE_SECONDS,
            _deployment_ready_live_checks,
        )

        active_release = dict(release or environment_release_status(environment))
        if not active_release.get("ok") or not active_release.get("artifact_root"):
            raise DeploymentConfigError(
                "operational readiness requires a valid active sealed release"
            )
        with _sealed_environment_resolution(environment, active_release):
            return _deployment_ready_live_checks(
                deployment=environment.graph_deployment_name,
                source_namespace=environment.definition_id,
                deployment_dir=environment.manifest.deployment_dir,
                dsn=environment.database.dsn,
                max_active_queue_depth=0,
                max_source_age_seconds=_READY_DOGFOOD_MAX_SOURCE_AGE_SECONDS,
                readiness_profile=environment.readiness.policy,
                max_producer_age_seconds=(
                    _READY_DOGFOOD_MAX_PRODUCER_AGE_SECONDS
                ),
                skip_doctor=False,
            )
    except Exception as exc:  # noqa: BLE001 - readiness preserves blockers.
        failure = _unavailable_probe(exc)
        return {
            "checks": [
                {
                    "id": "database.operational_readiness",
                    "status": "blocker",
                    "ok": False,
                    "message": "operational readiness checks failed",
                    "details": failure,
                }
            ],
            "blockers": [],
            "warnings": [],
            # Present in every shape: a missing key must not read as
            # "nothing was left unmeasured" (issue #1290).
            "unknowns": [],
            "probes": [],
            "database": failure,
            "queues": failure,
            "source_freshness": failure,
            "doctor": failure,
        }


_ENVIRONMENT_RESOLUTION_LOCK = threading.Lock()


@contextmanager
def _sealed_environment_resolution(
    environment: DeploymentEnvironment,
    release: Mapping[str, Any],
):
    values = {
        "OKG_ENVIRONMENT_DESCRIPTOR": str(environment.descriptor_path),
        "OKG_ENVIRONMENT_ID": environment.id,
        "OKG_RELEASE_ARTIFACT_ID": str(release["artifact_id"]),
        "OKG_RELEASE_ROOT": str(release["artifact_root"]),
        "OKG_ACTIVE_RELEASE_POINTER": str(environment.release.active_pointer),
    }
    with _ENVIRONMENT_RESOLUTION_LOCK:
        previous = {key: os.environ.get(key) for key in values}
        os.environ.update(values)
        try:
            yield
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


def _probe_pact_prerequisites(
    environment: DeploymentEnvironment,
    *,
    release: Mapping[str, Any],
) -> dict[str, Any]:
    required = environment.readiness.prerequisite_pacts
    if not required:
        return {
            "ok": True,
            "status": "not_required",
            "required": [],
            "results": [],
        }
    artifact_root_value = release.get("artifact_root")
    if not isinstance(artifact_root_value, str) or not artifact_root_value:
        return {
            "ok": False,
            "status": "unavailable",
            "required": list(required),
            "results": [],
            "message": "sealed release root is required for PACT validation",
        }
    artifact_root = Path(artifact_root_value).expanduser().resolve()
    repo_root = artifact_root / "repo"
    if repo_root.is_symlink() or not repo_root.is_dir():
        return {
            "ok": False,
            "status": "unavailable",
            "required": list(required),
            "results": [],
            "artifact_id": release.get("artifact_id"),
            "code_revision": release.get("code_revision"),
            "message": "sealed release repository is missing or not regular",
        }
    results: list[dict[str, Any]] = []
    try:
        from okg.substrate.pact.cli import _status

        for change_id in required:
            status = _status(Path(repo_root), change_id, include_graph=False)
            complete = bool(
                status.get("ok")
                and status.get("retired")
                and not status.get("evidence_gaps")
                and not status.get("task_evidence_issues")
                and not status.get("next")
            )
            results.append(
                {
                    "id": change_id,
                    "ok": complete,
                    "status": status.get("status"),
                    "retired": status.get("retired"),
                    "evidence_gaps": status.get("evidence_gaps") or [],
                    "task_evidence_issues": (
                        status.get("task_evidence_issues") or []
                    ),
                    "validation_errors": status.get("errors") or [],
                }
            )
    except Exception as exc:  # noqa: BLE001 - readiness preserves blockers.
        return {
            "ok": False,
            "status": "unavailable",
            "required": list(required),
            "results": results,
            "error": type(exc).__name__,
            "message": str(exc),
        }
    ok = all(row["ok"] for row in results) and len(results) == len(required)
    return {
        "ok": ok,
        "status": "complete" if ok else "blocked",
        "required": list(required),
        "results": results,
        "artifact_id": release.get("artifact_id"),
        "code_revision": release.get("code_revision"),
        "repo_root": str(repo_root),
    }


def _probe_ui_http(
    environment: DeploymentEnvironment,
    *,
    processes: Mapping[str, Any] | None = None,
    timeout_seconds: float,
) -> dict[str, Any]:
    url = f"http://{environment.ui.host}:{environment.ui.port}/viz/api/health"
    request: urllib.request.Request | str = url
    token_env = getattr(environment.ui, "auth_token_env", None)
    if token_env:
        # The UI refuses unauthenticated reads (issue #3278), so the probe must
        # present the same token. It goes in the Authorization header only: it
        # is never put in the URL, the result, or an error message.
        token = os.environ.get(token_env, "")
        if not token.strip():
            return {
                "ok": False,
                "status": "unavailable",
                "url": url,
                "error": "UiAuthTokenUnavailable",
                "message": (
                    f"ui.auth_token_env names ${token_env}, which is unset or "
                    "empty in this process; the health probe cannot "
                    "authenticate"
                ),
            }
        # Prove the listener is the supervised UI process BEFORE sending the
        # credential: a foreign process on the port must never receive it.
        pre_service = ((processes or {}).get("services") or {}).get(
            environment.ui.service,
            {},
        )
        pre_authority = pre_service.get("authority") or {}
        pre_pid = pre_authority.get("pid")
        if not (
            pre_service.get("ok")
            and isinstance(pre_pid, int)
            and _pid_owns_tcp_listener(
                pre_pid, environment.ui.host, environment.ui.port,
            )
            and process_start_identity(pre_pid)
            == pre_authority.get("process_start_identity")
        ):
            return {
                "ok": False,
                "status": "foreign_or_unready",
                "url": url,
                "deployment_matches": False,
                "owner_matches": False,
                "expected_pid": pre_pid if isinstance(pre_pid, int) else None,
                "message": (
                    "the UI port is not owned by the supervised UI process; "
                    "no credential was sent"
                ),
            }
        request = urllib.request.Request(
            url, headers={"Authorization": f"Bearer {token}"},
        )
    try:
        with open_validated_url(
            request,
            allow_hosts=(environment.ui.host,),
            same_origin=environment.ui.host,
            timeout=timeout_seconds,
            max_redirects=0,
        ) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("UI health response is not an object")
        deployment_matches = (
            payload.get("deployment_name") == environment.graph_deployment_name
        )
        service = ((processes or {}).get("services") or {}).get(
            environment.ui.service,
            {},
        )
        expected_pid = (service.get("authority") or {}).get("pid")
        owner_matches = bool(
            service.get("ok")
            and isinstance(expected_pid, int)
            and payload.get("pid") == expected_pid
            and _pid_owns_tcp_listener(
                expected_pid,
                environment.ui.host,
                environment.ui.port,
            )
            and process_start_identity(expected_pid)
            == (service.get("authority") or {}).get("process_start_identity")
        )
        healthy = bool(
            payload.get("ok")
            and payload.get("ready")
            and deployment_matches
            and owner_matches
        )
        return {
            **payload,
            "ok": healthy,
            "status": "healthy" if healthy else "foreign_or_unready",
            "url": url,
            "deployment_matches": deployment_matches,
            "owner_matches": owner_matches,
            "expected_pid": expected_pid,
        }
    except Exception as exc:  # noqa: BLE001 - readiness preserves blockers.
        return {
            "ok": False,
            "status": "unavailable",
            "url": url,
            "error": type(exc).__name__,
            "message": str(exc),
        }


def _probe_webhook_owner(
    environment: DeploymentEnvironment,
    *,
    processes: Mapping[str, Any],
    timeout_seconds: float,
) -> dict[str, Any]:
    worker = processes.get("worker") or {}
    worker_authority = worker.get("authority") or {}
    url = (
        f"http://{environment.webhook.host}:{environment.webhook.port}"
        "/__okg/health"
    )
    try:
        with open_validated_url(
            url,
            allow_hosts=(environment.webhook.host,),
            same_origin=environment.webhook.host,
            timeout=timeout_seconds,
            max_redirects=0,
        ) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("webhook health response is not an object")
    except Exception as exc:  # noqa: BLE001 - readiness preserves blockers.
        payload = {}
        error = f"{type(exc).__name__}: {exc}"
    else:
        error = None
    config = (
        environment.manifest.runtime.source_sync.get("webhook", {})
        if environment.manifest.runtime.source_sync
        else {}
    )
    endpoint_matches = (
        config.get("enabled") is True
        and config.get("host") == environment.webhook.host
        and config.get("port") == environment.webhook.port
    )
    owner_matches = bool(
        worker.get("ok")
        and payload.get("ok") is True
        and payload.get("service") == "embedded_webhook"
        and payload.get("deployment") == environment.graph_deployment_name
        and payload.get("pid") == worker_authority.get("pid")
        and isinstance(worker_authority.get("pid"), int)
        and _pid_owns_tcp_listener(
            worker_authority["pid"],
            environment.webhook.host,
            environment.webhook.port,
        )
        and process_start_identity(worker_authority["pid"])
        == worker_authority.get("process_start_identity")
    )
    return {
        "ok": bool(payload) and endpoint_matches and owner_matches,
        "status": "healthy" if payload and endpoint_matches and owner_matches else "blocked",
        "service_id": environment.webhook.service,
        "owner_service": environment.runtime.worker_service,
        "endpoint": f"{environment.webhook.host}:{environment.webhook.port}",
        "url": url,
        "reachable": bool(payload),
        "endpoint_matches": endpoint_matches,
        "owner_matches": owner_matches,
        "observed": payload,
        "error": error,
    }


def _load_mcp_registration(
    environment: DeploymentEnvironment,
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    path = environment.runtime.state_root / "supervisor/mcp-registration.json"
    try:
        artifact = next(
            row
            for row in plan.get("artifacts") or []
            if row.get("kind") == "mcp_registration"
        )
        if Path(str(artifact.get("path") or "")) != path:
            raise DeploymentConfigError("MCP registration path differs from plan")
        if path.is_symlink() or not path.is_file():
            raise DeploymentConfigError("MCP registration is missing or not regular")
        if stat.S_IMODE(path.stat().st_mode) != 0o644:
            raise DeploymentConfigError("MCP registration mode is not 0644")
        expected_content = str(artifact.get("content") or "")
        if path.read_text(encoding="utf-8") != expected_content:
            raise DeploymentConfigError("MCP registration content differs from plan")
        raw = json.loads(path.read_text(encoding="utf-8"))
        if raw.get("schema") != "okg.environment_mcp_registration/v1":
            raise DeploymentConfigError("MCP registration has the wrong schema")
        if raw.get("environment_id") != environment.id:
            raise DeploymentConfigError("MCP registration belongs to another environment")
        if raw.get("deployment") != environment.graph_deployment_name:
            raise DeploymentConfigError("MCP registration targets another deployment")
        servers = raw.get("mcpServers") or {}
        server = servers.get(environment.mcp.registration_id)
        if not isinstance(server, dict):
            raise DeploymentConfigError("expected MCP registration id is missing")
        command = [str(server.get("command") or ""), *map(str, server.get("args") or [])]
        if not command[0] or not any(
            Path(part).name == "release_launcher.py" for part in command
        ):
            raise DeploymentConfigError("MCP registration bypasses the trusted launcher")
        if str(environment.ui.port) in command:
            raise DeploymentConfigError("MCP registration incorrectly uses the UI port")
        return {
            "ok": True,
            "status": "current",
            "path": str(path),
            "registration_id": environment.mcp.registration_id,
            "transport": environment.mcp.transport,
            "command": command,
            "plan_digest": plan.get("plan_digest"),
        }
    except Exception as exc:  # noqa: BLE001 - readiness preserves blockers.
        return {
            "ok": False,
            "status": "invalid" if path.exists() else "missing",
            "path": str(path),
            "registration_id": environment.mcp.registration_id,
            "error": type(exc).__name__,
            "message": str(exc),
        }


def _probe_mcp_registration(
    environment: DeploymentEnvironment,
    *,
    registration: Mapping[str, Any],
    expected_release: Mapping[str, Any],
    expected_generation_id: Any,
    timeout_seconds: float,
) -> dict[str, Any]:
    if not registration.get("ok"):
        return {
            "ok": False,
            "status": "not_registered",
            "release_authority_ok": False,
            "inspect_ok": False,
            "search_ok": False,
            "generation_aligned": False,
        }
    try:
        from okg.substrate.library.sources.mcp_composed.client import McpClient

        command = list(registration["command"])
        probe_environment = dict(os.environ)
        probe_environment.pop("OKG_SERVICE_ID", None)
        with McpClient(
            command,
            env=probe_environment,
            subprocess_timeout=timeout_seconds,
            client_info={"name": "okg-environment-readiness"},
        ) as client:
            # A program reads these results, so each call asks for the
            # compact-JSON result format (issue #3059).
            json_meta = {"okg/result_format": "json"}
            surface = _tool_payload(
                client.call_tool(
                    "inspect", {"target": {"kind": "surface"}}, meta=json_meta,
                )
            )
            inspect = _tool_payload(
                client.call_tool(
                    "inspect", {"target": {"kind": "graph"}}, meta=json_meta,
                )
            )
            search = _tool_payload(
                client.call_tool(
                    "search",
                    {
                        "query": environment.graph_deployment_name,
                        "method": "lexical",
                        "limit": 1,
                    },
                    meta=json_meta,
                )
            )
        authority = surface.get("release_authority") or {}
        release_authority_ok = (
            authority.get("status") == "complete"
            and authority.get("environment_id") == environment.id
            and authority.get("artifact_id") == expected_release.get("artifact_id")
            and Path(str(authority.get("artifact_root") or "")).resolve()
            == Path(str(expected_release.get("artifact_root") or "")).resolve()
            and authority.get("active_pointer")
            == str(environment.release.active_pointer)
        )
        generation_ids = [
            value
            for value in (
                inspect.get("pinned_generation_id") or inspect.get("generation_id"),
                search.get("pinned_generation_id") or search.get("generation_id"),
            )
            if value is not None
        ]
        inspect_ok = bool(
            inspect.get("status") != "refused"
            and "error" not in inspect
            and (
                inspect.get("pinned_generation_id")
                or inspect.get("generation_id")
            )
        )
        search_ok = search.get("status") != "refused" and "error" not in search
        generation_aligned = bool(
            expected_generation_id
            and len(generation_ids) == 2
            and all(value == expected_generation_id for value in generation_ids)
        )
        return {
            "ok": release_authority_ok and inspect_ok and search_ok and generation_aligned,
            "status": "healthy" if release_authority_ok and inspect_ok and search_ok and generation_aligned else "blocked",
            "release_authority_ok": release_authority_ok,
            "inspect_ok": inspect_ok,
            "search_ok": search_ok,
            "generation_aligned": generation_aligned,
            "generation_ids": generation_ids,
            "surface": surface,
            "inspect": inspect,
            "search": search,
        }
    except Exception as exc:  # noqa: BLE001 - readiness preserves blockers.
        return {
            "ok": False,
            "status": "probe_failed",
            "release_authority_ok": False,
            "inspect_ok": False,
            "search_ok": False,
            "generation_aligned": False,
            "error": type(exc).__name__,
            "message": str(exc),
        }


def _tool_payload(result: Mapping[str, Any]) -> dict[str, Any]:
    structured = result.get("structuredContent")
    if isinstance(structured, dict):
        candidate = structured.get("result", structured)
        if isinstance(candidate, dict):
            return dict(candidate)
    for item in result.get("content") or []:
        if not isinstance(item, Mapping):
            continue
        if isinstance(item.get("json"), Mapping):
            return dict(item["json"])
        if isinstance(item.get("text"), str):
            try:
                candidate = json.loads(item["text"])
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, dict):
                return candidate
    raise DeploymentConfigError("MCP tool returned no structured object")


def _unavailable_probe(exc: Exception) -> dict[str, Any]:
    return {
        "ok": False,
        "status": "unavailable",
        "error": type(exc).__name__,
        "message": str(exc),
    }


def _pid_owns_tcp_listener(
    pid: int,
    host: str,
    port: int,
    *,
    proc_root: Path = Path("/proc"),
) -> bool:
    """Verify the expected process owns the listening socket, fail closed."""
    expected_addresses = _resolved_addresses(host, port)
    if not expected_addresses:
        return False
    if sys.platform == "linux":
        inodes: set[str] = set()
        expected_port = f"{port:04X}"
        for table, family in (
            (proc_root / "net/tcp", socket.AF_INET),
            (proc_root / "net/tcp6", socket.AF_INET6),
        ):
            try:
                lines = table.read_text(encoding="utf-8").splitlines()[1:]
            except OSError:
                continue
            for line in lines:
                fields = line.split()
                if len(fields) > 9:
                    address_hex, local_port = fields[1].rsplit(":", 1)
                    observed = _decode_proc_address(address_hex, family)
                    if (
                        local_port == expected_port
                        and fields[3] == "0A"
                        and observed is not None
                        and _address_matches(
                            observed,
                            family,
                            expected_addresses,
                        )
                    ):
                        inodes.add(fields[9])
        if not inodes:
            return False
        try:
            fd_paths = list((proc_root / str(pid) / "fd").iterdir())
        except OSError:
            return False
        socket_targets = {f"socket:[{inode}]" for inode in inodes}
        for path in fd_paths:
            try:
                link = path.readlink()
            except FileNotFoundError:
                # Descriptors can close between iterdir() and readlink(). An
                # unrelated transient FD must not hide a still-owned listener.
                continue
            except OSError:
                return False
            if str(link) in socket_targets:
                return True
        return False
    if sys.platform == "darwin":
        executable = Path("/usr/sbin/lsof")
        command = str(executable) if executable.is_file() else "lsof"
        versions = {address.version for address in expected_addresses}
        family_hint = (
            socket.AF_INET
            if versions == {4}
            else socket.AF_INET6
            if versions == {6}
            else None
        )
        network_selector = (
            f"-i4TCP:{port}"
            if family_hint == socket.AF_INET
            else f"-i6TCP:{port}"
            if family_hint == socket.AF_INET6
            else f"-iTCP:{port}"
        )
        try:
            proc = subprocess.run(
                [
                    command,
                    "-nP",
                    "-a",
                    "-p",
                    str(pid),
                    network_selector,
                    "-sTCP:LISTEN",
                    "-FpPn",
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=2,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        if proc.returncode != 0:
            return False
        for line in proc.stdout.splitlines():
            if not line.startswith("n"):
                continue
            endpoint = line[1:].removesuffix(" (LISTEN)")
            address = endpoint.rsplit(":", 1)[0].strip("[]")
            if address == "*":
                if family_hint is None:
                    continue
                address = "0.0.0.0" if family_hint == socket.AF_INET else "::"
            try:
                observed = ipaddress.ip_address(address)
            except ValueError:
                continue
            family = socket.AF_INET if observed.version == 4 else socket.AF_INET6
            if _address_matches(observed, family, expected_addresses):
                return True
        return False
    return False


def _resolved_addresses(host: str, port: int) -> set[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    try:
        rows = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return set()
    addresses: set[ipaddress.IPv4Address | ipaddress.IPv6Address] = set()
    for row in rows:
        try:
            addresses.add(ipaddress.ip_address(row[4][0]))
        except ValueError:
            continue
    return addresses


def _address_matches(
    observed: ipaddress.IPv4Address | ipaddress.IPv6Address,
    family: int,
    expected: set[ipaddress.IPv4Address | ipaddress.IPv6Address],
) -> bool:
    same_family = {
        address
        for address in expected
        if (family == socket.AF_INET and address.version == 4)
        or (family == socket.AF_INET6 and address.version == 6)
    }
    return bool(same_family) and (observed.is_unspecified or observed in same_family)


def _decode_proc_address(
    value: str,
    family: int,
) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    try:
        raw = bytes.fromhex(value)
        if family == socket.AF_INET:
            packed = raw[::-1]
        else:
            packed = b"".join(
                raw[index : index + 4][::-1]
                for index in range(0, len(raw), 4)
            )
        return ipaddress.ip_address(packed)
    except ValueError:
        return None


def _check(
    checks: list[dict[str, Any]],
    check_id: str,
    ok: bool,
    pass_message: str,
    fail_message: str,
    details: Mapping[str, Any],
) -> None:
    checks.append(
        {
            "id": check_id,
            "status": "pass" if ok else "blocker",
            "ok": ok,
            "message": pass_message if ok else fail_message,
            "details": dict(details),
        }
    )
