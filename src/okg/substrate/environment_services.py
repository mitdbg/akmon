"""Environment-owned service topology, rendering, and lifecycle control."""

from __future__ import annotations

import fcntl
import hashlib
import ipaddress
import json
import os
import plistlib
import re
import secrets
import shlex
import stat
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import unquote, urlsplit

import psycopg
from okg.substrate.db.connection import connect as db_connect
from okg.substrate.db.role_lock import lock_roles_for_alter
from okg.substrate.db.wal_settings import graph_wal_command_args
from psycopg import sql
from psycopg.conninfo import make_conninfo
import yaml

from okg.substrate.deployments import DeploymentConfigError
from okg.substrate.deployments.environment import DeploymentEnvironment
from okg.substrate.unknown import Unknown, read_ok
from okg.substrate.release_activation import (
    _environment_release_status_locked,
    active_release_pointer_digest,
    environment_release_status,
    historical_release_environment,
    launcher_descriptor_path,
    load_active_release,
    release_activation_lock,
)
from okg.substrate.runtime.release_authority import (
    environment_worker_authority_status,
    pid_alive,
    process_runtime_identity,
    process_start_identity,
    receipt_launched_arguments,
)
from okg.substrate.sealed_release import SealedRelease


ENVIRONMENT_SERVICES_SCHEMA = "okg.environment_services/v1"
ENVIRONMENT_DATABASE_CREDENTIALS_SCHEMA = "okg.environment_database_role_credentials/v1"
ENVIRONMENT_DATABASE_ROLES = ("app_ro", "app_rw", "okg_mcp")
SERVICE_ADAPTERS = ("launchd", "systemd")
_SERVICE_ID_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
)
SERVICE_ACTIONS = (
    "render",
    "install",
    "start",
    "stop",
    "restart",
    "status",
    "logs",
)


CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]
_LIFECYCLE_LOCK_LOCAL = threading.local()


@dataclass(frozen=True)
class InstalledServiceFiles:
    environment_id: str
    paths: tuple[Path, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "environment_id": self.environment_id,
            "paths": [str(path) for path in self.paths],
        }


@contextmanager
def _environment_service_lifecycle_lock(
    environment: DeploymentEnvironment,
):
    """Serialize service mutation before taking the activation read lock."""
    # The lock is coordination metadata, not environment authority.  Keep it
    # outside state_root so a refused first install cannot change the byte
    # inventory whose predecessor authority it is still trying to prove.
    state_root = environment.runtime.state_root.resolve(strict=False)
    # state_root is the mutation boundary.  Keying solely by its canonical
    # path also serializes two mistakenly aliased environment descriptors.
    lock_key = hashlib.sha256(str(state_root).encode("utf-8")).hexdigest()
    lock_root = state_root.parent / ".okg-service-lifecycle-locks"
    if not lock_root.exists():
        lock_root.mkdir(mode=0o700, parents=True)
    if (
        lock_root.is_symlink()
        or not lock_root.is_dir()
        or stat.S_IMODE(lock_root.stat().st_mode) != 0o700
        or lock_root.stat().st_uid != os.geteuid()
    ):
        raise DeploymentConfigError(
            "service lifecycle lock directory must be owner-controlled mode 0700"
        )
    path = lock_root / f"{lock_key}.lock"
    key = str(path.resolve(strict=False))
    held = getattr(_LIFECYCLE_LOCK_LOCAL, "held", set())
    if key in held:
        yield
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.geteuid()
        ):
            raise DeploymentConfigError(
                "service lifecycle lock must be a 0600 regular file"
            )
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        held.add(key)
        _LIFECYCLE_LOCK_LOCAL.held = held
        try:
            yield
        finally:
            held.remove(key)
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


@contextmanager
def environment_service_lifecycle_transaction(
    environment: DeploymentEnvironment,
):
    """Hold the durable service mutex across a multi-step manager cutover."""
    with _environment_service_lifecycle_lock(environment):
        yield


def activate_environment_release_with_handoff(
    environment: DeploymentEnvironment,
    release: SealedRelease,
    *,
    source_repo: str | Path,
    activated_by: str,
    adapter: str,
    launcher_python: str = "/usr/bin/python3",
    systemd_scope: str = "system",
    postgres_image: str = "okg-pg17:local",
    environ: Mapping[str, str] | None = None,
    staging_environment: DeploymentEnvironment | None = None,
    approved_definition_digest: str | None = None,
    plan_validator: Callable[[Mapping[str, Any]], None] | None = None,
    runner: CommandRunner | None = None,
    authority_timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    """Select a release and complete its service-manager handoff atomically.

    A pointer move is only an intermediate state.  The returned receipt is
    successful only after installation, restart/start, and exact live authority
    readback converge.  A retry after convergence performs no service action.
    """
    from okg.substrate.release_activation import (  # noqa: PLC0415
        activate_environment_release,
    )

    with environment_service_lifecycle_transaction(environment):
        activation = activate_environment_release(
            environment=environment,
            release=release,
            source_repo=source_repo,
            activated_by=activated_by,
            environ=environ,
            staging_environment=staging_environment,
            approved_definition_digest=approved_definition_digest,
        )
        expected = {
            "expected_artifact_id": activation.artifact_id,
            "expected_definition_digest": activation.definition_digest,
            "expected_pointer_digest": activation.active_pointer_digest,
        }
        before = environment_release_handoff_status(
            environment,
            environ=environ,
            **expected,
        )
        payload: dict[str, Any] = {
            "schema": "okg.environment_release_activation_handoff/v1",
            "ok": False,
            "status": "incomplete",
            "environment_id": environment.id,
            "artifact_id": activation.artifact_id,
            "definition_digest": activation.definition_digest,
            "active_pointer_digest": activation.active_pointer_digest,
            "changed": activation.changed,
            "activation": activation.to_dict(),
            "handoff_before": before,
            "handoff": before,
            "service_action": None,
            "installed": None,
            "database_credentials": None,
            "install_results": [],
            "results": [],
            "supervision_evidence": None,
        }
        if before.get("ok") is True:
            payload.update({"ok": True, "status": "complete"})
            return payload

        plan = render_environment_services(
            environment,
            adapter=adapter,
            launcher_python=launcher_python,
            systemd_scope=systemd_scope,
            postgres_image=postgres_image,
            environ=environ,
        )
        if plan_validator is not None:
            plan_validator(plan)
        payload["plan_digest"] = plan["plan_digest"]
        installed = install_environment_services(environment, plan)
        credentials = reconcile_environment_database_credentials(environment)
        install_results = execute_environment_service_action(
            plan,
            action="install",
            environment=environment,
            runner=runner,
            authority_timeout_seconds=authority_timeout_seconds,
        )
        payload.update(
            {
                "installed": installed.to_dict(),
                "database_credentials": credentials,
                "install_results": install_results,
            }
        )
        if any(int(row.get("returncode", 1)) != 0 for row in install_results):
            payload["handoff"] = environment_release_handoff_status(
                environment,
                environ=environ,
                **expected,
            )
            return payload

        release_status = plan.get("release_status") or {}
        action = "restart" if release_status.get("previous_artifact_id") else "start"
        results = execute_environment_service_action(
            plan,
            action=action,
            environment=environment,
            runner=runner,
            authority_timeout_seconds=authority_timeout_seconds,
        )
        payload.update({"service_action": action, "results": results})
        if all(int(row.get("returncode", 1)) == 0 for row in results):
            payload["supervision_evidence"] = (
                write_environment_service_supervision_evidence(
                    environment,
                    plan,
                    action=action,
                    results=results,
                )
            )
        after = environment_release_handoff_status(
            environment,
            environ=environ,
            **expected,
        )
        complete = bool(
            after.get("ok") is True
            and install_results
            and all(int(row.get("returncode", 1)) == 0 for row in install_results)
            and results
            and all(int(row.get("returncode", 1)) == 0 for row in results)
        )
        payload.update(
            {
                "ok": complete,
                "status": "complete" if complete else "incomplete",
                "handoff": after,
            }
        )
        return payload


def render_environment_services(
    environment: DeploymentEnvironment,
    *,
    adapter: str,
    launcher_python: str = "/usr/bin/python3",
    systemd_scope: str = "system",
    postgres_image: str = "okg-pg17:local",
    environ: Mapping[str, str] | None = None,
    _release_status: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Render one environment's complete secret-free service topology."""
    if adapter not in SERVICE_ADAPTERS:
        raise DeploymentConfigError(
            f"environment service adapter must be one of {SERVICE_ADAPTERS}; "
            f"observed {adapter!r}"
        )
    if systemd_scope not in {"system", "user"}:
        raise DeploymentConfigError("systemd scope must be 'system' or 'user'")
    manager_scope = systemd_scope if adapter == "systemd" else None
    if environment.release.mode != "sealed":
        raise DeploymentConfigError(
            "environment service supervision requires release.mode 'sealed'"
        )
    if not environment.manifest.runtime.enabled:
        raise DeploymentConfigError(
            f"environment {environment.id!r} runtime.config.enabled must be true"
        )
    if (
        environment.database.container_uid is None
        or environment.database.container_gid is None
    ):
        raise DeploymentConfigError(
            f"environment {environment.id!r} cannot install services without "
            "an explicit sealed Postgres container owner declaration"
        )
    python = Path(launcher_python).expanduser()
    if not python.is_absolute():
        raise DeploymentConfigError("launcher Python must be an absolute path")

    state_root = environment.runtime.state_root
    supervisor_root = state_root / "supervisor"
    launcher_path = supervisor_root / "release_launcher.py"
    environment_exec_path = supervisor_root / "environment_exec.py"
    compose_path = supervisor_root / "postgres.compose.yaml"
    postgres_password_path = supervisor_root / "postgres-password"
    mcp_registration_path = supervisor_root / "mcp-registration.json"
    launcher_descriptor = launcher_descriptor_path(environment)
    worker_command = [
        str(python),
        str(launcher_path),
        "--descriptor",
        str(launcher_descriptor),
    ]
    ui_command = [
        *worker_command,
        "--",
        "-m",
        "okg.viz",
        "--deployment",
        environment.graph_deployment_name,
        "--host",
        environment.ui.host,
        "--port",
        str(environment.ui.port),
    ]
    mirror_command = [
        *worker_command,
        "--",
        "deployments/okg-ci/mirror_ci_results.py",
        "--cache",
        str(environment.runtime.data_root / "ci-results"),
        "--limit",
        "20",
        "--watch-interval-seconds",
        "300",
    ]
    mcp_command = [
        str(python),
        str(environment_exec_path),
        "--env-file",
        str(environment.runtime.local_env_file),
        "--",
        *worker_command,
        "--",
        "-m",
        "okg.substrate.cli.main",
        "mcp-serve",
        "--deployment",
        environment.graph_deployment_name,
    ]

    compose_content = _render_postgres_compose(
        environment,
        postgres_image=postgres_image,
        password_path=postgres_password_path,
    )
    service_artifacts = _render_host_services(
        environment,
        adapter=adapter,
        systemd_scope=systemd_scope,
        worker_command=worker_command,
        ui_command=ui_command,
        mirror_command=mirror_command,
        supervisor_root=supervisor_root,
        environment_exec_path=environment_exec_path,
    )
    artifacts = [
        {
            "kind": "launcher",
            "path": str(launcher_path),
            "mode": "0555",
            "source": str(_launcher_source()),
            "digest": _file_digest(_launcher_source()),
        },
        {
            "kind": "environment_file",
            "path": str(environment.runtime.local_env_file),
            "mode": "0600",
            "content": "<redacted>",
        },
        {
            "kind": "environment_exec",
            "path": str(environment_exec_path),
            "mode": "0555",
            "source": str(_environment_exec_source()),
            "digest": _file_digest(_environment_exec_source()),
        },
        {
            "kind": "postgres_password_file",
            "path": str(postgres_password_path),
            "mode": "0600",
            "owner": {
                "uid": environment.database.container_uid,
                "gid": environment.database.container_gid,
            },
            "content": "<redacted>",
        },
        {
            "kind": "database_compose",
            "path": str(compose_path),
            "mode": "0644",
            "content": compose_content,
        },
        {
            "kind": "mcp_registration",
            "path": str(mcp_registration_path),
            "mode": "0644",
            "content": _render_mcp_registration(
                environment,
                command=mcp_command,
            ),
        },
        *service_artifacts,
    ]
    release_status = (
        dict(_release_status)
        if _release_status is not None
        else environment_release_status(environment, environ=environ)
    )
    handoff_topology = _handoff_service_topology(
        environment,
        current_release=release_status,
        adapter=adapter,
        systemd_scope=manager_scope,
        environ=environ,
    )
    handoff_service_ids = tuple(handoff_topology["stop_services"])
    commands = _lifecycle_commands(
        environment,
        adapter=adapter,
        systemd_scope=systemd_scope,
        compose_path=compose_path,
        service_artifacts=service_artifacts,
        handoff_service_ids=handoff_service_ids,
    )
    process_authority = environment_process_authority_status(
        environment,
        release_status=release_status,
    )
    plan = {
        "schema": ENVIRONMENT_SERVICES_SCHEMA,
        "ok": True,
        "environment_id": environment.id,
        "tier": environment.tier,
        "graph_deployment_name": environment.graph_deployment_name,
        "adapter": adapter,
        "systemd_scope": manager_scope,
        "postgres_image": postgres_image,
        "release_status": release_status,
        "worker_authority": process_authority["worker"],
        "service_authority": process_authority["services"],
        "launchable": bool(release_status["ok"]),
        "launcher": {
            "python": str(python),
            "path": str(launcher_path),
            "descriptor": str(launcher_descriptor),
        },
        "artifacts": artifacts,
        "topology": _service_topology(environment),
        "handoff_topology": handoff_topology,
        "commands": commands,
    }
    plan["plan_digest"] = _service_plan_digest(plan)
    plan["installation"] = environment_service_installation_status(
        environment,
        plan,
    )
    plan["startable"] = bool(plan["launchable"] and plan["installation"]["ok"])
    return plan


def install_environment_services(
    environment: DeploymentEnvironment,
    plan: Mapping[str, Any],
) -> InstalledServiceFiles:
    """Atomically install rendered files and the secret environment file."""
    # Lock order is lifecycle first, then the shared activation snapshot.
    # Activation never acquires the lifecycle lock, so this has no reverse edge.
    with _environment_service_lifecycle_lock(environment):
        # This is an early refusal only; the complete check is repeated under
        # the activation lock below.  Keeping the manager census ahead of that
        # lock ensures a refused first install does not create active.json.lock
        # inside the protected environment state.
        _require_environment_service_install_preflight(environment, plan)
        with release_activation_lock(
            environment.release.active_pointer,
            exclusive=False,
        ):
            return _install_environment_services_locked(environment, plan)


def _install_environment_services_locked(
    environment: DeploymentEnvironment,
    plan: Mapping[str, Any],
) -> InstalledServiceFiles:
    """Install files while holding the shared release-activation lock."""
    if plan.get("environment_id") != environment.id:
        raise DeploymentConfigError("service plan belongs to a different environment")
    observed_release = environment_release_status(environment)
    _require_plan_release_snapshot(plan, observed_release=observed_release)
    _validate_service_plan(
        environment,
        plan,
        release_status=observed_release,
    )
    release = plan.get("release_status") or {}
    previous_installation = _previous_installation_binding(environment, plan)
    if release.get("previous_artifact_id") and previous_installation is None:
        raise DeploymentConfigError(
            "verified predecessor service installation binding is unavailable"
        )
    # Validate the read-only predecessor binding before process-authority
    # reconciliation.  The latter may persist a stop-record rebind after it
    # has proved the old process is gone; a refused install must not mutate
    # that protected lifecycle state when its installation ancestry is bad.
    _require_environment_service_install_authority(environment, plan)
    environment.runtime.log_root.mkdir(parents=True, exist_ok=True)
    environment.runtime.log_root.chmod(0o750)
    _require_managed_loopback_database(environment)
    role_passwords = _environment_database_role_passwords(
        environment,
        create=True,
    )
    credentials_path = _environment_database_credentials_path(environment)
    installed: list[Path] = [credentials_path]
    for artifact in plan.get("artifacts") or ():
        if not isinstance(artifact, Mapping):
            raise DeploymentConfigError("service artifact must be a mapping")
        kind = str(artifact.get("kind") or "")
        path = Path(str(artifact.get("path") or "")).expanduser()
        _require_install_path(environment, path)
        mode = int(str(artifact.get("mode") or "0"), 8)
        expected_modes = {
            "launcher": 0o555,
            "environment_file": 0o600,
            "environment_exec": 0o555,
            "postgres_password_file": 0o600,
            "database_compose": 0o644,
            "worker_service": 0o644,
            "ui_service": 0o644,
            "mirror_service": 0o644,
            "mcp_registration": 0o644,
        }
        if kind not in expected_modes or mode != expected_modes[kind]:
            raise DeploymentConfigError(
                f"service artifact {kind!r} has invalid mode {mode:o}"
            )
        owner: tuple[int, int] | None = None
        declared_owner = artifact.get("owner")
        if kind == "postgres_password_file":
            expected_owner = {
                "uid": environment.database.container_uid,
                "gid": environment.database.container_gid,
            }
            if declared_owner != expected_owner:
                raise DeploymentConfigError(
                    "postgres password artifact owner does not match the "
                    "environment container identity"
                )
            owner = (expected_owner["uid"], expected_owner["gid"])
        elif declared_owner is not None:
            raise DeploymentConfigError(
                f"service artifact {kind!r} declares an unexpected owner"
            )
        if kind == "launcher":
            content = _launcher_source().read_bytes()
        elif kind == "environment_exec":
            content = _environment_exec_source().read_bytes()
        elif kind == "environment_file":
            content = _environment_file_content(
                environment,
                role_passwords=role_passwords,
            ).encode("utf-8")
        elif kind == "postgres_password_file":
            content = (_database_password(environment) + "\n").encode("utf-8")
        else:
            rendered = artifact.get("content")
            if not isinstance(rendered, str):
                raise DeploymentConfigError(
                    f"service artifact {kind!r} has no rendered content"
                )
            content = rendered.encode("utf-8")
        _atomic_write(path, content, mode=mode, owner=owner)
        installed.append(path)
    marker = environment.runtime.state_root / "supervisor/install.json"
    marker_payload = {
        "schema": "okg.environment_service_install/v1",
        "environment_id": environment.id,
        "artifact_id": release.get("artifact_id"),
        "plan_digest": plan.get("plan_digest"),
        "adapter": plan.get("adapter"),
        "systemd_scope": (
            plan.get("systemd_scope") if plan.get("adapter") == "systemd" else None
        ),
        "launcher_python": (plan.get("launcher") or {}).get("python"),
        "postgres_image": plan.get("postgres_image"),
        "service_topology": (
            ((plan.get("handoff_topology") or {}).get("generations") or {}).get(
                "current"
            )
        ),
        "installed_at": datetime.now(timezone.utc).isoformat(),
    }
    if previous_installation is not None:
        marker_payload["previous_installation"] = previous_installation
    _atomic_write(
        marker,
        (json.dumps(marker_payload, indent=2, sort_keys=True) + "\n").encode("utf-8"),
        mode=0o600,
    )
    installed.append(marker)
    return InstalledServiceFiles(
        environment_id=environment.id,
        paths=tuple(installed),
    )


def reconcile_environment_database_credentials(
    environment: DeploymentEnvironment,
) -> dict[str, Any]:
    """Reset supervised loopback role credentials to the runtime contract."""
    host = _require_managed_loopback_database(environment)

    role_passwords = _environment_database_role_passwords(
        environment,
        create=True,
    )
    roles = tuple(sorted(role_passwords))
    try:
        with db_connect(environment.database.dsn) as conn:
            existing = {
                str(row[0])
                for row in conn.execute(
                    "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)",
                    (list(roles),),
                ).fetchall()
            }
            missing = sorted(set(roles) - existing)
            if missing:
                raise DeploymentConfigError(
                    "environment database is missing required supervised roles: "
                    + ", ".join(missing)
                )
            # These roles are cluster-wide rows another session (a runtime
            # bootstrap, another environment) may be altering right now; hold
            # their cluster locks until the commit below (issue #1985).
            lock_roles_for_alter(conn, roles)
            for role, password in role_passwords.items():
                conn.execute(
                    sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {}").format(
                        sql.Identifier(role),
                        sql.Literal(password),
                    )
                )
            conn.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(environment.database.name),
                    sql.SQL(", ").join(sql.Identifier(role) for role in roles),
                )
            )
            conn.commit()
    except DeploymentConfigError:
        raise
    except psycopg.Error as exc:
        raise DeploymentConfigError(
            "environment service installation could not reconcile local "
            f"database role credentials: {type(exc).__name__}"
        ) from exc

    return {
        "schema": "okg.environment_database_credentials/v1",
        "ok": True,
        "environment_id": environment.id,
        "host": host or "local-socket",
        "roles": list(roles),
        "credentials_path": str(_environment_database_credentials_path(environment)),
    }


def _require_managed_loopback_database(
    environment: DeploymentEnvironment,
) -> str:
    host = urlsplit(environment.database.dsn).hostname or ""
    if host and host != "localhost":
        try:
            loopback = ipaddress.ip_address(host).is_loopback
        except ValueError:
            loopback = False
        if not loopback:
            raise DeploymentConfigError(
                "environment service installation cannot manage database role "
                f"credentials on non-loopback host {host!r}; configure explicit "
                "OKG_MCP_RO_DSN and OKG_MCP_RW_DSN secrets instead"
            )
    return host


def execute_environment_service_action(
    plan: Mapping[str, Any],
    *,
    action: str,
    environment: DeploymentEnvironment,
    runner: CommandRunner | None = None,
    authority_timeout_seconds: float = 30.0,
) -> list[dict[str, Any]]:
    """Execute one environment-scoped lifecycle plan without shell expansion."""
    if action not in set(SERVICE_ACTIONS) - {"render"}:
        raise DeploymentConfigError(f"unsupported service action: {action}")
    _validate_service_plan(environment, plan)
    if action in {"start", "restart"} and not plan.get("launchable"):
        release_status = plan.get("release_status") or {}
        raise DeploymentConfigError(
            "sealed service start requires a valid active release; "
            f"observed {release_status.get('status', 'unknown')}"
        )
    command_rows = (plan.get("commands") or {}).get(action) or ()
    invoke = runner or _run_command
    results: list[dict[str, Any]] = []
    if plan.get("environment_id") != environment.id:
        raise DeploymentConfigError(
            "service lifecycle plan belongs to a different environment"
        )
    requires_start_guard = action in {"start", "restart"}
    process_authority = _environment_process_authority_status(environment)
    status_installation = environment_service_installation_status(environment, plan)
    if action == "status" and (
        not process_authority["ok"] or not status_installation.get("ok")
    ):
        return [
            _authority_result(
                process_authority,
                installation=status_installation,
            )
        ]
    previous_release_handoff = bool(
        action in {"install", "restart"}
        and not process_authority["ok"]
        and (plan.get("release_status") or {}).get("previous_artifact_id")
    )
    attested_stop_retry = bool(
        action == "stop"
        and not process_authority["ok"]
        and _attested_stop_retry_available(environment, plan=plan)
    )
    if (
        process_authority["status"] == "invalid"
        and not previous_release_handoff
        and action != "restart"
        and not attested_stop_retry
    ):
        raise DeploymentConfigError(
            "environment process authority is invalid or partial: "
            f"{process_authority['message']}"
        )
    if (
        action in {"stop", "logs"}
        and not process_authority["ok"]
        and not attested_stop_retry
    ):
        raise DeploymentConfigError(
            f"environment service {action} requires healthy process authority"
        )
    staging_guard = (
        _staging_clone_start_guard(environment, plan)
        if requires_start_guard and environment.readiness.requires_clone_rebind
        else nullcontext()
    )
    with (
        _environment_service_lifecycle_lock(environment),
        staging_guard,
        release_activation_lock(environment.release.active_pointer, exclusive=False),
    ):
        locked_release = environment_release_status(environment)
        if action in {"install", "start", "restart"}:
            _require_plan_release_snapshot(
                plan,
                observed_release=locked_release,
            )
            _validate_service_plan(
                environment,
                plan,
                release_status=locked_release,
            )
        current_release: Mapping[str, Any] | None = None
        process_inventory: Mapping[str, Any] | None = None
        if requires_start_guard:
            installation = environment_service_installation_status(
                environment,
                plan,
            )
            if not installation["ok"]:
                raise DeploymentConfigError(
                    "environment service installation does not match the plan: "
                    f"{installation.get('message', installation['status'])}"
                )
            current_release = environment_release_status(environment)
            planned_artifact = (plan.get("release_status") or {}).get("artifact_id")
            if (
                not current_release.get("ok")
                or current_release.get("artifact_id") != planned_artifact
            ):
                raise DeploymentConfigError(
                    "active release changed before service start"
                )
            locked_process_authority = _environment_process_authority_status(
                environment,
                release_status=current_release,
            )
            if action == "start" and not locked_process_authority.get("ok"):
                if locked_process_authority.get("status") != "stopped":
                    raise DeploymentConfigError(
                        "environment service start requires healthy or absent "
                        "process authority"
                    )
                _require_receiptless_managers_stopped(environment, plan=plan)
        elif previous_release_handoff:
            current_release = environment_release_status(environment)
            planned_artifact = (plan.get("release_status") or {}).get("artifact_id")
            if (
                not current_release.get("ok")
                or current_release.get("artifact_id") != planned_artifact
            ):
                raise DeploymentConfigError(
                    "active release changed before service install"
                )
        if previous_release_handoff and action != "restart":
            _require_verified_previous_process_authority(
                environment,
                plan=plan,
                current_release=current_release,
            )
        if action in {"restart", "stop"}:
            if current_release is None:
                current_release = environment_release_status(environment)
            if not current_release.get("ok"):
                raise DeploymentConfigError(
                    f"environment service {action} requires a valid active release"
                )
            process_inventory = _restart_process_inventory(
                environment,
                plan=plan,
                current_release=current_release,
            )
            pending, pending_snapshot_deferred = (
                _pending_dbos_workflows_for_service_action(
                    environment,
                    action=action,
                    plan=plan,
                    process_inventory=process_inventory,
                    allow_restart_database_recovery=True,
                )
            )
            restart_recovery: tuple[dict[str, Any], ...] = ()
            if pending:
                if action == "restart" and process_inventory.get(
                    "recovered_from_stop_record"
                ):
                    restart_recovery = _recoverable_pending_workflows(pending)
                else:
                    _raise_pending_dbos_workflows(pending)
        else:
            restart_recovery = ()
            pending_snapshot_deferred = False
        replacement_started_after_epoch_ms: int | None = None
        stop_record_invalidated_for_start = False
        stop_boundary_attested = False
        for row in command_rows:
            argv = [str(part) for part in row.get("argv") or ()]
            if not argv:
                raise DeploymentConfigError("service command has empty argv")
            if pending_snapshot_deferred and not _is_database_start_command(argv):
                raise DeploymentConfigError(
                    "restart must restore the exact managed database before "
                    "mutating host services"
                )
            if (
                requires_start_guard
                and row.get("handoff_phase") == "start"
                and not stop_record_invalidated_for_start
            ):
                assert current_release is not None
                _invalidate_predecessor_stop_before_start(
                    environment,
                    plan=plan,
                    release_status=current_release,
                )
                stop_record_invalidated_for_start = True
                replacement_started_after_epoch_ms = int(time.time() * 1000)
            try:
                proc = invoke(argv)
            except OSError as exc:
                results.append(
                    {
                        "argv": argv,
                        "display": row.get("display"),
                        "returncode": 127,
                        "stdout": "",
                        "stderr": f"{type(exc).__name__}: {exc}",
                    }
                )
                break
            accepted_stopped = bool(
                action == "restart"
                and row.get("handoff_phase") == "stop"
                and proc.returncode != 0
                and _restart_stop_target_is_stopped(
                    environment,
                    plan=plan,
                    service_id=row.get("service_id"),
                )
            )
            results.append(
                {
                    "argv": argv,
                    "display": row.get("display"),
                    "returncode": 0 if accepted_stopped else int(proc.returncode),
                    "stdout": proc.stdout,
                    "stderr": "" if accepted_stopped else proc.stderr,
                    "already_stopped": accepted_stopped,
                }
            )
            if results[-1]["returncode"] != 0:
                break
            if pending_snapshot_deferred and _is_database_start_command(argv):
                pending = _pending_dbos_workflows(environment)
                pending_snapshot_deferred = False
                if pending:
                    _raise_pending_dbos_workflows(pending)
            if action in {"restart", "stop"} and row.get("handoff_boundary") is True:
                assert process_inventory is not None
                pending, _ = _pending_dbos_workflows_for_service_action(
                    environment,
                    action=action,
                    plan=plan,
                    process_inventory=process_inventory,
                )
                _attest_restart_inventory_stopped(
                    environment,
                    plan=plan,
                    inventory=process_inventory,
                )
                _record_predecessor_stop(
                    environment,
                    inventory=process_inventory,
                    cross_activation_eligible=action == "stop",
                )
                if pending:
                    if action != "restart":
                        _raise_pending_dbos_workflows(
                            pending,
                            recovery="restart the stopped services to recover them",
                        )
                    restart_recovery = _recoverable_pending_workflows(pending)
                _remove_environment_authority_receipts(
                    environment,
                    service_ids=_inventory_service_ids(
                        environment,
                        process_inventory,
                    ),
                )
                stop_boundary_attested = True
        commands_succeeded = all(row["returncode"] == 0 for row in results)
        if action == "stop" and commands_succeeded and not stop_boundary_attested:
            assert process_inventory is not None
            _attest_restart_inventory_stopped(
                environment,
                plan=plan,
                inventory=process_inventory,
            )
            _record_predecessor_stop(
                environment,
                inventory=process_inventory,
                cross_activation_eligible=True,
            )
            _remove_environment_authority_receipts(
                environment,
                service_ids=_inventory_service_ids(
                    environment,
                    process_inventory,
                ),
            )
        if requires_start_guard and commands_succeeded:
            authority = _wait_for_service_authority(
                environment,
                timeout_seconds=authority_timeout_seconds,
            )
            results.append(
                _authority_result(
                    authority,
                    installation=environment_service_installation_status(
                        environment,
                        plan,
                    ),
                )
            )
            if restart_recovery:
                if replacement_started_after_epoch_ms is None:
                    raise DeploymentConfigError(
                        "replacement worker recovery lacks a start boundary"
                    )
                _wait_for_dbos_workflow_recovery(
                    environment,
                    captured=restart_recovery,
                    replacement_started_after_epoch_ms=(
                        replacement_started_after_epoch_ms
                    ),
                    timeout_seconds=authority_timeout_seconds,
                )
        elif action == "status" and commands_succeeded:
            results.append(
                _authority_result(
                    process_authority,
                    installation=status_installation,
                )
            )
    return results


def _attested_stop_retry_available(
    environment: DeploymentEnvironment,
    *,
    plan: Mapping[str, Any],
) -> bool:
    """Admit only an exact all-stopped retry after authority cleanup began."""
    release = environment_release_status(environment)
    planned = plan.get("release_status") or {}
    if (
        not release.get("ok")
        or release.get("artifact_id") != planned.get("artifact_id")
        or release.get("definition_digest") != planned.get("definition_digest")
        or release.get("active_pointer_digest")
        != planned.get("active_pointer_digest")
    ):
        return False
    topology = plan.get("handoff_topology")
    services = topology.get("stop_services") if isinstance(topology, Mapping) else None
    if not isinstance(services, list) or not all(
        isinstance(service_id, str) and service_id for service_id in services
    ):
        return False
    service_ids = tuple(services)
    status = _predecessor_stop_status(
        environment,
        release_status=release,
        expected_services=service_ids,
        expected_plan_digest=plan.get("plan_digest"),
        plan=plan,
        require_record=True,
        require_obsolete_receipts_absent=False,
        allow_completed_start=True,
    )
    return bool(
        status.get("ok") is True
        and status.get("status") == "stopped"
        and _all_handoff_services_stopped(
            environment,
            plan=plan,
            service_ids=service_ids,
        )
    )


def _pending_dbos_workflows_for_service_action(
    environment: DeploymentEnvironment,
    *,
    action: str,
    plan: Mapping[str, Any],
    process_inventory: Mapping[str, Any],
    allow_restart_database_recovery: bool = False,
) -> tuple[tuple[dict[str, Any], ...], bool]:
    """Snapshot DBOS, admitting only an exact repeat of a completed stop."""
    try:
        return _pending_dbos_workflows(environment), False
    except DeploymentConfigError as exc:
        database_error = exc.__cause__
        service_ids = _inventory_service_ids(environment, process_inventory)
        exact_stopped_retry = bool(
            process_inventory.get("recovered_from_stop_record") is True
            and isinstance(database_error, psycopg.Error)
            and _all_handoff_services_stopped(
                environment,
                plan=plan,
                service_ids=service_ids,
            )
            and _managed_postgres_target_is_stopped(plan)
        )
        if not exact_stopped_retry or action not in {"stop", "restart"}:
            raise
        if action == "restart":
            if not allow_restart_database_recovery:
                raise
            return (), True
        return (), False


def _managed_postgres_target_is_stopped(plan: Mapping[str, Any]) -> bool:
    """Independently read back the exact Compose-managed Postgres target."""
    status_rows = (plan.get("commands") or {}).get("status") or ()
    matches = [
        tuple(str(part) for part in row.get("argv") or ())
        for row in status_rows
        if isinstance(row, Mapping)
        and _is_database_status_command(row.get("argv") or ())
    ]
    if len(matches) != 1:
        return False
    values = list(matches[0])
    try:
        ps_index = values.index("ps")
    except ValueError:
        return False
    argv = [*values[:ps_index], "ps", "--all", "--format", "json", "postgres"]
    try:
        observed = _run_command(argv)
    except OSError:
        return False
    if observed.returncode != 0:
        return False
    raw = observed.stdout.strip()
    if not raw:
        return True
    try:
        decoded = json.loads(raw)
        rows = decoded if isinstance(decoded, list) else [decoded]
    except (TypeError, ValueError):
        try:
            rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
        except (TypeError, ValueError):
            return False
    if not rows or not all(isinstance(row, Mapping) for row in rows):
        return False
    stopped_states = {"created", "dead", "exited"}
    return all(
        str(row.get("Service") or "") == "postgres"
        and str(row.get("State") or "").casefold() in stopped_states
        for row in rows
    )


def fence_environment_host_services(
    plan: Mapping[str, Any],
    *,
    environment: DeploymentEnvironment,
    runner: CommandRunner | None = None,
) -> dict[str, Any]:
    """Stop environment-owned host processes while keeping Postgres available."""
    _validate_service_plan(environment, plan)
    rows = list((plan.get("commands") or {}).get("stop") or ())
    database_rows = [
        row for row in rows if _is_database_stop_command(row.get("argv") or ())
    ]
    if len(database_rows) != 1:
        raise DeploymentConfigError(
            "service plan must identify exactly one database stop command"
        )
    invoke = runner or _run_command
    results: list[dict[str, Any]] = []
    with (
        _environment_service_lifecycle_lock(environment),
        release_activation_lock(
            environment.release.active_pointer,
            exclusive=False,
        ),
    ):
        release = environment_release_status(environment)
        if not release.get("ok"):
            raise DeploymentConfigError(
                "host-service fence requires valid release authority"
            )
        inventory = _restart_process_inventory(
            environment,
            plan=plan,
            current_release=release,
        )
        _require_no_pending_dbos_workflows(environment)
        for row in rows:
            argv = [str(part) for part in row.get("argv") or ()]
            if _is_database_stop_command(argv):
                continue
            if not argv:
                raise DeploymentConfigError("service stop command has empty argv")
            proc = invoke(argv)
            accepted_absent = bool(
                proc.returncode != 0 and _service_stop_target_is_absent(argv)
            )
            result = {
                "argv": argv,
                "display": row.get("display"),
                "returncode": 0 if accepted_absent else int(proc.returncode),
                "stdout": proc.stdout,
                "stderr": "" if accepted_absent else proc.stderr,
                "already_absent": accepted_absent,
            }
            results.append(result)
            if result["returncode"] != 0:
                raise DeploymentConfigError(
                    f"failed to fence environment host service: {row.get('display')}"
                )
        pending = _pending_dbos_workflows(environment)
        if pending:
            _attest_restart_inventory_stopped(
                environment,
                plan=plan,
                inventory=inventory,
            )
            _record_predecessor_stop(environment, inventory=inventory)
            _raise_pending_dbos_workflows(
                pending,
                recovery="restart the fenced services to recover them",
            )
        _attest_restart_inventory_stopped(
            environment,
            plan=plan,
            inventory=inventory,
        )
        _record_predecessor_stop(environment, inventory=inventory)
        _remove_environment_authority_receipts(
            environment,
            service_ids=_inventory_service_ids(environment, inventory),
        )
    return {
        "schema": "okg.environment_host_service_fence/v1",
        "ok": True,
        "status": "fenced",
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "environment_id": environment.id,
        "descriptor_digest": environment.descriptor_digest,
        "plan_digest": plan.get("plan_digest"),
        "artifact_id": (plan.get("release_status") or {}).get("artifact_id"),
        "expected_host_command_count": len(rows) - len(database_rows),
        "results": results,
    }


def write_environment_service_supervision_evidence(
    environment: DeploymentEnvironment,
    plan: Mapping[str, Any],
    *,
    action: str,
    results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Persist promotion-grade evidence from a successful supervised start."""
    if action not in {"start", "restart"}:
        raise DeploymentConfigError(
            "supervision evidence requires a start or restart action"
        )
    _validate_service_plan(environment, plan)
    if not results or any(int(row.get("returncode", 1)) != 0 for row in results):
        raise DeploymentConfigError(
            "supervision evidence requires successful service results"
        )
    authority = next(
        (
            row.get("process_authority")
            for row in reversed(results)
            if isinstance(row.get("process_authority"), Mapping)
        ),
        None,
    )
    installation = environment_service_installation_status(environment, plan)
    release = environment_release_status(environment)
    handoff = environment_release_handoff_status(
        environment,
        expected_artifact_id=release.get("artifact_id"),
        expected_definition_digest=release.get("definition_digest"),
        expected_pointer_digest=release.get("active_pointer_digest"),
    )
    if (
        not isinstance(authority, Mapping)
        or authority.get("ok") is not True
        or authority.get("status") != "healthy"
        or installation.get("ok") is not True
        or release.get("ok") is not True
        or handoff.get("ok") is not True
        or not release.get("artifact_id")
    ):
        raise DeploymentConfigError(
            "supervision evidence requires healthy process, install, release, "
            "and completed handoff authority"
        )
    path = environment.runtime.state_root / "supervisor/promotion-supervision.json"
    payload = {
        "schema": "okg.environment_service_supervision_evidence/v1",
        "ok": True,
        "status": "completed",
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "environment_id": environment.id,
        "artifact_id": release["artifact_id"],
        "plan_digest": plan.get("plan_digest"),
        "action": action,
        "evidence_path": str(path),
        "process_authority": dict(authority),
        "service_installation": installation,
        "release_handoff": handoff,
        "results": [
            {
                "display": row.get("display"),
                "returncode": int(row.get("returncode", 1)),
            }
            for row in results
        ],
    }
    _atomic_write(
        path,
        (json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n").encode(
            "utf-8"
        ),
        mode=0o600,
    )
    return {**payload, "path": str(path)}


def _render_postgres_compose(
    environment: DeploymentEnvironment,
    *,
    postgres_image: str,
    password_path: Path,
) -> str:
    parsed = urlsplit(environment.database.dsn)
    database_user = unquote(parsed.username or "postgres")
    worker_settings = {
        "max_worker_processes": environment.database.max_worker_processes,
        "max_parallel_workers": environment.database.max_parallel_workers,
        "timescaledb.max_background_workers": (
            environment.database.timescaledb_max_background_workers
        ),
    }
    if any(value is None for value in worker_settings.values()):
        raise DeploymentConfigError(
            f"environment {environment.id!r} cannot render Postgres without "
            "declared worker-capacity settings"
        )
    postgres_command = ["postgres"]
    for setting, value in worker_settings.items():
        postgres_command.extend(("-c", f"{setting}={value}"))
    # Checkpoint and WAL sizing for graph ingest: the module defaults
    # (okg.substrate.db.wal_settings) overlaid by database.wal.
    postgres_command.extend(
        graph_wal_command_args(environment.database.wal_settings)
    )
    postgres_service = {
        "image": postgres_image,
        "container_name": environment.database.container,
        # Init as PID 1: an orphaned healthcheck pg_isready exiting 2 would
        # otherwise read as a crashed backend and reset Postgres (see
        # ops/pg/docker-compose.yaml).
        "init": True,
        "command": postgres_command,
        "environment": {
            "POSTGRES_DB": environment.database.name,
            "POSTGRES_USER": database_user,
            "POSTGRES_PASSWORD_FILE": "/run/secrets/postgres_password",
        },
        "ports": [f"127.0.0.1:{environment.database.host_port}:5432"],
        "volumes": ["database:/home/postgres/pgdata/data"],
        "healthcheck": {
            "test": [
                "CMD-SHELL",
                (f"pg_isready -U {database_user} -d {environment.database.name}"),
            ],
            "interval": "5s",
            "timeout": "3s",
            "retries": 40,
        },
        # PACT bound-host-storage-pressure / req.host.staging-governance: a
        # staging clone must never come back on a daemon restart alone (on
        # 2026-07-17 `restart: unless-stopped` let the staging Postgres
        # auto-resume and replay 58 -> 77.9 GB unattended, zeroing the host
        # twice). Staging replay is explicit opt-in per session, so the
        # staging container does NOT auto-restart; production/development keep
        # `unless-stopped`.
        "restart": "no" if environment.tier == "staging" else "unless-stopped",
        "secrets": ["postgres_password"],
    }
    if environment.database.shm_size is not None:
        postgres_service["shm_size"] = environment.database.shm_size
    if environment.database.memory_limit is not None:
        postgres_service["mem_limit"] = environment.database.memory_limit
    document = {
        "name": f"okg-{environment.id}",
        "services": {"postgres": postgres_service},
        "volumes": {"database": {"name": environment.database.volume}},
        "secrets": {"postgres_password": {"file": str(password_path)}},
    }
    return yaml.safe_dump(document, sort_keys=False)


def _render_mcp_registration(
    environment: DeploymentEnvironment,
    *,
    command: list[str],
) -> str:
    document = {
        "schema": "okg.environment_mcp_registration/v1",
        "environment_id": environment.id,
        "deployment": environment.graph_deployment_name,
        "transport": environment.mcp.transport,
        "mcpServers": {
            environment.mcp.registration_id: {
                "command": command[0],
                "args": command[1:],
            }
        },
    }
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


def _render_host_services(
    environment: DeploymentEnvironment,
    *,
    adapter: str,
    systemd_scope: str,
    worker_command: list[str],
    ui_command: list[str],
    mirror_command: list[str],
    supervisor_root: Path,
    environment_exec_path: Path,
) -> list[dict[str, Any]]:
    if adapter == "systemd":
        artifacts = [
            _systemd_artifact(
                environment,
                kind="worker_service",
                service_id=environment.runtime.worker_service,
                command=worker_command,
                output=environment.runtime.log_root / "worker.log",
                supervisor_root=supervisor_root,
                systemd_scope=systemd_scope,
            ),
        ]
        if environment.ui.enabled:
            artifacts.append(_systemd_artifact(
                environment,
                kind="ui_service",
                service_id=environment.ui.service,
                command=ui_command,
                output=environment.runtime.log_root / "ui.log",
                supervisor_root=supervisor_root,
                systemd_scope=systemd_scope,
            ))
        if environment.runtime.mirror_enabled and environment.runtime.mirror_service:
            artifacts.append(_systemd_artifact(
                environment,
                kind="mirror_service",
                service_id=environment.runtime.mirror_service,
                command=mirror_command,
                output=environment.runtime.log_root / "mirror.log",
                supervisor_root=supervisor_root,
                systemd_scope=systemd_scope,
            ))
        return artifacts
    artifacts = [
        _launchd_artifact(
            environment,
            kind="worker_service",
            service_id=environment.runtime.worker_service,
            command=worker_command,
            output=environment.runtime.log_root / "worker.log",
            supervisor_root=supervisor_root,
            environment_exec_path=environment_exec_path,
        ),
    ]
    if environment.ui.enabled:
        artifacts.append(_launchd_artifact(
            environment,
            kind="ui_service",
            service_id=environment.ui.service,
            command=ui_command,
            output=environment.runtime.log_root / "ui.log",
            supervisor_root=supervisor_root,
            environment_exec_path=environment_exec_path,
        ))
    if environment.runtime.mirror_enabled and environment.runtime.mirror_service:
        artifacts.append(_launchd_artifact(
            environment,
            kind="mirror_service",
            service_id=environment.runtime.mirror_service,
            command=mirror_command,
            output=environment.runtime.log_root / "mirror.log",
            supervisor_root=supervisor_root,
            environment_exec_path=environment_exec_path,
        ))
    return artifacts


def _systemd_artifact(
    environment: DeploymentEnvironment,
    *,
    kind: str,
    service_id: str,
    command: list[str],
    output: Path,
    supervisor_root: Path,
    systemd_scope: str,
) -> dict[str, Any]:
    unit = _systemd_unit(service_id)
    path = supervisor_root / unit
    content = "\n".join(
        (
            "[Unit]",
            f"Description=OKG environment service {service_id}",
            "After=network.target",
            "",
            "[Service]",
            "Type=simple",
            f"WorkingDirectory={environment.runtime.state_root}",
            f"EnvironmentFile={environment.runtime.local_env_file}",
            f'Environment="OKG_SERVICE_ID={service_id}"',
            f"ExecStart={shlex.join(command)}",
            "KillSignal=SIGTERM",
            "TimeoutStopSec=30",
            "SuccessExitStatus=75",
            # PACT bound-host-storage-pressure / req.host.staging-governance:
            # the staging worker (replay driver) does not auto-restart so a
            # crash never silently resumes an unbounded replay; it stays down
            # until an operator re-arms. Non-staging keeps on-failure.
            (
                "Restart=no"
                if (environment.tier == "staging" and kind == "worker_service")
                else "Restart=on-failure"
            ),
            "RestartSec=5",
            f"StandardOutput=append:{output}",
            f"StandardError=append:{output}",
            "",
            "[Install]",
            (
                "WantedBy=default.target"
                if systemd_scope == "user"
                else "WantedBy=multi-user.target"
            ),
            "",
        )
    )
    return {
        "kind": kind,
        "service_id": service_id,
        "unit": unit,
        "path": str(path),
        "mode": "0644",
        "content": content,
    }


def _launchd_artifact(
    environment: DeploymentEnvironment,
    *,
    kind: str,
    service_id: str,
    command: list[str],
    output: Path,
    supervisor_root: Path,
    environment_exec_path: Path,
) -> dict[str, Any]:
    path = supervisor_root / f"{service_id}.plist"
    document = {
        "Label": service_id,
        "ProgramArguments": [
            command[0],
            str(environment_exec_path),
            "--env-file",
            str(environment.runtime.local_env_file),
            "--supervision-receipt",
            str(_launchd_supervision_receipt_path(environment, service_id)),
            "--",
            *command,
        ],
        "EnvironmentVariables": {"OKG_SERVICE_ID": service_id},
        "WorkingDirectory": str(environment.runtime.state_root),
        # PACT bound-host-storage-pressure / req.host.staging-governance: the
        # staging WORKER (the replay driver) must not auto-restart -- staging
        # replay is explicit opt-in per session, so a crashed staging worker
        # stays down until an operator re-arms and restarts it, rather than
        # relaunching and resuming an unbounded replay. Non-staging tiers and
        # the ui/mirror services keep KeepAlive.
        "KeepAlive": not (
            environment.tier == "staging" and kind == "worker_service"
        ),
        "RunAtLoad": False,
        "ThrottleInterval": 10,
        "ExitTimeOut": 30,
        "StandardOutPath": str(output),
        "StandardErrorPath": str(output),
    }
    content = plistlib.dumps(document, sort_keys=True).decode("utf-8")
    return {
        "kind": kind,
        "service_id": service_id,
        "unit": service_id,
        "path": str(path),
        "mode": "0644",
        "content": content,
    }


def _lifecycle_commands(
    environment: DeploymentEnvironment,
    *,
    adapter: str,
    systemd_scope: str,
    compose_path: Path,
    service_artifacts: list[dict[str, Any]],
    handoff_service_ids: tuple[str, ...],
) -> dict[str, list[dict[str, Any]]]:
    compose = [
        "docker",
        "compose",
        "--project-name",
        f"okg-{environment.id}",
        "--file",
        str(compose_path),
    ]
    database_up = _command([*compose, "up", "--detach", "postgres"])
    database_stop = _command([*compose, "stop", "postgres"])
    database_status = _command([*compose, "ps", "postgres"])
    database_logs = _command([*compose, "logs", "postgres"])
    if adapter == "systemd":
        systemctl = ["systemctl"]
        journalctl = ["journalctl"]
        if systemd_scope == "user":
            systemctl.append("--user")
            journalctl.append("--user")
        units = [str(item["unit"]) for item in service_artifacts]
        stop_units = [_systemd_unit(service_id) for service_id in handoff_service_ids]
        obsolete_units = [unit for unit in stop_units if unit not in units]
        paths = [str(item["path"]) for item in service_artifacts]
        restart_stop = _command(
            [*systemctl, "stop", *stop_units],
            handoff_phase="stop",
            handoff_boundary=not obsolete_units,
            service_ids=list(handoff_service_ids),
        )
        obsolete_disable = (
            [
                _command(
                    [*systemctl, "disable", *obsolete_units],
                    handoff_phase="disable_obsolete",
                    handoff_boundary=True,
                    service_ids=[
                        service_id
                        for service_id in handoff_service_ids
                        if _systemd_unit(service_id) in obsolete_units
                    ],
                )
            ]
            if obsolete_units
            else []
        )
        return {
            "install": [
                *[_command([*systemctl, "link", path]) for path in paths],
                _command([*systemctl, "daemon-reload"]),
            ],
            "start": [
                database_up,
                _command(
                    [*systemctl, "enable", "--now", *units],
                    handoff_phase="start",
                ),
            ],
            "stop": [
                _command(
                    [*systemctl, "disable", "--now", *reversed(stop_units)],
                    handoff_phase="stop",
                    handoff_boundary=True,
                    service_ids=list(reversed(handoff_service_ids)),
                ),
                database_stop,
            ],
            "restart": [
                database_up,
                restart_stop,
                *obsolete_disable,
                _command(
                    [*systemctl, "enable", "--now", *units],
                    handoff_phase="start",
                ),
            ],
            "status": [database_status, _command([*systemctl, "status", *units])],
            "logs": [
                database_logs,
                _command(
                    [
                        *journalctl,
                        *[part for unit in units for part in ("--unit", unit)],
                    ]
                ),
            ],
        }

    domain = f"gui/{os.getuid()}"
    service_ids = [str(item["service_id"]) for item in service_artifacts]
    stop_service_ids = list(handoff_service_ids)
    paths = [str(item["path"]) for item in service_artifacts]
    return {
        "install": [],
        "start": [
            database_up,
            *[
                _command(
                    ["launchctl", "bootstrap", domain, path],
                    handoff_phase="start",
                )
                for path in paths
            ],
        ],
        "stop": [
            *[
                _command(
                    ["launchctl", "bootout", f"{domain}/{service_id}"],
                    handoff_phase="stop",
                    handoff_boundary=index == len(stop_service_ids) - 1,
                    service_id=service_id,
                )
                for index, service_id in enumerate(reversed(stop_service_ids))
            ],
            database_stop,
        ],
        "restart": [
            database_up,
            *[
                _command(
                    ["launchctl", "bootout", f"{domain}/{service_id}"],
                    handoff_phase="stop",
                    handoff_boundary=index == len(stop_service_ids) - 1,
                    service_id=service_id,
                )
                for index, service_id in enumerate(reversed(stop_service_ids))
            ],
            *[
                _command(
                    ["launchctl", "bootstrap", domain, path],
                    handoff_phase="start",
                )
                for path in paths
            ],
        ],
        "status": [
            database_status,
            *[
                _command(["launchctl", "print", f"{domain}/{service_id}"])
                for service_id in service_ids
            ],
        ],
        "logs": [
            database_logs,
            _command(
                [
                    "tail",
                    "-n",
                    "200",
                    str(environment.runtime.log_root / "worker.log"),
                    str(environment.runtime.log_root / "ui.log"),
                ]
            ),
        ],
    }


def _service_topology(environment: DeploymentEnvironment) -> list[dict[str, Any]]:
    topology = [
        {
            "kind": "database",
            "service_id": environment.database.container,
            "owner": environment.id,
            "database": environment.database.name,
            "volume": environment.database.volume,
            "endpoint": f"127.0.0.1:{environment.database.host_port}",
        },
        {
            "kind": "worker",
            "service_id": environment.runtime.worker_service,
            "owner": environment.id,
            "dbos_identity": environment.database.dbos_identity,
            "state_root": str(environment.runtime.state_root),
            "data_root": str(environment.runtime.data_root),
            "log_root": str(environment.runtime.log_root),
        },
        {
            "kind": "mcp_registration",
            "service_id": environment.mcp.registration_id,
            "owner": environment.id,
            "transport": environment.mcp.transport,
        },
    ]
    if environment.ui.enabled:
        topology.append({
            "kind": "ui",
            "service_id": environment.ui.service,
            "owner": environment.id,
            "endpoint": f"{environment.ui.host}:{environment.ui.port}",
        })
    if environment.webhook.enabled:
        topology.append({
            "kind": "embedded_webhook",
            "service_id": environment.webhook.service,
            "owner": environment.id,
            "owner_service": environment.runtime.worker_service,
            "endpoint": f"{environment.webhook.host}:{environment.webhook.port}",
        })
    if environment.runtime.mirror_enabled and environment.runtime.mirror_service:
        topology.append({
            "kind": "mirror",
            "service_id": environment.runtime.mirror_service,
            "owner": environment.id,
            "cache_root": str(environment.runtime.data_root / "ci-results"),
        })
    return topology


def environment_service_installation_status(
    environment: DeploymentEnvironment,
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    marker = environment.runtime.state_root / "supervisor/install.json"
    if not marker.exists():
        return {
            "schema": "okg.environment_service_installation_status/v1",
            "ok": False,
            "status": "missing",
            "path": str(marker),
        }
    try:
        observed_digest = _service_plan_digest(plan)
        if plan.get("plan_digest") != observed_digest:
            raise DeploymentConfigError("service plan content digest is invalid")
        if marker.is_symlink() or not marker.is_file():
            raise DeploymentConfigError("service install marker is not regular")
        if stat.S_IMODE(marker.stat().st_mode) != 0o600:
            raise DeploymentConfigError("service install marker mode is not 0600")
        raw = json.loads(marker.read_text(encoding="utf-8"))
        if (
            not isinstance(raw, dict)
            or raw.get("schema") != "okg.environment_service_install/v1"
        ):
            raise DeploymentConfigError("service install marker has the wrong schema")
        if raw.get("environment_id") != environment.id:
            raise DeploymentConfigError("service install marker environment mismatch")
        release = plan.get("release_status") or {}
        if raw.get("artifact_id") != release.get("artifact_id"):
            raise DeploymentConfigError("installed service artifact is stale")
        if raw.get("plan_digest") != plan.get("plan_digest"):
            raise DeploymentConfigError("installed service plan digest is stale")
        if raw.get("adapter") != plan.get("adapter"):
            raise DeploymentConfigError("installed service adapter is stale")
        expected_scope = (
            plan.get("systemd_scope") if plan.get("adapter") == "systemd" else None
        )
        if raw.get("systemd_scope") != expected_scope:
            raise DeploymentConfigError("installed systemd scope is stale")
        if raw.get("launcher_python") != (plan.get("launcher") or {}).get("python"):
            raise DeploymentConfigError("installed launcher Python is stale")
        if raw.get("postgres_image") != plan.get("postgres_image"):
            raise DeploymentConfigError("installed Postgres image is stale")
        previous_installation = raw.get("previous_installation")
        expected_previous_topology = (
            ((plan.get("handoff_topology") or {}).get("generations") or {}).get(
                "previous"
            )
        )
        if release.get("previous_artifact_id") and not isinstance(
            previous_installation, Mapping
        ):
            raise DeploymentConfigError(
                "previous service installation binding is missing"
            )
        if isinstance(previous_installation, Mapping) and (
            previous_installation.get("artifact_id")
            != release.get("previous_artifact_id")
            or not _is_sha256(previous_installation.get("plan_digest"))
            or previous_installation.get("adapter") != plan.get("adapter")
            or previous_installation.get("systemd_scope")
            != plan.get("systemd_scope")
            or previous_installation.get("service_topology")
            != expected_previous_topology
        ):
            raise DeploymentConfigError(
                "previous service installation binding is invalid"
            )
        for artifact in plan.get("artifacts") or ():
            path = Path(str(artifact.get("path") or ""))
            expected_mode = int(str(artifact.get("mode") or "0"), 8)
            if path.is_symlink() or not path.is_file():
                raise DeploymentConfigError(
                    f"installed service artifact is missing: {path}"
                )
            if stat.S_IMODE(path.stat().st_mode) != expected_mode:
                raise DeploymentConfigError(
                    f"installed service artifact mode changed: {path}"
                )
            kind = artifact.get("kind")
            declared_owner = artifact.get("owner")
            if kind == "postgres_password_file":
                expected_owner = {
                    "uid": environment.database.container_uid,
                    "gid": environment.database.container_gid,
                }
                if declared_owner != expected_owner:
                    raise DeploymentConfigError(
                        "postgres password artifact owner does not match the "
                        "environment container identity"
                    )
                observed = path.stat()
                if _host_owner_reaches_container() and (
                    observed.st_uid != expected_owner["uid"]
                    or observed.st_gid != expected_owner["gid"]
                ):
                    # Same rule as the install-time chown: where host
                    # ownership does not reach the container, the installer
                    # never applies it, so asserting it here would refuse
                    # every freshness check on a correctly-installed
                    # environment — and `restart` requires this check to
                    # pass, which is what left a dead deployment stuck.
                    raise DeploymentConfigError(
                        f"installed service artifact owner changed: {path}"
                    )
            elif declared_owner is not None:
                raise DeploymentConfigError(
                    f"service artifact {kind!r} declares an unexpected owner"
                )
            if kind == "launcher":
                expected = _launcher_source().read_bytes()
            elif kind == "environment_exec":
                expected = _environment_exec_source().read_bytes()
            elif kind == "environment_file":
                expected = _environment_file_content(environment).encode("utf-8")
            elif kind == "postgres_password_file":
                expected = (_database_password(environment) + "\n").encode("utf-8")
            else:
                expected = str(artifact.get("content") or "").encode("utf-8")
            if path.read_bytes() != expected:
                raise DeploymentConfigError(
                    f"installed service artifact content changed: {path}"
                )
        return {
            "schema": "okg.environment_service_installation_status/v1",
            "ok": True,
            "status": "current",
            "path": str(marker),
            "plan_digest": raw["plan_digest"],
            "artifact_id": raw["artifact_id"],
            "previous_installation": previous_installation,
        }
    except Exception as exc:  # noqa: BLE001 - status preserves the failure.
        return {
            "schema": "okg.environment_service_installation_status/v1",
            "ok": False,
            "status": "invalid",
            "path": str(marker),
            "error": type(exc).__name__,
            "message": str(exc),
        }


def installed_environment_service_status(
    environment: DeploymentEnvironment,
    *,
    environ: Mapping[str, str] | None = None,
    _release_status: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Reconstruct and verify the exact installed, secret-free service plan."""
    marker = environment.runtime.state_root / "supervisor/install.json"
    try:
        if (
            marker.is_symlink()
            or not marker.is_file()
            or stat.S_IMODE(marker.stat().st_mode) != 0o600
        ):
            raise DeploymentConfigError(
                "installed service readback requires a 0600 regular marker"
            )
        raw = json.loads(marker.read_text(encoding="utf-8"))
        if (
            not isinstance(raw, Mapping)
            or raw.get("schema") != "okg.environment_service_install/v1"
            or raw.get("environment_id") != environment.id
        ):
            raise DeploymentConfigError("installed service marker identity is invalid")
        launcher_python = raw.get("launcher_python")
        postgres_image = raw.get("postgres_image")
        adapter = raw.get("adapter")
        systemd_scope = raw.get("systemd_scope")
        if not isinstance(launcher_python, str) or not Path(launcher_python).is_absolute():
            raise DeploymentConfigError("installed launcher Python is unavailable")
        if not isinstance(postgres_image, str) or not postgres_image:
            raise DeploymentConfigError("installed Postgres image is unavailable")
        plan = render_environment_services(
            environment,
            adapter=str(adapter or ""),
            launcher_python=launcher_python,
            systemd_scope=str(systemd_scope or "system"),
            postgres_image=postgres_image,
            environ=environ,
            _release_status=_release_status,
        )
        status = environment_service_installation_status(environment, plan)
        return {**status, "plan": plan}
    except Exception as exc:  # noqa: BLE001 - readback is fail-closed status.
        return {
            "schema": "okg.environment_service_installation_status/v1",
            "ok": False,
            "status": "invalid" if marker.exists() else "missing",
            "path": str(marker),
            "error": type(exc).__name__,
            "message": str(exc),
            "plan": None,
        }


def _previous_installation_binding(
    environment: DeploymentEnvironment,
    plan: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Carry forward independently verified predecessor manager authority."""
    release = plan.get("release_status") or {}
    previous_artifact_id = release.get("previous_artifact_id")
    if not _is_sha256(previous_artifact_id):
        return None
    marker_path = environment.runtime.state_root / "supervisor/install.json"
    try:
        if (
            marker_path.is_symlink()
            or not marker_path.is_file()
            or stat.S_IMODE(marker_path.stat().st_mode) != 0o600
        ):
            return None
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    previous_topology = (
        ((plan.get("handoff_topology") or {}).get("generations") or {}).get(
            "previous"
        )
    )
    if (
        not isinstance(marker, Mapping)
        or marker.get("schema") != "okg.environment_service_install/v1"
        or marker.get("environment_id") != environment.id
        or not isinstance(previous_topology, Mapping)
    ):
        return None
    try:
        binding: Mapping[str, Any] | None = _previous_manager_binding(
            environment,
            current_release=release,
        )
    except DeploymentConfigError:
        return None
    if (
        binding is None
        or not _is_sha256(binding.get("plan_digest"))
        or binding.get("adapter") not in {None, plan.get("adapter")}
        or (
            binding.get("adapter") == "systemd"
            and binding.get("systemd_scope") != plan.get("systemd_scope")
        )
    ):
        return None
    installed_topology = binding.get("service_topology")
    if not isinstance(installed_topology, Mapping):
        pointer = load_active_release(environment.release.previous_pointer)
        if pointer is None:
            return None
        installed_topology = _installed_generation_from_artifacts(
            environment,
            pointer=pointer,
            adapter=str(binding.get("adapter") or plan.get("adapter") or ""),
        )
    if dict(installed_topology) != dict(previous_topology):
        raise DeploymentConfigError(
            "installed predecessor topology differs from the verified handoff plan"
        )
    adapter = binding.get("adapter") or plan.get("adapter")
    carried = {
        "artifact_id": str(previous_artifact_id),
        "plan_digest": str(binding["plan_digest"]),
        "adapter": adapter,
        "systemd_scope": (
            binding.get("systemd_scope") or plan.get("systemd_scope")
            if adapter == "systemd"
            else None
        ),
        "launcher_python": binding.get("launcher_python")
        or (plan.get("launcher") or {}).get("python"),
        "postgres_image": binding.get("postgres_image") or plan.get("postgres_image"),
        "service_topology": dict(previous_topology),
    }
    if adapter == "launchd":
        services = previous_topology.get("services")
        if not isinstance(services, Mapping):
            raise DeploymentConfigError(
                "installed predecessor launchd topology is invalid"
            )
        process_models = binding.get("launchd_process_models")
        if not isinstance(process_models, Mapping):
            process_models = _installed_launchd_process_models(
                environment,
                service_ids=tuple(str(item) for item in services),
            )
        if set(process_models) != set(services) or any(
            model not in {"direct_exec", "supervised_wrapper"}
            for model in process_models.values()
        ):
            raise DeploymentConfigError(
                "installed predecessor launchd process model is invalid"
            )
        carried["launchd_process_models"] = dict(process_models)
    return carried


def environment_service_authority_status(
    environment: DeploymentEnvironment,
    *,
    service_id: str,
    environ: Mapping[str, str] | None = None,
    release_status: Mapping[str, Any] | None = None,
    plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate a launcher-written receipt for one supervised service."""
    path = environment.runtime.state_root / "service-authority" / f"{service_id}.json"
    observed_release = (
        dict(release_status)
        if release_status is not None
        else environment_release_status(environment, environ=environ)
    )
    if not path.exists():
        return {
            "schema": "okg.environment_service_authority_status/v1",
            "ok": False,
            "status": "missing",
            "environment_id": environment.id,
            "service_id": service_id,
            "path": str(path),
        }
    reason_code: str | None = None
    try:
        if path.is_symlink() or not path.is_file():
            raise DeploymentConfigError("service authority receipt is not regular")
        if stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise DeploymentConfigError("service authority receipt mode is not 0600")
        raw = json.loads(path.read_text(encoding="utf-8"))
        if (
            not isinstance(raw, dict)
            or raw.get("schema") != "okg.service_release_authority/v1"
        ):
            raise DeploymentConfigError(
                "service authority receipt has the wrong schema"
            )
        expected = {
            "service_id": service_id,
            "environment_id": environment.id,
            "artifact_id": observed_release.get("artifact_id"),
            "artifact_root": observed_release.get("artifact_root"),
            "definition_digest": observed_release.get("definition_digest"),
            "active_pointer": str(environment.release.active_pointer),
        }
        if not observed_release.get("ok"):
            raise DeploymentConfigError("active release status is not valid")
        if not _is_sha256(observed_release.get("definition_digest")):
            raise DeploymentConfigError(
                "service authority release definition digest is missing"
            )
        for field, wanted in expected.items():
            if (
                field == "definition_digest"
                and "definition_digest" not in raw
                and _legacy_service_receipt_definition_is_bound(
                    environment,
                    raw=raw,
                    observed_release=observed_release,
                )
            ):
                continue
            if raw.get(field) != wanted:
                raise DeploymentConfigError(
                    f"service authority mismatch for {field}: "
                    f"expected {wanted!r}, observed {raw.get(field)!r}"
                )
        artifact_root = Path(str(raw["artifact_root"])).resolve()
        for field in ("interpreter", "repo_root"):
            _require_within(
                Path(str(raw.get(field) or "")).resolve(),
                artifact_root,
                field,
            )
        if not raw.get("isolated"):
            raise DeploymentConfigError("service authority is not isolated")
        pid = raw.get("pid")
        if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
            raise DeploymentConfigError("service authority pid is invalid")
        receipt_start = raw.get("process_start_identity")
        if not isinstance(receipt_start, str) or not receipt_start:
            raise DeploymentConfigError(
                "service authority process start identity is invalid"
            )
        arguments = raw.get("arguments")
        if not isinstance(arguments, list) or not all(
            isinstance(item, str) for item in arguments
        ):
            raise DeploymentConfigError("service authority arguments are invalid")
        expected_arguments = _expected_service_arguments(environment, service_id)
        if arguments != list(expected_arguments):
            raise DeploymentConfigError(
                "service authority arguments do not match the configured service role"
            )
        if not pid_alive(pid):
            reason_code = "service_process_not_alive"
            raise DeploymentConfigError("service authority process is not alive")
        if process_start_identity(pid) != receipt_start:
            raise DeploymentConfigError("service authority process identity changed")
        supervisor = _supervisor_process_status(
            environment,
            service_id=service_id,
            plan=plan,
        )
        if not supervisor.get("ok") or supervisor.get("status") != "running":
            raise DeploymentConfigError(
                "service manager did not attest a running main process"
            )
        if not _service_supervisor_matches_authority(
            environment,
            service_id=service_id,
            supervisor=supervisor,
            authority=raw,
        ):
            raise DeploymentConfigError(
                "service authority pid differs from the service manager main pid"
            )
        runtime_identity = process_runtime_identity(pid)
        if runtime_identity is None:
            raise DeploymentConfigError(
                "service authority process runtime identity is unavailable"
            )
        if (
            Path(runtime_identity["executable"]).resolve()
            != Path(str(raw["interpreter"])).resolve()
        ):
            raise DeploymentConfigError(
                "service authority process executable differs from sealed runtime"
            )
        try:
            launched_arguments = receipt_launched_arguments(
                raw,
                arguments,
                state_root=environment.runtime.state_root,
            )
        except ValueError as exc:
            raise DeploymentConfigError(
                f"service authority bytecode isolation is invalid: {exc}"
            ) from exc
        if runtime_identity["argv"][1:] != launched_arguments:
            raise DeploymentConfigError(
                "service authority process arguments differ from sealed runtime"
            )
        if process_start_identity(pid) != receipt_start:
            raise DeploymentConfigError(
                "service authority process identity changed during readback"
            )
        final_supervisor = _supervisor_process_status(
            environment,
            service_id=service_id,
            plan=plan,
        )
        if (
            final_supervisor.get("ok") is not True
            or final_supervisor.get("status") != "running"
            or not _service_supervisor_matches_authority(
                environment,
                service_id=service_id,
                supervisor=final_supervisor,
                authority=raw,
            )
        ):
            raise DeploymentConfigError(
                "service manager main pid changed during authority readback"
            )
        return {
            "schema": "okg.environment_service_authority_status/v1",
            "ok": True,
            "status": "healthy",
            "environment_id": environment.id,
            "service_id": service_id,
            "path": str(path),
            "authority": raw,
            "supervisor": final_supervisor,
        }
    except Exception as exc:  # noqa: BLE001 - status preserves the failure.
        status = {
            "schema": "okg.environment_service_authority_status/v1",
            "ok": False,
            "status": "invalid",
            "environment_id": environment.id,
            "service_id": service_id,
            "path": str(path),
            "error": type(exc).__name__,
            "message": str(exc),
        }
        if reason_code is not None:
            status["reason_code"] = reason_code
        return status


def _wait_for_service_authority(
    environment: DeploymentEnvironment,
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    while True:
        release_status = environment_release_status(environment)
        authority = environment_process_authority_status(
            environment,
            release_status=release_status,
        )
        status = {
            **authority,
            "status": "healthy" if authority.get("ok") else "starting",
        }
        if status["ok"] or time.monotonic() >= deadline:
            if not status["ok"]:
                status["status"] = "invalid"
                status["message"] = "supervised process authority did not converge"
            return status
        time.sleep(0.1)


def _environment_process_authority_status(
    environment: DeploymentEnvironment,
    *,
    release_status: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return environment_process_authority_status(
        environment,
        release_status=release_status,
    )


def _require_environment_service_install_preflight(
    environment: DeploymentEnvironment,
    plan: Mapping[str, Any],
) -> None:
    """Reject independently unsafe first installs before state-local locking."""
    process_authority = _environment_process_authority_status(environment)
    if process_authority.get("ok"):
        return
    planned_release = plan.get("release_status") or {}
    if process_authority.get("status") == "stopped":
        _require_receiptless_managers_stopped(environment, plan=plan)
        return
    if not planned_release.get("previous_artifact_id"):
        raise DeploymentConfigError(
            "environment service install refuses partial or invalid process "
            "authority"
        )


def _require_environment_service_install_authority(
    environment: DeploymentEnvironment,
    plan: Mapping[str, Any],
) -> None:
    process_authority = _environment_process_authority_status(environment)
    planned_release = plan.get("release_status") or {}
    if process_authority.get("ok"):
        return
    if process_authority.get("status") == "stopped":
        _require_receiptless_managers_stopped(environment, plan=plan)
        if not planned_release.get("previous_artifact_id"):
            return
    elif not planned_release.get("previous_artifact_id"):
        raise DeploymentConfigError(
            "environment service install refuses partial or invalid process "
            "authority"
        )
    current_release = environment_release_status(environment)
    planned_artifact = planned_release.get("artifact_id")
    if (
        not current_release.get("ok")
        or current_release.get("artifact_id") != planned_artifact
    ):
        raise DeploymentConfigError(
            "active release changed before service install"
        )
    _require_verified_previous_process_authority(
        environment,
        plan=plan,
        current_release=current_release,
    )


def _require_verified_previous_process_authority(
    environment: DeploymentEnvironment,
    *,
    plan: Mapping[str, Any],
    current_release: Mapping[str, Any],
) -> None:
    planned_release = plan.get("release_status") or {}
    predecessor_fields = (
        "previous_artifact_id",
        "previous_definition_digest",
        "previous_pointer_digest",
    )
    if any(
        current_release.get(field) != planned_release.get(field)
        for field in predecessor_fields
    ):
        raise DeploymentConfigError(
            "previous release changed before service handoff"
        )
    _restart_process_inventory(
        environment,
        plan=plan,
        current_release=current_release,
    )


def _require_receiptless_managers_stopped(
    environment: DeploymentEnvironment,
    *,
    plan: Mapping[str, Any],
) -> None:
    """Refuse a missing-receipt mutation unless every planned role is down."""
    topology = plan.get("handoff_topology")
    service_ids = (
        topology.get("stop_services") if isinstance(topology, Mapping) else None
    )
    if not isinstance(service_ids, list) or not service_ids or not all(
        isinstance(service_id, str) and service_id for service_id in service_ids
    ):
        raise DeploymentConfigError(
            "receipt-less service mutation has no exact manager inventory"
        )
    for service_id in service_ids:
        supervisor = _supervisor_process_status(
            environment,
            service_id=service_id,
            plan=plan,
        )
        if not _receiptless_supervisor_is_safe(
            environment,
            plan=plan,
            service_id=service_id,
            status=supervisor,
        ):
            raise DeploymentConfigError(
                "receipt-less service mutation found a running, transitional, "
                f"or unknown manager process for role {service_id!r}"
            )


def _receiptless_supervisor_is_safe(
    environment: DeploymentEnvironment,
    *,
    plan: Mapping[str, Any],
    service_id: str,
    status: Any,
) -> bool:
    if (
        not isinstance(status, Mapping)
        or status.get("ok") is not True
        or status.get("status") != "stopped"
        or status.get("pid") is not None
    ):
        return False
    outcome = status.get("manager_outcome")
    if not isinstance(outcome, Mapping):
        return False
    if outcome.get("adapter") == "launchd":
        if outcome.get("exit_status_observed") is not False:
            return False
        receipt_path = _launchd_supervision_receipt_path(environment, service_id)
        if not receipt_path.exists():
            # First install (and the exact legacy direct-exec model) has no
            # wrapper outcome.  No loaded job means there is no process to
            # mutate behind the absent authority receipts.
            return True
        if not _attested_stop_retry_available(environment, plan=plan):
            return False
        try:
            raw = _read_authority_receipt(
                receipt_path,
                schema="okg.launchd_service_supervision/v1",
            )
            terminal = _read_launchd_supervision_receipt(
                environment,
                service_id=service_id,
                authority={
                    "pid": raw.get("child_pid"),
                    "process_start_identity": raw.get(
                        "child_process_start_identity"
                    ),
                    "lifecycle_nonce": raw.get("lifecycle_nonce"),
                },
                expected_state="exited",
            )
        except DeploymentConfigError:
            return False
        worker_role = service_id in _handoff_worker_service_ids(plan)
        return bool(
            terminal.get("exit_code") == 0
            or (
                not worker_role
                and terminal.get("exit_code") is None
                and terminal.get("term_signal") == 15
            )
        )
    return bool(
        outcome.get("adapter") == "systemd"
        and outcome.get("exit_status_observed") is True
        and outcome.get("result") == "success"
        and (
            outcome.get("load_state") == "not-found"
            or (
                outcome.get("exec_main_code"),
                outcome.get("exec_main_status"),
            )
            in {(0, 0), (1, 0), (2, 15)}
        )
    )


def _restart_process_inventory(
    environment: DeploymentEnvironment,
    *,
    plan: Mapping[str, Any],
    current_release: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate every managed role as current, predecessor, or stopped."""
    contexts: list[tuple[str, DeploymentEnvironment, Mapping[str, Any]]] = [
        ("current", environment, current_release)
    ]
    authority_contexts = contexts
    if current_release.get("previous_artifact_id"):
        previous_release, previous_environment = _previous_release_environment(
            environment,
            current_release=current_release,
        )
        contexts.append(("previous", previous_environment, previous_release))
        # A predecessor unit may auto-relaunch through the shared launcher in
        # the activation-to-restart window. Admit that exact cross-generation
        # shape only into the stop inventory: current release identity plus the
        # independently reconstructed predecessor role and argv.
        authority_contexts = [
            *contexts,
            ("straddled", previous_environment, current_release),
        ]
    service_ids = _service_plan_handoff_ids(
        environment,
        plan=plan,
        contexts=contexts,
    )
    completed_stop = _predecessor_stop_status(
        environment,
        release_status=current_release,
        expected_services=service_ids,
        expected_plan_digest=plan.get("plan_digest"),
        plan=plan,
        require_obsolete_receipts_absent=False,
        allow_completed_start=True,
    )
    if completed_stop.get("ok") is not True:
        rebound = _rebind_predecessor_stop_after_activation(
            environment,
            plan=plan,
            current_release=current_release,
            service_ids=service_ids,
        )
        if rebound is not None:
            completed_stop = rebound
    if (
        completed_stop.get("ok") is True
        and completed_stop.get("status") == "stopped"
        and _all_handoff_services_stopped(
            environment,
            plan=plan,
            service_ids=service_ids,
        )
    ):
        return _restart_inventory_from_stop_record(
            environment,
            plan=plan,
            release_status=current_release,
            stop_status=completed_stop,
            service_ids=service_ids,
        )

    completed_record = completed_stop.get("record")
    services: dict[str, dict[str, Any]] = {}
    for service_id in service_ids:
        candidates = [
            (
                generation,
                environment_service_authority_status(
                    candidate_environment,
                    service_id=service_id,
                    release_status=release,
                    plan=plan,
                ),
            )
            for generation, candidate_environment, release in authority_contexts
            if service_id in _supervised_service_ids(candidate_environment)
        ]
        selected = next(
            (
                (generation, row)
                for generation, row in candidates
                if row.get("ok") is True
                or _verified_dead_authority_row(
                    row,
                    schema="okg.environment_service_authority_status/v1",
                    environment_id=environment.id,
                    error="DeploymentConfigError",
                    reason_code="service_process_not_alive",
                )
            ),
            None,
        )
        if selected is None and all(
            row.get("status") == "missing" for _generation, row in candidates
        ):
            predecessor_owned = any(
                generation == "previous"
                and service_id in _supervised_service_ids(candidate_environment)
                for generation, candidate_environment, _release in contexts
            )
            supervisor = _supervisor_process_status(
                environment,
                service_id=service_id,
                plan=plan,
            )
            if (
                not predecessor_owned
                and _supervisor_has_no_process(supervisor)
            ):
                services[service_id] = {
                    "generation": "absent",
                    "state": "stopped",
                    "identity": None,
                    "supervisor": supervisor,
                }
                continue
            recorded_roles = (
                completed_record.get("observed_roles")
                if isinstance(completed_record, Mapping)
                else None
            )
            recorded_role = (
                recorded_roles.get(service_id)
                if isinstance(recorded_roles, Mapping)
                else None
            )
            if (
                predecessor_owned
                and completed_stop.get("ok") is True
                and isinstance(recorded_role, Mapping)
                and _supervisor_has_no_process(supervisor)
            ):
                services[service_id] = {
                    "generation": recorded_role.get("generation"),
                    "state": "stopped",
                    "identity": recorded_role.get("identity"),
                    "supervisor": supervisor,
                    "recorded_stop": True,
                }
                continue
        if selected is None:
            raise DeploymentConfigError(
                "environment service handoff has an unknown or foreign process "
                f"for role {service_id!r}"
            )
        generation, row = selected
        supervisor = row.get("supervisor")
        if row.get("ok") is not True:
            supervisor = _supervisor_process_status(
                environment,
                service_id=service_id,
                plan=plan,
            )
            if not _supervisor_has_no_process(supervisor):
                raise DeploymentConfigError(
                    "environment service handoff found a service-manager process "
                    f"without live authority for role {service_id!r}"
                )
        raw = row.get("authority")
        if not isinstance(raw, Mapping):
            raw = _read_authority_receipt(
                Path(str(row.get("path") or "")),
                schema="okg.service_release_authority/v1",
            )
        services[service_id] = {
            "generation": generation,
            "state": "running" if row.get("ok") is True else "stopped",
            "identity": _process_identity(raw),
            "authority": dict(raw),
            "supervisor": supervisor,
        }

    worker_services: list[
        tuple[str, DeploymentEnvironment, Mapping[str, Any], Mapping[str, Any]]
    ] = []
    for generation, candidate_environment, release in authority_contexts:
        worker_service_id = candidate_environment.runtime.worker_service
        service = services[worker_service_id]
        if (
            service.get("generation") == generation
            and service.get("recorded_stop") is not True
        ):
            worker_services.append(
                (generation, candidate_environment, release, service)
            )
    worker: dict[str, Any]
    if not worker_services:
        worker_rows = [
            environment_worker_authority_status(
                candidate_environment,
                release_status=release,
                expected_arguments=_expected_service_arguments(
                    candidate_environment,
                    candidate_environment.runtime.worker_service,
                ),
            )
            for _generation, candidate_environment, release in authority_contexts
        ]
        if not all(row.get("status") == "missing" for row in worker_rows):
            raise DeploymentConfigError(
                "environment worker authority exists without a supervised worker"
            )
        recorded_worker = (
            completed_record.get("observed_worker")
            if isinstance(completed_record, Mapping)
            else None
        )
        recorded_worker_service = (
            recorded_worker.get("service_id")
            if isinstance(recorded_worker, Mapping)
            else None
        )
        recorded_worker_role = services.get(str(recorded_worker_service or ""))
        if (
            completed_stop.get("ok") is True
            and isinstance(recorded_worker, Mapping)
            and isinstance(recorded_worker_role, Mapping)
            and recorded_worker_role.get("recorded_stop") is True
        ):
            worker = {
                "generation": recorded_worker.get("generation"),
                "state": "stopped",
                "identity": recorded_worker.get("identity"),
                "recorded_stop": True,
            }
        else:
            worker = {
                "generation": "absent",
                "state": "stopped",
                "identity": None,
            }
    else:
        if len(worker_services) != 1:
            raise DeploymentConfigError(
                "environment handoff found multiple supervised worker processes"
            )
        generation, worker_environment, selected_release, worker_service = (
            worker_services[0]
        )
        expected_pid = int(worker_service["identity"]["pid"])
        worker_row = environment_worker_authority_status(
            worker_environment,
            release_status=selected_release,
            expected_arguments=_expected_service_arguments(
                worker_environment,
                worker_environment.runtime.worker_service,
            ),
            expected_pid=expected_pid,
        )
        worker_dead = _verified_dead_authority_row(
            worker_row,
            schema="okg.environment_worker_authority_status/v1",
            environment_id=environment.id,
            error="SealedReleaseInvalid",
            reason_code="worker_process_not_alive",
        )
        worker_readiness_missing_after_dead_service = bool(
            worker_row.get("status") == "missing"
            and worker_service.get("state") == "stopped"
            and isinstance(worker_service.get("authority"), Mapping)
            and _supervisor_has_no_process(worker_service.get("supervisor"))
        )
        if (
            worker_row.get("ok") is not True
            and not worker_dead
            and not worker_readiness_missing_after_dead_service
        ):
            raise DeploymentConfigError(
                "environment worker authority does not match the supervised worker"
            )
        raw = (
            worker_service.get("authority")
            if worker_readiness_missing_after_dead_service
            else worker_row.get("authority")
        )
        if not isinstance(raw, Mapping):
            raw = _read_authority_receipt(
                Path(str(worker_row.get("path") or "")),
                schema="okg.worker_release_authority/v1",
            )
        identity = _process_identity(raw)
        if identity != worker_service["identity"]:
            raise DeploymentConfigError(
                "worker and worker-service authorities identify different processes"
            )
        worker = {
            "generation": generation,
            "state": "running" if worker_row.get("ok") is True else "stopped",
            "identity": identity,
            "authority": dict(raw),
            "readiness_receipt_missing": (
                worker_readiness_missing_after_dead_service
            ),
        }
    result = {
        "schema": "okg.environment_restart_process_inventory/v1",
        "environment_id": environment.id,
        "target_artifact_id": current_release.get("artifact_id"),
        "target_definition_digest": current_release.get("definition_digest"),
        "target_pointer_digest": current_release.get("active_pointer_digest"),
        "previous_artifact_id": current_release.get("previous_artifact_id"),
        "previous_definition_digest": current_release.get(
            "previous_definition_digest"
        ),
        "previous_pointer_digest": current_release.get("previous_pointer_digest"),
        "plan_digest": plan.get("plan_digest"),
        "worker": worker,
        "services": services,
    }
    if completed_stop.get("ok") is True and isinstance(completed_record, Mapping):
        result.update(
            {
                "predecessor_processes": list(
                    completed_record.get("predecessor_processes") or ()
                ),
                "stopped_processes": list(
                    completed_record.get("stopped_processes") or ()
                ),
                "displaced_predecessor_processes": list(
                    completed_record.get("predecessor_processes") or ()
                ),
                "recovered_from_stop_record": True,
            }
        )
    return result


def _all_handoff_services_stopped(
    environment: DeploymentEnvironment,
    *,
    plan: Mapping[str, Any],
    service_ids: Sequence[str],
) -> bool:
    """Return true only when every exact manager target is independently down."""
    return all(
        (status := _supervisor_process_status(
            environment,
            service_id=service_id,
            plan=plan,
        )).get("ok") is True
        and status.get("status") == "stopped"
        for service_id in service_ids
    )


def _service_plan_handoff_ids(
    environment: DeploymentEnvironment,
    *,
    plan: Mapping[str, Any],
    contexts: Sequence[
        tuple[str, DeploymentEnvironment, Mapping[str, Any]]
    ],
) -> tuple[str, ...]:
    expected: list[str] = []
    for _generation, candidate, _release in contexts:
        for service_id in _supervised_service_ids(candidate):
            if service_id not in expected:
                expected.append(service_id)
    topology = plan.get("handoff_topology")
    observed = topology.get("stop_services") if isinstance(topology, Mapping) else None
    if observed != expected:
        raise DeploymentConfigError(
            "service plan handoff topology does not cover current and predecessor roles"
        )
    generations = topology.get("generations") if isinstance(topology, Mapping) else None
    expected_generations = {
        generation: _service_generation_topology(
            candidate,
            definition_digest=release.get("definition_digest"),
        )
        for generation, candidate, release in contexts
    }
    if generations != expected_generations:
        raise DeploymentConfigError(
            "service plan handoff argv does not match current and predecessor authority"
        )
    manager = topology.get("manager") if isinstance(topology, Mapping) else None
    if manager != {
        "adapter": plan.get("adapter"),
        "systemd_scope": plan.get("systemd_scope"),
    }:
        raise DeploymentConfigError("service plan handoff manager binding is invalid")
    return tuple(expected)


def _restart_inventory_from_stop_record(
    environment: DeploymentEnvironment,
    *,
    plan: Mapping[str, Any],
    release_status: Mapping[str, Any],
    stop_status: Mapping[str, Any],
    service_ids: tuple[str, ...],
) -> dict[str, Any]:
    """Recover a retry after receipt cleanup began at an attested boundary."""
    raw = stop_status.get("record")
    if not isinstance(raw, Mapping):
        raise DeploymentConfigError("predecessor stop record is unavailable")
    for service_id in service_ids:
        supervisor = _supervisor_process_status(
            environment,
            service_id=service_id,
            plan=plan,
        )
        if supervisor.get("ok") is not True or supervisor.get("status") != "stopped":
            raise DeploymentConfigError(
                "receipt-cleanup retry found a running service-manager process for "
                f"{service_id!r}"
            )
    roles = raw.get("observed_roles")
    assert isinstance(roles, Mapping)
    services = {
        service_id: {
            "generation": roles[service_id].get("generation"),
            "state": "stopped",
            "identity": roles[service_id].get("identity"),
            "supervisor": {"ok": True, "status": "stopped", "pid": None},
        }
        for service_id in service_ids
    }
    observed_worker = raw.get("observed_worker")
    if not isinstance(observed_worker, Mapping):
        raise DeploymentConfigError("predecessor stop worker inventory is invalid")
    return {
        "schema": "okg.environment_restart_process_inventory/v1",
        "environment_id": environment.id,
        "target_artifact_id": release_status.get("artifact_id"),
        "target_definition_digest": release_status.get("definition_digest"),
        "target_pointer_digest": release_status.get("active_pointer_digest"),
        "previous_artifact_id": release_status.get("previous_artifact_id"),
        "previous_definition_digest": release_status.get(
            "previous_definition_digest"
        ),
        "previous_pointer_digest": release_status.get("previous_pointer_digest"),
        "plan_digest": plan.get("plan_digest"),
        "worker": {
            "generation": observed_worker.get("generation"),
            "state": "stopped",
            "identity": observed_worker.get("identity"),
        },
        "services": services,
        "predecessor_processes": list(raw.get("predecessor_processes") or ()),
        "stopped_processes": list(raw.get("stopped_processes") or ()),
        "displaced_predecessor_processes": list(
            raw.get("predecessor_processes") or ()
        ),
        "recovered_from_stop_record": True,
    }


def _require_no_pending_dbos_workflows(
    environment: DeploymentEnvironment,
) -> None:
    """Refuse generic lifecycle mutation while DBOS work is still active."""
    pending = _pending_dbos_workflows(environment)
    if pending:
        _raise_pending_dbos_workflows(pending)


def _pending_dbos_workflows(
    environment: DeploymentEnvironment,
) -> tuple[dict[str, Any], ...]:
    try:
        with db_connect(
            environment.database.dsn,
            connect_timeout=5,
        ) as conn, conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cur.execute("SELECT to_regclass('dbos.workflow_status')")
            if cur.fetchone()[0] is None:
                raise DeploymentConfigError(
                    "DBOS workflow authority table is unavailable before restart"
                )
            cur.execute(
                """
                SELECT workflow_uuid::text, started_at_epoch_ms,
                       recovery_attempts, application_version,
                       inputs::text, updated_at
                  FROM dbos.workflow_status
                 WHERE upper(status) = 'PENDING'
                 ORDER BY workflow_uuid
                """
            )
            rows = tuple(
                {
                    "workflow_id": str(row[0]),
                    "started_at_epoch_ms": _dbos_epoch_ms(row[1]),
                    "recovery_attempts": row[2],
                    "application_version": row[3],
                    "inputs_present": row[4] is not None,
                    "input_digest": (
                        hashlib.sha256(str(row[4]).encode("utf-8")).hexdigest()
                        if row[4] is not None
                        else None
                    ),
                    "updated_at_epoch_ms": _dbos_epoch_ms(row[5]),
                }
                for row in cur.fetchall()
            )
    except DeploymentConfigError:
        raise
    except psycopg.Error as exc:
        raise DeploymentConfigError(
            "DBOS workflow authority snapshot failed before service mutation"
        ) from exc
    workflow_ids = tuple(row["workflow_id"] for row in rows)
    if any(not workflow_id for workflow_id in workflow_ids) or len(
        set(workflow_ids)
    ) != len(workflow_ids):
        raise DeploymentConfigError("DBOS workflow authority snapshot is invalid")
    return rows


def _raise_pending_dbos_workflows(
    pending: Sequence[Mapping[str, Any]],
    *,
    recovery: str = "wait for them to drain before retrying",
) -> None:
    raise DeploymentConfigError(
        "generic service lifecycle requires DBOS PENDING workflows to drain; "
        f"{recovery}; workflow_ids="
        + ",".join(str(row.get("workflow_id")) for row in pending)
    )


def _dbos_epoch_ms(value: Any) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return None


def _recoverable_pending_workflows(
    pending: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    invalid = [
        str(row.get("workflow_id"))
        for row in pending
        if row.get("started_at_epoch_ms") is None
        or row.get("inputs_present") is not True
        or not isinstance(row.get("input_digest"), str)
        or not row.get("input_digest")
        or isinstance(row.get("recovery_attempts"), bool)
        or not isinstance(row.get("recovery_attempts"), int)
        or int(row["recovery_attempts"]) >= 10
        or not isinstance(row.get("application_version"), str)
        or not row.get("application_version")
    ]
    if invalid:
        raise DeploymentConfigError(
            "stopped-worker DBOS recovery authority is incomplete for workflow_ids="
            + ",".join(invalid)
        )
    return tuple(dict(row) for row in pending)


def _wait_for_dbos_workflow_recovery(
    environment: DeploymentEnvironment,
    *,
    captured: Sequence[Mapping[str, Any]],
    replacement_started_after_epoch_ms: int,
    timeout_seconds: float,
) -> None:
    captured_by_id = {str(row["workflow_id"]): dict(row) for row in captured}
    workflow_ids = tuple(captured_by_id)
    deadline = time.monotonic() + max(timeout_seconds, 0.0)
    while True:
        try:
            with db_connect(
                environment.database.dsn,
                connect_timeout=5,
            ) as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT workflow_uuid::text, upper(status),
                           started_at_epoch_ms, recovery_attempts,
                           application_version, inputs::text,
                           updated_at
                      FROM dbos.workflow_status
                     WHERE workflow_uuid::text = ANY(%s)
                     ORDER BY workflow_uuid
                    """,
                    (list(workflow_ids),),
                )
                observed = {
                    str(row[0]): {
                        "workflow_id": str(row[0]),
                        "status": str(row[1]),
                        "started_at_epoch_ms": _dbos_epoch_ms(row[2]),
                        "recovery_attempts": row[3],
                        "application_version": row[4],
                        "input_digest": (
                            hashlib.sha256(str(row[5]).encode("utf-8")).hexdigest()
                            if row[5] is not None
                            else None
                        ),
                        "updated_at_epoch_ms": _dbos_epoch_ms(row[6]),
                    }
                    for row in cur.fetchall()
                }
        except psycopg.Error as exc:
            raise DeploymentConfigError(
                "replacement worker recovery readback failed"
            ) from exc
        if set(observed) != set(workflow_ids):
            raise DeploymentConfigError(
                "replacement worker recovery lost a captured workflow identity"
            )
        terminal_failures = sorted(
            workflow_id
            for workflow_id, row in observed.items()
            if row["status"] in {
                "ERROR",
                "CANCELLED",
                "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
            }
        )
        if terminal_failures:
            raise DeploymentConfigError(
                "replacement worker recovery entered a terminal failure: "
                + ",".join(terminal_failures)
            )
        unresolved: list[str] = []
        for workflow_id, before in captured_by_id.items():
            row = observed[workflow_id]
            identity_matches = (
                row.get("input_digest") == before.get("input_digest")
                and row.get("application_version")
                == before.get("application_version")
            )
            if row.get("status") == "SUCCESS" and identity_matches:
                continue
            attempts = row.get("recovery_attempts")
            started = row.get("started_at_epoch_ms")
            updated = row.get("updated_at_epoch_ms")
            before_attempts = before.get("recovery_attempts")
            successor_pending = (
                row.get("status") == "PENDING"
                and identity_matches
                and isinstance(attempts, int)
                and not isinstance(attempts, bool)
                and attempts < 10
                and isinstance(started, int)
                and not isinstance(started, bool)
                and isinstance(updated, int)
                and not isinstance(updated, bool)
                and (
                    started >= replacement_started_after_epoch_ms
                    or (
                        isinstance(before_attempts, int)
                        and not isinstance(before_attempts, bool)
                        and attempts > before_attempts
                        and updated >= replacement_started_after_epoch_ms
                    )
                )
            )
            if not successor_pending:
                unresolved.append(workflow_id)
        if not unresolved:
            return
        if time.monotonic() >= deadline:
            raise DeploymentConfigError(
                "replacement worker did not prove recovery of captured DBOS "
                "workflows: "
                + ",".join(sorted(unresolved))
            )
        time.sleep(0.1)


def _read_authority_receipt(path: Path, *, schema: str) -> Mapping[str, Any]:
    if (
        not path.is_absolute()
        or path.is_symlink()
        or not path.is_file()
        or stat.S_IMODE(path.stat().st_mode) != 0o600
    ):
        raise DeploymentConfigError("process authority receipt is not a 0600 regular file")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping) or raw.get("schema") != schema:
        raise DeploymentConfigError("process authority receipt schema is invalid")
    return raw


def _read_launchd_supervision_receipt(
    environment: DeploymentEnvironment,
    *,
    service_id: str,
    authority: Mapping[str, Any],
    expected_state: str,
) -> Mapping[str, Any]:
    raw = _read_authority_receipt(
        _launchd_supervision_receipt_path(environment, service_id),
        schema="okg.launchd_service_supervision/v1",
    )
    required = {
        "schema",
        "state",
        "service_id",
        "lifecycle_nonce",
        "supervisor_pid",
        "supervisor_process_start_identity",
        "child_pid",
        "child_process_start_identity",
        "started_at",
        "exit_code",
        "term_signal",
    }
    if expected_state == "exited":
        required.add("exited_at")
    if set(raw) != required or raw.get("state") != expected_state:
        raise DeploymentConfigError("launchd supervision receipt state is invalid")
    if (
        raw.get("service_id") != service_id
        or raw.get("child_pid") != authority.get("pid")
        or raw.get("child_process_start_identity")
        != authority.get("process_start_identity")
        or isinstance(raw.get("supervisor_pid"), bool)
        or not isinstance(raw.get("supervisor_pid"), int)
        or int(raw["supervisor_pid"]) <= 0
        or not isinstance(raw.get("supervisor_process_start_identity"), str)
        or not raw.get("supervisor_process_start_identity")
        or not isinstance(raw.get("lifecycle_nonce"), str)
        or not raw.get("lifecycle_nonce")
    ):
        raise DeploymentConfigError("launchd supervision receipt identity is invalid")
    authority_nonce = authority.get("lifecycle_nonce")
    if authority_nonce is not None and authority_nonce != raw.get("lifecycle_nonce"):
        raise DeploymentConfigError("launchd lifecycle nonce differs from authority")
    if expected_state == "running" and (
        raw.get("exit_code") is not None or raw.get("term_signal") is not None
    ):
        raise DeploymentConfigError("running launchd receipt has a terminal outcome")
    if expected_state == "exited":
        exit_code = raw.get("exit_code")
        term_signal = raw.get("term_signal")
        valid_exit = (
            isinstance(exit_code, int)
            and not isinstance(exit_code, bool)
            and exit_code >= 0
            and term_signal is None
        )
        valid_signal = (
            exit_code is None
            and isinstance(term_signal, int)
            and not isinstance(term_signal, bool)
            and term_signal > 0
        )
        if not (valid_exit or valid_signal):
            raise DeploymentConfigError(
                "terminal launchd receipt has an invalid process outcome"
            )
    return raw


def _service_supervisor_matches_authority(
    environment: DeploymentEnvironment,
    *,
    service_id: str,
    supervisor: Mapping[str, Any],
    authority: Mapping[str, Any],
) -> bool:
    outcome = supervisor.get("manager_outcome")
    adapter = outcome.get("adapter") if isinstance(outcome, Mapping) else None
    if adapter == "systemd":
        invocation = authority.get("systemd_invocation_id")
        return bool(
            supervisor.get("pid") == authority.get("pid")
            and (
                invocation is None
                or (
                    isinstance(outcome, Mapping)
                    and outcome.get("invocation_id") == invocation
                    and outcome.get("exec_main_pid") == authority.get("pid")
                )
            )
        )
    if adapter != "launchd":
        return False
    supervision_path = _launchd_supervision_receipt_path(environment, service_id)
    if (
        not supervision_path.exists()
        and supervisor.get("pid") == authority.get("pid")
        and _legacy_launchd_direct_exec_is_bound(
            environment,
            service_id=service_id,
            authority=authority,
        )
    ):
        if isinstance(supervisor, dict):
            supervisor["legacy_direct_exec"] = True
        return True
    try:
        receipt = _read_launchd_supervision_receipt(
            environment,
            service_id=service_id,
            authority=authority,
            expected_state="running",
        )
    except DeploymentConfigError:
        return False
    supervisor_pid = receipt.get("supervisor_pid")
    return bool(
        supervisor.get("pid") == supervisor_pid
        and process_start_identity(int(supervisor_pid))
        == receipt.get("supervisor_process_start_identity")
    )


def _process_identity(authority: Mapping[str, Any]) -> dict[str, Any]:
    pid = authority.get("pid")
    start = authority.get("process_start_identity")
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise DeploymentConfigError("process authority pid is invalid")
    if not isinstance(start, str) or not start:
        raise DeploymentConfigError("process authority start identity is invalid")
    return {"pid": pid, "process_start_identity": start}


def _predecessor_stop_path(environment: DeploymentEnvironment) -> Path:
    return environment.runtime.state_root / "supervisor/handoff-predecessor-stop.json"


def _attest_restart_inventory_stopped(
    environment: DeploymentEnvironment,
    *,
    plan: Mapping[str, Any],
    inventory: Mapping[str, Any],
) -> None:
    identities: list[Mapping[str, Any]] = []
    worker = inventory.get("worker")
    if isinstance(worker, Mapping) and isinstance(worker.get("identity"), Mapping):
        identities.append(worker["identity"])
    services = inventory.get("services")
    if not isinstance(services, Mapping):
        raise DeploymentConfigError("restart process inventory is invalid")
    worker_service_ids = _handoff_worker_service_ids(plan)
    for row in services.values():
        if isinstance(row, Mapping) and isinstance(row.get("identity"), Mapping):
            identities.append(row["identity"])
    for identity in identities:
        pid = int(identity["pid"])
        if pid_alive(pid) and process_start_identity(pid) == identity.get(
            "process_start_identity"
        ):
            raise DeploymentConfigError(
                f"restart stop boundary left process incarnation {pid} alive"
            )
    for service_id in services:
        supervisor = _supervisor_process_status(
            environment,
            service_id=service_id,
            plan=plan,
        )
        if not _supervisor_has_no_process(supervisor):
            raise DeploymentConfigError(
                "restart stop boundary lacks stopped service-manager readback for "
                f"{service_id!r}"
            )
        before = services[service_id]
        if (
            isinstance(before, Mapping)
            and isinstance(before.get("authority"), Mapping)
            and before.get("recorded_stop") is not True
        ):
            _require_clean_manager_stop_outcome(
                environment=environment,
                service_id=service_id,
                supervisor=supervisor,
                worker_role=service_id in worker_service_ids,
                service_before=before,
            )
    _attest_obsolete_services_disabled(environment, plan=plan)


def _require_clean_manager_stop_outcome(
    *,
    environment: DeploymentEnvironment,
    service_id: str,
    supervisor: Mapping[str, Any],
    worker_role: bool,
    service_before: Mapping[str, Any],
) -> None:
    """Admit only a manager-proven clean exit for a process we stopped."""
    outcome = supervisor.get("manager_outcome")
    if not isinstance(outcome, Mapping):
        raise DeploymentConfigError(
            f"service-manager stop outcome is missing for role {service_id!r}"
        )
    if outcome.get("adapter") == "launchd":
        authority = service_before.get("authority")
        if not isinstance(authority, Mapping):
            raise DeploymentConfigError(
                "launchd stop outcome lacks exact pre-stop service authority"
            )
        if _legacy_launchd_direct_exec_is_bound(
            environment,
            service_id=service_id,
            authority=authority,
        ):
            if worker_role:
                _require_legacy_launchd_clean_worker_log(
                    environment,
                    authority=authority,
                )
            return
        terminal = _read_launchd_supervision_receipt(
            environment,
            service_id=service_id,
            authority=authority,
            expected_state="exited",
        )
        clean_launchd = bool(
            terminal.get("exit_code") == 0
            or (
                not worker_role
                and terminal.get("exit_code") is None
                and terminal.get("term_signal") == 15
            )
        )
        if clean_launchd:
            return
        raise DeploymentConfigError(
            "launchd service did not emit a clean exact-PID shutdown outcome for "
            f"role {service_id!r}"
        )
    if outcome.get("adapter") != "systemd":
        raise DeploymentConfigError(
            f"service-manager adapter is unknown for role {service_id!r}"
        )
    exited = (
        outcome.get("exec_main_code"),
        outcome.get("exec_main_status"),
    )
    authority = service_before.get("authority")
    before_supervisor = service_before.get("supervisor")
    before_outcome = (
        before_supervisor.get("manager_outcome")
        if isinstance(before_supervisor, Mapping)
        else None
    )
    authority_pid = (
        authority.get("pid") if isinstance(authority, Mapping) else None
    )
    authority_invocation = (
        authority.get("systemd_invocation_id")
        if isinstance(authority, Mapping)
        else None
    )
    observed_invocation = outcome.get("invocation_id")
    invocation_bound = bool(
        isinstance(authority_pid, int)
        and not isinstance(authority_pid, bool)
        and outcome.get("exec_main_pid") == authority_pid
        and isinstance(observed_invocation, str)
        and observed_invocation
        and (
            authority_invocation == observed_invocation
            if authority_invocation is not None
            else (
                isinstance(before_outcome, Mapping)
                and before_outcome.get("exec_main_pid") == authority_pid
                and before_outcome.get("invocation_id") == observed_invocation
            )
        )
    )
    clean = bool(
        invocation_bound
        and
        outcome.get("exit_status_observed") is True
        and outcome.get("result") == "success"
        and (
            exited == (1, 0)
            or (not worker_role and exited == (2, 15))
        )
    )
    if not clean:
        raise DeploymentConfigError(
            "service-manager stop outcome was not a clean process exit for "
            f"role {service_id!r}: result={outcome.get('result')!r} "
            f"code={outcome.get('exec_main_code')!r} "
            f"status={outcome.get('exec_main_status')!r}"
        )


def _require_legacy_launchd_clean_worker_log(
    environment: DeploymentEnvironment,
    *,
    authority: Mapping[str, Any],
) -> None:
    """Prove an origin/dev direct-exec worker reached its clean terminal log."""
    verified_at = _require_aware_datetime(
        authority.get("verified_at"),
        label="legacy launchd service authority timestamp",
    )
    path = environment.runtime.log_root / "worker.log"
    if (
        path.is_symlink()
        or not path.is_file()
        or path.stat().st_uid != os.geteuid()
        or stat.S_IMODE(path.stat().st_mode) & 0o022
    ):
        raise DeploymentConfigError(
            "legacy launchd worker log is not an owner-controlled regular file"
        )
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise DeploymentConfigError(
            "legacy launchd worker log is unreadable"
        ) from exc
    deployment = re.escape(environment.graph_deployment_name)
    terminal = re.compile(
        rf"^(?P<at>\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}},\d+) "
        rf"(?P<level>[A-Z]+)\s+\S+: (?P<message>"
        rf"worker exited cleanly: deployment={deployment}|"
        rf"worker shutdown incomplete: deployment={deployment}\b.*|"
        rf"worker recovery interruption was not attested: deployment={deployment}\b.*)$"
    )
    observations: list[tuple[float, str]] = []
    for line in content.splitlines():
        match = terminal.fullmatch(line)
        if match is None:
            continue
        try:
            observed_at = datetime.strptime(
                match.group("at"), "%Y-%m-%d %H:%M:%S,%f"
            ).timestamp()
        except (ValueError, OSError, OverflowError):
            continue
        if observed_at >= verified_at.timestamp():
            observations.append((observed_at, match.group("message")))
    clean = f"worker exited cleanly: deployment={environment.graph_deployment_name}"
    if not observations or observations[-1][1] != clean:
        raise DeploymentConfigError(
            "legacy launchd worker did not emit a clean shutdown outcome"
        )


def _handoff_worker_service_ids(plan: Mapping[str, Any]) -> set[str]:
    topology = plan.get("handoff_topology")
    generations = (
        topology.get("generations") if isinstance(topology, Mapping) else None
    )
    if not isinstance(generations, Mapping):
        raise DeploymentConfigError("service handoff worker inventory is invalid")
    service_ids = {
        str(generation.get("worker_service"))
        for generation in generations.values()
        if isinstance(generation, Mapping)
        and isinstance(generation.get("worker_service"), str)
        and generation.get("worker_service")
    }
    if not service_ids:
        raise DeploymentConfigError("service handoff worker inventory is empty")
    return service_ids


def _attest_obsolete_services_disabled(
    environment: DeploymentEnvironment,
    *,
    plan: Mapping[str, Any],
) -> None:
    """Prove removed systemd roles cannot return on a later boot."""
    if plan.get("adapter") != "systemd":
        return
    topology = plan.get("handoff_topology")
    generations = topology.get("generations") if isinstance(topology, Mapping) else None
    current = generations.get("current") if isinstance(generations, Mapping) else None
    current_services = set(
        (current.get("services") or {}) if isinstance(current, Mapping) else ()
    )
    stop_services = (
        topology.get("stop_services") if isinstance(topology, Mapping) else None
    )
    if not isinstance(stop_services, list):
        raise DeploymentConfigError("service handoff stop inventory is invalid")
    for service_id in sorted(set(stop_services) - current_services):
        status = _systemd_unit_enablement_status(
            service_id,
            systemd_scope=str(plan.get("systemd_scope") or ""),
        )
        if status not in {"disabled", "not-found"}:
            raise DeploymentConfigError(
                f"obsolete predecessor service {service_id!r} remains {status}"
            )


def _systemd_unit_enablement_status(
    service_id: str,
    *,
    systemd_scope: str,
) -> str:
    if systemd_scope not in {"system", "user"}:
        raise DeploymentConfigError("installed systemd scope is invalid")
    argv = ["systemctl"]
    if systemd_scope == "user":
        argv.append("--user")
    argv.extend(["is-enabled", _systemd_unit(service_id)])
    try:
        observed = subprocess.run(  # noqa: S603 - fixed manager argv.
            argv,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except OSError as exc:
        raise DeploymentConfigError(
            "systemd enablement readback failed"
        ) from exc
    value = observed.stdout.strip()
    if observed.returncode == 1 and value == "disabled":
        return "disabled"
    if observed.returncode in {1, 4} and value == "not-found":
        return "not-found"
    return value or f"unknown(exit={observed.returncode})"


def _restart_stop_target_is_stopped(
    environment: DeploymentEnvironment,
    *,
    plan: Mapping[str, Any],
    service_id: Any,
) -> bool:
    row_ids = plan.get("handoff_topology")
    planned_targets = (
        row_ids.get("stop_services") if isinstance(row_ids, Mapping) else None
    )
    targets = (
        (str(service_id),)
        if isinstance(service_id, str) and service_id
        else tuple(str(item) for item in planned_targets or ())
    )
    if not targets:
        return False
    return all(
        (status := _supervisor_process_status(
            environment,
            service_id=target,
            plan=plan,
        ))
        and _supervisor_has_no_process(status)
        for target in targets
    )


def _record_predecessor_stop(
    environment: DeploymentEnvironment,
    *,
    inventory: Mapping[str, Any],
    cross_activation_eligible: bool = True,
) -> None:
    path = _predecessor_stop_path(environment)
    services = inventory.get("services")
    if not isinstance(services, Mapping):
        raise DeploymentConfigError("restart process inventory has no service roles")
    expected_services = tuple(str(service_id) for service_id in services)
    if set(services) != set(expected_services):
        raise DeploymentConfigError("restart process inventory service roles are partial")
    inherited_predecessors: tuple[tuple[int, str], ...] = ()
    if inventory.get("recovered_from_stop_record") is True:
        existing = _predecessor_stop_status(
            environment,
            release_status=inventory,
            expected_services=expected_services,
            expected_plan_digest=inventory.get("plan_digest"),
            require_record=True,
            allow_completed_start=True,
        )
        raw = existing.get("record")
        if existing.get("ok") is not True or not isinstance(raw, Mapping):
            raise DeploymentConfigError(
                "record-derived restart inventory lost its exact stop authority"
            )
        if (
            inventory.get("stopped_processes") != raw.get("stopped_processes")
            or inventory.get("predecessor_processes")
            != raw.get("predecessor_processes")
        ):
            raise DeploymentConfigError(
                "record-derived restart inventory changed process identities"
            )
        if cross_activation_eligible and raw.get("lifecycle_state") == "stopped":
            return
        inherited_predecessors = _canonical_stop_processes(
            raw.get("predecessor_processes"),
            label="record-derived predecessor",
        )
    elif inventory.get("displaced_predecessor_processes") is not None:
        inherited_predecessors = _canonical_stop_processes(
            inventory.get("displaced_predecessor_processes"),
            label="displaced predecessor",
        )
    predecessor_processes: dict[tuple[int, str], dict[str, Any]] = {}
    stopped_processes: dict[tuple[int, str], dict[str, Any]] = {}
    observed_roles: dict[str, dict[str, Any]] = {}
    for service_id, row in services.items():
        if not isinstance(row, Mapping):
            raise DeploymentConfigError("restart process inventory role is invalid")
        generation = row.get("generation")
        state = row.get("state")
        observed_role = {
            "generation": generation,
            "state": state,
        }
        identity = row.get("identity")
        if isinstance(identity, Mapping):
            key = (int(identity["pid"]), str(identity["process_start_identity"]))
            observed_role["identity"] = {
                "pid": key[0],
                "process_start_identity": key[1],
            }
            stopped_processes[key] = {
                "pid": key[0],
                "process_start_identity": key[1],
            }
            if generation == "previous":
                predecessor_processes[key] = dict(stopped_processes[key])
        observed_roles[str(service_id)] = observed_role
    worker = inventory.get("worker")
    if not isinstance(worker, Mapping):
        raise DeploymentConfigError("restart process inventory worker is invalid")
    observed_worker: dict[str, Any] = {
        "generation": worker.get("generation"),
        "state": worker.get("state"),
    }
    identity = worker.get("identity")
    if isinstance(identity, Mapping):
        key = (int(identity["pid"]), str(identity["process_start_identity"]))
        observed_worker["identity"] = {
            "pid": key[0],
            "process_start_identity": key[1],
        }
        stopped_processes[key] = {
            "pid": key[0],
            "process_start_identity": key[1],
        }
        if worker.get("generation") == "previous":
            predecessor_processes[key] = dict(stopped_processes[key])
    worker_matches = [
        service_id
        for service_id, role in observed_roles.items()
        if role.get("generation") == observed_worker.get("generation")
        and role.get("identity") == observed_worker.get("identity")
    ]
    if observed_worker.get("generation") == "absent":
        worker_matches = [environment.runtime.worker_service]
    if len(worker_matches) != 1 or worker_matches[0] not in observed_roles:
        raise DeploymentConfigError(
            "restart worker identity does not bind one exact service role"
        )
    observed_worker["service_id"] = worker_matches[0]
    displaced_predecessors = {
        identity: {
            "pid": identity[0],
            "process_start_identity": identity[1],
        }
        for identity in inherited_predecessors
    }
    predecessor_processes.update(displaced_predecessors)
    stopped_processes.update(displaced_predecessors)
    payload = {
        "schema": "okg.environment_predecessor_stop/v1",
        "lifecycle_epoch": secrets.token_hex(32),
        "lifecycle_state": (
            "stopped" if cross_activation_eligible else "restart_stopped"
        ),
        "environment_id": environment.id,
        "target_artifact_id": inventory.get("target_artifact_id"),
        "target_definition_digest": inventory.get("target_definition_digest"),
        "target_pointer_digest": inventory.get("target_pointer_digest"),
        "previous_artifact_id": inventory.get("previous_artifact_id"),
        "previous_definition_digest": inventory.get("previous_definition_digest"),
        "previous_pointer_digest": inventory.get("previous_pointer_digest"),
        "plan_digest": inventory.get("plan_digest"),
        "declared_services": list(expected_services),
        "observed_roles": observed_roles,
        "observed_worker": observed_worker,
        "displaced_predecessor_processes": sorted(
            displaced_predecessors.values(),
            key=lambda item: (item["pid"], item["process_start_identity"]),
        ),
        "predecessor_processes": sorted(
            predecessor_processes.values(),
            key=lambda item: (item["pid"], item["process_start_identity"]),
        ),
        "stopped_processes": sorted(
            stopped_processes.values(),
            key=lambda item: (item["pid"], item["process_start_identity"]),
        ),
        "stopped_at": datetime.now(timezone.utc).isoformat(),
    }
    _atomic_write(
        path,
        (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8"),
        mode=0o600,
    )


def _invalidate_predecessor_stop_before_start(
    environment: DeploymentEnvironment,
    *,
    plan: Mapping[str, Any],
    release_status: Mapping[str, Any],
) -> None:
    """Consume stopped evidence before a manager may launch a new process."""
    path = _predecessor_stop_path(environment)
    if not path.exists():
        return
    raw = _read_authority_receipt(
        path,
        schema="okg.environment_predecessor_stop/v1",
    )
    lifecycle_epoch = raw.get("lifecycle_epoch")
    if not isinstance(lifecycle_epoch, str) or not lifecycle_epoch:
        raise DeploymentConfigError(
            "predecessor stop record lacks a start-safe lifecycle epoch"
        )
    consumed = {
        **dict(raw),
        "lifecycle_state": "start_attempted",
        "start_attempted_at": datetime.now(timezone.utc).isoformat(),
        "start_target_artifact_id": release_status.get("artifact_id"),
        "start_target_definition_digest": release_status.get("definition_digest"),
        "start_target_pointer_digest": release_status.get("active_pointer_digest"),
        "start_plan_digest": plan.get("plan_digest"),
    }
    _atomic_write(
        path,
        (json.dumps(consumed, indent=2, sort_keys=True) + "\n").encode("utf-8"),
        mode=0o600,
    )


def _rebind_predecessor_stop_after_activation(
    environment: DeploymentEnvironment,
    *,
    plan: Mapping[str, Any],
    current_release: Mapping[str, Any],
    service_ids: tuple[str, ...],
) -> dict[str, Any] | None:
    """Carry one fresh stopped boundary across its exact activation edge."""
    path = _predecessor_stop_path(environment)
    if not path.exists():
        return None
    raw = _read_authority_receipt(
        path,
        schema="okg.environment_predecessor_stop/v1",
    )
    displaced = {
        "target_artifact_id": current_release.get("previous_artifact_id"),
        "target_definition_digest": current_release.get(
            "previous_definition_digest"
        ),
        "target_pointer_digest": current_release.get("previous_pointer_digest"),
    }
    if any(raw.get(field) != wanted for field, wanted in displaced.items()):
        return None
    if raw.get("lifecycle_state") != "stopped" or not isinstance(
        raw.get("lifecycle_epoch"), str
    ):
        raise DeploymentConfigError(
            "displaced release stop evidence was consumed by a later start attempt"
        )
    declared = raw.get("declared_services")
    if (
        not isinstance(declared, list)
        or not declared
        or not all(isinstance(service_id, str) and service_id for service_id in declared)
        or len(set(declared)) != len(declared)
    ):
        raise DeploymentConfigError(
            "displaced release stop service inventory is invalid"
        )
    prior = _predecessor_stop_status(
        environment,
        release_status=raw,
        expected_services=tuple(declared),
        expected_plan_digest=raw.get("plan_digest"),
        require_record=True,
        require_obsolete_receipts_absent=False,
    )
    if prior.get("ok") is not True:
        raise DeploymentConfigError(
            "displaced release stop evidence is invalid: "
            f"{prior.get('message') or prior.get('status')}"
        )
    marker = _read_service_install_marker(environment)
    if (
        marker.get("artifact_id") != raw.get("target_artifact_id")
        or marker.get("plan_digest") != raw.get("plan_digest")
    ):
        raise DeploymentConfigError(
            "displaced release stop evidence differs from installed service authority"
        )
    topology = plan.get("handoff_topology")
    generations = topology.get("generations") if isinstance(topology, Mapping) else None
    current_generation = (
        generations.get("current") if isinstance(generations, Mapping) else None
    )
    previous_generation = (
        generations.get("previous") if isinstance(generations, Mapping) else None
    )
    current_services = (
        current_generation.get("services")
        if isinstance(current_generation, Mapping)
        else None
    )
    previous_services = (
        previous_generation.get("services")
        if isinstance(previous_generation, Mapping)
        else None
    )
    if (
        not isinstance(current_services, Mapping)
        or not isinstance(previous_services, Mapping)
        or set(previous_services) - set(declared)
    ):
        raise DeploymentConfigError(
            "displaced release stop evidence does not cover predecessor topology"
        )
    all_manager_targets = tuple(dict.fromkeys([*declared, *service_ids]))
    if not _all_handoff_services_stopped(
        environment,
        plan=plan,
        service_ids=all_manager_targets,
    ):
        raise DeploymentConfigError(
            "displaced release service manager is no longer stopped"
        )
    if plan.get("adapter") == "systemd":
        for service_id in declared:
            if _systemd_unit_enablement_status(
                service_id,
                systemd_scope=str(plan.get("systemd_scope") or ""),
            ) not in {"disabled", "not-found"}:
                raise DeploymentConfigError(
                    "displaced release stop evidence was not produced by a "
                    f"durable fence for role {service_id!r}"
                )
    old_roles = raw.get("observed_roles")
    if not isinstance(old_roles, Mapping):
        raise DeploymentConfigError(
            "displaced release stop role inventory is invalid"
        )
    rebound_services: dict[str, dict[str, Any]] = {}
    for service_id in service_ids:
        old_role = old_roles.get(service_id)
        if service_id in previous_services:
            if not isinstance(old_role, Mapping):
                raise DeploymentConfigError(
                    "displaced release stop evidence is missing predecessor role "
                    f"{service_id!r}"
                )
            generation = old_role.get("generation")
            if generation not in {"current", "straddled", "absent"}:
                raise DeploymentConfigError(
                    "displaced release stop role does not belong to the exact "
                    f"activated predecessor: {service_id!r}"
                )
            identity = old_role.get("identity")
            rebound_services[service_id] = {
                "generation": "previous" if identity is not None else "absent",
                "state": "stopped",
                "identity": dict(identity) if isinstance(identity, Mapping) else None,
            }
        else:
            rebound_services[service_id] = {
                "generation": "absent",
                "state": "stopped",
                "identity": None,
            }
    previous_worker = previous_generation.get("worker_service")
    previous_worker_role = rebound_services.get(str(previous_worker or ""))
    if isinstance(previous_worker_role, Mapping) and isinstance(
        previous_worker_role.get("identity"), Mapping
    ):
        rebound_worker = dict(previous_worker_role)
    else:
        rebound_worker = {
            "generation": "absent",
            "state": "stopped",
            "identity": None,
        }
    rebound_inventory = {
        "target_artifact_id": current_release.get("artifact_id"),
        "target_definition_digest": current_release.get("definition_digest"),
        "target_pointer_digest": current_release.get("active_pointer_digest"),
        "previous_artifact_id": current_release.get("previous_artifact_id"),
        "previous_definition_digest": current_release.get(
            "previous_definition_digest"
        ),
        "previous_pointer_digest": current_release.get("previous_pointer_digest"),
        "plan_digest": plan.get("plan_digest"),
        "services": rebound_services,
        "worker": rebound_worker,
    }
    _record_predecessor_stop(environment, inventory=rebound_inventory)
    rebound = _predecessor_stop_status(
        environment,
        release_status=current_release,
        expected_services=service_ids,
        expected_plan_digest=plan.get("plan_digest"),
        plan=plan,
        require_record=True,
        require_obsolete_receipts_absent=False,
    )
    if rebound.get("ok") is not True:
        raise DeploymentConfigError(
            "activation-bound predecessor stop evidence did not validate: "
            f"{rebound.get('message') or rebound.get('status')}"
        )
    return rebound


def _validated_stop_role(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) not in (
        {"generation", "state"},
        {"generation", "state", "identity"},
    ):
        raise DeploymentConfigError(f"predecessor stop {label} role is invalid")
    generation = value.get("generation")
    state = value.get("state")
    if generation not in {"current", "previous", "straddled", "absent"}:
        raise DeploymentConfigError(
            f"predecessor stop {label} generation is invalid"
        )
    if state not in {"running", "stopped"}:
        raise DeploymentConfigError(f"predecessor stop {label} state is invalid")
    identity = value.get("identity")
    if (generation == "absent") != (identity is None):
        raise DeploymentConfigError(
            f"predecessor stop {label} identity is inconsistent"
        )
    if generation == "absent" and state != "stopped":
        raise DeploymentConfigError(
            f"predecessor stop {label} absent role cannot be running"
        )
    result = {"generation": generation, "state": state, "identity": None}
    if identity is not None:
        if not isinstance(identity, Mapping):
            raise DeploymentConfigError(
                f"predecessor stop {label} identity is invalid"
            )
        result["identity"] = _process_identity(identity)
    return result


def _canonical_stop_processes(
    value: Any,
    *,
    label: str,
) -> tuple[tuple[int, str], ...]:
    if not isinstance(value, list):
        raise DeploymentConfigError(
            f"{label} process inventory is invalid"
        )
    identities: list[tuple[int, str]] = []
    for item in value:
        if not isinstance(item, Mapping) or set(item) != {
            "pid",
            "process_start_identity",
        }:
            raise DeploymentConfigError(
                f"{label} process identity is invalid"
            )
        checked = _process_identity(item)
        identities.append(
            (int(checked["pid"]), str(checked["process_start_identity"]))
        )
    if identities != sorted(set(identities)):
        raise DeploymentConfigError(
            f"{label} process inventory is not canonical"
        )
    return tuple(identities)


def _predecessor_stop_status(
    environment: DeploymentEnvironment,
    *,
    release_status: Mapping[str, Any],
    expected_services: tuple[str, ...] | None = None,
    expected_plan_digest: Any = None,
    plan: Mapping[str, Any] | None = None,
    require_record: bool = False,
    require_obsolete_receipts_absent: bool = True,
    allow_completed_start: bool = False,
) -> dict[str, Any]:
    path = _predecessor_stop_path(environment)
    envelope = {
        "schema": "okg.environment_predecessor_stop_status/v1",
        "environment_id": environment.id,
        "path": str(path),
    }
    if not require_record and not release_status.get("previous_artifact_id"):
        return {**envelope, "ok": True, "status": "not_required"}
    if not path.exists():
        return {**envelope, "ok": False, "status": "missing"}
    try:
        if (
            path.is_symlink()
            or not path.is_file()
            or stat.S_IMODE(path.stat().st_mode) != 0o600
        ):
            raise DeploymentConfigError(
                "predecessor stop record is not a 0600 regular file"
            )
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping) or raw.get("schema") != (
            "okg.environment_predecessor_stop/v1"
        ):
            raise DeploymentConfigError("predecessor stop record schema is invalid")
        expected = {
            "environment_id": environment.id,
            "target_artifact_id": release_status.get("artifact_id")
            or release_status.get("target_artifact_id"),
            "target_definition_digest": release_status.get("definition_digest")
            or release_status.get("target_definition_digest"),
            "target_pointer_digest": release_status.get("active_pointer_digest")
            or release_status.get("target_pointer_digest"),
            "previous_artifact_id": release_status.get("previous_artifact_id"),
            "previous_definition_digest": release_status.get(
                "previous_definition_digest"
            ),
            "previous_pointer_digest": release_status.get("previous_pointer_digest"),
        }
        lifecycle_state = raw.get("lifecycle_state")
        valid_completed_start = bool(
            allow_completed_start
            and (
                lifecycle_state == "restart_stopped"
                or (
                    lifecycle_state == "start_attempted"
                    and raw.get("start_target_artifact_id")
                    == expected["target_artifact_id"]
                    and raw.get("start_target_definition_digest")
                    == expected["target_definition_digest"]
                    and raw.get("start_target_pointer_digest")
                    == expected["target_pointer_digest"]
                    and raw.get("start_plan_digest") == raw.get("plan_digest")
                    and (
                        expected_plan_digest is None
                        or raw.get("start_plan_digest") == expected_plan_digest
                    )
                )
            )
        )
        if (
            lifecycle_state != "stopped"
            and not valid_completed_start
        ) or not isinstance(raw.get("lifecycle_epoch"), str) or len(
            str(raw.get("lifecycle_epoch"))
        ) != 64:
            raise DeploymentConfigError(
                "predecessor stop record is not a fresh stopped or exact "
                "completed-start lifecycle boundary"
            )
        for field, wanted in expected.items():
            if raw.get(field) != wanted:
                raise DeploymentConfigError(
                    f"predecessor stop record mismatch for {field}"
                )
        if (
            not _is_sha256(raw.get("plan_digest"))
            or (
                expected_plan_digest is not None
                and raw.get("plan_digest") != expected_plan_digest
            )
        ):
            raise DeploymentConfigError(
                "predecessor stop record does not match the installed service plan"
            )
        declared_services = expected_services or _handoff_supervised_service_ids(
            environment,
            current_release=release_status,
        )
        if raw.get("declared_services") != list(declared_services):
            raise DeploymentConfigError("predecessor stop service inventory is partial")
        roles = raw.get("observed_roles")
        if not isinstance(roles, Mapping) or set(roles) != set(declared_services):
            raise DeploymentConfigError("predecessor stop role inventory is partial")
        role_rows = {
            str(service_id): _validated_stop_role(
                row,
                label=f"service {service_id!r}",
            )
            for service_id, row in roles.items()
        }
        observed_worker = raw.get("observed_worker")
        if not isinstance(observed_worker, Mapping):
            raise DeploymentConfigError("predecessor stop worker inventory is invalid")
        worker_service_id = observed_worker.get("service_id")
        worker_payload = {
            key: value
            for key, value in observed_worker.items()
            if key != "service_id"
        }
        worker_row = _validated_stop_role(
            worker_payload,
            label="worker",
        )
        if (
            not isinstance(worker_service_id, str)
            or worker_service_id not in role_rows
            or role_rows[worker_service_id].get("generation")
            != worker_row.get("generation")
            or role_rows[worker_service_id].get("identity")
            != worker_row.get("identity")
        ):
            raise DeploymentConfigError(
                "predecessor stop worker identity lacks its exact service authority"
            )
        stopped_processes = _canonical_stop_processes(
            raw.get("stopped_processes"),
            label="stopped",
        )
        predecessor_processes = _canonical_stop_processes(
            raw.get("predecessor_processes"),
            label="predecessor",
        )
        displaced_predecessors = _canonical_stop_processes(
            raw.get("displaced_predecessor_processes") or [],
            label="displaced predecessor",
        )
        role_identities = {
            (identity["pid"], identity["process_start_identity"])
            for row in (*role_rows.values(), worker_row)
            if isinstance((identity := row.get("identity")), Mapping)
        }
        predecessor_identities = {
            (identity["pid"], identity["process_start_identity"])
            for row in (*role_rows.values(), worker_row)
            if row.get("generation") == "previous"
            and isinstance((identity := row.get("identity")), Mapping)
        }
        displaced_identities = set(displaced_predecessors)
        if set(stopped_processes) != role_identities | displaced_identities:
            raise DeploymentConfigError(
                "stopped process inventory is unrelated to recorded roles"
            )
        if set(predecessor_processes) != (
            predecessor_identities | displaced_identities
        ):
            raise DeploymentConfigError(
                "predecessor process inventory is not the previous-role subset"
            )
        for pid, start_identity in stopped_processes:
            if pid_alive(pid) and process_start_identity(pid) == start_identity:
                raise DeploymentConfigError(
                    f"stopped process incarnation {pid} is still alive"
                )
        if plan is not None:
            topology = plan.get("handoff_topology")
            generations = (
                topology.get("generations")
                if isinstance(topology, Mapping)
                else None
            )
            current = (
                generations.get("current")
                if isinstance(generations, Mapping)
                else None
            )
            current_services = set(
                (current.get("services") or {})
                if isinstance(current, Mapping)
                else ()
            )
            obsolete = set(declared_services) - current_services
            for service_id in sorted(obsolete):
                supervisor = _supervisor_process_status(
                    environment,
                    service_id=service_id,
                    plan=plan,
                )
                receipt = (
                    environment.runtime.state_root
                    / "service-authority"
                    / f"{service_id}.json"
                )
                if (
                    supervisor.get("ok") is not True
                    or supervisor.get("status") != "stopped"
                    or (require_obsolete_receipts_absent and receipt.exists())
                ):
                    raise DeploymentConfigError(
                        f"obsolete predecessor service {service_id!r} is not fenced"
                    )
            _attest_obsolete_services_disabled(environment, plan=plan)
        return {**envelope, "ok": True, "status": "stopped", "record": dict(raw)}
    except Exception as exc:  # noqa: BLE001 - status preserves refusal cause.
        return {
            **envelope,
            "ok": False,
            "status": "invalid",
            "error": type(exc).__name__,
            "message": str(exc),
        }


def _no_process_is_alive(
    authority: Mapping[str, Any],
    *,
    environment_id: str,
    expected_services: tuple[str, ...],
) -> bool:
    """Accept only validator-issued proof that every expected process is dead."""
    declared_services = authority.get("declared_services")
    services = authority.get("services")
    # Proof of death is an explicit `ok: false`. A missing or non-boolean
    # verdict is unknown and proves nothing (#2573 slice 10).
    verdict = read_ok(authority, source="process_authority")
    if (
        authority.get("schema") != "okg.environment_process_authority_status/v1"
        or isinstance(verdict, Unknown)
        or verdict
        or authority.get("status") != "invalid"
        or authority.get("environment_id") != environment_id
        or len(set(expected_services)) != len(expected_services)
        or declared_services != list(expected_services)
        or not isinstance(services, Mapping)
        or set(services) != set(expected_services)
    ):
        return False
    if not _verified_dead_authority_row(
        authority.get("worker"),
        schema="okg.environment_worker_authority_status/v1",
        environment_id=environment_id,
        error="SealedReleaseInvalid",
        reason_code="worker_process_not_alive",
    ):
        return False
    return all(
        isinstance(row, Mapping)
        and row.get("service_id") == service_id
        and _verified_dead_authority_row(
            row,
            schema="okg.environment_service_authority_status/v1",
            environment_id=environment_id,
            error="DeploymentConfigError",
            reason_code="service_process_not_alive",
        )
        for service_id, row in services.items()
    )


def _verified_dead_authority_row(
    row: Any,
    *,
    schema: str,
    environment_id: str,
    error: str,
    reason_code: str,
) -> bool:
    return (
        isinstance(row, Mapping)
        and row.get("schema") == schema
        and row.get("ok") is False
        and row.get("status") == "invalid"
        and row.get("environment_id") == environment_id
        and row.get("error") == error
        and row.get("reason_code") == reason_code
    )


def _previous_release_authority_status(
    environment: DeploymentEnvironment,
    *,
    current_release: Mapping[str, Any],
) -> dict[str, Any]:
    previous_artifact_id = current_release.get("previous_artifact_id")
    previous_definition_digest = current_release.get("previous_definition_digest")
    previous_pointer_digest = current_release.get("previous_pointer_digest")
    previous_artifact_root = current_release.get("previous_artifact_root")
    if (
        not _is_sha256(previous_artifact_id)
        or not _is_sha256(previous_definition_digest)
        or not _is_sha256(previous_pointer_digest)
        or not isinstance(previous_artifact_root, str)
    ):
        raise DeploymentConfigError(
            "service restart has no predecessor-bound previous release"
        )
    artifact_store = environment.release.artifact_store.resolve()
    artifact_root = Path(previous_artifact_root).resolve()
    expected_root = artifact_store / str(previous_artifact_id).removeprefix("sha256:")
    if artifact_root != expected_root or artifact_root.parent != artifact_store:
        raise DeploymentConfigError(
            "verified previous release is outside the artifact store"
        )
    try:
        previous_pointer = load_active_release(environment.release.previous_pointer)
    except Exception as exc:  # noqa: BLE001 - normalize pointer validation failures.
        raise DeploymentConfigError(
            "verified previous release pointer is unavailable"
        ) from exc
    if (
        previous_pointer is None
        or previous_pointer.get("artifact_id") != previous_artifact_id
        or previous_pointer.get("definition_digest") != previous_definition_digest
        or active_release_pointer_digest(previous_pointer) != previous_pointer_digest
    ):
        raise DeploymentConfigError(
            "verified previous release pointer changed before service handoff"
        )
    return {
        "schema": "okg.environment_release_status/v1",
        "ok": True,
        "status": "previous",
        "environment_id": environment.id,
        "artifact_id": previous_artifact_id,
        "artifact_root": str(artifact_root),
        "definition_digest": previous_definition_digest,
        "pointer_digest": previous_pointer_digest,
        "successor_pointer_digest": current_release.get("active_pointer_digest"),
        "active_pointer": str(environment.release.active_pointer),
        "pointer_path": str(environment.release.previous_pointer),
    }


def _require_staging_clone_start_ready(
    environment: DeploymentEnvironment,
    *,
    plan: Mapping[str, Any] | None = None,
    conn: psycopg.Connection | None = None,
) -> None:
    path = environment.runtime.state_root / "clone-rebind.json"
    try:
        if path.is_symlink() or not path.is_file():
            raise DeploymentConfigError(
                "staging service start requires clone-rebind evidence"
            )
        if stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise DeploymentConfigError("clone-rebind evidence mode is not 0600")
        report = json.loads(path.read_text(encoding="utf-8"))
        target = report.get("target") if isinstance(report, dict) else None
        if (
            report.get("schema") != "okg.environment_clone_rebind/v1"
            or report.get("ok") is not True
            or report.get("apply") is not True
            or not isinstance(target, dict)
            or target.get("environment_id") != environment.id
            or target.get("graph_deployment_name") != environment.graph_deployment_name
            or report.get("ordinary_sessions_read_only") is not False
        ):
            raise DeploymentConfigError(
                "clone-rebind evidence is incomplete or foreign"
            )
        rebound_at = _require_aware_datetime(
            report.get("captured_at"),
            label="clone-rebind evidence timestamp",
        )
    except DeploymentConfigError:
        raise
    except (OSError, ValueError, TypeError) as exc:
        raise DeploymentConfigError("clone-rebind evidence is unreadable") from exc

    from okg.substrate.generation_resolver import (
        chronos_generation_inventory,
        resolve_okg_generation,
    )
    from okg.substrate.generation_bootstrap import (
        formal_bootstrap_generation_inventory,
    )

    connection_context = (
        nullcontext(conn)
        if conn is not None
        else db_connect(environment.database.dsn, autocommit=True)
    )
    with connection_context as active_conn:
        owner = active_conn.execute(
            "SELECT owner_deployment_name FROM okg.database_owner WHERE singleton"
        ).fetchone()
        latest = resolve_okg_generation(
            active_conn,
            deployment_name=environment.graph_deployment_name,
            requested=None,
            status="published",
            include_counts=True,
        )
        bootstrap_inventory = formal_bootstrap_generation_inventory(active_conn)
        chronos_inventory = chronos_generation_inventory(
            active_conn,
            deployment_name=environment.graph_deployment_name,
            checkpoint_id=latest.generation_id if latest is not None else None,
        )
    if owner is None or owner[0] != environment.graph_deployment_name:
        raise DeploymentConfigError(
            "staging database owner does not match clone-rebind target"
        )
    if bootstrap_inventory["ok"] is not True:
        raise DeploymentConfigError(
            "staging database bootstrap generation is malformed"
        )
    latest_created_at = (
        _require_aware_datetime(
            latest.created_at,
            label="published staging generation timestamp",
        )
        if latest is not None
        else None
    )
    if (
        latest is None
        or latest.deployment_name != environment.graph_deployment_name
        or latest.status != "published"
        or int(latest.node_count or 0) <= 0
        or int(latest.edge_count or 0) <= 0
        or latest_created_at <= rebound_at
        or chronos_inventory["foreign"] != 0
    ):
        raise DeploymentConfigError(
            "staging service start requires a nonempty post-rebind staging generation"
        )
    has_valid_supervision = _validated_staging_supervision_exists(
        environment,
        plan=plan,
        rebound_at=rebound_at,
    )
    if not has_valid_supervision and (
        chronos_inventory["total"] != 1
        or chronos_inventory["matching"] != 1
        or chronos_inventory["root_lineage"] is not True
    ):
        raise DeploymentConfigError(
            "first staging service start requires exactly one predecessor-free "
            "post-rebind generation"
        )


@contextmanager
def _staging_clone_start_guard(
    environment: DeploymentEnvironment,
    plan: Mapping[str, Any],
):
    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_lock_on_connection,
    )

    with (
        db_connect(
            environment.database.dsn,
            autocommit=True,
        ) as conn,
        publisher_publish_lock_on_connection(
            conn=conn,
            deployment=environment.graph_deployment_name,
            operation="staging service start",
        ),
    ):
        _require_staging_clone_start_ready(
            environment,
            plan=plan,
            conn=conn,
        )
        yield


def _validated_staging_supervision_exists(
    environment: DeploymentEnvironment,
    *,
    plan: Mapping[str, Any] | None,
    rebound_at: datetime,
) -> bool:
    path = environment.runtime.state_root / "supervisor/promotion-supervision.json"
    if not path.exists() and not path.is_symlink():
        return False
    try:
        if path.is_symlink() or not path.is_file():
            raise DeploymentConfigError("staging supervision evidence is invalid")
        if stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise DeploymentConfigError("staging supervision evidence mode is not 0600")
        evidence = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(evidence, Mapping):
            raise DeploymentConfigError("staging supervision evidence is invalid")
        raw_authority = evidence.get("process_authority") or {}
        raw_installation = evidence.get("service_installation") or {}
        raw_handoff = evidence.get("release_handoff") or {}
        authority = raw_authority if isinstance(raw_authority, Mapping) else {}
        installation = raw_installation if isinstance(raw_installation, Mapping) else {}
        handoff = raw_handoff if isinstance(raw_handoff, Mapping) else {}
        results = evidence.get("results") or []
        release = (plan or {}).get("release_status") or {}
        captured_at = _require_aware_datetime(
            evidence.get("captured_at"),
            label="staging supervision evidence timestamp",
        )
        valid_results = bool(results) and all(
            isinstance(row, Mapping)
            and type(row.get("returncode")) is int
            and row["returncode"] == 0
            for row in results
        )
        current_receipt = evidence.get("artifact_id") == release.get(
            "artifact_id"
        ) and evidence.get("plan_digest") == (plan or {}).get("plan_digest")
        previous_artifact_id = release.get("previous_artifact_id")
        process_authority = _environment_process_authority_status(environment)
        current_installation = environment_service_installation_status(
            environment,
            plan or {},
        )
        previous_installation = current_installation.get("previous_installation")
        fenced_artifact_handoff = bool(
            previous_artifact_id
            and evidence.get("artifact_id") == previous_artifact_id
            and evidence.get("artifact_id") != release.get("artifact_id")
            and isinstance(previous_installation, Mapping)
            and previous_installation.get("artifact_id") == previous_artifact_id
            and previous_installation.get("plan_digest") == evidence.get("plan_digest")
            and process_authority.get("status") == "stopped"
            and _environment_host_services_stopped(plan or {})
            and current_installation.get("ok") is True
            and current_installation.get("status") == "current"
        )
        if (
            evidence.get("schema") != "okg.environment_service_supervision_evidence/v1"
            or evidence.get("ok") is not True
            or evidence.get("status") != "completed"
            or evidence.get("environment_id") != environment.id
            or not (current_receipt or fenced_artifact_handoff)
            or evidence.get("action") not in {"start", "restart"}
            or evidence.get("evidence_path") != str(path)
            or captured_at <= rebound_at
            or authority.get("ok") is not True
            or authority.get("status") != "healthy"
            or installation.get("ok") is not True
            or installation.get("status") != "current"
            or (current_receipt and handoff.get("ok") is not True)
            or not valid_results
        ):
            raise DeploymentConfigError("staging supervision evidence is invalid")
    except DeploymentConfigError:
        raise
    except (OSError, ValueError, TypeError) as exc:
        raise DeploymentConfigError(
            "staging supervision evidence is unreadable"
        ) from exc
    return True


def _require_aware_datetime(value: Any, *, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise DeploymentConfigError(f"{label} is invalid") from exc
    else:
        raise DeploymentConfigError(f"{label} is invalid")
    try:
        offset = parsed.utcoffset()
    except Exception as exc:  # noqa: BLE001 - normalize hostile tzinfo objects.
        raise DeploymentConfigError(f"{label} is invalid") from exc
    if parsed.tzinfo is None or offset is None:
        raise DeploymentConfigError(f"{label} lacks timezone")
    try:
        return parsed.astimezone(timezone.utc)
    except Exception as exc:  # noqa: BLE001 - normalize hostile tzinfo callbacks.
        raise DeploymentConfigError(f"{label} cannot normalize to UTC") from exc


def environment_process_authority_status(
    environment: DeploymentEnvironment,
    *,
    release_status: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the aggregate worker/UI authority state for one environment."""
    observed_release = (
        dict(release_status)
        if release_status is not None
        else environment_release_status(environment)
    )
    services = {
        service_id: environment_service_authority_status(
            environment,
            service_id=service_id,
            release_status=observed_release,
        )
        for service_id in _supervised_service_ids(environment)
    }
    worker_service = services.get(environment.runtime.worker_service) or {}
    worker_service_authority = worker_service.get("authority") or {}
    worker = environment_worker_authority_status(
        environment,
        release_status=observed_release,
        expected_arguments=_expected_service_arguments(
            environment,
            environment.runtime.worker_service,
        ),
        expected_pid=(
            worker_service_authority.get("pid")
            if worker_service.get("ok") is True
            else None
        ),
    )
    envelope = {
        "schema": "okg.environment_process_authority_status/v1",
        "environment_id": environment.id,
        "declared_services": list(services),
        "worker": worker,
        "services": services,
    }
    rows = [worker, *services.values()]
    if all(row["ok"] for row in rows):
        return {
            **envelope,
            "ok": True,
            "status": "healthy",
        }
    if all(row.get("status") == "missing" for row in rows):
        return {
            **envelope,
            "ok": False,
            "status": "stopped",
            "message": "all environment process authority receipts are missing",
        }
    return {
        **envelope,
        "ok": False,
        "status": "invalid",
        "message": "environment process authority receipts are partial or invalid",
    }


def environment_release_handoff_status(
    environment: DeploymentEnvironment,
    *,
    environ: Mapping[str, str] | None = None,
    expected_artifact_id: str | None = None,
    expected_definition_digest: str | None = None,
    expected_pointer_digest: str | None = None,
) -> dict[str, Any]:
    """Report whether activation has converged in every supervised process.

    Pointer activation is intentionally separate from service-manager mutation.
    This result makes that separation mechanical: an applied activation is not
    successful until worker and service receipts read back the active artifact
    and the definition that launched it.
    """
    try:
        with (
            _environment_service_lifecycle_lock(environment),
            release_activation_lock(
                environment.release.active_pointer,
                exclusive=False,
            ),
        ):
            release_status = _environment_release_status_locked(
                environment,
                environ=environ,
            )
            expected = {
                "artifact_id": expected_artifact_id,
                "definition_digest": expected_definition_digest,
                "active_pointer_digest": expected_pointer_digest,
            }
            mismatches = {
                field: {"expected": wanted, "observed": release_status.get(field)}
                for field, wanted in expected.items()
                if wanted is not None and release_status.get(field) != wanted
            }
            if mismatches:
                return {
                    "schema": "okg.environment_release_handoff/v1",
                    "ok": False,
                    "status": "invalid",
                    "environment_id": environment.id,
                    "expected_artifact_id": expected_artifact_id,
                    "expected_definition_digest": expected_definition_digest,
                    "expected_pointer_digest": expected_pointer_digest,
                    "release_status": release_status,
                    "process_authority": None,
                    "service_installation": None,
                    "predecessor_stop": None,
                    "required_actions": [],
                    "message": "active release changed before handoff readback",
                    "mismatches": mismatches,
                }
            return _environment_release_handoff_status_locked(
                environment,
                release_status=release_status,
                environ=environ,
            )
    except Exception as exc:  # noqa: BLE001 - handoff status fails closed.
        return {
            "schema": "okg.environment_release_handoff/v1",
            "ok": False,
            "status": "invalid",
            "environment_id": environment.id,
            "expected_artifact_id": expected_artifact_id,
            "expected_definition_digest": expected_definition_digest,
            "expected_pointer_digest": expected_pointer_digest,
            "release_status": None,
            "process_authority": None,
            "service_installation": None,
            "predecessor_stop": None,
            "required_actions": [],
            "error": type(exc).__name__,
            "message": str(exc),
        }


def _environment_release_handoff_status_locked(
    environment: DeploymentEnvironment,
    *,
    release_status: Mapping[str, Any],
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Read complete handoff authority while the release snapshot is locked."""
    process_authority = environment_process_authority_status(
        environment,
        release_status=release_status,
    )
    installation = installed_environment_service_status(
        environment,
        environ=environ,
        _release_status=release_status,
    )
    plan = (
        installation.get("plan")
        if isinstance(installation.get("plan"), Mapping)
        else None
    )
    predecessor_stop = _predecessor_stop_status(
        environment,
        release_status=release_status,
        expected_plan_digest=installation.get("plan_digest"),
        expected_services=(
            tuple(
                str(service_id)
                for service_id in (
                    ((plan.get("handoff_topology") or {}).get("stop_services") or ())
                )
            )
            if isinstance(plan, Mapping)
            else None
        ),
        plan=plan,
        allow_completed_start=bool(
            process_authority.get("ok") is True
            and process_authority.get("status") == "healthy"
            and installation.get("ok") is True
        ),
    )
    complete = bool(
        release_status.get("ok")
        and process_authority.get("ok")
        and installation.get("ok")
        and predecessor_stop.get("ok")
    )
    required_actions: list[dict[str, Any]] = []
    if release_status.get("ok") and not complete:
        common = {
            "environment": environment.id,
            "requires_adapter": list(SERVICE_ADAPTERS),
            "apply": True,
        }
        required_actions = [
            {
                "sequence": 1,
                "operation": "environment.service.install",
                **common,
            },
            {
                "sequence": 2,
                "operation": "environment.service.restart",
                **common,
            },
            {
                "sequence": 3,
                "operation": "environment.service.status",
                "environment": environment.id,
                "requires_adapter": list(SERVICE_ADAPTERS),
                "expected_artifact_id": release_status.get("artifact_id"),
                "expected_definition_digest": release_status.get(
                    "definition_digest"
                ),
            },
        ]
    return {
        "schema": "okg.environment_release_handoff/v1",
        "ok": complete,
        "status": (
            "complete"
            if complete
            else "incomplete"
            if release_status.get("ok")
            else "invalid"
        ),
        "environment_id": environment.id,
        "expected_artifact_id": release_status.get("artifact_id"),
        "expected_definition_digest": release_status.get("definition_digest"),
        "expected_pointer_digest": release_status.get("active_pointer_digest"),
        "release_status": release_status,
        "process_authority": process_authority,
        "service_installation": {
            key: value for key, value in installation.items() if key != "plan"
        },
        "predecessor_stop": predecessor_stop,
        "required_actions": required_actions,
    }


def _authority_result(
    authority: Mapping[str, Any],
    *,
    installation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ok = bool(
        authority.get("ok")
        and (installation is None or installation.get("ok"))
    )
    return {
        "argv": [],
        "display": "verify sealed environment process authority",
        "returncode": 0 if ok else 1,
        "stdout": "",
        "stderr": (
            ""
            if ok
            else str(
                (
                    installation.get("message")
                    if installation is not None and not installation.get("ok")
                    else authority.get("message")
                )
                or "unhealthy"
            )
        ),
        "process_authority": dict(authority),
        "service_installation": (
            dict(installation) if installation is not None else None
        ),
    }


def _remove_environment_authority_receipts(
    environment: DeploymentEnvironment,
    *,
    service_ids: Sequence[str] | None = None,
) -> None:
    paths = [
        environment.runtime.state_root / "worker-authority.json",
        *(
            environment.runtime.state_root / "service-authority" / f"{service_id}.json"
            for service_id in (
                tuple(service_ids)
                if service_ids is not None
                else _supervised_service_ids(environment)
            )
        ),
    ]
    for path in paths:
        path.unlink(missing_ok=True)


def _inventory_service_ids(
    environment: DeploymentEnvironment,
    inventory: Mapping[str, Any],
) -> tuple[str, ...]:
    services = inventory.get("services")
    if isinstance(services, Mapping) and services:
        return tuple(str(service_id) for service_id in services)
    return _supervised_service_ids(environment)


def _supervised_service_ids(environment: DeploymentEnvironment) -> tuple[str, ...]:
    services = [environment.runtime.worker_service]
    # Environments serialized before optional-service flags were introduced still
    # own a UI receipt. Treat a missing flag as enabled so cleanup remains
    # conservative across descriptor upgrades.
    if getattr(environment.ui, "enabled", True):
        services.append(environment.ui.service)
    if getattr(environment.runtime, "mirror_enabled", False) and getattr(
        environment.runtime, "mirror_service", None
    ):
        services.append(environment.runtime.mirror_service)
    if len(set(services)) != len(services):
        raise DeploymentConfigError(
            "supervised service identifiers must be unique within an environment"
        )
    return tuple(services)


def environment_manager_process_census(
    environment: DeploymentEnvironment,
    *,
    service_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Prove whether any known service-manager target still owns a process.

    This census deliberately does not trust the install marker or process
    receipts.  It is the last authority available when those files have been
    removed or replaced and activation must decide whether an older release
    pointer can be rotated away.
    """
    targets = tuple(service_ids or _supervised_service_ids(environment))
    if not targets or len(set(targets)) != len(targets):
        return {
            "ok": False,
            "status": "invalid",
            "message": "service-manager census target inventory is invalid",
            "services": [],
        }
    if sys.platform == "darwin":
        managers: tuple[tuple[str, str | None], ...] = (("launchd", None),)
    elif sys.platform.startswith("linux"):
        # An environment can be installed into either scope.  Both must be
        # conclusively empty before missing authority files mean no runtime.
        managers = (("systemd", "system"), ("systemd", "user"))
    else:
        return {
            "ok": False,
            "status": "invalid",
            "message": "service-manager census is unsupported on this platform",
            "services": [],
        }
    rows = [
        {
            "adapter": adapter,
            "systemd_scope": scope,
            "service_id": service_id,
            "process": _supervisor_process_status(
                environment,
                service_id=service_id,
                plan={
                    "environment_id": environment.id,
                    "adapter": adapter,
                    "systemd_scope": scope,
                },
            ),
        }
        for adapter, scope in managers
        for service_id in targets
    ]
    running = [
        row
        for row in rows
        if row["process"].get("ok") is True
        and row["process"].get("status") == "running"
        and isinstance(row["process"].get("pid"), int)
    ]
    if running:
        return {
            "ok": True,
            "status": "running",
            "process_present": True,
            "services": rows,
        }
    if all(_supervisor_has_no_process(row["process"]) for row in rows):
        return {
            "ok": True,
            "status": "stopped",
            "process_present": False,
            "services": rows,
        }
    return {
        "ok": False,
        "status": "invalid",
        "process_present": None,
        "message": "service-manager process absence could not be proved",
        "services": rows,
    }


def _previous_release_environment(
    environment: DeploymentEnvironment,
    *,
    current_release: Mapping[str, Any],
    environ: Mapping[str, str] | None = None,
) -> tuple[dict[str, Any], DeploymentEnvironment]:
    previous_release = _previous_release_authority_status(
        environment,
        current_release=current_release,
    )
    try:
        pointer = load_active_release(environment.release.previous_pointer)
        if pointer is None:
            raise DeploymentConfigError(
                "verified previous release pointer is unavailable"
            )
        previous_environment = historical_release_environment(
            environment,
            pointer,
            environ=environ,
        )
    except Exception as exc:  # noqa: BLE001 - use installed immutable topology.
        try:
            previous_environment = _installed_historical_environment(
                environment,
                current_release=current_release,
            )
        except Exception as fallback_exc:  # noqa: BLE001 - preserve both causes.
            raise DeploymentConfigError(
                "verified previous release topology is unavailable from both "
                "the sealed descriptor and installed service authority"
            ) from ExceptionGroup(
                "historical topology authorities failed",
                [exc, fallback_exc],
            )
    return previous_release, previous_environment


def _service_generation_topology(
    environment: DeploymentEnvironment,
    *,
    definition_digest: Any,
) -> dict[str, Any]:
    return {
        "definition_digest": definition_digest,
        "worker_service": environment.runtime.worker_service,
        "services": {
            service_id: list(_expected_service_arguments(environment, service_id))
            for service_id in _supervised_service_ids(environment)
        },
    }


def _handoff_service_topology(
    environment: DeploymentEnvironment,
    *,
    current_release: Mapping[str, Any],
    adapter: str,
    systemd_scope: str | None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Return the complete current/predecessor service-manager authority."""
    generations = {
        "current": _service_generation_topology(
            environment,
            definition_digest=current_release.get("definition_digest"),
        )
    }
    candidates = [environment]
    if current_release.get("previous_artifact_id"):
        _release, previous_environment = _previous_release_environment(
            environment,
            current_release=current_release,
            environ=environ,
        )
        _require_handoff_manager_compatible(
            environment,
            current_release=current_release,
            adapter=adapter,
            systemd_scope=systemd_scope,
        )
        candidates.append(previous_environment)
        generations["previous"] = _service_generation_topology(
            previous_environment,
            definition_digest=current_release.get("previous_definition_digest"),
        )
    services: list[str] = []
    for candidate in candidates:
        for service_id in _supervised_service_ids(candidate):
            if service_id not in services:
                services.append(service_id)
    return {
        "stop_services": services,
        "manager": {"adapter": adapter, "systemd_scope": systemd_scope},
        "generations": generations,
    }


def _handoff_supervised_service_ids(
    environment: DeploymentEnvironment,
    *,
    current_release: Mapping[str, Any],
    environ: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    """Recover the stop set for readback from the installed plan authority."""
    installed = installed_environment_service_status(environment, environ=environ)
    plan = installed.get("plan")
    topology = plan.get("handoff_topology") if isinstance(plan, Mapping) else None
    services = topology.get("stop_services") if isinstance(topology, Mapping) else None
    if not isinstance(services, list) or not all(
        isinstance(service_id, str) and service_id for service_id in services
    ):
        raise DeploymentConfigError(
            "installed service handoff topology is unavailable"
        )
    return tuple(services)


def _read_service_install_marker(
    environment: DeploymentEnvironment,
) -> Mapping[str, Any]:
    path = environment.runtime.state_root / "supervisor/install.json"
    if (
        path.is_symlink()
        or not path.is_file()
        or stat.S_IMODE(path.stat().st_mode) != 0o600
    ):
        raise DeploymentConfigError(
            "historical topology requires a 0600 regular install marker"
        )
    raw = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(raw, Mapping)
        or raw.get("schema") != "okg.environment_service_install/v1"
        or raw.get("environment_id") != environment.id
    ):
        raise DeploymentConfigError("historical install marker identity is invalid")
    return raw


def _previous_manager_binding(
    environment: DeploymentEnvironment,
    *,
    current_release: Mapping[str, Any],
) -> Mapping[str, Any]:
    marker = _read_service_install_marker(environment)
    previous_artifact = current_release.get("previous_artifact_id")
    binding: Mapping[str, Any] | None = None
    if marker.get("artifact_id") == previous_artifact:
        binding = marker
    prior = marker.get("previous_installation")
    if binding is None and (
        marker.get("artifact_id") == current_release.get("artifact_id")
        and isinstance(prior, Mapping)
        and prior.get("artifact_id") == previous_artifact
    ):
        binding = prior
    if binding is None:
        raise DeploymentConfigError(
            "installed service manager is not bound to the predecessor release"
        )
    adapter = binding.get("adapter")
    systemd_scope = binding.get("systemd_scope")
    if adapter in SERVICE_ADAPTERS and (
        adapter != "systemd" or systemd_scope in {"system", "user"}
    ):
        return binding
    pointer = load_active_release(environment.release.previous_pointer)
    if pointer is None:
        raise DeploymentConfigError(
            "legacy installed service manager lacks a verified predecessor pointer"
        )
    inferred = _installed_legacy_manager_binding(
        environment,
        pointer=pointer,
    )
    if adapter is not None and adapter != inferred["adapter"]:
        raise DeploymentConfigError(
            "legacy installed service adapter conflicts with unit artifacts"
        )
    if (
        inferred["adapter"] == "systemd"
        and systemd_scope is not None
        and systemd_scope != inferred["systemd_scope"]
    ):
        raise DeploymentConfigError(
            "legacy installed systemd scope conflicts with unit artifacts"
        )
    return {**dict(binding), **inferred}


def _installed_legacy_manager_binding(
    environment: DeploymentEnvironment,
    *,
    pointer: Mapping[str, Any],
) -> dict[str, Any]:
    """Infer an old marker's manager only from secure installed artifacts."""
    supervisor = environment.runtime.state_root / "supervisor"
    systemd_paths = sorted(supervisor.glob("*.service"))
    launchd_paths = sorted(supervisor.glob("*.plist"))
    if bool(systemd_paths) == bool(launchd_paths):
        raise DeploymentConfigError(
            "legacy installed service manager artifacts are missing or ambiguous"
        )
    if launchd_paths:
        generation = _installed_generation_from_artifacts(
            environment,
            pointer=pointer,
            adapter="launchd",
        )
        return {
            "adapter": "launchd",
            "systemd_scope": None,
            "service_topology": generation,
        }
    scopes: set[str] = set()
    for path in systemd_paths:
        if (
            path.is_symlink()
            or not path.is_file()
            or stat.S_IMODE(path.stat().st_mode) != 0o644
        ):
            raise DeploymentConfigError("legacy systemd artifact is insecure")
        wanted_by = [
            line.removeprefix("WantedBy=")
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.startswith("WantedBy=")
        ]
        if wanted_by == ["default.target"]:
            scopes.add("user")
        elif wanted_by == ["multi-user.target"]:
            scopes.add("system")
        else:
            raise DeploymentConfigError(
                "legacy systemd artifact has no unambiguous installed scope"
            )
    if len(scopes) != 1:
        raise DeploymentConfigError("legacy installed systemd scope is ambiguous")
    scope = scopes.pop()
    generation = _installed_generation_from_artifacts(
        environment,
        pointer=pointer,
        adapter="systemd",
    )
    return {
        "adapter": "systemd",
        "systemd_scope": scope,
        "service_topology": generation,
    }


def _require_handoff_manager_compatible(
    environment: DeploymentEnvironment,
    *,
    current_release: Mapping[str, Any],
    adapter: str,
    systemd_scope: str | None,
) -> None:
    previous = _previous_manager_binding(
        environment,
        current_release=current_release,
    )
    manager_changed = previous.get("adapter") != adapter or (
        adapter == "systemd" and previous.get("systemd_scope") != systemd_scope
    )
    if manager_changed:
        raise DeploymentConfigError(
            "service handoff cannot change adapter or systemd scope; stop the "
            "predecessor with its installed manager before migrating"
        )


def _installed_historical_environment(
    environment: DeploymentEnvironment,
    *,
    current_release: Mapping[str, Any],
) -> DeploymentEnvironment:
    """Reconstruct legacy predecessor argv from its installed unit authority."""
    pointer = load_active_release(environment.release.previous_pointer)
    if pointer is None:
        raise DeploymentConfigError("historical release pointer is unavailable")
    manager = _previous_manager_binding(
        environment,
        current_release=current_release,
    )
    generation = manager.get("service_topology")
    if not isinstance(generation, Mapping):
        generation = _installed_generation_from_artifacts(
            environment,
            pointer=pointer,
            adapter=str(manager.get("adapter") or ""),
        )
    services = generation.get("services")
    worker_service = generation.get("worker_service")
    if (
        not isinstance(services, Mapping)
        or not isinstance(worker_service, str)
        or worker_service not in services
    ):
        raise DeploymentConfigError("installed historical service topology is invalid")
    ui_rows = [
        (service_id, list(arguments))
        for service_id, arguments in services.items()
        if isinstance(arguments, list) and "okg.viz" in arguments
    ]
    mirror_rows = [
        (service_id, list(arguments))
        for service_id, arguments in services.items()
        if isinstance(arguments, list)
        and "deployments/okg-ci/mirror_ci_results.py" in arguments
    ]
    if len(ui_rows) > 1 or len(mirror_rows) > 1:
        raise DeploymentConfigError("installed historical optional roles are ambiguous")
    ui = replace(environment.ui, enabled=False)
    if ui_rows:
        service_id, arguments = ui_rows[0]
        try:
            host = arguments[arguments.index("--host") + 1]
            port = int(arguments[arguments.index("--port") + 1])
        except (ValueError, IndexError) as exc:
            raise DeploymentConfigError(
                "installed historical UI arguments are invalid"
            ) from exc
        ui = replace(ui, enabled=True, service=service_id, host=host, port=port)
    historical_data_root = Path(str(pointer.get("data_root") or ""))
    if not historical_data_root.is_absolute():
        raise DeploymentConfigError("historical pointer data root is invalid")
    runtime = replace(
        environment.runtime,
        data_root=historical_data_root,
        worker_service=worker_service,
        mirror_enabled=bool(mirror_rows),
        mirror_service=mirror_rows[0][0] if mirror_rows else None,
    )
    database = pointer.get("database")
    if not isinstance(database, Mapping):
        raise DeploymentConfigError("historical pointer database binding is invalid")
    candidate = replace(
        environment,
        tier=str(pointer.get("tier") or ""),
        graph_deployment_name=str(pointer.get("graph_deployment_name") or ""),
        descriptor_digest=str(pointer.get("descriptor_digest") or ""),
        definition_digest=str(pointer.get("definition_digest") or ""),
        database=replace(
            environment.database,
            name=str(database.get("name") or ""),
            dbos_identity=str(database.get("dbos_identity") or ""),
        ),
        runtime=runtime,
        ui=ui,
    )
    observed = _service_generation_topology(
        candidate,
        definition_digest=candidate.definition_digest,
    )
    if observed != dict(generation):
        raise DeploymentConfigError(
            "reconstructed historical topology differs from installed authority"
        )
    return candidate


def _installed_generation_from_artifacts(
    environment: DeploymentEnvironment,
    *,
    pointer: Mapping[str, Any],
    adapter: str,
) -> dict[str, Any]:
    supervisor = environment.runtime.state_root / "supervisor"
    if adapter == "systemd":
        paths = sorted(supervisor.glob("*.service"))
    elif adapter == "launchd":
        paths = sorted(supervisor.glob("*.plist"))
    else:
        raise DeploymentConfigError("historical service adapter is invalid")
    services: dict[str, list[str]] = {}
    for path in paths:
        if path.is_symlink() or not path.is_file() or stat.S_IMODE(path.stat().st_mode) != 0o644:
            raise DeploymentConfigError("historical service artifact is insecure")
        if adapter == "systemd":
            lines = path.read_text(encoding="utf-8").splitlines()
            service_lines = [line for line in lines if "OKG_SERVICE_ID=" in line]
            exec_lines = [line for line in lines if line.startswith("ExecStart=")]
            if len(service_lines) != 1 or len(exec_lines) != 1:
                raise DeploymentConfigError("historical systemd artifact is invalid")
            service_id = service_lines[0].split("OKG_SERVICE_ID=", 1)[1].rstrip('"')
            argv = shlex.split(exec_lines[0].removeprefix("ExecStart="))
        else:
            document = plistlib.loads(path.read_bytes())
            service_id = document.get("Label")
            argv = document.get("ProgramArguments")
            if not isinstance(service_id, str) or not isinstance(argv, list):
                raise DeploymentConfigError("historical launchd artifact is invalid")
            argv = [str(item) for item in argv]
        if adapter == "launchd":
            expected_environment_exec = supervisor / "environment_exec.py"
            if (
                len(argv) < 9
                or not Path(argv[0]).is_absolute()
                or Path(argv[1]) != expected_environment_exec
                or argv[2] != "--env-file"
                or Path(argv[3]) != environment.runtime.local_env_file
            ):
                raise DeploymentConfigError(
                    "historical launchd environment wrapper is invalid"
                )
            if argv[4] == "--":
                launcher_argv = argv[5:]
            elif (
                len(argv) >= 11
                and argv[4] == "--supervision-receipt"
                and Path(argv[5])
                == _launchd_supervision_receipt_path(environment, service_id)
                and argv[6] == "--"
            ):
                launcher_argv = argv[7:]
            else:
                raise DeploymentConfigError(
                    "historical launchd supervision wrapper is invalid"
                )
        else:
            launcher_argv = argv
        expected_launcher = supervisor / "release_launcher.py"
        expected_descriptor = launcher_descriptor_path(environment)
        if (
            len(launcher_argv) < 4
            or not Path(launcher_argv[0]).is_absolute()
            or Path(launcher_argv[1]) != expected_launcher
            or launcher_argv[2] != "--descriptor"
            or Path(launcher_argv[3]) != expected_descriptor
        ):
            raise DeploymentConfigError("historical service launcher is invalid")
        if len(launcher_argv) == 4:
            deployment = pointer.get("graph_deployment_name")
            if not isinstance(deployment, str) or not deployment:
                raise DeploymentConfigError(
                    "historical worker deployment identity is invalid"
                )
            arguments = [
                "-I",
                "-m",
                "okg.substrate.runtime.worker",
                "--deployment",
                deployment,
            ]
        else:
            if launcher_argv[4] != "--" or len(launcher_argv) == 5:
                raise DeploymentConfigError("historical service command is invalid")
            arguments = ["-I", *launcher_argv[5:]]
        if not (
            "okg.substrate.runtime.worker" in arguments
            or "okg.viz" in arguments
            or "deployments/okg-ci/mirror_ci_results.py" in arguments
        ):
            raise DeploymentConfigError("historical service role is unknown")
        receipt = environment.runtime.state_root / "service-authority" / f"{service_id}.json"
        if receipt.exists():
            raw = _read_authority_receipt(
                receipt,
                schema="okg.service_release_authority/v1",
            )
            expected = {
                "service_id": service_id,
                "environment_id": environment.id,
                "artifact_id": pointer.get("artifact_id"),
                "arguments": arguments,
            }
            definition_matches = raw.get("definition_digest") == pointer.get(
                "definition_digest"
            ) or (
                "definition_digest" not in raw
                and _legacy_service_receipt_definition_is_bound(
                    environment,
                    raw=raw,
                    observed_release=_previous_release_authority_status(
                        environment,
                        current_release=environment_release_status(environment),
                    ),
                )
            )
            if (
                not definition_matches
                or any(raw.get(field) != wanted for field, wanted in expected.items())
            ):
                raise DeploymentConfigError(
                    "historical service receipt differs from installed unit authority"
                )
        services[service_id] = arguments
    workers = [
        service_id
        for service_id, arguments in services.items()
        if "okg.substrate.runtime.worker" in arguments
    ]
    if len(workers) != 1:
        raise DeploymentConfigError(
            "installed historical topology must identify exactly one worker"
        )
    return {
        "definition_digest": pointer.get("definition_digest"),
        "worker_service": workers[0],
        "services": services,
    }


def _installed_launchd_process_models(
    environment: DeploymentEnvironment,
    *,
    service_ids: tuple[str, ...],
) -> dict[str, str]:
    """Classify exact installed plist wrappers for one verified topology."""
    supervisor = environment.runtime.state_root / "supervisor"
    models: dict[str, str] = {}
    for service_id in service_ids:
        path = supervisor / f"{service_id}.plist"
        if (
            path.is_symlink()
            or not path.is_file()
            or stat.S_IMODE(path.stat().st_mode) != 0o644
        ):
            raise DeploymentConfigError("historical launchd artifact is insecure")
        try:
            document = plistlib.loads(path.read_bytes())
        except (OSError, ValueError, TypeError) as exc:
            raise DeploymentConfigError(
                "historical launchd artifact is invalid"
            ) from exc
        argv = document.get("ProgramArguments")
        if (
            document.get("Label") != service_id
            or document.get("EnvironmentVariables")
            != {"OKG_SERVICE_ID": service_id}
            or not isinstance(argv, list)
            or not all(isinstance(item, str) for item in argv)
            or len(argv) < 9
            or Path(argv[1]) != supervisor / "environment_exec.py"
            or argv[2] != "--env-file"
            or Path(argv[3]) != environment.runtime.local_env_file
        ):
            raise DeploymentConfigError(
                "historical launchd process wrapper is invalid"
            )
        if argv[4] == "--":
            models[service_id] = "direct_exec"
        elif (
            len(argv) >= 11
            and argv[4] == "--supervision-receipt"
            and Path(argv[5])
            == _launchd_supervision_receipt_path(environment, service_id)
            and argv[6] == "--"
        ):
            models[service_id] = "supervised_wrapper"
        else:
            raise DeploymentConfigError(
                "historical launchd supervision wrapper is invalid"
            )
    return models


def _legacy_launchd_direct_exec_is_bound(
    environment: DeploymentEnvironment,
    *,
    service_id: str,
    authority: Mapping[str, Any],
) -> bool:
    """Recognize only the exact origin/dev direct-exec launchd installation."""
    if "definition_digest" in authority or "lifecycle_nonce" in authority:
        return False
    try:
        marker = _read_service_install_marker(environment)
    except DeploymentConfigError:
        return False
    candidates: list[tuple[Mapping[str, Any], Mapping[str, Any] | None]] = []
    current_topology = marker.get("service_topology")
    if marker.get("adapter") == "launchd":
        try:
            current_models: Mapping[str, Any] = _installed_launchd_process_models(
                environment,
                service_ids=tuple(
                    str(item)
                    for item in (
                        current_topology.get("services")
                        if isinstance(current_topology, Mapping)
                        else ()
                    )
                ),
            )
        except DeploymentConfigError:
            current_models = {}
        candidates.append((marker, current_models))
    elif marker.get("artifact_id") == authority.get("artifact_id"):
        try:
            pointer = load_active_release(environment.release.previous_pointer)
            if pointer is not None:
                inferred = _installed_legacy_manager_binding(
                    environment,
                    pointer=pointer,
                )
                if inferred.get("adapter") == "launchd":
                    topology = inferred.get("service_topology")
                    services = (
                        topology.get("services")
                        if isinstance(topology, Mapping)
                        else None
                    )
                    if isinstance(services, Mapping):
                        models = _installed_launchd_process_models(
                            environment,
                            service_ids=tuple(str(item) for item in services),
                        )
                        candidates.append(
                            ({**dict(marker), **dict(inferred)}, models)
                        )
        except DeploymentConfigError:
            pass
    previous = marker.get("previous_installation")
    if isinstance(previous, Mapping) and previous.get("adapter") == "launchd":
        models = previous.get("launchd_process_models")
        candidates.append((previous, models if isinstance(models, Mapping) else None))
    for binding, models in candidates:
        topology = binding.get("service_topology")
        services = topology.get("services") if isinstance(topology, Mapping) else None
        if (
            binding.get("artifact_id") == authority.get("artifact_id")
            and isinstance(services, Mapping)
            and services.get(service_id) == authority.get("arguments")
            and isinstance(models, Mapping)
            and models.get(service_id) == "direct_exec"
        ):
            return True
    return False


def _legacy_service_receipt_definition_is_bound(
    environment: DeploymentEnvironment,
    *,
    raw: Mapping[str, Any],
    observed_release: Mapping[str, Any],
) -> bool:
    """Bind origin/dev's definition-less receipt to one exact predecessor.

    The launcher on the issue's base commit wrote the artifact and runtime
    roots but omitted ``definition_digest``.  That legacy shape is useful only
    for removing the process displaced by the first upgraded activation.  The
    active pointer must therefore name the exact previous pointer by artifact,
    definition, and canonical pointer digest; no current receipt or explicit
    but incorrect definition receives this compatibility treatment.
    """
    if (
        "definition_digest" in raw
        or observed_release.get("ok") is not True
        or observed_release.get("status") != "previous"
        or observed_release.get("environment_id") != environment.id
        or observed_release.get("active_pointer")
        != str(environment.release.active_pointer)
        or observed_release.get("pointer_path")
        != str(environment.release.previous_pointer)
        or raw.get("active_pointer") != str(environment.release.active_pointer)
        or not _is_sha256(observed_release.get("artifact_id"))
        or not _is_sha256(observed_release.get("definition_digest"))
        or not _is_sha256(observed_release.get("pointer_digest"))
        or not _is_sha256(observed_release.get("successor_pointer_digest"))
    ):
        return False
    try:
        active = load_active_release(environment.release.active_pointer)
        previous = load_active_release(environment.release.previous_pointer)
        assert active is not None and previous is not None
    except Exception:  # noqa: BLE001 - compatibility is fail-closed.
        return False
    expected_predecessor = {
        "schema": "okg.release_predecessor/v1",
        "artifact_id": previous.get("artifact_id"),
        "definition_digest": previous.get("definition_digest"),
        "pointer_digest": active_release_pointer_digest(previous),
    }
    expected_root = (
        environment.release.artifact_store.resolve()
        / str(previous.get("artifact_id") or "").removeprefix("sha256:")
    )
    return bool(
        active.get("predecessor") == expected_predecessor
        and active_release_pointer_digest(active)
        == observed_release.get("successor_pointer_digest")
        and expected_predecessor["pointer_digest"]
        == observed_release.get("pointer_digest")
        and previous.get("artifact_id") == observed_release.get("artifact_id")
        and previous.get("definition_digest")
        == observed_release.get("definition_digest")
        and Path(str(previous.get("artifact_root") or "")).resolve() == expected_root
        and observed_release.get("artifact_root") == str(expected_root)
        and raw.get("artifact_id") == previous.get("artifact_id")
        and raw.get("artifact_root") == str(expected_root)
    )


def _expected_service_arguments(
    environment: DeploymentEnvironment,
    service_id: str,
) -> tuple[str, ...]:
    """Return the sealed interpreter argv for one configured service role."""
    if service_id == environment.runtime.worker_service:
        return (
            "-I",
            "-m",
            "okg.substrate.runtime.worker",
            "--deployment",
            environment.graph_deployment_name,
        )
    if getattr(environment.ui, "enabled", True) and service_id == environment.ui.service:
        return (
            "-I",
            "-m",
            "okg.viz",
            "--deployment",
            environment.graph_deployment_name,
            "--host",
            environment.ui.host,
            "--port",
            str(environment.ui.port),
        )
    if (
        getattr(environment.runtime, "mirror_enabled", False)
        and service_id == getattr(environment.runtime, "mirror_service", None)
    ):
        return (
            "-I",
            "deployments/okg-ci/mirror_ci_results.py",
            "--cache",
            str(environment.runtime.data_root / "ci-results"),
            "--limit",
            "20",
            "--watch-interval-seconds",
            "300",
        )
    raise DeploymentConfigError(
        f"service {service_id!r} is not a configured supervised role"
    )


def _supervisor_process_status(
    environment: DeploymentEnvironment,
    *,
    service_id: str,
    plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Read the independently observed main PID from the installed supervisor."""
    marker = environment.runtime.state_root / "supervisor/install.json"
    envelope = {
        "schema": "okg.environment_supervisor_process_status/v1",
        "environment_id": environment.id,
        "service_id": service_id,
    }
    try:
        if plan is None:
            if (
                marker.is_symlink()
                or not marker.is_file()
                or stat.S_IMODE(marker.stat().st_mode) != 0o600
            ):
                raise DeploymentConfigError(
                    "service manager identity requires a 0600 regular install marker"
                )
            raw = json.loads(marker.read_text(encoding="utf-8"))
            if (
                not isinstance(raw, Mapping)
                or raw.get("schema") != "okg.environment_service_install/v1"
                or raw.get("environment_id") != environment.id
            ):
                raise DeploymentConfigError("service install marker identity is invalid")
            adapter = raw.get("adapter")
            systemd_scope = raw.get("systemd_scope")
        else:
            if plan.get("environment_id") != environment.id:
                raise DeploymentConfigError("service plan environment is invalid")
            adapter = plan.get("adapter")
            systemd_scope = plan.get("systemd_scope")
        if adapter == "systemd":
            if systemd_scope not in {"system", "user"}:
                raise DeploymentConfigError("installed systemd scope is invalid")
            argv = ["systemctl"]
            if systemd_scope == "user":
                argv.append("--user")
            argv.extend(
                [
                    "show",
                    "--property=LoadState",
                    "--property=MainPID",
                    "--property=ActiveState",
                    "--property=SubState",
                    "--property=Job",
                    "--property=Result",
                    "--property=ExecMainCode",
                    "--property=ExecMainStatus",
                    "--property=ExecMainPID",
                    "--property=InvocationID",
                    _systemd_unit(service_id),
                ]
            )
            observed = subprocess.run(  # noqa: S603 - fixed supervisor argv.
                argv,
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
            if observed.returncode != 0:
                raise DeploymentConfigError(
                    "systemd main-pid readback failed: "
                    + (observed.stderr.strip() or f"exit {observed.returncode}")
                )
            properties: dict[str, str] = {}
            for line in observed.stdout.splitlines():
                if "=" not in line:
                    raise DeploymentConfigError(
                        "systemd returned an invalid lifecycle property"
                    )
                name, value = line.split("=", 1)
                if name in properties:
                    raise DeploymentConfigError(
                        "systemd returned a duplicate lifecycle property"
                    )
                properties[name] = value
            expected_properties = {
                "LoadState",
                "MainPID",
                "ActiveState",
                "SubState",
                "Job",
                "Result",
                "ExecMainCode",
                "ExecMainStatus",
                "ExecMainPID",
                "InvocationID",
            }
            if set(properties) != expected_properties:
                raise DeploymentConfigError(
                    "systemd lifecycle readback is incomplete"
                )
            if (
                not properties["ExecMainCode"].isdigit()
                or not properties["ExecMainStatus"].isdigit()
                or not properties["ExecMainPID"].isdigit()
                or not properties["Result"]
                or (
                    properties["InvocationID"]
                    and re.fullmatch(r"[0-9a-f]{32}", properties["InvocationID"])
                    is None
                )
            ):
                raise DeploymentConfigError(
                    "systemd exit outcome readback is invalid"
                )
            manager_outcome = {
                "adapter": "systemd",
                "load_state": properties["LoadState"],
                "result": properties["Result"],
                "exec_main_code": int(properties["ExecMainCode"]),
                "exec_main_status": int(properties["ExecMainStatus"]),
                "exec_main_pid": int(properties["ExecMainPID"]),
                "invocation_id": properties["InvocationID"],
                "exit_status_observed": True,
            }
            value = properties["MainPID"]
            job = properties["Job"]
            if job not in {"", "0"}:
                raise DeploymentConfigError("systemd service has a pending job")
            if value == "0":
                lifecycle = (
                    properties["ActiveState"],
                    properties["SubState"],
                )
                if lifecycle == ("inactive", "dead"):
                    status = "stopped"
                elif lifecycle == ("failed", "failed"):
                    status = "failed"
                else:
                    raise DeploymentConfigError(
                        "systemd service is not conclusively stopped"
                    )
                return {
                    **envelope,
                    "ok": True,
                    "status": status,
                    "pid": None,
                    "manager_outcome": manager_outcome,
                }
            if not value.isdigit() or int(value) <= 0:
                raise DeploymentConfigError("systemd returned an invalid main pid")
            if (
                properties["ActiveState"] != "active"
                or properties["SubState"] != "running"
                or int(properties["ExecMainPID"]) != int(value)
                or not properties["InvocationID"]
            ):
                raise DeploymentConfigError(
                    "systemd service is not conclusively running"
                )
            return {
                **envelope,
                "ok": True,
                "status": "running",
                "pid": int(value),
                "manager_outcome": manager_outcome,
            }
        if adapter == "launchd":
            domain = f"gui/{os.getuid()}"
            observed = subprocess.run(  # noqa: S603 - fixed supervisor argv.
                ["launchctl", "print", f"{domain}/{service_id}"],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
            if observed.returncode == 113 and "Could not find service" in observed.stderr:
                return {
                    **envelope,
                    "ok": True,
                    "status": "stopped",
                    "pid": None,
                    "manager_outcome": {
                        "adapter": "launchd",
                        "exit_status_observed": False,
                    },
                }
            if observed.returncode != 0:
                raise DeploymentConfigError(
                    "launchd main-pid readback failed: "
                    + (observed.stderr.strip() or f"exit {observed.returncode}")
                )
            matches = [
                line.split("=", 1)[1].strip()
                for line in observed.stdout.splitlines()
                if line.strip().startswith("pid =")
            ]
            if len(matches) != 1 or not matches[0].isdigit() or int(matches[0]) <= 0:
                raise DeploymentConfigError("launchd returned an invalid main pid")
            return {
                **envelope,
                "ok": True,
                "status": "running",
                "pid": int(matches[0]),
                "manager_outcome": {
                    "adapter": "launchd",
                    "exit_status_observed": False,
                },
            }
        raise DeploymentConfigError("installed service adapter is invalid")
    except Exception as exc:  # noqa: BLE001 - status preserves readback failure.
        return {
            **envelope,
            "ok": False,
            "status": "invalid",
            "pid": None,
            "error": type(exc).__name__,
            "message": str(exc),
        }


def _supervisor_has_no_process(status: Any) -> bool:
    return bool(
        isinstance(status, Mapping)
        and status.get("ok") is True
        and status.get("status") in {"stopped", "failed"}
        and status.get("pid") is None
    )


def _environment_file_content(
    environment: DeploymentEnvironment,
    *,
    role_passwords: Mapping[str, str] | None = None,
) -> str:
    secret_ref = environment.database.dsn_secret_ref
    if not secret_ref.startswith("env:"):
        raise DeploymentConfigError(
            "environment database secret reference must use env: for supervision"
        )
    dsn_name = secret_ref.removeprefix("env:")
    for name, value in ((dsn_name, environment.database.dsn),):
        if not value or "\n" in value or "\r" in value:
            raise DeploymentConfigError(
                f"environment service secret {name!r} is empty or multiline"
            )
    passwords = dict(
        role_passwords
        or _environment_database_role_passwords(environment, create=False)
    )
    role_dsns = {
        "OKG_MCP_RO_DSN": make_conninfo(
            environment.database.dsn,
            user="okg_mcp",
            password=passwords["okg_mcp"],
        ),
        "OKG_MCP_RW_DSN": make_conninfo(
            environment.database.dsn,
            user="app_rw",
            password=passwords["app_rw"],
        ),
    }
    path_environment = [
        _env_assignment(name, value)
        for name, value in sorted({**environment.path_environment, **role_dsns}.items())
    ]
    credential_names = sorted({
        ref.removeprefix("env:")
        for refs in environment.runtime.credential_refs.values()
        for ref in refs
    })
    credential_environment: list[str] = []
    for name in credential_names:
        value = os.environ.get(name)
        if not value or "\n" in value or "\r" in value:
            raise DeploymentConfigError(
                f"declared service credential {name!r} is unavailable or multiline"
            )
        credential_environment.append(_env_assignment(name, value))
    return "\n".join(
        (
            _env_assignment(dsn_name, environment.database.dsn),
            *path_environment,
            *credential_environment,
            "PYTHONUNBUFFERED=1",
            "OKG_IVM_INTERVAL_VIEW=1",
            "",
        )
    )


def _environment_database_credentials_path(
    environment: DeploymentEnvironment,
) -> Path:
    return environment.runtime.state_root / "supervisor/database-role-credentials.json"


def _environment_database_role_passwords(
    environment: DeploymentEnvironment,
    *,
    create: bool,
) -> dict[str, str]:
    path = _environment_database_credentials_path(environment)
    if path.exists():
        if path.is_symlink() or not path.is_file():
            raise DeploymentConfigError(
                "environment database role credentials are not a regular file"
            )
        if stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise DeploymentConfigError(
                "environment database role credentials mode is not 0600"
            )
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise DeploymentConfigError(
                "environment database role credentials are unreadable"
            ) from exc
        passwords = payload.get("passwords") if isinstance(payload, dict) else None
        if (
            not isinstance(payload, dict)
            or payload.get("schema") != ENVIRONMENT_DATABASE_CREDENTIALS_SCHEMA
            or payload.get("environment_id") != environment.id
            or not isinstance(passwords, dict)
            or set(passwords) != set(ENVIRONMENT_DATABASE_ROLES)
            or any(
                not isinstance(value, str) or len(value) < 32
                for value in passwords.values()
            )
        ):
            raise DeploymentConfigError(
                "environment database role credentials are invalid or foreign"
            )
        return {role: str(passwords[role]) for role in ENVIRONMENT_DATABASE_ROLES}
    if not create:
        raise DeploymentConfigError(
            "environment database role credentials are missing; reinstall services"
        )
    passwords = {role: secrets.token_urlsafe(32) for role in ENVIRONMENT_DATABASE_ROLES}
    payload = {
        "schema": ENVIRONMENT_DATABASE_CREDENTIALS_SCHEMA,
        "environment_id": environment.id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "passwords": passwords,
    }
    _atomic_write(
        path,
        (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8"),
        mode=0o600,
    )
    return passwords


def _env_assignment(name: str, value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'{name}="{escaped}"'


def _require_install_path(
    environment: DeploymentEnvironment,
    path: Path,
) -> None:
    resolved = path.resolve()
    if resolved == environment.runtime.local_env_file:
        return
    try:
        resolved.relative_to(environment.runtime.state_root)
    except ValueError as exc:
        raise DeploymentConfigError(
            f"service artifact path is outside environment state: {resolved}"
        ) from exc


def _require_within(path: Path, root: Path, label: str) -> None:
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise DeploymentConfigError(
            f"service authority {label} is outside active artifact"
        ) from exc


def _launcher_source() -> Path:
    return Path(__file__).resolve().parent / "runtime" / "release_launcher.py"


def _environment_exec_source() -> Path:
    return Path(__file__).resolve().parent / "runtime" / "environment_exec.py"


def _database_password(environment: DeploymentEnvironment) -> str:
    password = unquote(urlsplit(environment.database.dsn).password or "")
    if not password or "\n" in password or "\r" in password:
        raise DeploymentConfigError(
            "environment database password is empty or multiline"
        )
    return password


def _service_plan_digest(plan: Mapping[str, Any]) -> str:
    identity = {
        "schema": plan.get("schema"),
        "environment_id": plan.get("environment_id"),
        "adapter": plan.get("adapter"),
        "systemd_scope": plan.get("systemd_scope"),
        "postgres_image": plan.get("postgres_image"),
        "launcher": plan.get("launcher"),
        "artifacts": plan.get("artifacts"),
        "topology": plan.get("topology"),
        "handoff_topology": plan.get("handoff_topology"),
        "commands": plan.get("commands"),
    }
    encoded = json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _is_sha256(value: Any) -> bool:
    if not isinstance(value, str) or not value.startswith("sha256:"):
        return False
    digest = value.removeprefix("sha256:")
    return len(digest) == 64 and all(char in "0123456789abcdef" for char in digest)


def _require_plan_release_snapshot(
    plan: Mapping[str, Any],
    *,
    observed_release: Mapping[str, Any],
) -> None:
    planned_release = plan.get("release_status")
    if not isinstance(planned_release, Mapping):
        raise DeploymentConfigError("service lifecycle plan has no release snapshot")
    fields = (
        "ok",
        "status",
        "artifact_id",
        "definition_digest",
        "active_pointer_digest",
        "previous_artifact_id",
        "previous_definition_digest",
        "previous_pointer_digest",
    )
    if any(
        planned_release.get(field) != observed_release.get(field)
        for field in fields
    ):
        raise DeploymentConfigError(
            "active release changed after the service plan was rendered"
        )


def _validate_service_plan(
    environment: DeploymentEnvironment,
    plan: Mapping[str, Any],
    *,
    release_status: Mapping[str, Any] | None = None,
) -> None:
    if plan.get("schema") != ENVIRONMENT_SERVICES_SCHEMA:
        raise DeploymentConfigError("service lifecycle plan has the wrong schema")
    if plan.get("environment_id") != environment.id:
        raise DeploymentConfigError(
            "service lifecycle plan belongs to a different environment"
        )
    observed_digest = _service_plan_digest(plan)
    if plan.get("plan_digest") != observed_digest:
        raise DeploymentConfigError("service lifecycle plan content was modified")
    launcher = plan.get("launcher") or {}
    expected = render_environment_services(
        environment,
        adapter=str(plan.get("adapter") or ""),
        launcher_python=str(launcher.get("python") or ""),
        systemd_scope=str(plan.get("systemd_scope") or "system"),
        postgres_image=str(plan.get("postgres_image") or ""),
        _release_status=release_status,
    )
    if expected.get("plan_digest") != observed_digest:
        raise DeploymentConfigError(
            "service lifecycle plan differs from current environment topology"
        )


def _file_digest(path: Path) -> str:
    return f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}"


def _systemd_unit(service_id: str) -> str:
    return service_id if service_id.endswith(".service") else f"{service_id}.service"


def _launchd_supervision_receipt_path(
    environment: DeploymentEnvironment,
    service_id: str,
) -> Path:
    if (
        not service_id
        or not service_id[0].isalnum()
        or any(char not in _SERVICE_ID_CHARS for char in service_id)
    ):
        raise DeploymentConfigError("launchd service identifier is invalid")
    return (
        environment.runtime.state_root
        / "supervisor"
        / "launchd-supervision"
        / f"{service_id}.json"
    )


def _command(argv: list[str], **metadata: Any) -> dict[str, Any]:
    return {"argv": argv, "display": shlex.join(argv), **metadata}


def _is_database_stop_command(argv: Sequence[str]) -> bool:
    values = [str(part) for part in argv]
    return bool(
        values
        and Path(values[0]).name in {"docker", "podman"}
        and "compose" in values
        and "stop" in values
        and values[-1] == "postgres"
    )


def _is_database_start_command(argv: Sequence[str]) -> bool:
    values = [str(part) for part in argv]
    return bool(
        values
        and Path(values[0]).name in {"docker", "podman"}
        and "compose" in values
        and "up" in values
        and "--detach" in values
        and values[-1] == "postgres"
    )


def _is_database_status_command(argv: Sequence[str]) -> bool:
    values = [str(part) for part in argv]
    return bool(
        values
        and Path(values[0]).name in {"docker", "podman"}
        and "compose" in values
        and "ps" in values
        and values[-1] == "postgres"
    )


def _service_stop_target_is_absent(argv: Sequence[str]) -> bool:
    values = [str(part) for part in argv]
    if (
        len(values) != 3
        or Path(values[0]).name != "launchctl"
        or values[1] != "bootout"
    ):
        return _systemd_stop_targets_are_inactive(values)
    try:
        result = _run_command([values[0], "print", values[2]])
    except OSError:
        return False
    return result.returncode == 113 and "Could not find service" in result.stderr


def _environment_host_services_stopped(plan: Mapping[str, Any]) -> bool:
    rows = [
        row
        for row in ((plan.get("commands") or {}).get("stop") or ())
        if isinstance(row, Mapping)
        and not _is_database_stop_command(row.get("argv") or ())
    ]
    return bool(rows) and all(
        _service_stop_target_is_absent(row.get("argv") or ()) for row in rows
    )


def _systemd_stop_targets_are_inactive(values: Sequence[str]) -> bool:
    if not values or Path(values[0]).name != "systemctl":
        return False
    try:
        action = values.index("disable")
    except ValueError:
        return False
    if action + 2 >= len(values) or values[action + 1] != "--now":
        return False
    prefix = list(values[:action])
    units = list(values[action + 2 :])
    if not units:
        return False
    try:
        for unit in units:
            active = _run_command([*prefix, "is-active", unit])
            enabled = _run_command([*prefix, "is-enabled", unit])
            if not (
                active.returncode == 3
                and active.stdout.strip() == "inactive"
                and enabled.returncode == 1
                and enabled.stdout.strip() == "disabled"
            ):
                return False
        return True
    except OSError:
        return False


def _run_command(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - argv is rendered from validated topology.
        list(argv),
        check=False,
        capture_output=True,
        text=True,
    )


def _host_owner_reaches_container() -> bool:
    """Does chowning a file on the HOST change what the container sees?

    The only owned service artifact is the postgres password file, and the
    reason it is chowned to the container's uid is so the container can
    read the secret. Host ownership is the MEANS; the container being able
    to read it is the end.

    On Linux those are the same thing: Docker shares the host kernel, uids
    are real, and without the chown the container genuinely cannot read the
    file. The chown is load-bearing and stays.

    On macOS they are not. Containers run inside Docker Desktop's VM and a
    bind mount is presented through a translation layer that reports the
    container's own uid regardless of who owns the file on the host.
    Measured on okg-workspace-production: the secret is 501:20 on the host,
    the container reads it as

        uid=1000(postgres) gid=1000(postgres)
        -rw------- 1 1000 1000 /run/secrets/postgres_password

    and Postgres has consumed it for weeks. So the host chown there buys
    nothing — while costing everything, because changing a file to a
    different uid requires root. It made `okg environment service install`
    a root-only command, and since `restart` requires a current install,
    it made a dead deployment unrecoverable without sudo.

    This is a declared rule rather than a swallowed PermissionError on
    purpose. Catching the error and carrying on would be a silent fallback:
    it would also skip the chown on Linux, where skipping it leaves a
    secret the container cannot read, and nothing would say so.
    """
    return sys.platform != "darwin"


def _atomic_write(
    path: Path,
    content: bytes,
    *,
    mode: int,
    owner: tuple[int, int] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        apply_owner = owner is not None and _host_owner_reaches_container()
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            if apply_owner:
                os.fchown(handle.fileno(), owner[0], owner[1])
            os.fchmod(handle.fileno(), mode)
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        if stat.S_IMODE(path.stat().st_mode) != mode:
            raise DeploymentConfigError(f"installed service mode mismatch: {path}")
        if apply_owner:
            observed = path.stat()
            if observed.st_uid != owner[0] or observed.st_gid != owner[1]:
                raise DeploymentConfigError(
                    f"installed service owner mismatch: {path}"
                )
    finally:
        temporary.unlink(missing_ok=True)
