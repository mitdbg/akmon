"""Substrate-wide lifecycle verbs: `okg start/stop/restart`.

Per `dead-simple-install`: any deployment whose `deployment.yaml`
carries a `docker:` block (compose_path + project_name) can be
managed with these verbs. Profile-agnostic — academic-researcher, cms,
af_pubs, and future compose-backed deployments all use the same
verbs.

Deployments without a `docker:` block (e.g. one installed with
`--use-local-postgres`) get a clear error:
*"deployment <name> has no Docker stack registered; lifecycle
verbs don't apply."*

The verbs delegate to `docker compose -p <project> <verb>`. They
don't reimplement Docker semantics; they just translate the
operator's deployment name into the compose primitives.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path
from typing import Optional

import yaml
from okg.substrate import fastyaml


def _find_repo_root() -> Path:
    """Walk up from cwd looking for a `src/okg/` directory."""
    here = Path.cwd()
    for parent in [here, *here.parents]:
        if (parent / "src" / "okg").is_dir():
            return parent
    return Path(__file__).resolve().parents[3]


def _read_docker_block(deployment: str) -> Optional[dict]:
    """Load the resolved deployment's `docker:` block, or None
    if absent / unreadable."""
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        find_deployment_dir,
    )
    try:
        dep_dir = find_deployment_dir(deployment)
    except DeploymentResolutionError:
        dep_dir = None
    if dep_dir is None:
        repo_root = _find_repo_root()
        dep_dir = repo_root / "deployments" / deployment
    dy = dep_dir / "deployment.yaml"
    if not dy.is_file():
        return None
    try:
        raw = fastyaml.safe_load(dy.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return None
    block = raw.get("docker")
    if not isinstance(block, dict):
        return None
    return block


def _emit(payload: dict, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, indent=2))
    else:
        for k, v in payload.items():
            print(f"{k}: {v}")


def _fail_no_docker(deployment: str, *, as_json: bool) -> int:
    msg = (
        f"deployment {deployment!r} has no Docker stack "
        f"registered; lifecycle verbs don't apply. (No "
        f"`docker:` block in the resolved deployment.yaml — "
        f"was it installed with "
        f"`--use-local-postgres`?)"
    )
    _emit(
        {"status": "no_docker_block", "error": msg,
         "deployment": deployment},
        as_json=as_json,
    )
    return 1


def _compose_invoke(
    *,
    deployment: str,
    block: dict,
    verb: str,
    extra_args: Optional[list[str]] = None,
    capture: bool = False,
) -> subprocess.CompletedProcess:
    """Invoke `docker compose -f <compose_path> -p <project>
    <verb>` and return the completed process. The caller checks
    `returncode`. `verb` is one of: start, stop, restart, ps,
    port, …

    The compose file interpolates `${DEPLOYMENT_NAME}` +
    `${POSTGRES_PORT}` — we pass them in env so compose can
    resolve container_name / volume_name / port_mapping
    consistently. Without this, `docker compose start` warns
    about unset variables AND can target the wrong container.
    """
    import os
    compose_path = block.get("compose_path")
    project_name = block.get("project_name") or f"okg-{deployment}"
    host_port = block.get("host_port")
    if not compose_path:
        raise RuntimeError(
            "docker.compose_path missing from deployment.yaml"
        )
    cmd = [
        "docker", "compose",
        "-f", str(compose_path),
        "-p", str(project_name),
        verb,
    ]
    if extra_args:
        cmd.extend(extra_args)
    env = os.environ.copy()
    env["DEPLOYMENT_NAME"] = deployment
    # Preserve the assigned host port across lifecycle calls so
    # compose's `${POSTGRES_PORT}` interpolation produces the
    # same port mapping it did at install time.
    if host_port is not None:
        env["POSTGRES_PORT"] = str(host_port)
    # The compose file now REQUIRES POSTGRES_PASSWORD (the `?` form,
    # per harden-graph-store-exposure). Lifecycle verbs (start/stop/
    # restart/ps/port) never re-initialize the DB — the container's
    # password is already baked — so the value only needs to satisfy
    # interpolation, not match. A placeholder keeps these verbs
    # working without reading the real secret out of deployment.yaml.
    env.setdefault("POSTGRES_PASSWORD", "lifecycle-placeholder")
    return subprocess.run(
        cmd,
        check=False,
        capture_output=capture,
        env=env,
    )


def _ensure_docker_on_path(as_json: bool) -> Optional[int]:
    """Preflight: docker on PATH, daemon reachable. Returns exit
    code on failure, None on success."""
    if shutil.which("docker") is None:
        _emit(
            {
                "status": "docker_not_on_path",
                "error": (
                    "docker not on PATH; install Docker Desktop "
                    "from https://www.docker.com/products/"
                    "docker-desktop/ to use lifecycle verbs."
                ),
            },
            as_json=as_json,
        )
        return 1
    return None


# ---------- public command handlers ----------


def cmd_start(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg start --deployment <name>` — start the deployment's
    Docker stack. Idempotent: `docker compose up -d --wait`
    leaves an already-correct stack running and reconciles legacy
    ephemeral-port containers to the pinned host port in
    deployment.yaml."""
    from okg.substrate.deployments.admission import require_active_deployment
    from okg.substrate.errors import SubstrateError

    try:
        require_active_deployment(args.deployment)
    except SubstrateError as exc:
        _emit(exc.to_json(), as_json=as_json)
        return 1
    deployment = args.deployment
    rc = _ensure_docker_on_path(as_json)
    if rc is not None:
        return rc
    block = _read_docker_block(deployment)
    if block is None:
        return _fail_no_docker(deployment, as_json=as_json)
    proc = _compose_invoke(
        deployment=deployment, block=block, verb="up",
        extra_args=["-d", "--wait"],
    )
    if proc.returncode != 0:
        _emit(
            {
                "status": "start_failed",
                "deployment": deployment,
                "returncode": proc.returncode,
            },
            as_json=as_json,
        )
        return proc.returncode
    _emit(
        {"status": "started", "deployment": deployment},
        as_json=as_json,
    )
    return 0


def cmd_stop(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg stop --deployment <name>` — stop the deployment's
    Docker stack. Does NOT remove the volume; data persists."""
    deployment = args.deployment
    rc = _ensure_docker_on_path(as_json)
    if rc is not None:
        return rc
    block = _read_docker_block(deployment)
    if block is None:
        return _fail_no_docker(deployment, as_json=as_json)
    proc = _compose_invoke(
        deployment=deployment, block=block, verb="stop",
    )
    if proc.returncode != 0:
        _emit(
            {
                "status": "stop_failed",
                "deployment": deployment,
                "returncode": proc.returncode,
            },
            as_json=as_json,
        )
        return proc.returncode
    _emit(
        {"status": "stopped", "deployment": deployment},
        as_json=as_json,
    )
    return 0


def cmd_restart(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg restart --deployment <name>` — restart the deployment's
    Docker stack while preserving the pinned host port."""
    from okg.substrate.deployments.admission import require_active_deployment
    from okg.substrate.errors import SubstrateError

    try:
        require_active_deployment(args.deployment)
    except SubstrateError as exc:
        _emit(exc.to_json(), as_json=as_json)
        return 1
    deployment = args.deployment
    rc = _ensure_docker_on_path(as_json)
    if rc is not None:
        return rc
    block = _read_docker_block(deployment)
    if block is None:
        return _fail_no_docker(deployment, as_json=as_json)
    proc = _compose_invoke(
        deployment=deployment, block=block, verb="up",
        extra_args=["-d", "--wait", "--force-recreate"],
    )
    if proc.returncode != 0:
        _emit(
            {
                "status": "restart_failed",
                "deployment": deployment,
                "returncode": proc.returncode,
            },
            as_json=as_json,
        )
        return proc.returncode
    _emit(
        {"status": "restarted", "deployment": deployment},
        as_json=as_json,
    )
    return 0
