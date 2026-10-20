"""Cross-surface deployment database agreement (the DSN doctor).

Per `standardize-deployment-authority` req.deploy.agreement-doctor: the
local env file, deployment authority resolution, MCP registrations, and
running MCP server processes must all denote the SAME database for a
deployment. The 2026-06-10 incident - a publish into a worktree clone while
the MCP served a different instance entirely - is exactly what this check
turns from an afternoon of archaeology into one command.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

from okg.substrate.deployment_authority import (
    _declared_database,
    resolve_deployment_dsn_authority,
    resolved_target,
)


def gather_surfaces(deployment: str) -> list[dict[str, Any]]:
    """Collect every answer to "which database is <deployment>"."""
    expected = _declared_database(deployment)
    surfaces: list[dict[str, Any]] = []

    def add(surface: str, dsn: str | None, *, detail: str | None = None) -> None:
        target = resolved_target(
            dsn,
            deployment=deployment,
            authority_mode=surface,
            expected_database=expected,
        )
        entry = {"surface": surface, **target}
        if detail:
            entry["detail"] = detail
        surfaces.append(entry)

    resolution = resolve_deployment_dsn_authority(
        deployment, local_profile_authoritative=True
    )
    add(
        "deployment_authority",
        resolution.get("dsn"),
        detail=(resolution.get("authority") or {}).get("mode"),
    )

    add("process_env", os.environ.get("OKG_DSN"))

    from okg.substrate.local_env import configured_local_env_path, parse_local_env

    try:
        path = configured_local_env_path()
        values = parse_local_env(path) if path.is_file() else {}
        add("local_env_file", values.get("OKG_DSN"), detail=str(path))
    except Exception as exc:  # noqa: BLE001 - report, don't crash the doctor.
        surfaces.append({
            "surface": "local_env_file",
            "identity": "error",
            "detail": str(exc),
        })

    surfaces.extend(_mcp_registration_surfaces(deployment, expected))
    surfaces.extend(_running_server_surfaces(deployment, expected))
    return surfaces


def _mcp_registration_surfaces(
    deployment: str, expected: str | None
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    candidates: list[tuple[str, Path]] = []
    cwd_mcp = Path.cwd() / ".mcp.json"
    if cwd_mcp.is_file():
        candidates.append(("repo .mcp.json", cwd_mcp))
    user_cfg = Path.home() / ".claude.json"
    if user_cfg.is_file():
        candidates.append(("~/.claude.json", user_cfg))
    for label, path in candidates:
        try:
            cfg = json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        servers = dict(cfg.get("mcpServers") or {})
        for proj_cfg in (cfg.get("projects") or {}).values():
            servers.update(proj_cfg.get("mcpServers") or {})
        for name, server in servers.items():
            args = " ".join(server.get("args") or [])
            if (
                name != f"okg-{deployment}"
                and f"--deployment {deployment}" not in args
                and f"--deployment={deployment}" not in args
            ):
                continue
            dsn = (server.get("env") or {}).get("OKG_DSN")
            target = resolved_target(
                dsn,
                deployment=deployment,
                authority_mode="mcp_registration",
                expected_database=expected,
            )
            if not dsn:
                target["identity"] = "resolves_at_launch"
            out.append({
                "surface": "mcp_registration",
                "detail": f"{label}: {name}",
                **target,
            })
    return out


def _running_server_surfaces(
    deployment: str, expected: str | None
) -> list[dict[str, Any]]:
    """Ask live mcp-serve processes what DSN they actually hold."""
    out: list[dict[str, Any]] = []
    try:
        listing = subprocess.run(
            ["ps", "axeww"], capture_output=True, text=True, timeout=10,
        ).stdout
    except Exception:  # noqa: BLE001
        return out
    pattern = re.compile(
        rf"mcp-serve\s+--deployment[= ]{re.escape(deployment)}\b"
    )
    for line in listing.splitlines():
        if not pattern.search(line):
            continue
        match = re.search(r"OKG_DSN=(\S+)", line)
        pid = line.split(None, 1)[0]
        out.append({
            "surface": "running_mcp_server",
            "detail": f"pid {pid}",
            **resolved_target(
                match.group(1) if match else None,
                deployment=deployment,
                authority_mode="running_mcp_server",
                expected_database=expected,
            ),
        })
    return out


def agreement_report(
    deployment: str, surfaces: list[dict[str, Any]]
) -> dict[str, Any]:
    """Pure comparison: do all resolved surfaces denote one database?"""
    resolved = [
        s for s in surfaces if s.get("database") and s.get("host")
    ]
    tuples = {
        (s["host"], s.get("port"), s["database"]) for s in resolved
    }
    divergent: list[dict[str, Any]] = []
    if len(tuples) > 1:
        for s in resolved:
            divergent.append({
                "surface": s["surface"],
                "detail": s.get("detail"),
                "database": s["database"],
                "host": s["host"],
                "port": s.get("port"),
            })
    ok = len(tuples) == 1 and bool(resolved)
    report: dict[str, Any] = {
        "check": "deployment_dsn_agreement",
        "deployment": deployment,
        "ok": ok,
        "resolved_surface_count": len(resolved),
        "distinct_databases": len(tuples),
        "surfaces": surfaces,
        "divergent": divergent,
    }
    if not resolved:
        report["reason"] = "no surface resolves a database at all"
    elif not ok:
        agreed = sorted(tuples)
        report["reason"] = (
            f"{len(tuples)} different databases claim to be "
            f"{deployment}: " + "; ".join(
                f"{db}@{host}:{port}" for host, port, db in agreed
            )
        )
        report["repair"] = (
            "pick the canonical database, declare it as postgres.database "
            f"in deployments/{deployment}/deployment.yaml, and repoint the "
            "divergent surfaces (local.env / MCP registration) at it"
        )
    return report


def run_agreement_check(deployment: str) -> dict[str, Any]:
    return agreement_report(deployment, gather_surfaces(deployment))
