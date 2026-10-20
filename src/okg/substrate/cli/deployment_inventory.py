"""Host-wide OKG deployment inventory for ``okg deployment list``.

Enumerates every deployment resolvable on the host -- both manifest-discovered
and MCP-registered -- with its database/port/role, MCP registration state
(scope + whether it is actually enabled/loaded), reachability, and live
node_count. This is the command that disambiguates *which MCP server points at
which graph*: the absence of it is what let a wrong deployment's MCP get used.

The probe connects as the least-privilege ``okg_mcp`` role and reads through the
MCP read path (``open_session`` + ``inspect_graph``), so a green row here also
confirms the least-privilege read path works for that deployment/branch
(PACT ``fix-mcp-read-least-privilege``).
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional


def _load_json(path: Optional[Path]) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 -- a missing/garbled config file must not fail the scan.
        return {}


def _arg_value(args: list, flag: str) -> Optional[str]:
    """Return the value following ``flag`` in an argv-style list."""
    for i, a in enumerate(args or []):
        if a == flag and i + 1 < len(args):
            return args[i + 1]
    return None


def _deployment_from_mcp_spec(spec: dict) -> Optional[str]:
    """Extract the ``--deployment`` an okg mcp-serve server targets."""
    args = spec.get("args") or []
    if "mcp-serve" not in args:
        return None
    return _arg_value(args, "--deployment")


def _branch_from_mcp_spec(spec: dict) -> Optional[str]:
    return _arg_value(spec.get("args") or [], "--branch")


def _project_mcp_enabled(
    claude_cfg: dict, project_key: Optional[str], server_name: str
) -> bool:
    """A project-scoped MCP server loads only if approved in ~/.claude.json."""
    if not project_key:
        return False
    proj = (claude_cfg.get("projects") or {}).get(project_key) or {}
    if proj.get("enableAllProjectMcpServers") is True:
        return True
    return server_name in (proj.get("enabledMcpjsonServers") or [])


def discover_mcp_registrations(
    *,
    claude_json_path: Optional[Path],
    project_mcp_paths: list[Path],
    project_key: Optional[str],
) -> dict[str, dict[str, Any]]:
    """Map deployment_name -> {server, scope, enabled, branch} from MCP config.

    Global (user-scope) servers in ~/.claude.json always load; project-scope
    servers in a .mcp.json load only when enabled for the project. A global
    registration wins over a project one (it's the one that actually loads).
    """
    claude_cfg = _load_json(claude_json_path)
    regs: dict[str, dict[str, Any]] = {}
    for name, spec in (claude_cfg.get("mcpServers") or {}).items():
        dep = _deployment_from_mcp_spec(spec or {})
        if dep:
            regs[dep] = {
                "server": name, "scope": "global", "enabled": True,
                "branch": _branch_from_mcp_spec(spec or {}),
                "dsn": (spec.get("env") or {}).get("OKG_DSN"),
            }
    for mcp_path in project_mcp_paths:
        for name, spec in (_load_json(mcp_path).get("mcpServers") or {}).items():
            dep = _deployment_from_mcp_spec(spec or {})
            if dep and dep not in regs:
                regs[dep] = {
                    "server": name, "scope": "project",
                    "enabled": _project_mcp_enabled(claude_cfg, project_key, name),
                    "branch": _branch_from_mcp_spec(spec or {}),
                    "dsn": (spec.get("env") or {}).get("OKG_DSN"),
                }
    return regs


def _dsn_target(dsn: Optional[str]) -> dict[str, Any]:
    if not dsn:
        return {"database": None, "host": None, "port": None, "user": None}
    try:
        from psycopg.conninfo import conninfo_to_dict
        d = conninfo_to_dict(dsn)
        return {
            "database": d.get("dbname"), "host": d.get("host"),
            "port": d.get("port"), "user": d.get("user"),
        }
    except Exception:  # noqa: BLE001
        return {"database": None, "host": None, "port": None, "user": None}


def _probe(deployment: str, base_dsn: str, branch: Optional[str]) -> dict[str, Any]:
    """Connect as okg_mcp and read node_count via the MCP read path.

    Returns reachable + node_count + a short error. Never raises: an unreachable
    deployment yields reachable=False rather than failing the whole inventory.
    """
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.mcp.server import _derive_role_dsn
    from okg.substrate.mcp.session import open_session
    from okg.substrate.mcp.tools import inspect_graph
    try:
        mcp_dsn = _derive_role_dsn(base_dsn, role="okg_mcp", env_var="OKG_MCP_DSN")
    except Exception as exc:  # noqa: BLE001
        return {"reachable": False, "node_count": None, "edge_count": None,
                "error": f"derive_role_dsn: {exc}"[:160]}
    try:
        with db_connect(mcp_dsn, connect_timeout=5, autocommit=True) as conn:
            session = open_session(
                conn, deployment_name=deployment, branch_name=branch or "default")
            g = inspect_graph(conn, session)
            return {"reachable": True, "node_count": g.get("node_count"),
                    "edge_count": g.get("edge_count"), "error": None}
    except Exception as exc:  # noqa: BLE001
        return {"reachable": False, "node_count": None, "edge_count": None,
                "error": str(exc).strip().splitlines()[0][:160] if str(exc).strip() else "unreachable"}


def collect_deployment_inventory(
    *,
    deployments_root: Optional[Path] = None,
    claude_json_path: Optional[Path] = None,
    project_mcp_paths: Optional[list[Path]] = None,
    project_key: Optional[str] = None,
    probe: bool = True,
) -> dict[str, Any]:
    """Enumerate every deployment (manifest-discovered UNION MCP-registered)."""
    from okg.substrate.db.management import discover_manifest_databases
    from okg.substrate.dsn_form import (
        is_keyword_form_dsn,
        keyword_form_refusal,
    )
    from okg.substrate.deployment_authority import (
        deployment_self_hosted_authoritative,
        resolve_deployment_dsn_authority,
    )

    if claude_json_path is None:
        claude_json_path = Path(os.path.expanduser("~/.claude.json"))
    if project_mcp_paths is None:
        cwd = Path.cwd()
        project_mcp_paths = [cwd / ".mcp.json"]
        if project_key is None:
            project_key = str(cwd)

    mcp_regs = discover_mcp_registrations(
        claude_json_path=claude_json_path,
        project_mcp_paths=project_mcp_paths,
        project_key=project_key,
    )
    manifests = {
        m.deployment: m
        for m in discover_manifest_databases(deployments_root=deployments_root)
    }
    names = sorted(set(manifests) | set(mcp_regs))

    rows: list[dict[str, Any]] = []
    for name in names:
        reg = mcp_regs.get(name)
        base_dsn: Optional[str] = None
        refused_reason: Optional[str] = None
        try:
            res = resolve_deployment_dsn_authority(
                name,
                local_profile_authoritative=deployment_self_hosted_authoritative(name),
            )
            base_dsn = res.get("dsn")
            if (res.get("authority") or {}).get("dsn_form") == "libpq_keyword":
                # The chain refused this deployment's DSN for its spelling.
                # The manifest fallback below would re-adopt the very string
                # it refused and then try to connect with it, so stop here
                # and report the refusal instead of a bare "unresolved DSN".
                refused_reason = str(res.get("reason"))
        except Exception:  # noqa: BLE001
            base_dsn = None
        if not base_dsn and not refused_reason and name in manifests:
            base_dsn = manifests[name].dsn
        if not base_dsn and not refused_reason and reg:
            base_dsn = reg.get("dsn")  # MCP-only deployment: use its server's env DSN
        if base_dsn and is_keyword_form_dsn(base_dsn):
            # Whatever source supplied it -- authority chain, manifest
            # fallback, or the MCP registration -- a keyword/value DSN is
            # one `_probe` would hand to Chronos, which refuses it. Report
            # the refusal instead of attempting the connection.
            refused_reason = keyword_form_refusal(
                f"deployment {name!r}'s DSN", base_dsn,
            )
            base_dsn = None
        target = _dsn_target(base_dsn)

        row: dict[str, Any] = {
            "name": name,
            "database": target["database"]
            or (manifests[name].database if name in manifests else None),
            "host": target["host"],
            "port": target["port"],
            "role": "okg_mcp",
            "has_manifest": name in manifests,
            "mcp_registered": reg is not None,
            "mcp_server": reg["server"] if reg else None,
            "mcp_scope": reg["scope"] if reg else None,
            "mcp_enabled": bool(reg["enabled"]) if reg else False,
            "mcp_branch": reg["branch"] if reg else None,
            "reachable": None,
            "node_count": None,
            "error": (
                refused_reason or (None if base_dsn else "unresolved DSN")
            ),
        }
        if probe and base_dsn:
            p = _probe(name, base_dsn, row["mcp_branch"])
            row["reachable"] = p["reachable"]
            row["node_count"] = p["node_count"]
            row["error"] = p["error"]
        rows.append(row)

    return {"deployment_count": len(rows), "deployments": rows}
