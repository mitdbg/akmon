"""Unit tests for the `okg deployment list` host inventory
(PACT worktree-fork-wiring, req.host-inventory).

These exercise the pure enumeration/merge logic with injected config paths and
an unreachable DB, so they need no live deployment: the probe must yield
reachable=False without raising.
"""
from __future__ import annotations

import json
from pathlib import Path

import yaml

from okg.substrate.cli.deployment_inventory import (
    collect_deployment_inventory,
    discover_mcp_registrations,
)

UNREACHABLE_DSN = "postgres://okg_mcp:okg_mcp@127.0.0.1:59999/wf_test_db"

EXPECTED_KEYS = {
    "name", "database", "host", "port", "role", "has_manifest",
    "mcp_registered", "mcp_server", "mcp_scope", "mcp_enabled", "mcp_branch",
    "reachable", "node_count", "error",
}


def _write_manifest(root: Path, name: str, dsn: str) -> None:
    d = root / name
    d.mkdir(parents=True)
    (d / "schema").mkdir()
    (d / "source_registry.yaml").write_text("sources: []\n", encoding="utf-8")
    (d / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": name,
            "schema_dir": "schema",
            "source_registry": "source_registry.yaml",
            "postgres": {"dsn": dsn},
        }),
        encoding="utf-8",
    )


def _mcp_server(deployment: str, branch: str | None = None) -> dict:
    args = ["run", "okg", "mcp-serve", "--deployment", deployment]
    if branch:
        args += ["--branch", branch]
    return {"command": "uv", "args": args, "env": {"OKG_DSN": UNREACHABLE_DSN}}


def test_inventory_merges_manifest_and_mcp_sources(tmp_path: Path) -> None:
    deployments_root = tmp_path / "deployments"
    _write_manifest(deployments_root, "wf-manifest-only", UNREACHABLE_DSN)

    project_key = str(tmp_path / "proj")
    claude_json = tmp_path / "claude.json"
    claude_json.write_text(json.dumps({
        "mcpServers": {"okg-wf-global": _mcp_server("wf-global")},
        "projects": {project_key: {"enabledMcpjsonServers": []}},
    }), encoding="utf-8")

    project_mcp = tmp_path / "proj" / ".mcp.json"
    project_mcp.parent.mkdir(parents=True)
    project_mcp.write_text(json.dumps({
        "mcpServers": {"okg-wf-project": _mcp_server("wf-project", branch="experiment:wt1")},
    }), encoding="utf-8")

    out = collect_deployment_inventory(
        deployments_root=deployments_root,
        claude_json_path=claude_json,
        project_mcp_paths=[project_mcp],
        project_key=project_key,
        probe=True,
    )

    rows = {r["name"]: r for r in out["deployments"]}
    # Union of manifest-discovered and MCP-registered deployments.
    assert set(rows) == {"wf-manifest-only", "wf-global", "wf-project"}
    assert out["deployment_count"] == 3

    for r in out["deployments"]:
        assert set(r) == EXPECTED_KEYS, r
        # Unreachable DB must be handled gracefully, never raise.
        assert r["reachable"] is False
        assert r["node_count"] is None

    # Manifest-only deployment: present, no MCP.
    mo = rows["wf-manifest-only"]
    assert mo["has_manifest"] is True
    assert mo["mcp_registered"] is False
    assert mo["mcp_scope"] is None
    assert mo["mcp_enabled"] is False

    # Global MCP server: always enabled.
    g = rows["wf-global"]
    assert g["mcp_registered"] is True
    assert g["mcp_scope"] == "global"
    assert g["mcp_enabled"] is True
    assert g["mcp_server"] == "okg-wf-global"

    # Project MCP server not in enabledMcpjsonServers: registered but NOT enabled
    # (the approval gap that kept okg-workspace from loading).
    p = rows["wf-project"]
    assert p["mcp_registered"] is True
    assert p["mcp_scope"] == "project"
    assert p["mcp_enabled"] is False
    assert p["mcp_branch"] == "experiment:wt1"


def test_project_enable_flags_flip_enabled(tmp_path: Path) -> None:
    project_key = str(tmp_path / "proj")
    project_mcp = tmp_path / "proj" / ".mcp.json"
    project_mcp.parent.mkdir(parents=True)
    project_mcp.write_text(json.dumps({
        "mcpServers": {"okg-wf-project": _mcp_server("wf-project")},
    }), encoding="utf-8")

    # Enabled explicitly.
    claude_enabled = tmp_path / "enabled.json"
    claude_enabled.write_text(json.dumps({
        "projects": {project_key: {"enabledMcpjsonServers": ["okg-wf-project"]}},
    }), encoding="utf-8")
    regs = discover_mcp_registrations(
        claude_json_path=claude_enabled,
        project_mcp_paths=[project_mcp],
        project_key=project_key,
    )
    assert regs["wf-project"]["enabled"] is True

    # enableAllProjectMcpServers also flips it on.
    claude_all = tmp_path / "all.json"
    claude_all.write_text(json.dumps({
        "projects": {project_key: {"enableAllProjectMcpServers": True}},
    }), encoding="utf-8")
    regs_all = discover_mcp_registrations(
        claude_json_path=claude_all,
        project_mcp_paths=[project_mcp],
        project_key=project_key,
    )
    assert regs_all["wf-project"]["enabled"] is True


def test_no_probe_skips_connection(tmp_path: Path) -> None:
    deployments_root = tmp_path / "deployments"
    _write_manifest(deployments_root, "wf-x", UNREACHABLE_DSN)
    out = collect_deployment_inventory(
        deployments_root=deployments_root,
        claude_json_path=tmp_path / "missing.json",
        project_mcp_paths=[],
        project_key=str(tmp_path),
        probe=False,
    )
    row = out["deployments"][0]
    assert row["name"] == "wf-x"
    assert row["reachable"] is None  # not probed
