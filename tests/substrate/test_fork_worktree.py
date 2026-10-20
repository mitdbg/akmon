"""Unit tests for the worktree fork-and-wire config logic
(PACT worktree-fork-wiring, req.worktree-fork-provisioned).

Covers the pure config wiring (.mcp.json + ~/.claude.json enablement), its
data-loss safety (atomic + fail-closed), unique per-path fork naming, and
path-based GC orphan detection. The Chronos fork creation + the session.py
read-through isolation are exercised by live checks, not here.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.cli.deployment_worktree import (
    fork_branch_name,
    orphaned_forks,
    wire_worktree_mcp,
)
from okg.substrate.cli.deployment_inventory import discover_mcp_registrations
from okg.substrate.errors import SubstrateError

DSN = "postgres://postgres:okg@127.0.0.1:55434/okg_workspace"


def _wire(wt: Path, claude: Path, *, server="okg-okg-workspace", branch="experiment:wt-x"):
    wt.mkdir(parents=True, exist_ok=True)
    return wire_worktree_mcp(
        wt, deployment="okg-workspace", branch=branch, dsn=DSN,
        server_name=server, claude_json_path=claude, apply=True,
    )


def test_wire_writes_branch_pinned_mcp_and_enables(tmp_path: Path) -> None:
    wt = tmp_path / "wt-x"
    claude = tmp_path / "claude.json"
    claude.write_text(json.dumps({"projects": {}}), encoding="utf-8")

    res = _wire(wt, claude)
    assert res["mcp_written"] is True and res["enabled_added"] is True

    spec = json.loads((wt / ".mcp.json").read_text())["mcpServers"]["okg-okg-workspace"]
    args = spec["args"]
    assert "mcp-serve" in args and "--branch" in args
    assert args[args.index("--branch") + 1] == "experiment:wt-x"
    assert args[args.index("--deployment") + 1] == "okg-workspace"
    assert spec["env"]["OKG_DSN"] == DSN

    cj = json.loads(claude.read_text())
    assert cj["projects"][str(wt)]["enabledMcpjsonServers"] == ["okg-okg-workspace"]

    regs = discover_mcp_registrations(
        claude_json_path=claude,
        project_mcp_paths=[wt / ".mcp.json"],
        project_key=str(wt),
    )
    assert regs["okg-workspace"]["scope"] == "project"
    assert regs["okg-workspace"]["enabled"] is True
    assert regs["okg-workspace"]["branch"] == "experiment:wt-x"


def test_wire_is_idempotent(tmp_path: Path) -> None:
    wt = tmp_path / "wt-y"
    claude = tmp_path / "claude.json"
    claude.write_text(json.dumps({"projects": {}}), encoding="utf-8")
    first = _wire(wt, claude, branch="experiment:wt-y")
    assert first["enabled_added"] is True
    second = _wire(wt, claude, branch="experiment:wt-y")
    assert second["mcp_written"] is False and second["enabled_added"] is False
    cj = json.loads(claude.read_text())
    assert cj["projects"][str(wt)]["enabledMcpjsonServers"] == ["okg-okg-workspace"]


def test_wire_requires_existing_worktree(tmp_path: Path) -> None:
    wt = tmp_path / "does-not-exist"
    claude = tmp_path / "claude.json"
    claude.write_text("{}", encoding="utf-8")
    with pytest.raises(SubstrateError):
        wire_worktree_mcp(
            wt, deployment="okg-workspace", branch="experiment:x", dsn=DSN,
            server_name="okg-okg-workspace", claude_json_path=claude, apply=True,
        )


def test_wire_preserves_rich_claude_json_no_data_loss(tmp_path: Path) -> None:
    """An existing config with real oauth/mcpServers/other projects must be
    fully preserved -- only the worktree project's enable list is touched."""
    wt = tmp_path / "wt-rich"
    claude = tmp_path / "claude.json"
    original = {
        "oauthAccount": {"token": "keepme"},
        "mcpServers": {"okg-jansci-42": {"command": "uv"}},
        "projects": {
            "/some/other/project": {"enabledMcpjsonServers": ["okg-other"], "x": 1},
        },
        "skillUsage": {"a": 2},
    }
    claude.write_text(json.dumps(original), encoding="utf-8")
    _wire(wt, claude)
    cj = json.loads(claude.read_text())
    # Everything unrelated survives.
    assert cj["oauthAccount"] == {"token": "keepme"}
    assert cj["mcpServers"] == {"okg-jansci-42": {"command": "uv"}}
    assert cj["skillUsage"] == {"a": 2}
    assert cj["projects"]["/some/other/project"] == {
        "enabledMcpjsonServers": ["okg-other"], "x": 1}
    # And the worktree project got enabled.
    assert cj["projects"][str(wt)]["enabledMcpjsonServers"] == ["okg-okg-workspace"]


def test_wire_aborts_on_corrupt_claude_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A corrupt existing ~/.claude.json must abort -- NEVER be overwritten with
    a stripped-down config (the data-loss footgun)."""
    wt = tmp_path / "wt-corrupt"
    wt.mkdir()
    claude = tmp_path / "claude.json"
    corrupt = '{"oauthAccount": {"token": "keepme"}, NOT VALID JSON'
    claude.write_text(corrupt, encoding="utf-8")
    class StaleJSONDecodeError(ValueError):
        pass

    monkeypatch.setattr(json, "JSONDecodeError", StaleJSONDecodeError)
    with pytest.raises(SubstrateError):
        wire_worktree_mcp(
            wt, deployment="okg-workspace", branch="experiment:x", dsn=DSN,
            server_name="okg-okg-workspace", claude_json_path=claude, apply=True,
        )
    # The corrupt file is left untouched, not clobbered.
    assert claude.read_text() == corrupt


def test_fork_branch_name_unique_per_path() -> None:
    """Worktrees that share a basename (e.g. the .../<hash>/okg Codex layout)
    must get DISTINCT fork branch names."""
    a = fork_branch_name(Path("/a/111/okg"))
    b = fork_branch_name(Path("/a/222/okg"))
    assert a != b
    assert a.startswith("experiment:okg-") and b.startswith("experiment:okg-")


def test_orphaned_forks_matches_recorded_worktree_paths() -> None:
    forks = [
        ("experiment:okg-aaa", "/live/wt-a"),
        ("experiment:okg-bbb", "/dead/wt-b"),
        ("experiment:okg-ccc", None),  # no recorded path -> never reaped
    ]
    live = {"/live/wt-a", "/repo/okg"}
    assert orphaned_forks(forks, live) == ["experiment:okg-bbb"]
    # A live worktree's fork is never reaped.
    assert orphaned_forks([("experiment:okg-aaa", "/live/wt-a")], live) == []
