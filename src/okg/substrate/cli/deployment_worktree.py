"""Fork-and-wire a git worktree to its own OKG Chronos branch
(PACT worktree-fork-wiring, req.worktree-fork-provisioned).

``okg deployment fork-worktree`` idempotently: (1) creates an experiment branch
``experiment:<wt>-<path-token>`` off the deployment's default branch (the
path-token keeps forks unique even when worktree basenames collide, e.g. the
``.../<hash>/okg`` Codex layout), (2) writes the worktree's ``.mcp.json`` pinning
an okg MCP server to that branch, and (3) enables that server in the worktree
project's ``enabledMcpjsonServers`` in ``~/.claude.json`` so Claude Code actually
loads it -- closing the approval gap.

Config writes are atomic and fail-closed: a corrupt existing ``~/.claude.json``
aborts the edit (never overwritten with a stripped-down config), writes go
through ``atomic_write_text``, and the global-config edit holds an advisory lock
so concurrent fork-worktree runs don't lose each other's enablement.
"""
from __future__ import annotations

import contextlib
import hashlib
import json
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.library.agent_runtimes import atomic_write_text


def _read_config(path: Path, *, label: str) -> dict[str, Any]:
    """Load a JSON config dict. Returns {} for a missing/empty file, but RAISES
    SubstrateError for an existing non-empty file that fails to parse -- so a
    writer never clobbers a config it couldn't read."""
    if not path.is_file():
        return {}
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        return {}
    try:
        loaded = json.loads(text)
    # JSONDecodeError is a ValueError. Catch the stable builtin superclass so
    # a reloaded json.decoder cannot make the exported exception alias stale.
    except ValueError as exc:
        raise SubstrateError(
            f"{label} at {path} is not valid JSON; aborting to avoid "
            f"overwriting it ({exc})",
            fix_hint="fix or remove the file, then retry",
        ) from exc
    return loaded if isinstance(loaded, dict) else {}


@contextlib.contextmanager
def _file_lock(path: Path) -> Iterator[None]:
    """Best-effort advisory lock around a config read-modify-write, so
    concurrent fork-worktree runs serialize on the shared ~/.claude.json."""
    lock_path = path.with_name(path.name + ".lock")
    try:
        import fcntl
    except Exception:  # noqa: BLE001 -- no fcntl (non-POSIX): proceed unlocked.
        yield
        return
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "w", encoding="utf-8")  # noqa: SIM115
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        finally:
            fh.close()


def _path_token(worktree_path: Path) -> str:
    """Short stable token from a worktree's absolute path -- keeps fork branch
    names unique even when basenames collide across worktrees."""
    abspath = str(worktree_path.resolve())
    return hashlib.sha1(abspath.encode("utf-8")).hexdigest()[:8]


def fork_branch_name(worktree_path: Path) -> str:
    return f"experiment:{worktree_path.name}-{_path_token(worktree_path)}"


def _mcp_server_spec(
    worktree_path: Path, deployment: str, branch: str, dsn: Optional[str], command: str
) -> dict[str, Any]:
    return {
        "command": command,
        "args": [
            "--directory", str(worktree_path), "run", "--extra", "mcp",
            "okg", "mcp-serve", "--deployment", deployment, "--branch", branch,
        ],
        "env": {"OKG_DSN": dsn} if dsn else {},
    }


def wire_worktree_mcp(
    worktree_path: Path,
    *,
    deployment: str,
    branch: str,
    dsn: Optional[str],
    server_name: str,
    command: str = "uv",
    claude_json_path: Optional[Path] = None,
    apply: bool = True,
) -> dict[str, Any]:
    """Write the worktree's branch-pinned .mcp.json and enable it for the
    worktree project in ~/.claude.json. Pure config; idempotent; never DDLs.
    Atomic + fail-closed (see module docstring). Returns what changed."""
    worktree_path = Path(worktree_path)
    if apply and not worktree_path.is_dir():
        raise SubstrateError(
            f"worktree path does not exist: {worktree_path}",
            fix_hint="pass --worktree-path to an existing git worktree directory",
        )
    server = _mcp_server_spec(worktree_path, deployment, branch, dsn, command)

    # --- worktree-local .mcp.json (no cross-process contention) ---
    mcp_path = worktree_path / ".mcp.json"
    existing_mcp = _read_config(mcp_path, label="worktree .mcp.json")
    servers = dict(existing_mcp.get("mcpServers") or {})
    mcp_changed = servers.get(server_name) != server
    servers[server_name] = server
    new_mcp = dict(existing_mcp)
    new_mcp["mcpServers"] = servers

    if claude_json_path is None:
        import os
        claude_json_path = Path(os.path.expanduser("~/.claude.json"))
    project_key = str(worktree_path)

    if apply:
        atomic_write_text(mcp_path, json.dumps(new_mcp, indent=2) + "\n")

    # --- shared ~/.claude.json enablement (locked read-modify-write) ---
    def _enable() -> bool:
        claude = _read_config(claude_json_path, label="~/.claude.json")
        projects = dict(claude.get("projects") or {})
        proj = dict(projects.get(project_key) or {})
        enabled = list(proj.get("enabledMcpjsonServers") or [])
        changed = server_name not in enabled
        if changed:
            enabled.append(server_name)
        proj["enabledMcpjsonServers"] = enabled
        projects[project_key] = proj
        new_claude = dict(claude)
        new_claude["projects"] = projects
        if apply:
            atomic_write_text(claude_json_path, json.dumps(new_claude, indent=2) + "\n")
        return changed

    if apply:
        with _file_lock(claude_json_path):
            enable_changed = _enable()
    else:
        enable_changed = _enable()

    return {
        "mcp_json_path": str(mcp_path),
        "server_name": server_name,
        "branch": branch,
        "project_key": project_key,
        "mcp_written": mcp_changed,
        "enabled_added": enable_changed,
        "claude_json_path": str(claude_json_path),
    }


def ensure_fork(
    dsn: str, deployment: str, branch: str, base_branch: str,
    *, worktree_path: Optional[str] = None,
) -> str:
    """Create experiment branch `branch` off `base_branch`, idempotently.
    Returns 'created' | 'exists'. Requires a write-capable DSN. Records the
    worktree path in branch metadata so GC can match forks to live worktrees."""
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.graph_store import ChronosGraphStore

    with db_connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        try:
            store.context.get_branch(branch)
            return "exists"
        except Exception as exc:  # noqa: BLE001
            if type(exc).__name__ != "BranchNotFoundError":
                raise  # a real DB error must not be read as "branch absent"
        base_cp = store.latest_generation(
            deployment_name=deployment, branch=base_branch, status="published")
        base_gen = base_cp.checkpoint_id if base_cp is not None else None
        try:
            store.create_branch(
                branch,
                from_branch=base_branch,
                metadata={
                    "kind": "okg_branch", "deployment_name": deployment,
                    "branch_name": branch, "role": "experiment", "status": "active",
                    "base_branch": base_branch, "base_generation_id": base_gen,
                    "worktree_path": worktree_path,
                    "created_by": "okg deployment fork-worktree",
                },
            )
            return "created"
        except Exception as exc:  # noqa: BLE001 -- tolerate a concurrent create.
            if type(exc).__name__ == "BranchAlreadyExistsError":
                return "exists"
            raise


def fork_worktree(
    *,
    worktree_path: Path,
    dsn: str,
    deployment: str,
    base_branch: str = "default",
    branch: Optional[str] = None,
    server_dsn: Optional[str] = None,
    server_name: Optional[str] = None,
    command: str = "uv",
    claude_json_path: Optional[Path] = None,
    apply: bool = True,
) -> dict[str, Any]:
    """Fork the deployment for one worktree and wire its branch-pinned MCP."""
    worktree_path = Path(worktree_path)
    if apply and not worktree_path.is_dir():
        raise SubstrateError(
            f"worktree path does not exist: {worktree_path}",
            fix_hint="pass --worktree-path to an existing git worktree directory",
        )
    branch = branch or fork_branch_name(worktree_path)
    server_name = server_name or f"okg-{deployment}"

    fork_state = (
        ensure_fork(dsn, deployment, branch, base_branch,
                    worktree_path=str(worktree_path.resolve()))
        if apply else "skipped"
    )
    wiring = wire_worktree_mcp(
        worktree_path,
        deployment=deployment,
        branch=branch,
        dsn=server_dsn or dsn,
        server_name=server_name,
        command=command,
        claude_json_path=claude_json_path,
        apply=apply,
    )
    return {
        "worktree": str(worktree_path),
        "deployment": deployment,
        "branch": branch,
        "fork": fork_state,
        "wiring": wiring,
    }


def orphaned_forks(
    forks: list[tuple[str, Optional[str]]],
    live_worktree_paths: set[str] | list[str],
) -> list[str]:
    """Return fork branch names whose recorded worktree path is not a live
    worktree. `forks` is a list of (branch_name, recorded_worktree_path). A fork
    with no recorded path is NEVER reaped (can't safely match it)."""
    live = {str(Path(p).resolve()) for p in live_worktree_paths}
    out: list[str] = []
    for name, wt in forks:
        if not wt:
            continue
        if str(Path(wt).resolve()) not in live:
            out.append(name)
    return out


def live_worktree_paths(repo_path: Path) -> set[str]:
    """Absolute paths of every live git worktree of the repo (incl. main checkout)."""
    import subprocess
    try:
        res = subprocess.run(
            ["git", "-C", str(repo_path), "worktree", "list", "--porcelain"],
            capture_output=True, text=True, timeout=15,
        )
    except Exception:  # noqa: BLE001 -- no git / not a repo: treat as no live worktrees.
        return set()
    paths: set[str] = set()
    prefix = "worktree "
    for line in res.stdout.splitlines():
        if line.startswith(prefix):
            paths.add(str(Path(line[len(prefix):].strip()).resolve()))
    return paths


def gc_forks(
    *, dsn: str, deployment: str, repo_path: Path, apply: bool = False
) -> dict[str, Any]:
    """Reap experiment forks whose recorded worktree no longer exists. Dry-run
    unless apply=True; abandoning flips branch metadata status to 'abandoned'."""
    import datetime as _dt

    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.graph_store import ChronosGraphStore

    live = live_worktree_paths(Path(repo_path))
    forks: list[tuple[str, Optional[str]]] = []
    infos: dict[str, Any] = {}
    with db_connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        for info in store.context.list_branches():
            md = info.metadata or {}
            name = md.get("branch_name") or getattr(info, "branch_id", None)
            if (
                name
                and md.get("deployment_name") == deployment
                and md.get("role") == "experiment"
                and md.get("status") == "active"
            ):
                forks.append((name, md.get("worktree_path")))
                infos[name] = info
        orphaned = orphaned_forks(forks, live)
        abandoned: list[str] = []
        if apply:
            for name in orphaned:
                md = dict(infos[name].metadata or {})
                md["status"] = "abandoned"
                md["closed_at"] = _dt.datetime.now(_dt.timezone.utc).isoformat()
                md["abandoned_by"] = "okg deployment gc-forks"
                store.context.update_branch_metadata(name, md)
                abandoned.append(name)

    return {
        "deployment": deployment,
        "live_worktree_count": len(live),
        "active_fork_count": len(forks),
        "orphaned": orphaned,
        "abandoned": abandoned,
        "applied": apply,
    }
