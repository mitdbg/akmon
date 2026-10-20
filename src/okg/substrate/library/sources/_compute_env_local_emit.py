"""LocalEnvSource — probe the local machine for compute env metadata.

Runs a small set of subprocess probes (uname, version queries) plus
parses ``~/.claude/settings.json`` for MCP server inventory. Emits
one ``Machine`` node, one ``Runtime`` per detected runtime, and one
``MCPServer`` per configured server.

Design:

- Probes that fail produce no node for that probe but never crash
  the source (a missing `node` binary is normal; a missing `git`
  is suspicious but we still proceed).
- Incremental: store sha256 of all probe outputs in ``state``;
  short-circuit when unchanged.
- Conforms to source-class config contract (dataclass with explicit
  fields).
"""

from __future__ import annotations

import hashlib
import json
import logging
import platform
import socket
import subprocess  # nosec B404
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from okg.substrate.library.sources.source_specs import EdgeSpec, NodeSpec

logger = logging.getLogger(__name__)


# Runtimes we know how to probe. Each is `(name, command, parse_fn)`
# where parse_fn takes the command's stdout and returns a version
# string (or None if unparseable).
def _parse_python_version(out: str) -> str | None:
    # "Python 3.12.7" → "3.12.7"
    parts = out.strip().split()
    return parts[1] if len(parts) >= 2 else None


def _parse_node_version(out: str) -> str | None:
    # "v20.10.0" → "20.10.0"
    return out.strip().lstrip("v") or None


def _parse_git_version(out: str) -> str | None:
    # "git version 2.45.0" → "2.45.0"
    parts = out.strip().split()
    return parts[2] if len(parts) >= 3 else None


_RUNTIME_PROBES = [
    ("python", ["python3", "--version"], _parse_python_version),
    ("node", ["node", "--version"], _parse_node_version),
    ("git", ["git", "--version"], _parse_git_version),
    ("uv", ["uv", "--version"], _parse_node_version),
]


@dataclass
class LocalEnvSource:
    """Probe-based source for the local compute environment.

    Parameters
    ----------
    settings_path : Path
        Path to Claude Code's settings.json (default
        ``~/.claude/settings.json``). Used to enumerate configured MCP
        servers. Missing file is fine — no MCP server nodes emitted.
    hostname_override : str | None
        Override the detected hostname (useful for tests + multi-host
        deployments where the agent runs on multiple machines and you
        want them distinguished by name rather than `socket.gethostname()`).
    """

    settings_path: Path = field(
        default_factory=lambda: Path("~/.claude/settings.json").expanduser()
    )
    hostname_override: str | None = None
    state: dict[str, Any] = field(default_factory=dict)
    _emit_nodes: list[NodeSpec] | None = None
    _emit_edges: list[EdgeSpec] | None = None
    _emit_aliases: dict[str, str] = field(default_factory=dict)

    ownership_id: str = "compute_env.local"
    supports_incremental: bool = True
    run_state_kind: str = (
        "metadata"  # bind_run_state passes us state.metadata as a dict
    )

    def __post_init__(self) -> None:
        if isinstance(self.settings_path, str):
            self.settings_path = Path(self.settings_path).expanduser()

    # --- Source protocol ----------------------------------------------------

    def nodes(self) -> Iterable[NodeSpec]:
        if self._emit_nodes is None:
            self._compute_emit()
        yield from self._emit_nodes or []

    def edges(self) -> Iterable[EdgeSpec]:
        if self._emit_edges is None:
            self._compute_emit()
        yield from self._emit_edges or []

    def aliases(self) -> dict[str, str]:
        if self._emit_nodes is None:
            self._compute_emit()
        return dict(self._emit_aliases)

    # --- State hooks --------------------------------------------------------

    def set_run_state(self, state: Any) -> None:
        self.state = dict(state) if state else {}

    def get_run_state(self) -> dict[str, Any]:
        return dict(self.state)

    @property
    def stale_node_ids(self) -> list[str]:
        return []

    def probe_state_hash(self) -> str:
        """Cheap content/state digest of the local env (issue #323 change-probe).

        Exactly the ``probe_hash`` the source short-circuits emission on -- a
        sha256 over hostname + os_release + arch + runtime versions + parsed
        settings.json MCP servers -- computed WITHOUT the full node/edge emit, so
        the runner gate can skip a no-change run. No wall-clock input, so it is a
        sound change-identity (it advances iff an emitted fact would change).
        """
        return _hash_probes(self._probe())

    # --- Implementation -----------------------------------------------------

    def _compute_emit(self) -> None:
        # Probe everything once, hash the result; short-circuit if
        # unchanged from prior run.
        probe_data = self._probe()
        probe_hash = _hash_probes(probe_data)
        if self.state.get("probe_hash") == probe_hash:
            self._emit_nodes = []
            self._emit_edges = []
            return

        nodes: list[NodeSpec] = []
        edges: list[EdgeSpec] = []
        aliases: dict[str, str] = {}

        # Machine node
        hostname = probe_data["hostname"]
        machine_id = f"machine:{hostname}"
        nodes.append(
            NodeSpec(
                node_id=machine_id,
                subtype="machine",
                label=hostname,
                attrs={
                    "hostname": hostname,
                    "os_family": probe_data["os_family"],
                    "os_release": probe_data["os_release"],
                    "arch": probe_data["arch"],
                },
                source=self.ownership_id,
            )
        )
        aliases[hostname] = machine_id

        # Runtime nodes
        for runtime in probe_data["runtimes"]:
            runtime_id = f"runtime:{hostname}:{runtime['name']}:{runtime['version']}"
            nodes.append(
                NodeSpec(
                    node_id=runtime_id,
                    subtype="runtime",
                    label=f"{runtime['name']} {runtime['version']}",
                    attrs={
                        "name": runtime["name"],
                        "version": runtime["version"],
                    },
                    source=self.ownership_id,
                )
            )
            edges.append(EdgeSpec(src=machine_id, dst=runtime_id, edge_type="contains"))

        # MCPServer nodes
        for server in probe_data["mcp_servers"]:
            mcp_id = f"mcp:{hostname}:{server['name']}"
            attrs: dict[str, Any] = {"name": server["name"]}
            if server.get("command"):
                attrs["command"] = server["command"]
            if server.get("transport"):
                attrs["transport"] = server["transport"]
            nodes.append(
                NodeSpec(
                    node_id=mcp_id,
                    subtype="mcp_server",
                    label=server["name"],
                    attrs=attrs,
                    source=self.ownership_id,
                )
            )
            edges.append(EdgeSpec(src=machine_id, dst=mcp_id, edge_type="contains"))

        self._emit_nodes = nodes
        self._emit_edges = edges
        self._emit_aliases = aliases
        self.state = {"probe_hash": probe_hash}

    def _probe(self) -> dict[str, Any]:
        """Run all probes and return a structured dict."""
        hostname = self.hostname_override or socket.gethostname()
        os_family = _detect_os_family()
        os_release = platform.platform()
        arch = platform.machine()

        runtimes = []
        for name, cmd, parse in _RUNTIME_PROBES:
            version = _try_probe(cmd, parse)
            if version:
                runtimes.append({"name": name, "version": version})

        mcp_servers = self._parse_mcp_settings()

        return {
            "hostname": hostname,
            "os_family": os_family,
            "os_release": os_release,
            "arch": arch,
            "runtimes": runtimes,
            "mcp_servers": mcp_servers,
        }

    def _parse_mcp_settings(self) -> list[dict[str, Any]]:
        """Parse settings.json for MCP server entries."""
        if not self.settings_path.is_file():
            return []
        try:
            data = json.loads(self.settings_path.read_text())
        except (OSError, json.JSONDecodeError) as e:
            logger.warning("Cannot parse %s: %s", self.settings_path, e)
            return []
        # Two common shapes:
        # 1. Top-level "mcpServers": {name: {command, args, ...}}
        # 2. Nested under "claude.mcpServers"
        servers_dict = data.get("mcpServers") or {}
        if not servers_dict:
            servers_dict = (data.get("claude") or {}).get("mcpServers") or {}
        out = []
        for name, cfg in (servers_dict or {}).items():
            if not isinstance(cfg, dict):
                continue
            command = cfg.get("command")
            args = cfg.get("args", [])
            transport = "stdio"  # default for command-based
            if cfg.get("url"):
                transport = "http" if cfg["url"].startswith("http") else "sse"
            out.append(
                {
                    "name": name,
                    "command": " ".join([str(command), *map(str, args)])
                    if command
                    else cfg.get("url", ""),
                    "transport": transport,
                }
            )
        return out


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _detect_os_family() -> str:
    sys_name = platform.system().lower()
    if sys_name == "darwin":
        return "darwin"
    if sys_name == "linux":
        return "linux"
    if sys_name == "windows":
        return "windows"
    return "other"


def _try_probe(cmd: list[str], parse_fn) -> str | None:
    """Run a probe command; return parsed version or None if it fails."""
    try:
        proc = subprocess.run(  # nosec B603
            cmd,
            capture_output=True,
            check=False,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    try:
        return parse_fn(proc.stdout.decode("utf-8", errors="replace"))
    except Exception:  # noqa: BLE001 — probe-output parser threw on this host; caller treats None as "no facts from this probe".
        return None


def _hash_probes(data: dict[str, Any]) -> str:
    """Stable hash over the entire probe output for incremental short-circuit."""
    canonical = json.dumps(data, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
