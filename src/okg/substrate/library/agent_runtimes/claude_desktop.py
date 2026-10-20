"""Claude Desktop runtime: register an OKG deployment with
Claude Desktop's MCP config.

Per `dead-simple-install`: this is the refactor of the original
`_write_claude_desktop_config` + `_build_claude_desktop_entry`
from `academic-researcher/cli/install.py`. The substantive behavior
preserved from `fix-okg-install-claude-config`:

  - `command` is the absolute interpreter running this okg, with
    `-P -m okg.substrate.cli.main` (issue #2301), so the spawned
    subprocess works without inheriting the operator's PATH and
    cannot pick up a different okg from it.
  - `args` SHALL NOT contain `--port` — `okg mcp-serve` only
    accepts `--dsn`, `--deployment`, `--embedder`, `--branch`.
  - raw DSNs are not written to the agent config; the profiled
    launcher resolves deployment authority before `mcp-serve`.
  - Upsert by deployment key; preserves other servers.
  - Back up the existing config to `<file>.okg-backup` before every
    write (refreshed when the file changed; a foreign `.bak` is ignored).

Locations (Claude Help Center, "Getting started with local MCP servers on
Claude Desktop" and "Using Skills in Claude", checked 2026-10-06): the config
is `claude_desktop_config.json` under `~/Library/Application Support/Claude/`
on macOS; the Linux build is a beta and keeps it under `~/.config/Claude/`
(`$XDG_CONFIG_HOME/Claude/`). Claude Desktop has no skills folder: a skill is
uploaded as a ZIP in Settings, so `okg agent connect` writes one ZIP per skill
and prints that step. The app reads the config only at start, so it needs a
full quit and restart.
"""
from __future__ import annotations

import json
import os
import platform
from pathlib import Path
from typing import Optional

from . import (
    backup_agent_config,
    RegisterResult, atomic_write_text, load_json_config, render_json_config,
)
from .launcher import build_profiled_mcp_server_entry


class ClaudeDesktopRuntime:
    """Implements `AgentRuntime` for Claude Desktop's
    `claude_desktop_config.json`."""

    name: str = "claude-desktop"
    servers_key: str = "mcpServers"
    config_format: str = "json"

    def skills_root(self) -> Optional[Path]:
        return None  # no skills folder: skills are uploaded as ZIPs

    def load_config(self, cfg_path: Path) -> dict:
        return load_json_config(cfg_path)

    def render_config(self, data: dict) -> str:
        return render_json_config(data)

    def shape_entry(self, entry: dict) -> dict:
        return dict(entry)

    def config_path(self) -> Optional[Path]:
        """Platform-aware Claude Desktop config path."""
        home = Path.home()
        system = platform.system()
        if system == "Darwin":
            return (
                home
                / "Library/Application Support/Claude/claude_desktop_config.json"
            )
        if system == "Linux":
            xdg = os.environ.get("XDG_CONFIG_HOME")
            base = Path(xdg) if xdg else home / ".config"
            return base / "Claude/claude_desktop_config.json"
        if system == "Windows":
            appdata = os.environ.get("APPDATA")
            if appdata:
                return Path(appdata) / "Claude/claude_desktop_config.json"
        return None

    def detect(self) -> bool:
        cfg = self.config_path()
        return bool(cfg and cfg.is_file())

    def register(
        self, *, deployment: str, dsn: str,
        extra_env: dict | None = None,
    ) -> RegisterResult:
        cfg_path = self.config_path()
        if cfg_path is None or not cfg_path.is_file():
            return RegisterResult(
                runtime=self.name,
                status="not_detected",
                config_path=cfg_path,
            )
        try:
            existing = self._read_existing(cfg_path)
            entry = self._build_entry(
                deployment=deployment, dsn=dsn, extra_env=extra_env,
            )
            server_key = f"okg-{deployment}"
            self._upsert(existing, server_key, entry)
            backup_agent_config(cfg_path, load=self.load_config, servers_key=self.servers_key)
            atomic_write_text(
                cfg_path,
                json.dumps(existing, indent=2) + "\n",
            )
            return RegisterResult(
                runtime=self.name,
                status="registered",
                config_path=cfg_path,
                config_mtime=cfg_path.stat().st_mtime,
            )
        except (OSError, ValueError) as e:
            return RegisterResult(
                runtime=self.name,
                status="failed",
                config_path=cfg_path,
                error=str(e),
            )

    def unregister(self, deployment: str) -> bool:
        cfg_path = self.config_path()
        if cfg_path is None or not cfg_path.is_file():
            return False
        existing = self._read_existing(cfg_path)
        servers = existing.get("mcpServers")
        if not isinstance(servers, dict):
            return False
        server_key = f"okg-{deployment}"
        if server_key not in servers:
            return False
        del servers[server_key]
        backup_agent_config(cfg_path, load=self.load_config, servers_key=self.servers_key)
        atomic_write_text(
            cfg_path,
            json.dumps(existing, indent=2) + "\n",
        )
        return True

    # ---------- internals ----------

    def _build_entry(
        self, *, deployment: str, dsn: str,
        extra_env: dict | None = None,
    ) -> dict:
        return build_profiled_mcp_server_entry(
            runtime=self.name,
            deployment=deployment,
            dsn=dsn,
            extra_env=extra_env,
        )

    def _read_existing(self, cfg_path: Path) -> dict:
        """Load existing config; an empty file is ``{}``. A non-object or
        unparseable file is refused, never replaced (PACT
        agent-connect-3412)."""
        return load_json_config(cfg_path)

    def _upsert(self, root: dict, server_key: str, entry: dict) -> None:
        servers = root.get("mcpServers")
        if not isinstance(servers, dict):
            servers = {}
            root["mcpServers"] = servers
        servers[server_key] = entry
