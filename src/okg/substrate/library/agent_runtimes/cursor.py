"""Cursor runtime: register an OKG deployment with Cursor's
`~/.cursor/mcp.json` config.

Config shape (matches Claude Desktop's format — same JSON
`mcpServers` keyed object):

  {
    "mcpServers": {
      "okg-<deployment>": {
        "command": "/absolute/path/to/okg",
        "args": [
          "agent-runtime-launch", "--runtime", "cursor",
          "--permissions", "<profile>", "--deployment", "<deployment>",
          "--", "/absolute/path/to/okg", "mcp-serve",
          "--deployment", "<deployment>"
        ],
        "env": {
          "OKG_AGENT_RUNTIME_PROFILE": "<profile>",
          "OKG_DEPLOYMENT": "<deployment>"
        }
      }
    }
  }

Cursor reads this on startup; in-app changes require a
restart, same as Claude Desktop. The upsert preserves any
other MCP servers the operator has configured.

User-scope locations (Cursor docs, "Model Context Protocol" and "Agent
Skills", https://cursor.com/docs/context/mcp and .../skills, checked
2026-10-06): global MCP servers live in `~/.cursor/mcp.json` under
`mcpServers`, and a local server entry carries `"type": "stdio"`; user skills
are read from `~/.agents/skills/` (Cursor also reads `~/.claude/skills/`, so a
user who connects Claude Code too sees both copies).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from . import (
    backup_agent_config,
    RegisterResult, atomic_write_text, load_json_config, render_json_config,
)
from .launcher import build_profiled_mcp_server_entry


class CursorRuntime:
    """Implements `AgentRuntime` for Cursor's
    `~/.cursor/mcp.json`."""

    name: str = "cursor"
    servers_key: str = "mcpServers"
    config_format: str = "json"

    def config_path(self) -> Optional[Path]:
        return Path.home() / ".cursor" / "mcp.json"

    def skills_root(self) -> Optional[Path]:
        return Path.home() / ".agents" / "skills"

    def load_config(self, cfg_path: Path) -> dict:
        return load_json_config(cfg_path)

    def render_config(self, data: dict) -> str:
        return render_json_config(data)

    def shape_entry(self, entry: dict) -> dict:
        """Cursor's documented stdio shape: ``"type": "stdio"`` first."""
        return {"type": "stdio", **{k: v for k, v in entry.items() if k != "type"}}

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
        return self.shape_entry(build_profiled_mcp_server_entry(
            runtime=self.name,
            deployment=deployment,
            dsn=dsn,
            extra_env=extra_env,
        ))

    def _read_existing(self, cfg_path: Path) -> dict:
        # Strict: a non-object or unparseable file is refused, never
        # replaced by `{}` (PACT agent-connect-3412).
        return load_json_config(cfg_path)

    def _upsert(self, root: dict, server_key: str, entry: dict) -> None:
        servers = root.get("mcpServers")
        if not isinstance(servers, dict):
            servers = {}
            root["mcpServers"] = servers
        servers[server_key] = entry
