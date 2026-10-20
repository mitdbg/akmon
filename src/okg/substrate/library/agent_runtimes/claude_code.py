"""Claude Code runtime: register an OKG deployment with Claude
Code's user-scope `~/.claude.json` config.

Claude Code (the CLI) is the agent runtime most likely to exist on
a Linux workstation — Claude Desktop ships only for macOS/Windows,
so before this runtime existed a Linux operator finished `okg
install` with zero registered agents (observed live on a PI's
first install, 2026-06-09).

Config shape: same JSON `mcpServers` keyed object as Claude
Desktop / Cursor, at the top level of `~/.claude.json` (Claude
Code's user-scope MCP servers):

  {
    "mcpServers": {
      "okg-<deployment>": {
        "command": "/absolute/path/to/okg",
        "args": [
          "agent-runtime-launch", "--runtime", "claude-code",
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

`~/.claude.json` is Claude Code's live config and holds much more
than MCP servers; the upsert preserves every other key and the
write is atomic (temp + rename), per the module contract. Claude
Code picks the entry up on its next session start.

User-scope locations (Claude Code docs, "Connect Claude Code to tools via
MCP" and "Agent Skills", https://docs.claude.com/en/docs/claude-code/mcp
and .../skills, checked 2026-10-06): user-scope MCP servers live in
`~/.claude.json` under top-level `mcpServers`, and the supported way to add
one is `claude mcp add-json <name> '<json>' --scope user`; personal skills
live in `~/.claude/skills/<name>/SKILL.md`. `okg agent connect` uses the
`claude` CLI when it is on PATH and edits the file only when it is not.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Optional

from . import (
    backup_agent_config,
    RegisterResult, atomic_write_text, load_json_config, render_json_config,
)
from .launcher import build_profiled_mcp_server_entry


class ClaudeCodeRuntime:
    """Implements `AgentRuntime` for Claude Code's
    `~/.claude.json`.

    Detection is config-file OR `claude` binary on PATH: Claude
    Code only creates `~/.claude.json` on first launch, so
    "installed but never run" was invisible to file-only
    detection (the live PI case, 2026-06-09 — CLI installed, zero
    runtimes detected). When the binary is present without a
    config, `register` creates the file with just the
    `mcpServers` entry; Claude Code merges its own fields in on
    first launch.
    """

    name: str = "claude-code"
    servers_key: str = "mcpServers"
    config_format: str = "json"

    def config_path(self) -> Optional[Path]:
        return Path.home() / ".claude.json"

    def skills_root(self) -> Optional[Path]:
        return Path.home() / ".claude" / "skills"

    def load_config(self, cfg_path: Path) -> dict:
        return load_json_config(cfg_path)

    def render_config(self, data: dict) -> str:
        return render_json_config(data)

    def shape_entry(self, entry: dict) -> dict:
        return dict(entry)

    def _binary_on_path(self) -> bool:
        return shutil.which("claude") is not None

    def detect(self) -> bool:
        cfg = self.config_path()
        return bool(cfg and cfg.is_file()) or self._binary_on_path()

    def register(
        self, *, deployment: str, dsn: str,
        extra_env: dict | None = None,
    ) -> RegisterResult:
        cfg_path = self.config_path()
        if cfg_path is None or (
            not cfg_path.is_file() and not self._binary_on_path()
        ):
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
        # Strict: a non-object or unparseable file is refused, never
        # replaced by `{}` (PACT agent-connect-3412).
        return load_json_config(cfg_path)

    def _upsert(self, root: dict, server_key: str, entry: dict) -> None:
        servers = root.get("mcpServers")
        if not isinstance(servers, dict):
            servers = {}
            root["mcpServers"] = servers
        servers[server_key] = entry
