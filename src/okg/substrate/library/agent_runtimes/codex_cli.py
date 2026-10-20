"""codex CLI runtime: register an OKG deployment with codex's
`~/.codex/config.toml` MCP-servers config.

Config-block shape (verified against `~/.codex/config.toml` on
2026-05-21):

  [mcp_servers.okg-<deployment>]
  command = "/absolute/path/to/okg"
  args = [
    "agent-runtime-launch", "--runtime", "codex",
    "--permissions", "<profile>", "--deployment", "<deployment>",
    "--", "/absolute/path/to/okg", "mcp-serve",
    "--deployment", "<deployment>"
  ]

Other top-level tables (e.g. `[marketplaces.*]`, model config,
profile blocks) are preserved verbatim. The upsert replaces the
two `[mcp_servers.okg-<deployment>*]` tables for the matching
deployment name.

Uses stdlib `tomllib` for reading + `tomli_w` for writing, so a rewrite
keeps every key and value but not comments or layout.

Locations (Codex docs, "Model Context Protocol" and "Agent Skills",
https://developers.openai.com/codex/mcp and .../skills, checked 2026-10-06):
the config is `$CODEX_HOME/config.toml` (default `~/.codex/config.toml`) with
one `[mcp_servers.<name>]` table per server; user skills are read from
`~/.agents/skills/`.
"""
from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Optional

import tomli_w

from . import (
    backup_agent_config,
    AgentConfigParseError, RegisterResult, atomic_write_text,
)
from .launcher import build_profiled_mcp_server_entry


class CodexCliRuntime:
    """Implements `AgentRuntime` for codex CLI's
    `~/.codex/config.toml`."""

    name: str = "codex"
    servers_key: str = "mcp_servers"
    config_format: str = "toml"

    def codex_home(self) -> Path:
        explicit = (os.environ.get("CODEX_HOME") or "").strip()
        return Path(explicit).expanduser() if explicit else Path.home() / ".codex"

    def config_path(self) -> Optional[Path]:
        return self.codex_home() / "config.toml"

    def skills_root(self) -> Optional[Path]:
        return Path.home() / ".agents" / "skills"

    def load_config(self, cfg_path: Path) -> dict:
        if not cfg_path.is_file():
            return {}
        return self._read_existing(cfg_path)

    def render_config(self, data: dict) -> str:
        return tomli_w.dumps(data)

    def shape_entry(self, entry: dict) -> dict:
        return dict(entry)

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
            server_key = f"okg-{deployment}"
            self._upsert(
                existing, server_key,
                deployment=deployment, dsn=dsn, extra_env=extra_env,
            )
            backup_agent_config(cfg_path, load=self.load_config, servers_key=self.servers_key)
            atomic_write_text(
                cfg_path, tomli_w.dumps(existing),
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
        servers = existing.get("mcp_servers")
        if not isinstance(servers, dict):
            return False
        server_key = f"okg-{deployment}"
        if server_key not in servers:
            return False
        del servers[server_key]
        # If mcp_servers ended up empty, drop the table entirely
        # so we don't write an empty `[mcp_servers]` line.
        if not servers:
            del existing["mcp_servers"]
        backup_agent_config(cfg_path, load=self.load_config, servers_key=self.servers_key)
        atomic_write_text(
            cfg_path, tomli_w.dumps(existing),
        )
        return True

    # ---------- internals ----------

    def _read_existing(self, cfg_path: Path) -> dict:
        text = cfg_path.read_text(encoding="utf-8")
        if not text.strip():
            return {}
        try:
            return tomllib.loads(text)
        except tomllib.TOMLDecodeError as exc:
            raise AgentConfigParseError(cfg_path, f"invalid TOML ({exc})") from exc

    def _upsert(
        self,
        root: dict,
        server_key: str,
        *,
        deployment: str,
        dsn: str,
        extra_env: dict | None = None,
    ) -> None:
        """Upsert the `[mcp_servers.<server_key>]` block. tomli_w
        emits nested-dict structure as `[parent.child]` tables
        in the output — so we just write the nested dict.
        """
        servers = root.get("mcp_servers")
        if not isinstance(servers, dict):
            servers = {}
            root["mcp_servers"] = servers
        servers[server_key] = build_profiled_mcp_server_entry(
            runtime=self.name,
            deployment=deployment,
            dsn=dsn,
            extra_env=extra_env,
        )
