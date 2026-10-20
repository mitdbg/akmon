"""Base class for sources that ingest via a local MCP server
subprocess (`agent-led-mcp-onboarding` Phase 2).

The `McpComposedSource` follows the same `Source` protocol as the
existing in-process source classes (`run() -> SourceRun`,
optional `preflight()`), but its `run()` method spawns an MCP
server subprocess, calls its tools to fetch data, transforms the
results into `NodeFact` / `EdgeFact` emissions, then SIGTERMs the
subprocess.

Subclasses provide:
  - `mcp_server_command: list[str]` constructor param (the argv
    to spawn).
  - `_emit_facts(client: McpClient) -> Iterable` — call the MCP
    tools, transform responses, yield facts + progress markers.

The base class handles:
  - subprocess lifecycle (via `McpClient`)
  - preflight (checks `mcp_server_command[0]` resolves)
  - structured failure surface — spawn failures, handshake
    failures, tool-call errors all become an empty `SourceRun`
    with a warning log + `completed_scope=True` so a broken
    third-party MCP server doesn't tank the substrate publish.
"""
from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from okg.substrate.library.sources.base import (
    SourcePreflightResult,
    SourceRun,
)

from .client import McpClient, McpServerError


_log = logging.getLogger(__name__)


@dataclass
class McpComposedSource:
    """Base class for any source that ingests via a local MCP
    server subprocess.

    Subclasses MUST override `_emit_facts(client)` to call MCP
    tools and yield `NodeFact` / `EdgeFact` / `ProgressMarker`
    objects. The base class wraps the subprocess lifecycle.

    Constructor params (the `params:` block of the source-defaults
    YAML — `ownership_id` lives at the registry top level, not
    here):
      - `mcp_server_command`: argv list to spawn the MCP server.
        First element resolves via PATH (or absolute path).
      - `deployment_name`: per-deployment isolation (passed to
        env vars + cache scope where applicable). Optional —
        defaults to empty when not interpolated.
      - `env`: optional dict of extra env vars to merge into the
        subprocess's environment (e.g. OAuth tokens).
      - `subprocess_timeout`: read timeout for any single MCP
        response. Default 30 seconds.
    """

    mcp_server_command: list[str] = field(default_factory=list)
    deployment_name: str = ""
    env: Optional[dict[str, str]] = None
    subprocess_timeout: float = 30.0

    # Subclasses override.
    name: str = "mcp_composed"
    profile: str = "mutable_api"

    def preflight(self, mode: str = "live") -> SourcePreflightResult:
        """Verify the MCP server command resolves to an executable.

        Treats a missing executable as a setup gate
        (`missing_credential`) — that mirrors the OAuth-sources
        preflight pattern: the substrate doesn't fail-open into
        an actual ingest attempt that will surely fail. The
        operator gets a fix hint pointing at
        `mcp_server_command` in `source_registry.yaml`.
        """
        now = datetime.now(timezone.utc).isoformat()
        cmd = self.mcp_server_command or []
        if not cmd:
            return SourcePreflightResult(
                source_name=self.name,
                status="missing_credential",
                mode=mode,
                required=False,
                reason="mcp_server_command is empty",
                checked_at=now,
            )
        exe = cmd[0]
        resolved = shutil.which(exe) or (
            exe if Path(exe).is_file() else None
        )
        if resolved is None:
            return SourcePreflightResult(
                source_name=self.name,
                status="missing_credential",
                mode=mode,
                required=False,
                reason=(
                    f"mcp_server_command executable not on PATH: {exe}. "
                    f"Install it (e.g. `npm i -g {exe}`) or override "
                    "`mcp_server_command` in source_registry.yaml"
                ),
                checked_at=now,
            )
        return SourcePreflightResult(
            source_name=self.name,
            status="ok",
            mode=mode,
            required=False,
            reason=f"mcp_server_command={exe} (resolved {resolved})",
            checked_at=now,
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        **_ignored: Any,
    ) -> SourceRun:
        """Spawn the MCP server, ingest, and SIGTERM. On any
        infrastructure failure (spawn / handshake / tool call),
        emit an empty stream + `completed_scope=True` with a
        WARNING-level log, NOT an uncaught exception. This keeps
        third-party-MCP-server breakage from cratering the
        broader publish."""
        try:
            with McpClient(
                command=list(self.mcp_server_command),
                env=self.env,
                subprocess_timeout=self.subprocess_timeout,
            ) as client:
                facts = list(self._emit_facts(client))
        except McpServerError as e:
            _log.warning(
                "%s: MCP server failed (stage=%s): %s",
                self.name, e.stage, e,
            )
            return SourceRun(facts=iter([]), completed_scope=True)
        except Exception as e:  # noqa: BLE001
            _log.warning(
                "%s: unexpected error during MCP ingest: %s",
                self.name, e,
            )
            return SourceRun(facts=iter([]), completed_scope=True)
        return SourceRun(facts=iter(facts), completed_scope=True)

    def _emit_facts(self, client: McpClient) -> Iterable[Any]:
        """Subclass hook: call MCP tools + transform responses.

        Default raises NotImplementedError so the framework + the
        scaffold-only subclasses (Gmail / Calendar / Slack) all
        present the same error: "this source is wired, but the
        concrete MCP-server choice is pending — see the follow-up
        proposal." Concrete subclasses override.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement _emit_facts. "
            "See openspec/changes/compose-<provider>-mcp-source/ "
            "for the follow-up proposal that wires this source to "
            "a specific community MCP server."
        )

    def get_health(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "mcp_server_command": list(self.mcp_server_command),
            "deployment_name": self.deployment_name,
        }
