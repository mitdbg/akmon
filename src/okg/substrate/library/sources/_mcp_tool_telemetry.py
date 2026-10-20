"""Helpers for transcript-derived MCP tool telemetry."""

from __future__ import annotations

from dataclasses import dataclass


MCP_TOOL_PREFIX = "mcp__"

# Registered OKG MCP servers are conventionally named ``okg-<deployment>``, so
# the deployment is recovered by stripping the ``okg-`` prefix. A few
# deployments carry the prefix in their own name and therefore map to
# themselves; list those irregular server->deployment pairs as data rather than
# branching on a hardcoded name at runtime.
_IRREGULAR_SERVER_DEPLOYMENTS = {
    "okg-workspace": "okg-workspace",
}


@dataclass(frozen=True)
class McpToolTelemetry:
    server: str
    deployment: str
    operator: str

    def attrs(self) -> dict[str, str]:
        attrs = {
            "mcp_server": self.server,
            "mcp_operator": self.operator,
        }
        if self.deployment:
            attrs["mcp_deployment"] = self.deployment
        return attrs


def parse_mcp_tool_name(tool_name: str) -> McpToolTelemetry | None:
    """Parse Claude/Codex MCP tool names shaped as mcp__server__operator."""
    if not tool_name.startswith(MCP_TOOL_PREFIX):
        return None
    parts = tool_name.split("__", 2)
    if len(parts) != 3 or parts[0] != "mcp":
        return None
    server = parts[1].strip()
    operator = parts[2].strip()
    if not server or not operator:
        return None
    server = _normalize_server_name(server)
    return McpToolTelemetry(
        server=server,
        deployment=_infer_deployment(server),
        operator=operator,
    )


def parse_mcp_tool_call(
    tool_name: str,
    *,
    namespace: str | None = None,
) -> McpToolTelemetry | None:
    """Parse explicit or Codex bare-name MCP calls.

    Claude transcripts carry MCP calls as ``mcp__server__operator`` tool
    names. Codex rollouts preserve the operator as the bare function name
    and carry the MCP server in ``payload.namespace``.
    """
    explicit = parse_mcp_tool_name(tool_name)
    if explicit is not None:
        return explicit
    server = parse_mcp_namespace(namespace)
    operator = tool_name.strip()
    if server is None or not operator:
        return None
    return McpToolTelemetry(
        server=server,
        deployment=_infer_deployment(server),
        operator=operator,
    )


def parse_mcp_namespace(namespace: str | None) -> str | None:
    if not namespace:
        return None
    value = str(namespace).strip()
    if not value.startswith(MCP_TOOL_PREFIX):
        return None
    server = value.removeprefix(MCP_TOOL_PREFIX).strip("_")
    if not server:
        return None
    return _normalize_server_name(server)


def is_mcp_tool_name(tool_name: str) -> bool:
    return parse_mcp_tool_name(tool_name) is not None


def _normalize_server_name(server: str) -> str:
    return server.strip().strip("_").replace("_", "-")


def _infer_deployment(server: str) -> str:
    irregular = _IRREGULAR_SERVER_DEPLOYMENTS.get(server)
    if irregular is not None:
        return irregular
    if server.startswith("okg-") and len(server) > len("okg-"):
        return server.removeprefix("okg-")
    return ""
