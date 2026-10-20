"""Substrate sources that ingest via a local MCP server subprocess.

`agent-led-mcp-onboarding` (Phase 2): the framework — McpClient
(stdio JSON-RPC) + McpComposedSource (Source-protocol base
class). Provider-specific subclasses (Gmail / Calendar / Slack)
scaffold in Phase 3 but defer concrete `_emit_facts` to the
follow-up `compose-<provider>-mcp-source` proposals.
"""
from __future__ import annotations

from .base import McpComposedSource
from .client import McpClient, McpServerError


__all__ = [
    "McpClient",
    "McpComposedSource",
    "McpServerError",
]
