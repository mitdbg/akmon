"""Slack MCP-composed source — SCAFFOLD (Phase 3.1 of
`agent-led-mcp-onboarding`).

Framework only; concrete subclass body deferred to follow-up
proposal once a community Slack MCP server is selected +
validated against a real Slack workspace.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from .base import McpComposedSource


EXPECTED_TOOLS = {
    "list_channels": {
        "args": {"types": "list[str]"},  # public_channel, private...
        "response_keys": {"channels"},
    },
    "list_messages": {
        "args": {
            "channel_id": "str",
            "cursor": "str",
            "limit": "int",
        },
        "response_keys": {"messages", "response_metadata"},
    },
    "get_user": {
        "args": {"user_id": "str"},
        "response_keys": {
            "id", "name", "real_name", "email", "tz",
        },
    },
}


@dataclass
class SlackMcpSource(McpComposedSource):
    """SCAFFOLD: defer concrete body to follow-up proposal."""

    name: str = "slack_mcp"
    profile: str = "mutable_api"

    def _emit_facts(self, client) -> Iterable[Any]:
        raise NotImplementedError(
            "SlackMcpSource is a scaffold — the concrete MCP-composed "
            "ingest body ships in the follow-up proposal "
            "openspec/changes/compose-slack-mcp-source/. Until then, "
            "wire Slack via `okg add slack` (the legacy "
            "slack_api.SlackApiSource)."
        )
