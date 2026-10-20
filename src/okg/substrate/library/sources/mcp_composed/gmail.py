"""Gmail MCP-composed source — SCAFFOLD (Phase 3.1 of
`agent-led-mcp-onboarding`).

This module exists to:
  1. Reserve the substrate-side import path
     (`okg.substrate.library.sources.mcp_composed.gmail.GmailMcpSource`)
     so source-defaults / source_registry can declare it today.
  2. Pin the expected MCP tool surface (`list_messages`,
     `get_message`, `list_labels`) in code so reviewers + follow-up
     proposals can reason about the data-shape mapping without
     reading separate docs.
  3. Raise `NotImplementedError` with a clear pointer to the
     follow-up proposal that selects + validates a community Gmail
     MCP server. The framework is wired; the concrete subclass body
     ships separately, once we validate one specific community MCP
     server against a real Gmail OAuth app.

Why the deferred body: choosing + validating a community MCP
server (Google's official Gmail MCP, the Composio/Arcade
wrappers, the various npm packages) needs integration testing
against a real OAuth app + community-package-quality review.
Bounding that work into the same proposal as the framework would
turn a one-session deliverable into open-ended community-package
evaluation. The follow-up proposal —
`openspec/changes/compose-gmail-mcp-source/` — picks one server,
validates it, and replaces the body below.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from .base import McpComposedSource


# Expected MCP tool surface, documented up front so the follow-up
# proposal's `_emit_facts` implementation can be reviewed against
# the spec. Each entry is `(tool_name, args_shape, response_shape)`.
EXPECTED_TOOLS = {
    "list_labels": {
        "args": {},  # no params; returns all labels
        "response_keys": {"labels"},
    },
    "list_messages": {
        "args": {"label_ids": "list[str]", "max_results": "int"},
        "response_keys": {"messages"},  # list of {id, threadId}
    },
    "get_message": {
        "args": {"message_id": "str"},
        "response_keys": {
            "id", "subject", "from", "to", "date", "snippet",
            "thread_id",
        },
    },
}


@dataclass
class GmailMcpSource(McpComposedSource):
    """SCAFFOLD: emits no facts until the follow-up proposal lands.

    Operators wiring Gmail today should keep using the existing
    `gmail_api.GmailApiSource` (in-process, OAuth via keyring).
    This class will replace it once the MCP-composed path is
    validated.
    """

    name: str = "gmail_mcp"
    profile: str = "mutable_api"

    def _emit_facts(self, client) -> Iterable[Any]:
        raise NotImplementedError(
            "GmailMcpSource is a scaffold — the concrete MCP-composed "
            "ingest body ships in the follow-up proposal "
            "openspec/changes/compose-gmail-mcp-source/. Until then, "
            "wire Gmail via `okg add gmail` (the legacy "
            "gmail_api.GmailApiSource) — the install path's INSTALL.md "
            "documents the CLI fallback."
        )
