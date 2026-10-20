"""Google Calendar MCP-composed source — SCAFFOLD (Phase 3.1 of
`agent-led-mcp-onboarding`).

See `gmail.py` docstring for the rationale (framework now;
concrete subclass body in a follow-up proposal once a community
MCP server is validated). The expected tool surface below pins
what the follow-up's `_emit_facts` will call.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from .base import McpComposedSource


EXPECTED_TOOLS = {
    "list_calendars": {
        "args": {},
        "response_keys": {"calendars"},
    },
    "list_events": {
        "args": {
            "calendar_id": "str",
            "time_min": "str",
            "time_max": "str",
        },
        "response_keys": {"events"},
    },
    "get_event": {
        "args": {"calendar_id": "str", "event_id": "str"},
        "response_keys": {
            "id", "summary", "start", "end", "attendees",
            "description", "location",
        },
    },
}


@dataclass
class GoogleCalendarMcpSource(McpComposedSource):
    """SCAFFOLD: defer concrete body to follow-up proposal."""

    name: str = "google_calendar_mcp"
    profile: str = "mutable_api"

    def _emit_facts(self, client) -> Iterable[Any]:
        raise NotImplementedError(
            "GoogleCalendarMcpSource is a scaffold — the concrete "
            "MCP-composed ingest body ships in the follow-up proposal "
            "openspec/changes/compose-google-calendar-mcp-source/. "
            "Until then, wire calendars via `okg add cal-ics` "
            "(`CalendarIcsSource`) for the local-ICS path or "
            "`okg add gcal` (`GoogleCalendarApiSource`) for the "
            "OAuth path."
        )
