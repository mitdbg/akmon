"""`McpClient` re-lists tools when a server says the list changed.

Issue #1458, req.mcp.client-relists-on-notification. Before this, the client
cached `tools/list` once per session and never dropped it, so a server that
widened its surface mid-session was invisible to OKG for the rest of the run
— the last of the three things #1458 said had to be measured before
in-session expansion could be claimed.

Driven against the real `stub_mcp_server.py` subprocess in `--expandable`
mode, which writes a real `notifications/tools/list_changed` frame before the
response of the call that widened it. A mocked session would prove the
handler runs; it would not prove the SDK routes the frame to the handler at
all, which is the part that was unverified.
"""
from __future__ import annotations

import sys
from pathlib import Path

from okg.substrate.library.sources.mcp_composed.client import McpClient


STUB_PATH = Path(__file__).parent / "stub_mcp_server.py"


def _expandable_cmd() -> list[str]:
    return [sys.executable, str(STUB_PATH), "--expandable"]


def test_tool_list_is_still_cached_without_a_notification() -> None:
    """The caching this change relaxes must not simply disappear.

    A client that re-listed on every call would pay a round trip per tool
    call for a tool list that almost never changes.
    """
    with McpClient(_expandable_cmd()) as cli:
        first = cli.list_tools()
        assert cli.list_tools() is first
        assert cli.tool_list_changed_count == 0


def test_a_widened_server_surface_reaches_the_client() -> None:
    """The full round trip: notification, re-list, call the new tool."""
    with McpClient(_expandable_cmd()) as cli:
        assert cli.capabilities is not None
        assert cli.capabilities["tools"]["listChanged"] is True

        before = {t["name"] for t in cli.list_tools()}
        assert "late" not in before

        cli.call_tool("widen", {})
        assert cli.tool_list_changed_count == 1

        after = {t["name"] for t in cli.list_tools()}
        assert "late" in after, (
            "the client returned its cached tool list after the server said "
            "the list changed"
        )

        result = cli.call_tool("late", {})
        assert result["isError"] is False


def test_force_refresh_bypasses_the_cache_without_a_notification() -> None:
    """An escape hatch for a caller with reason to distrust the cache."""
    with McpClient(_expandable_cmd()) as cli:
        cli.list_tools()
        cli.call_tool("widen", {})
        cli.invalidate_tools_cache()
        assert "late" in {t["name"] for t in cli.list_tools(force_refresh=True)}


def test_a_server_that_never_notifies_keeps_the_old_behavior() -> None:
    """The plain stub advertises listChanged false and sends nothing."""
    with McpClient([sys.executable, str(STUB_PATH)]) as cli:
        assert cli.capabilities is not None
        assert cli.capabilities["tools"]["listChanged"] is False
        names = {t["name"] for t in cli.list_tools()}
        assert names == {"echo", "boom"}
        assert cli.tool_list_changed_count == 0
