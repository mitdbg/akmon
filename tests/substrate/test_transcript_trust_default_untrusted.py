"""Trust boundary #696: unknown transcript tools default to untrusted.

``transcript_trust`` used to name the *untrusted* set positively
(``WEB_DERIVED_TOOL_NAMES = {"WebSearch", "WebFetch"}``): any tool name
outside that pair -- a browser fetch, an MCP fetch variant, or a
tool the enumeration never anticipated -- crossed the boundary tagged
*trusted*. That is allow-by-omission on a content-provenance boundary.

Per design.md Section 2(e)/(g), the boundary must be allow-list-by-membership:
a positive internal-tool allow-list, with every unrecognized name defaulting
to untrusted. These tests fail on the pre-fix code, which tagged unseen tool
names trusted.
"""

from __future__ import annotations

from okg.substrate.transcript_trust import (
    INTERNAL_TRUSTED_TOOL_NAMES,
    is_untrusted_content_tool,
)


def test_known_internal_tools_are_trusted():
    for name in ("Read", "Write", "Edit", "Bash", "Grep", "Glob"):
        assert name in INTERNAL_TRUSTED_TOOL_NAMES
        assert is_untrusted_content_tool(name) is False


def test_web_tools_are_untrusted():
    assert is_untrusted_content_tool("WebSearch") is True
    assert is_untrusted_content_tool("WebFetch") is True


def test_browser_and_mcp_fetch_variants_are_untrusted():
    for name in (
        "mcp__Claude_Browser__navigate",
        "mcp__chrome-devtools__navigate_page",
        "mcp__fetch__fetch",
        "WebFetchTool",
    ):
        assert is_untrusted_content_tool(name) is True


def test_unknown_tool_name_defaults_untrusted():
    # A tool name the allow-list never anticipated must not cross trusted.
    assert is_untrusted_content_tool("SomeFutureExternalFetcher") is True
    assert is_untrusted_content_tool("") is True
    assert is_untrusted_content_tool(None) is True


def test_extract_marks_unknown_tool_result_untrusted():
    # The agent-sessions extractor collects untrusted tool-result names; an
    # unseen tool feeding transcript content must land in that set, while a
    # known internal tool must not.
    from okg.substrate.library.sources._agent_sessions_extract import (
        _external_tool_result_names,
    )

    content = [
        {
            "type": "tool_result",
            "tool_use_id": "u1",
            "name": "SomeFutureExternalFetcher",
        },
        {
            "type": "tool_result",
            "tool_use_id": "u2",
            "name": "Bash",
        },
    ]
    names = _external_tool_result_names(
        content,
        tool_use_names_by_id={},
        fallback_tool_name=None,
    )
    assert "SomeFutureExternalFetcher" in names
    assert "Bash" not in names
