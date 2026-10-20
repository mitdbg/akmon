"""Semantic labels for the graph and node inspector."""

from okg.substrate.console_views.node_detail import _label_for


def test_message_label_prefers_readable_content_over_opaque_identity() -> None:
    assert _label_for(
        "message:codex:session-42:994",
        "agent_message",
        {"content_excerpt": "  Investigate\nwhy graph expansion resets.  "},
    ) == "Investigate why graph expansion resets."


def test_session_and_tool_labels_name_their_operational_context() -> None:
    assert _label_for(
        "session:codex:session-42",
        "agent_session",
        {"harness": "codex", "git_branch": "codex/graph-inspection"},
    ) == "Codex session · codex/graph-inspection"
    assert _label_for(
        "tool:session-42:12",
        "tool_use_event",
        {"tool_name": "apply_patch"},
    ) == "Tool · apply_patch"


def test_generic_fallback_still_explains_the_record_kind() -> None:
    assert _label_for(
        "verification:opaque-42",
        "verification_step",
        {},
    ) == "Verification Step · opaque-42"
