"""Trust markers for mixed-origin agent transcript nodes."""

from __future__ import annotations

from typing import Any


UNTRUSTED_CONTENT_ORIGIN_ATTR = "untrusted_content_origin"
UNTRUSTED_CONTENT_TOOL_NAMES_ATTR = "untrusted_content_tool_names"
WEB_TOOL_RESULT_ORIGIN = "web_tool_result"

# Allow-list by membership (design.md Section 2(e)/(g)): only tools whose
# results are content from the local workspace / the agent's own machine are
# trusted. Everything else -- web/search/fetch, browser drivers, MCP tools
# (which cross to another server), and any name this list never anticipated --
# defaults to untrusted. The set names the built-in local tools of the two
# harnesses whose transcripts this source ingests (Claude Code, Codex CLI).
INTERNAL_TRUSTED_TOOL_NAMES = frozenset(
    {
        # Claude Code built-in local-workspace tools.
        "Read",
        "Write",
        "Edit",
        "MultiEdit",
        "NotebookEdit",
        "NotebookRead",
        "Bash",
        "BashOutput",
        "KillBash",
        "KillShell",
        "Glob",
        "Grep",
        "LS",
        "TodoWrite",
        "Task",
        "ExitPlanMode",
        "SlashCommand",
        "Skill",
        # Codex CLI built-in local tools.
        "shell",
        "apply_patch",
        "update_plan",
        "view_image",
    }
)


def is_untrusted_content_tool(tool_name: str | None) -> bool:
    """Return whether a tool's result content must be treated as untrusted.

    Fail-closed: a tool is trusted only if its name is in the positive
    internal-tool allow-list. Unknown names -- including empty/unresolvable
    names, web/search/fetch tools, browser drivers, and MCP fetch variants --
    are untrusted. This inverts the prior allow-by-omission
    (``{"WebSearch", "WebFetch"}``) under which any unanticipated tool crossed
    the boundary trusted.
    """
    name = str(tool_name or "").strip()
    if not name:
        return True
    return name not in INTERNAL_TRUSTED_TOOL_NAMES


def untrusted_web_tool_result_attrs(
    tool_names: tuple[str, ...],
) -> dict[str, Any]:
    if not tool_names:
        return {}
    return {
        UNTRUSTED_CONTENT_ORIGIN_ATTR: WEB_TOOL_RESULT_ORIGIN,
        UNTRUSTED_CONTENT_TOOL_NAMES_ATTR: list(tool_names),
    }


def attrs_mark_untrusted_transcript_content(attrs: Any) -> bool:
    if not isinstance(attrs, dict):
        return False
    return attrs.get(UNTRUSTED_CONTENT_ORIGIN_ATTR) == WEB_TOOL_RESULT_ORIGIN
