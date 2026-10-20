"""Streaming extractors for Claude Code session JSONL transcripts.

Single-pass scan produces a :class:`SessionScan` containing every
record we care about. The source adapter then materializes this into
graph nodes and edges.

Tolerant parsing: malformed JSON lines are logged and skipped, never
raised. Unknown event types are ignored.

The exact JSONL shapes are documented in the proposal's design.md
(reconnaissance section). Key event types we touch:

- ``type=user``: messages.content[].type=text or tool_result
- ``type=assistant``: messages.content[].type=text|thinking|tool_use
- ``type=system``: subtype in {informational, local_command, ...}
- ``type=permission-mode``: top-level permissionMode change
- ``type=file-history-snapshot``: ignored
- ``type=attachment`` / ``custom-title`` / ``ai-title`` / etc.: ignored
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from okg.substrate.library.sources._mcp_tool_telemetry import (
    parse_mcp_tool_name,
)
from okg.substrate.library.sources._transcript_redaction import scrub_transcript_text
from okg.substrate.library.sources import _transcript_resume
from okg.substrate.transcript_trust import is_untrusted_content_tool

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------
@dataclass
class ChapterRecord:
    title: str
    summary: str | None
    index: int
    started_at: str | None


@dataclass
class ChipRecord:
    title: str
    prompt: str
    tldr: str | None
    index: int
    chapter_index: int | None = None  # the chapter this chip lived inside


@dataclass
class SkillRecord:
    name: str
    plugin: str | None = None


@dataclass
class FileEditRecord:
    file_path: str
    tool_name: str  # "Edit" | "Write" | "NotebookEdit"


@dataclass
class SshHostRecord:
    """An ssh host the session reached (mined from `Bash` tool calls).

    Mirrors the shape compute_env's RemoteHostsFromSessions extracts.
    Emitted by agent_sessions so the cross-module REFERENCES to
    ``host:<hostname>`` lands without requiring compute_env to also
    scan the same JSONL file.
    """
    hostname: str
    username: str


@dataclass
class PermissionEventRecord:
    kind: str  # "denied" | "hook_blocked" | "interrupted" | "mode_changed"
    timestamp: str
    tool_name: str | None
    excerpt: str | None
    tool_input_summary: str | None
    chapter_index: int | None
    # When the halted tool's input contained a file_path (Edit/Write/etc.),
    # capture it for downstream PermissionEvent → SourceFile linkage.
    tool_input_file_path: str | None = None


@dataclass
class AgentMessageRecord:
    """One bounded, redacted transcript message.

    The node stores enough structure to navigate a session trace without
    storing full prompt/response payloads in the graph.
    """
    role: str
    message_index: int
    line_index: int
    timestamp: str
    message_kind: str
    content_hash: str
    content_excerpt: str
    tool_call_count: int
    parent_message_id: str | None = None
    chapter_index: int | None = None
    untrusted_content_origin: str | None = None
    untrusted_content_tool_names: tuple[str, ...] = ()


@dataclass
class ToolUseEventRecord:
    """One tool_use block from an assistant turn, captured per the
    source's `capture_tool_use_events` allowlist or MCP namespace
    telemetry.

    Per `add-tool-use-event-capture` (2026-04-27).
    """
    tool_name: str
    turn_index: int  # 0-based assistant turn index in the parent transcript
    block_index: int  # 0-based tool_use block index within the turn
    timestamp: str  # event timestamp, falls back to scan.ended_at if missing
    file_path: str | None = None
    command: str | None = None  # only set for Bash
    input_excerpt: str | None = None
    chapter_index: int | None = None
    message_index: int | None = None
    # Native error signal (reduce-agent-friction
    # req.friction.event-observability): set from the matching tool_result
    # block. None = no result observed in this scan chunk (unknown), never
    # a silent default.
    is_error: bool | None = None
    error_kind: str | None = None  # denied | hook_blocked | interrupted | error
    mcp_server: str | None = None
    mcp_deployment: str | None = None
    mcp_operator: str | None = None


@dataclass
class SessionScan:
    """Everything one transcript scan yields."""

    session_id: str | None = None
    cwd: str | None = None
    git_branch: str | None = None
    cc_version: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    model: str | None = None
    harness: str = "claude_code"

    # Aggregates
    turn_count: int = 0
    tool_call_count: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cache_read_tokens: int = 0
    total_cache_creation_tokens: int = 0
    model_calls: int = 0
    auto_allow_count: int = 0
    permission_mode: str = "default"

    # Per-event lists
    chapters: list[ChapterRecord] = field(default_factory=list)
    chips: list[ChipRecord] = field(default_factory=list)
    skills: list[SkillRecord] = field(default_factory=list)
    file_edits: list[FileEditRecord] = field(default_factory=list)
    permission_events: list[PermissionEventRecord] = field(default_factory=list)
    messages: list[AgentMessageRecord] = field(default_factory=list)
    ssh_hosts: list[SshHostRecord] = field(default_factory=list)
    tool_use_events: list[ToolUseEventRecord] = field(default_factory=list)

    # Append-frontier bookkeeping. Event lists above contain only records
    # emitted by this scan, while these counters are cumulative so node IDs
    # remain stable when a later run starts at a byte offset.
    line_count: int = 0
    last_line_hash: str = ""
    message_count: int = 0
    permission_event_start_index: int = 0
    permission_event_count: int = 0
    # Hard tool errors (is_error tool_results) regardless of permission
    # classification — the 4.9% class the 2026-06-11 mining could only
    # text-scrape (reduce-agent-friction req.friction.event-observability).
    tool_error_count: int = 0
    chapter_count: int = 0
    current_chapter_index: int = -1
    chip_count: int = 0
    last_tool_use_name: str | None = None
    last_tool_use_summary: str | None = None
    last_tool_use_file_path: str | None = None
    pending_tool_use_names_by_id: dict[str, str] = field(default_factory=dict)
    append_delta: bool = False


@dataclass(frozen=True)
class SessionScanState:
    bytes: int
    mtime_ns: int
    line_count: int
    last_line_hash: str
    session_id: str | None
    cwd: str | None
    git_branch: str | None
    cc_version: str | None
    started_at: str | None
    ended_at: str | None
    model: str | None
    harness: str
    turn_count: int
    tool_call_count: int
    total_input_tokens: int
    total_output_tokens: int
    total_cache_read_tokens: int
    total_cache_creation_tokens: int
    model_calls: int
    auto_allow_count: int
    permission_mode: str
    message_count: int
    permission_event_count: int
    tool_error_count: int
    chapter_count: int
    current_chapter_index: int
    chip_count: int
    last_tool_use_name: str | None
    last_tool_use_summary: str | None
    last_tool_use_file_path: str | None
    pending_tool_use_names_by_id: dict[str, str]
    append_safe: bool


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PERMISSION_DENIED_RE = re.compile(r"^Permission to use (\w+) has been denied")
# Hooks emit "Blocked X..." or "Blocked: X..." etc; require word-boundary
# after "Blocked" so it doesn't match "BlockedFooBar".
HOOK_BLOCK_RE = re.compile(r"^Blocked\b")
# The harness wraps hook refusals as
# "PreToolUse:<Tool> hook error: [<command>]: <hook output>" — the prefix
# form kept hook_block_count structurally zero until the 2026-07-02
# verification run against the real corpus caught it
# (reduce-agent-friction req.friction.event-observability).
HOOK_PREFIX_RE = re.compile(r"^(Pre|Post)ToolUse\b.* hook (error|blocked)", re.IGNORECASE)

# Match `ssh user@host`. Same shape as compute_env.remote_hosts_source's
# pattern — flag-shaped intermediate tokens only, to avoid matching
# emails in prose. Kept in sync; the two modules extract the same shape.
SSH_PATTERN = re.compile(
    r"\bssh\s+"
    r"(?:-\S+(?:\s+[^\s\-]\S*)?\s+){0,4}"
    r"([a-zA-Z][\w_-]*)@([a-zA-Z][\w.-]+)",
)
INTERRUPT_KEYWORDS = ("cancelled", "interrupted")
FILE_EDIT_TOOLS = frozenset({"Edit", "Write", "NotebookEdit"})
CHAPTER_TOOL = "mcp__ccd_session__mark_chapter"
SPAWN_TASK_TOOL = "mcp__ccd_session__spawn_task"
SKILL_TOOL = "Skill"
APPEND_CURSOR_SCHEMA = "okg.agent_sessions.append_cursor.v1"


# ---------------------------------------------------------------------------
# Main scan
# ---------------------------------------------------------------------------
def scan_state_from_cursor(raw: Any) -> SessionScanState | None:
    if not isinstance(raw, dict):
        return None
    state = raw.get("append_cursor")
    if not isinstance(state, dict):
        return None
    if state.get("schema") != APPEND_CURSOR_SCHEMA:
        return None
    try:
        return SessionScanState(
            bytes=int(state["bytes"]),
            mtime_ns=int(state.get("mtime_ns") or 0),
            line_count=int(state["line_count"]),
            last_line_hash=str(state.get("last_line_hash") or ""),
            session_id=_optional_str(state.get("session_id")),
            cwd=_optional_str(state.get("cwd")),
            git_branch=_optional_str(state.get("git_branch")),
            cc_version=_optional_str(state.get("cc_version")),
            started_at=_optional_str(state.get("started_at")),
            ended_at=_optional_str(state.get("ended_at")),
            model=_optional_str(state.get("model")),
            harness=str(state.get("harness") or "claude_code"),
            turn_count=int(state.get("turn_count") or 0),
            tool_call_count=int(state.get("tool_call_count") or 0),
            total_input_tokens=int(state.get("total_input_tokens") or 0),
            total_output_tokens=int(state.get("total_output_tokens") or 0),
            total_cache_read_tokens=int(
                state.get("total_cache_read_tokens") or 0,
            ),
            total_cache_creation_tokens=int(
                state.get("total_cache_creation_tokens") or 0,
            ),
            model_calls=int(state.get("model_calls") or 0),
            auto_allow_count=int(state.get("auto_allow_count") or 0),
            permission_mode=str(state.get("permission_mode") or "default"),
            message_count=int(state.get("message_count") or 0),
            permission_event_count=int(
                state.get("permission_event_count") or 0,
            ),
            tool_error_count=int(state.get("tool_error_count") or 0),
            chapter_count=int(state.get("chapter_count") or 0),
            current_chapter_index=int(
                state.get("current_chapter_index")
                if state.get("current_chapter_index") is not None else -1
            ),
            chip_count=int(state.get("chip_count") or 0),
            last_tool_use_name=_optional_str(state.get("last_tool_use_name")),
            last_tool_use_summary=_optional_str(
                state.get("last_tool_use_summary"),
            ),
            last_tool_use_file_path=_optional_str(
                state.get("last_tool_use_file_path"),
            ),
            pending_tool_use_names_by_id={
                str(k): str(v)
                for k, v in (
                    state.get("pending_tool_use_names_by_id") or {}
                ).items()
            },
            append_safe=bool(state.get("append_safe")),
        )
    except (KeyError, TypeError, ValueError):
        return None


def scan_state_to_cursor(
    scan: SessionScan,
    *,
    bytes_size: int,
    mtime_ns: int,
    append_safe: bool,
) -> dict[str, Any]:
    return {
        "append_cursor": {
            "schema": APPEND_CURSOR_SCHEMA,
            "bytes": bytes_size,
            "mtime_ns": mtime_ns,
            "line_count": scan.line_count,
            "last_line_hash": scan.last_line_hash,
            "session_id": scan.session_id,
            "cwd": scan.cwd,
            "git_branch": scan.git_branch,
            "cc_version": scan.cc_version,
            "started_at": scan.started_at,
            "ended_at": scan.ended_at,
            "model": scan.model,
            "harness": scan.harness,
            "turn_count": scan.turn_count,
            "tool_call_count": scan.tool_call_count,
            "total_input_tokens": scan.total_input_tokens,
            "total_output_tokens": scan.total_output_tokens,
            "total_cache_read_tokens": scan.total_cache_read_tokens,
            "total_cache_creation_tokens": scan.total_cache_creation_tokens,
            "model_calls": scan.model_calls,
            "auto_allow_count": scan.auto_allow_count,
            "permission_mode": scan.permission_mode,
            "message_count": scan.message_count,
            "permission_event_count": scan.permission_event_count,
            "tool_error_count": scan.tool_error_count,
            "chapter_count": scan.chapter_count,
            "current_chapter_index": scan.current_chapter_index,
            "chip_count": scan.chip_count,
            "last_tool_use_name": scan.last_tool_use_name,
            "last_tool_use_summary": scan.last_tool_use_summary,
            "last_tool_use_file_path": scan.last_tool_use_file_path,
            "pending_tool_use_names_by_id": scan.pending_tool_use_names_by_id,
            "append_safe": append_safe,
        }
    }


# The append-resume prefix check lives in `_transcript_resume`, shared with
# the `codex_sessions` adapter. These are aliases, not wrappers: a fix to the
# shared module reaches both adapters, so the 64 KiB-window bug cannot be
# fixed in one copy and left standing in the other again -- which is exactly
# what happened between PR #872 (codex) and PACT
# `agent-sessions-resume-long-line` (here).
append_state_for_path = _transcript_resume.append_resume_state
prior_last_line_hash_matches = _transcript_resume.prior_last_line_hash_matches
path_ends_with_newline = _transcript_resume.path_ends_with_newline


def _scan_from_state(state: SessionScanState | None) -> SessionScan:
    if state is None:
        return SessionScan()
    return SessionScan(
        session_id=state.session_id,
        cwd=state.cwd,
        git_branch=state.git_branch,
        cc_version=state.cc_version,
        started_at=state.started_at,
        ended_at=state.ended_at,
        model=state.model,
        harness=state.harness,
        turn_count=state.turn_count,
        tool_call_count=state.tool_call_count,
        total_input_tokens=state.total_input_tokens,
        total_output_tokens=state.total_output_tokens,
        total_cache_read_tokens=state.total_cache_read_tokens,
        total_cache_creation_tokens=state.total_cache_creation_tokens,
        model_calls=state.model_calls,
        auto_allow_count=state.auto_allow_count,
        permission_mode=state.permission_mode,
        line_count=state.line_count,
        last_line_hash=state.last_line_hash,
        message_count=state.message_count,
        permission_event_start_index=state.permission_event_count,
        permission_event_count=state.permission_event_count,
        tool_error_count=state.tool_error_count,
        chapter_count=state.chapter_count,
        current_chapter_index=state.current_chapter_index,
        chip_count=state.chip_count,
        last_tool_use_name=state.last_tool_use_name,
        last_tool_use_summary=state.last_tool_use_summary,
        last_tool_use_file_path=state.last_tool_use_file_path,
        pending_tool_use_names_by_id=dict(state.pending_tool_use_names_by_id),
        append_delta=True,
    )


_line_hash = _transcript_resume.line_hash


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def scan_transcript(
    jsonl_path: Path,
    *,
    capture_tool_use_events: list[str] | None = None,
    initial_state: SessionScanState | None = None,
) -> SessionScan:
    """Stream-read a Claude Code JSONL transcript and return a SessionScan.

    Tolerant: malformed lines are logged and skipped, not raised.
    Unknown event types are ignored.

    Per `add-tool-use-event-capture` (2026-04-27): when
    ``capture_tool_use_events`` is set, every ``tool_use`` block whose
    ``name`` is in the list emits a :class:`ToolUseEventRecord`. Default
    (None / empty) emits zero native-tool records — the existing aggregate
    counters on :class:`SessionScan` remain the only native-tool signal.
    The allowlist is the operator-controlled fan-out knob for native
    harness tools. MCP calls are emitted whenever their server/operator
    namespace is parseable, so self-observation telemetry is not gated by
    the native-tool allowlist.
    """
    capture_set = (
        frozenset(capture_tool_use_events) if capture_tool_use_events else None
    )
    scan = _scan_from_state(initial_state)
    last_tool_use_summary = scan.last_tool_use_summary
    last_tool_use_name = scan.last_tool_use_name
    last_tool_use_file_path = scan.last_tool_use_file_path
    tool_use_names_by_id: dict[str, str] = dict(
        scan.pending_tool_use_names_by_id,
    )
    # Captured tool_use records awaiting their tool_result, keyed by the
    # transcript's tool_use id. Same-chunk matching only: a result landing
    # in a later incremental chunk leaves is_error=None (honest unknown).
    captured_tool_events_by_id: dict[str, ToolUseEventRecord] = {}
    chapter_idx_at_event = scan.current_chapter_index
    chip_index = scan.chip_count

    try:
        fh = open(jsonl_path, "rb")
    except OSError as e:
        logger.warning("Cannot open transcript %s: %s", jsonl_path, e)
        return scan

    with fh:
        if initial_state is not None:
            fh.seek(initial_state.bytes)
        for raw_line in fh:
            scan.line_count += 1
            line_no = scan.line_count
            scan.last_line_hash = _line_hash(raw_line.rstrip(b"\n"))
            line = raw_line.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Skipping malformed JSONL line %d in %s", line_no, jsonl_path)
                continue

            ev_type = ev.get("type")
            ts = ev.get("timestamp")

            # Capture session-level metadata once
            if scan.session_id is None and "sessionId" in ev:
                scan.session_id = ev["sessionId"]
            if scan.cwd is None and "cwd" in ev:
                scan.cwd = ev["cwd"]
            if scan.git_branch is None and "gitBranch" in ev:
                scan.git_branch = ev["gitBranch"]
            if scan.cc_version is None and "version" in ev:
                scan.cc_version = ev["version"]
            if ts:
                if scan.started_at is None or ts < scan.started_at:
                    scan.started_at = ts
                if scan.ended_at is None or ts > scan.ended_at:
                    scan.ended_at = ts

            if ev_type == "permission-mode":
                # Top-level mode change.
                # 2026-04-25 fix (debug-substrate-okg-self Phase 8):
                # the prior `timestamp=ts or ""` produced empty
                # strings when permission-mode events arrived
                # without a top-level `timestamp` field. The audit
                # found at least one such event. Fall back to the
                # scan's running-max timestamp (`scan.ended_at`) so
                # the permission_event always carries a usable time.
                new_mode = ev.get("permissionMode")
                if new_mode and new_mode != scan.permission_mode:
                    scan.permission_mode = new_mode
                    event_ts = ts or scan.ended_at or ""
                    scan.permission_events.append(
                        PermissionEventRecord(
                            kind="mode_changed",
                            timestamp=event_ts,
                            tool_name=None,
                            excerpt=f"permissionMode changed to {new_mode}",
                            tool_input_summary=None,
                            chapter_index=chapter_idx_at_event if chapter_idx_at_event >= 0 else None,
                        )
                    )
                    scan.permission_event_count += 1
                continue

            if ev_type == "system":
                message_index = scan.message_count
                scan.messages.append(
                    _message_record(
                        ev_type=ev_type,
                        ev=ev,
                        message_index=message_index,
                        line_index=line_no,
                        timestamp=ts or "",
                        tool_call_count=0,
                        chapter_index=(
                            chapter_idx_at_event
                            if chapter_idx_at_event >= 0 else None
                        ),
                    )
                )
                scan.message_count += 1
                _handle_system_event(ev, scan, chapter_idx_at_event)
                continue

            if ev_type == "assistant":
                scan.turn_count += 1
                turn_index = scan.turn_count - 1  # 0-based for tool_use_event ids
                message_index = scan.message_count
                block_idx_in_turn = 0
                tool_call_count_in_message = 0
                msg = ev.get("message") or {}
                if scan.model is None:
                    scan.model = msg.get("model")
                # Token usage
                usage = msg.get("usage") or {}
                if usage:
                    scan.model_calls += 1
                scan.total_input_tokens += int(usage.get("input_tokens") or 0)
                scan.total_output_tokens += int(usage.get("output_tokens") or 0)
                scan.total_cache_read_tokens += int(
                    usage.get("cache_read_input_tokens") or 0
                )
                scan.total_cache_creation_tokens += int(
                    usage.get("cache_creation_input_tokens") or 0
                )

                for block in msg.get("content") or []:
                    if not isinstance(block, dict):
                        continue
                    btype = block.get("type")
                    if btype != "tool_use":
                        continue
                    scan.tool_call_count += 1
                    tool_call_count_in_message += 1
                    tname = block.get("name") or ""
                    tinput = block.get("input") or {}
                    tool_use_id = block.get("id")
                    if tool_use_id:
                        tool_use_names_by_id[str(tool_use_id)] = str(tname)
                    captured_record: ToolUseEventRecord | None = None
                    last_tool_use_name = tname
                    last_tool_use_summary = _summarize_tool_input(tname, tinput)
                    last_tool_use_file_path = (
                        tinput.get("file_path") or tinput.get("filePath") or tinput.get("notebook_path")
                        if isinstance(tinput, dict) else None
                    )

                    # Opt-in fine-mode emission. The block_index
                    # tracks position among tool_use blocks in
                    # this turn (regardless of allowlist) so
                    # re-ingest produces stable ids even if the
                    # allowlist widens or narrows.
                    mcp_tool = parse_mcp_tool_name(str(tname))
                    if (
                        mcp_tool is not None
                        or (capture_set is not None and tname in capture_set)
                    ):
                        cmd_str: str | None = None
                        if tname == "Bash" and isinstance(tinput, dict):
                            raw_cmd = tinput.get("command")
                            if isinstance(raw_cmd, str):
                                cmd_str = scrub_transcript_text(
                                    _sanitize_graph_text(raw_cmd)
                                )[:500]
                        captured_record = ToolUseEventRecord(
                                tool_name=tname,
                                turn_index=turn_index,
                                block_index=block_idx_in_turn,
                                timestamp=ts or scan.ended_at or "",
                                file_path=(
                                    str(last_tool_use_file_path)
                                    if last_tool_use_file_path else None
                                ),
                                command=cmd_str,
                                input_excerpt=(
                                    last_tool_use_summary[:500]
                                    if last_tool_use_summary else None
                                ),
                                chapter_index=(
                                    chapter_idx_at_event
                                    if chapter_idx_at_event >= 0 else None
                                ),
                                message_index=message_index,
                                mcp_server=(
                                    mcp_tool.server if mcp_tool else None
                                ),
                                mcp_deployment=(
                                    mcp_tool.deployment if mcp_tool else None
                                ),
                                mcp_operator=(
                                    mcp_tool.operator if mcp_tool else None
                                ),
                            )
                        scan.tool_use_events.append(captured_record)
                        if tool_use_id:
                            captured_tool_events_by_id[str(tool_use_id)] = (
                                captured_record
                            )
                    block_idx_in_turn += 1

                    if tname == CHAPTER_TOOL:
                        chapter_index = scan.chapter_count
                        scan.chapters.append(
                            ChapterRecord(
                                title=scrub_transcript_text(
                                    str(tinput.get("title") or "")
                                )[:200],
                                summary=(
                                    scrub_transcript_text(str(tinput.get("summary")))
                                    [:500]
                                    if tinput.get("summary") else None
                                ),
                                index=chapter_index,
                                started_at=ts,
                            )
                        )
                        scan.chapter_count += 1
                        chapter_idx_at_event = chapter_index
                        scan.current_chapter_index = chapter_idx_at_event

                    elif tname == SPAWN_TASK_TOOL:
                        scan.chips.append(
                            ChipRecord(
                                title=scrub_transcript_text(
                                    str(tinput.get("title") or "")
                                )[:200],
                                prompt=scrub_transcript_text(
                                    str(tinput.get("prompt") or "")
                                )[:2000],
                                tldr=(
                                    scrub_transcript_text(str(tinput.get("tldr")))
                                    [:500]
                                    if tinput.get("tldr") else None
                                ),
                                index=chip_index,
                                chapter_index=chapter_idx_at_event if chapter_idx_at_event >= 0 else None,
                            )
                        )
                        chip_index += 1
                        scan.chip_count = chip_index

                    elif tname == SKILL_TOOL:
                        skill_name = str(tinput.get("skill") or "").strip()
                        if skill_name:
                            plugin = None
                            if ":" in skill_name:
                                plugin, skill_name = skill_name.split(":", 1)
                            scan.skills.append(SkillRecord(name=skill_name, plugin=plugin))

                    elif tname in FILE_EDIT_TOOLS:
                        fp = tinput.get("file_path") or tinput.get("filePath") or tinput.get("notebook_path")
                        if fp:
                            scan.file_edits.append(
                                FileEditRecord(file_path=str(fp), tool_name=tname)
                            )

                    elif tname == "Bash":
                        # Mine ssh hostnames from Bash commands so the
                        # cross-module REFERENCES to `host:<hostname>`
                        # (compute_env's RemoteHost) lands without
                        # requiring compute_env to also scan the JSONL.
                        cmd = tinput.get("command", "")
                        if isinstance(cmd, str):
                            for m in SSH_PATTERN.finditer(cmd):
                                username, host = m.group(1), m.group(2)
                                scan.ssh_hosts.append(
                                    SshHostRecord(hostname=host, username=username)
                                )

                scan.messages.append(
                    _message_record(
                        ev_type=ev_type,
                        ev=ev,
                        message_index=message_index,
                        line_index=line_no,
                        timestamp=ts or "",
                        tool_call_count=tool_call_count_in_message,
                        chapter_index=(
                            chapter_idx_at_event
                            if chapter_idx_at_event >= 0 else None
                        ),
                    )
                )
                scan.message_count += 1
                continue

            if ev_type == "user":
                message_index = scan.message_count
                msg = ev.get("message") or {}
                content = msg.get("content") if isinstance(msg, dict) else None
                external_tool_names = _external_tool_result_names(
                    content,
                    tool_use_names_by_id=tool_use_names_by_id,
                    fallback_tool_name=last_tool_use_name,
                )
                scan.messages.append(
                    _message_record(
                        ev_type=ev_type,
                        ev=ev,
                        message_index=message_index,
                        line_index=line_no,
                        timestamp=ts or "",
                        tool_call_count=0,
                        chapter_index=(
                            chapter_idx_at_event
                            if chapter_idx_at_event >= 0 else None
                        ),
                        untrusted_content_tool_names=external_tool_names,
                    )
                )
                scan.message_count += 1
                # Look for tool_result blocks indicating permission denials / hook blocks
                # Real-world transcripts sometimes have content as a string
                # (e.g., bare user prompts) — only iterate if it's a list of dicts
                if not isinstance(content, list):
                    continue
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    if block.get("type") != "tool_result":
                        continue
                    tool_use_id = block.get("tool_use_id")
                    captured = None
                    if tool_use_id is not None:
                        tool_use_names_by_id.pop(str(tool_use_id), None)
                        captured = captured_tool_events_by_id.pop(
                            str(tool_use_id), None
                        )
                    if not block.get("is_error"):
                        # Successful tool result implies an auto-allow (or at least no block)
                        scan.auto_allow_count += 1
                        if captured is not None:
                            captured.is_error = False
                        continue
                    scan.tool_error_count += 1
                    content = block.get("content")
                    excerpt = _content_to_excerpt(content)
                    pevent = _classify_permission_excerpt(
                        excerpt=excerpt,
                        ts=ts or "",
                        last_tool_name=last_tool_use_name,
                        last_tool_input_summary=last_tool_use_summary,
                        last_tool_file_path=last_tool_use_file_path,
                        chapter_index=chapter_idx_at_event if chapter_idx_at_event >= 0 else None,
                    )
                    if captured is not None:
                        # Native error signal on the event itself
                        # (reduce-agent-friction: friction analyses query
                        # attributes, not truncated text excerpts).
                        captured.is_error = True
                        captured.error_kind = (
                            pevent.kind if pevent is not None else "error"
                        )
                    if pevent is not None:
                        scan.permission_events.append(pevent)
                        scan.permission_event_count += 1
                continue

            # All other event types ignored at coarse mode
            # (file-history-snapshot, attachment, custom-title, etc.)

    scan.current_chapter_index = chapter_idx_at_event
    scan.chip_count = chip_index
    scan.last_tool_use_name = last_tool_use_name
    scan.last_tool_use_summary = last_tool_use_summary
    scan.last_tool_use_file_path = last_tool_use_file_path
    scan.pending_tool_use_names_by_id = dict(tool_use_names_by_id)
    return scan


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _handle_system_event(ev: dict, scan: SessionScan, chapter_idx: int) -> None:
    """Detect user interrupts in `system` / `local_command` events."""
    subtype = ev.get("subtype")
    if subtype != "local_command":
        return
    content = ev.get("content") or ""
    if not isinstance(content, str):
        return
    lower = content.lower()
    if any(k in lower for k in INTERRUPT_KEYWORDS):
        ts = ev.get("timestamp") or ""
        scan.permission_events.append(
            PermissionEventRecord(
                kind="interrupted",
                timestamp=ts,
                tool_name=None,
                excerpt=_sanitize_graph_text(content)[:200],
                tool_input_summary=None,
                chapter_index=chapter_idx if chapter_idx >= 0 else None,
            )
        )
        scan.permission_event_count += 1


def _message_record(
    *,
    ev_type: str,
    ev: dict,
    message_index: int,
    line_index: int,
    timestamp: str,
    tool_call_count: int,
    chapter_index: int | None,
    untrusted_content_tool_names: tuple[str, ...] = (),
) -> AgentMessageRecord:
    payload = _message_payload(ev_type, ev)
    origin = "web_tool_result" if untrusted_content_tool_names else None
    return AgentMessageRecord(
        role=_message_role(ev_type, ev),
        message_index=message_index,
        line_index=line_index,
        timestamp=timestamp,
        message_kind=_message_kind(ev_type, ev, tool_call_count),
        content_hash=_stable_content_hash(payload),
        content_excerpt=_message_excerpt(payload),
        tool_call_count=tool_call_count,
        parent_message_id=_parent_message_id(ev),
        chapter_index=chapter_index,
        untrusted_content_origin=origin,
        untrusted_content_tool_names=untrusted_content_tool_names,
    )


def _message_role(ev_type: str, ev: dict) -> str:
    msg = ev.get("message")
    if isinstance(msg, dict) and msg.get("role"):
        return str(msg.get("role"))
    if ev_type in {"assistant", "user", "system"}:
        return ev_type
    return "unknown"


def _assistant_content_is_encrypted_thinking(ev: dict) -> bool:
    """Return whether an assistant turn carries only encrypted reasoning.

    Claude Code emits ``{"type": "thinking", "thinking": "", "signature": ...}``
    when a turn's reasoning is encrypted: the plaintext is absent and is not
    recoverable. Such a turn has no tool calls and no readable text, so the
    tool-call test alone labels it ``assistant_text`` -- asserting it carries
    text when it carries none (#1326).

    Only the exact encrypted shape qualifies. A turn with readable thinking, any
    readable text, or any other block type is NOT this case, so a genuine future
    body loss keeps failing loudly instead of hiding in this bucket.
    """
    payload = _message_payload("assistant", ev)
    if not isinstance(payload, list) or not payload:
        return False
    saw_encrypted = False
    for block in payload:
        if not isinstance(block, dict):
            return False
        btype = block.get("type")
        if btype == "thinking":
            if str(block.get("thinking") or "").strip():
                return False
            if not block.get("signature"):
                return False
            saw_encrypted = True
            continue
        if btype == "text":
            if str(block.get("text") or "").strip():
                return False
            continue
        return False
    return saw_encrypted


def _message_kind(ev_type: str, ev: dict, tool_call_count: int) -> str:
    if ev_type == "assistant":
        if tool_call_count:
            return "assistant_tool_request"
        if _assistant_content_is_encrypted_thinking(ev):
            return "assistant_redacted_thinking"
        return "assistant_text"
    if ev_type == "user":
        payload = _message_payload(ev_type, ev)
        if _payload_has_block_type(payload, "tool_result"):
            return "tool_result"
        return "user_prompt"
    if ev_type == "system":
        subtype = ev.get("subtype")
        return f"system:{subtype}" if subtype else "system"
    return str(ev_type or "unknown")


def _message_payload(ev_type: str, ev: dict):
    if ev_type in {"assistant", "user"}:
        msg = ev.get("message")
        if isinstance(msg, dict):
            return msg.get("content", msg)
        return msg
    if ev_type == "system":
        return ev.get("content", ev)
    return ev


def _parent_message_id(ev: dict) -> str | None:
    for key in ("parentUuid", "parent_uuid", "parentMessageId", "parent_message_id"):
        value = ev.get(key)
        if value:
            return str(value)
    return None


def _stable_content_hash(payload) -> str:
    try:
        text = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    except TypeError:
        text = str(payload)
    text = scrub_transcript_text(text)
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()


def _message_excerpt(payload) -> str:
    text = _message_payload_text(payload)
    text = re.sub(r"\s+", " ", text).strip()
    text = scrub_transcript_text(text)
    return _sanitize_graph_text(text)[:500]


def _message_payload_text(payload) -> str:
    if isinstance(payload, str):
        return payload
    if isinstance(payload, list):
        parts: list[str] = []
        for block in payload:
            if isinstance(block, str):
                parts.append(block)
                continue
            if not isinstance(block, dict):
                continue
            btype = block.get("type")
            if btype in {"text", "thinking"}:
                parts.append(str(block.get("text") or block.get("thinking") or ""))
            elif btype == "tool_use":
                name = str(block.get("name") or "unknown")
                summary = _summarize_tool_input(name, block.get("input") or {})
                parts.append(f"tool_use:{name} {summary}".strip())
            elif btype == "tool_result":
                parts.append(_content_to_excerpt(block.get("content")))
        return "\n".join(p for p in parts if p)
    if isinstance(payload, dict):
        if "text" in payload:
            return str(payload.get("text") or "")
        if "content" in payload:
            return _message_payload_text(payload.get("content"))
        return json.dumps(payload, sort_keys=True, default=str)
    return ""


def _payload_has_block_type(payload, block_type: str) -> bool:
    if isinstance(payload, list):
        return any(
            isinstance(block, dict) and block.get("type") == block_type
            for block in payload
        )
    return False


def _external_tool_result_names(
    content: Any,
    *,
    tool_use_names_by_id: dict[str, str],
    fallback_tool_name: str | None,
) -> tuple[str, ...]:
    if not isinstance(content, list):
        return ()
    names: set[str] = set()
    for block in content:
        if not isinstance(block, dict) or block.get("type") != "tool_result":
            continue
        tool_name = ""
        tool_use_id = block.get("tool_use_id")
        if tool_use_id is not None:
            tool_name = tool_use_names_by_id.get(str(tool_use_id), "")
        if not tool_name:
            tool_name = str(block.get("name") or fallback_tool_name or "")
        if is_untrusted_content_tool(tool_name):
            names.add(tool_name)
    return tuple(sorted(names))


def _classify_permission_excerpt(
    *,
    excerpt: str,
    ts: str,
    last_tool_name: str | None,
    last_tool_input_summary: str | None,
    last_tool_file_path: str | None,
    chapter_index: int | None,
) -> PermissionEventRecord | None:
    """Classify a tool_result excerpt as a permission denial, hook block, or noise."""
    if not excerpt:
        return None
    m = PERMISSION_DENIED_RE.match(excerpt)
    if m:
        return PermissionEventRecord(
            kind="denied",
            timestamp=ts,
            tool_name=m.group(1),
            excerpt=excerpt[:200],
            tool_input_summary=last_tool_input_summary,
            chapter_index=chapter_index,
            tool_input_file_path=last_tool_file_path,
        )
    if HOOK_BLOCK_RE.match(excerpt) or HOOK_PREFIX_RE.match(excerpt):
        return PermissionEventRecord(
            kind="hook_blocked",
            timestamp=ts,
            tool_name=last_tool_name,
            excerpt=excerpt[:200],
            tool_input_summary=last_tool_input_summary,
            chapter_index=chapter_index,
            tool_input_file_path=last_tool_file_path,
        )
    return None


def _content_to_excerpt(content) -> str:
    """tool_result.content can be string, list-of-blocks, or dict; coerce to string."""
    if isinstance(content, str):
        return _sanitize_graph_text(scrub_transcript_text(content))
    if isinstance(content, list):
        # Concatenate text blocks
        parts = []
        for blk in content:
            if isinstance(blk, dict) and blk.get("type") == "text":
                parts.append(str(blk.get("text") or ""))
            elif isinstance(blk, str):
                parts.append(blk)
        return _sanitize_graph_text(scrub_transcript_text("\n".join(parts)))
    if isinstance(content, dict):
        return _sanitize_graph_text(
            scrub_transcript_text(json.dumps(content))
        )[:500]
    return ""


def _summarize_tool_input(tool_name: str, tinput: dict) -> str:
    """Compact one-line summary of a tool's input for PermissionEvent context."""
    if not isinstance(tinput, dict):
        return ""
    # Tool-specific shortcuts
    if tool_name == "Bash":
        cmd = str(tinput.get("command") or "")
        return _sanitize_graph_text(scrub_transcript_text(f"Bash: {cmd}"))[:200]
    if tool_name in ("Edit", "Write"):
        fp = tinput.get("file_path") or ""
        return f"{tool_name}: {fp}"
    if tool_name == "Read":
        return f"Read: {tinput.get('file_path') or ''}"
    # Fallback: first key=value pair, truncated
    keys = list(tinput.keys())
    if keys:
        k = keys[0]
        v = str(tinput[k])
        return _sanitize_graph_text(
            scrub_transcript_text(f"{tool_name}: {k}={v}")
        )[:200]
    return tool_name


def _sanitize_graph_text(text: str) -> str:
    """Remove characters Postgres cannot store inside JSON/text values."""
    return "".join(
        ch
        for ch in text
        if ch == "\n" or ch == "\t" or ord(ch) >= 32
    )


# ---------------------------------------------------------------------------
# Cwd / repo / OS helpers
# ---------------------------------------------------------------------------
def os_from_cwd(cwd: str | None) -> str | None:
    """Best-effort OS heuristic from a working-directory path."""
    if not cwd:
        return None
    if cwd.startswith("/Users/"):
        return "darwin"
    if cwd.startswith("/home/"):
        return "linux"
    if re.match(r"^[A-Z]:\\", cwd):
        return "windows"
    return None


def resolve_repo(cwd: str, cwd_to_repo: dict[str, str] | None = None) -> str | None:
    """Resolve a session's cwd to a repo short name.

    Order: explicit override (longest-prefix match), then cwd basename
    fallback. Returns None for empty cwd.
    """
    if not cwd:
        return None
    if cwd_to_repo:
        # Prefer exact match, then longest prefix
        if cwd in cwd_to_repo:
            return cwd_to_repo[cwd]
        for prefix, repo in sorted(cwd_to_repo.items(), key=lambda kv: -len(kv[0])):
            if cwd.startswith(prefix.rstrip("/") + "/") or cwd == prefix.rstrip("/"):
                return repo
    return Path(cwd).name or None


# ---------------------------------------------------------------------------
# Slug extraction from branch + chapter text
# ---------------------------------------------------------------------------
# Match an OpenSpec-style verb-led slug at the end of a branch, with
# an optional trailing worktree/session hash suffix (`-x<hex>` or
# `-<6+hex>`). Branches like `claude/thirsty-goodall-47c817` (worktree
# names without a verb prefix) deliberately do NOT match.
_BRANCH_SLUG_RE = re.compile(
    r"(?:^|/)((?:add|improve|update|remove|refactor|fix)-[\w-]+?)"
    r"(?:-[a-f0-9]{6,}|-x[a-f0-9]+)?$"
)
_INLINE_CHANGE_REF_RE = re.compile(r"\bchange:([\w-]+)\b")
_BARE_ADD_SLUG_RE = re.compile(r"\b(add-[\w-]+)\b")


def extract_proposal_slugs(branch: str | None, chapter_texts: list[str]) -> list[str]:
    """Return distinct proposal slugs referenced by branch + chapter content."""
    slugs: list[str] = []
    seen: set[str] = set()

    def _add(s: str) -> None:
        if s and s not in seen:
            seen.add(s)
            slugs.append(s)

    if branch:
        m = _BRANCH_SLUG_RE.search(branch)
        if m:
            _add(m.group(1))

    for text in chapter_texts:
        if not text:
            continue
        for m in _INLINE_CHANGE_REF_RE.finditer(text):
            _add(m.group(1))
        for m in _BARE_ADD_SLUG_RE.finditer(text):
            _add(m.group(1))

    return slugs


# ---------------------------------------------------------------------------
# Cold-start since parsing
# ---------------------------------------------------------------------------
_DURATION_RE = re.compile(r"^(\d+)([dh])$")


def parse_since(spec: str | None) -> datetime | None:
    """Parse a `since` spec into an aware UTC datetime, or None if unbounded.

    Accepts:
      - None / "" / "all"  → None (no filter)
      - "30d", "7d", "24h" → relative duration from now
      - ISO date "YYYY-MM-DD" → absolute date
    """
    from datetime import timedelta, timezone

    if not spec or spec.lower() == "all":
        return None

    m = _DURATION_RE.match(spec.strip())
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        delta = timedelta(days=n) if unit == "d" else timedelta(hours=n)
        return datetime.now(timezone.utc) - delta

    # ISO date
    try:
        return datetime.fromisoformat(spec).replace(tzinfo=timezone.utc) if "T" not in spec else datetime.fromisoformat(spec)
    except ValueError:
        logger.warning("Could not parse since=%r; ignoring filter", spec)
        return None
