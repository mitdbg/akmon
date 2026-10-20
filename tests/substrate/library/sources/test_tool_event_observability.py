"""Per `reduce-agent-friction` req.friction.event-observability.

tool_use_event nodes carry a native error signal from both session
sources, and session rollups derive tool_error_count from events instead
of leaving friction text-scraped.
"""

from __future__ import annotations

import json
from pathlib import Path

from okg.substrate.library.sources._agent_sessions_extract import (
    scan_transcript,
)
from okg.substrate.library.sources.codex_sessions import _parse_transcript


def _write_jsonl(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )
    return path


def _claude_transcript(tmp_path: Path) -> Path:
    base = {
        "sessionId": "obs-fixture",
        "cwd": "/repo",
        "version": "test",
        "timestamp": "2026-07-02T00:00:00Z",
    }

    def tool_use(tid: str, name: str, cmd: str) -> dict:
        return {
            **base,
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [{
                    "type": "tool_use", "id": tid, "name": name,
                    "input": {"command": cmd},
                }],
            },
        }

    def tool_result(tid: str, content: str, is_error: bool) -> dict:
        block: dict = {
            "type": "tool_result", "tool_use_id": tid, "content": content,
        }
        if is_error:
            block["is_error"] = True
        return {
            **base,
            "type": "user",
            "message": {"role": "user", "content": [block]},
        }

    return _write_jsonl(tmp_path / "claude" / "obs-fixture.jsonl", [
        tool_use("t1", "Bash", "echo ok"),
        tool_result("t1", "ok", is_error=False),
        tool_use("t2", "Bash", "explode"),
        tool_result("t2", "boom: exploded", is_error=True),
        tool_use("t3", "Bash", "cat <<EOF"),
        tool_result(
            "t3",
            "Blocked opaque Bash command. Reasons: heredoc",
            is_error=True,
        ),
        # The harness's actual wrapper form — kept hook_block_count
        # structurally zero until the 2026-07-02 real-corpus verification.
        tool_use("t4", "Bash", "a && b && c && d"),
        tool_result(
            "t4",
            "PreToolUse:Bash hook error: [python3 check.py]: Blocked "
            "opaque Bash command. Reasons: 3 statement separators",
            is_error=True,
        ),
    ])


def test_claude_tool_events_carry_native_error_signal(tmp_path) -> None:
    path = _claude_transcript(tmp_path)
    scan = scan_transcript(path, capture_tool_use_events=["Bash"])
    events = {
        (e.turn_index, e.block_index): e for e in scan.tool_use_events
    }
    flags = sorted(
        (e.is_error, e.error_kind)
        for e in scan.tool_use_events
    )
    assert (False, None) in flags, "successful result must mark is_error False"
    assert (True, "error") in flags, "hard error must mark is_error True"
    assert (True, "hook_blocked") in flags, (
        "hook-blocked result must carry the classified error_kind"
    )
    assert scan.tool_error_count == 3
    assert len(events) == 4
    # both hook-block spellings classify (bare "Blocked ..." and the
    # harness's "PreToolUse:<Tool> hook error: ..." wrapper)
    assert sum(
        1 for p in scan.permission_events if p.kind == "hook_blocked"
    ) == 2


def test_codex_tool_events_carry_exit_code(tmp_path) -> None:
    rows = [
        {
            "timestamp": "2026-07-02T00:00:00Z",
            "type": "session_meta",
            "payload": {"id": "codex-obs", "cwd": "/repo"},
        },
        {
            "timestamp": "2026-07-02T00:00:01Z",
            "type": "response_item",
            "payload": {
                "type": "function_call", "name": "shell", "call_id": "c1",
                "arguments": json.dumps({"cmd": "true"}),
            },
        },
        {
            "timestamp": "2026-07-02T00:00:02Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output", "call_id": "c1",
                # the REAL corpus shape: exit status as wrapper text
                "output": "ok\n\nProcess exited with code 0",
            },
        },
        {
            "timestamp": "2026-07-02T00:00:03Z",
            "type": "response_item",
            "payload": {
                "type": "function_call", "name": "shell", "call_id": "c2",
                "arguments": json.dumps({"cmd": "false"}),
            },
        },
        {
            "timestamp": "2026-07-02T00:00:04Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output", "call_id": "c2",
                "output": "boom\n\nProcess exited with code 1",
            },
        },
    ]
    path = _write_jsonl(tmp_path / "codex" / "codex-obs.jsonl", rows)
    parsed = _parse_transcript(path, allowlist={"shell"})
    events = parsed["tool_events"]
    assert len(events) == 2
    by_exit = {e.exit_code: e for e in events}
    assert by_exit[0].is_error is False
    assert by_exit[1].is_error is True
    assert parsed["tool_error_count"] == 1


def test_codex_output_shapes_are_honest_unknowns() -> None:
    from okg.substrate.library.sources.codex_sessions import (
        _parse_call_output,
    )

    assert _parse_call_output(None) == (None, None)
    # the real corpus shape: exit status as wrapper text (29.9k nonzero
    # occurrences on the 2026-07-02 audit); the first parser version
    # ignored it and defaulted unknown text to success — the exact
    # silent-default P0 the adversarial review refuted
    assert _parse_call_output("out\nProcess exited with code 2") == (2, True)
    assert _parse_call_output("ok\nProcess exited with code 0") == (0, False)
    assert _parse_call_output(
        '{"output": "x\\nProcess exited with code 127"}'
    ) == (127, True)
    assert _parse_call_output('{"metadata": {"exit_code": 7}}') == (7, True)
    assert _parse_call_output('{"success": true}') == (None, False)
    assert _parse_call_output("Error: no such tool") == (None, True)
    # unrecognized text is an HONEST UNKNOWN, never a guessed success
    assert _parse_call_output("plain output text") == (None, None)
