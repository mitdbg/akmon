"""Hermetic fail-closed contracts for the grounded OpenWebUI API proof.

These run in the ordinary fast lane. The live driver itself is never imported
here: it needs Docker and a container, and importing it would turn any defect
in it into a collection error for unrelated tests. What is imported is the
pure contract module the driver and this test share, and the driver's source
text, read as text for the fabrication guard.
"""

from __future__ import annotations

import ast
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from xml.etree import ElementTree

import pytest

from tests.live.chat.scrub_junit_identity import scrub_junit_identity
from tests.live.chat.grounded_api_chain import (
    GENERATION,
    NODE_ID,
    PERSISTED_TOOL_NAME,
    SOURCE_REF,
    TOOL_NAME,
    ChainError,
    Refusal,
    Scrubber,
    assert_no_secret_shapes,
    driver_literal_findings,
    parse_task_acknowledgement,
    call_id_for,
    docker_run_command,
    persisted_chain,
    poll_until,
    read_jsonl,
    trim_recorded_log,
    validate_causal_chain,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
LIVE = REPO_ROOT / "tests" / "live" / "chat"
DRIVER = REPO_ROOT / "tests/live/chat/drive_grounded_api_turn.py"


SESSION = "0123456789abcdef0123456789abcdef"
WINDOW_START = datetime(2026, 9, 21, 1, 0, tzinfo=timezone.utc)
WINDOW_END = WINDOW_START + timedelta(minutes=5)
WINDOW = (WINDOW_START, WINDOW_END)
INSIDE = (WINDOW_START + timedelta(seconds=30)).isoformat()


def _validate(mcp, models, stored):
    return validate_causal_chain(
        mcp_rows=mcp,
        model_rows=models,
        stored=stored,
        session_id=SESSION,
        window=WINDOW,
    )


def _mcp() -> list[dict]:
    return [
        {
            "session": SESSION,
            "at": INSIDE,
            "kind": "mcp_call",
            "tool": TOOL_NAME,
            "arguments": {"node_id": NODE_ID},
            "result": {
                "pinned_generation_id": GENERATION,
                "items": [{"node_id": NODE_ID, "source_reference": SOURCE_REF}],
                "total_count": 1,
            },
        }
    ]


def _models() -> list[dict]:
    return [
        {
            "session": SESSION,
            "at": INSIDE,
            "kind": "model_request",
            "body": {
                "messages": [{"role": "user", "content": "inspect"}],
                "tools": [{"function": {"name": TOOL_NAME}}],
            },
        },
        {
            "session": SESSION,
            "at": INSIDE,
            "kind": "model_request",
            "body": {
                "messages": [
                    {"role": "user", "content": "inspect"},
                    {
                        "role": "tool",
                        "content": f"{GENERATION} {NODE_ID} {SOURCE_REF}",
                    },
                ]
            },
        },
    ]


#: Shaped as OpenWebUI really persists a native tool turn, copied from the
#: recorded run under the change's evidence directory (v0.11.0); the 0.11.4
#: compatibility image persisted the same shape on 2026-09-30
#: (pact/changes/live-chat-drivers-0114/evidence/grounded-0114/).
def _stored() -> dict:
    return {
        "role": "assistant",
        "done": True,
        "content": f"grounded {GENERATION} {NODE_ID} {SOURCE_REF}",
        "output": [
            {
                "type": "function_call",
                "id": call_id_for(SESSION),
                "call_id": call_id_for(SESSION),
                "name": PERSISTED_TOOL_NAME,
                "arguments": json.dumps({"node_id": NODE_ID}),
                "status": "completed",
            },
            {
                "type": "function_call_output",
                "call_id": call_id_for(SESSION),
                "status": "completed",
                "output": [
                    {
                        "type": "input_text",
                        "text": json.dumps(
                            {
                                "pinned_generation_id": GENERATION,
                                "items": [
                                    {"node_id": NODE_ID, "source_reference": SOURCE_REF}
                                ],
                            }
                        ),
                    }
                ],
            },
            {
                "type": "message",
                "role": "assistant",
                "status": "completed",
                "content": [
                    {"type": "output_text", "text": f"grounded {GENERATION}"}
                ],
            },
        ],
    }


def test_accepts_one_complete_causal_chain() -> None:
    result = _validate(_mcp(), _models(), _stored())
    assert result["mcp_calls"] == 1
    assert result["model_requests"] == 2
    assert result["tool_messages"] == 1
    assert result["mcp_tool"] == TOOL_NAME
    assert result["mcp_arguments"] == {"node_id": NODE_ID}
    assert result["stored_answer"] == _stored()["content"]
    assert result["persisted"]["function_call_name"] == PERSISTED_TOOL_NAME
    assert result["session_id"] == SESSION
    assert result["persisted"]["has_sources"] is False


#: arm -> the refusal it must draw. Matching the MESSAGE, not just the
#: exception type: a refusal for the wrong reason is a test that passes while
#: the guard it names is gone. Removing the two-request guard used to leave
#: every arm green, because a later check refused the same input for another
#: reason.
ARMS = {
    "mcp": "expected exactly one MCP call, recorded 0",
    "continuation": "model received 1 request",
    "one-body-already-holding-a-tool-message": "model received 1 request",
    "generation": f"stored assistant answer lost {GENERATION}",
    "node": f"stored assistant answer lost {NODE_ID}",
    "source": f"stored assistant answer lost {SOURCE_REF}",
    "refusal": "stored a refusal as the result",
    "assistant-role": "expected 'assistant'",
    "answer-text": "stored assistant answer has no text",
    "two-mcp-calls": "expected exactly one MCP call, recorded 2",
    "wrong-tool-name": "MCP call was 'read'",
    "wrong-tool-arguments": "MCP call arguments did not name the node",
    "tool-result-lost-evidence": f"MCP tool result did not carry {GENERATION}",
    "continuation-lost-evidence": f"OpenWebUI lost {GENERATION}",
    "no-tool-message": "no tool-result message",
    "no-persisted-output": "no output record of the tool turn",
    "persisted-call-missing": "records 0 function calls",
    "persisted-result-missing": "records 0 tool results",
    "persisted-wrong-tool": "expected exactly",
    "persisted-wrong-node": "arguments do not name the node",
    "persisted-result-orphaned": "does not belong to the stored call",
    "persisted-result-lost-evidence": f"stored tool result lost {GENERATION}",
    "stale-session-mcp-row": "carries session 'a-previous-run'",
    "stale-session-model-row": "carries session 'a-previous-run'",
    "mcp-row-before-the-window": "falls outside this run window",
    "model-row-after-the-window": "falls outside this run window",
    "mcp-row-without-a-session": "carries session None",
    "mcp-row-without-a-timestamp": "no readable timestamp",
    "mcp-row-timestamp-without-a-zone": "names no time zone",
    "persisted-near-miss-tool-name": "expected exactly",
    "persisted-call-id-from-another-run": "is not this run's",
    # A session that shares a long prefix with this run, and one that differs
    # only by case. Both must be refused, so a prefix or case-insensitive
    # comparison cannot pass for the exact one the contract promises.
    "session-sharing-a-long-prefix": "carries session '0123456789abcdef0123456789abcdee'",
    "session-differing-only-by-case": "carries session '0123456789ABCDEF0123456789ABCDEF'",
    # "a wrong or inexact tool name ... must fail" applies to the MCP-side
    # name too, not only the persisted one.
    "mcp-near-miss-tool-name": "MCP call was 'xinspect'",
    "mcp-tool-name-with-a-suffix": "MCP call was 'inspecting'",
    # 0.11.4 marks a failed tool call on both persisted items and stores its
    # output as {"error": ...}. The status refuses first, by name.
    "persisted-call-marked-failed": "stored function call has status 'failed'",
    "persisted-result-marked-failed": "stored tool result has status 'failed'",
    "persisted-result-without-a-status": "stored tool result has status None",
}


@pytest.mark.parametrize("broken", sorted(ARMS))
def test_each_broken_link_fails_closed(broken: str) -> None:
    mcp, models, stored = _mcp(), _models(), _stored()
    if broken == "mcp":
        mcp = []
    elif broken == "two-mcp-calls":
        mcp = mcp * 2
    elif broken == "wrong-tool-name":
        mcp[0]["tool"] = "read"
    elif broken == "wrong-tool-arguments":
        mcp[0]["arguments"] = {"node_id": "okg:fixture:node:other"}
    elif broken == "tool-result-lost-evidence":
        mcp[0]["result"]["pinned_generation_id"] = ""
    elif broken == "continuation":
        models = models[:1]
    elif broken == "no-tool-message":
        models[1]["body"]["messages"] = [{"role": "user", "content": "inspect"}]
    elif broken == "continuation-lost-evidence":
        models[1]["body"]["messages"][1]["content"] = f"{NODE_ID} {SOURCE_REF}"
    elif broken == "generation":
        stored["content"] = stored["content"].replace(GENERATION, "")
    elif broken == "node":
        stored["content"] = stored["content"].replace(NODE_ID, "")
    elif broken == "source":
        stored["content"] = stored["content"].replace(SOURCE_REF, "")
    elif broken == "refusal":
        stored["error"] = {"detail": "access denied"}
    elif broken == "assistant-role":
        stored["role"] = "tool"
    elif broken == "answer-text":
        stored["content"] = ""
    elif broken == "no-persisted-output":
        stored.pop("output")
    elif broken == "persisted-call-missing":
        stored["output"] = stored["output"][1:]
    elif broken == "persisted-result-missing":
        del stored["output"][1]
    elif broken == "persisted-wrong-tool":
        stored["output"][0]["name"] = "okg-grounded-fixture_read"
    elif broken == "persisted-wrong-node":
        stored["output"][0]["arguments"] = json.dumps({"node_id": "other"})
    elif broken == "persisted-result-orphaned":
        stored["output"][1]["call_id"] = "another-call"
    elif broken == "persisted-result-lost-evidence":
        stored["output"][1]["output"][0]["text"] = json.dumps({"items": []})
    elif broken == "one-body-already-holding-a-tool-message":
        # The continuation guard's own arm. Without it, a single request that
        # already carries a tool message satisfies every later check.
        models = models[1:]
    elif broken == "stale-session-mcp-row":
        mcp[0]["session"] = "a-previous-run"
    elif broken == "stale-session-model-row":
        models[1]["session"] = "a-previous-run"
    elif broken == "mcp-row-before-the-window":
        mcp[0]["at"] = (WINDOW_START - timedelta(seconds=1)).isoformat()
    elif broken == "model-row-after-the-window":
        models[1]["at"] = (WINDOW_END + timedelta(seconds=1)).isoformat()
    elif broken == "mcp-row-without-a-session":
        del mcp[0]["session"]
    elif broken == "mcp-row-without-a-timestamp":
        del mcp[0]["at"]
    elif broken == "mcp-row-timestamp-without-a-zone":
        mcp[0]["at"] = "2026-09-21T01:00:30"
    elif broken == "persisted-near-miss-tool-name":
        stored["output"][0]["name"] = f"{PERSISTED_TOOL_NAME}_fake_{TOOL_NAME}"
    elif broken == "session-sharing-a-long-prefix":
        mcp[0]["session"] = SESSION[:-1] + "e"
    elif broken == "session-differing-only-by-case":
        mcp[0]["session"] = SESSION.upper()
    elif broken == "mcp-near-miss-tool-name":
        mcp[0]["tool"] = f"x{TOOL_NAME}"
    elif broken == "mcp-tool-name-with-a-suffix":
        mcp[0]["tool"] = "inspecting"
    elif broken == "persisted-call-marked-failed":
        stored["output"][0]["status"] = "failed"
        stored["output"][1]["status"] = "failed"
        stored["output"][1]["output"] = [
            {"type": "input_text", "text": json.dumps({"error": "refused"})}
        ]
    elif broken == "persisted-result-marked-failed":
        stored["output"][1]["status"] = "failed"
    elif broken == "persisted-result-without-a-status":
        del stored["output"][1]["status"]
    elif broken == "persisted-call-id-from-another-run":
        other = call_id_for("ffffffffffffffffffffffffffffffff")
        stored["output"][0]["call_id"] = other
        stored["output"][1]["call_id"] = other
    else:  # pragma: no cover - a new arm name must not pass silently
        raise AssertionError(f"unhandled arm {broken!r}")
    with pytest.raises(ChainError, match=re.escape(ARMS[broken])):
        _validate(mcp, models, stored)


def test_read_jsonl_ignores_a_half_written_last_line(tmp_path: Path) -> None:
    """The recorder appends from another process while the driver polls."""
    path = tmp_path / "mcp.jsonl"
    path.write_text('{"kind": "a"}\n{"kind": "b"}\n{"kind": "c', encoding="utf-8")
    assert read_jsonl(path) == [{"kind": "a"}, {"kind": "b"}]


def test_read_jsonl_still_refuses_a_broken_earlier_line(tmp_path: Path) -> None:
    """A complete line that will not parse means the recorder is broken."""
    path = tmp_path / "model.jsonl"
    path.write_text('{"kind": "a"\n{"kind": "b"}\n', encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        read_jsonl(path)


def test_read_jsonl_on_a_missing_file_is_empty(tmp_path: Path) -> None:
    assert read_jsonl(tmp_path / "absent.jsonl") == []


def test_the_acknowledgement_must_name_this_chat() -> None:
    """What the vendor really returns: a queued background task, not a turn."""
    acknowledged = parse_task_acknowledgement(
        {"status": True, "task_ids": ["t-1"], "chat_id": "chat-1"},
        expected_chat_id="chat-1",
    )
    assert acknowledged == {
        "kind": "task_acknowledgement",
        "task_ids": 1,
        "chat_id": "chat-1",
        "keys": ["chat_id", "status", "task_ids"],
    }


@pytest.mark.parametrize(
    "payload",
    [
        "not json at all",
        None,
        {"status": False, "task_ids": ["t-1"], "chat_id": "chat-1"},
        {"status": True, "task_ids": [], "chat_id": "chat-1"},
        {"status": True, "chat_id": "chat-1"},
        {"status": True, "task_ids": ["t-1"], "chat_id": "someone-elses-chat"},
    ],
)
def test_a_refused_or_foreign_acknowledgement_fails_closed(payload: object) -> None:
    with pytest.raises(ChainError):
        parse_task_acknowledgement(payload, expected_chat_id="chat-1")


def test_persisted_sources_can_be_true_so_reporting_it_is_not_vacuous() -> None:
    """The earlier field could never be true; this one can."""
    stored = _stored()
    assert persisted_chain(stored, session_id=SESSION)["has_sources"] is False
    stored["sources"] = [{"source": {"name": "inspect"}}]
    assert persisted_chain(stored, session_id=SESSION)["has_sources"] is True


@pytest.mark.parametrize("edge", ["start", "end"])
def test_a_row_exactly_on_a_window_boundary_belongs_to_the_run(edge: str) -> None:
    """The window is closed at both ends.

    The driver takes the start before launching the fixtures and the end after
    reading the rows, so a row written in the same clock tick as a boundary is
    legitimate. Narrowing either bound would reject a correct run.
    """
    mcp = _mcp()
    mcp[0]["at"] = (WINDOW_START if edge == "start" else WINDOW_END).isoformat()
    assert _validate(mcp, _models(), _stored())["mcp_calls"] == 1


class _FakeClock:
    """A clock that only advances when the code under test sleeps."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_a_result_arriving_before_the_deadline_is_accepted() -> None:
    clock = _FakeClock()
    attempts = []

    def fetch() -> str:
        attempts.append(clock.now)
        return "ready" if len(attempts) == 3 else ""

    result = poll_until(
        fetch,
        lambda value: value == "ready",
        timeout=10,
        on_timeout=lambda value: ChainError(f"never ready, last {value!r}"),
        clock=clock,
        sleep=clock.sleep,
        interval=1.0,
    )
    assert result == "ready"
    assert len(attempts) == 3


def test_the_deadline_refuses_and_reports_the_last_reading() -> None:
    """"a timeout must fail" is a requirement, so it gets a test."""
    clock = _FakeClock()
    with pytest.raises(ChainError, match="never ready, last 'still-empty'"):
        poll_until(
            lambda: "still-empty",
            lambda value: value == "ready",
            timeout=5,
            on_timeout=lambda value: ChainError(f"never ready, last {value!r}"),
            clock=clock,
            sleep=clock.sleep,
            interval=1.0,
        )
    assert clock.now == 1005.0


def test_a_result_in_the_very_last_instant_still_counts() -> None:
    """The poll accepts the reading taken AT the deadline, not just before."""
    clock = _FakeClock()
    readings = iter(["", "", "ready"])
    assert (
        poll_until(
            lambda: next(readings),
            lambda value: value == "ready",
            timeout=2,
            on_timeout=lambda value: ChainError("gave up"),
            clock=clock,
            sleep=clock.sleep,
            interval=1.0,
        )
        == "ready"
    )


def test_a_zero_timeout_still_gets_one_attempt() -> None:
    _clock = _FakeClock()
    assert (
        poll_until(
            lambda: "ready",
            lambda value: value == "ready",
            timeout=0,
            on_timeout=lambda value: ChainError("gave up"),
            clock=_clock,
            sleep=_clock.sleep,
        )
        == "ready"
    )


def test_a_refusal_raised_by_on_timeout_keeps_its_own_type() -> None:
    """The three live deadlines raise two different types; both must survive."""
    clock = _FakeClock()
    with pytest.raises(Refusal, match="container never came up"):
        poll_until(
            lambda: "",
            bool,
            timeout=1,
            on_timeout=lambda value: Refusal("container never came up"),
            clock=clock,
            sleep=clock.sleep,
        )


def test_the_container_is_started_with_exactly_these_flags() -> None:
    """The manifest sentence about configuration is only as good as this list.

    It says the container runs with authentication on and is otherwise
    configured through its own HTTP API. Pinned exactly, so adding a mount, a
    second environment variable, or a patched-file bind cannot pass unnoticed
    and cannot quietly falsify that sentence.
    """
    assert docker_run_command(
        image="ghcr.io/open-webui/open-webui:v0.11.0",
        container="okg-s14-1183-openwebui",
        label="okg.acceptance=s14-1183",
        web_port=54321,
    ) == [
        "docker",
        "run",
        "-d",
        "--name",
        "okg-s14-1183-openwebui",
        "--label",
        "okg.acceptance=s14-1183",
        "-p",
        "127.0.0.1:54321:8080",
        "--add-host",
        "host.docker.internal:host-gateway",
        "-e",
        "WEBUI_AUTH=True",
        "ghcr.io/open-webui/open-webui:v0.11.0",
    ]


def test_every_driver_container_defines_the_host_alias_for_plain_docker(
    tmp_path: Path,
) -> None:
    """Plain Docker on Linux does not define host.docker.internal; podman does.

    Every URL the drivers give the site names that alias, so every container
    they start must define it, the way ``okg chat-instance up`` does for a
    bridge network. Without the flag a Docker run (the CI docker pods) cannot
    reach the fixture model or the MCP server even though podman passes.
    """
    from tests.live.chat import drive_instance

    flag = ["--add-host", "host.docker.internal:host-gateway"]
    grounded = docker_run_command(image="i:v1", container="c", label="l=1", web_port=1)
    compaction = drive_instance.container_run_args(tmp_path / "a", tmp_path / "v")
    for command in (grounded, compaction):
        index = command.index("--add-host")
        assert command[index : index + 2] == flag
        assert command.count("--add-host") == 1
        # Before the image: an argument after it goes to the container.
        assert index < len(command) - 1
        assert command[-1] in {"i:v1", drive_instance.OPENWEBUI_IMAGE}
    # The alias the flag defines is the one every driver URL names.
    for name in ("drive_grounded_api_turn.py", "drive_real_graph_turn.py", "drive_instance.py"):
        source = (LIVE / name).read_text("utf-8")
        assert "http://host.docker.internal" not in source, name
        assert "{HOST_GATEWAY_ALIAS}" in source, name


def test_the_container_is_never_given_a_mount_or_a_patched_file() -> None:
    """A bind mount would make "unpatched official image" untrue."""
    command = docker_run_command(
        image="i:v1.0.0", container="c", label="l=1", web_port=1
    )
    for forbidden in ("-v", "--volume", "--mount", "--entrypoint", "--privileged"):
        assert forbidden not in command
    assert command.count("-e") == 1
    assert command[-1] == "i:v1.0.0"
    assert "127.0.0.1:1:8080" in command


def test_the_log_trim_keeps_the_tail_and_declares_its_normalisation() -> None:
    """The tail is where a refused tool call would show up."""
    text = "\n".join(f"line {index}   " for index in range(10))
    trimmed = trim_recorded_log(text, lines=3)
    assert trimmed == "line 7\nline 8\nline 9\n"


def test_the_log_trim_bounds_the_character_budget_too() -> None:
    trimmed = trim_recorded_log("a" * 50, chars=10)
    assert trimmed == "a" * 10 + "\n"


def test_the_log_trim_of_an_empty_log_is_a_single_newline() -> None:
    assert trim_recorded_log("") == "\n"


def test_the_scrubber_hides_what_the_run_minted(tmp_path: Path) -> None:
    """The run creates a throwaway account; its values must not be recorded."""
    scrub = Scrubber()
    scrub.hide("s3cret-password-value", "<redacted:admin-password>")
    scrub.hide("/Users/somebody/checkout", "<repo>")
    target = tmp_path / "log.txt"
    scrub.write(target, "pw=s3cret-password-value at /Users/somebody/checkout/x")
    assert target.read_text("utf-8") == "pw=<redacted:admin-password> at <repo>/x"


def test_the_scrubber_refuses_a_value_too_short_to_replace_safely() -> None:
    """A three-character value would rewrite unrelated text."""
    scrub = Scrubber()
    scrub.hide("abc", "<redacted:x>")
    assert scrub("abcdef") == "abcdef"


@pytest.mark.parametrize(
    "leak",
    [
        # Named, because an unnamed parameter puts these fabricated strings
        # into the pytest node id, and the node id is copied into the junit
        # artifact this change commits as evidence. Nothing here is real; the
        # ids keep the committed record free of credential-shaped text.
        pytest.param(
            "authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.signature",
            id="json-web-token",
        ),
        pytest.param("OPENAI_API_KEY=sk-abcdefgh12345678", id="api-key"),
        pytest.param("Authorization: Bearer 0123456789abcdef", id="bearer-header"),
    ],
)
def test_a_surviving_credential_shape_fails_the_run(tmp_path: Path, leak: str) -> None:
    """The check must be able to fire, or the clean result proves nothing."""
    (tmp_path / "artifact.txt").write_text(leak, encoding="utf-8")
    with pytest.raises(Refusal):
        assert_no_secret_shapes(tmp_path)


def test_a_clean_artifact_set_passes_and_names_what_it_checked(
    tmp_path: Path,
) -> None:
    (tmp_path / "verdict.json").write_text('{"ok": true}', encoding="utf-8")
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "run.json").write_text("{}", encoding="utf-8")
    assert assert_no_secret_shapes(tmp_path) == ["nested/run.json", "verdict.json"]


def test_the_driver_authors_no_answer_and_writes_no_finished_turn() -> None:
    """The driver must receive the evidence, never type it.

    Read through the Python parser, so this holds for any spelling of the
    mistake rather than the two substrings one earlier attempt used.
    """
    assert driver_literal_findings(DRIVER.read_text("utf-8")) == []


@pytest.mark.parametrize(
    ("literal", "expected"),
    [
        ('"/api/chat/completed"', "finished-turn write endpoint"),
        (f'"the answer is {GENERATION}"', "evidence value"),
        (f'"node {NODE_ID}"', "evidence value"),
        (f'"source {SOURCE_REF}"', "evidence value"),
    ],
)
def test_the_fabrication_guard_catches_a_driver_that_cheats(
    literal: str, expected: str
) -> None:
    findings = driver_literal_findings(f"value = {literal}\n")
    assert findings, literal
    assert expected in findings[0]


def test_a_raising_fetch_propagates_unchanged() -> None:
    """Pinned behaviour, not an accident.

    No live caller wants a failing read retried until the deadline hides it,
    so poll_until deliberately has no tolerate-list and lets the error out
    with its own message.
    """
    clock = _FakeClock()

    def fetch() -> str:
        raise RuntimeError("the chat read failed outright")

    with pytest.raises(RuntimeError, match="the chat read failed outright"):
        poll_until(
            fetch,
            bool,
            timeout=30,
            on_timeout=lambda value: ChainError("should never be reached"),
            clock=clock,
            sleep=clock.sleep,
        )
    assert clock.now == 1000.0, "it must fail on the first attempt, not retry"


def test_the_junit_scrub_removes_the_machine_name_and_nothing_else(
    tmp_path: Path,
) -> None:
    report = tmp_path / "report.xml"
    report.write_text(
        '<?xml version="1.0" encoding="utf-8"?><testsuites name="pytest tests">'
        '<testsuite name="pytest" tests="2" failures="0" errors="0" '
        'hostname="somebodys-laptop.local" time="1.5">'
        '<testcase classname="a" name="b" time="0.1" /></testsuite></testsuites>',
        encoding="utf-8",
    )
    assert scrub_junit_identity(report) == ["somebodys-laptop.local"]
    after = report.read_text("utf-8")
    assert "somebodys-laptop" not in after
    assert "hostname" not in after
    suite = ElementTree.parse(report).getroot()[0]
    assert suite.get("tests") == "2"
    assert suite.get("failures") == "0"
    assert suite.get("time") == "1.5"
    assert suite[0].get("name") == "b"
    assert scrub_junit_identity(report) == [], "a second pass finds nothing left"


def _driver_tree() -> ast.Module:
    return ast.parse(DRIVER.read_text("utf-8"))


def test_the_driver_bounds_every_wait_with_the_tested_primitive() -> None:
    """The deadline arms are only proved if the driver actually uses them.

    Three waits: the container coming up, the recorder files filling, and the
    assistant answer being persisted. A hand-rolled loop here would leave a
    deadline nothing tests.
    """
    tree = _driver_tree()
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "poll_until"
    ]
    assert len(calls) == 3, f"expected three poll_until sites, found {len(calls)}"
    # The chat read must happen ONLY through the bounded wait. Counting the
    # references catches a hand-rolled read placed beside a still-present
    # poll_until call, which the site count alone cannot see.
    reads = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id == "_read_stored"
    ]
    assert len(reads) == 1, (
        f"_read_stored is referenced {len(reads)} times; it must be reached "
        "only as the bounded wait's fetch"
    )
    for node in ast.walk(tree):
        if isinstance(node, ast.While):
            assert not (
                isinstance(node.test, ast.Constant) and node.test.value is True
            ), f"line {node.lineno}: an unbounded loop in the driver"


def test_every_driver_subprocess_spawn_is_session_isolated() -> None:
    """Each Popen leads its own session and does not hold the parent's stdin.

    A child left in the pytest-xdist worker's process group can signal the
    worker and make a required lane nondeterministic (#703). The repository
    lint in tests/substrate/test_xdist_spawn_isolation_lint.py scans the whole
    test tree for this and caught it here after review had passed; this local
    test keeps the driver honest without waiting for hosted CI.
    """
    spawns = [
        node
        for node in ast.walk(_driver_tree())
        if isinstance(node, ast.Call)
        and (
            (isinstance(node.func, ast.Attribute) and node.func.attr == "Popen")
            or (isinstance(node.func, ast.Name) and node.func.id == "Popen")
        )
    ]
    assert spawns, "the driver must spawn the fixture; this test guards how"
    for node in spawns:
        keywords = {
            keyword.arg: keyword.value
            for keyword in node.keywords
            if keyword.arg is not None
        }
        session = keywords.get("start_new_session")
        assert (
            isinstance(session, ast.Constant) and session.value is True
        ), f"line {node.lineno}: Popen without start_new_session=True"
        stdin = keywords.get("stdin")
        assert (
            isinstance(stdin, ast.Attribute) and stdin.attr == "DEVNULL"
        ), f"line {node.lineno}: Popen must not inherit the parent's stdin"
        assert "stdout" in keywords, (
            f"line {node.lineno}: Popen must redirect stdout off the parent stream"
        )


def test_the_driver_stops_the_whole_fixture_group() -> None:
    """Teardown must signal the group, or a grandchild keeps a port."""
    tree = _driver_tree()
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "killpg"
        for node in ast.walk(tree)
    ), "teardown must signal the fixture's process group, not just the child"
    called = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "terminate_process_group"
    ]
    assert len(called) == 1, "the driver must tear the group down exactly once"


def test_the_driver_closes_the_window_after_reading_the_rows() -> None:
    """Ordering, because the other order rejects correct runs.

    If the end of the window is taken before the recorder files are read, a
    row appended between the two lands outside the window and fails a run that
    was in fact right. Review caught exactly that, so the order is pinned.
    """
    tree = _driver_tree()
    reads = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "read_jsonl"
    ]
    windows = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "window"
            for target in node.targets
        )
    ]
    assert reads, "the driver must read the recorder rows into names"
    assert len(windows) == 1, "the window must be closed in exactly one place"
    assert max(reads) < windows[0], (
        f"rows are read at {sorted(reads)} but the window closes at "
        f"{windows[0]}; a row written in between would be refused"
    )


def test_the_driver_starts_the_container_from_the_tested_command() -> None:
    """The recorded flag list must be the one actually executed.

    The manifest describes how the container is configured, and a test pins
    docker_run_command exactly. That only means something if the driver runs
    its return value rather than a second list built inline.
    """
    tree = _driver_tree()
    built = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "docker_run_command"
    ]
    assert len(built) == 1, "the command must be built in exactly one place"
    assigned = {
        target.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "docker_run_command"
    }
    assert len(assigned) == 1, "its result must be bound to one name"
    name = assigned.pop()
    ran = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_run"
        and node.args
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id == name
    ]
    assert ran, f"the driver never runs {name}"
    for node in ast.walk(tree):
        if isinstance(node, ast.List) and node.elts:
            first = node.elts[0]
            if isinstance(first, ast.Constant) and first.value == "docker":
                rest = [
                    element.value
                    for element in node.elts[1:2]
                    if isinstance(element, ast.Constant)
                ]
                assert rest != ["run"], (
                    f"line {node.lineno}: a second inline docker run command"
                )


# --------------------------------------------------------------------------
# the image default and the port block (live-chat-drivers-0114)
# --------------------------------------------------------------------------


def test_the_expected_digest_defaults_from_a_digest_reference() -> None:
    from okg.substrate.chat.instance import PINNED_IMAGE_REF
    from tests.live.chat.grounded_api_chain import digest_of

    assert PINNED_IMAGE_REF.endswith("@" + digest_of(PINNED_IMAGE_REF))


@pytest.mark.parametrize(
    "image",
    [
        "ghcr.io/open-webui/open-webui:v0.11.4",
        "repo@sha256:short",
        "repo@sha512:" + "0" * 64,
    ],
)
def test_a_reference_naming_no_digest_refuses(image: str) -> None:
    from tests.live.chat.grounded_api_chain import Refusal, digest_of

    with pytest.raises(Refusal, match="names no registry digest"):
        digest_of(image)


def test_both_drivers_default_to_the_product_pin_not_a_copy() -> None:
    """The drivers import the product constant; no second digest literal."""
    from okg.substrate.chat.instance import PINNED_IMAGE_REF, TESTED_IMAGES

    assert PINNED_IMAGE_REF in TESTED_IMAGES
    digest = PINNED_IMAGE_REF.partition("@")[2].split(":")[1]
    for name in ("drive_grounded_api_turn.py", "drive_real_graph_turn.py", "drive_instance.py"):
        source = (LIVE / name).read_text("utf-8")
        assert "PINNED_IMAGE_REF" in source, name
        assert digest not in source, f"{name} carries its own copy of the pin"
        assert "open-webui:v0.11.0" not in source, name


def test_the_port_range_is_parsed_or_refused() -> None:
    from tests.live.chat.grounded_api_chain import Refusal, parse_port_range

    assert parse_port_range("5600-5699") == (5600, 5699)
    for bad in ("5600", "5699-5600", "80-90", "a-b", "", "5600-70000"):
        with pytest.raises(Refusal, match="LOW-HIGH"):
            parse_port_range(bad)


def test_a_port_is_picked_inside_the_range_and_skips_taken_ones() -> None:
    import socket

    from tests.live.chat.grounded_api_chain import Refusal, free_port

    with socket.socket() as holder:
        holder.bind(("127.0.0.1", 0))
        busy = holder.getsockname()[1]
        holder.listen()
        low = free_port()
        picked = free_port((low, low), taken=())
        assert picked == low
        with pytest.raises(Refusal, match="no free port"):
            free_port((low, low), taken=(low,))
        with pytest.raises(Refusal, match="no free port"):
            free_port((busy, busy))
