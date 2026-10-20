"""Setup's test question against RECORDED Open WebUI 0.11.0 responses (#2907).

The fixtures were captured from the pinned image
(``okg-openwebui-compat@sha256:e0b1f475...``, version 0.11.0) serving a real
okg graph with qwen3.8:27B, then scrubbed for the public mirror (deployment
and source names, session ids, query text):

* ``openwebui_v0_11_0_probe_nonstream_answer.json`` -- what a non-streamed
  completion with the graph tools attached answers: the model's tool call,
  handed back unexecuted (``finish_reason: tool_calls``, no ``sources``).
  The probe used to ask this way and could never see a tool run.
* ``openwebui_v0_11_0_probe_stored_chat.json`` -- the stored chat after a
  streamed completion that named the chat and the assistant message: the
  graph tool ran and its result sits in the message's ``output`` items.
* ``openwebui_v0_11_0_probe_refused_calls.json`` -- three graph calls the
  graph REFUSED, as stored output items: one refusal returned as an ordinary
  JSON result, two through the MCP error flag (stored as a Python dict
  literal).

:class:`RecordedInstance` answers each request the way the recorded instance
did, keyed on the request's shape, so the probe's verdict is decided by real
vendor bytes rather than by what a fake chooses to return.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat.setup import (
    REASON_TOOL_USE_UNOBSERVED,
    STATUS_FAILED,
    STATUS_OK,
    probe_conversation,
)

FIXTURES = Path(__file__).parent / "fixtures"
STORED = json.loads((FIXTURES / "openwebui_v0_11_0_probe_stored_chat.json").read_text("utf-8"))
NONSTREAM = json.loads(
    (FIXTURES / "openwebui_v0_11_0_probe_nonstream_answer.json").read_text("utf-8")
)
REFUSED = json.loads(
    (FIXTURES / "openwebui_v0_11_0_probe_refused_calls.json").read_text("utf-8")
)
PRESET = "okg-example-kb"
TOOL_ID = f"server:mcp:{PRESET}"
QUESTION = "What kinds of records does this graph hold?"
#: The vendor routes, spelled out so this file also runs against the code
#: before #2907 (the fail-before arm).
PATH_CHAT_NEW = "/api/v1/chats/new"
PATH_COMPLETIONS = "/api/chat/completions"


class RecordedInstance:
    """Answers like the recorded 0.11.0 instance; records every call."""

    def __init__(self, *, stored_message: dict[str, Any] | None = None) -> None:
        self.calls: list[tuple[str, str, Any]] = []
        self._message = copy.deepcopy(
            stored_message
            if stored_message is not None
            else STORED["row"]["chat"]["history"]["messages"][STORED["assistant_id"]]
        )
        self._assistant_id: str | None = None

    def call(self, method: str, path: str, payload: Any = None, *, secrets: Any = ()) -> Any:
        self.calls.append((method, path, payload))
        if (method, path) == ("POST", PATH_CHAT_NEW):
            return {"id": STORED["chat_id"], "title": payload["chat"]["title"], "chat": {}}
        if (method, path) == ("POST", PATH_COMPLETIONS):
            if payload.get("chat_id") == STORED["chat_id"] and payload.get("stream") is True:
                self._assistant_id = payload["id"]
                return None  # measured: HTTP 200, body `null`
            return copy.deepcopy(NONSTREAM["answer"])
        if (method, path) == ("GET", f"/api/v1/chats/{STORED['chat_id']}"):
            row = copy.deepcopy(STORED["row"])
            messages = row["chat"]["history"]["messages"]
            messages.pop(STORED["assistant_id"])
            if self._assistant_id is not None:
                messages[self._assistant_id] = {**self._message, "id": self._assistant_id}
            return row
        raise AssertionError(f"unrecorded request {method} {path}")


def _probe(instance: RecordedInstance) -> tuple[Any, dict[str, Any]]:
    return probe_conversation(
        instance, preset_id=PRESET, tool_id=TOOL_ID, question=QUESTION,
    )


def test_the_recorded_nonstreamed_answer_is_an_unexecuted_tool_call() -> None:
    """The fixture is what #2907 saw: a tool call came back, nothing ran."""
    choice = NONSTREAM["answer"]["choices"][0]
    assert choice["finish_reason"] == "tool_calls"
    assert choice["message"]["tool_calls"][0]["function"]["name"] == f"{PRESET}_inspect"
    assert "sources" not in NONSTREAM["answer"]


def test_a_recorded_011_tool_run_is_ready() -> None:
    """Fail-before: the probe asked without a chat and without streaming, got
    the unexecuted call above, and reported tool_use_unobserved."""
    instance = RecordedInstance()

    step, conversation = _probe(instance)

    assert step.status == STATUS_OK, step.detail
    assert conversation["ready"] is True
    assert conversation["tools_invoked"] == [f"{PRESET}_inspect"]
    assert conversation["tool_call_count"] == 1
    assert conversation["tool_result_count"] == 1
    assert conversation["generation_evidence"] is True
    assert conversation["chat_id"] == STORED["chat_id"]
    assert conversation["reply_excerpt"].startswith("This graph")
    (completion,) = [p for m, path, p in instance.calls if path == PATH_COMPLETIONS]
    assert completion["stream"] is True
    assert completion["chat_id"] == STORED["chat_id"]


def test_a_recorded_reply_without_its_tool_run_is_not_ready() -> None:
    """The same recorded answer with the tool-call items removed is a model
    answering from its own knowledge (#1183): never ready."""
    message = copy.deepcopy(
        STORED["row"]["chat"]["history"]["messages"][STORED["assistant_id"]]
    )
    message["output"] = [
        item for item in message["output"]
        if item["type"] not in ("function_call", "function_call_output")
    ]
    instance = RecordedInstance(stored_message=message)

    step, conversation = _probe(instance)

    assert step.status == STATUS_FAILED
    assert conversation["ready"] is False
    assert conversation["reason"] == REASON_TOOL_USE_UNOBSERVED
    assert conversation["replied"] is True
    assert conversation["tools_invoked"] == []


def _with_output(items: list[dict[str, Any]]) -> dict[str, Any]:
    message = copy.deepcopy(
        STORED["row"]["chat"]["history"]["messages"][STORED["assistant_id"]]
    )
    reply = [item for item in message["output"] if item["type"] == "message"]
    message["output"] = copy.deepcopy(items) + reply
    return message


def test_recorded_refusals_alone_are_not_ready() -> None:
    """Fail-before (#2911 review): every recorded refusal parsed as a graph
    answer, so a graph that refused every call was reported ready."""
    instance = RecordedInstance(stored_message=_with_output(REFUSED["output"]))

    step, conversation = _probe(instance)

    assert step.status == STATUS_FAILED
    assert conversation["ready"] is False
    assert conversation["reason"] == "graph_tool_calls_refused"
    assert conversation["tool_call_count"] == 3
    assert conversation["tool_refusal_count"] == 3
    assert conversation["refusal_codes"] == [
        "invalid_operator_arguments", "select_unknown_field", "semantic_search_unavailable",
    ]
    # The refusals carry a session generation id; it is not evidence of a read.
    assert conversation["generation_evidence"] is False


def test_recorded_refusals_beside_a_recorded_answer_are_ready() -> None:
    answered = [
        item
        for item in STORED["row"]["chat"]["history"]["messages"][STORED["assistant_id"]]["output"]
        if item["type"] in ("function_call", "function_call_output")
    ]
    instance = RecordedInstance(stored_message=_with_output(REFUSED["output"] + answered))

    step, conversation = _probe(instance)

    assert step.status == STATUS_OK, step.detail
    assert conversation["tools_invoked"] == [f"{PRESET}_inspect"]
    assert conversation["tool_result_count"] == 1
    assert conversation["tool_refusal_count"] == 3
    assert conversation["generation_evidence"] is True


# -- the same refusals as Open WebUI 0.11.4 stores them ----------------------
#
# 0.11.4 catches a raising tool as ``result = {'error': str(e)}``
# (utils/middleware.py:6027, revision 8bd8b4fa) where 0.11.0 stored
# ``str(e)`` (middleware.py:5006), serializes the dict with
# ``tool_result_content`` (:313-318), and marks the output ``failed``
# (:6124-6127). The recorded 0.11.0 bytes are re-wrapped EXACTLY that way:
# the inner text is the recorded ``str(e)``, unchanged.


def _as_0114(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = copy.deepcopy(items)
    for item in out:
        if item["type"] != "function_call_output":
            continue
        text = item["output"][0]["text"]
        try:
            json.loads(text)
        except ValueError:  # the MCP error flag: 0.11.0 stored str(e)
            item["output"][0]["text"] = json.dumps({"error": text}, ensure_ascii=False)
            item["status"] = "failed"
    return out


def test_0114_error_flag_refusals_are_refusals_not_answers() -> None:
    """Fail-before: the 0.11.4 ``{"error": "<dict literal>"}`` parsed as plain
    JSON, carried no ``status: refused``, and counted as a graph ANSWER, so a
    graph that refused every call was reported ready."""
    rewrapped = _as_0114(REFUSED["output"])
    assert sum(1 for i in rewrapped if i.get("status") == "failed") == 2
    instance = RecordedInstance(stored_message=_with_output(rewrapped))

    step, conversation = _probe(instance)

    assert step.status == STATUS_FAILED
    assert conversation["ready"] is False
    assert conversation["reason"] == "graph_tool_calls_refused"
    assert conversation["tool_refusal_count"] == 3
    assert conversation["refusal_codes"] == [
        "invalid_operator_arguments", "select_unknown_field", "semantic_search_unavailable",
    ]


def test_0114_error_text_that_is_not_a_graph_object_is_a_failure() -> None:
    """A call that never reached the graph (connection refused) is stored as
    ``{"error": "<exception text>"}`` at 0.11.4: a failure, never a refusal
    and never an answer."""
    call = {
        "type": "function_call", "id": "call_x", "call_id": "call_x",
        "name": f"{PRESET}_search", "arguments": "{}", "status": "failed",
    }
    output = {
        "type": "function_call_output", "id": "fco_x", "call_id": "call_x",
        "output": [{"type": "input_text", "text": json.dumps(
            {"error": "All connection attempts failed"}
        )}],
        "status": "failed",
    }
    instance = RecordedInstance(stored_message=_with_output([call, output]))

    step, conversation = _probe(instance)

    assert step.status == STATUS_FAILED
    assert conversation["ready"] is False
    assert conversation["reason"] == "graph_tool_calls_failed"
    assert conversation["tool_refusal_count"] == 0


def _one_call(text: str, *, status: str = "completed") -> list[dict[str, Any]]:
    return [
        {
            "type": "function_call", "id": "call_y", "call_id": "call_y",
            "name": f"{PRESET}_inspect", "arguments": "{}", "status": status,
        },
        {
            "type": "function_call_output", "id": "fco_y", "call_id": "call_y",
            "output": [{"type": "input_text", "text": text}],
            "status": status,
        },
    ]


@pytest.mark.parametrize("text", [
    json.dumps({"status": "error", "detail": "backend down", "generation": "g1"}),
    json.dumps({"error": {"message": "boom"}, "generation": "g1"}),
])
def test_an_error_shaped_result_is_never_an_answer(text: str) -> None:
    """`{"status": "error"}` and `{"error": {...}}` are failures by the
    vendor's own error test (utils/middleware.py:158-188); before the
    hardening either counted as a graph answer and made the turn ready."""
    instance = RecordedInstance(stored_message=_with_output(_one_call(text)))

    step, conversation = _probe(instance)

    assert step.status == STATUS_FAILED
    assert conversation["reason"] == "graph_tool_calls_failed"


def test_an_output_the_vendor_marked_failed_is_never_an_answer() -> None:
    """Whatever the text, a `function_call_output` the vendor stored with
    `status: failed` (0.11.4, middleware.py:6124-6127) is not a graph answer."""
    text = json.dumps({"nodes": [], "generation": "g1"})
    instance = RecordedInstance(
        stored_message=_with_output(_one_call(text, status="failed")),
    )

    step, conversation = _probe(instance)

    assert step.status == STATUS_FAILED
    assert conversation["reason"] == "graph_tool_calls_failed"
