"""The deterministic, purely extractive model for the real-graph proof.

This is a local test fixture and holds no provider credential -- no real
language model takes part in this proof. It composes no language of its own
about the graph. On the first request it emits one search call for the node
id it READS out of the user turn. okg's text search result names no
generation (PACT read-passage-rows-3666, D8), so on that continuation it
emits one ``inspect(target={"kind":"graph"})`` call. Then it answers only by
quoting the published generation id and the provenance source it FINDS
inside the tool results OpenWebUI handed back, and it answers HTTP 500 when
they do not carry them.

Nothing here knows a generation, a node, a source, or a deployment: every
value in the answer arrives from the tool result. That is what makes the
stored answer proof that the container really called the real MCP server and
really returned its reading of the real published graph.

There is no MCP fixture in this proof. The tool is a real ``okg mcp-serve``
process the driver starts from a wheel-installed virtual environment.

Run it as a module so the package import below resolves::

    python -m tests.live.chat.real_graph_model_fixture --model-port ... \
        --out ... --session-id ...
"""

from __future__ import annotations

import argparse
import json
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from tests.live.chat.grounded_api_chain import ChainError, Refusal
from tests.live.chat.real_graph_chain import (
    GRAPH_ARGUMENTS,
    GRAPH_TOOL_NAME,
    MODEL_ID,
    TOOL_NAME,
    call_id_for,
    graph_call_id_for,
    needs_graph_read,
    node_id_from_prompt,
    search_arguments,
    turn_values,
)

_LOCK = threading.Lock()


def _append(path: Path, session: str, row: dict[str, Any]) -> None:
    """Record one row, stamped with the run it belongs to.

    The session id and the timestamp are what tie this file to one turn, so a
    leftover row from an earlier run cannot satisfy the validator.
    """
    stamped = {
        **row,
        "session": session,
        "at": datetime.now(timezone.utc).isoformat(),
    }
    with _LOCK, path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(stamped, sort_keys=True) + "\n")


def answer_text(node_id: str, generation_id: str, source: str) -> str:
    """The whole assistant answer, built only from values found in the tool result."""
    return (
        f"Node {node_id} is published in generation {generation_id} and its "
        f"provenance source is {source}."
    )


def offered_tool(tools: list[Any], operator: str) -> str:
    """The name of the offered tool that is `operator`: named `operator`
    or `<server id>_<operator>`; the bare name when no tool is offered; a
    refusal when tools are offered and none is it."""
    names = [
        str(((tool or {}).get("function") or {}).get("name") or "")
        for tool in tools
        if isinstance(tool, dict)
    ]
    for name in names:
        if name == operator or name.endswith(f"_{operator}"):
            return name
    if not tools:
        return operator
    raise Refusal(f"no {operator} tool offered: {names!r}")


def offered_search_tool(tools: list[Any]) -> str:
    """The name of the offered tool that IS the graph search operator.

    The driven proof offers exactly one tool. A deployment preset -- what
    ``okg chat setup``'s conversation step asks through -- offers all seven
    read operators, in an order the vendor chooses, so taking the first one
    would send search arguments to ``inspect``. The search tool is the one
    named ``search`` or ``<server id>_search``. With no tools offered at all
    the bare operator name is used (the direct proof's request shape). When
    tools ARE offered and none is the search operator, this refuses rather
    than sending search arguments to whichever tool came first.
    """
    names = [
        str(((tool or {}).get("function") or {}).get("name") or "")
        for tool in tools
        if isinstance(tool, dict)
    ]
    for name in names:
        if name == TOOL_NAME or name.endswith(f"_{TOOL_NAME}"):
            return name
    if not tools:
        return TOOL_NAME
    raise Refusal(f"no search tool offered: {names!r}")


def plan_reply(body: dict[str, Any], session: str) -> tuple[dict[str, Any], str]:
    """Decide the reply for one upstream request, or raise.

    Pure apart from reading ``body``, so the hermetic test drives both branches
    without a socket. Raises :class:`Refusal` or :class:`ChainError` when the
    turn cannot be answered honestly; the handler turns that into HTTP 500.
    """
    messages = body.get("messages") or []
    user_turns = [
        str(message.get("content") or "")
        for message in messages
        if isinstance(message, dict) and message.get("role") == "user"
    ]
    if not user_turns:
        raise Refusal("the upstream request carries no user turn")
    node_id = node_id_from_prompt("\n".join(user_turns))
    tool_messages = [
        message
        for message in messages
        if isinstance(message, dict) and message.get("role") == "tool"
    ]
    if not tool_messages:
        name = offered_search_tool(body.get("tools") or [])
        message = {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": call_id_for(session),
                    "type": "function",
                    "function": {
                        "name": name,
                        "arguments": json.dumps(search_arguments(node_id)),
                    },
                }
            ],
        }
        return message, "tool_calls"
    contents = [str(message.get("content") or "") for message in tool_messages]
    origin = "tool result handed to the model"
    if needs_graph_read(contents, origin=origin):
        # okg's text search result names no generation (PACT
        # read-passage-rows-3666, D8): read it once for the turn from
        # inspect(target={"kind":"graph"}).
        name = offered_tool(body.get("tools") or [], GRAPH_TOOL_NAME)
        message = {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": graph_call_id_for(session),
                    "type": "function",
                    "function": {"name": name, "arguments": json.dumps(GRAPH_ARGUMENTS)},
                }
            ],
        }
        return message, "tool_calls"
    values = turn_values(contents, node_id=node_id, origin=origin)
    first = values[0]
    for other in values[1:]:
        if other != first:
            raise ChainError(
                f"the handed-back tool results disagree: {first!r} and {other!r}"
            )
    message = {
        "role": "assistant",
        "content": answer_text(first.node_id, first.generation_id, first.source),
    }
    return message, "stop"


class _ModelHandler(BaseHTTPRequestHandler):
    capture_path = Path("model.jsonl")
    session = ""

    def log_message(self, _format: str, *_args: Any) -> None:
        pass

    def do_GET(self) -> None:  # noqa: N802
        if self.path.rstrip("/") in ("/v1/models", "/models"):
            self._json(
                200, {"object": "list", "data": [{"id": MODEL_ID, "object": "model"}]}
            )
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path.rstrip("/") not in ("/v1/chat/completions", "/chat/completions"):
            self._json(404, {"error": "not found"})
            return
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        body = json.loads(raw or b"{}")
        _append(self.capture_path, self.session, {"kind": "model_request", "body": body})
        try:
            message, finish = plan_reply(body, self.session)
        except (Refusal, ChainError) as exc:
            _append(
                self.capture_path,
                self.session,
                {"kind": "model_refusal", "reason": str(exc)},
            )
            self._json(500, {"error": str(exc)})
            return
        if body.get("stream"):
            self._stream(message, finish)
            return
        self._json(
            200,
            {
                "id": "fixture-completion",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": MODEL_ID,
                "choices": [{"index": 0, "message": message, "finish_reason": finish}],
            },
        )

    def _stream(self, message: dict[str, Any], finish: str) -> None:
        """Emit the OpenAI SSE shape consumed by the OpenWebUI tool loop."""
        if message.get("tool_calls"):
            delta: dict[str, Any] = {
                "role": "assistant",
                "content": None,
                "tool_calls": [{"index": 0, **message["tool_calls"][0]}],
            }
        else:
            delta = {"role": "assistant", "content": message["content"]}
        chunks = [
            {
                "id": "fixture-completion",
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": MODEL_ID,
                "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
            },
            {
                "id": "fixture-completion",
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": MODEL_ID,
                "choices": [{"index": 0, "delta": {}, "finish_reason": finish}],
            },
        ]
        rows = [f"data: {json.dumps(chunk)}\n\n" for chunk in chunks]
        rows.append("data: [DONE]\n\n")
        data = "".join(rows).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def serve(model_port: int, out: Path, session: str) -> None:
    out.mkdir(parents=True, exist_ok=True)
    handler = type(
        "BoundModelHandler",
        (_ModelHandler,),
        {"capture_path": out / "model.jsonl", "session": session},
    )
    # 0.0.0.0 because the container reaches this process over the Docker
    # host bridge; the port is a run-time free port and the process exits with
    # the run.
    server = ThreadingHTTPServer(("0.0.0.0", model_port), handler)  # noqa: S104
    server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-port", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--session-id", required=True)
    args = parser.parse_args()
    serve(args.model_port, args.out, args.session_id)


if __name__ == "__main__":
    main()
