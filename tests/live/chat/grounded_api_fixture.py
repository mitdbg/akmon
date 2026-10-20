"""Deterministic model and MCP fixtures for the real OpenWebUI API proof.

These are local test fixtures. They prove vendor integration mechanics, not a
production model, graph, identity provider, or deployment. The model is a
fixture: it composes no language of its own, and answers at all only when the
tool result it was handed still carries the evidence strings. That is what
makes the proof causal -- an answer can only exist downstream of a real tool
call whose real result came back.

Run it as a module so the package import below resolves:
``python -m tests.live.chat.grounded_api_fixture --model-port ... --mcp-port ... --out ...``
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

from tests.live.chat.grounded_api_chain import (
    GENERATION,
    MODEL_ID,
    NODE_ID,
    SOURCE_REF,
    TOOL_NAME,
    call_id_for,
)

_LOCK = threading.Lock()


def _append(path: Path, session: str, row: dict[str, Any]) -> None:
    """Record one row, stamped with the run it belongs to.

    The session id and timestamp are what let the validator tie this file to
    one turn. Without them two runs of this proof wrote byte-identical
    recorder files and a leftover row could have satisfied the chain.
    """
    stamped = {
        **row,
        "session": session,
        "at": datetime.now(timezone.utc).isoformat(),
    }
    with _LOCK, path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(stamped, sort_keys=True) + "\n")


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
        messages = body.get("messages") or []
        tool_messages = [
            m for m in messages if isinstance(m, dict) and m.get("role") == "tool"
        ]
        _append(self.capture_path, self.session, {"kind": "model_request", "body": body})
        if not tool_messages:
            tools = body.get("tools") or []
            function = (tools[0] if tools else {}).get("function") or {}
            name = function.get("name") or TOOL_NAME
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        # Per-run, so the call and its result that OpenWebUI
                        # persists are pinned to THIS turn.
                        "id": call_id_for(self.session),
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": json.dumps({"node_id": NODE_ID}),
                        },
                    }
                ],
            }
            finish = "tool_calls"
        else:
            joined = "\n".join(str(m.get("content") or "") for m in tool_messages)
            if (
                GENERATION not in joined
                or SOURCE_REF not in joined
                or NODE_ID not in joined
            ):
                _append(
                    self.capture_path,
                    self.session,
                    {"kind": "model_refusal", "reason": "tool result lost", "saw": joined},
                )
                self._json(500, {"error": "tool result lost before model continuation"})
                return
            message = {
                "role": "assistant",
                "content": f"Grounded answer for {NODE_ID}; generation {GENERATION}; source {SOURCE_REF}.",
            }
            finish = "stop"
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
        """Emit the OpenAI SSE shape consumed by OpenWebUI's tool loop."""
        chunks: list[dict[str, Any]] = []
        if message.get("tool_calls"):
            chunks.append(
                {
                    "id": "fixture-completion",
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": MODEL_ID,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [
                                    {"index": 0, **message["tool_calls"][0]}
                                ],
                            },
                            "finish_reason": None,
                        }
                    ],
                }
            )
        else:
            chunks.append(
                {
                    "id": "fixture-completion",
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": MODEL_ID,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "role": "assistant",
                                "content": message["content"],
                            },
                            "finish_reason": None,
                        }
                    ],
                }
            )
        chunks.append(
            {
                "id": "fixture-completion",
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": MODEL_ID,
                "choices": [{"index": 0, "delta": {}, "finish_reason": finish}],
            }
        )
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


def run(model_port: int, mcp_port: int, out: Path, session: str) -> None:
    from mcp.server.fastmcp import FastMCP

    out.mkdir(parents=True, exist_ok=True)
    model_capture = out / "model.jsonl"
    mcp_capture = out / "mcp.jsonl"
    handler = type(
        "BoundModelHandler",
        (_ModelHandler,),
        {"capture_path": model_capture, "session": session},
    )
    model = ThreadingHTTPServer(("0.0.0.0", model_port), handler)  # noqa: S104
    threading.Thread(target=model.serve_forever, daemon=True).start()

    mcp = FastMCP("okg-grounded-api-fixture", host="0.0.0.0", port=mcp_port)

    @mcp.tool(name=TOOL_NAME)
    def inspect(node_id: str) -> dict[str, Any]:
        row = {
            "pinned_generation_id": GENERATION,
            "items": [{"node_id": node_id, "source_reference": SOURCE_REF}],
            "total_count": 1,
        }
        _append(
            mcp_capture,
            session,
            {
                "kind": "mcp_call",
                "tool": TOOL_NAME,
                "arguments": {"node_id": node_id},
                "result": row,
            },
        )
        return row

    mcp.run(transport="streamable-http")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-port", type=int, required=True)
    parser.add_argument("--mcp-port", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--session-id", required=True)
    args = parser.parse_args()
    run(args.model_port, args.mcp_port, args.out, args.session_id)


if __name__ == "__main__":
    main()
