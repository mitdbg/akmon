"""A model endpoint that records exactly what Open WebUI sent it.

The post-compaction equality proof needs one thing no real provider gives
you: the request body as it left Open WebUI, byte for byte, labeled with
which of Open WebUI's model calls it was. This is that endpoint — an
OpenAI-compatible server that answers every completion with a canned reply
and appends the raw request to a capture file.

Why the labeling matters
------------------------
A single user turn makes Open WebUI issue SEVERAL completions: the user's
own, plus title generation, tag generation, follow-up generation, and the
compaction summarization call itself. Comparing the audit record against
"the last request the endpoint saw" would compare it against a title
prompt about half the time, and that comparison would fail — or, worse,
pass on a run where the two happened to line up.

Labels come from two independent signals, in this order:

1. ``metadata.task`` in the request body. Open WebUI's task routers stamp
   it, and the compaction summarizer stamps ``context_compaction``. On the
   OpenAI path this is POPPED before the upstream call
   (``routers/openai.py``), so it is present only on providers that forward
   it — which is exactly why it is not the only signal.
2. The MODEL ID. Open WebUI lets the task model and the compaction model be
   configured separately from the chat model, so pointing each at its own
   id here turns the label into a property of the payload itself rather
   than a vendor behavior we assume.

A request that matches neither is labeled ``user_turn`` — but the test that
consumes these captures asserts the label it compares, so an unlabeled
capture cannot silently become the user turn.

Keyless by construction: it never reads an Authorization header and has no
credential of its own.
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

#: Model ids this endpoint serves. Point Open WebUI's chat model, task
#: model, and compaction model at three different ones and every capture is
#: self-labeling.
CHAT_MODEL = "okg-recorder-chat"
TASK_MODEL = "okg-recorder-task"
COMPACTION_MODEL = "okg-recorder-compaction"

MODEL_TASK_KINDS = {
    CHAT_MODEL: "user_turn",
    TASK_MODEL: "auxiliary_task",
    COMPACTION_MODEL: "context_compaction",
}

CANNED_REPLY = "Recorded."

_WRITE_LOCK = threading.Lock()


def canonical_payload(body: dict[str, Any]) -> str:
    """The in-scope subtree under the pinned canonical form.

    Deliberately duplicated from
    ``okg.substrate.chat.audit_record.canonical_payload`` because this file
    also runs where okg is not importable. The two are asserted byte-equal
    by ``tests/substrate/chat/test_live_filter_agreement.py``; a drift
    between them fails that test rather than silently changing what the
    proof compares.
    """
    return json.dumps(
        {"messages": body.get("messages"), "model": body.get("model")},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def label_capture(body: dict[str, Any]) -> str:
    """Which Open WebUI call this was."""
    metadata = body.get("metadata")
    if isinstance(metadata, dict):
        task = metadata.get("task")
        if isinstance(task, str) and task.strip():
            return task.strip()
    model = body.get("model")
    if isinstance(model, str):
        for known, kind in MODEL_TASK_KINDS.items():
            if model == known or model.endswith(known):
                return kind
    return "user_turn"


class _Handler(BaseHTTPRequestHandler):
    capture_path: Path = Path("captures.jsonl")
    server_version = "OKGRecordingModelEndpoint/1.0"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        pass  # the capture file is the log

    # -- routes ----------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        if self.path.rstrip("/") in ("/v1/models", "/models"):
            self._json(
                200,
                {
                    "object": "list",
                    "data": [
                        {"id": model, "object": "model", "owned_by": "okg-recorder"}
                        for model in MODEL_TASK_KINDS
                    ],
                },
            )
            return
        if self.path.rstrip("/") in ("/health", ""):
            self._json(200, {"ok": True})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        if self.path.rstrip("/") not in ("/v1/chat/completions", "/chat/completions"):
            self._json(404, {"error": "not found"})
            return

        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            body = {}

        payload = canonical_payload(body) if isinstance(body, dict) else "{}"
        capture = {
            "captured_at": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
            "task_kind": label_capture(body) if isinstance(body, dict) else "unknown",
            "model": body.get("model") if isinstance(body, dict) else None,
            "message_count": len(body.get("messages") or []) if isinstance(body, dict) else 0,
            "stream": bool(body.get("stream")) if isinstance(body, dict) else False,
            # The raw body is retained alongside the canonical digests so a
            # mismatch can be diagnosed rather than argued about.
            "raw_body": raw.decode("utf-8", "replace"),
            "canonical_payload": payload,
            "headers": {
                key: value
                for key, value in self.headers.items()
                if key.lower().startswith("x-openwebui")
            },
        }
        with _WRITE_LOCK:
            with self.capture_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(capture, ensure_ascii=False) + "\n")
                handle.flush()

        if isinstance(body, dict) and body.get("stream"):
            self._stream(body.get("model"))
        else:
            self._json(
                200,
                {
                    "id": "okg-recorder-1",
                    "object": "chat.completion",
                    "created": int(time.time()),
                    "model": body.get("model") if isinstance(body, dict) else None,
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": CANNED_REPLY},
                            "finish_reason": "stop",
                        }
                    ],
                    # NO `usage` block. Open WebUI stores a reported usage on
                    # the assistant message and then TRUSTS it over its own
                    # estimate when deciding whether to compact
                    # (`utils/context_compaction.py::_exceeds_token_threshold`).
                    # A recorder that invented token counts would therefore
                    # decide when compaction fires, which is the behavior
                    # under measurement. Omitting it leaves Open WebUI on its
                    # own estimator, which is what a real provider's
                    # conversation would exercise.
                },
            )

    # -- helpers ---------------------------------------------------------

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _stream(self, model: Any) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        chunk = {
            "id": "okg-recorder-1",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {"index": 0, "delta": {"content": CANNED_REPLY}, "finish_reason": None}
            ],
        }
        done = {
            "id": "okg-recorder-1",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        }
        for piece in (chunk, done):
            self.wfile.write(f"data: {json.dumps(piece)}\n\n".encode())
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()


def serve(port: int, capture_path: Path) -> ThreadingHTTPServer:
    capture_path.parent.mkdir(parents=True, exist_ok=True)
    handler = type("_BoundHandler", (_Handler,), {"capture_path": capture_path})
    return ThreadingHTTPServer(("0.0.0.0", port), handler)  # noqa: S104


def read_captures(capture_path: Path) -> list[dict[str, Any]]:
    """Every capture, in the order the endpoint received them."""
    if not capture_path.exists():
        return []
    return [
        json.loads(line)
        for line in capture_path.read_text("utf-8").splitlines()
        if line.strip()
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8199)
    parser.add_argument("--capture", type=Path, default=Path("captures.jsonl"))
    args = parser.parse_args()
    server = serve(args.port, args.capture)
    print(f"recording model endpoint on :{args.port} -> {args.capture}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
