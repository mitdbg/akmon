"""Act-route stub MCP server (adopt-act-mcp-routes tests).

Line-delimited JSON-RPC on stdio, mirroring
mcp_composed/stub_mcp_server.py, but exposing the calendar
read/create pair the act-surface round-trip exercises. The server
holds a credential (env `ACT_STUB_TOKEN`) the way a real
workspace-mcp instance holds OAuth tokens — it authenticates with
it internally and must never include it in tool output.
"""
from __future__ import annotations

import json
import os
import sys

_EVENTS = [
    {"id": "evt-1", "title": "Lab meeting", "start": "2026-06-11T14:00:00Z"},
]


def _write(payload: dict) -> None:
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()


def main() -> int:
    token = os.environ.get("ACT_STUB_TOKEN", "")
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        method = msg.get("method")
        rid = msg.get("id")
        params = msg.get("params") or {}

        if method == "initialize":
            _write({
                "jsonrpc": "2.0", "id": rid,
                "result": {
                    "protocolVersion": (
                        params.get("protocolVersion") or "2025-11-25"
                    ),
                    "serverInfo": {
                        "name": "act-route-stub", "version": "0.0.1",
                    },
                    "capabilities": {"tools": {"listChanged": False}},
                },
            })
        elif method == "notifications/initialized":
            pass
        elif method == "tools/list":
            _write({
                "jsonrpc": "2.0", "id": rid,
                "result": {
                    "tools": [
                        {
                            "name": "calendar_read",
                            "description": "List events.",
                            "inputSchema": {"type": "object"},
                        },
                        {
                            "name": "calendar_create",
                            "description": "Create an event.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "title": {"type": "string"},
                                    "start": {"type": "string"},
                                },
                            },
                        },
                    ],
                },
            })
        elif method == "tools/call":
            name = params.get("name")
            arguments = params.get("arguments") or {}
            # The "auth" the real server would perform; the token
            # stays server-side.
            assert token, "stub requires ACT_STUB_TOKEN"
            if name == "calendar_read":
                _write({
                    "jsonrpc": "2.0", "id": rid,
                    "result": {
                        "content": [{
                            "type": "text",
                            "text": json.dumps({"events": _EVENTS}),
                        }],
                        "isError": False,
                    },
                })
            elif name == "calendar_create":
                created = {
                    "id": f"evt-{len(_EVENTS) + 1}",
                    "title": arguments.get("title", ""),
                    "start": arguments.get("start", ""),
                }
                _EVENTS.append(created)
                _write({
                    "jsonrpc": "2.0", "id": rid,
                    "result": {
                        "content": [{
                            "type": "text",
                            "text": json.dumps({"created": created}),
                        }],
                        "isError": False,
                    },
                })
            else:
                _write({
                    "jsonrpc": "2.0", "id": rid,
                    "result": {
                        "content": [{
                            "type": "text",
                            "text": f"unknown tool {name}",
                        }],
                        "isError": True,
                    },
                })
        elif rid is not None:
            _write({
                "jsonrpc": "2.0", "id": rid,
                "error": {"code": -32601, "message": "Method not found"},
            })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
