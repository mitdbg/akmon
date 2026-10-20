"""Stub MCP server — minimal JSON-RPC stdio responder used by the
mcp_composed unit tests.

Speaks line-delimited JSON on stdin/stdout, per the MCP stdio
transport spec. Implements only the subset McpClient exercises:

  - `initialize`: returns `serverInfo` + `capabilities` +
    protocolVersion. Honors the version the client sent (must be
    in MCP's supported set: 2024-11-05 / 2025-03-26 / ...).
  - `notifications/initialized`: ack (no response).
  - `tools/list`: returns two declared tools (`echo`, `boom`).
  - `tools/call`:
      - `echo(payload)`: returns the payload in `result.content`.
      - `boom()`: returns `isError: True` so the client raises.
      - `slow()`: sleeps 5 s before responding (exercises the
        client-side read timeout).
  - any other method: returns `Method not found`.

Run-mode flags (selected by argv):
  - `--crash-on-tool`: exit non-zero after the first `tools/call`
    instead of responding (exercises the SDK's transport-error
    surface).
  - `--expandable`: advertise `tools.listChanged: true`, add a
    `widen` tool, and expose a `late` tool only AFTER `widen` is
    called. `widen` writes `notifications/tools/list_changed`
    before its own response, exactly as the substrate server
    does, so the client's cache invalidation is exercised against
    a real subprocess and real frames rather than a stub session
    (issue #1458).
"""
from __future__ import annotations

import json
import sys
import time


def _write_ld(payload: dict) -> None:
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()


def main() -> int:
    args = set(sys.argv[1:])
    crash_on_tool = "--crash-on-tool" in args
    expandable = "--expandable" in args
    write = _write_ld
    tools_called = 0
    widened = False

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
            requested_version = params.get("protocolVersion") or "2025-11-25"
            write({
                "jsonrpc": "2.0", "id": rid,
                "result": {
                    "protocolVersion": requested_version,
                    "serverInfo": {
                        "name": "stub-mcp-server",
                        "version": "0.0.1",
                    },
                    "capabilities": {
                        "tools": {"listChanged": bool(expandable)},
                    },
                    "instructions": "stub server instructions",
                },
            })
        elif method == "notifications/initialized":
            # Notifications have no `id` and no response per the
            # JSON-RPC spec.
            pass
        elif method == "tools/list":
            listed = [
                {
                    "name": "echo",
                    "description": "Echoes the payload back.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "payload": {"type": "object"},
                        },
                    },
                },
                {
                    "name": "boom",
                    "description": "Always returns isError.",
                    "inputSchema": {"type": "object"},
                },
            ]
            if expandable:
                listed.append({
                    "name": "widen",
                    "description": "Adds `late` to the tool list.",
                    "inputSchema": {"type": "object"},
                })
                if widened:
                    listed.append({
                        "name": "late",
                        "description": "Only exists after widen.",
                        "inputSchema": {"type": "object"},
                    })
            write({
                "jsonrpc": "2.0", "id": rid,
                "result": {"tools": listed},
            })
        elif method == "tools/call":
            tools_called += 1
            if crash_on_tool:
                # Don't respond; just exit. The SDK's read stream
                # will surface this as a transport error.
                sys.exit(2)
            name = params.get("name")
            arguments = params.get("arguments") or {}
            if expandable and name == "widen":
                widened = True
                # Notification FIRST, then the response — the same order
                # the substrate server writes them, so a client that only
                # invalidates on the response would still pass and a
                # client that ignores notifications would not.
                write({
                    "jsonrpc": "2.0",
                    "method": "notifications/tools/list_changed",
                })
                write({
                    "jsonrpc": "2.0", "id": rid,
                    "result": {
                        "content": [{"type": "text", "text": "widened"}],
                        "isError": False,
                    },
                })
            elif expandable and name == "late":
                write({
                    "jsonrpc": "2.0", "id": rid,
                    "result": {
                        "content": [{"type": "text", "text": "late ok"}],
                        "isError": bool(not widened),
                    },
                })
            elif name == "echo":
                write({
                    "jsonrpc": "2.0", "id": rid,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(
                                    arguments.get("payload") or {},
                                ),
                            },
                        ],
                        "isError": False,
                    },
                })
            elif name == "boom":
                write({
                    "jsonrpc": "2.0", "id": rid,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": "boom: stub error",
                            },
                        ],
                        "isError": True,
                    },
                })
            elif name == "slow":
                time.sleep(5.0)
                write({
                    "jsonrpc": "2.0", "id": rid,
                    "result": {"content": [], "isError": False},
                })
            else:
                write({
                    "jsonrpc": "2.0", "id": rid,
                    "error": {
                        "code": -32601,
                        "message": f"unknown tool: {name}",
                    },
                })
        else:
            write({
                "jsonrpc": "2.0", "id": rid,
                "error": {
                    "code": -32601, "message": "method not found",
                },
            })
    return 0


if __name__ == "__main__":
    sys.exit(main())
