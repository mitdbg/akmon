"""Read an operator result as a program does: in the JSON result format.

Since issue #3059 every okg MCP result is ONE text block. By default that
block is the plain-text rendering a model reads; a program asks for the
compact JSON of the same dictionary through the request `_meta` key
`okg/result_format` (or, for an in-process `FastMCP.call_tool`, inside
`result_format_scope("json")`).

Program-side tests assert on the dictionary, so they call through
`call_json` (in-process) or pass `JSON_META` to a client session's
`call_tool`, then read it with `payload()`. Model-side tests assert on the
text, through `result_text()`.

Both readers refuse a result that carries `structuredContent`: since #3059
no okg result has one, so its presence is a regression to report, never a
second copy to read.
"""
from __future__ import annotations

import json
from typing import Any

from okg.substrate.mcp.result_text import RESULT_FORMAT_META_KEY, result_format_scope

#: Request `_meta` that asks an okg server for the JSON result format.
JSON_META = {RESULT_FORMAT_META_KEY: "json"}


async def call_json(mcp: Any, name: str, arguments: dict[str, Any]) -> Any:
    """An in-process `call_tool` whose result is the JSON format."""
    with result_format_scope("json"):
        return await mcp.call_tool(name, arguments)


def json_client_sessions(monkeypatch: Any) -> None:
    """Make every MCP `ClientSession.call_tool` in this test ask for JSON.

    For program-side tests that drive a real client session (in-memory or
    over HTTP): each call sends `JSON_META` unless it names its own `meta`.
    """
    from mcp import ClientSession

    original = ClientSession.call_tool

    async def call_tool(self, name, arguments=None, *args, meta=None, **kwargs):
        return await original(
            self, name, arguments, *args,
            meta=JSON_META if meta is None else meta, **kwargs,
        )

    monkeypatch.setattr(ClientSession, "call_tool", call_tool)


def _refuse_structured(result: Any) -> None:
    structured = getattr(result, "structuredContent", None)
    if structured is not None:
        raise AssertionError(
            "the result carries structuredContent; since issue #3059 an okg "
            f"result is one text block only (keys {sorted(structured)[:8]!r})"
        )


def payload(result: Any) -> dict[str, Any]:
    """The operator's answer, from a JSON-format result."""
    _refuse_structured(result)
    content = getattr(result, "content", None) or []
    if not content:
        raise AssertionError(
            "the tool result carried no content "
            "block; there is no answer in it to assert against"
        )
    text = content[0].text
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise AssertionError(
            "the result's text block is not JSON: the call did not ask for the "
            f"JSON result format. Text starts: {text[:120]!r}"
        ) from exc


def result_text(result: Any) -> str:
    """The text block the model reads, for assertions about the wire form."""
    _refuse_structured(result)
    content = getattr(result, "content", None) or []
    return content[0].text if content else ""
