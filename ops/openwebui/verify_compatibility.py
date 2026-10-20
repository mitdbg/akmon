"""Verify the patched OpenWebUI client and its downstream result formatter."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from mcp.types import CallToolResult, TextContent
import open_webui
from open_webui.config import CACHE_DIR, STATIC_DIR
from open_webui.env import (
    DATA_DIR,
    ENABLE_DB_MIGRATIONS,
    OAUTH_SESSION_TOKEN_ENCRYPTION_KEY,
    OFFLINE_MODE,
    WEBUI_AUTH,
)
from open_webui.utils.mcp.client import MCPClient
from open_webui.utils.middleware import process_tool_result


class _Session:
    def __init__(self, result: CallToolResult) -> None:
        self.result = result

    async def call_tool(
        self, function_name: str, function_args: dict[str, Any]
    ) -> CallToolResult:
        _require(function_name == "inspect", "unexpected tool name")
        _require(function_args == {"node_id": "n:1"}, "unexpected tool arguments")
        return self.result


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


async def _call(result: CallToolResult) -> Any:
    client = MCPClient()
    client.session = _Session(result)
    return await client.call_tool("inspect", {"node_id": "n:1"})


async def _format(value: Any) -> str:
    formatted, files, embeds = await process_tool_result(
        None,
        "inspect",
        value,
        "mcp",
    )
    _require(files == [], f"unexpected formatter files: {files!r}")
    _require(embeds == [], f"unexpected formatter embeds: {embeds!r}")
    _require(isinstance(formatted, str), "formatter did not return text")
    return formatted


async def _expect_refusal(payload: dict[str, Any]) -> None:
    result = CallToolResult(content=[], structuredContent=payload, isError=True)
    try:
        await _call(result)
    except Exception as exc:  # OpenWebUI deliberately raises this boundary.
        _require(
            exc.args == (payload,),
            f"refusal detail changed: expected {(payload,)!r}, found {exc.args!r}",
        )
        formatted = await _format(str(exc))
        _require(
            formatted == str(payload),
            f"refusal formatter changed {payload!r} into {formatted!r}",
        )
        return
    raise AssertionError(f"structured refusal was returned as success: {payload!r}")


def _require_inside(root: Path, raw_path: str | Path, name: str) -> None:
    path = Path(raw_path).resolve()
    _require(
        path.is_relative_to(root),
        f"{name} escaped isolated verifier root {root}: {path}",
    )


def _verify_isolated_import_state() -> None:
    root = Path(os.environ["OKG_OPENWEBUI_VERIFY_ROOT"]).resolve()
    package_origin = Path(open_webui.__file__ or "").resolve()
    pinned_package_root = Path("/app/backend/open_webui").resolve()
    _require(root.is_dir(), f"isolated verifier root is missing: {root}")
    _require(
        package_origin.is_relative_to(pinned_package_root),
        "open_webui imported outside the pinned package root "
        f"{pinned_package_root}: {package_origin}",
    )
    _require(not ENABLE_DB_MIGRATIONS, "database migrations were enabled")
    _require(OFFLINE_MODE, "offline mode was not enabled")
    _require(os.environ["HF_HUB_OFFLINE"] == "1", "model downloads were not disabled")
    _require(os.environ["CUSTOM_NAME"] == "", "custom branding could access the network")
    _require(
        OAUTH_SESSION_TOKEN_ENCRYPTION_KEY
        == "okg-public-verifier-fixture",
        "OAuth session key is not the public verifier-only fixture",
    )
    _require(not WEBUI_AUTH, "authentication unexpectedly required a secret")
    _require(
        os.environ["DATABASE_URL"] == f"sqlite:///{root}/data/webui.db",
        f"database escaped isolated verifier root: {os.environ['DATABASE_URL']!r}",
    )
    _require_inside(root, DATA_DIR, "DATA_DIR")
    _require_inside(root, CACHE_DIR, "CACHE_DIR")
    _require_inside(root, STATIC_DIR, "STATIC_DIR")
    _require_inside(root, Path.home(), "HOME")
    for name in (
        "XDG_CACHE_HOME",
        "HF_HOME",
        "SENTENCE_TRANSFORMERS_HOME",
        "WHISPER_MODEL_DIR",
        "TIKTOKEN_CACHE_DIR",
        "AUDIT_LOGS_FILE_PATH",
    ):
        _require_inside(root, os.environ[name], name)


async def verify() -> None:
    _verify_isolated_import_state()
    legacy = CallToolResult(
        content=[TextContent(type="text", text='{"legacy": true}')],
        structuredContent=None,
    )
    legacy_dump = legacy.model_dump(mode="json")
    _require(
        "structuredContent" in legacy_dump
        and legacy_dump["structuredContent"] is None,
        f"SDK legacy result shape changed: {legacy_dump!r}",
    )
    legacy_content = await _call(legacy)
    _require(
        legacy_content == legacy_dump["content"],
        f"legacy content-list fallback changed: {legacy_content!r}",
    )
    legacy_formatted = await _format(legacy_content)
    _require(
        json.loads(legacy_formatted) == {"legacy": True},
        f"legacy formatter output changed: {legacy_formatted!r}",
    )

    empty: dict[str, Any] = {}
    empty_result = CallToolResult(content=[], structuredContent=empty)
    _require(await _call(empty_result) == {}, "empty structured success was lost")
    _require(
        await _format(empty) == "{}",
        "empty structured success did not format exactly as '{}'",
    )

    success = {"items": [{"node_id": "n:1"}], "total_count": 1}
    success_result = CallToolResult(
        content=[TextContent(type="text", text='{"legacy_duplicate": true}')],
        structuredContent=success,
    )
    selected = await _call(success_result)
    _require(selected == success, f"structured success changed: {selected!r}")
    formatted = await _format(selected)
    _require(
        json.loads(formatted) == success,
        f"structured formatter changed the success payload: {formatted!r}",
    )
    _require(
        "legacy_duplicate" not in formatted and formatted.count('"node_id"') == 1,
        f"dual-payload result was duplicated by the formatter: {formatted!r}",
    )

    await _expect_refusal(
        {"error": {"code": "access_denied", "message": "not allowed"}}
    )
    await _expect_refusal({})

    # okg issue #3059: an okg result is ONE text block and no structured
    # copy, so the patched client selects `content` for it, success and
    # refusal alike, and the text reaches the model whole.
    okg_text = "inspect ok · generation g1\n--- other fields\nnode_id: n:1"
    okg_success = CallToolResult(
        content=[TextContent(type="text", text=okg_text)],
        structuredContent=None,
    )
    okg_selected = await _call(okg_success)
    _require(
        okg_selected == okg_success.model_dump(mode="json")["content"],
        f"one-text-block okg success was not selected as content: {okg_selected!r}",
    )
    okg_formatted = await _format(okg_selected)
    _require(
        "inspect ok" in okg_formatted and "node_id: n:1" in okg_formatted,
        f"one-text-block okg success lost its text: {okg_formatted!r}",
    )
    okg_refusal_text = (
        "inspect refused access_denied\nmessage: not allowed\n"
        "fix_hint: ask an operator"
    )
    okg_refusal = CallToolResult(
        content=[TextContent(type="text", text=okg_refusal_text)],
        structuredContent=None,
        isError=True,
    )
    refusal_content = okg_refusal.model_dump(mode="json")["content"]
    try:
        await _call(okg_refusal)
    except Exception as exc:  # OpenWebUI deliberately raises this boundary.
        _require(
            exc.args == (refusal_content,),
            f"one-text-block okg refusal detail changed: {exc.args!r}",
        )
        _require(
            "inspect refused access_denied" in str(exc)
            and "fix_hint: ask an operator" in str(exc),
            f"one-text-block okg refusal lost its fix-it text: {exc!r}",
        )
    else:
        raise AssertionError("one-text-block okg refusal was returned as success")


def main() -> int:
    asyncio.run(verify())
    print("OpenWebUI structured MCP client and formatter compatibility verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
