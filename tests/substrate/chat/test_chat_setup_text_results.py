"""The chat probe classifies okg's one-text-block results (issue #3059).

Re-review finding B1 on the #3059 design: with no `structuredContent`, both
the Open WebUI compatibility patch and upstream 0.11.4 select the result's
`content` LIST. A success is stored as that list (JSON); a refusal is raised
as `Exception(<the list>)`, whose `str` is a Python list literal, stored
bare at 0.11.0 and as `{"error": "<literal>"}` at 0.11.4. Before this change
`_classify_result` read only dict literals, so every such refusal counted as
`failed` instead of `refused`.

The stored forms here are built from REAL results: a branch server (the
hermetic probe build) answers over an in-memory MCP client session, and the
patch's own result-selection code (`COMPATIBLE_RESULT_SELECTION`, executed
as written) selects and raises. `fixtures/openwebui_v0_11_4_text_result_calls.json`
holds the same forms, recorded by `_stored_forms` with the session id
scrubbed.
"""
from __future__ import annotations

import importlib.util
import json
import re
import textwrap
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat.setup import (
    RESULT_ANSWER,
    RESULT_FAILED,
    RESULT_REFUSED,
    _classify_result,
)

pytestmark = pytest.mark.anyio

REPO = Path(__file__).resolve().parents[3]
FIXTURE = Path(__file__).parent / "fixtures" / "openwebui_v0_11_4_text_result_calls.json"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def _hermetic_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)


def _patched_select():
    """The compatibility patch's selection block, as a callable."""
    spec = importlib.util.spec_from_file_location(
        "okg_owui_patch", REPO / "ops" / "openwebui" / "apply_mcp_client_patch.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    body = textwrap.dedent(module.COMPATIBLE_RESULT_SELECTION.decode())
    source = "def select(result):\n" + textwrap.indent(body, "    ")
    namespace: dict[str, Any] = {}
    exec(compile(source, "apply_mcp_client_patch.COMPATIBLE_RESULT_SELECTION", "exec"), namespace)
    return namespace["select"]


def _stored(select, result) -> dict[str, str]:
    """Each Open WebUI storage of one MCP result, by version."""
    try:
        selected = select(result)
    except Exception as exc:  # the vendor raises on isError
        return {"0.11.0": str(exc), "0.11.4": json.dumps({"error": str(exc)})}
    return {"answer": json.dumps(selected, ensure_ascii=False)}


async def _stored_forms(monkeypatch: pytest.MonkeyPatch) -> dict[str, dict[str, str]]:
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setattr(
        server_mod, "_server_dsn_resolution",
        lambda _dsn, _name: {"dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"}},
    )
    mcp = server_mod.build_server(dsn=PROBE_DSN)
    page = {"items": [{"node_id": "n:1", "subtype": "t"}], "total_count": 1,
            "pinned_generation_id": "g1"}
    monkeypatch.setattr(mcp._tool_manager._tools["filter"], "fn", lambda *a, **k: dict(page))

    def widget_write() -> dict:
        raise RuntimeError("widget store unavailable")

    mcp.tool(name="widget_write", structured_output=False)(widget_write)
    select = _patched_select()
    try:
        async with create_connected_server_and_client_session(mcp._mcp_server) as session:
            return {
                "filter_answer": _stored(select, await session.call_tool(
                    "filter", {"input": {"kind": "nodes"}, "predicate": {}})),
                # The probe pool is never opened: a raised read refusal.
                "search_refused": _stored(select, await session.call_tool(
                    "search", {"query": "x"})),
                "search_invalid_budget": _stored(select, await session.call_tool(
                    "search", {"query": "x", "max_bytes": 10_000_000})),
                "write_raised": _stored(select, await session.call_tool("widget_write", {})),
            }
    finally:
        mcp._okg_pool.close()


async def test_real_text_results_classify_as_answer_refused_and_failed(monkeypatch) -> None:
    forms = await _stored_forms(monkeypatch)
    assert _classify_result(forms["filter_answer"]["answer"]) == (RESULT_ANSWER, None)
    for version in ("0.11.0", "0.11.4"):
        kind, code = _classify_result(forms["search_refused"][version])
        assert (kind, code) == (RESULT_REFUSED, "operator_execution_failed"), version
        kind, code = _classify_result(forms["search_invalid_budget"][version])
        assert (kind, code) == (RESULT_REFUSED, "invalid_response_byte_budget"), version
        # A write tool's raised exception is the SDK's own text: no status
        # line, so a failure, never a refusal or an answer.
        assert _classify_result(forms["write_raised"][version]) == (RESULT_FAILED, None)


def test_the_recorded_fixture_classifies_the_same_way() -> None:
    recorded = json.loads(FIXTURE.read_text("utf-8"))
    forms = recorded["forms"]
    assert _classify_result(forms["filter_answer"]["answer"]) == (RESULT_ANSWER, None)
    for version in ("0.11.0", "0.11.4"):
        assert _classify_result(forms["search_refused"][version]) == (
            RESULT_REFUSED, "operator_execution_failed",
        )
        assert _classify_result(forms["search_invalid_budget"][version]) == (
            RESULT_REFUSED, "invalid_response_byte_budget",
        )
        assert _classify_result(forms["write_raised"][version]) == (RESULT_FAILED, None)


def scrub(forms: dict) -> dict:
    """The forms with run-specific ids replaced (session uuid, error id)."""
    text = json.dumps(forms, sort_keys=True)
    text = re.sub(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        "00000000-0000-0000-0000-000000000000", text,
    )
    text = re.sub(r"(?<![0-9a-f-])[0-9a-f]{12}(?![0-9a-f])", "<error-id>", text)
    return json.loads(text)


async def test_the_fixture_matches_what_the_branch_produces(monkeypatch) -> None:
    """The fixture is the real form, not a hand-written guess: regenerated
    with the run-specific ids scrubbed, it is identical."""
    forms = scrub(await _stored_forms(monkeypatch))
    recorded = json.loads(FIXTURE.read_text("utf-8"))["forms"]
    assert forms == recorded
