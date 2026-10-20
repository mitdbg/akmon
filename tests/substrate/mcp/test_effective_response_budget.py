"""One response byte budget per read call, through the REAL entry point.

Issue #2156. The MCP SDK hands a tool every argument, schema defaults
included. While the read tools declared `max_bytes: Any = 65536`, the wrapper's
"did the caller set it?" test was always true over MCP, the deployment default
was never applied, and a deployment whose ceiling sat below 65,536 refused
every call that omitted `max_bytes`. Calling the Python function directly
hid it, so every test here goes through `build_server` + `FastMCP.call_tool`.

The server runs in surface-probe mode (no database). A call that passes the
budget check then fails on the unopened pool, and that failure is itself the
raised-error refusal path, bounded by the same budget. The database-backed
partner is in `tests/substrate/external_backed/test_live_relation_call_tool.py`.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.mcp.operators import OPERATOR_TOOLS
from tests.substrate.mcp._result_payload import (
    call_json,
    json_client_sessions,
    payload as _json_payload,
)


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch):
    """Program-side test: every call here reads the JSON result format
    (issue #3059), through request `_meta` or the in-process scope."""
    json_client_sessions(monkeypatch)


REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = REPO_ROOT / "tests/substrate/e2e/fixtures/live-http-ci"
BUDGET = 16_384


@pytest.fixture
def probe_server(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """A probe-mode server on a deployment with default = ceiling = 16 KiB."""
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    root = tmp_path / "deployments"
    shutil.copytree(FIXTURE, root / "live-http-ci")
    manifest = root / "live-http-ci" / "deployment.yaml"
    text = manifest.read_text(encoding="utf-8")
    declared = "  response_bytes:\n    ceiling: 32768\n"
    assert declared in text, "the fixture's response_bytes block moved"
    manifest.write_text(
        text.replace(
            declared,
            f"  response_bytes:\n    default: {BUDGET}\n    ceiling: {BUDGET}\n",
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.setattr(
        server_mod, "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )
    mcp = server_mod.build_server(dsn=PROBE_DSN, deployment_name="live-http-ci")
    try:
        yield mcp
    finally:
        mcp._okg_pool.close()


async def _search(mcp: Any, **extra: Any) -> dict[str, Any]:
    result = await call_json(mcp, "search", {"query": "x", **extra})
    return _json_payload(result)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "extra", [{}, {"max_bytes": None}], ids=["omitted", "null"],
)
async def test_an_unset_max_bytes_takes_the_deployment_default(
    probe_server: Any, extra: dict[str, Any],
) -> None:
    """The #2156 regression: omitted and null both mean "use the default".

    Before the fix the omitted call was refused `invalid_response_byte_budget`
    with `max_bytes: 65536`, a number the caller never sent.
    """
    body = await _search(probe_server, **extra)
    error = body.get("error") or {}
    assert error.get("code") != "invalid_response_byte_budget", body
    # It passed the budget check and reached the operator, which fails on
    # the probe pool. That refusal is bounded by, and names, the SAME budget.
    assert error.get("code") == "operator_execution_failed", body
    assert error.get("max_bytes") == BUDGET, body


@pytest.mark.anyio
@pytest.mark.parametrize("tool", ["search", "inspect"])
async def test_an_explicit_budget_inside_the_ceiling_is_used_as_is(
    probe_server: Any, tool: str,
) -> None:
    """Non-vacuity partner: an explicit value is honored, not replaced.

    Both operators then RAISE on the probe pool, so this is also the
    raised-error path reporting the caller's valid integer, not a default.
    """
    arguments: dict[str, Any] = (
        {"query": "x"} if tool == "search" else {"target": {"kind": "ontology"}}
    )
    result = await call_json(probe_server, tool, {**arguments, "max_bytes": 8192})
    body = _json_payload(result)
    error = body.get("error") or {}
    assert error.get("code") == "operator_execution_failed", body
    assert error.get("max_bytes") == 8192, body


@pytest.mark.anyio
@pytest.mark.parametrize(
    "value", ["8192", 16384.0, True], ids=["string", "float", "bool"],
)
async def test_a_non_integer_budget_is_refused_by_name_not_coerced(
    probe_server: Any, value: Any,
) -> None:
    """`max_bytes` is a STRICT integer.

    A lax `int` coerced these for the operator while the raised-error path,
    which reads the raw arguments, rejected them, so an operator failure
    reported a 4,096-byte budget the caller never sent. `True` became 1.
    """
    body = await _search(probe_server, max_bytes=value)
    error = body.get("error") or {}
    assert error.get("code") == "invalid_operator_arguments", body
    assert "max_bytes" in error["data"]["invalid_fields"], body


@pytest.mark.anyio
async def test_an_explicit_budget_over_the_ceiling_is_refused_naming_it(
    probe_server: Any,
) -> None:
    """Refused, never clamped, and the refusal names the ceiling."""
    body = await _search(probe_server, max_bytes=65_536)
    error = body.get("error") or {}
    assert error.get("code") == "invalid_response_byte_budget", body
    assert error.get("max_bytes") == 65_536, body
    assert f"between 4096 and {BUDGET}" in error["message"], body
    assert "agent_surface.response_bytes.ceiling" in error["message"], body


@pytest.mark.anyio
async def test_a_raised_error_refusal_carries_the_resolved_budget(
    probe_server: Any,
) -> None:
    """The raised-error path resolves the budget the wrapper would have.

    `inspect(kind='ontology')` passes the budget check and then raises on the
    probe pool, and `ByteBoundedFastMCP.call_tool` bounds that exception.
    That path used to read the raw argument, falling back to 65,536 and
    ignoring the deployment entirely.
    """
    result = await call_json(probe_server,
        "inspect", {"target": {"kind": "ontology"}},
    )
    body = _json_payload(result)
    assert result.isError, body
    assert body["error"]["max_bytes"] == BUDGET, body


@pytest.mark.anyio
async def test_every_read_tool_publishes_max_bytes_as_a_plain_optional_integer(
    probe_server: Any,
) -> None:
    """Decision 3A on #2141: no published default and no maximum.

    A published default is what the SDK materializes into every call, which
    is the whole of #2156. The argument must also stay optional. The maximum
    stays unpublished because the ceiling is per deployment.

    #2792 publishes the MINIMUM, which 3A had also left out: it is the same
    4,096 B on every deployment, and an Ollama-served model that could not
    see it asked for 4,000 and was refused. Publishing it changes no
    outcome -- `resolve_budget` still refuses below it with its own typed
    envelope, because the SDK validates against the argument model, not
    against this schema.
    """
    from okg.substrate.mcp.response_bounds import MIN_RESPONSE_BYTES

    tools = {tool.name: tool for tool in await probe_server.list_tools()}
    served = [name for name in OPERATOR_TOOLS if name in tools]
    assert served, "the fixture serves no read tool"
    for name in served:
        schema = tools[name].inputSchema
        if name == "search":
            # read-passage-rows-3666 (D3): search accepts max_bytes but does
            # not publish it.
            assert "max_bytes" not in schema["properties"]
            continue
        assert schema["properties"]["max_bytes"] == {
            "minimum": MIN_RESPONSE_BYTES, "type": "integer",
        }, (
            name, json.dumps(schema["properties"]["max_bytes"]),
        )
        assert "max_bytes" not in schema.get("required", []), name
