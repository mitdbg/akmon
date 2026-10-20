"""Run the Helm readiness hook's MCP proof against a real MCP server.

The chart's ``mcp`` test container (``ops/kubernetes/okg/templates/tests/
readiness.yaml``) calls ``inspect`` and two ``filter`` operators over
streamable HTTP and prints ``OKG_MCP_PROOF=<json>``. The Kubernetes
verifier refuses an empty node or edge sample. The hook once counted the
``rows`` field, which ``filter`` stopped returning, so it reported zero
samples against a populated graph. These tests execute the hook's own
script, taken from the chart template, against the in-process MCP server
with a published fixture generation.
"""

from __future__ import annotations

import json
import re
import textwrap
from contextlib import asynccontextmanager
from pathlib import Path

import anyio
import pytest

pytest.importorskip("mcp")

import mcp.client.streamable_http as streamable_http  # noqa: E402
from mcp.shared.memory import create_client_server_memory_streams  # noqa: E402

from okg.substrate.mcp.server import build_server  # noqa: E402
from tests.substrate.mcp.test_operator_surface_integration import (  # noqa: E402
    _seed_operator_fixture,
)

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.usefixtures("okg_mcp_can_log_in"),
]

REPO = Path(__file__).resolve().parents[3]
READINESS_TEMPLATE = (
    REPO / "ops" / "kubernetes" / "okg" / "templates" / "tests" / "readiness.yaml"
)
HOOK_URL = "http://okg-hook-test-mcp:8000/mcp"
_TEMPLATE_EXPRESSION = re.compile(r"\{\{.*?\}\}")


def mcp_hook_script_from_template(url: str = HOOK_URL) -> str:
    """Return the ``mcp`` container's Python script from the chart template.

    The block holds exactly one Helm expression, the MCP URL; it is replaced
    by ``url`` as a JSON string literal, which is what ``| quote`` renders.
    """
    lines = READINESS_TEMPLATE.read_text(encoding="utf-8").splitlines()
    container = lines.index("    - name: mcp")
    args = lines.index("      args:", container)
    if lines[args + 1].strip() != "- |":
        raise AssertionError("mcp container args is not a literal block")
    block: list[str] = []
    for line in lines[args + 2 :]:
        if line.strip() and not line.startswith("          "):
            break
        block.append(line)
    script = textwrap.dedent("\n".join(block)).strip("\n") + "\n"
    expressions = _TEMPLATE_EXPRESSION.findall(script)
    if len(expressions) != 1 or "mcp.port" not in expressions[0]:
        raise AssertionError(f"unexpected Helm expressions: {expressions!r}")
    return _TEMPLATE_EXPRESSION.sub(lambda _match: json.dumps(url), script)


def _flatten(error: BaseException) -> list[BaseException]:
    if isinstance(error, BaseExceptionGroup):
        return [leaf for inner in error.exceptions for leaf in _flatten(inner)]
    return [error]


def _run_hook(server, monkeypatch: pytest.MonkeyPatch, capsys) -> dict:
    """Execute the hook script with its HTTP transport bound to ``server``."""
    urls: list[str] = []

    @asynccontextmanager
    async def in_memory_client(url, *args, **kwargs):
        urls.append(url)
        lowlevel = server._mcp_server
        async with create_client_server_memory_streams() as (client, served):
            async with anyio.create_task_group() as group:
                group.start_soon(
                    lambda: lowlevel.run(
                        served[0],
                        served[1],
                        lowlevel.create_initialization_options(),
                        raise_exceptions=True,
                    )
                )
                try:
                    yield client[0], client[1], lambda: None
                finally:
                    group.cancel_scope.cancel()

    monkeypatch.setattr(streamable_http, "streamable_http_client", in_memory_client)
    script = mcp_hook_script_from_template()
    exec(compile(script, "<mcp-helm-test>", "exec"), {"__name__": "__main__"})
    assert urls == [HOOK_URL]
    proof_lines = [
        line
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("OKG_MCP_PROOF=")
    ]
    assert len(proof_lines) == 1, proof_lines
    return json.loads(proof_lines[0].removeprefix("OKG_MCP_PROOF="))


def _close(server) -> None:
    server._okg_pool.close()
    if server._okg_write_pool:
        server._okg_write_pool.close()


def test_readiness_hook_counts_real_filter_items(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    fixture = _seed_operator_fixture(clean_runtime)
    generation_id = str(fixture["generation_id"])
    server = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        proof = _run_hook(server, monkeypatch, capsys)
    finally:
        _close(server)

    # The fixture publishes two nodes and one edge; the hook asks for 100.
    assert proof["visible_node_sample_count"] == 2, proof
    assert proof["visible_edge_sample_count"] == 1, proof
    assert proof["node_sample_pinned_generation_id"] == generation_id
    assert proof["edge_sample_pinned_generation_id"] == generation_id


def test_readiness_hook_refuses_a_filter_response_without_items(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    _seed_operator_fixture(clean_runtime)
    server = build_server(dsn=clean_runtime, deployment_name="default")
    filter_tool = server._tool_manager._tools["filter"]
    real_filter = filter_tool.fn
    observed: list[dict] = []

    def filter_without_items(*args, **kwargs):
        response = real_filter(*args, **kwargs)
        observed.append(response)
        return {key: value for key, value in response.items() if key != "items"}

    monkeypatch.setattr(filter_tool, "fn", filter_without_items)
    try:
        with pytest.raises(BaseException) as raised:
            _run_hook(server, monkeypatch, capsys)
    finally:
        _close(server)

    # The real response had items; only the stripped copy reached the hook.
    assert observed and isinstance(observed[0].get("items"), list)
    messages = [str(error) for error in _flatten(raised.value)]
    assert any(
        "MCP filter nodes response has no items list" in message for message in messages
    ), messages
    assert "OKG_MCP_PROOF=" not in capsys.readouterr().out
