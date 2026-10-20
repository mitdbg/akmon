"""Unit tests for `McpComposedSource` base class
(`agent-led-mcp-onboarding` Phase 2).

Validates:
  - preflight resolves the MCP server command via PATH /
    absolute-path lookup
  - missing executable surfaces as `missing_credential` with a
    fix-hint pointing the operator at `source_registry.yaml`
  - run() spawns the subprocess, calls `_emit_facts`, and returns
    the facts as a SourceRun
  - run() returns an empty stream + `completed_scope=True` (not an
    uncaught exception) when the subprocess fails to spawn
"""
from __future__ import annotations

import sys
from pathlib import Path

from okg.substrate.library.sources.base import NodeFact, SourceRun
from okg.substrate.library.sources.mcp_composed import (
    McpClient, McpComposedSource,
)


STUB_PATH = Path(__file__).parent / "stub_mcp_server.py"


class _EchoSource(McpComposedSource):
    name = "echo_source"

    def _emit_facts(self, client: McpClient):
        result = client.call_tool(
            "echo", {"payload": {"sample": True}},
        )
        # Synthesize one NodeFact from the echoed payload so we can
        # assert end-to-end roundtrip.
        yield NodeFact(
            node_id="paper:roundtrip",
            subtype="paper",
            attrs={"title": "roundtrip", "result": result},
            source_record_id={"source": self.name, "key": "rt"},
            source_revision={"content_sha": "deadbeef"},
        )


def _stub_cmd() -> list[str]:
    return [sys.executable, str(STUB_PATH)]


def test_preflight_ok_when_command_resolves() -> None:
    src = _EchoSource(
        mcp_server_command=_stub_cmd(),
        deployment_name="test",
    )
    res = src.preflight()
    assert res.status == "ok"


def test_preflight_missing_credential_when_command_missing() -> None:
    src = _EchoSource(
        mcp_server_command=["definitely-not-a-real-command-xyz123"],
        deployment_name="test",
    )
    res = src.preflight()
    assert res.status == "missing_credential"
    assert "source_registry.yaml" in res.reason


def test_preflight_missing_when_command_empty() -> None:
    src = _EchoSource(
        mcp_server_command=[],
        deployment_name="test",
    )
    res = src.preflight()
    assert res.status == "missing_credential"
    assert "empty" in res.reason.lower()


def test_run_yields_facts_from_subprocess() -> None:
    src = _EchoSource(
        mcp_server_command=_stub_cmd(),
        deployment_name="test",
    )
    run = src.run(run_id="r1", mode="cursor")
    assert isinstance(run, SourceRun)
    facts = list(run.facts)
    assert len(facts) == 1
    assert isinstance(facts[0], NodeFact)
    assert facts[0].node_id == "paper:roundtrip"
    assert run.completed_scope is True


def test_run_returns_empty_when_command_missing() -> None:
    src = _EchoSource(
        mcp_server_command=["definitely-not-a-real-command-xyz123"],
        deployment_name="test",
    )
    run = src.run(run_id="r1", mode="cursor")
    # Spawn failure surfaces as an EMPTY SourceRun, NOT an
    # uncaught exception. The broader publish keeps going.
    assert list(run.facts) == []
    assert run.completed_scope is True


def test_default_emit_facts_raises_not_implemented() -> None:
    """The base class's `_emit_facts` raises NotImplementedError —
    swallowed by `run`'s catch-all, producing an empty stream so
    a misconfigured deployment doesn't crash the publish."""
    src = McpComposedSource(
        mcp_server_command=_stub_cmd(),
        deployment_name="test",
    )
    run = src.run(run_id="r1", mode="cursor")
    assert list(run.facts) == []
    assert run.completed_scope is True
