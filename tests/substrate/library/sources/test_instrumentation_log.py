from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.instrumentation_log import (
    InstrumentationLogSource,
)
from tests.mirror_scope import private_path


def _fixture_path() -> Path:
    """This repository's own gap register; the public copy skips its tests."""
    return private_path(
        "deployments/okg-workspace/fixtures/instrumentation/instrument_log.yaml"
    )


_SYNTHETIC_REGISTER = """\
version: 1
source:
  study: synthetic-register
  window: 2026-01-01_to_2026-02-01
  report_ref: a synthetic register for the source's own tests
  warehouse_table: instrument_log
rows:
  - field: example.tokens
    valid_from: "2026-01-01"
    valid_to: "2026-01-20"
    coverage: "half the sessions until the fix"
    consequence: token counts were modeled before the fix
    closed_by: example-fix
  - field: example.cost
    valid_from: "2026-01-01"
    coverage: 3/10
    consequence: cost is estimated for three sessions only
  - field: example.role
    valid_from: "2026-01-05"
    coverage: one harness only
    consequence: role labels arrive with the second harness
"""


def test_instrumentation_log_source_emits_a_synthetic_gap_register(tmp_path: Path) -> None:
    """The source's counting on a register the public copy ships with the test."""
    register = tmp_path / "instrument_log.yaml"
    register.write_text(_SYNTHETIC_REGISTER, encoding="utf-8")
    source = InstrumentationLogSource(
        deployment="example", repo="example", register_path=register,
    )

    run = source.run("run-1", mode="scope_complete")
    nodes = {fact.attrs["field"]: fact for fact in run.facts if isinstance(fact, NodeFact)}

    assert run.completed_scope is True
    assert run.record_set is not None
    assert len(nodes) == 3
    assert len(run.record_set.records) == 3
    assert {node.subtype for node in nodes.values()} == {"instrumentation_gap"}
    assert source.last_run_stats == {
        "rows": 3,
        "open_gaps": 2,
        "closed_gaps": 1,
        "register_path": str(register.resolve()),
    }
    closed = nodes["example.tokens"].attrs
    assert closed["status"] == "closed"
    assert closed["valid_to"] == "2026-01-20"
    assert closed["closed_at"] == "2026-01-20"
    assert closed["closed_by"] == "example-fix"
    assert nodes["example.cost"].attrs["status"] == "open"
    assert nodes["example.cost"].attrs["valid_to"] is None


def test_instrumentation_log_source_emits_standing_gap_register() -> None:
    source = InstrumentationLogSource(
        deployment="okg-workspace",
        repo="okg",
        register_path=_fixture_path(),
    )

    run = source.run("run-1", mode="scope_complete")
    nodes = [fact for fact in run.facts if isinstance(fact, NodeFact)]

    assert run.completed_scope is True
    assert run.record_set is not None
    assert len(nodes) == 9
    assert len(run.record_set.records) == 9
    assert {node.subtype for node in nodes} == {"instrumentation_gap"}
    assert source.last_run_stats == {
        "rows": 9,
        "open_gaps": 7,
        "closed_gaps": 2,
        "register_path": str(_fixture_path().resolve()),
    }


def test_closed_pact_54_gaps_have_dates_and_open_cost_caveat_remains() -> None:
    source = InstrumentationLogSource(
        deployment="okg-workspace",
        repo="okg",
        register_path=_fixture_path(),
    )
    nodes = {
        fact.attrs["field"]: fact
        for fact in source.run("run-1", mode="scope_complete").facts
        if isinstance(fact, NodeFact)
    }

    token_gap = nodes["agent_session.tokens"]
    assert token_gap.attrs["status"] == "closed"
    assert token_gap.attrs["valid_to"] == "2026-06-11"
    assert token_gap.attrs["closed_at"] == "2026-06-11"
    assert token_gap.attrs["closed_by"] == "close-memory-self-observation-gaps"
    assert "PACT #54" in token_gap.attrs["coverage"]
    assert "token_count" in token_gap.attrs["consequence"]

    mcp_gap = nodes["tool_use_event.mcp_tool_call"]
    assert mcp_gap.attrs["status"] == "closed"
    assert mcp_gap.attrs["valid_to"] == "2026-06-11"
    assert "PACT #54" in mcp_gap.attrs["coverage"]
    assert "shell-level memory-surface proxies" in mcp_gap.attrs["consequence"]

    exact_cost_gap = nodes["agent_session.cost_usd_estimate"]
    assert exact_cost_gap.attrs["status"] == "open"
    assert exact_cost_gap.attrs["valid_to"] is None
    assert "direct dollar cost remains harness-provided" in exact_cost_gap.attrs["consequence"]


def test_instrumentation_log_source_requires_valid_register(tmp_path: Path) -> None:
    register = tmp_path / "instrument_log.yaml"
    register.write_text("rows:\n  - field: missing pieces\n", encoding="utf-8")
    source = InstrumentationLogSource(
        deployment="okg-workspace",
        repo="okg",
        register_path=register,
    )

    with pytest.raises(SubstrateError, match="missing required fields"):
        source.run("run-1")
