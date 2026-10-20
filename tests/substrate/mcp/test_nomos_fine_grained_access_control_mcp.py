from __future__ import annotations

import psycopg
import pytest

from okg.substrate.errors import NodeNotFound, SubstrateError
from okg.substrate.mcp import open_session
from okg.substrate.mcp.run_sql import RunSqlInvalid, run_sql
from okg.substrate.mcp.server import build_server
from tests.substrate.nomos.test_nomos_fine_grained_access_control import (
    _activate_fine_grained_policy,
    _publish_fine_grained_graph,
)


pytestmark = pytest.mark.postgres

# These tests open the real MCP read pool, which logs in as ``okg_mcp``. A
# fresh migration creates that role NOLOGIN, so without this fixture they
# pass only when an earlier test in the same session enabled the login.
_MCP_READ_POOL_LOGIN = pytest.mark.usefixtures("okg_mcp_can_log_in")


def _assert_nomos_attribute_aggregate_refused(error: SubstrateError) -> None:
    """An attribute aggregate under Nomos enforcement is refused, not counted.

    A SQL GROUP BY cannot apply per-record Nomos visibility, and the broad
    Python fallback that used to count visible rows was removed on purpose
    (PACT fix-mcp-attr-sql-fallback, req.no-aggregate-fallback). The operator
    must refuse with a typed, actionable error, so no denied record can reach
    a count. A regression that counted every row, denied ones included, would
    return groups instead of raising and fail this check.
    """
    # Issue #3666: said in plain words, not "cannot execute exactly in SQL".
    from okg.substrate.mcp.server import AGGREGATE_UNDER_ACCESS_RULES

    assert str(error) == AGGREGATE_UNDER_ACCESS_RULES
    assert "counting is not available under this deployment's access rules" in str(error)
    assert getattr(error, "code", None) == "unsupported_operator_form"
    assert error.data["nomos_enforced"] is True
    assert error.data["by"] == ["attrs.classification"]
    assert error.fix_hint


@_MCP_READ_POOL_LOGIN
def test_mcp_read_surfaces_apply_fine_grained_denies(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fine_grained_graph(clean_runtime)
    _activate_fine_grained_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:benchmark")

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        tools = mcp._tool_manager._tools
        visible = tools["inspect"].fn(
            target={"kind": "node", "node_id": "node:ticket-allowed"},
            generation_id=generation_id,
        )
        with pytest.raises(NodeNotFound):
            tools["inspect"].fn(
                target={"kind": "node", "node_id": "node:ticket-record-denied"},
                generation_id=generation_id,
            )
        with pytest.raises(NodeNotFound):
            tools["inspect"].fn(
                target={"kind": "node", "node_id": "node:derived-mixed"},
                generation_id=generation_id,
            )
        filtered = tools["filter"].fn(
            input={"kind": "nodes"},
            predicate={},
            select=["node_id"],
            limit=20,
            generation_id=generation_id,
        )
        searched = tools["search"].fn(
            query="ticket",
            method="lexical",
            limit=10,
            generation_id=generation_id,
        )
        expanded = tools["expand"].fn(
            input={"node_id": "node:ticket-allowed"},
            generation_id=generation_id,
        )
        with pytest.raises(SubstrateError) as aggregate_refusal:
            tools["aggregate"].fn(
                input={"kind": "nodes"},
                by=["attrs.classification"],
                metrics=["count"],
                generation_id=generation_id,
            )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert visible["node_id"] == "node:ticket-allowed"
    assert sorted(item["node_id"] for item in filtered["items"]) == [
        "node:derived-allowed",
        "node:derived-future",
        "node:note-edge-denied",
        "node:note-visible",
        "node:ticket-allowed",
        "node:ticket-future",
    ]
    assert sorted(row["node_id"] for row in searched["results"]) == [
        "node:derived-allowed",
        "node:derived-future",
        "node:ticket-allowed",
        "node:ticket-future",
    ]
    # Issue #3666: a 1-hop row is the linked record. The record behind the
    # denied link is not reached and not counted, and nothing names it.
    assert [row["node_id"] for row in expanded["neighbors"]] == ["node:note-visible"]
    assert [row["link_count"] for row in expanded["neighbors"]] == [1]
    assert expanded["total_count"] == 1
    assert "node:note-edge-denied" not in str(expanded)
    _assert_nomos_attribute_aggregate_refused(aggregate_refusal.value)


@_MCP_READ_POOL_LOGIN
def test_mcp_search_uses_authority_access_context(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fine_grained_graph(clean_runtime)
    _activate_fine_grained_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:benchmark")

    authority = {
        "access_context": {
            "context_id": "ctx:mcp-horizon",
            "as_of": "2026-06-15T00:00:00Z",
        }
    }

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        result = mcp._tool_manager._tools["search"].fn(
            query="ticket",
            method="lexical",
            limit=10,
            generation_id=generation_id,
            authority=authority,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert sorted(row["node_id"] for row in result["results"]) == [
        "node:derived-allowed",
        "node:ticket-allowed",
    ]


def test_graph_backed_sql_remains_refused_under_fine_grained_nomos(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_fine_grained_graph(clean_runtime)
    _activate_fine_grained_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:benchmark")

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        with pytest.raises(RunSqlInvalid, match="source-lineage filtered"):
            run_sql(conn, session, "SELECT node_id FROM okg.v_nodes")


@_MCP_READ_POOL_LOGIN
def test_aggregate_counts_exclude_denied_records_or_refuse(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fine_grained_graph(clean_runtime)
    _activate_fine_grained_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:benchmark")

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        with pytest.raises(SubstrateError) as refusal:
            mcp._tool_manager._tools["aggregate"].fn(
                input={"kind": "nodes"},
                by=["attrs.classification"],
                metrics=["count"],
                generation_id=generation_id,
            )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    _assert_nomos_attribute_aggregate_refused(refusal.value)


@_MCP_READ_POOL_LOGIN
def test_operator_subject_can_read_records_hidden_from_restricted_subject(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fine_grained_graph(clean_runtime)
    _activate_fine_grained_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:operator")

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        result = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes"},
            predicate={},
            select=["node_id"],
            limit=20,
            generation_id=generation_id,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert sorted(item["node_id"] for item in result["items"]) == [
        "node:derived-allowed",
        "node:derived-future",
        "node:derived-mixed",
        "node:note-edge-denied",
        "node:note-visible",
        "node:ticket-allowed",
        "node:ticket-future",
        "node:ticket-graph-denied",
        "node:ticket-record-denied",
    ]
