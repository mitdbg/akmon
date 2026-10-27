"""A promoted waiting edge is read through the real MCP tool boundary.

SECURITY (cross-source-endpoint-wedge). Sources b and d each write an edge
onto a widget source a has not written yet; both edges wait. After a
publishes the widgets both are promoted. Under a policy that allows only
source ``svc.d``, an MCP ``filter`` over edges shows d's edge and hides b's.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.mcp.server import build_server
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from tests.pytest import endpoint_wedge_fixture as fx
from tests.pytest.source_jobs_inline import use_inline_source_jobs

pytestmark = pytest.mark.postgres

_SUBJECT = "subject:mcp-client"


def _allow(rule_id: str, **match: str) -> dict:
    return {
        "rule_id": rule_id, "effect": "allow",
        "actions": ["mcp.tool.call", "graph.read.mcp"],
        "subjects": [_SUBJECT],
        "match": {"deployment": "svc", "scope": "deployment:svc", **match},
        "priority": 100,
    }


def _activate(dsn: str) -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:waiting-edge-mcp",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            _allow("tool_call", resource_kind="mcp_tool"),
            _allow("graph_boundary", resource_kind="mcp_graph_read_boundary"),
            _allow("operator_boundary", operator_name="mcp.read"),
            _allow("only_d", source_id="svc.d"),
        ],
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn, deployment_name="svc", bundle=bundle,
            scope="deployment:svc", activated_by="test:operator",
        )
        conn.commit()


def test_mcp_hides_a_promoted_edge_from_a_denied_source(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    use_inline_source_jobs(monkeypatch)
    dep = fx.build_deployment(tmp_path, clean_runtime)
    fx.write_fixture(
        dep, "b", [fx.node("note:1", "note")], [fx.edge("note:1", "widget:1")],
    )
    fx.write_fixture(
        dep, "d", [fx.node("note:9", "note")], [fx.edge("note:9", "widget:2")],
    )
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    assert fx.ingest(clean_runtime, dep, "d").any_failed is False
    assert len(fx.waiting_rows(clean_runtime)) == 2
    fx.write_fixture(
        dep, "a",
        [fx.node("widget:1", "widget"), fx.node("widget:2", "widget")], [],
    )
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    assert fx.waiting_rows(clean_runtime) == []
    assert fx.edge_triples(clean_runtime) == {
        ("note:1", "references", "widget:1"),
        ("note:9", "references", "widget:2"),
    }

    _activate(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", _SUBJECT)
    mcp = build_server(dsn=clean_runtime, deployment_name="svc")
    try:
        edges = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "edges"}, predicate={},
            select=["edge_id", "src", "dst"], limit=10,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert [(i["src"], i["dst"]) for i in edges["items"]] == [
        ("note:9", "widget:2"),
    ], "the denied source's promoted edge is hidden, the allowed one shown"
    assert edges["total_count"] == 1
