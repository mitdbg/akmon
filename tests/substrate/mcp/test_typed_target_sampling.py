"""Typed MCP targets remain sampleable through the public call boundary."""

from __future__ import annotations

import asyncio

import psycopg
import pytest

from okg.substrate.external_backed.call_sample import (
    default_registry,
    graph_read_policy,
    register_relation_sample_policy,
)

pytest.importorskip("mcp")

from test_principal_mapping import (  # noqa: E402
    PRINCIPAL_A,
    PRINCIPAL_HEADER,
    SUBJECT_A,
    _activate_two_principal_policy,
    _configure_header_identity,
    _decision_rows,
    _multi_user_server,
    _publish_two_source_graph,
    request_headers,
)


@pytest.mark.postgres
@pytest.mark.serial_db
def test_public_typed_node_target_reaches_the_registered_sample_writer(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A public typed target remains node-addressable after validation.

    FastMCP validates the caller's mapping into ``TNode`` before the tracer
    sees it. The tracer must keep a caller-shaped mapping for target
    resolution, while the call still crosses the real principal gate and the
    production process registry writes a sample joined to this MCP span.
    """
    relation = "graph.inspect"
    session_id = "session:typed-target-sampling"
    runtime_principal = "runtime:typed-target-sampling"
    prior_policy = default_registry().policy_for(relation)

    _publish_two_source_graph(clean_runtime)
    _configure_header_identity(
        clean_runtime,
        principals={PRINCIPAL_A: SUBJECT_A},
    )
    _activate_two_principal_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "1")
    monkeypatch.setenv("OKG_MCP_SESSION_ID", session_id)
    monkeypatch.setenv("OKG_PRINCIPAL", runtime_principal)

    register_relation_sample_policy(graph_read_policy(relation, policy="metadata"))
    try:
        with _multi_user_server(clean_runtime) as mcp:
            with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}):
                result = asyncio.run(
                    mcp.call_tool(
                        "inspect",
                        {
                            "target": {
                                "kind": "node",
                                "node_id": "node:public-docs",
                            }
                        },
                    )
                )
        assert result

        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT (SELECT count(*) FROM okg.call_sample_hot), "
                "       (SELECT count(*) FROM okg.call_sample_warm), "
                "       (SELECT count(*) FROM okg.call_sample_cold)"
            )
            assert cur.fetchone() == (0, 1, 0)
            cur.execute(
                """
                SELECT s.node_id, s.relation, s.policy, s.status,
                       s.principal, s.decision, s.session_id, s.attrs,
                       sp.span_name, sp.tool_name, sp.session_id
                  FROM okg.call_sample_warm s
                  JOIN okg.telemetry_spans sp
                    ON sp.trace_id = s.trace_id
                   AND sp.session_id = s.session_id
                 WHERE s.tool_name = 'inspect'
                """
            )
            joined = cur.fetchall()
    finally:
        register_relation_sample_policy(prior_policy)

    assert len(joined) == 1
    (
        node_id,
        sampled_relation,
        policy,
        status,
        principal,
        decision,
        sampled_session,
        attrs,
        span_name,
        span_tool,
        span_session,
    ) = joined[0]
    assert (node_id, sampled_relation, policy, status) == (
        "node:public-docs",
        relation,
        "metadata",
        "ok",
    )
    assert (principal, decision) == (runtime_principal, "allow")
    assert attrs["decision_source"] == "graph_read_default"
    assert (sampled_session, span_session) == (session_id, session_id)
    assert (span_name, span_tool) == ("mcp.inspect", "inspect")

    tool_decisions = [
        row for row in _decision_rows(clean_runtime) if row["action"] == "mcp.tool.call"
    ]
    assert {row["subject"] for row in tool_decisions} == {SUBJECT_A}
    assert {row["effect"] for row in tool_decisions} == {"allow"}
