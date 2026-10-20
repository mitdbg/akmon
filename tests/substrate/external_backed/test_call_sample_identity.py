"""task.csp.identity / req.csp.identity (issue #2011).

Every sample carries deployment, node_id, relation, observed_at, status,
principal, authorization decision, trace_id and session_id -- and joins back to
the agent session that produced it.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from okg.substrate.external_backed.call_sample import (
    RelationPolicyRegistry,
    graph_read_policy,
    live_identity,
    live_relation_policy,
    record_live_relation_call_sample,
    register_relation_sample_policy,
)
from okg.substrate.telemetry import TelemetrySpan, record_span

pytestmark = pytest.mark.postgres

_DEPLOY = "default"
_NODE = "node:cluster:gpu07"
_RELATION = "live.cluster_jobs"
_SESSION = "session:agent:opus-5:2026-09-19"
_TRACE = "trace-agent-session-1"


def _conn(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=True)


@pytest.fixture
def agent_session(clean_runtime: str) -> str:
    """A fixture agent session: one trace + one MCP span, as the tracer writes.

    ``okg.telemetry_traces`` is the runtime's record of an agent session --
    ``session_id`` is the agent's stable session identity and ``trace_id`` the
    call's trace. A sample must be joinable back to this row.
    """
    started = datetime.now(timezone.utc) - timedelta(seconds=5)
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE okg.call_sample_hot, okg.call_sample_warm, "
            "okg.call_sample_cold, okg.call_sample_watermark"
        )
    with _conn(clean_runtime) as conn:
        record_span(
            conn,
            TelemetrySpan(
                trace_id=_TRACE,
                span_id="span-1",
                span_name="mcp.expand",
                span_kind="server",
                deployment_name=_DEPLOY,
                session_id=_SESSION,
                tool_name="expand",
                started_at=started,
                ended_at=started + timedelta(milliseconds=12),
                root_name="mcp.expand",
                root_kind="agent_session",
            ),
        )
    return clean_runtime


def _write_sample(conn: psycopg.Connection, **overrides) -> None:
    identity = overrides.pop(
        "identity",
        live_identity(
            principal="user:operator",
            decision="allow",
            trace_id=_TRACE,
            session_id=_SESSION,
        ),
    )
    record_live_relation_call_sample(
        conn,
        deployment_name=_DEPLOY,
        tool_name="expand",
        relation=_RELATION,
        node_id=_NODE,
        principal=identity.principal,
        decision=identity.decision,
        trace_id=identity.trace_id,
        session_id=identity.session_id,
        status=overrides.pop("status", "ok"),
        payload={"rows": [{"job": "a"}]},
        registry=RelationPolicyRegistry([live_relation_policy(_RELATION)]),
        **overrides,
    )


def test_sample_carries_every_identity_field(agent_session: str) -> None:
    with _conn(agent_session) as conn:
        _write_sample(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT deployment_name, node_id, relation, observed_at, status, "
                "principal, decision, trace_id, session_id "
                "FROM okg.call_sample_warm"
            )
            row = cur.fetchone()
    assert row is not None
    (
        deployment,
        node_id,
        relation,
        observed_at,
        status,
        principal,
        decision,
        trace_id,
        session_id,
    ) = row
    assert deployment == _DEPLOY
    assert node_id == _NODE
    assert relation == _RELATION
    assert observed_at is not None
    assert status == "ok"
    assert principal == "user:operator"
    assert decision == "allow"
    assert trace_id == _TRACE
    assert session_id == _SESSION


def test_sample_joins_back_to_the_agent_session(agent_session: str) -> None:
    with _conn(agent_session) as conn:
        _write_sample(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.node_id, s.relation, s.principal, s.decision,
                       t.session_id, t.root_kind, sp.tool_name
                FROM okg.call_sample_warm s
                JOIN okg.telemetry_traces t
                  ON t.trace_id = s.trace_id
                 AND t.session_id = s.session_id
                 AND t.deployment_name = s.deployment_name
                JOIN okg.telemetry_spans sp
                  ON sp.trace_id = s.trace_id
                 AND sp.session_id = s.session_id
                """
            )
            joined = cur.fetchall()
    assert len(joined) == 1, "the sample must join to exactly one agent session"
    node_id, relation, principal, decision, session_id, root_kind, tool_name = joined[0]
    assert (node_id, relation) == (_NODE, _RELATION)
    assert (principal, decision) == ("user:operator", "allow")
    assert session_id == _SESSION
    assert root_kind == "agent_session"
    assert tool_name == "expand"


def test_a_sample_from_another_session_does_not_join(agent_session: str) -> None:
    # The join is on identity, not on proximity: a sample written under a
    # different session must not attach itself to this agent session.
    with _conn(agent_session) as conn:
        _write_sample(
            conn,
            identity=live_identity(
                principal="service:collector",
                decision="allow",
                trace_id="trace-other",
                session_id="session:other",
            ),
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                FROM okg.call_sample_warm s
                JOIN okg.telemetry_traces t
                  ON t.trace_id = s.trace_id AND t.session_id = s.session_id
                """
            )
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM okg.call_sample_warm")
            assert cur.fetchone()[0] == 1


def test_a_denied_call_records_the_decision(agent_session: str) -> None:
    with _conn(agent_session) as conn:
        _write_sample(
            conn,
            status="denied",
            identity=live_identity(
                principal="user:stranger",
                decision="deny",
                trace_id=_TRACE,
                session_id=_SESSION,
            ),
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, principal, decision, attrs "
                "FROM okg.call_sample_warm"
            )
            status, principal, decision, attrs = cur.fetchone()
    assert (status, principal, decision) == ("denied", "user:stranger", "deny")
    # A real decision is NOT marked as a graph-read default.
    assert "decision_source" not in attrs


# --- the production call site ---------------------------------------------
#
# The wrapper's sampling arm is reached from _write_unified_mcp_span, not
# called directly. This exercises that path against a real database: the same
# function the tracer calls writes the telemetry span AND the sample, and the
# two join on the identity it derived.


def test_the_unified_span_writer_also_leaves_a_sample(agent_session: str) -> None:
    from okg.substrate.mcp.tracing import _write_unified_mcp_span

    relation = "graph.configured_mentions"
    register_relation_sample_policy(graph_read_policy(relation, policy="inline"))
    try:
        with _conn(agent_session) as conn:
            _write_unified_mcp_span(
                conn,
                deployment_name=_DEPLOY,
                tool_name="expand",
                request={"args": (), "kwargs": {"input": _NODE, "edge_type": relation}},
                response={"rows": [{"job": "a"}], "returned_count": 1},
                error=None,
                duration_ms=12,
                session_id=_SESSION,
                generation_id=None,
                question_label=None,
                branch_name="default",
                route_id=None,
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.node_id, s.relation, s.policy, s.session_id,
                           sp.span_name, sp.tool_name
                    FROM okg.call_sample_warm s
                    JOIN okg.telemetry_spans sp
                      ON sp.trace_id = s.trace_id AND sp.session_id = s.session_id
                    """
                )
                joined = cur.fetchall()
    finally:
        register_relation_sample_policy(graph_read_policy(relation))

    assert len(joined) == 1, "the span writer must leave exactly one sample"
    node_id, relation, policy, session_id, span_name, tool_name = joined[0]
    assert (node_id, relation, policy) == (
        _NODE,
        "graph.configured_mentions",
        "inline",
    )
    assert session_id == _SESSION
    assert (span_name, tool_name) == ("mcp.expand", "expand")


def test_the_unified_span_writer_samples_nothing_by_default(agent_session: str) -> None:
    # No relation declares a policy, so the production path writes a span and
    # no sample at all.
    from okg.substrate.mcp.tracing import _write_unified_mcp_span

    with _conn(agent_session) as conn:
        _write_unified_mcp_span(
            conn,
            deployment_name=_DEPLOY,
            tool_name="expand",
            request={"args": (), "kwargs": {"input": _NODE, "edge_type": "graph.mentions"}},
            response={"rows": []},
            error=None,
            duration_ms=3,
            session_id=_SESSION,
            generation_id=None,
            question_label=None,
            branch_name="default",
            route_id=None,
        )
        with conn.cursor() as cur:
            for table in (
                "okg.call_sample_hot",
                "okg.call_sample_warm",
                "okg.call_sample_cold",
            ):
                cur.execute(f"SELECT count(*) FROM {table}")
                assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM okg.telemetry_spans")
            assert cur.fetchone()[0] >= 1
