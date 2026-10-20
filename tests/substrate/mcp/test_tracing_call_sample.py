"""req.csp.policy at the tracing wrapper (issue #2011).

Every operator call passes through `make_tracer`'s wrapper, so that is where a
node-targeted call leaves its sample. These are wrapper-contract tests: they
drive `record_call_sample_for_tool_call` against a recording connection, so
they assert WHAT the wrapper decides to write, not how Postgres stores it (the
storage contract is proved in tests/substrate/external_backed).
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

from okg.substrate.external_backed.call_sample import (
    INLINE_MAX_BYTES,
    RelationPolicyRegistry,
    encode_payload,
    graph_read_policy,
    live_relation_policy,
    register_relation_sample_policy,
)
from okg.substrate.mcp.tracing import (
    _write_unified_mcp_span,
    record_call_sample_for_tool_call,
)

_DEPLOY = "default"
_NODE = "node:cluster:gpu07"


class _RecordingCursor:
    def __init__(self, log: list[tuple[str, Any]]) -> None:
        self._log = log

    def execute(self, sql: str, params: Any = None) -> None:
        self._log.append((sql, params))


class _RecordingConnection:
    """A connection that records statements instead of executing them."""

    def __init__(self) -> None:
        self.statements: list[tuple[str, Any]] = []

    @contextmanager
    def cursor(self):
        yield _RecordingCursor(self.statements)

    @property
    def inserts(self) -> list[tuple[str, Any]]:
        return [s for s in self.statements if s[0].lstrip().startswith("INSERT")]


def _call(conn, tool_name: str, kwargs: dict[str, Any], **overrides):
    return record_call_sample_for_tool_call(
        conn,
        deployment_name=_DEPLOY,
        tool_name=tool_name,
        request={"args": (), "kwargs": kwargs},
        response=overrides.pop("response", {"rows": [1, 2]}),
        error=overrides.pop("error", None),
        trace_id=overrides.pop("trace_id", "trace-1"),
        session_id=overrides.pop("session_id", "session-1"),
        registry=overrides.pop("registry", None),
    )


def _registry(relation: str, policy: str) -> RelationPolicyRegistry:
    """A GRAPH-READ policy: the only kind this wrapper is allowed to sample."""
    return RelationPolicyRegistry([graph_read_policy(relation, policy=policy)])


def test_a_graph_read_writes_no_sample() -> None:
    # No relation has declared a policy, so expand is a graph read: none.
    conn = _RecordingConnection()
    assert _call(conn, "expand", {"input": _NODE, "edge_type": "mentions"}) == ()
    assert conn.statements == []


def test_a_search_writes_no_sample_even_when_declared() -> None:
    conn = _RecordingConnection()
    result = _call(
        conn,
        "search",
        {"query": "gpu"},
        registry=_registry("graph.search", "inline"),
    )
    assert result == ()
    assert conn.statements == []


def test_a_declared_relation_writes_one_sample() -> None:
    conn = _RecordingConnection()
    written = _call(
        conn,
        "expand",
        {"input": _NODE, "edge_type": "live.cluster_jobs"},
        registry=_registry("live.cluster_jobs", "inline"),
    )
    assert len(written) == 1
    sample = written[0]
    assert sample.node_id == _NODE
    assert sample.relation == "live.cluster_jobs"
    assert sample.policy == "inline"
    assert sample.identity.trace_id == "trace-1"
    assert sample.identity.session_id == "session-1"
    # The wrapper's decision is the GRAPH-READ default, and says so.
    assert sample.identity.decision == "allow"
    assert sample.attrs["decision_source"] == "graph_read_default"
    # Exactly one sample row, plus its watermark upsert.
    assert len(conn.inserts) == 2


def test_production_span_writer_uses_the_process_registry() -> None:
    relation = "graph.production_registry_contract"
    register_relation_sample_policy(graph_read_policy(relation, policy="metadata"))
    try:
        conn = _RecordingConnection()
        _write_unified_mcp_span(
            conn,
            deployment_name=_DEPLOY,
            tool_name="expand",
            request={"args": (), "kwargs": {"input": _NODE, "edge_type": relation}},
            response={"rows": [1]},
            error=None,
            duration_ms=2,
            session_id="session-production-registry",
            generation_id=None,
            question_label=None,
            branch_name="default",
            route_id=None,
        )
        sample_inserts = [
            statement
            for statement in conn.inserts
            if "call_sample_warm" in statement[0]
        ]
        assert len(sample_inserts) == 1
        assert relation in sample_inserts[0][1]
    finally:
        register_relation_sample_policy(graph_read_policy(relation))


def test_an_over_cap_response_degrades_to_digest_with_a_pointer() -> None:
    overhead = len(encode_payload({"blob": ""}))
    response = {"blob": "x" * (INLINE_MAX_BYTES + 1 - overhead)}
    conn = _RecordingConnection()
    written = _call(
        conn,
        "expand",
        {"input": _NODE, "edge_type": "live.cluster_jobs"},
        response=response,
        registry=_registry("live.cluster_jobs", "inline"),
    )
    assert len(written) == 1
    sample = written[0]
    assert sample.degraded is True
    assert sample.policy == "digest" and sample.declared_policy == "inline"
    assert sample.payload is None
    assert sample.payload_bytes == INLINE_MAX_BYTES + 1
    # The pointer carries the argument SHAPE, not the arguments.
    assert sample.refetch == {
        "query": {"arg_shape": "input=str"},
        "source": "mcp:expand",
    }


def test_a_failed_call_is_sampled_with_status_error() -> None:
    conn = _RecordingConnection()
    written = _call(
        conn,
        "expand",
        {"input": _NODE, "edge_type": "live.cluster_jobs"},
        response=None,
        error=RuntimeError("adapter down"),
        registry=_registry("live.cluster_jobs", "metadata"),
    )
    assert len(written) == 1
    assert written[0].status == "error"


def test_a_sample_write_failure_does_not_break_the_tool_call(caplog) -> None:
    class _BrokenConnection(_RecordingConnection):
        @contextmanager
        def cursor(self):
            raise RuntimeError("database gone")
            yield  # pragma: no cover

    with caplog.at_level("ERROR"):
        result = _call(
            _BrokenConnection(),
            "expand",
            {"input": _NODE, "edge_type": "live.cluster_jobs"},
            registry=_registry("live.cluster_jobs", "inline"),
        )
    assert result == ()
    # Swallowed, but never silent.
    assert any("call sample write failed" in r.message for r in caplog.records)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"target": {"kind": "graph"}},
        {"target": {"kind": "subtype", "subtype": "pact_change"}},
        {},
    ],
)
def test_inspect_without_a_node_target_writes_nothing(kwargs: dict[str, Any]) -> None:
    conn = _RecordingConnection()
    assert (
        _call(
            conn,
            "inspect",
            kwargs,
            registry=_registry("graph.inspect", "inline"),
        )
        == ()
    )
    assert conn.statements == []


# --- the wrapper never records a live relation's decision -----------------


def test_a_live_relation_is_skipped_with_a_warning(caplog) -> None:
    # The wrapper holds no authorization decision. Sampling a live relation
    # here would stamp the graph-read default `allow` on a call a Nomos gate
    # may have DENIED. It is skipped, loudly, and the dispatcher writes the
    # only sample.
    registry = RelationPolicyRegistry([live_relation_policy("live.cluster_jobs")])
    assert registry.policy_for("live.cluster_jobs").writes is True
    conn = _RecordingConnection()
    with caplog.at_level("WARNING"):
        written = _call(
            conn,
            "expand",
            {"input": _NODE, "edge_type": "live.cluster_jobs"},
            registry=registry,
        )
    assert written == ()
    assert conn.statements == []
    assert any(
        "live.cluster_jobs" in r.message and "live relation" in r.message
        for r in caplog.records
    )


def test_a_graph_read_relation_is_sampled_without_a_warning(caplog) -> None:
    conn = _RecordingConnection()
    with caplog.at_level("WARNING"):
        written = _call(
            conn,
            "expand",
            {"input": _NODE, "edge_type": "graph.mentions"},
            registry=_registry("graph.mentions", "metadata"),
        )
    assert len(written) == 1
    assert [r for r in caplog.records if "skipped" in r.message] == []


# --- multi-node anchors and unreadable anchors ----------------------------


def test_a_list_anchor_writes_one_sample_per_node() -> None:
    conn = _RecordingConnection()
    written = _call(
        conn,
        "expand",
        {"input": [_NODE, "node:cluster:gpu08"], "edge_type": "graph.mentions"},
        registry=_registry("graph.mentions", "metadata"),
    )
    assert [s.node_id for s in written] == [_NODE, "node:cluster:gpu08"]
    # Two sample inserts plus two watermark upserts.
    assert len(conn.inserts) == 4


def test_a_kind_id_target_is_resolved() -> None:
    conn = _RecordingConnection()
    written = _call(
        conn,
        "inspect",
        {"target": {"kind": "node", "id": _NODE}},
        registry=_registry("graph.inspect", "metadata"),
    )
    assert [s.node_id for s in written] == [_NODE]


@pytest.mark.parametrize(
    "tool", ["list_sources", "filter", "map", "record", "add_source"]
)
def test_a_tool_that_names_no_node_does_not_warn(tool: str, caplog) -> None:
    # The denylist treated every unlisted tool as node-targeted, so these
    # warned on every call about an anchor they were never meant to have.
    conn = _RecordingConnection()
    with caplog.at_level("WARNING"):
        written = _call(
            conn, tool, {"name": "git"}, registry=_registry(f"graph.{tool}", "inline")
        )
    assert written == ()
    assert conn.statements == []
    assert [r for r in caplog.records if "node-targeted" in r.message] == []


def test_the_stored_refetch_pointer_does_not_carry_caller_arguments() -> None:
    # A digest sample can live 730 days. A caller's arguments -- a token, a
    # person's name -- are not copied into it.
    conn = _RecordingConnection()
    written = _call(
        conn,
        "expand",
        {
            "input": _NODE,
            "edge_type": "graph.mentions",
            "api_token": "hunter2",
            "person": "Riley",
        },
        response={"blob": "x" * (INLINE_MAX_BYTES + 64)},
        registry=_registry("graph.mentions", "inline"),
    )
    assert len(written) == 1
    sample = written[0]
    assert sample.policy == "digest"
    assert sample.refetch["query"] == {
        "arg_shape": "input=str",
    }
    assert sample.refetch["source"] == "mcp:expand"
    assert "hunter2" not in repr(sample.refetch)
    assert "Riley" not in repr(sample.refetch)
    # And nothing reached the INSERT parameters either.
    assert "hunter2" not in repr(conn.statements)


def test_a_relation_may_opt_one_argument_into_the_pointer() -> None:
    conn = _RecordingConnection()
    registry = RelationPolicyRegistry(
        [
            graph_read_policy(
                "graph.mentions", policy="digest"
            ).__class__(
                relation="graph.mentions",
                policy="digest",
                graph_read=True,
                refetch_query_keys=("edge_type",),
            )
        ]
    )
    written = _call(
        conn,
        "expand",
        {"input": _NODE, "edge_type": "graph.mentions", "api_token": "hunter2"},
        registry=registry,
    )
    assert written[0].refetch["query"]["args"] == {"edge_type": "graph.mentions"}
    assert "hunter2" not in repr(written[0].refetch)


def test_an_unreadable_anchor_is_logged_not_swallowed(caplog) -> None:
    conn = _RecordingConnection()
    with caplog.at_level("WARNING"):
        written = _call(
            conn,
            "expand",
            {"input": 42, "edge_type": "graph.mentions"},
            registry=_registry("graph.mentions", "metadata"),
        )
    assert written == ()
    assert conn.statements == []
    warnings = [r.message for r in caplog.records]
    assert any("expand" in m and "node-targeted" in m for m in warnings)
    assert any("input=int" in m for m in warnings)


def test_an_untargeted_tool_is_skipped_without_a_warning(caplog) -> None:
    conn = _RecordingConnection()
    with caplog.at_level("WARNING"):
        written = _call(
            conn, "search", {"query": "gpu"}, registry=_registry("graph.search", "inline")
        )
    assert written == ()
    # search naming no node is correct, not a gap: nothing to report.
    assert [r for r in caplog.records if "node-targeted" in r.message] == []


def test_a_setup_failure_is_named_as_a_sample_skip(caplog) -> None:
    # Everything before the write loop runs inside the unified-telemetry
    # writer. An error from here must not surface under that subsystem's name.
    class _ExplodingRegistry:
        def policy_for(self, relation):
            raise RuntimeError("registry unavailable")

    conn = _RecordingConnection()
    with caplog.at_level("ERROR"):
        written = _call(
            conn,
            "expand",
            {"input": _NODE, "edge_type": "graph.mentions"},
            registry=_ExplodingRegistry(),
        )
    assert written == ()
    assert conn.statements == []
    assert any("call sample skipped for expand" in r.message for r in caplog.records)
