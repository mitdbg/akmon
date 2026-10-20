"""req.csp.policy / req.csp.identity -- the parts that need no database.

Deliberately unmarked and DSN-free so the fast lane runs them on every pull
request. Anything that reads or writes a sample table lives in the marked
postgres modules beside this one; the rules those tables enforce are decided
here first.
"""
from __future__ import annotations

import hashlib
from contextlib import contextmanager

import pytest

from okg.substrate.external_backed.call_sample import (
    INLINE_MAX_BYTES,
    NODE_TARGETED_TOOLS,
    RETENTION_CLASSES,
    SAMPLE_POLICIES,
    CallSampleError,
    RefetchPointer,
    RelationPolicyRegistry,
    RelationSamplePolicy,
    build_sample,
    default_registry,
    digest_of,
    encode_payload,
    graph_read_identity,
    graph_read_policy,
    live_identity,
    live_relation_policy,
    record_live_relation_call_sample,
    refetch_pointer_for_call,
    register_relation_sample_policy,
    relation_for_call,
    resolve_call_target,
    summarize,
    table_for_class,
    target_node_for_call,
    target_nodes_for_call,
)

_DEPLOY = "default"
_NODE = "node:cluster:gpu07"
_RELATION = "live.cluster_jobs"


def _identity():
    return live_identity(
        principal="user:operator",
        decision="allow",
        trace_id="t",
        session_id="s",
    )


# --- the policy vocabulary -------------------------------------------------


def test_the_four_policies_and_three_classes() -> None:
    assert SAMPLE_POLICIES == {"none", "metadata", "inline", "digest"}
    assert RETENTION_CLASSES == ("hot", "warm", "cold")
    assert [table_for_class(c) for c in RETENTION_CLASSES] == [
        "okg.call_sample_hot",
        "okg.call_sample_warm",
        "okg.call_sample_cold",
    ]


def test_graph_reads_default_to_none() -> None:
    policy = default_registry().policy_for("graph.expand")
    assert policy.policy == "none"
    assert policy.writes is False
    # An undeclared relation is a graph read, so the tracing wrapper owns it.
    assert policy.graph_read is True


def test_graph_read_and_live_policies_differ_in_who_may_write() -> None:
    read = graph_read_policy(_RELATION, policy="metadata")
    live = live_relation_policy(_RELATION)
    assert read.graph_read is True
    assert live.graph_read is False
    # Both store something; only one may be stored by the tracing wrapper.
    assert read.writes and live.writes


def test_an_unknown_policy_or_class_is_refused() -> None:
    with pytest.raises(CallSampleError, match="policy"):
        RelationSamplePolicy(relation=_RELATION, policy="sometimes")
    with pytest.raises(CallSampleError, match="retention_class"):
        RelationSamplePolicy(relation=_RELATION, policy="inline", retention_class="warmish")
    with pytest.raises(CallSampleError, match="retention_class"):
        table_for_class("lukewarm")


# --- the identity contract -------------------------------------------------


@pytest.mark.parametrize("missing", ["principal", "decision", "trace_id", "session_id"])
def test_live_identity_refuses_a_missing_value(missing: str) -> None:
    values = {
        "principal": "user:operator",
        "decision": "allow",
        "trace_id": "t",
        "session_id": "s",
    }
    values[missing] = None
    with pytest.raises(CallSampleError, match=missing):
        live_identity(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize("blank", ["", "   "])
def test_live_identity_refuses_a_blank_principal(blank: str) -> None:
    with pytest.raises(CallSampleError, match="principal"):
        live_identity(
            principal=blank, decision="allow", trace_id="t", session_id="s"
        )


def test_an_unknown_decision_is_refused() -> None:
    with pytest.raises(CallSampleError, match="decision"):
        live_identity(
            principal="p", decision="maybe", trace_id="t", session_id="s"
        )


def test_graph_read_identity_discloses_its_default() -> None:
    identity = graph_read_identity(trace_id="t", session_id="s")
    assert identity.decision == "allow"
    assert identity.decision_source == "graph_read_default"
    # trace_id and session_id are NOT defaulted: inventing them would break
    # the join back to the agent session.
    with pytest.raises(CallSampleError, match="session_id"):
        graph_read_identity(trace_id="t", session_id=None)


# --- the 16 KiB cap, decided without a database ---------------------------


def _payload_of_exactly(byte_target: int) -> dict[str, str]:
    overhead = len(encode_payload({"blob": ""}))
    return {"blob": "x" * (byte_target - overhead)}


def test_inline_at_the_cap_stays_inline() -> None:
    payload = _payload_of_exactly(INLINE_MAX_BYTES)
    sample = build_sample(
        deployment_name=_DEPLOY,
        node_id=_NODE,
        relation=_RELATION,
        policy=RelationSamplePolicy(relation=_RELATION, policy="inline"),
        identity=_identity(),
        status="ok",
        payload=payload,
    )
    assert sample is not None
    assert sample.policy == "inline" and sample.degraded is False
    assert sample.payload == payload
    assert sample.payload_bytes == INLINE_MAX_BYTES


def test_inline_one_byte_over_the_cap_degrades_to_digest() -> None:
    payload = _payload_of_exactly(INLINE_MAX_BYTES + 1)
    sample = build_sample(
        deployment_name=_DEPLOY,
        node_id=_NODE,
        relation=_RELATION,
        policy=RelationSamplePolicy(relation=_RELATION, policy="inline"),
        identity=_identity(),
        status="ok",
        payload=payload,
        refetch=RefetchPointer(query={"edge_type": _RELATION}, source="slurm"),
    )
    assert sample is not None
    assert sample.policy == "digest" and sample.declared_policy == "inline"
    assert sample.degraded is True
    assert sample.payload is None
    assert sample.payload_bytes == INLINE_MAX_BYTES + 1
    assert sample.payload_sha256 == hashlib.sha256(encode_payload(payload)).hexdigest()
    assert sample.attrs["degraded_from"] == "inline"
    assert sample.refetch == {"query": {"edge_type": _RELATION}, "source": "slurm"}


def test_digest_of_a_payload_without_a_pointer_is_refused() -> None:
    with pytest.raises(CallSampleError, match="re-fetch pointer"):
        build_sample(
            deployment_name=_DEPLOY,
            node_id=_NODE,
            relation=_RELATION,
            policy=RelationSamplePolicy(relation=_RELATION, policy="digest"),
            identity=_identity(),
            status="ok",
            payload={"rows": [1]},
        )


def test_digest_is_of_the_canonical_encoding() -> None:
    sha, size = digest_of({"b": 1, "a": 2})
    assert sha == hashlib.sha256(b'{"a": 2, "b": 1}').hexdigest()
    assert size == len(b'{"a": 2, "b": 1}')


def test_summary_is_bounded() -> None:
    summary = summarize({"blob": "x" * 5000}, limit=64)
    assert len(summary) == 64
    assert summary.endswith("…")


def test_policy_none_and_a_missing_node_build_nothing() -> None:
    args = dict(
        deployment_name=_DEPLOY,
        relation=_RELATION,
        identity=_identity(),
        status="ok",
        payload={"rows": [1]},
    )
    assert (
        build_sample(
            node_id=_NODE,
            policy=RelationSamplePolicy(relation=_RELATION, policy="none"),
            **args,
        )
        is None
    )
    assert (
        build_sample(
            node_id=None,
            policy=RelationSamplePolicy(relation=_RELATION, policy="inline"),
            **args,
        )
        is None
    )


def test_a_policy_for_another_relation_is_refused() -> None:
    with pytest.raises(CallSampleError, match="declared for relation"):
        build_sample(
            deployment_name=_DEPLOY,
            node_id=_NODE,
            relation="live.other",
            policy=RelationSamplePolicy(relation=_RELATION, policy="inline"),
            identity=_identity(),
            status="ok",
        )


# --- which node a call targets --------------------------------------------


def test_only_two_tools_target_a_node() -> None:
    assert NODE_TARGETED_TOOLS == {"inspect", "expand"}


@pytest.mark.parametrize(
    "tool",
    [
        "search",
        "aggregate",
        "query",
        # These were the denylist's blind spot: they have no anchor argument,
        # so a denylist treated them as node-targeted and warned on every call.
        "filter",
        "map",
        "list_sources",
        "add_source",
        "remove_source",
        "source_status",
        "record",
        "propose",
        "upload",
        "a_tool_added_next_year",
    ],
)
def test_a_tool_that_is_not_node_targeted_reports_no_gap(tool: str) -> None:
    target = resolve_call_target(tool, kwargs={"query": _NODE, "name": "git"})
    assert target.node_ids == ()
    # Not a gap: these were never supposed to name a node.
    assert target.node_targeted is False
    assert target_node_for_call(tool, kwargs={"query": "gpu"}) is None


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"target": {"kind": "node", "node_id": _NODE}}, (_NODE,)),
        # The review's shape: kind/id rather than kind/node_id.
        ({"target": {"kind": "node", "id": _NODE}}, (_NODE,)),
        ({"input": _NODE}, (_NODE,)),
        ({"node_id": _NODE}, (_NODE,)),
        ({"input": {"node_id": _NODE}}, (_NODE,)),
        # A list anchor: one sample per node, order preserved, duplicates dropped.
        ({"input": [_NODE, "node:b", _NODE]}, (_NODE, "node:b")),
        ({"input": [{"id": "node:a"}, {"node_id": "node:b"}]}, ("node:a", "node:b")),
    ],
)
def test_node_anchors_are_resolved(kwargs: dict, expected: tuple) -> None:
    assert target_nodes_for_call("expand", kwargs=kwargs) == expected


@pytest.mark.parametrize(
    "kind", ["graph", "coverage", "subtype", "view", "surface", "ontology"]
)
def test_inspect_on_a_non_node_kind_is_not_node_targeted(kind: str) -> None:
    target = resolve_call_target("inspect", kwargs={"target": {"kind": kind}})
    assert target.node_ids == ()
    assert target.node_targeted is False


@pytest.mark.parametrize("anchor", [42, None, [], {"kind": "node"}, [None]])
def test_an_unreadable_anchor_is_reported_as_a_gap(anchor) -> None:
    # Node-targeted by name, but nothing usable in the arguments: the caller
    # must be able to log this rather than skip it in silence.
    kwargs = {"target": anchor} if isinstance(anchor, dict) else {"input": anchor}
    target = resolve_call_target("expand", kwargs=kwargs)
    assert target.node_ids == ()
    assert target.node_targeted is True
    assert target.arg_shape


def test_arg_shape_describes_shape_not_values() -> None:
    shape = resolve_call_target(
        "expand", kwargs={"input": {"secret_token": "hunter2"}}
    ).arg_shape
    assert "hunter2" not in shape
    assert "secret_token" in shape  # the KEY is shape; the value is not


# --- which relation a call is sampled under -------------------------------


def test_expand_is_sampled_per_edge_type() -> None:
    assert relation_for_call("expand", kwargs={"edge_type": "live.jobs"}) == "live.jobs"
    assert relation_for_call("expand", kwargs={}) == "graph.expand"
    assert relation_for_call("inspect", kwargs={}) == "graph.inspect"


def test_registry_declaration_replaces_the_default() -> None:
    registry = RelationPolicyRegistry()
    assert registry.policy_for(_RELATION).policy == "none"
    registry.declare(live_relation_policy(_RELATION, retention_class="hot"))
    declared = registry.policy_for(_RELATION)
    assert declared.policy == "inline"
    assert declared.retention_class == "hot"
    assert declared.graph_read is False
    assert registry.declared_relations == (_RELATION,)
    with pytest.raises(CallSampleError, match="RelationSamplePolicy"):
        registry.declare("live.cluster_jobs")  # type: ignore[arg-type]


def test_process_registration_configures_the_production_registry() -> None:
    relation = "graph.unit_process_registration"
    configured = graph_read_policy(relation, policy="metadata")
    try:
        assert register_relation_sample_policy(configured) is configured
        assert default_registry().policy_for(relation) is configured
    finally:
        # Registration is startup-scoped and has no reload lifecycle. Restore
        # the process registry to its safe no-write behavior for later tests.
        register_relation_sample_policy(graph_read_policy(relation))


# --- the live writer requires a declared live policy and real identity -----


def _live_writer_kwargs() -> dict[str, object]:
    return {
        "deployment_name": _DEPLOY,
        "tool_name": "expand",
        "relation": _RELATION,
        "node_id": _NODE,
        "principal": "user:operator",
        "decision": "allow",
        "trace_id": "trace-live",
        "session_id": "session-live",
        "status": "ok",
        "payload": {"rows": []},
    }


class _RecordingCursor:
    def __init__(self, statements: list[tuple[str, object]]) -> None:
        self._statements = statements

    def execute(self, sql: str, params: object = None) -> None:
        self._statements.append((sql, params))


class _RecordingConnection:
    def __init__(self) -> None:
        self.statements: list[tuple[str, object]] = []

    @contextmanager
    def cursor(self):
        yield _RecordingCursor(self.statements)


def test_live_writer_refuses_an_undeclared_policy() -> None:
    with pytest.raises(CallSampleError, match="no registered sample policy"):
        record_live_relation_call_sample(
            object(), registry=RelationPolicyRegistry(), **_live_writer_kwargs()
        )


def test_live_writer_refuses_a_graph_read_policy() -> None:
    registry = RelationPolicyRegistry(
        [graph_read_policy(_RELATION, policy="metadata")]
    )
    with pytest.raises(CallSampleError, match="graph-read policy"):
        record_live_relation_call_sample(
            object(), registry=registry, **_live_writer_kwargs()
        )


@pytest.mark.parametrize(
    ("decision", "status"), [("allow", "ok"), ("deny", "denied")]
)
def test_live_writer_records_the_supplied_authorization_identity(
    decision: str, status: str
) -> None:
    registry = RelationPolicyRegistry([live_relation_policy(_RELATION)])
    conn = _RecordingConnection()
    kwargs = _live_writer_kwargs()
    kwargs.update(decision=decision, status=status)
    stored = record_live_relation_call_sample(conn, registry=registry, **kwargs)
    assert stored is not None
    assert stored.policy == "inline"  # live_relation_policy's declared default
    assert stored.identity.principal == "user:operator"
    assert stored.identity.decision == decision
    assert stored.identity.trace_id == "trace-live"
    assert stored.identity.session_id == "session-live"
    assert stored.identity.decision_source is None
    assert len(conn.statements) == 2  # sample insert + watermark upsert


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("principal", None),
        ("decision", "maybe"),
        ("trace_id", None),
        ("session_id", " "),
    ],
)
def test_live_writer_refuses_invented_or_invalid_identity(
    field: str, value: object
) -> None:
    registry = RelationPolicyRegistry([live_relation_policy(_RELATION)])
    kwargs = _live_writer_kwargs()
    kwargs[field] = value
    with pytest.raises(CallSampleError, match=field):
        record_live_relation_call_sample(object(), registry=registry, **kwargs)


def test_explicit_live_none_policy_writes_zero_rows() -> None:
    registry = RelationPolicyRegistry(
        [RelationSamplePolicy(relation=_RELATION, policy="none", graph_read=False)]
    )
    assert (
        record_live_relation_call_sample(
            object(), registry=registry, **_live_writer_kwargs()
        )
        is None
    )


# --- what a digest sample may keep from the call's arguments --------------


def test_a_refetch_pointer_keeps_only_the_shape_by_default() -> None:
    policy = live_relation_policy(_RELATION)
    assert policy.refetch_query_keys == ()
    pointer = refetch_pointer_for_call(
        policy,
        kwargs={"api_token": "hunter2", "person": "Riley", "edge_type": _RELATION},
        arg_shape="input=str",
        source="slurm",
    )
    stored = pointer.as_dict()
    assert stored["query"] == {"arg_shape": "input=str"}
    assert "hunter2" not in repr(stored)
    assert "Riley" not in repr(stored)


def test_a_relation_opts_a_key_in_by_name() -> None:
    policy = RelationSamplePolicy(
        relation=_RELATION,
        policy="digest",
        refetch_query_keys=("edge_type",),
    )
    stored = refetch_pointer_for_call(
        policy,
        kwargs={"edge_type": _RELATION, "api_token": "hunter2"},
        arg_shape="input=str",
        source="slurm",
    ).as_dict()
    assert stored["query"]["args"] == {"edge_type": _RELATION}
    assert "hunter2" not in repr(stored)


def test_refetch_query_keys_must_be_a_tuple_of_names() -> None:
    with pytest.raises(CallSampleError, match="refetch_query_keys"):
        RelationSamplePolicy(
            relation=_RELATION, policy="digest", refetch_query_keys=["edge_type"]
        )
    with pytest.raises(CallSampleError, match="refetch_query_keys"):
        RelationSamplePolicy(
            relation=_RELATION, policy="digest", refetch_query_keys=("",)
        )


# --- a writing default must not hand the tracer a live relation -----------


def test_a_writing_default_is_not_treated_as_a_graph_read() -> None:
    # With a `none` default an undeclared relation is a graph read, and the
    # tracing wrapper owns it. With a WRITING default it is not: the wrapper
    # would otherwise stamp its graph-read `allow` on a relation nobody
    # declared, which is the defect graph_read exists to prevent.
    assert RelationPolicyRegistry().policy_for("live.unknown").graph_read is True
    for writing in ("metadata", "inline", "digest"):
        fallback = RelationPolicyRegistry(default_policy=writing).policy_for(
            "live.unknown"
        )
        assert fallback.policy == writing
        assert fallback.writes is True
        assert fallback.graph_read is False
