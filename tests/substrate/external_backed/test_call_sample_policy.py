"""task.csp.writer / req.csp.policy (issue #2011) -- the STORED behaviour.

The decisions these tests exercise are made without a database in
test_call_sample_unit.py; this module proves what actually lands in the three
sample tables and what the column constraints refuse.

One sample per node-targeted operator call, under a per-relation policy in
{none, metadata, inline, digest}. Graph reads default to none. inline stores
the payload at or below 16 KiB and degrades to digest above it, with a valid
sha256, a bounded summary and a re-fetch pointer carrying query and source. A
call with no target node writes nothing.
"""
from __future__ import annotations

import hashlib

import psycopg
import psycopg.errors
import pytest

from okg.substrate.external_backed.call_sample import (
    INLINE_MAX_BYTES,
    RefetchPointer,
    RelationPolicyRegistry,
    RelationSamplePolicy,
    SampleIdentity,
    encode_payload,
    graph_read_identity,
    live_identity,
    record_tool_call_sample,
    relation_for_call,
    target_node_for_call,
)

pytestmark = pytest.mark.postgres

_DEPLOY = "default"
_NODE = "node:cluster:gpu07"
_RELATION = "live.cluster_jobs"


def _conn(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=True)


@pytest.fixture(autouse=True)
def _empty_sample_tables(clean_runtime: str):
    """The shared clean_runtime fixture does not know these tables yet.

    Each test starts from an empty sample store so a row count is a statement
    about THIS call, not about the file's history.
    """
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE okg.call_sample_hot, okg.call_sample_warm, "
            "okg.call_sample_cold, okg.call_sample_watermark"
        )
    yield


def _identity() -> SampleIdentity:
    return live_identity(
        principal="user:operator",
        decision="allow",
        trace_id="trace-csp-1",
        session_id="session-csp-1",
    )


def _registry(policy: str, *, retention_class: str = "warm") -> RelationPolicyRegistry:
    return RelationPolicyRegistry(
        [
            RelationSamplePolicy(
                relation=_RELATION, policy=policy, retention_class=retention_class
            )
        ]
    )


def _payload_of_exactly(byte_target: int) -> dict[str, str]:
    """A payload whose canonical encoding is exactly ``byte_target`` bytes."""
    overhead = len(encode_payload({"blob": ""}))
    return {"blob": "x" * (byte_target - overhead)}


def _rows(conn: psycopg.Connection, table: str) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT policy, declared_policy, payload, payload_sha256, "
            f"payload_bytes, summary, refetch, principal, decision, attrs "
            f"FROM {table} WHERE node_id=%s AND relation=%s "
            f"ORDER BY observed_at",
            (_NODE, _RELATION),
        )
        return cur.fetchall()


def test_policy_none_writes_no_row(clean_runtime: str) -> None:
    with _conn(clean_runtime) as conn:
        stored = record_tool_call_sample(
            conn,
            deployment_name=_DEPLOY,
            tool_name="expand",
            relation=_RELATION,
            node_id=_NODE,
            identity=_identity(),
            status="ok",
            payload={"rows": [1, 2, 3]},
            registry=_registry("none"),
        )
        assert stored is None
        assert _rows(conn, "okg.call_sample_warm") == []


# --- the three writing policies -------------------------------------------


def test_policy_metadata_stores_no_payload_bytes(clean_runtime: str) -> None:
    with _conn(clean_runtime) as conn:
        stored = record_tool_call_sample(
            conn,
            deployment_name=_DEPLOY,
            tool_name="expand",
            relation=_RELATION,
            node_id=_NODE,
            identity=_identity(),
            status="ok",
            payload={"rows": [1, 2, 3]},
            registry=_registry("metadata"),
        )
        assert stored is not None and stored.policy == "metadata"
        rows = _rows(conn, "okg.call_sample_warm")
    assert len(rows) == 1
    policy, declared, payload, sha, byte_count, summary, _, principal, decision, _ = (
        rows[0]
    )
    assert (policy, declared) == ("metadata", "metadata")
    assert payload is None and sha is None and byte_count is None and summary is None
    assert (principal, decision) == ("user:operator", "allow")


def test_policy_inline_stores_the_payload(clean_runtime: str) -> None:
    payload = {"rows": [{"job": "a"}, {"job": "b"}]}
    with _conn(clean_runtime) as conn:
        stored = record_tool_call_sample(
            conn,
            deployment_name=_DEPLOY,
            tool_name="expand",
            relation=_RELATION,
            node_id=_NODE,
            identity=_identity(),
            status="ok",
            payload=payload,
            registry=_registry("inline"),
        )
        assert stored is not None and stored.degraded is False
        rows = _rows(conn, "okg.call_sample_warm")
    assert len(rows) == 1
    policy, declared, stored_payload, sha, byte_count, _, _, _, _, _ = rows[0]
    assert (policy, declared) == ("inline", "inline")
    assert stored_payload == payload
    assert byte_count == len(encode_payload(payload)) <= INLINE_MAX_BYTES
    assert sha == hashlib.sha256(encode_payload(payload)).hexdigest()


def test_policy_digest_stores_hash_summary_and_pointer(clean_runtime: str) -> None:
    payload = {"rows": [{"job": "a"}]}
    with _conn(clean_runtime) as conn:
        record_tool_call_sample(
            conn,
            deployment_name=_DEPLOY,
            tool_name="expand",
            relation=_RELATION,
            node_id=_NODE,
            identity=_identity(),
            status="ok",
            payload=payload,
            refetch=RefetchPointer(query={"edge_type": _RELATION}, source="slurm"),
            registry=_registry("digest"),
        )
        rows = _rows(conn, "okg.call_sample_warm")
    assert len(rows) == 1
    policy, declared, stored_payload, sha, _, summary, refetch, _, _, _ = rows[0]
    assert (policy, declared) == ("digest", "digest")
    assert stored_payload is None
    assert sha == hashlib.sha256(encode_payload(payload)).hexdigest()
    assert summary
    assert refetch == {"query": {"edge_type": _RELATION}, "source": "slurm"}


# --- the 16 KiB cap --------------------------------------------------------


def test_inline_at_the_cap_stays_inline(clean_runtime: str) -> None:
    payload = _payload_of_exactly(INLINE_MAX_BYTES)
    assert len(encode_payload(payload)) == INLINE_MAX_BYTES
    with _conn(clean_runtime) as conn:
        stored = record_tool_call_sample(
            conn,
            deployment_name=_DEPLOY,
            tool_name="expand",
            relation=_RELATION,
            node_id=_NODE,
            identity=_identity(),
            status="ok",
            payload=payload,
            refetch=RefetchPointer(query={"edge_type": _RELATION}, source="slurm"),
            registry=_registry("inline"),
        )
        assert stored is not None
        rows = _rows(conn, "okg.call_sample_warm")
    assert rows[0][0] == "inline"
    assert rows[0][2] == payload


def test_inline_one_byte_over_the_cap_degrades_to_digest(clean_runtime: str) -> None:
    payload = _payload_of_exactly(INLINE_MAX_BYTES + 1)
    encoded = encode_payload(payload)
    assert len(encoded) == INLINE_MAX_BYTES + 1
    with _conn(clean_runtime) as conn:
        stored = record_tool_call_sample(
            conn,
            deployment_name=_DEPLOY,
            tool_name="expand",
            relation=_RELATION,
            node_id=_NODE,
            identity=_identity(),
            status="ok",
            payload=payload,
            refetch=RefetchPointer(query={"edge_type": _RELATION}, source="slurm"),
            registry=_registry("inline"),
        )
        assert stored is not None and stored.degraded is True
        rows = _rows(conn, "okg.call_sample_warm")
    policy, declared, stored_payload, sha, byte_count, summary, refetch, _, _, attrs = (
        rows[0]
    )
    # Degraded, and the degradation is recorded rather than hidden.
    assert policy == "digest" and declared == "inline"
    assert attrs["degraded_from"] == "inline"
    assert stored_payload is None
    assert byte_count == INLINE_MAX_BYTES + 1
    assert sha == hashlib.sha256(encoded).hexdigest()
    assert summary and len(summary) <= 512
    assert refetch["query"] == {"edge_type": _RELATION}
    assert refetch["source"] == "slurm"


def test_search_writes_no_sample_even_under_a_writing_policy(
    clean_runtime: str,
) -> None:
    registry = RelationPolicyRegistry(
        [RelationSamplePolicy(relation="graph.search", policy="inline")]
    )
    relation = relation_for_call("search", kwargs={"query": "gpu"})
    assert relation == "graph.search"
    assert registry.policy_for(relation).writes is True
    with _conn(clean_runtime) as conn:
        stored = record_tool_call_sample(
            conn,
            deployment_name=_DEPLOY,
            tool_name="search",
            relation=relation,
            node_id=target_node_for_call("search", kwargs={"query": "gpu"}),
            identity=_identity(),
            status="ok",
            payload={"items": [1, 2]},
            registry=registry,
        )
        assert stored is None
        with conn.cursor() as cur:
            for table in (
                "okg.call_sample_hot",
                "okg.call_sample_warm",
                "okg.call_sample_cold",
            ):
                cur.execute(f"SELECT count(*) FROM {table}")
                assert cur.fetchone()[0] == 0


# --- the identity contract has no silent default --------------------------


def test_graph_read_default_is_disclosed_on_the_row(clean_runtime: str) -> None:
    identity = graph_read_identity(trace_id="t-1", session_id="s-1")
    assert identity.decision == "allow"
    assert identity.decision_source == "graph_read_default"
    with _conn(clean_runtime) as conn:
        record_tool_call_sample(
            conn,
            deployment_name=_DEPLOY,
            tool_name="expand",
            relation=_RELATION,
            node_id=_NODE,
            identity=identity,
            status="ok",
            payload={"rows": []},
            registry=_registry("metadata"),
        )
        rows = _rows(conn, "okg.call_sample_warm")
    assert rows[0][9]["decision_source"] == "graph_read_default"


def test_database_refuses_a_blank_principal(clean_runtime: str) -> None:
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        with pytest.raises(psycopg.errors.CheckViolation):
            cur.execute(
                "INSERT INTO okg.call_sample_warm "
                "(deployment_name, node_id, relation, status, principal, "
                " decision, trace_id, session_id, policy, declared_policy) "
                "VALUES (%s,%s,%s,'ok','  ','allow','t','s','metadata','metadata')",
                (_DEPLOY, _NODE, _RELATION),
            )


# --- the role that actually writes the samples ----------------------------
#
# The tracing wrapper runs on the okg_mcp READ pool, not on app_rw. okg_mcp
# gets its table privileges only from okg.grant_registry rows in views.sql, so
# without those rows every live sample insert would fail with permission
# denied, be caught by the tracer, and read back later as `no_data` -- a call
# that happened would look exactly like a call that never did.


_SAMPLE_TABLES = (
    "okg.call_sample_hot",
    "okg.call_sample_warm",
    "okg.call_sample_cold",
)


@pytest.mark.parametrize("table", _SAMPLE_TABLES)
def test_okg_mcp_may_append_a_sample(clean_runtime: str, table: str) -> None:
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname='okg_mcp'")
        assert cur.fetchone() is not None, "okg_mcp role is missing"
        cur.execute(
            "SELECT has_table_privilege('okg_mcp', %s, 'INSERT'), "
            "       has_table_privilege('okg_mcp', %s, 'SELECT')",
            (table, table),
        )
        may_insert, may_select = cur.fetchone()
    assert may_insert, f"okg_mcp cannot INSERT into {table}; the tracer cannot sample"
    assert may_select, f"okg_mcp cannot SELECT from {table}; the reader is blind"


@pytest.mark.parametrize("table", _SAMPLE_TABLES)
@pytest.mark.parametrize("privilege", ["UPDATE", "DELETE"])
def test_a_stored_sample_cannot_be_rewritten(
    clean_runtime: str, table: str, privilege: str
) -> None:
    # Append-only in the strong sense: the read pool holds no grant that could
    # edit or remove a sample after the fact.
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT has_table_privilege('okg_mcp', %s, %s)", (table, privilege)
        )
        assert cur.fetchone()[0] is False, (
            f"okg_mcp holds {privilege} on {table}; a stored sample must not "
            "be rewritable"
        )


def test_okg_mcp_may_upsert_the_watermark(clean_runtime: str) -> None:
    # The watermark IS an upsert (monotonic-min first_observed_at), so it needs
    # UPDATE -- and still no DELETE, because losing a watermark row is what
    # turns `aged_out` back into `no_data`.
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT has_table_privilege('okg_mcp','okg.call_sample_watermark','INSERT'),"
            "       has_table_privilege('okg_mcp','okg.call_sample_watermark','SELECT'),"
            "       has_table_privilege('okg_mcp','okg.call_sample_watermark','UPDATE'),"
            "       has_table_privilege('okg_mcp','okg.call_sample_watermark','DELETE')"
        )
        may_insert, may_select, may_update, may_delete = cur.fetchone()
    assert may_insert and may_select and may_update
    assert may_delete is False


def test_a_real_okg_mcp_session_can_write_a_sample(clean_runtime: str) -> None:
    # has_table_privilege can be satisfied by a grant the session cannot use,
    # so write one sample end to end AS okg_mcp. The role is created NOLOGIN by
    # migration, so SET ROLE is how a session gets its privilege evaluation
    # without inventing a password.
    identity = _identity()
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute("SET ROLE okg_mcp")
        cur.execute(
            "INSERT INTO okg.call_sample_warm "
            "(deployment_name, node_id, relation, status, principal, decision, "
            " trace_id, session_id, policy, declared_policy) "
            "VALUES (%s,%s,%s,'ok',%s,%s,%s,%s,'metadata','metadata')",
            (
                _DEPLOY,
                _NODE,
                _RELATION,
                identity.principal,
                identity.decision,
                identity.trace_id,
                identity.session_id,
            ),
        )
        cur.execute(
            "INSERT INTO okg.call_sample_watermark "
            "(deployment_name, node_id, relation, retention_class, first_observed_at) "
            "VALUES (%s,%s,%s,'warm', now()) "
            "ON CONFLICT (deployment_name, node_id, relation, retention_class) "
            "DO UPDATE SET first_observed_at = LEAST("
            "  okg.call_sample_watermark.first_observed_at,"
            "  EXCLUDED.first_observed_at)",
            (_DEPLOY, _NODE, _RELATION),
        )
        cur.execute("SELECT count(*) FROM okg.call_sample_warm")
        assert cur.fetchone()[0] == 1
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            cur.execute("DELETE FROM okg.call_sample_warm")


def test_the_stored_row_keeps_no_caller_secret(clean_runtime: str) -> None:
    # A cold-class row lives 730 days. What the tracing wrapper stores from a
    # call's arguments is the SHAPE plus whatever keys the relation opted in --
    # never the whole kwargs mapping.
    from okg.substrate.mcp.tracing import record_call_sample_for_tool_call

    registry = RelationPolicyRegistry(
        [
            RelationSamplePolicy(
                relation=_RELATION,
                policy="digest",
                graph_read=True,
                refetch_query_keys=("edge_type",),
            )
        ]
    )
    with _conn(clean_runtime) as conn:
        written = record_call_sample_for_tool_call(
            conn,
            deployment_name=_DEPLOY,
            tool_name="expand",
            request={
                "args": (),
                "kwargs": {
                    "input": _NODE,
                    "edge_type": _RELATION,
                    "api_token": "hunter2",
                    "person": "Riley",
                },
            },
            response={"rows": [{"job": "a"}]},
            error=None,
            trace_id="trace-secret",
            session_id="session-secret",
            registry=registry,
        )
        assert len(written) == 1
        with conn.cursor() as cur:
            cur.execute(
                "SELECT refetch, summary, payload, attrs FROM okg.call_sample_warm"
            )
            row = cur.fetchone()
    refetch, summary, payload, attrs = row
    assert refetch["query"] == {
        "arg_shape": "input=str",
        "args": {"edge_type": _RELATION},
    }
    assert refetch["source"] == "mcp:expand"
    stored = repr(row)
    assert "hunter2" not in stored
    assert "Riley" not in stored
    assert payload is None  # digest stores no payload
    assert summary  # but keeps a bounded summary of the RESPONSE
    assert isinstance(attrs, dict)
