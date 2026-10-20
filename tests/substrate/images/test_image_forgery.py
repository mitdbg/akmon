"""Only okg's ingest image step may complete an image value (#3046).

PACT first-class-images, req.first-class-images.no-forgery, for every write
path other than the source runner. Each path below is called for real against
a disposable Postgres database:

* an image value carrying ``png`` or ``error`` is refused with
  ``image_value_forged``, and the fact log, the graph, the admission tables and
  the attribute observations are unchanged;
* an unknown kind, a bad path and a non-https URL are refused with
  ``image_ref_invalid``, with nothing written;
* a plain ``file`` or ``url`` value passes and is stored unchanged, except
  on an agent record write (admission submit, MCP ``record``/``propose``,
  the CLI admission submit), which refuses a ``file`` reference with
  ``image_ref_invalid`` and accepts a ``url`` one.

It also covers the paths that carry image values between okgs or chunks:

* Poros export reduces each value to its bare reference, and a real export
  imported through the receiver's admission is accepted and stored bare;
* a forged value in a later runner chunk is not written.

The write paths are the ones listed in the `okg.substrate.images.values`
module docstring.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Iterator

import psycopg
import pytest
from psycopg.types.json import Jsonb

from okg.substrate.images.values import (
    ImageRefInvalid,
    ImageValueForged,
    file_value,
    url_value,
)

pytestmark = pytest.mark.postgres

_SHA = "ab" * 32
FORGED_PNG = {
    "okg_image": "file",
    "path": "figs/f1.pdf",
    "png": {
        "id": _SHA, "sha256": _SHA, "mime_type": "image/png",
        "width": 10, "height": 10, "byte_count": 100,
    },
}
FORGED_ERROR = {"okg_image": "file", "path": "figs/f1.pdf", "error": "image_file_missing"}
UNKNOWN_KIND = {"okg_image": "blob", "path": "figs/f1.pdf"}
BAD_PATH = {"okg_image": "file", "path": "../etc/passwd"}
HTTP_URL = {"okg_image": "url", "url": "http://example.com/a.png"}

#: (image value, expected code, expected error class), one case per id
REFUSED = [
    pytest.param(FORGED_PNG, "image_value_forged", ImageValueForged, id="png"),
    pytest.param(FORGED_ERROR, "image_value_forged", ImageValueForged, id="error"),
    pytest.param(UNKNOWN_KIND, "image_ref_invalid", ImageRefInvalid, id="unknown-kind"),
    pytest.param(BAD_PATH, "image_ref_invalid", ImageRefInvalid, id="bad-path"),
    pytest.param(HTTP_URL, "image_ref_invalid", ImageRefInvalid, id="http-url"),
]
VALID_FILE = file_value("figs/f1.pdf")
VALID_URL = url_value("https://example.com/a.png")


# ── shared helpers ─────────────────────────────────────────────────────

_TABLES = (
    "okg.node_facts",
    "okg.edge_facts",
    "okg.admission_envelopes",
    "okg.admission_record_refs",
    "okg.fact_batch_receipts",
    "okg.attribute_observations",
)


def _state(dsn: str) -> dict[str, int]:
    """Row counts of every table a refused write could touch, plus the
    default branch's graph nodes and edges."""
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        counts = {
            table: int(conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            for table in _TABLES
        }
        graph = ChronosGraphStore.from_connection(conn).checkout("default")
        for name in ("okg.graph_nodes", "okg.graph_edges"):
            rows = graph.query(f"SELECT count(*) AS n FROM {name}")
            counts[name] = int(rows[0]["n"])
    return counts


def _node_fact_attrs(dsn: str, node_id: str) -> list[dict[str, Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT attrs FROM okg.node_facts WHERE node_id = %s ORDER BY fact_id",
            (node_id,),
        ).fetchall()
    return [row[0] for row in rows]


def _graph_node_attrs(dsn: str, node_id: str) -> dict[str, Any] | None:
    """The node's graph attrs, without the source-lineage attr the graph
    store stamps on every write."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.nomos.source_lineage import LINEAGE_ATTR

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(conn).checkout("default").query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": node_id},
        )
    if not rows:
        return None
    return {k: v for k, v in rows[0]["attrs"].items() if k != LINEAGE_ATTR}


# ── SDK: emit_node_fact, emit_edge_fact, update_node_attrs ─────────────

@pytest.fixture
def sdk_conn(clean_runtime: str) -> Iterator[psycopg.Connection]:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        yield conn


@pytest.mark.parametrize(("image", "code", "error"), REFUSED)
def test_sdk_emit_node_fact_refuses(
    clean_runtime: str, sdk_conn: psycopg.Connection, image, code, error,
) -> None:
    from okg.deployment import emit_node_fact
    from okg.deployment._internal.context import bind_test_context

    before = _state(clean_runtime)
    with bind_test_context(deployment="test", conn=sdk_conn):
        with pytest.raises(error) as caught:
            emit_node_fact("doc:1", "document", {"figure": image}, dedupe_key="dk-1")
    assert caught.value.code == code
    assert _state(clean_runtime) == before


@pytest.mark.parametrize(("image", "code", "error"), REFUSED)
def test_sdk_emit_node_fact_refuses_a_list_element(
    clean_runtime: str, sdk_conn: psycopg.Connection, image, code, error,
) -> None:
    from okg.deployment import emit_node_fact
    from okg.deployment._internal.context import bind_test_context

    before = _state(clean_runtime)
    with bind_test_context(deployment="test", conn=sdk_conn):
        with pytest.raises(error) as caught:
            emit_node_fact(
                "doc:1", "document", {"figures": [VALID_FILE, image]},
                dedupe_key="dk-1",
            )
    assert caught.value.code == code
    assert caught.value.data["index"] == 1
    assert _state(clean_runtime) == before


@pytest.mark.parametrize(("image", "code", "error"), REFUSED)
def test_sdk_emit_edge_fact_refuses(
    clean_runtime: str, sdk_conn: psycopg.Connection, image, code, error,
) -> None:
    from okg.deployment import emit_edge_fact
    from okg.deployment._internal.context import bind_test_context

    before = _state(clean_runtime)
    with bind_test_context(deployment="test", conn=sdk_conn):
        with pytest.raises(error) as caught:
            emit_edge_fact("doc:1", "doc:2", "cites", {"figure": image}, dedupe_key="dk-e")
    assert caught.value.code == code
    assert _state(clean_runtime) == before


def test_sdk_emit_node_fact_stores_valid_values_unchanged(
    clean_runtime: str, sdk_conn: psycopg.Connection,
) -> None:
    from okg.deployment import emit_node_fact
    from okg.deployment._internal.context import bind_test_context

    attrs = {"figure": VALID_FILE, "logos": [VALID_URL, VALID_FILE]}
    with bind_test_context(deployment="test", conn=sdk_conn):
        emit_node_fact("doc:valid", "document", attrs, dedupe_key="dk-valid")
    assert _node_fact_attrs(clean_runtime, "doc:valid") == [attrs]
    assert _graph_node_attrs(clean_runtime, "doc:valid") == attrs


@pytest.mark.parametrize(("image", "code", "error"), REFUSED)
def test_sdk_update_node_attrs_refuses_new_attrs_and_keeps_ingest_values(
    clean_runtime: str, sdk_conn: psycopg.Connection, image, code, error,
) -> None:
    """The current node holds a value ingest completed. An update that plants
    a forged value is refused; an ordinary update carries the completed
    value forward unchanged."""
    from okg.deployment import update_node_attrs
    from okg.deployment._internal import mutations
    from okg.deployment._internal.context import bind_test_context

    # Stand in for ingest: write a completed value through the unchecked
    # internal writer, the way the runner's own output is not re-checked.
    completed = dict(FORGED_PNG)
    with bind_test_context(deployment="test", conn=sdk_conn):
        mutations._emit_node_fact(
            "doc:u", "document", {"figure": completed, "title": "t"},
            dedupe_key="dk-seed", op="I",
        )
    before = _state(clean_runtime)
    with bind_test_context(deployment="test", conn=sdk_conn):
        with pytest.raises(error) as caught:
            update_node_attrs("doc:u", {"cover": image})
    assert caught.value.code == code
    assert _state(clean_runtime) == before

    with bind_test_context(deployment="test", conn=sdk_conn):
        update_node_attrs("doc:u", {"title": "t2", "cover": VALID_URL})
    assert _graph_node_attrs(clean_runtime, "doc:u") == {
        "figure": completed, "title": "t2", "cover": VALID_URL,
    }


# ── enrichers: _fact_emitters.emit_node_fact / emit_edge_fact ──────────

def _enricher_node(conn: psycopg.Connection, attrs: dict[str, Any], node_id: str):
    from okg.substrate.library.enrichers._fact_emitters import emit_node_fact

    run_id = str(uuid.uuid4())
    return emit_node_fact(
        conn,
        enricher_name="image-forgery-test",
        node_id=node_id,
        subtype="markdown_heading",
        attrs=attrs,
        dedupe_key=f"image-forgery-test:{node_id}",
        source_record_id={"id": node_id},
        source_revision={"run_id": run_id},
        run_id=run_id,
        observed_at=datetime.now(timezone.utc),
        derived_from=["input:1"],
        support_policy="all_members",
    )


@pytest.mark.parametrize(("image", "code", "error"), REFUSED)
def test_enricher_emitters_refuse(clean_runtime: str, image, code, error) -> None:
    from okg.substrate.library.enrichers._fact_emitters import emit_edge_fact

    before = _state(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(error) as node_caught:
            _enricher_node(conn, {"figure": image}, "enr:1")
        run_id = str(uuid.uuid4())
        with pytest.raises(error) as edge_caught:
            emit_edge_fact(
                conn,
                enricher_name="image-forgery-test",
                src="enr:1", dst="enr:2", edge_type="references",
                attrs={"figure": image},
                provenance="derived_deterministic", confidence=1.0,
                dedupe_key="image-forgery-test:edge",
                source_record_id={"id": "e"},
                source_revision={"run_id": run_id},
                run_id=run_id,
                observed_at=datetime.now(timezone.utc),
            )
    assert node_caught.value.code == code
    assert edge_caught.value.code == code
    assert _state(clean_runtime) == before


def test_enricher_emitter_stores_valid_values_unchanged(clean_runtime: str) -> None:
    attrs = {"figure": VALID_FILE, "logo": VALID_URL}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        outcome = _enricher_node(conn, attrs, "enr:valid")
    assert outcome.inserted is True
    assert _node_fact_attrs(clean_runtime, "enr:valid") == [attrs]


# ── review admission: submit, MCP record/propose, apply ────────────────

_FAST_TRACK_POLICY = {
    "producer_id": "agent:fast",
    "producer_kind": "agent",
    "trust_label": "fast-track-trusted",
    "admission_mode": "fast_track",
    "allowed_record_kinds": ["node"],
    "output_signature": {"nodes": [{"subtype": "alpha", "clause_id": "allow_alpha"}]},
    "policy_clauses": [{
        "id": "allow_alpha", "effect": "allow",
        "record_kind": "node", "subtype": "alpha",
    }],
}


def _submit(dsn: str, monkeypatch: pytest.MonkeyPatch, payload: dict[str, Any], key: str):
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(admission_workflows, "_resolve_dsn", lambda _d: dsn)
    return admission_workflows._run_submit(
        deployment="review-test",
        payload=payload,
        producer_id="agent:fast",
        producer_kind="agent",
        intent="record_observation",
        idempotency_key=key,
        authority={"producer_policy": dict(_FAST_TRACK_POLICY)},
        check_summary={"record": {"kind": "observation"}},
    )


def _payload_shapes(image: Any) -> list[dict[str, Any]]:
    """The node and edge shapes a review payload can take."""
    node = {"node_id": "fast:img", "subtype": "alpha", "attrs": {"figure": image}}
    edge = {"src": "fast:a", "dst": "fast:b", "edge_type": "links_to",
            "attrs": {"figure": image}}
    return [
        {"nodes": [node]},
        {"node": node},
        {"kind": "node", **node},
        {"edges": [edge]},
        {"edge": edge},
        {"kind": "edge", **edge},
    ]


@pytest.mark.parametrize(("image", "code", "error"), REFUSED)
def test_admission_submit_refuses_every_payload_shape(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, image, code, error,
) -> None:
    before = _state(clean_runtime)
    for index, payload in enumerate(_payload_shapes(image)):
        with pytest.raises(error) as caught:
            _submit(clean_runtime, monkeypatch, payload, f"forged-{index}")
        assert caught.value.code == code, payload
    assert _state(clean_runtime) == before


def test_admission_submit_refuses_a_file_reference_in_every_payload_shape(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An agent record write may not carry a ``file`` reference: file images
    come only from a connector's ingest run (decision b)."""
    before = _state(clean_runtime)
    for image in (VALID_FILE, [VALID_URL, VALID_FILE]):
        for index, payload in enumerate(_payload_shapes(image)):
            with pytest.raises(ImageRefInvalid) as caught:
                _submit(clean_runtime, monkeypatch, payload, f"file-{index}")
            assert caught.value.code == "image_ref_invalid", payload
            assert caught.value.data["problem"] == "file_ref_outside_ingest"
            assert "connector's ingest run" in caught.value.message
    assert _state(clean_runtime) == before


def test_admission_submit_stores_valid_values_unchanged(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    attrs = {"text": "alpha", "figure": VALID_URL, "logos": [VALID_URL]}
    result = _submit(
        clean_runtime, monkeypatch,
        {"nodes": [{"node_id": "fast:valid", "subtype": "alpha", "attrs": attrs}]},
        "valid-images",
    )
    assert result["status"] == "accepted", result
    assert _node_fact_attrs(clean_runtime, "fast:valid") == [attrs]
    assert _graph_node_attrs(clean_runtime, "fast:valid") == attrs


@pytest.mark.parametrize(("image", "code", "error"), REFUSED)
def test_admission_apply_rejects_a_stored_forged_payload(
    clean_runtime: str, image, code, error,
) -> None:
    """A payload ref stored by another writer (the Poros bridge attaches its
    own) is rejected at apply with the image code, before any fact insert."""
    from okg.substrate.admission import attach_payload_ref, ensure_envelope
    from okg.substrate.runtime import admission_workflows

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        envelope = ensure_envelope(
            conn,
            deployment_name="review-test",
            producer_id="agent:fast",
            producer_kind="agent",
            intent="record_observation",
            admission_mode="review",
            status="pending",
            idempotency_key=f"apply-{code}-{uuid.uuid4().hex}",
            catalog_version_id=None,
            authority={},
            check_summary={},
        )
        attach_payload_ref(
            conn,
            admission_id=envelope.admission_id,
            payload={"nodes": [{
                "node_id": "fast:stored", "subtype": "alpha",
                "attrs": {"figure": image},
            }]},
        )
        before = _state(clean_runtime)
        current = admission_workflows._load_any_envelope(
            conn, deployment="review-test", admission_id=envelope.admission_id,
        )
        with pytest.raises(admission_workflows._ReviewPayloadRejected) as caught:
            admission_workflows._emit_review_payload_facts(
                conn, envelope=current, workflow_ref={},
            )
    assert caught.value.reason == code
    assert caught.value.verdict["code"] == code
    assert caught.value.verdict["kind"] == "node"
    assert _state(clean_runtime) == before


def _build_writing_server(dsn: str, monkeypatch: pytest.MonkeyPatch):
    """An MCP server whose submit workflow only records that it was reached."""
    from okg.substrate.mcp.server import build_server
    from tests.substrate.test_least_privilege_mcp import _seed_published

    _seed_published(dsn)
    reached: list[dict[str, Any]] = []

    def _workflow(payload, *_args):
        reached.append(payload)
        return {"status": "pending", "admission_id": "adm:test"}

    monkeypatch.setattr(
        "okg.substrate.runtime.admission_workflows.admission_submit_workflow",
        lambda: _workflow,
    )
    return build_server(dsn=dsn, deployment_name="lp", allow_writes=True), reached


def _close(mcp) -> None:
    mcp._okg_pool.close()
    if mcp._okg_write_pool:
        mcp._okg_write_pool.close()


@pytest.mark.parametrize(("image", "code", "error"), REFUSED)
def test_mcp_propose_and_record_refuse_with_the_code(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, image, code, error,
) -> None:
    mcp, reached = _build_writing_server(clean_runtime, monkeypatch)
    try:
        before = _state(clean_runtime)
        propose = mcp._tool_manager._tools["propose"].fn
        result = propose(
            action="submit",
            payload={
                "intent": "review_proposal",
                "nodes": [{"node_id": "x:1", "subtype": "document",
                           "attrs": {"figure": image}}],
            },
        )
        assert result["status"] == "refused", result
        assert result["error"]["code"] == code
        assert result["operator_name"] == "propose"

        record = mcp._tool_manager._tools["record"].fn
        result = record(
            kind="decision",
            payload={"summary": "Use figure 1.", "rationale": image},
        )
        assert result["status"] == "refused", result
        assert result["error"]["code"] == code
        assert result["operator_name"] == "record"
        assert reached == []
        assert _state(clean_runtime) == before
    finally:
        _close(mcp)


def test_mcp_propose_and_record_pass_valid_values_unchanged(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    mcp, reached = _build_writing_server(clean_runtime, monkeypatch)
    try:
        propose = mcp._tool_manager._tools["propose"].fn
        result = propose(
            action="submit",
            payload={
                "intent": "review_proposal",
                "nodes": [{"node_id": "x:1", "subtype": "document",
                           "attrs": {"figure": VALID_URL}}],
            },
        )
        assert result["status"] == "pending", result
        record = mcp._tool_manager._tools["record"].fn
        result = record(
            kind="decision",
            payload={"summary": "Use figure 1.", "rationale": VALID_URL},
        )
        assert result["status"] == "pending", result
    finally:
        _close(mcp)
    assert reached[0]["nodes"][0]["attrs"]["figure"] == VALID_URL
    record_nodes = reached[1]["nodes"]
    assert [n["attrs"]["rationale"] for n in record_nodes if "rationale" in n["attrs"]] == [
        VALID_URL,
    ]


# ── MCP connector ingest ───────────────────────────────────────────────

def _connector_batch(conn, subject: Any):
    from okg.substrate.mcp.connector_ingest import connector_ingest_batch

    return connector_ingest_batch(
        conn,
        "default",
        "gmail_api",
        "gmail_api:host_connector",
        [
            {"external_id": "message-ok", "labels": ["INBOX"], "subject": "fine"},
            {"external_id": "message-1", "labels": ["INBOX"], "subject": subject},
        ],
        provenance={"allowlist": {"labels": ["INBOX"]}},
    )


@pytest.mark.parametrize(("image", "code", "error"), REFUSED)
def test_connector_ingest_refuses_the_batch(
    clean_runtime: str, image, code, error,
) -> None:
    before = _state(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        result = _connector_batch(conn, image)
    assert result["status"] == "validation_failed", result
    assert result["errors"] == [
        {"index": 1, "code": code, "error": result["errors"][0]["error"]},
    ]
    assert _state(clean_runtime) == before


def test_connector_ingest_stores_valid_values_unchanged(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _SkippedPublisher:
        def cycle(self) -> dict[str, str]:
            return {"status": "skipped_by_test"}

    monkeypatch.setattr(
        "okg.substrate.publisher.factory.build_deployment_publisher",
        lambda **_kwargs: _SkippedPublisher(),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        result = _connector_batch(conn, VALID_FILE)
    assert result["status"] == "ingest_complete", result
    attrs = _node_fact_attrs(clean_runtime, "email:gmail:message-1")
    assert [a["subject"] for a in attrs] == [VALID_FILE]


# ── MCP fan_out and review_as audit nodes ──────────────────────────────

@pytest.fixture
def captured_graph_facts(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Capture the Chronos apply these primitives make; the fact log rows
    are still written for real."""
    from okg.substrate import graph_store

    applied: list[Any] = []

    def _apply_facts(_self, facts, **_kwargs):
        applied.extend(facts)

    monkeypatch.setattr(graph_store.ChronosGraphStore, "apply_facts", _apply_facts)
    return applied


def _fan_out(conn, scope: Any) -> None:
    from okg.substrate.mcp.fan_out import FanOutWorker, _emit_audit_nodes

    _emit_audit_nodes(
        conn=conn,
        session=SimpleNamespace(branch_name="default"),
        dispatch_id="dispatch:img",
        brief_id="brief:img",
        per_worker=[
            FanOutWorker("w0", "tool", {"q": "fine"}, {}),
            FanOutWorker("w1", "tool", scope, {}),
        ],
        sub_agent_session_ids=["sub:w0", "sub:w1"],
        observed_at=datetime.now(timezone.utc),
        dispatched_by="agent:test",
        default_tool="tool",
    )


@pytest.mark.parametrize(("image", "code", "error"), REFUSED)
def test_fan_out_refuses_before_the_first_audit_node(
    clean_runtime: str, captured_graph_facts: list[Any], image, code, error,
) -> None:
    before = _state(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(error) as caught:
            _fan_out(conn, image)
    assert caught.value.code == code
    assert captured_graph_facts == []
    assert _state(clean_runtime) == before


def test_fan_out_stores_valid_values_unchanged(
    clean_runtime: str, captured_graph_facts: list[Any],
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _fan_out(conn, VALID_FILE)
    [attrs] = _node_fact_attrs(clean_runtime, "sub:w1")
    assert attrs["scope"] == VALID_FILE
    [graph_node] = [
        fact for fact in captured_graph_facts
        if getattr(fact, "node_id", None) == "sub:w1"
    ]
    assert graph_node.attrs["scope"] == VALID_FILE


def _critique(conn, attrs: dict[str, Any]) -> None:
    from okg.substrate.mcp.review_as import _emit_critique

    _emit_critique(
        conn=conn,
        critique_id="critique:img",
        target_node_id="doc:1",
        attrs=attrs,
        observed_at=datetime.now(timezone.utc),
        run_id=uuid.uuid4(),
    )


@pytest.mark.parametrize(("image", "code", "error"), REFUSED)
def test_review_as_refuses_the_critique(
    clean_runtime: str, captured_graph_facts: list[Any], image, code, error,
) -> None:
    before = _state(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(error) as caught:
            _critique(conn, {"text": "see figure", "figure": image})
    assert caught.value.code == code
    assert captured_graph_facts == []
    assert _state(clean_runtime) == before


def test_review_as_stores_valid_values_unchanged(
    clean_runtime: str, captured_graph_facts: list[Any],
) -> None:
    attrs = {"text": "see figure", "figure": VALID_URL}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _critique(conn, attrs)
    assert _node_fact_attrs(clean_runtime, "critique:img") == [attrs]


# ── attribute overlays ─────────────────────────────────────────────────

def _observe(conn, value: Any, attr_key: str = "figure"):
    from okg.substrate.attributes import record_attribute_observation

    return record_attribute_observation(
        conn,
        target_kind="node",
        target_id="doc:1",
        generation_id=1,
        attr_key=attr_key,
        value=value,
        producer="image-forgery-test",
    )


@pytest.mark.parametrize(("image", "code", "error"), REFUSED)
def test_attribute_observation_refuses(clean_runtime: str, image, code, error) -> None:
    before = _state(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(error) as caught:
            _observe(conn, image)
        with pytest.raises(error):
            _observe(conn, [VALID_FILE, image], attr_key="figures")
    assert caught.value.code == code
    assert caught.value.data["attr"] == "figure"
    assert _state(clean_runtime) == before


def test_attribute_observation_stores_valid_values_unchanged(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        stored = _observe(conn, VALID_FILE)
        listed = _observe(conn, [VALID_URL, VALID_FILE], attr_key="figures")
        row = conn.execute(
            "SELECT value FROM okg.attribute_observations WHERE observation_id = %s",
            (stored.observation_id,),
        ).fetchone()
    assert stored.value == VALID_FILE
    assert row[0] == VALID_FILE
    assert listed.value == [VALID_URL, VALID_FILE]


# ── lens promotion ─────────────────────────────────────────────────────

def _committed_lens_overlay(conn: psycopg.Connection, new_value: Any):
    from okg.substrate.lenses import (
        LensPutbackDelta,
        commit_lens_overlay,
        prepare_lens_putback,
        register_lens_definition,
    )
    from tests.substrate.test_lens_promotion import (
        _activate_lens_profile,
        _generation_id,
        _insert_generation,
        _person_attr_mapping,
        _publish_editable_view,
        _set_promotion_mapping,
    )

    generation_id = _generation_id()
    _insert_generation(conn, generation_id)
    _view, lens, published = _publish_editable_view(
        conn, suffix=uuid.uuid4().hex, generation_id=generation_id,
    )
    register_lens_definition(conn, lens)
    prepared = prepare_lens_putback(
        conn,
        lens,
        base_view_manifest_id=published.manifest_id,
        target_generation_id=generation_id,
        actor_id="operator:test",
        idempotency_key=f"image-forgery-{uuid.uuid4().hex}",
        deltas=[LensPutbackDelta(
            slot_name="editable_people",
            scope_key="person",
            row_key="person:p-1",
            field_path=("display_name",),
            expected_value="Old name",
            new_value=new_value,
            source_authority="test.people",
            source_record_id={"node_id": "person:p-1"},
            expected_source_revision={"revision": "rev-1"},
        )],
    )
    committed = commit_lens_overlay(conn, request_id=prepared.request_id)
    _set_promotion_mapping(conn, lens.definition_id, _person_attr_mapping())
    _activate_lens_profile(conn, lens=lens)
    return lens, committed


def _lens_node_facts(conn: psycopg.Connection, lens) -> list[dict[str, Any]]:
    from okg.substrate.lenses import lens_producer_id

    rows = conn.execute(
        "SELECT attrs FROM okg.node_facts WHERE source = %s",
        (lens_producer_id(lens.definition_id),),
    ).fetchall()
    return [row[0] for row in rows]


@pytest.mark.parametrize(("image", "code", "error"), REFUSED)
def test_lens_promotion_refuses_at_plan_time(
    clean_runtime: str, image, code, error,
) -> None:
    from okg.substrate.lenses import promote_lens_overlay

    with psycopg.connect(clean_runtime) as conn:
        lens, committed = _committed_lens_overlay(conn, image)
        before = _state(clean_runtime)
        with pytest.raises(error) as caught:
            promote_lens_overlay(
                conn, request_id=committed.request_id, actor_id="operator:test",
            )
        conn.rollback()
        assert _lens_node_facts(conn, lens) == []
    assert caught.value.code == code
    assert _state(clean_runtime) == before


@pytest.mark.parametrize(("image", "code", "error"), REFUSED)
def test_lens_promotion_rejects_a_planned_forged_value_at_apply(
    clean_runtime: str, image, code, error,
) -> None:
    """An envelope planned before the plan-time check existed is rejected at
    apply with the image code, and no node fact is written."""
    from okg.substrate.lenses import apply_promoted_overlay, promote_lens_overlay

    with psycopg.connect(clean_runtime) as conn:
        lens, committed = _committed_lens_overlay(conn, "New name")
        promotion = promote_lens_overlay(
            conn, request_id=committed.request_id, actor_id="operator:test",
        )
        conn.execute(
            """
            UPDATE okg.admission_envelopes
               SET verdict = jsonb_set(
                     verdict, '{planned_mutations,0,attrs}', %s::jsonb)
             WHERE admission_id = %s::uuid
            """,
            (Jsonb({"display_name": image}), promotion.admission_id),
        )
        conn.commit()
        applied = apply_promoted_overlay(conn, admission_id=promotion.admission_id)
        envelope = conn.execute(
            "SELECT status, verdict->'rejection' FROM okg.admission_envelopes "
            "WHERE admission_id = %s::uuid",
            (promotion.admission_id,),
        ).fetchone()
        facts = _lens_node_facts(conn, lens)
    assert applied.status == "rejected"
    assert applied.nodes_added == 0
    assert envelope[0] == "rejected"
    assert envelope[1]["code"] == code
    assert facts == []


def test_lens_promotion_stores_valid_values_unchanged(clean_runtime: str) -> None:
    from okg.substrate.lenses import apply_promoted_overlay, promote_lens_overlay

    with psycopg.connect(clean_runtime) as conn:
        lens, committed = _committed_lens_overlay(conn, VALID_FILE)
        promotion = promote_lens_overlay(
            conn, request_id=committed.request_id, actor_id="operator:test",
        )
        applied = apply_promoted_overlay(conn, admission_id=promotion.admission_id)
        facts = _lens_node_facts(conn, lens)
    assert applied.status == "published"
    assert facts == [{"display_name": VALID_FILE}]


# ── agent record writes refuse file references (decision b) ───────────

def test_mcp_propose_and_record_refuse_a_file_reference(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MCP ``record`` and ``propose`` are agent record writes: a ``file``
    reference is a structured refusal with ``image_ref_invalid``; nothing
    reaches the submit worker or the database."""
    mcp, reached = _build_writing_server(clean_runtime, monkeypatch)
    try:
        before = _state(clean_runtime)
        propose = mcp._tool_manager._tools["propose"].fn
        result = propose(
            action="submit",
            payload={
                "intent": "review_proposal",
                "nodes": [{"node_id": "x:1", "subtype": "document",
                           "attrs": {"panels": [VALID_URL, VALID_FILE]}}],
            },
        )
        assert result["status"] == "refused", result
        assert result["error"]["code"] == "image_ref_invalid"
        assert result["error"]["data"]["problem"] == "file_ref_outside_ingest"
        assert "connector's ingest run" in result["error"]["message"]

        record = mcp._tool_manager._tools["record"].fn
        result = record(
            kind="decision",
            payload={"summary": "Use figure 1.", "rationale": VALID_FILE},
        )
        assert result["status"] == "refused", result
        assert result["error"]["code"] == "image_ref_invalid"
        assert result["error"]["data"]["problem"] == "file_ref_outside_ingest"
        assert reached == []
        assert _state(clean_runtime) == before
    finally:
        _close(mcp)


def _cli_submit(dsn: str, monkeypatch: pytest.MonkeyPatch, payload: dict[str, Any], key: str):
    """``okg admission submit`` on a runtime-disabled deployment: the inline
    path, which runs the same submit body as the DBOS step."""
    import json

    from okg.substrate.cli import admission as admission_cli
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(admission_workflows, "_resolve_dsn", lambda _d: dsn)
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(runtime=SimpleNamespace(enabled=False)),
    )
    return admission_cli._cmd_admission_submit(
        SimpleNamespace(
            deployment="review-test",
            payload_json=json.dumps(payload),
            payload_file=None,
            authority_json=json.dumps({"producer_policy": dict(_FAST_TRACK_POLICY)}),
            check_summary_json=json.dumps({"record": {"kind": "observation"}}),
            producer_id="agent:fast",
            producer_kind="agent",
            intent="record_observation",
            idempotency_key=key,
            max_wait_seconds=1,
            poll_interval_seconds=0.1,
        ),
        dsn=dsn,
        as_json=True,
    )


def test_cli_admission_submit_refuses_a_file_reference_and_accepts_a_url(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import json

    before = _state(clean_runtime)
    rc = _cli_submit(
        clean_runtime, monkeypatch,
        {"nodes": [{"node_id": "fast:cli", "subtype": "alpha",
                    "attrs": {"figure": VALID_FILE}}]},
        "cli-file",
    )
    error = json.loads(capsys.readouterr().err)["error"]
    assert rc == 1
    assert error["code"] == "image_ref_invalid"
    assert error["data"]["problem"] == "file_ref_outside_ingest"
    assert _state(clean_runtime) == before

    rc = _cli_submit(
        clean_runtime, monkeypatch,
        {"nodes": [{"node_id": "fast:cli", "subtype": "alpha",
                    "attrs": {"figure": VALID_URL}}]},
        "cli-url",
    )
    capsys.readouterr()
    assert rc == 0
    assert _node_fact_attrs(clean_runtime, "fast:cli") == [{"figure": VALID_URL}]


# ── Poros export reduces image values to references (decision c) ───────

def _seed_context_packet_catalog(conn: psycopg.Connection) -> None:
    """The receiver's catalog: one ``context_packet`` subtype."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version_id = int(cur.fetchone()[0])
        cur.execute(
            "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) "
            "VALUES ('context_packet', '{}'::jsonb, %s)",
            (catalog_version_id,),
        )
        cur.execute(
            """
            INSERT INTO okg.subtype_ancestors
              (catalog_version_id, subtype_name, ancestor_name, depth)
            VALUES (%s, 'context_packet', 'context_packet', 0)
            ON CONFLICT DO NOTHING
            """,
            (catalog_version_id,),
        )


def test_poros_export_sends_bare_references_and_the_receiver_accepts_them(
    clean_runtime: str,
) -> None:
    """A real export of a node whose image values ingest completed, received
    by a peer through its inbox and admission: the envelope carries no blob
    id and no ingest error, the receiver accepts the node, and the stored
    node carries the bare references (a ``file`` one reads as not
    ingested). Before the fix the export sent ``png`` and the receiver
    refused the whole admission with ``image_value_forged``.

    One database plays both okgs in turn: the sender publishes and exports,
    then the database is replaced by a fresh clone for the receiver."""
    import json

    from okg.substrate.exchange import (
        PorosInbox,
        TrustPin,
        TrustPinRegistry,
        submit_poros_candidate_to_admission,
    )
    from okg.substrate.exchange.canonical_json import canonical_json_bytes
    from okg.substrate.exchange.outbox import export_published_subgraph_to_outbox
    from okg.substrate.exchange.signing import generate_ed25519_keypair
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.images.values import check_incoming, classify
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
    from tests.substrate._db_helpers import seed_catalog_version
    from tests.substrate.conftest import reset_okg_test_runtime

    node_id = "poros:img:1"
    completed = FORGED_PNG  # what ingest leaves on the sender
    failed = FORGED_ERROR
    with psycopg.connect(clean_runtime) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(
                node_id, "context_packet",
                {"text": "a figure", "figure": completed,
                 "panels": [failed, VALID_URL]},
                {"id": node_id}, {"v": 1},
            )],
            source="default", strict_endpoints=False,
        )
    publish = Publisher(
        clean_runtime, "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert publish["status"] == "published", publish

    sender = generate_ed25519_keypair("img-sender")
    export = export_published_subgraph_to_outbox(
        dsn=clean_runtime,
        sender_authority_id="okg:lab-a",
        recipient_authority_id="okg:mit",
        sender_generation_id=publish["generation_id"],
        graph_id="img:graph:1",
        root_node_ids=[node_id],
        signing_private_key=sender.private_key,
        signing_key_id=sender.kid,
        message_id="img-export-1",
        classification="public",
        graph_profile="context",
    )
    wire = json.dumps(export.envelope, default=str)
    assert _SHA not in wire
    assert "image_file_missing" not in wire
    exported = export.envelope["body"]["nodes"][0]["attrs"]
    assert exported["figure"] == {"okg_image": "file", "path": "figs/f1.pdf"}
    assert exported["panels"] == [{"okg_image": "file", "path": "figs/f1.pdf"}, VALID_URL]

    # The receiver: a fresh database with its own catalog.
    reset_okg_test_runtime(clean_runtime)
    inbox = PorosInbox(
        receiver_authority_id="okg:mit",
        trust_registry=TrustPinRegistry.from_pins(
            [TrustPin(authority_id="okg:lab-a", kid=sender.kid,
                      public_key=sender.public_key)]
        ),
        receiver_private_key=b"\x62" * 32,
        receiver_signing_kid="mit-sign-1",
    )
    received = inbox.handle_bytes(canonical_json_bytes(export.envelope))
    assert received.status == "accepted_for_admission", received
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_context_packet_catalog(conn)
        with conn.transaction():
            admitted = submit_poros_candidate_to_admission(
                conn, deployment_name="default",
                candidate=received.admission_candidate,
            )
        verdict = conn.execute(
            "SELECT verdict FROM okg.admission_envelopes WHERE admission_id = %s::uuid",
            (admitted.admission_id,),
        ).fetchone()[0]
    assert admitted.status == "accepted", verdict
    stored = _node_fact_attrs(clean_runtime, node_id)
    assert len(stored) == 1
    assert stored[0]["figure"] == {"okg_image": "file", "path": "figs/f1.pdf"}
    assert stored[0]["panels"][1] == VALID_URL
    check_incoming(stored[0])
    assert classify(stored[0]["figure"]).state == "not_ingested"

    publish = Publisher(
        clean_runtime, "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert publish["status"] == "published", publish
    graph_attrs = _graph_node_attrs(clean_runtime, node_id)
    assert graph_attrs is not None
    assert graph_attrs["figure"] == {"okg_image": "file", "path": "figs/f1.pdf"}
    assert _SHA not in json.dumps(graph_attrs)


# ── a forged value in a later runner chunk (finding 8) ─────────────────

def test_a_forged_value_in_a_later_chunk_is_not_written(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The runner checks each chunk's raw facts before it completes or writes
    any of them. With a chunk size of 2, the forged fact sits in chunk 2:
    the run fails, and neither it nor its chunk-mate is written, so no
    ``png`` reaches the fact log or the graph.

    Chunk 1 was already flushed when chunk 2 is refused. What happens to it
    is the runner's normal mid-run failure behaviour (it is not changed
    here), so this test asserts nothing about chunk 1 beyond its own facts
    carrying no image value."""
    import json

    import okg.deployment as sdk
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.ingest import runner as runner_module
    from okg.substrate.library.sources.base import NodeFact
    from tests.pytest.catalog_fixture_builders import seed_catalog

    source = "test.images.chunks"

    def node(node_id: str, attrs: dict[str, Any]) -> NodeFact:
        return NodeFact(node_id=node_id, subtype="concept", attrs=attrs,
                        source_record_id={"id": node_id}, source_revision={"v": 1})

    facts = [
        node("n:chunk1-a", {"text": "a"}),
        node("n:chunk1-b", {"text": "b"}),
        node("n:chunk2-good", {"figure": VALID_URL}),
        node("n:chunk2-forged", {"figure": FORGED_PNG}),
    ]

    class _Connector:
        name = "chunked-image-connector"
        profile = "discovery_crawl"

        def run(self, ctx: Any) -> Any:
            del ctx
            return sdk.ConnectorRun(
                facts=list(facts), completed_scope=True, run_mode="scope_complete",
            )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["concept"])
    monkeypatch.setattr(runner_module, "COPY_CHUNK_ROWS", 2)
    flushed: list[list[str]] = []
    real_flush = SourceRunner._flush_chunk

    def counting_flush(self, facts_in, *args, **kwargs):
        flushed.append([getattr(f, "node_id", "?") for f in facts_in])
        return real_flush(self, facts_in, *args, **kwargs)

    monkeypatch.setattr(SourceRunner, "_flush_chunk", counting_flush)

    with pytest.raises(ImageValueForged) as raised:
        SourceRunner(clean_runtime, source_name=source).run(
            sdk.ConnectorAdapter(_Connector()), mode="scope_complete",
        )
    assert raised.value.code == "image_value_forged"
    assert raised.value.data["fact_identity"] == "n:chunk2-forged"
    # Two chunks reached the flush: the forged fact was in the second.
    assert flushed[:2] == [
        ["n:chunk1-a", "n:chunk1-b"], ["n:chunk2-good", "n:chunk2-forged"],
    ]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fact_rows = conn.execute(
            "SELECT node_id, attrs FROM okg.node_facts WHERE source = %s",
            (source,),
        ).fetchall()
        graph = ChronosGraphStore.from_connection(conn).checkout("default")
        graph_rows = graph.query(
            "SELECT node_id, attrs FROM okg.graph_nodes WHERE source = :source",
            {"source": source},
        )
    written = {row[0] for row in fact_rows} | {row["node_id"] for row in graph_rows}
    assert "n:chunk2-forged" not in written
    assert "n:chunk2-good" not in written
    assert _SHA not in json.dumps([row[1] for row in fact_rows])
    assert _SHA not in json.dumps([row["attrs"] for row in graph_rows])
