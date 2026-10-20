"""inspect(kind="attr") under Nomos (issue #3189).

Before this change, `inspect(kind="attr")` read `okg.attribute_observations`
with plain SQL: no visibility check and no masking. A reader who could not
see a node still got its whole attribute history, and a reader whose node
read was redacted got the raw values the node read masked.

These rows drive the real MCP boundary (`FastMCP.call_tool`, JSON result
format) and pin these things:

* a hidden node is refused exactly as `inspect(kind="node")` refuses it, and
  an absent node gets the same refusal (absence and denial look the same);
* a hidden edge is refused the same way, including an edge the filter
  passes whose endpoint node is hidden; a visible edge still answers;
* a visible node or edge whose read is redacted returns masked observation
  values;
* with Nomos off nothing changes: raw history, empty history for an absent id.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")
pytest.importorskip("mcp")

from okg.substrate.attributes import (  # noqa: E402
    attribute_value_hash,
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.mcp import open_session  # noqa: E402
from okg.substrate.mcp.result_text import result_format_scope  # noqa: E402
from okg.substrate.mcp.server import build_server  # noqa: E402
from okg.substrate.mcp.tools import _visible_edge_rows  # noqa: E402
from okg.substrate.nomos import (  # noqa: E402
    activate_policy_bundle,
    compile_policy_graph,
)
from okg.substrate.nomos.read_masking import MASK_TOKEN, REDACTION_MARKER  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    bind_test_catalog_writer,
    seed_catalog,
)
from tests.substrate.mcp.test_nomos_source_lineage_mcp import (  # noqa: E402
    _activate_source_policy,
    _publish_lineage_graph,
)
from tests.substrate.mcp.test_operator_attr_nomos_parity import (  # noqa: E402
    _redact_on_restricted_policy,
    _seed_with_classification_overlay,
)


pytestmark = pytest.mark.postgres

_HIDDEN_NODE = "node:google-drive"
_VISIBLE_NODE = "node:public-docs"
_ABSENT_NODE = "node:never-published-3189"
_SECRET = "hidden-history-3189"


def _call(mcp: Any, arguments: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    """One `inspect` call through FastMCP.call_tool, read as JSON."""
    import anyio

    async def drive():
        with result_format_scope("json"):
            return await mcp.call_tool("inspect", arguments)

    result = anyio.run(drive)
    return bool(getattr(result, "isError", False)), json.loads(result.content[0].text)


def _close(mcp: Any) -> None:
    mcp._okg_pool.close()
    if mcp._okg_write_pool:
        mcp._okg_write_pool.close()


def _publish_lineage_with_history(dsn: str) -> dict[str, str]:
    """The source-lineage graph, plus one observation on each target."""
    gen = _publish_lineage_graph(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        edges = {
            str(row["dst"]): str(row["edge_id"])
            for row in _visible_edge_rows(conn, session)
        }
        for kind, target in (
            ("node", _HIDDEN_NODE),
            ("node", _VISIBLE_NODE),
            ("edge", edges[_HIDDEN_NODE]),
            ("edge", edges["node:public-more"]),
        ):
            record_attribute_observation(
                conn,
                target_kind=kind,
                target_id=target,
                generation_id=gen,
                attr_key="review_note",
                value=f"{_SECRET}:{target}",
                producer="reviewer",
            )
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    return {
        "generation_id": gen,
        "hidden_edge": edges[_HIDDEN_NODE],
        "visible_edge": edges["node:public-more"],
    }


def _enforce_source_policy(dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    _activate_source_policy(dsn)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")


def test_hidden_node_history_is_refused_like_inspect_node(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _publish_lineage_with_history(clean_runtime)
    gen = fixture["generation_id"]
    _enforce_source_policy(clean_runtime, monkeypatch)
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        node_err, node_body = _call(
            mcp,
            {"target": {"kind": "node", "node_id": _HIDDEN_NODE}, "generation_id": gen},
        )
        refusals = {}
        for kind in ("attr", "attribute", "attribute_provenance"):
            refusals[kind] = _call(
                mcp,
                {
                    "target": {"kind": kind, "node_id": _HIDDEN_NODE},
                    "generation_id": gen,
                },
            )
        absent_err, absent_body = _call(
            mcp,
            {"target": {"kind": "attr", "node_id": _ABSENT_NODE}, "generation_id": gen},
        )
        visible_err, visible_body = _call(
            mcp,
            {"target": {"kind": "attr", "node_id": _VISIBLE_NODE}, "generation_id": gen},
        )
    finally:
        _close(mcp)

    assert node_err is True, node_body
    node_text = json.dumps(node_body, default=str)
    assert "node_not_found" in node_text and '"exists": false' in node_text, node_body
    for kind, (is_error, body) in refusals.items():
        assert is_error is True, (kind, body)
        # The identical refusal inspect(kind="node") gives for this id:
        # code, message, data and hint, byte for byte.
        assert body == node_body, (kind, body)
        assert _SECRET not in json.dumps(body, default=str), (kind, body)
    # An absent id reads the same as a hidden one, apart from the id itself.
    assert absent_err is True, absent_body
    assert json.dumps(absent_body, sort_keys=True, default=str).replace(
        _ABSENT_NODE, _HIDDEN_NODE
    ) == json.dumps(node_body, sort_keys=True, default=str), absent_body
    # A node the reader can see still answers with its history.
    assert visible_err is False, visible_body
    assert [obs["value"] for obs in visible_body["observations"]] == [
        f"{_SECRET}:{_VISIBLE_NODE}"
    ]


def test_hidden_edge_history_is_refused(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _publish_lineage_with_history(clean_runtime)
    gen = fixture["generation_id"]
    _enforce_source_policy(clean_runtime, monkeypatch)
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        hidden_err, hidden_body = _call(
            mcp,
            {
                "target": {
                    "kind": "attr",
                    "target_kind": "edge",
                    "edge_id": fixture["hidden_edge"],
                },
                "generation_id": gen,
            },
        )
        absent_err, absent_body = _call(
            mcp,
            {
                "target": {"kind": "attr", "target_kind": "edge", "edge_id": "987654321"},
                "generation_id": gen,
            },
        )
        visible_err, visible_body = _call(
            mcp,
            {
                "target": {
                    "kind": "attr",
                    "target_kind": "edge",
                    "edge_id": fixture["visible_edge"],
                },
                "generation_id": gen,
            },
        )
    finally:
        _close(mcp)

    assert hidden_err is True, hidden_body
    assert _SECRET not in json.dumps(hidden_body, default=str), hidden_body
    assert "node_not_found" in json.dumps(hidden_body), hidden_body
    assert absent_err is True, absent_body
    assert json.dumps(absent_body, sort_keys=True, default=str).replace(
        "987654321", fixture["hidden_edge"]
    ) == json.dumps(hidden_body, sort_keys=True, default=str), absent_body
    assert visible_err is False, visible_body
    assert [obs["value"] for obs in visible_body["observations"]] == [
        f"{_SECRET}:{fixture['visible_edge']}"
    ]


def test_masked_attribute_history_comes_back_masked(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    fixture = _seed_with_classification_overlay(clean_runtime)
    gen, nid = fixture["generation_id"], fixture["node_id"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # A free-text overlay: the node read drops `text` entirely.
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id=nid,
            generation_id=gen,
            attr_key="text",
            value="secret-overlay-body",
            producer="reviewer",
        )
        # A replacement over the fixture's alice@example.com: the store
        # writes decision.diff with the raw old and new values and both
        # hashes. Free-form metadata quotes the value too.
        replacement = record_attribute_observation(
            conn,
            target_kind="node",
            target_id=nid,
            generation_id=gen,
            attr_key="owner_email",
            value="bob@example.com",
            producer="reviewer",
            provenance={"quote": "owner is bob@example.com", "nested": {"v": "bob@example.com"}},
            dependencies={"evidence": "bob@example.com"},
            dependency_fingerprint="fp:bob@example.com",
            decision={"note": "was alice@example.com"},
        )
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    secrets = [
        "alice@example.com",
        "bob@example.com",
        "secret-overlay-body",
        attribute_value_hash("alice@example.com"),
        attribute_value_hash("bob@example.com"),
        attribute_value_hash("secret-overlay-body"),
    ]
    mcp = build_server(dsn=clean_runtime)
    try:
        node_err, node_body = _call(
            mcp, {"target": {"kind": "node", "node_id": nid}, "generation_id": gen},
        )
        attr_err, attr_body = _call(
            mcp, {"target": {"kind": "attr", "node_id": nid}, "generation_id": gen},
        )
        email_err, email_body = _call(
            mcp,
            {
                "target": {"kind": "attr", "node_id": nid, "attr_key": "owner_email"},
                "generation_id": gen,
            },
        )
    finally:
        _close(mcp)

    assert node_err is False, node_body
    # The node read fences untrusted strings, so the token sits inside a fence.
    assert MASK_TOKEN in node_body["attrs"]["owner_email"]
    assert "alice@example.com" not in json.dumps(node_body, default=str)
    assert attr_err is False, attr_body
    by_key: dict[str, list[dict[str, Any]]] = {}
    for obs in attr_body["observations"]:
        by_key.setdefault(obs["attr_key"], []).append(obs)
    assert set(by_key) == {"classification", "owner_email", "text"}, by_key
    # The policy input is a governance key: never masked, as on the node.
    assert [obs["value"] for obs in by_key["classification"]] == ["restricted"]
    # The direct identifier reads as the node read shows it, in BOTH the
    # replaced and the replacing observation.
    assert len(by_key["owner_email"]) == 2, by_key["owner_email"]
    for obs in by_key["owner_email"]:
        assert obs["value"] == MASK_TOKEN, obs
        assert obs[REDACTION_MARKER] is True, obs
        assert "value_hash" not in obs, obs
        assert obs["provenance"] == {} and obs["dependencies"] == {}, obs
        assert obs["dependency_fingerprint"] is None, obs
        assert set(obs["decision"]) <= {"superseded_by"}, obs
    newest = next(
        obs for obs in by_key["owner_email"]
        if obs["observation_id"] == replacement.observation_id
    )
    assert newest["diff"] == {"old": MASK_TOKEN, "new": MASK_TOKEN}, newest
    # Free text the node read drops is dropped from the history too.
    assert ["value" in obs for obs in by_key["text"]] == [False]
    assert by_key["text"][0][REDACTION_MARKER] is True
    assert attr_body[REDACTION_MARKER] is True
    # No raw value or value hash anywhere, at any depth, in either answer.
    assert email_err is False, email_body
    for body in (attr_body, email_body):
        served = json.dumps(body, default=str)
        leaked = [secret for secret in secrets if secret in served]
        assert leaked == [], (leaked, served)


def test_visible_attribute_metadata_on_redacted_record_is_trimmed(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row for a key the reader MAY see still loses free-form metadata.

    `classification` is a governance key, so its value is readable on a
    redacted record. Its observation's provenance, dependencies, fingerprint
    and decision note here quote the withheld email; none of it may come back.
    """
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    fixture = _seed_with_classification_overlay(clean_runtime)
    gen, nid = fixture["generation_id"], fixture["node_id"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        quoting = record_attribute_observation(
            conn,
            target_kind="node",
            target_id=nid,
            generation_id=gen,
            attr_key="classification",
            value="restricted",
            producer="reviewer",
            provenance={"quote": "owner alice@example.com asked", "deep": {"v": "alice@example.com"}},
            dependencies={"evidence": "alice@example.com"},
            dependency_fingerprint="fp:alice@example.com",
            decision={"note": "flagged by alice@example.com"},
        )
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    mcp = build_server(dsn=clean_runtime)
    try:
        is_error, body = _call(
            mcp,
            {
                "target": {"kind": "attr", "node_id": nid, "attr_key": "classification"},
                "generation_id": gen,
            },
        )
    finally:
        _close(mcp)

    assert is_error is False, body
    served = json.dumps(body, default=str)
    assert "alice@example.com" not in served, served
    assert attribute_value_hash("alice@example.com") not in served, served
    assert body[REDACTION_MARKER] is True
    rows = body["observations"]
    assert quoting.observation_id in {obs["observation_id"] for obs in rows}, rows
    for obs in rows:
        # The visible value itself stays readable and unmarked.
        assert obs["value"] == "restricted", obs
        assert REDACTION_MARKER not in obs, obs
        assert obs["provenance"] == {} and obs["dependencies"] == {}, obs
        assert obs["dependency_fingerprint"] is None, obs
        assert set(obs["decision"]) <= {"superseded_by"}, obs


def test_nomos_off_history_is_unchanged(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _publish_lineage_with_history(clean_runtime)
    gen = fixture["generation_id"]
    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        node_err, node_body = _call(
            mcp,
            {"target": {"kind": "attr", "node_id": _HIDDEN_NODE}, "generation_id": gen},
        )
        edge_err, edge_body = _call(
            mcp,
            {
                "target": {
                    "kind": "attr",
                    "target_kind": "edge",
                    "edge_id": fixture["hidden_edge"],
                },
                "generation_id": gen,
            },
        )
        absent_err, absent_body = _call(
            mcp,
            {"target": {"kind": "attr", "node_id": _ABSENT_NODE}, "generation_id": gen},
        )
    finally:
        _close(mcp)

    assert node_err is False, node_body
    assert [obs["value"] for obs in node_body["observations"]] == [
        f"{_SECRET}:{_HIDDEN_NODE}"
    ]
    assert "value_hash" in node_body["observations"][0]
    assert REDACTION_MARKER not in node_body
    assert edge_err is False, edge_body
    assert [obs["value"] for obs in edge_body["observations"]] == [
        f"{_SECRET}:{fixture['hidden_edge']}"
    ]
    # Nomos off: an absent id still answers with an empty history.
    assert absent_err is False, absent_body
    assert absent_body["observations"] == []
    assert absent_body["total_count"] == 0


def test_edge_hidden_only_by_its_endpoint_is_refused(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An edge the filter passes is still refused when an endpoint is hidden.

    The edge comes from the readable `public_docs` source; only its target
    node comes from the denied `google_drive` source. `expand` would not show
    it, so its history must not be readable either.
    """
    _publish_lineage_graph(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                EdgeFact(
                    "node:public-more",
                    _HIDDEN_NODE,
                    "references",
                    {"classification": "internal"},
                    source_record_id={"id": "public-edge-to-hidden"},
                    source_revision={"v": 1},
                ),
            ],
            source="public_docs",
        )
    published = Publisher(
        clean_runtime,
        "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert published["status"] == "published", published
    gen = str(published["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        edge_id = next(
            str(row["edge_id"])
            for row in _visible_edge_rows(conn, session)
            if str(row["src"]) == "node:public-more"
            and str(row["dst"]) == _HIDDEN_NODE
        )
        record_attribute_observation(
            conn,
            target_kind="edge",
            target_id=edge_id,
            generation_id=gen,
            attr_key="review_note",
            value=f"{_SECRET}:{edge_id}",
            producer="reviewer",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    _enforce_source_policy(clean_runtime, monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(
            conn, deployment_name="default", nomos_subject="subject:mcp-client",
        )
        # Precondition: the edge row itself passes the read filter, so only
        # the endpoint rule can refuse it.
        assert [
            str(row["edge_id"])
            for row in _visible_edge_rows(conn, session)
            if str(row["edge_id"]) == edge_id
        ] == [edge_id]
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        is_error, body = _call(
            mcp,
            {
                "target": {"kind": "attr", "target_kind": "edge", "edge_id": edge_id},
                "generation_id": gen,
            },
        )
    finally:
        _close(mcp)

    assert is_error is True, body
    served = json.dumps(body, default=str)
    assert "node_not_found" in served and '"exists": false' in served, body
    assert _SECRET not in served, body


def test_redacted_edge_history_is_masked_like_its_attrs(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An edge whose read is redacted returns its history masked.

    Edges carry no subtype, so the read filter masks them with the legacy
    classifier: `text` is free text and is dropped, a plain key stays.
    """
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["document"],
            narrowings=[("document", "references", "document")],
        )
        activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=compile_policy_graph(_redact_on_restricted_policy()),
            scope="deployment:default",
            activated_by="test:operator",
            bootstrap=True,
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "doc:edge-src-3189",
                    "document",
                    {"classification": "internal"},
                    {"id": "doc:edge-src-3189"},
                    {"v": 1},
                ),
                NodeFact(
                    "doc:edge-dst-3189",
                    "document",
                    {"classification": "internal"},
                    {"id": "doc:edge-dst-3189"},
                    {"v": 1},
                ),
                EdgeFact(
                    "doc:edge-src-3189",
                    "doc:edge-dst-3189",
                    "references",
                    {"classification": "restricted", "text": "edge-base-secret"},
                    source_record_id={"id": "edge-3189"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    published = Publisher(
        clean_runtime,
        "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert published["status"] == "published", published
    gen = str(published["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        edge_id = next(
            str(row["edge_id"])
            for row in _visible_edge_rows(conn, session)
            if str(row["src"]) == "doc:edge-src-3189"
        )
        record_attribute_observation(
            conn,
            target_kind="edge",
            target_id=edge_id,
            generation_id=gen,
            attr_key="text",
            value="edge-overlay-secret",
            producer="reviewer",
        )
        record_attribute_observation(
            conn,
            target_kind="edge",
            target_id=edge_id,
            generation_id=gen,
            attr_key="review_state",
            value="checked",
            producer="reviewer",
            provenance={"quote": "edge-overlay-secret"},
        )
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        is_error, body = _call(
            mcp,
            {
                "target": {"kind": "attr", "target_kind": "edge", "edge_id": edge_id},
                "generation_id": gen,
            },
        )
    finally:
        _close(mcp)

    assert is_error is False, body
    served = json.dumps(body, default=str)
    for secret in (
        "edge-overlay-secret",
        attribute_value_hash("edge-overlay-secret"),
    ):
        assert secret not in served, (secret, served)
    assert body[REDACTION_MARKER] is True, body
    by_key = {obs["attr_key"]: obs for obs in body["observations"]}
    assert set(by_key) == {"text", "review_state"}, by_key
    # Free text is dropped, with its hash and metadata.
    assert "value" not in by_key["text"], by_key["text"]
    assert "value_hash" not in by_key["text"], by_key["text"]
    assert by_key["text"][REDACTION_MARKER] is True
    # A plain key stays readable, but loses its free-form metadata.
    assert by_key["review_state"]["value"] == "checked"
    assert by_key["review_state"]["provenance"] == {}
