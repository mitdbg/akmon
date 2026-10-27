from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.attributes import (  # noqa: E402
    attribute_history_for_target,
    ensure_attribute_schema,
    mark_attribute_stale,
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.external_backed.handle import HANDLE_ATTR  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.mcp import (  # noqa: E402
    expand_neighbors,
    inspect_attr_provenance,
    inspect_node,
    open_session,
    search,
)
from okg.substrate.mcp.server import build_server  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _runner(result):
    def run(conn, spec, node_id):
        return result

    return run


def _assert_fenced_value(value, expected: str) -> None:
    assert expected in value
    assert "UNTRUSTED_DATA" in value


def _materialize(
    conn,
    *,
    generation_id: str | int,
    previous_generation_id: str | int | None = None,
) -> None:
    result = materialize_effective_attributes_for_generation(
        conn,
        generation_id=generation_id,
        previous_generation_id=previous_generation_id,
    )
    assert result["materialized"] is True or (
        result["reason"] == "already_materialized"
    ), result


def _raw_attrs(
    conn,
    *,
    generation_id: str,
    node_id: str,
    edge_id: int | str,
) -> tuple[dict, dict]:
    graph = ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
    ).checkout_generation(generation_id)
    node = graph.query(
        "SELECT attrs FROM okg.graph_nodes WHERE node_id = :node_id",
        {"node_id": node_id},
    )[0]["attrs"]
    edge = graph.query(
        "SELECT attrs FROM okg.graph_edges WHERE edge_id = :edge_id",
        {"edge_id": int(edge_id)},
    )[0]["attrs"]
    return node, edge


def _fixture(clean_runtime: str, *, external: bool = False) -> dict[str, str | int]:
    suffix = uuid.uuid4().hex[:8]
    src = f"node:src:{suffix}"
    dst = f"node:dst:{suffix}"
    src_attrs = {
        "text": "raw alpha text",
        "raw_status": "source",
        "raw_only": True,
    }
    if external:
        src_attrs[HANDLE_ATTR] = {
            "status": {
                "current": "probe",
                "adapter": {"kind": "python"},
                "ttl_seconds": 60,
                "cache_policy": "never-cache",
            }
        }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["case", "person"],
            narrowings=[("case", "reported_by", "person")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(src, "case", src_attrs, {"id": src}, {"v": 1}),
                NodeFact(dst, "person", {"text": "reporter"}, {"id": dst}, {"v": 1}),
                EdgeFact(
                    src,
                    dst,
                    "reported_by",
                    attrs={"edge_raw": "source"},
                    source_record_id={"id": f"edge:{suffix}"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published", published
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        edge_id = expand_neighbors(conn, session, src)["neighbors"][0]["edge_id"]
    return {
        "src": src,
        "dst": dst,
        "edge_id": edge_id,
        "generation_id": str(published["generation_id"]),
    }


def test_empty_attribute_materialization_skips_snapshot_queries(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ensure_attribute_schema(conn)
        result = materialize_effective_attributes_for_generation(
            conn,
            generation_id="gen-empty",
            previous_generation_id="gen-prev",
        )
        assert result == {
            "generation_id": "gen-empty",
            "previous_generation_id": "gen-prev",
            "namespace": "default",
            "materialized": True,
            "reason": "empty_attribute_state",
            "carried_forward": 0,
            "overlaid": 0,
        }
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.attribute_effective_attrs
                 WHERE generation_id = 'gen-empty'
                """,
            )
            assert cur.fetchone()[0] == 0


def test_attribute_history_preserves_past_observations(clean_runtime: str) -> None:
    fixture = _fixture(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        first = record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="triage",
            value="low",
            producer="model:v1",
            provenance={"source": "fixture"},
            dependencies={"prompt": "p1"},
            dependency_fingerprint="dep:triage:1",
        )
        second = record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="triage",
            value="high",
            producer="reviewer",
            provenance={"review_id": "r1"},
            dependencies={"prompt": "p2"},
            dependency_fingerprint="dep:triage:2",
        )
        edge_obs = record_attribute_observation(
            conn,
            target_kind="edge",
            target_id=fixture["edge_id"],
            generation_id=fixture["generation_id"],
            attr_key="confidence_label",
            value="reviewed",
            producer="reviewer",
        )
        _materialize(conn, generation_id=fixture["generation_id"])

        history = attribute_history_for_target(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="triage",
        )
        session = open_session(conn, deployment_name="default")
        node = inspect_node(conn, session, str(fixture["src"]))
        edge = expand_neighbors(conn, session, str(fixture["src"]))["neighbors"][0]

    assert [row["observation_id"] for row in history] == [
        first.observation_id,
        second.observation_id,
    ]
    assert [row["status"] for row in history] == ["superseded", "accepted"]
    assert history[1]["diff"]["old"] == "low"
    assert history[1]["diff"]["new"] == "high"
    _assert_fenced_value(node["attrs"]["triage"], "high")
    _assert_fenced_value(edge["attrs"]["confidence_label"], "reviewed")
    assert edge_obs.status == "accepted"


def test_overlay_write_keeps_raw_attrs_unchanged(clean_runtime: str) -> None:
    fixture = _fixture(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        raw_node_before, raw_edge_before = _raw_attrs(
            conn,
            generation_id=str(fixture["generation_id"]),
            node_id=str(fixture["src"]),
            edge_id=fixture["edge_id"],
        )
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="derived_status",
            value="accepted",
            producer="derived:test",
        )
        record_attribute_observation(
            conn,
            target_kind="edge",
            target_id=fixture["edge_id"],
            generation_id=fixture["generation_id"],
            attr_key="edge_label",
            value="accepted",
            producer="derived:test",
        )
        _materialize(conn, generation_id=fixture["generation_id"])
        raw_node_after, raw_edge_after = _raw_attrs(
            conn,
            generation_id=str(fixture["generation_id"]),
            node_id=str(fixture["src"]),
            edge_id=fixture["edge_id"],
        )
        session = open_session(conn, deployment_name="default")
        node = inspect_node(conn, session, str(fixture["src"]))
        edge = expand_neighbors(conn, session, str(fixture["src"]))["neighbors"][0]

    assert raw_node_after == raw_node_before
    assert raw_edge_after == raw_edge_before
    assert "derived_status" not in raw_node_after
    assert "edge_label" not in raw_edge_after
    _assert_fenced_value(node["attrs"]["derived_status"], "accepted")
    _assert_fenced_value(edge["attrs"]["edge_label"], "accepted")


def test_default_reads_return_flat_effective_attrs(clean_runtime: str) -> None:
    fixture = _fixture(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="text",
            value="derived searchable text",
            producer="derived:test",
        )
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="derived_flag",
            value=True,
            producer="derived:test",
        )
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="rejected_flag",
            value=True,
            status="rejected",
            producer="derived:test",
            decision={"reason": "bad input"},
        )
        stale = record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="stale_flag",
            value=True,
            producer="derived:test",
        )
        mark_attribute_stale(conn, stale.observation_id, reason="dependency changed")
        record_attribute_observation(
            conn,
            target_kind="edge",
            target_id=fixture["edge_id"],
            generation_id=fixture["generation_id"],
            attr_key="edge_effective",
            value="visible",
            producer="derived:test",
        )
        _materialize(conn, generation_id=fixture["generation_id"])
        session = open_session(conn, deployment_name="default")
        node = inspect_node(conn, session, str(fixture["src"]))
        found = search(
            conn,
            session,
            "derived",
            method="lexical",
            filter={"attr_filters": {"derived_flag": True}},
            select=["node_id", "attrs", "excerpt"],
        )
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        tools = mcp._tool_manager._tools  # type: ignore[attr-defined]
        filtered = tools["filter"].fn(
            input={"kind": "nodes"},
            predicate={
                "op": "eq",
                "field": "attrs.derived_flag",
                "value": True,
            },
            select=["node_id", "attrs.derived_flag"],
        )
        mapped = tools["map"].fn(
            input={"kind": "nodes"},
            projection=["node_id", "attrs.derived_flag"],
        )
        aggregated = tools["aggregate"].fn(
            input={"kind": "nodes"},
            by=["attrs.derived_flag"],
            metrics=["count"],
        )
    finally:
        mcp._okg_pool.close()

    _assert_fenced_value(node["attrs"]["text"], "derived searchable text")
    assert node["attrs"]["derived_flag"] is True
    assert "rejected_flag" not in node["attrs"]
    assert "stale_flag" not in node["attrs"]
    assert len(found["results"]) == 1
    assert found["results"][0]["attrs"]["derived_flag"] is True
    assert "derived searchable text" in found["results"][0]["excerpt"]
    # Issue #2306: a projection that reads attrs (here `attrs.derived_flag`)
    # carries the row's trust label, so the label is part of the row.
    assert filtered["items"] == [
        {
            "node_id": fixture["src"],
            "attrs_derived_flag": True,
            "provenance": {"source": "default", "trust": "untrusted"},
        }
    ]
    assert {
        (row["node_id"], row["attrs_derived_flag"])
        for row in mapped["items"]
    } >= {(fixture["src"], True)}
    assert {
        group["attrs.derived_flag"]: group["count"]
        for group in aggregated["groups"]
    }[True] == 1


def test_edge_effective_attrs_are_fenced_on_expand(clean_runtime: str) -> None:
    fixture = _fixture(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_attribute_observation(
            conn,
            target_kind="edge",
            target_id=fixture["edge_id"],
            generation_id=fixture["generation_id"],
            attr_key="edge_effective",
            value="visible",
            producer="derived:test",
        )
        _materialize(conn, generation_id=fixture["generation_id"])
        session = open_session(conn, deployment_name="default")
        edge = expand_neighbors(conn, session, str(fixture["src"]))["neighbors"][0]

    _assert_fenced_value(edge["attrs"]["edge_effective"], "visible")
    assert edge["read_provenance"]["trust"] == "untrusted"


def test_opt_in_attr_provenance_explains_effective_value(clean_runtime: str) -> None:
    fixture = _fixture(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="risk",
            value="medium",
            producer="model:v1",
            provenance={"source": "fixture"},
            dependencies={"prompt_hash": "p1"},
            dependency_fingerprint="dep:risk:1",
            confidence=0.5,
        )
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="risk",
            value="high",
            producer="reviewer",
            provenance={"review_id": "r2"},
            dependencies={"prompt_hash": "p2"},
            dependency_fingerprint="dep:risk:2",
            confidence=0.9,
        )
        _materialize(conn, generation_id=fixture["generation_id"])
        session = open_session(conn, deployment_name="default")
        node = inspect_node(conn, session, str(fixture["src"]))
        provenance = inspect_attr_provenance(
            conn,
            session,
            target_kind="node",
            target_id=fixture["src"],
            attr_key="risk",
        )

    assert "attr_provenance" not in node
    _assert_fenced_value(node["attrs"]["risk"], "high")
    assert provenance["total_count"] == 2
    accepted = provenance["observations"][1]
    assert accepted["producer"] == "reviewer"
    assert accepted["confidence"] == 0.9
    assert accepted["dependency_fingerprint"] == "dep:risk:2"
    assert accepted["diff"] == {
        "old": "medium",
        "new": "high",
        "old_value_hash": provenance["observations"][0]["value_hash"],
        "new_value_hash": accepted["value_hash"],
    }


def test_node_and_edge_overlays_share_contract(clean_runtime: str) -> None:
    fixture = _fixture(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        node_obs = record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="review_state",
            value="accepted",
            producer="reviewer",
        )
        edge_obs = record_attribute_observation(
            conn,
            target_kind="edge",
            target_id=fixture["edge_id"],
            generation_id=fixture["generation_id"],
            attr_key="review_state",
            value="accepted",
            producer="reviewer",
        )
        _materialize(conn, generation_id=fixture["generation_id"])
        session = open_session(conn, deployment_name="default")
        node = inspect_node(conn, session, str(fixture["src"]))
        edge = expand_neighbors(conn, session, str(fixture["src"]))["neighbors"][0]
        node_history = inspect_attr_provenance(
            conn,
            session,
            target_kind="node",
            target_id=fixture["src"],
            attr_key="review_state",
        )
        edge_history = inspect_attr_provenance(
            conn,
            session,
            target_kind="edge",
            target_id=fixture["edge_id"],
            attr_key="review_state",
        )

    _assert_fenced_value(node["attrs"]["review_state"], "accepted")
    _assert_fenced_value(edge["attrs"]["review_state"], "accepted")
    assert node_history["observations"][0]["observation_id"] == node_obs.observation_id
    assert edge_history["observations"][0]["observation_id"] == edge_obs.observation_id
    assert node_history["observations"][0]["status"] == "accepted"
    assert edge_history["observations"][0]["status"] == "accepted"


def test_external_live_never_becomes_effective_attrs_without_publish(
    clean_runtime: str,
) -> None:
    fixture = _fixture(clean_runtime, external=True)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="pinned_status",
            value="from-published-overlay",
            producer="derived:test",
        )
        _materialize(conn, generation_id=fixture["generation_id"])
        session = open_session(conn, deployment_name="default")
        live = inspect_node(
            conn,
            session,
            str(fixture["src"]),
            hydrate=["status"],
            principal="subject:probe-caller",
            probe_gate=lambda field, principal: True,
            probe_runners={"python": _runner(("ok", {"up": True}, None))},
        )
        plain = inspect_node(conn, session, str(fixture["src"]))
        external_filter = search(
            conn,
            session,
            "raw",
            method="lexical",
            filter={"attr_filters": {"status": "live"}},
        )
        pinned_filter = search(
            conn,
            session,
            "raw",
            method="lexical",
            filter={"attr_filters": {"pinned_status": "from-published-overlay"}},
        )

    assert live["external_live"]["fields"]["status"]["value"] == {"up": True}
    assert "status" not in live["attrs"]
    assert "status" not in plain["attrs"]
    _assert_fenced_value(
        plain["attrs"]["pinned_status"],
        "from-published-overlay",
    )
    assert external_filter["results"] == []
    assert len(pinned_filter["results"]) == 1


def test_materialized_attrs_carry_forward_to_next_generation(
    clean_runtime: str,
) -> None:
    fixture = _fixture(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="review_state",
            value="accepted",
            producer="reviewer",
        )
        _materialize(conn, generation_id=fixture["generation_id"])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    str(fixture["src"]),
                    "case",
                    {
                        "text": "raw alpha text updated",
                        "raw_status": "source",
                        "raw_only": True,
                    },
                    {"id": str(fixture["src"])},
                    {"v": 2},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    next_published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert next_published["status"] == "published", next_published
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _materialize(
            conn,
            generation_id=next_published["generation_id"],
            previous_generation_id=fixture["generation_id"],
        )
        session = open_session(conn, deployment_name="default")
        node = inspect_node(conn, session, str(fixture["src"]))
        provenance = inspect_attr_provenance(
            conn,
            session,
            target_kind="node",
            target_id=fixture["src"],
            attr_key="review_state",
        )

    _assert_fenced_value(node["attrs"]["review_state"], "accepted")
    assert provenance["observations"][0]["producer"] == "reviewer"


def test_publish_carries_forward_accepted_observation_without_old_rematerialize(
    clean_runtime: str,
) -> None:
    fixture = _fixture(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="post_publish_review",
            value="accepted-after-publish",
            producer="reviewer",
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    str(fixture["src"]),
                    "case",
                    {
                        "text": "raw alpha text next",
                        "raw_status": "source",
                        "raw_only": True,
                    },
                    {"id": str(fixture["src"])},
                    {"v": 3},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    next_published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert next_published["status"] == "published", next_published
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        node = inspect_node(conn, session, str(fixture["src"]))
        provenance = inspect_attr_provenance(
            conn,
            session,
            target_kind="node",
            target_id=fixture["src"],
            attr_key="post_publish_review",
        )

    _assert_fenced_value(
        node["attrs"]["post_publish_review"],
        "accepted-after-publish",
    )
    assert provenance["observations"][0]["producer"] == "reviewer"


def test_supersede_insert_failure_rolls_back_prior_current_state(
    clean_runtime: str,
) -> None:
    fixture = _fixture(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        first = record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="risk",
            value="medium",
            producer="model:v1",
        )
        with pytest.raises(psycopg.errors.UniqueViolation):
            record_attribute_observation(
                conn,
                target_kind="node",
                target_id=fixture["src"],
                generation_id=fixture["generation_id"],
                attr_key="risk",
                value="high",
                producer="model:v2",
                observation_id=first.observation_id,
            )
        history = attribute_history_for_target(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="risk",
        )

    assert [row["status"] for row in history] == ["accepted"]
    assert history[0]["superseded_by"] is None


def test_inspect_attr_provenance_is_agent_facing(clean_runtime: str) -> None:
    fixture = _fixture(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        obs = record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="risk",
            value="medium",
            producer="model:v1",
            provenance={"source": "fixture"},
        )
        _materialize(conn, generation_id=fixture["generation_id"])

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        tool = mcp._tool_manager._tools["inspect"]  # type: ignore[attr-defined]
        out = tool.fn(
            target={
                "kind": "attr",
                "target_kind": "node",
                "target_id": fixture["src"],
                "attr_key": "risk",
            },
        )
    finally:
        mcp._okg_pool.close()

    assert out["total_count"] == 1
    assert out["observations"][0]["observation_id"] == obs.observation_id
    assert out["observations"][0]["producer"] == "model:v1"


def test_dependency_change_removes_stale_effective_attr_on_next_generation(
    clean_runtime: str,
) -> None:
    fixture = _fixture(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        stale = record_attribute_observation(
            conn,
            target_kind="node",
            target_id=fixture["src"],
            generation_id=fixture["generation_id"],
            attr_key="taxonomy",
            value="old",
            producer="classifier:v1",
            dependencies={"taxonomy_hash": "hash:old"},
            dependency_fingerprint="hash:old",
        )
        _materialize(conn, generation_id=fixture["generation_id"])
        session = open_session(conn, deployment_name="default")
        before = inspect_node(conn, session, str(fixture["src"]))
        mark_attribute_stale(
            conn,
            stale.observation_id,
            reason="dependency fingerprint changed",
            decision={"new_dependency_fingerprint": "hash:new"},
        )
        pinned_after_stale = inspect_node(conn, session, str(fixture["src"]))
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    str(fixture["src"]),
                    "case",
                    {
                        "text": "raw alpha text",
                        "raw_status": "source",
                        "raw_only": True,
                        "publish_marker": "next",
                    },
                    {"id": str(fixture["src"])},
                    {"v": 2},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    next_published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert next_published["status"] == "published", next_published
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _materialize(
            conn,
            generation_id=next_published["generation_id"],
            previous_generation_id=fixture["generation_id"],
        )
        next_session = open_session(conn, deployment_name="default")
        after_stale = inspect_node(conn, next_session, str(fixture["src"]))
        history = inspect_attr_provenance(
            conn,
            session,
            target_kind="node",
            target_id=fixture["src"],
            attr_key="taxonomy",
        )

    _assert_fenced_value(before["attrs"]["taxonomy"], "old")
    _assert_fenced_value(pinned_after_stale["attrs"]["taxonomy"], "old")
    assert "taxonomy" not in after_stale["attrs"]
    assert [row["status"] for row in history["observations"]] == ["stale"]
