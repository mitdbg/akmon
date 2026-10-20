from __future__ import annotations

import json
from dataclasses import replace

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import inspect_subtype, open_session
from okg.substrate.mcp.continuation import ContinuationTokenInvalid
from okg.substrate.mcp.response_bounds import public_call_tool_result_bytes
from okg.substrate.mcp.subtype_summary import (
    SubtypeSummaryItemTooLarge,
    compact_subtype_summary,
)
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import SubtypeSpec, seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _publish_large_subtype(dsn: str) -> str:
    filterable = {f"key_{index:02d}": "text" for index in range(14)}
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[
                SubtypeSpec(
                    "large_doc",
                    metadata={"filterable_attrs": filterable},
                ),
                "related_doc",
            ],
            narrowings=[
                ("large_doc", "relates", "related_doc"),
                ("related_doc", "backlinks", "large_doc"),
            ],
        )
        attrs = {
            **{name: f"value-{name}" for name in filterable},
            "nullable": None,
            "wide": "x" * 20_000,
        }
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact("large:1", "large_doc", attrs, {"id": "large:1"}, {"v": 1}),
                NodeFact(
                    "large:2",
                    "large_doc",
                    {**attrs, "nullable": "present"},
                    {"id": "large:2"},
                    {"v": 1},
                ),
                NodeFact("related:1", "related_doc", {}, {"id": "related:1"}, {"v": 1}),
                EdgeFact(
                    "large:1",
                    "related:1",
                    "relates",
                    source_record_id={"id": "edge:1"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    "related:1",
                    "large:2",
                    "backlinks",
                    source_record_id={"id": "edge:2"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    published = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert published["status"] == "published", published
    return str(published["generation_id"])


def test_summary_cursor_reconstructs_large_schema_without_gaps_or_duplicates(
    clean_runtime: str,
) -> None:
    generation = _publish_large_subtype(clean_runtime)
    names: list[str] = []
    attributes: dict[str, dict] = {}
    filter_names: list[str] = []
    routes_out: list[tuple[str, str]] = []
    routes_in: list[tuple[str, str]] = []
    token = None
    pages = 0
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        while True:
            page = inspect_subtype(
                conn,
                session,
                "large_doc",
                continuation_token=token,
                page_size=6,
                max_bytes=4_096,
            )
            pages += 1
            assert public_call_tool_result_bytes(page) <= 4_096
            assert page["pinned_generation_id"] == generation
            assert page["sample_nodes"] is None
            assert "x" * 500 not in json.dumps(page)
            names.extend(item["name"] for item in page["attr_keys"])
            attributes.update({item["name"]: item for item in page["attr_keys"]})
            filter_names.extend(item["attr"] for item in page["recommended_filters"])
            routes_out.extend(
                (item["edge_type"], item["dst_subtype"]) for item in page["edges_out"]
            )
            routes_in.extend(
                (item["edge_type"], item["src_subtype"]) for item in page["edges_in"]
            )
            token = page.get("continuation_token")
            if token is None:
                break
            assert page["truncated"] is True
            assert pages < 30

    assert names == sorted(set(names))
    assert {f"key_{index:02d}" for index in range(14)}.issubset(names)
    assert {"nullable", "wide"}.issubset(names)
    assert attributes["key_00"]["value_types"] == ["string"]
    assert attributes["key_00"]["presence_frequency"] == 1.0
    assert attributes["key_00"]["null_frequency"] == 0.0
    assert attributes["key_00"]["frequency_denominator"] == 2
    assert len(attributes["key_00"]["sample_values"]) == 2
    assert attributes["nullable"]["value_types"] == ["null", "string"]
    assert attributes["nullable"]["null_frequency"] == 0.5
    assert filter_names == sorted(set(filter_names))
    assert {f"key_{index:02d}" for index in range(14)}.issubset(filter_names)
    assert routes_out == [("relates", "related_doc")]
    assert routes_in == [("backlinks", "related_doc")]
    assert pages > 1


def test_subtype_continuation_rejects_tampering_and_projection_changes(
    clean_runtime: str,
) -> None:
    _publish_large_subtype(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        first = inspect_subtype(
            conn,
            session,
            "large_doc",
            page_size=2,
            max_bytes=4_096,
        )
        repeated = inspect_subtype(
            conn,
            session,
            "large_doc",
            page_size=2,
            max_bytes=4_096,
        )
        token = first["continuation_token"]
        assert repeated["continuation_token"] == token
        with pytest.raises(ContinuationTokenInvalid):
            inspect_subtype(
                conn,
                session,
                "large_doc",
                continuation_token=token[:-1] + ("A" if token[-1] != "A" else "B"),
                page_size=2,
                max_bytes=4_096,
            )
        with pytest.raises(ContinuationTokenInvalid):
            inspect_subtype(
                conn,
                replace(session, nomos_subject="subject:another-reader"),
                "large_doc",
                continuation_token=token,
                page_size=2,
                max_bytes=4_096,
            )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "large:next-generation",
                    "large_doc",
                    {"key_00": "new"},
                    {"id": "large:next-generation"},
                    {"v": 1},
                )
            ],
            source="default",
            strict_endpoints=True,
        )
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published", published
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        next_session = open_session(conn, deployment_name="default")
        with pytest.raises(ContinuationTokenInvalid):
            inspect_subtype(
                conn,
                next_session,
                "large_doc",
                continuation_token=token,
                page_size=2,
                max_bytes=4_096,
            )
        with pytest.raises(ContinuationTokenInvalid):
            inspect_subtype(
                conn,
                session,
                "large_doc",
                continuation_token=token,
                page_size=3,
                max_bytes=4_096,
            )


def test_default_summary_never_embeds_wide_full_node_attrs(clean_runtime: str) -> None:
    _publish_large_subtype(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        summary = inspect_subtype(conn, session, "large_doc", max_bytes=4_096)
        # #3666 step 3b: the sample records are cut to fit, not refused.
        bounded_detail = inspect_subtype(
            conn,
            session,
            "large_doc",
            detail="sample_nodes",
            page_size=1,
            max_bytes=4_096,
        )
        roomy = inspect_subtype(
            conn, session, "large_doc",
            detail="sample_nodes", page_size=1, max_bytes=12_288,
        )
        detail = inspect_subtype(
            conn,
            session,
            "large_doc",
            detail="sample_nodes",
            page_size=1,
            max_bytes=64 * 1_024,
        )

    assert summary["sample_nodes"] is None
    assert public_call_tool_result_bytes(summary) <= 4_096
    assert public_call_tool_result_bytes(
        bounded_detail, tool="inspect", kind="subtype",
    ) <= 4_096
    # Each sample is served cut, or left out by name when even cut it does
    # not fit; none is dropped silently.
    samples = bounded_detail["sample_nodes"]
    omitted = bounded_detail.get("sample_nodes_omitted", [])
    assert [node["node_id"] for node in samples] + omitted == ["large:1", "large:2"]
    for node in samples:
        assert node["status"] == "cut"
        assert node["attrs_cut"]["wide"]["chars"] == 20_000
        assert len(node["attrs"]["wide"] or "") < 20_000
        assert node["read_whole"]["target"] == {"kind": "text", "node_id": node["node_id"]}
    # With room for the samples cut, both are served and the short values whole.
    assert [node["node_id"] for node in roomy["sample_nodes"]] == ["large:1", "large:2"]
    assert "sample_nodes_omitted" not in roomy
    for node in roomy["sample_nodes"]:
        assert node["status"] == "cut"
        assert "value-key_00" in node["attrs"]["key_00"]
    # The opt-in detail is intentionally subject to the outer byte-bound remedy;
    # it does not contaminate the default schema discovery response.
    assert detail["detail"] == "sample_nodes"
    assert detail["sample_nodes"]
    assert all("attrs_cut" not in node for node in detail["sample_nodes"])


def test_single_pathological_schema_item_returns_typed_budget_remedy(
    clean_runtime: str,
) -> None:
    _publish_large_subtype(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        with pytest.raises(SubtypeSummaryItemTooLarge) as error:
            compact_subtype_summary(
                conn,
                session,
                subtype="large_doc",
                node_count=2,
                attributes=[{"name": "k" * 10_000, "declared": True}],
                edges_out=[],
                edges_in=[],
                recommended_filters=[],
                visibility={"status": "observed_statistics_available"},
                max_bytes=4_096,
                requested_page_size=1,
                continuation_token=None,
                detail="summary",
            )

    assert error.value.code == "subtype_summary_item_too_large"
    assert error.value.data["max_bytes"] == 4_096
    assert error.value.fix_hint
    # The page room a retry needs, for the server to offer as a call (#3666).
    assert error.value.needed_page_bytes > error.value.data["minimum_item_bytes"] > 4_096
