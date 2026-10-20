from __future__ import annotations

from tests.substrate.mcp._result_payload import (
    payload as _result_payload,
    result_text as _result_text,
)


import anyio
import psycopg
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp.response_bounds import public_call_tool_result_bytes
from okg.substrate.mcp.server import build_server
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import SubtypeSpec, seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate.mcp._result_payload import (
    json_client_sessions,
)



@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch):
    """Program-side test: every call here reads the JSON result format
    (issue #3059), through request `_meta` or the in-process scope."""
    json_client_sessions(monkeypatch)

pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _mcp_role_can_log_in(clean_runtime: str, okg_mcp_can_log_in: str) -> None:
    """``okg_mcp`` can log in.

    Set once per session under the cluster role lock and never reverted
    per test; see ``okg_mcp_login_enabled`` in tests/substrate/conftest.py.
    """


def _publish_fixture(
    dsn: str,
    *,
    deployment_name: str = "default",
    seed_catalog_rows: bool = True,
) -> str:
    declared = {f"key_{index:02d}": "text" for index in range(12)}
    with psycopg.connect(dsn, autocommit=True) as conn:
        if seed_catalog_rows:
            seed_catalog(
                conn,
                subtypes=[
                    "page",
                    "paper",
                    SubtypeSpec(
                        "large_doc", metadata={"filterable_attrs": declared}
                    ),
                    "related_doc",
                ],
                narrowings=[
                    ("page", "first", "page"),
                    ("page", "second", "page"),
                    ("page", "empty", "page"),
                    ("large_doc", "relates", "related_doc"),
                ],
            )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "n:start", "page", {"text": "start"}, {"id": "start"}, {"v": 1}
                ),
                NodeFact(
                    "n:middle", "page", {"text": "middle"}, {"id": "middle"}, {"v": 1}
                ),
                NodeFact(
                    "n:terminal",
                    "page",
                    {"text": "terminal"},
                    {"id": "terminal"},
                    {"v": 1},
                ),
                NodeFact("n:a", "page", {"text": "a"}, {"id": "a"}, {"v": 1}),
                NodeFact("n:b", "page", {"text": "b"}, {"id": "b"}, {"v": 1}),
                NodeFact("n:c", "page", {"text": "c"}, {"id": "c"}, {"v": 1}),
                NodeFact(
                    "large:1",
                    "large_doc",
                    {
                        **{name: f"value-{name}" for name in declared},
                        "wide": "x" * 20_000,
                    },
                    {"id": "large:1"},
                    {"v": 1},
                ),
                NodeFact("related:1", "related_doc", {}, {"id": "related:1"}, {"v": 1}),
                EdgeFact(
                    "n:start",
                    "n:middle",
                    "first",
                    source_record_id={"id": "e1"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    "n:middle",
                    "n:terminal",
                    "second",
                    source_record_id={"id": "e2"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    "n:start",
                    "n:a",
                    "first",
                    source_record_id={"id": "e3"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    "n:start",
                    "n:b",
                    "first",
                    source_record_id={"id": "e4"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    "n:start",
                    "n:c",
                    "first",
                    source_record_id={"id": "e5"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    "large:1",
                    "related:1",
                    "relates",
                    source_record_id={"id": "e6"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    published = Publisher(dsn, deployment_name, catalog_identity=bind_test_catalog_writer(dsn, deployment_name)).cycle()
    assert published["status"] == "published", published
    return str(published["generation_id"])


def _payload(result) -> dict:
    payload = _result_payload(result)
    assert "operator_execution_failed" not in _result_text(result)
    return payload


def test_current_dev_failure_characterization_is_complete(clean_runtime: str) -> None:
    generation = _publish_fixture(clean_runtime)
    server = build_server(dsn=clean_runtime, deployment_name="default")

    async def drive() -> None:
        try:
            async with create_connected_server_and_client_session(server) as client:
                invalid_direction = await client.call_tool(
                    "expand",
                    {"input": {"node_id": "n:start"}, "path": ["first:sideways"]},
                )
                invalid_edge = await client.call_tool(
                    "expand",
                    {"input": {"node_id": "n:start"}, "path": ["unknown:out"]},
                )
                missing = await client.call_tool(
                    "expand",
                    {"input": {"node_id": "n:missing"}, "path": ["first:out"]},
                )
                first_empty = await client.call_tool(
                    "expand",
                    {"input": {"node_id": "n:start"}, "path": ["empty:out"]},
                )
                later_empty = await client.call_tool(
                    "expand",
                    {
                        "input": {"node_id": "n:start"},
                        "path": ["first:out", "empty:out"],
                    },
                )
                terminal_empty = await client.call_tool(
                    "expand",
                    {
                        "input": {"node_id": "n:start"},
                        "path": ["first:out", "second:out"],
                        "target_subtype": "paper",
                    },
                )
                truncated = await client.call_tool(
                    "expand",
                    {
                        "input": {"node_id": "n:start"},
                        "path": ["first:out"],
                        "limit": 2,
                        "select": ["node_id"],
                    },
                )
                one_hop = await client.call_tool(
                    "expand",
                    {
                        "input": {"node_id": "n:start"},
                        "edge_type": "first",
                        "limit": 1,
                    },
                )
                summary = await client.call_tool(
                    "inspect",
                    {
                        "target": {
                            "kind": "subtype",
                            "subtype": "large_doc",
                            "page_size": 8,
                        },
                        "max_bytes": 4_096,
                    },
                )
                invalid_subtype_detail = await client.call_tool(
                    "inspect",
                    {
                        "target": {
                            "kind": "subtype",
                            "subtype": "large_doc",
                            "detail": "full_everything",
                        },
                        "max_bytes": 4_096,
                    },
                )
                invalid_subtype_page = await client.call_tool(
                    "inspect",
                    {
                        "target": {
                            "kind": "subtype",
                            "subtype": "large_doc",
                            "page_size": "many",
                        },
                        "max_bytes": 4_096,
                    },
                )
        finally:
            server._okg_pool.close()

        assert _payload(invalid_direction)["error"]["code"] == "invalid_path_direction"
        assert _payload(invalid_edge)["error"]["code"] == "invalid_path_edge_type"
        expected = [
            (missing, "start_not_visible"),
            (first_empty, "no_matching_first_hop"),
            (later_empty, "empty_intermediate_hop"),
            (terminal_empty, "empty_terminal_result"),
            (truncated, "nonempty_truncated"),
        ]
        for result, code in expected:
            payload = _payload(result)
            assert payload["outcome"]["code"] == code
            assert payload["outcome"]["next_move"]
            assert payload["pinned_generation_id"] == generation
        truncated_payload = _payload(truncated)
        assert truncated_payload["continuation_token"]
        one_hop_payload = _payload(one_hop)
        assert one_hop_payload["neighbors"]
        assert one_hop_payload["next_offset"] > 0
        summary_payload = _payload(summary)
        assert summary_payload["pinned_generation_id"] == generation
        assert summary_payload["sample_nodes"] is None
        assert summary_payload["continuation_token"]
        assert public_call_tool_result_bytes(summary_payload) <= 4_096
        invalid_detail_payload = _payload(invalid_subtype_detail)
        assert invalid_subtype_detail.isError is True
        assert invalid_detail_payload["error"]["code"] == "invalid_subtype_inspection"
        invalid_page_payload = _payload(invalid_subtype_page)
        assert invalid_subtype_page.isError is True
        assert invalid_page_payload["error"]["code"] == "invalid_subtype_inspection"
        assert invalid_page_payload["error"]["data"]["field"] == "page_size"
        assert "operator_execution_failed" not in _result_text(invalid_subtype_page)

    anyio.run(drive)


def test_the_actionable_bounds_read_the_same_in_the_text_format(clean_runtime: str) -> None:
    """The characterization above, read as a model reads it: the default
    text format (review of 2102d20798: these bounds were tested in JSON
    only). Each refusal is an error whose status line names its code; each
    outcome, cursor and page fact is in the text; the subtype page fits its
    4 KiB budget as served."""
    from okg.substrate.mcp import result_text

    generation = _publish_fixture(clean_runtime)
    server = build_server(dsn=clean_runtime, deployment_name="default")
    calls = {
        "invalid_direction": ("expand", {"input": {"node_id": "n:start"}, "path": ["first:sideways"]}),
        "invalid_edge": ("expand", {"input": {"node_id": "n:start"}, "path": ["unknown:out"]}),
        "missing": ("expand", {"input": {"node_id": "n:missing"}, "path": ["first:out"]}),
        "first_empty": ("expand", {"input": {"node_id": "n:start"}, "path": ["empty:out"]}),
        "truncated": ("expand", {"input": {"node_id": "n:start"}, "path": ["first:out"],
                                 "limit": 2, "select": ["node_id"]}),
        "one_hop": ("expand", {"input": {"node_id": "n:start"}, "edge_type": "first", "limit": 1}),
        "summary": ("inspect", {"target": {"kind": "subtype", "subtype": "large_doc",
                                           "page_size": 8}, "max_bytes": 4_096}),
        "invalid_page": ("inspect", {"target": {"kind": "subtype", "subtype": "large_doc",
                                                "page_size": "many"}, "max_bytes": 4_096}),
    }
    results: dict = {}

    async def drive() -> None:
        try:
            async with create_connected_server_and_client_session(server) as client:
                for name, (tool, arguments) in calls.items():
                    # `meta={}` names no format: the server sends text.
                    results[name] = await client.call_tool(tool, arguments, meta={})
        finally:
            server._okg_pool.close()

    anyio.run(drive)
    texts = {}
    for name, result in results.items():
        assert result.structuredContent is None and len(result.content) == 1, name
        texts[name] = result.content[0].text
        assert "operator_execution_failed" not in texts[name], name
    for name, code in (("invalid_direction", "invalid_path_direction"),
                       ("invalid_edge", "invalid_path_edge_type"),
                       ("invalid_page", "invalid_subtype_inspection")):
        assert results[name].isError is True, name
        line = result_text.parse_status_line(texts[name])
        assert (line.status, line.code) == ("refused", code), texts[name][:300]
    assert "page_size" in texts["invalid_page"]
    for name, code in (("missing", "start_not_visible"),
                       ("first_empty", "no_matching_first_hop"),
                       ("truncated", "nonempty_truncated")):
        assert results[name].isError is False, name
        assert result_text.parse_status_line(texts[name]).status == "ok"
        assert f'"code":"{code}"' in texts[name], texts[name][:600]
    # D8 (read-passage-rows-3666, Jason): no read reply's text names the
    # generation, not even a session's first; inspect(graph) does.
    said = [result_text.result_generation(text) for text in texts.values()]
    assert all(value is None for value in said), said
    assert isinstance(generation, str) and generation
    paging = result_text.fact_value(result_text.status_facts(texts["truncated"]), "next:")
    # PACT read-model-s6-page-next (design read-model 5): `next: <token>`,
    # the bare token, passed back as `next`.
    assert isinstance(paging, str) and paging.count(".") == 1, paging
    assert not paging.startswith(("continuation_token=", "offset=")), paging
    one_hop = result_text.fact_value(result_text.status_facts(texts["one_hop"]), "next:")
    assert isinstance(one_hop, str) and int(one_hop[len("offset="):]) > 0
    assert result_text.result_cards(texts["one_hop"])
    served = len(results["summary"].model_dump_json(by_alias=True, exclude_none=True).encode())
    assert served <= 4_096
    summary_next = result_text.fact_value(result_text.status_facts(texts["summary"]), "next:")
    assert isinstance(summary_next, str) and summary_next.count(".") == 1, summary_next
    assert not summary_next.startswith("continuation_token="), summary_next


def test_inspect_view_answers_through_the_real_call_path(clean_runtime: str) -> None:
    """Issue #2793: `inspect` kind `view` refused every view.

    `inspect_view` answered, and the public result model then rejected its
    `{name, type}` columns, so the caller got `operator_execution_failed`.
    """
    generation = _publish_fixture(clean_runtime)
    server = build_server(dsn=clean_runtime, deployment_name="default")

    async def drive() -> None:
        try:
            async with create_connected_server_and_client_session(server) as client:
                described = await client.call_tool(
                    "inspect", {"target": {"kind": "view", "name": "v_nodes"}},
                )
        finally:
            server._okg_pool.close()

        assert described.isError is not True, _result_text(described)
        payload = _payload(described)
        assert payload["view"] == "okg.v_nodes"
        assert payload["pinned_generation_id"] == generation
        columns = payload["columns"]
        assert columns
        assert all(set(column) == {"name", "type"} for column in columns)
        assert "node_id" in {column["name"] for column in columns}

    anyio.run(drive)


def test_disposable_published_generation_proves_actionable_bounds(
    clean_runtime: str,
) -> None:
    generation = _publish_fixture(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        row = conn.execute(
            "SELECT status, catalog_version_id FROM okg.v_generations WHERE id = %s",
            (generation,),
        ).fetchone()
    assert row is not None
    assert row[0] == "published"
    assert row[1] is not None
