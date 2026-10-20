from __future__ import annotations

from tests.substrate.mcp._result_payload import (
    json_client_sessions,
    payload as _result_payload,
    result_text as _result_text,
)

import json

import anyio
import psycopg
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import expand_path, open_session, parse_path_hops
from okg.substrate.mcp.continuation import ContinuationTokenInvalid
from okg.substrate.mcp.path_diagnostics import (
    InvalidPathDirection,
    InvalidPathGrammar,
)
from okg.substrate.mcp.server import build_server
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.substrate.mcp.test_nomos_source_lineage_mcp import (
    _activate_source_policy,
    _append_restricted_lineage_graph,
    _publish_lineage_graph,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch: pytest.MonkeyPatch) -> None:
    """These tests assert on the dictionary, so every client call asks for
    the JSON result format (issue #3059: the default is one text block)."""
    json_client_sessions(monkeypatch)


@pytest.fixture(autouse=True)
def _mcp_role_can_log_in(clean_runtime: str, okg_mcp_can_log_in: str) -> None:
    """``okg_mcp`` can log in.

    Set once per session under the cluster role lock and never reverted
    per test; see ``okg_mcp_login_enabled`` in tests/substrate/conftest.py.
    """


def _publish_path_fixture(dsn: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["page", "paper"],
            narrowings=[
                ("page", "first", "page"),
                ("page", "second", "page"),
                ("page", "empty", "page"),
            ],
        )
        nodes = [
            NodeFact("n:start", "page", {"text": "start"}, {"id": "start"}, {"v": 1}),
            NodeFact(
                "n:middle", "page", {"text": "middle"}, {"id": "middle"}, {"v": 1}
            ),
            NodeFact(
                "n:terminal", "page", {"text": "terminal"}, {"id": "terminal"}, {"v": 1}
            ),
            NodeFact("n:a", "page", {"text": "a"}, {"id": "a"}, {"v": 1}),
            NodeFact("n:b", "page", {"text": "b"}, {"id": "b"}, {"v": 1}),
            NodeFact("n:c", "page", {"text": "c"}, {"id": "c"}, {"v": 1}),
        ]
        edges = [
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
        ]
        ChronosGraphStore.from_connection(conn).apply_facts(
            [*nodes, *edges],
            source="default",
            strict_endpoints=True,
        )
    published = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert published["status"] == "published", published
    return str(published["generation_id"])


def test_path_grammar_is_typed_before_graph_execution() -> None:
    with pytest.raises(InvalidPathGrammar) as empty:
        parse_path_hops([])
    assert empty.value.data["hop_index"] == 0

    with pytest.raises(InvalidPathDirection) as direction:
        parse_path_hops(["first:sideways"])
    assert direction.value.data == {
        "hop_index": 0,
        "edge_type": "first",
        "direction": "sideways",
    }


def test_empty_narrowing_catalog_preserves_permissive_edge_semantics(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"], narrowings=[])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact("n:start", "page", {}, {"id": "start"}, {"v": 1}),
                NodeFact("n:end", "page", {}, {"id": "end"}, {"v": 1}),
                EdgeFact(
                    "n:start",
                    "n:end",
                    "free_edge",
                    source_record_id={"id": "edge"},
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
        result = expand_path(
            conn,
            session,
            "n:start",
            hops=[("free_edge", "out")],
        )

    assert result["outcome"]["code"] == "nonempty_complete"
    assert [row["node_id"] for row in result["results"]] == ["n:end"]


def test_path_outcomes_name_decisive_visible_hop_without_generic_failure(
    clean_runtime: str,
) -> None:
    generation = _publish_path_fixture(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        missing = expand_path(conn, session, "n:missing", hops=[("first", "out")])
        first_empty = expand_path(conn, session, "n:start", hops=[("empty", "out")])
        later_empty = expand_path(
            conn,
            session,
            "n:start",
            hops=[("first", "out"), ("empty", "out")],
        )
        terminal_empty = expand_path(
            conn,
            session,
            "n:start",
            hops=[("first", "out"), ("second", "out")],
            target_subtype="paper",
        )

    assert missing["outcome"]["code"] == "start_not_visible"
    assert first_empty["outcome"]["code"] == "no_matching_first_hop"
    assert first_empty["outcome"]["hop_index"] == 0
    assert later_empty["outcome"]["code"] == "empty_intermediate_hop"
    assert later_empty["outcome"]["hop_index"] == 1
    assert terminal_empty["outcome"]["code"] == "empty_terminal_result"
    assert terminal_empty["outcome"]["hop_index"] == 1
    for payload in (missing, first_empty, later_empty, terminal_empty):
        assert payload["status"] == "ok"
        assert payload["results"] == []
        assert payload["truncated"] is False
        assert payload["pinned_generation_id"] == generation
        assert payload["outcome"]["next_move"]
        assert "operator_execution_failed" not in json.dumps(payload)


def test_path_truncation_continues_without_gaps_or_duplicates(
    clean_runtime: str,
) -> None:
    generation = _publish_path_fixture(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        first = expand_path(
            conn,
            session,
            "n:start",
            hops=[("first", "out")],
            limit=2,
            select=["node_id"],
        )
        second = expand_path(
            conn,
            session,
            "n:start",
            hops=[("first", "out")],
            limit=2,
            select=["node_id"],
            continuation_token=first["continuation_token"],
        )
        token = first["continuation_token"]
        tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
        with pytest.raises(ContinuationTokenInvalid) as tamper_error:
            expand_path(
                conn,
                session,
                "n:start",
                hops=[("first", "out")],
                limit=2,
                select=["node_id"],
                continuation_token=tampered,
            )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "n:new-generation",
                    "page",
                    {"text": "new generation"},
                    {"id": "new-generation"},
                    {"v": 1},
                )
            ],
            source="default",
            strict_endpoints=True,
        )
    newer = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert newer["status"] == "published", newer
    assert str(newer["generation_id"]) != generation
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        newer_session = open_session(conn, deployment_name="default")
        with pytest.raises(ContinuationTokenInvalid) as generation_error:
            expand_path(
                conn,
                newer_session,
                "n:start",
                hops=[("first", "out")],
                limit=2,
                select=["node_id"],
                continuation_token=token,
            )

    assert first["truncated"] is True
    assert first["outcome"]["code"] == "nonempty_truncated"
    ids = [row["node_id"] for row in first["results"] + second["results"]]
    assert ids == sorted(set(ids))
    assert ids == ["n:a", "n:b", "n:c", "n:middle"]
    assert second["truncated"] is False
    assert first["pinned_generation_id"] == second["pinned_generation_id"] == generation
    assert tamper_error.value.data["reason"] == "signature_invalid"
    assert generation_error.value.data["reason"] == "binding_mismatch"


def test_public_invalid_path_preserves_typed_error_envelope(clean_runtime: str) -> None:
    _publish_path_fixture(clean_runtime)
    server = build_server(dsn=clean_runtime, deployment_name="default")

    async def drive() -> None:
        try:
            async with create_connected_server_and_client_session(server) as client:
                direction_result = await client.call_tool(
                    "expand",
                    {
                        "input": {"node_id": "n:start"},
                        "path": ["first:sideways"],
                        "max_bytes": 4_096,
                    },
                )
                edge_result = await client.call_tool(
                    "expand",
                    {
                        "input": {"node_id": "n:start"},
                        "path": ["not_in_catalog:out"],
                        "max_bytes": 4_096,
                    },
                )
                empty_result = await client.call_tool(
                    "expand",
                    {
                        "input": {"node_id": "n:start"},
                        "path": [],
                        "max_bytes": 4_096,
                    },
                )
                zero_limit = await client.call_tool(
                    "expand",
                    {
                        "input": {"node_id": "n:start"},
                        "path": ["first:out"],
                        "limit": 0,
                        "max_bytes": 4_096,
                    },
                )
                uniform_direction = await client.call_tool(
                    "expand",
                    {
                        "input": {"node_id": "n:start"},
                        "edge_type": "first",
                        "direction": "sideways",
                        "depth": 2,
                        "max_bytes": 4_096,
                    },
                )
                uniform_depth = await client.call_tool(
                    "expand",
                    {
                        "input": {"node_id": "n:start"},
                        "edge_type": "first",
                        "depth": 4,
                        "max_bytes": 4_096,
                    },
                )
                first_page = await client.call_tool(
                    "expand",
                    {
                        "input": {"node_id": "n:start"},
                        "path": ["first:out"],
                        "limit": 2,
                        "select": ["node_id"],
                        "max_bytes": 4_096,
                    },
                )
                first_page_payload = _result_payload(first_page)
                token = first_page_payload["continuation_token"]
                public_tamper = await client.call_tool(
                    "expand",
                    {
                        "input": {"node_id": "n:start"},
                        "path": ["first:out"],
                        "limit": 2,
                        "select": ["node_id"],
                        "continuation_token": token[:-1]
                        + ("A" if token[-1] != "A" else "B"),
                        "max_bytes": 4_096,
                    },
                )
        finally:
            server._okg_pool.close()
        assert direction_result.isError is True
        payload = _result_payload(direction_result)
        assert payload["error"]["code"] == "invalid_path_direction"
        assert payload["error"]["data"]["hop_index"] == 0
        assert payload["error"]["data"]["pinned_generation_id"]
        assert "operator_execution_failed" not in _result_text(direction_result)
        edge_payload = _result_payload(edge_result)
        assert edge_result.isError is True
        assert edge_payload["error"]["code"] == "invalid_path_edge_type"
        assert edge_payload["error"]["data"]["edge_type"] == "not_in_catalog"
        empty_payload = _result_payload(empty_result)
        assert empty_result.isError is True
        assert empty_payload["error"]["code"] == "invalid_path_grammar"
        zero_limit_payload = _result_payload(zero_limit)
        assert zero_limit.isError is True
        assert zero_limit_payload["error"]["code"] == "invalid_expand_shape"
        assert zero_limit_payload["error"]["data"]["field"] == "limit"
        assert zero_limit_payload["error"]["data"]["pinned_generation_id"]
        assert "operator_execution_failed" not in _result_text(zero_limit)
        uniform_direction_payload = _result_payload(uniform_direction)
        assert uniform_direction_payload["error"]["code"] == "invalid_path_direction"
        uniform_depth_payload = _result_payload(uniform_depth)
        assert uniform_depth_payload["error"]["code"] == "invalid_path_grammar"
        tamper_payload = _result_payload(public_tamper)
        assert tamper_payload["error"]["code"] == "continuation_token_invalid"
        for result in (uniform_direction, uniform_depth, public_tamper):
            assert result.isError is True
            assert "operator_execution_failed" not in _result_text(result)

    anyio.run(drive)


def test_an_unknown_edge_type_points_at_a_read_that_fits_the_budget(
    clean_runtime: str,
) -> None:
    """#3666: the refusal sent the caller to inspect(target={'kind':'ontology'}),
    an unpaged payload larger than the default reply budget on a real
    deployment. It now names the closest catalog edge types and points at the
    graph's edge_types block, which is served at the default budget."""
    _publish_path_fixture(clean_runtime)
    server = build_server(dsn=clean_runtime, deployment_name="default")

    async def drive() -> tuple:
        try:
            async with create_connected_server_and_client_session(server) as client:
                refused = await client.call_tool(
                    "expand",
                    {"input": {"node_id": "n:start"}, "path": ["frist:out"]},
                )
                pointed = await client.call_tool(
                    "inspect",
                    {"target": {"kind": "graph"}, "select": ["edge_types"]},
                )
        finally:
            server._okg_pool.close()
        return refused, pointed

    refused, pointed = anyio.run(drive)
    assert refused.isError is True
    error = _result_payload(refused)["error"]
    assert error["code"] == "invalid_path_edge_type"
    assert "ontology" not in error["fix_hint"]
    assert error["data"]["closest_edge_types"] == ["first"]
    assert error["data"]["catalog_edge_type_count"] == 3
    assert "inspect(target={'kind':'graph'}, select=['edge_types'])" in error["fix_hint"]
    assert pointed.isError is False
    counts = _result_payload(pointed)["edge_type_counts"]
    assert {"first", "second"} <= set(counts)


def test_nomos_hidden_and_absent_path_structure_are_observationally_equivalent(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    absent_generation = _publish_lineage_graph(
        clean_runtime,
        include_restricted=False,
    )
    hidden_generation = _append_restricted_lineage_graph(clean_runtime)
    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        absent_session = open_session(
            conn,
            deployment_name="default",
            requested_generation=absent_generation,
        )
        absent = expand_path(
            conn,
            absent_session,
            "node:public-docs",
            hops=[("references", "out")],
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        hidden_session = open_session(
            conn,
            deployment_name="default",
            requested_generation=hidden_generation,
        )
        hidden = expand_path(
            conn,
            hidden_session,
            "node:public-docs",
            hops=[("references", "out")],
        )

    def observable(payload: dict) -> dict:
        return {
            "status": payload["status"],
            "outcome": payload["outcome"],
            "results": payload["results"],
            "total_count": payload["total_count"],
            "truncated": payload["truncated"],
        }

    assert observable(absent) == observable(hidden)
    assert absent["outcome"]["code"] == "visibility_limited"
    assert absent["total_count"] is None
    assert absent["count_state"] == "withheld"
    assert absent["pinned_generation_id"] == absent_generation
    assert hidden["pinned_generation_id"] == hidden_generation

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        absent_session = open_session(
            conn,
            deployment_name="default",
            requested_generation=absent_generation,
        )
        absent_start = expand_path(
            conn,
            absent_session,
            "node:google-drive",
            hops=[("references", "out")],
        )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        hidden_session = open_session(
            conn,
            deployment_name="default",
            requested_generation=hidden_generation,
        )
        hidden_start = expand_path(
            conn,
            hidden_session,
            "node:google-drive",
            hops=[("references", "out")],
        )
    assert observable(absent_start) == observable(hidden_start)
    assert absent_start["outcome"]["code"] == "start_not_visible"


def _publish_document_fixture(
    dsn: str, *, chunks: int = 12, filler: str = "",
) -> list[str]:
    """One document whose chunk ids sort OPPOSITE to their reading order.

    Returns the chunk ids in reading (``char_offset``) order.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["document", "document_chunk"],
            narrowings=[("document", "contains", "document_chunk")],
        )
        doc = "doc:r:guide.md"
        facts: list[NodeFact | EdgeFact] = [
            NodeFact(doc, "document", {"text": "guide"}, {"id": doc}, {"v": 1}),
        ]
        reading_order = []
        for position in range(chunks):
            chunk = f"chunk:{chunks - 1 - position:02d}"
            reading_order.append(chunk)
            facts.append(
                NodeFact(
                    chunk,
                    "document_chunk",
                    {
                        "text": f"part {position} {filler}",
                        "char_offset": position * 100,
                    },
                    {"id": chunk},
                    {"v": 1},
                )
            )
            facts.append(
                EdgeFact(
                    doc,
                    chunk,
                    "contains",
                    source_record_id={"id": f"{doc}|{chunk}"},
                    source_revision={"v": 1},
                )
            )
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source="default", strict_endpoints=True,
        )
    published = Publisher(
        dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    assert published["status"] == "published", published
    return reading_order


def test_the_instructed_whole_document_read_works(clean_runtime: str) -> None:
    """Issue #2676: the server instructions teach one path-shaped expand to
    read a whole document, paging by `continuation_token` and ordering by
    `attrs.char_offset`, and say a one-hop expand carries no chunk text. This runs
    those exact calls through the MCP server, so the instructions cannot
    drift from what the tools do."""
    reading_order = _publish_document_fixture(clean_runtime)
    server = build_server(dsn=clean_runtime, deployment_name="default")
    instructed = {
        "input": {"node_id": "doc:r:guide.md"},
        "path": ["contains:out"],
        "target_subtype": "document_chunk",
        "limit": 5,
    }

    async def drive() -> tuple[list[dict], int, object, object, dict]:
        try:
            async with create_connected_server_and_client_session(server) as client:
                rows: list[dict] = []
                pages = 0
                args = dict(instructed)
                while True:
                    page = _result_payload(await client.call_tool("expand", args))
                    pages += 1
                    rows.extend(page["results"])
                    if not page["truncated"]:
                        break
                    args = {**instructed, "continuation_token": page["continuation_token"]}
                one_hop = await client.call_tool(
                    "expand",
                    {
                        "input": {"node_id": "doc:r:guide.md"},
                        "edge_type": "contains",
                        "target_subtype": "document_chunk",
                    },
                )
                edges_only = _result_payload(
                    await client.call_tool(
                        "expand",
                        {"input": {"node_id": "doc:r:guide.md"}, "edge_type": "contains"},
                    )
                )
                nested_select = await client.call_tool(
                    "inspect",
                    {
                        "target": {"kind": "node", "node_id": reading_order[0]},
                        "select": ["attrs.text"],
                    },
                )
        finally:
            server._okg_pool.close()
        return rows, pages, one_hop, nested_select, edges_only

    rows, pages, one_hop, nested_select, edges_only = anyio.run(drive)

    assert pages == 3
    ids = [row["node_id"] for row in rows]
    assert sorted(ids) == sorted(reading_order)
    assert len(set(ids)) == len(ids) == 12
    # Non-vacuity: the rows do NOT arrive in reading order, which is why the
    # instructions tell the agent to sort them.
    assert ids != reading_order
    ordered = sorted(rows, key=lambda row: row["attrs"]["char_offset"])
    assert [row["node_id"] for row in ordered] == reading_order
    for position, row in enumerate(ordered):
        assert f"part {position}" in row["attrs"]["text"]

    # A one-hop expand's rows are the linked records (#3666), and
    # `target_subtype` there is the type condition on them, so this call is
    # served now; on link rows it is still refused (test_read_grammar_db.py
    # pins that). Neither one-hop reply carries chunk text.
    assert one_hop.isError is not True
    typed = _result_payload(one_hop)["neighbors"]
    assert sorted(row["node_id"] for row in typed) == sorted(reading_order)
    assert {row["subtype"] for row in typed} == {"document_chunk"}
    assert not any("part " in str(row) for row in typed)
    neighbors = edges_only["neighbors"]
    assert sorted(row["node_id"] for row in neighbors) == sorted(reading_order)
    assert not any("part " in str(row) for row in neighbors)
    assert nested_select.isError is True
    assert "select_unknown_field" in _result_text(nested_select)


def test_the_instructed_page_of_twenty_fits_the_default_budget(
    clean_runtime: str,
) -> None:
    """The instructions say `limit=20`. A path expand cannot page by bytes,
    so a page over the default response budget is refused outright; this
    pins that twenty chunks of the default chunker's size (~1,500
    characters) fit in one page under the default `max_bytes`."""
    reading_order = _publish_document_fixture(
        clean_runtime, chunks=20, filler="lorem ipsum dolor sit amet " * 55,
    )
    server = build_server(dsn=clean_runtime, deployment_name="default")

    async def drive() -> dict:
        try:
            async with create_connected_server_and_client_session(server) as client:
                return _result_payload(
                    await client.call_tool(
                        "expand",
                        {
                            "input": {"node_id": "doc:r:guide.md"},
                            "path": ["contains:out"],
                            "target_subtype": "document_chunk",
                            "limit": 20,
                        },
                    )
                )
        finally:
            server._okg_pool.close()

    page = anyio.run(drive)

    assert "error" not in page, page.get("error")
    assert page["truncated"] is False
    assert sorted(row["node_id"] for row in page["results"]) == sorted(reading_order)
    assert all(len(row["attrs"]["text"]) >= 1_500 for row in page["results"])


def test_star_is_any_link_in_the_path_grammar_and_its_token_rebuilds() -> None:
    """`*` is any link, as the expand description says: '*:out' parses to
    the any-edge hop, and a token over a mixed path rebuilds as that path."""
    from okg.substrate.mcp.continuation import expand_shape_from_hops

    assert parse_path_hops(["*:out", "first:in"]) == [(None, "out"), ("first", "in")]
    assert expand_shape_from_hops([[None, "out"], ["first", "in"]])["path"] == [
        "*:out", "first:in",
    ]
    assert expand_shape_from_hops([[None, "out"]])["path"] == ["*:out"]
    # Uniform any-edge hops keep their depth form.
    assert expand_shape_from_hops([[None, "in"], [None, "in"]]) == {
        "path": None, "edge_type": None, "direction": "in", "depth": 2,
    }


def _expand_calls(
    dsn: str, *calls: dict, deployment: str = "default", start: str = "n:start",
) -> list:
    server = build_server(dsn=dsn, deployment_name=deployment)
    out: list = []

    async def drive() -> None:
        try:
            async with create_connected_server_and_client_session(server) as client:
                for arguments in calls:
                    out.append(await client.call_tool(
                        "expand", {"input": {"node_id": start}, **arguments},
                    ))
        finally:
            server._okg_pool.close()
            if server._okg_write_pool:
                server._okg_write_pool.close()

    anyio.run(drive)
    return out


def test_star_reads_any_link_in_a_path_and_on_one_hop(clean_runtime: str) -> None:
    """Before this change '*' was refused in a path as an unknown edge type
    and, on one hop, matched no link and came back "0 of 0, complete"."""
    _publish_path_fixture(clean_runtime)
    one_path, two_hops, mixed, page_one, one_hop, one_hop_where = _expand_calls(
        clean_runtime,
        {"path": ["*:out"], "select": ["node_id"]},
        {"path": ["*:out", "*:out"], "select": ["node_id"]},
        {"path": ["first:out", "*:out"], "select": ["node_id"]},
        {"path": ["*:out"], "select": ["node_id"], "limit": 2},
        {"edge_type": "*"},
        {"edge_type": "*", "where": {"type": "page"}},
    )
    for result in (one_path, two_hops, mixed, page_one, one_hop, one_hop_where):
        assert result.isError is False, _result_text(result)
    assert [row["node_id"] for row in _result_payload(one_path)["results"]] == [
        "n:a", "n:b", "n:c", "n:middle",
    ]
    assert [row["node_id"] for row in _result_payload(two_hops)["results"]] == ["n:terminal"]
    assert [row["node_id"] for row in _result_payload(mixed)["results"]] == ["n:terminal"]
    first = _result_payload(page_one)
    assert first["truncated"] is True
    (rest,) = _expand_calls(
        clean_runtime,
        {"path": ["*:out"], "select": ["node_id"], "limit": 2,
         "continuation_token": first["continuation_token"]},
    )
    ids = [row["node_id"] for row in first["results"] + _result_payload(rest)["results"]]
    assert ids == ["n:a", "n:b", "n:c", "n:middle"]
    links = _result_payload(one_hop)
    # A one-hop row is the linked record (expand records).
    assert sorted(row["node_id"] for row in links["neighbors"]) == [
        "n:a", "n:b", "n:c", "n:middle",
    ]
    assert links["total_count"] == 4
    assert len(_result_payload(one_hop_where)["neighbors"]) == 4


def test_an_unknown_one_hop_edge_type_is_refused_with_the_valid_types(
    clean_runtime: str,
) -> None:
    """Failure case: a typo on one hop was an empty page stamped complete.
    It is refused, on both one-hop routes and in a path, the same way: the
    closest catalog names, the count, the bounded graph read that lists the
    edge types, and '*' for any link."""
    _publish_path_fixture(clean_runtime)
    plain, filtered, path = _expand_calls(
        clean_runtime,
        {"edge_type": "frist"},
        {"edge_type": "frist", "where": {"type": "page"}},
        {"path": ["frist:out"]},
    )
    errors = []
    for result in (plain, filtered, path):
        assert result.isError is True, _result_text(result)
        error = _result_payload(result)["error"]
        assert error["code"] == "invalid_path_edge_type"
        assert error["data"]["edge_type"] == "frist"
        assert error["data"]["closest_edge_types"] == ["first"]
        assert error["data"]["catalog_edge_type_count"] == 3
        assert error["data"]["any_link"] == "*"
        assert error["fix_hint"].startswith("did you mean first? ")
        assert "inspect(target={'kind':'graph'}, select=['edge_types'])" in error["fix_hint"]
        assert error["fix_hint"].endswith(", or '*' for any link")
        assert "ontology" not in error["fix_hint"]
        errors.append(error)
    # One refusal for both shapes: only where it stands differs.
    shared = {k: v for k, v in errors[0]["data"].items() if k != "pinned_generation_id"}
    assert all(
        {k: v for k, v in e["data"].items() if k != "pinned_generation_id"} == shared
        for e in errors
    )
    assert len({e["fix_hint"] for e in errors}) == 1


def test_a_path_select_takes_attrs_key_as_filter_and_map_do(clean_runtime: str) -> None:
    """`attrs.<key>` in a path select returns that attribute of each far
    record; before this change it was refused as an unknown field."""
    _publish_path_fixture(clean_runtime)
    keyed, absent, whole = _expand_calls(
        clean_runtime,
        {"path": ["first:out"], "select": ["node_id", "attrs.text"]},
        {"path": ["first:out"], "select": ["attrs.title"]},
        {"path": ["first:out"], "select": ["attrs", "attrs.title"]},
    )
    for result in (keyed, absent, whole):
        assert result.isError is False, _result_text(result)
    rows = {row["node_id"]: row for row in _result_payload(keyed)["results"]}
    assert sorted(rows) == ["n:a", "n:b", "n:c", "n:middle"]
    # Only the key named; its text arrives fenced, as every record text does.
    assert list(rows["n:middle"]["attrs"]) == ["text"]
    assert "\nmiddle\n" in rows["n:middle"]["attrs"]["text"]
    assert "provenance" in rows["n:middle"]
    # Only the keys named: a key the record lacks is not invented.
    rows = {row["node_id"]: row for row in _result_payload(absent)["results"]}
    assert rows["n:middle"]["attrs"] == {}
    # `attrs` beside it is the whole map.
    rows = {row["node_id"]: row for row in _result_payload(whole)["results"]}
    assert "\nmiddle\n" in rows["n:middle"]["attrs"]["text"]


def _one_page_catalog(dsn: str, narrowings: list) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"], narrowings=narrowings)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact("n:start", "page", {"text": "start"}, {"id": "start"}, {"v": 1}),
                NodeFact("n:end", "page", {"text": "end"}, {"id": "end"}, {"v": 1}),
                EdgeFact("n:start", "n:end", "t00",
                         source_record_id={"id": "e"}, source_revision={"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
    published = Publisher(
        dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    assert published["status"] == "published", published


def test_an_unknown_edge_type_refusal_counts_the_types_it_does_not_list(
    clean_runtime: str,
) -> None:
    """A catalog with 45 edge types: the one-hop refusal counts all 45 and
    names at most the 3 closest; nothing else is listed and nothing points
    at the whole ontology."""
    _one_page_catalog(clean_runtime, [("page", f"t{i:02d}", "page") for i in range(45)])
    (refused, far) = _expand_calls(clean_runtime, {"edge_type": "t0x"}, {"edge_type": "nope"})
    for result in (refused, far):
        assert result.isError is True, _result_text(result)
    error = _result_payload(refused)["error"]
    assert error["data"]["catalog_edge_type_count"] == 45
    closest = error["data"]["closest_edge_types"]
    assert 1 <= len(closest) <= 3
    assert set(closest) <= {f"t{i:02d}" for i in range(45)}
    named = [f"t{i:02d}" for i in range(45) if f"t{i:02d}" in error["fix_hint"]]
    assert sorted(named) == sorted(closest)
    assert "ontology" not in error["fix_hint"]
    assert "valid_edge_types" not in error["data"]
    # No close name: no guess, still the count and the bounded read.
    error = _result_payload(far)["error"]
    assert "closest_edge_types" not in error["data"]
    assert error["data"]["catalog_edge_type_count"] == 45
    assert error["fix_hint"].startswith("inspect(target={'kind':'graph'}, select=['edge_types'])")


def test_a_one_hop_records_read_refuses_an_unknown_edge_type(clean_runtime: str) -> None:
    """Failure case on the one-hop records route (the default since expand
    records): a typo is refused as on the link routes, with a record select,
    with a far-end condition, and with a link select; '*' still reads every
    link there."""
    _publish_path_fixture(clean_runtime)
    records, records_where, links, star = _expand_calls(
        clean_runtime,
        {"edge_type": "frist", "select": ["node_id", "title"]},
        {"edge_type": "frist", "select": ["node_id"], "where": {"type": "page"}},
        {"edge_type": "frist", "select": ["edge_id", "dst"]},
        {"edge_type": "*", "select": ["node_id"]},
    )
    for result in (records, records_where, links):
        assert result.isError is True, _result_text(result)
        error = _result_payload(result)["error"]
        assert error["code"] == "invalid_path_edge_type"
        assert error["data"]["closest_edge_types"] == ["first"]
        assert "0 of 0" not in _result_text(result)
    assert star.isError is False, _result_text(star)
    assert "n:middle" in _result_text(star) and "n:a" in _result_text(star)


def test_a_one_hop_attrs_key_select_is_served_as_on_a_path(clean_runtime: str) -> None:
    """`attrs.<key>` on one hop returns that attribute of each linked record,
    as a path select does (issue #3666, lane RD; it used to be refused)."""
    _publish_path_fixture(clean_runtime)
    (served,) = _expand_calls(
        clean_runtime, {"edge_type": "first", "select": ["node_id", "attrs.text"]},
    )
    assert served.isError is False, _result_text(served)
    rows = _result_payload(served)["neighbors"]
    assert rows, _result_text(served)
    for row in rows:
        assert set(row["attrs"]) <= {"text", "nomos_redacted"}, row
        assert "provenance" in row


def test_a_catalog_with_no_narrowings_refuses_no_edge_type(clean_runtime: str) -> None:
    """With no narrowing the catalog has no edge-type registry: any name is
    read as given on one hop, as in a path (the writer's own rule)."""
    _one_page_catalog(clean_runtime, [])
    named, other, star = _expand_calls(
        clean_runtime, {"edge_type": "t00"}, {"edge_type": "anything"}, {"edge_type": "*"},
    )
    for result in (named, other, star):
        assert result.isError is False, _result_text(result)
    assert [row["node_id"] for row in _result_payload(named)["neighbors"]] == ["n:end"]
    assert _result_payload(other)["neighbors"] == []
    assert [row["node_id"] for row in _result_payload(star)["neighbors"]] == ["n:end"]


def _redacted_hub(tmp_path, dsn: str) -> str:
    """ticket:hub contains comment:vis (readable) and comment:masked (redacted
    for the reader; comment text is free_text, so it is dropped, not masked)."""
    from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes
    from tests.substrate.mcp.test_search_records import (
        MASKED, NARROWINGS, OPEN, READER, SUBTYPES, _activate, _edge, _node,
    )

    deployment = make_search_deployment(tmp_path, dsn=dsn)
    publish_nodes(
        dsn, deployment, subtypes=SUBTYPES, narrowings=NARROWINGS, nodes=[],
        edges=[_edge("ticket:hub", "comment:vis"), _edge("ticket:hub", "comment:masked")],
        node_groups=[
            (OPEN, [_node("ticket:hub", "ticket", "hub ticket"),
                    _node("comment:vis", "comment", "visible comment")]),
            (MASKED, [_node("comment:masked", "comment", "secret comment")]),
        ],
    )
    _activate(dsn, deployment, [
        {"rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
         "subjects": [READER], "match": {"source_id": MASKED}, "priority": 200,
         "obligations": [{"type": "redaction"}, {"type": "audit"}]},
        {"rule_id": "allow_read", "effect": "allow",
         "actions": ["graph.read.mcp", "mcp.tool.call"], "subjects": [READER],
         "priority": 100},
    ])
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || jsonb_build_object('pii_classes', jsonb_build_object('text', 'free_text'))"
            " WHERE name = 'comment'",
        )
    return deployment


def test_an_attrs_key_select_keeps_the_redaction_marker_under_access_rules(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failure case: a redacted record whose text the access rules drop must
    not answer select=["attrs.text"] with an empty attrs, which reads as "no
    text". The redaction marker is kept beside the keys named. A path serves
    records under audit; under enforce it serves none, by design
    (visibility_limited), which this test pins too."""
    from tests.substrate.mcp.test_search_records import READER

    deployment = _redacted_hub(tmp_path, clean_runtime)
    call = {"path": ["contains:out"], "select": ["node_id", "attrs.text"]}
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    (keyed,) = _expand_calls(clean_runtime, call, deployment=deployment, start="ticket:hub")
    assert keyed.isError is False, _result_text(keyed)
    rows = {row["node_id"]: row for row in _result_payload(keyed)["results"]}
    assert sorted(rows) == ["comment:masked", "comment:vis"], _result_text(keyed)[:3000]
    assert rows["comment:masked"]["attrs"] == {"nomos_redacted": True}
    assert list(rows["comment:vis"]["attrs"]) == ["text"]
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    (enforced,) = _expand_calls(clean_runtime, call, deployment=deployment, start="ticket:hub")
    payload = _result_payload(enforced)
    assert payload["results"] == [] and payload["outcome"]["code"] == "visibility_limited"
    # Withheld, never counted: a count here would say how many rows hide.
    assert payload["total_count"] is None
    assert payload["count_state"] == "withheld"


def test_a_bare_path_hop_takes_the_calls_direction_through_the_served_tool(
    clean_runtime: str,
) -> None:
    """Lane RD: path=["*"] and a bare edge type are read with the call's own
    direction and say so; under direction="both" a bare hop is refused by
    name and never built as '*:both'; a refused call carries no hop note."""
    _publish_path_fixture(clean_runtime)
    star, bare, two, both, later = _expand_calls(
        clean_runtime,
        {"path": ["*"], "select": ["node_id"]},
        {"path": ["first"], "select": ["node_id"]},
        {"path": ["first", "second"], "select": ["node_id"]},
        {"path": ["*"], "direction": "both", "select": ["node_id"]},
        {"path": ["first", ":out"], "select": ["node_id"]},
    )
    (inward,) = _expand_calls(
        clean_runtime, {"path": ["first"], "direction": "in", "select": ["node_id"]},
        start="n:middle",
    )
    for result in (star, bare, two, inward):
        assert result.isError is False, _result_text(result)
    assert [row["node_id"] for row in _result_payload(star)["results"]] == [
        "n:a", "n:b", "n:c", "n:middle",
    ]
    assert {"rule": "path_hop_direction", "sent": "path hop '*'",
            "served": "'*:out'"} in _result_payload(star)["adjusted"]
    assert [row["node_id"] for row in _result_payload(bare)["results"]] == [
        "n:a", "n:b", "n:c", "n:middle",
    ]
    assert [row["node_id"] for row in _result_payload(two)["results"]] == ["n:terminal"]
    assert len([
        record for record in _result_payload(two)["adjusted"]
        if record["rule"] == "path_hop_direction"
    ]) == 2
    assert [row["node_id"] for row in _result_payload(inward)["results"]] == ["n:start"]
    assert {"rule": "path_hop_direction", "sent": "path hop 'first'",
            "served": "'first:in'"} in _result_payload(inward)["adjusted"]
    for refused in (both, later):
        assert refused.isError is True
        error = _result_payload(refused)["error"]
        assert error["code"] == "invalid_path_grammar", error
        assert "operator_execution_failed" not in _result_text(refused)
        assert all(
            record.get("rule") != "path_hop_direction"
            for record in _result_payload(refused).get("adjusted", [])
        )
    both_error = _result_payload(both)["error"]
    assert both_error["data"]["requested_hop"] == "*"
    assert "direction='both' does not give it one" in both_error["fix_hint"]
