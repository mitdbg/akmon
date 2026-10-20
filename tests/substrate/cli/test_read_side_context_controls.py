from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.cli.main import main
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import open_session
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _seed_catalog_version(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now())"
        )


def _seed_nodes(conn: psycopg.Connection, nodes: list[NodeFact]) -> None:
    ChronosGraphStore.from_connection(conn).apply_facts(nodes)


def _search_node(
    node_id: str,
    *,
    text: str | None = None,
    category: str = "kept",
) -> NodeFact:
    # Each node gets its own text: a lexical records page collapses rows of
    # equal type and text into one (PACT lexical-duplicate-collapse-3465).
    return NodeFact(
        node_id=node_id,
        subtype="page",
        attrs={
            "text": text or f"alpha context efficient cli page {node_id}",
            "title": node_id,
            "category": category,
        },
        source_record_id={"id": node_id},
        source_revision={"v": 1},
    )


def _last_stdout_json(capsys: pytest.CaptureFixture[str]) -> dict:
    out = capsys.readouterr()
    text = out.out.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return json.loads(text.splitlines()[-1])


def _last_stderr_json(capsys: pytest.CaptureFixture[str]) -> dict:
    out = capsys.readouterr()
    text = out.err.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return json.loads(text.splitlines()[-1])


def test_search_cli_exposes_page_select_filter_and_generation_pin(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _seed_nodes(conn, [
            _search_node("n:cli:01"),
            _search_node("n:cli:02"),
            _search_node("n:cli:03"),
            _search_node("n:cli:04"),
            _search_node("n:cli:drop", category="drop"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        pinned = open_session(conn, deployment_name="default").generation_id
        _seed_nodes(conn, [_search_node("n:cli:new")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    rc = main([
        "--json",
        "search",
        "--deployment", "default",
        "--dsn", clean_runtime,
        "--query", "alpha context",
        "--method", "lexical",
        "--subtype", "page",
        "--attr-filter", "category=kept",
        "--generation-id", pinned,
        "--limit", "2",
        "--offset", "1",
        "--select", "node_id,subtype,excerpt",
        "--completeness", "exact",
    ])

    assert rc == 0
    payload = _last_stdout_json(capsys)
    assert payload["completeness"] == "exact"
    assert payload["pinned_generation_id"] == pinned
    assert payload["limit"] == 2
    assert payload["offset"] == 1
    assert payload["next_offset"] == 3
    assert payload["total_count"] == 4
    assert len(payload["results"]) == 2
    assert {row["node_id"] for row in payload["results"]}.isdisjoint(
        {"n:cli:drop", "n:cli:new"},
    )
    for row in payload["results"]:
        assert "excerpt" in row
        assert "attrs" not in row


def test_search_cli_invalid_select_names_valid_fields(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _seed_nodes(conn, [_search_node("n:cli:select")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    rc = main([
        "--json",
        "search",
        "--deployment", "default",
        "--dsn", clean_runtime,
        "--query", "alpha",
        "--method", "lexical",
        "--select", "bogus",
    ])

    assert rc == 1
    err = _last_stderr_json(capsys)["error"]
    assert err["code"] == "select_unknown_field"
    assert err["data"]["unknown_fields"] == ["bogus"]
    assert "attrs" in err["data"]["valid_fields"]
    assert "excerpt" in err["data"]["valid_fields"]


def test_trace_node_reports_chronos_graph_presence_when_fact_log_empty(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        target = NodeFact(
            node_id="n:trace:chronos-only",
            subtype="page",
            attrs={
                "text": "trace should preview this Chronos graph node",
                "title": "Chronos trace fixture",
                "minted_from": "source-row:jes",
                "producer_id": "agent:trace",
                "nomos_source_lineage": {
                    "source_id": "cooper.aliases",
                    "policy_fingerprint": "policy:trace",
                },
            },
            source_record_id={"row": "jes"},
            source_revision={"v": 2},
        )
        neighbors = [
            NodeFact(
                node_id=f"n:trace:neighbor:{index:02d}",
                subtype="page",
                attrs={"title": f"Neighbor {index}"},
                source_record_id={"row": f"neighbor-{index}"},
                source_revision={"v": 1},
            )
            for index in range(27)
        ]
        edges = [
            EdgeFact(
                src=neighbor.node_id,
                dst=target.node_id,
                edge_type="grounds",
                attrs={"linker": "experimental-grounder"},
                provenance="derived_deterministic",
                source_record_id={"row": f"grounding-{index}"},
                source_revision={"v": 1},
            )
            for index, neighbor in enumerate(neighbors)
        ]
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [target, *neighbors],
            source="fixture:trace",
            run_id="trace-run",
        )
        store.apply_facts(
            edges,
            source="_enricher:experimental-grounder",
            run_id="trace-run",
        )
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    rc = main([
        "--json",
        "trace",
        "node",
        "n:trace:chronos-only",
        "--dsn", clean_runtime,
    ])

    assert rc == 0
    payload = _last_stdout_json(capsys)
    assert payload["facts"] == []
    assert payload["diagnostic"] == "fact_log_empty_graph_node_present"
    assert payload["graph_node"]["present"] is True
    assert payload["graph_node"]["subtype"] == "page"
    assert "text_excerpt" in payload["graph_node"]
    graph_node = payload["graph_node"]
    assert "replacement_command" not in graph_node
    assert graph_node["derivation_story_edge_cap"] == 25
    assert graph_node["derivation_story_edge_total"] == 27
    assert graph_node["derivation_story_truncated"] is True
    story = graph_node["derivation_story"]
    assert [step["ordinal"] for step in story] == list(range(len(story)))
    assert story[0]["kind"] == "node_origin"
    assert story[0]["minted_from"] == "source-row:jes"
    assert story[0]["lineage_attrs"]["producer_id"] == "agent:trace"
    assert story[1]["kind"] == "source_lineage"
    assert story[2]["kind"] == "adjacent_edge"
    assert story[2]["direction"] == "in"
    assert story[2]["provenance"] == "derived_deterministic"
    assert story[2]["lineage_attrs"]["linker"] == "experimental-grounder"


def test_trace_preview_reports_edge_read_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli.main import _trace_graph_node_preview

    class FailingGraph:
        calls = 0

        def query(self, _query: str, _params: dict) -> list[dict]:
            self.calls += 1
            if self.calls == 1:
                return [{
                    "node_id": "n:trace:edge-failure",
                    "subtype": "page",
                    "attrs": {},
                    "source": "proof",
                    "source_record_id": {},
                    "source_revision": {},
                    "run_id": "run:proof",
                    "observed_at": None,
                }]
            raise RuntimeError("edge read unavailable")

    class FailingStore:
        def checkout(self) -> FailingGraph:
            return FailingGraph()

    monkeypatch.setattr(
        ChronosGraphStore,
        "from_connection",
        lambda _conn, ensure_schema=False: FailingStore(),
    )

    preview = _trace_graph_node_preview(None, "n:trace:edge-failure")

    assert preview["present"] is None
    assert preview["diagnostic"] == "chronos_derivation_unavailable"
    assert preview["node_id"] == "n:trace:edge-failure"
    assert preview["error"] == "edge read unavailable"
    assert "replacement_command" in preview
