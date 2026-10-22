"""The deployment's citation id on every read row (PACT read-names-list-3666).

Issue #3666, bash-vs-okg finding 2: the id a benchmark answer cites (ERAG's
`dsid`) was in none of 873 text reads, and agents spent a read per record to
select it. A deployment names the attribute once (`citation_attr`); search
rows, grep list rows and inspect node and text reads then carry it.
"""
from __future__ import annotations

import json
from pathlib import Path

import anyio
import psycopg
import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes

# --------------------------------------------------------------------------- #
# (a) manifest and pure                                                        #
# --------------------------------------------------------------------------- #


def _write_manifest(root: Path, **extra) -> str:
    deployment = root / "cite-deploy"
    (deployment / "schemas").mkdir(parents=True)
    (deployment / "source_registry.yaml").write_text("{}\n", encoding="utf-8")
    manifest = {"name": "cite-deploy", "schema_dir": "schemas",
                "source_registry": "source_registry.yaml", **extra}
    (deployment / "deployment.yaml").write_text(json.dumps(manifest) + "\n", encoding="utf-8")
    return str(deployment)


def test_the_manifest_names_one_attribute_or_refuses_at_load(tmp_path):
    from okg.substrate.deployments import DeploymentConfigError, resolve_deployment

    assert resolve_deployment(_write_manifest(tmp_path / "a", citation_attr="dsid")).citation_attr == "dsid"
    assert resolve_deployment(_write_manifest(tmp_path / "b")).citation_attr is None
    for bad in ("two words", 12, "", ["dsid"]):
        root = tmp_path / f"bad{abs(hash(str(bad)))}"
        with pytest.raises(DeploymentConfigError):
            resolve_deployment(_write_manifest(root, citation_attr=bad))


def test_the_field_reads_visible_attrs_and_never_shows_a_hidden_value():
    from okg.substrate.mcp.citation_id import citation_field, fenced_citation
    from okg.substrate.nomos.read_masking import MASK_TOKEN, REDACTION_MARKER

    assert citation_field({"dsid": " d-1 "}, "dsid") == {"dsid": "d-1"}
    assert citation_field({"dsid": MASK_TOKEN}, "dsid") == {"dsid": MASK_TOKEN}
    # Dropped from a redacted record: masked, never read as absent.
    assert citation_field({REDACTION_MARKER: True}, "dsid") == {"dsid": MASK_TOKEN}
    assert citation_field({}, "dsid") is None and citation_field({"dsid": ""}, "dsid") is None
    assert citation_field({"dsid": {"x": 1}}, "dsid") is None
    assert citation_field({"dsid": "d-1"}, None) is None  # no attribute declared
    trusted = fenced_citation({"dsid": "d-1"}, {"trust": "trusted", "source": "s"})
    assert trusted == {"dsid": "d-1"}
    untrusted = fenced_citation({"dsid": "d-1"}, {"trust": "untrusted", "source": "s"})
    assert untrusted["dsid"].startswith("⟦UNTRUSTED_DATA") and "d-1" in untrusted["dsid"]
    assert fenced_citation({"dsid": MASK_TOKEN}, {"trust": "untrusted"}) == {"dsid": MASK_TOKEN}


def test_text_prints_the_id_beside_the_record_id():
    from okg.substrate.mcp import result_text

    page = {"operator_name": "search", "method": "grep", "query": "x", "view": "list",
            "results": [{"node_id": "doc:1", "subtype": "doc", "citation_id": {"dsid": "d-1"},
                         "matches": 1, "provenance": {"trust": "trusted", "source": "s"}}]}
    text = result_text.render(page, tool="search")
    assert "[1] doc:1 (doc) · dsid d-1 · 1 match" in text
    node = {"operator_name": "inspect", "status": "ok", "node_id": "doc:1", "subtype": "doc",
            "citation_id": {"dsid": "d-1"}, "attrs": {"title": "t"},
            "provenance": {"trust": "trusted", "source": "s"}}
    first = result_text.render(node, tool="inspect", kind="node").split("\n", 1)[0]
    assert "doc:1 (doc) · dsid d-1" in first and "citation_id" not in first


# --------------------------------------------------------------------------- #
# (b) database: the served tools                                               #
# --------------------------------------------------------------------------- #

#: The producer `publish_nodes` writes the main nodes as (trusted below).
MAIN = "system:analyst_search_fixture"
EXTERNAL = "connector:cite_external"
READER = "subject:cite-reader"
DOC = SubtypeSpec("doc", metadata={
    "searchable_text_fields": ["text"], "title_from": "summary",
    "passage_rollup": {"edge_type": "contains", "child_subtype": "doc_chunk"},
})
DOC_CHUNK = SubtypeSpec("doc_chunk", metadata={"searchable_text_fields": ["text"]})


def _node(node_id: str, subtype: str, text: str, **attrs) -> NodeFact:
    return NodeFact(node_id=node_id, subtype=subtype, attrs={"text": text, **attrs},
                    source_record_id={"id": node_id}, source_revision={"v": 1})


def _corpus(tmp_path, dsn: str, *, cite: bool) -> str:
    from tests.substrate.mcp.test_search_list import _trust

    deployment = make_search_deployment(tmp_path, dsn=dsn)
    if cite:
        path = Path(deployment) / "deployment.yaml"
        manifest = json.loads(path.read_text(encoding="utf-8"))
        path.write_text(json.dumps({**manifest, "citation_attr": "dsid"}) + "\n", encoding="utf-8")
    publish_nodes(
        dsn, deployment, subtypes=[DOC, DOC_CHUNK],
        narrowings=[("doc", "contains", "doc_chunk")],
        nodes=[
            _node("doc:1", "doc", "wombat in the first record", summary="First", dsid="dsid_0001",
                  path="docs/one.md"),
            _node("doc:2", "doc", "wombat in the second record", summary="Second", dsid="dsid_0002"),
            _node("doc:3", "doc", "wombat with no citation id", summary="Third"),
            _node("doc_chunk:1-0", "doc_chunk", "a wombat passage of the first record"),
        ],
        edges=[EdgeFact(src="doc:1", dst="doc_chunk:1-0", edge_type="contains",
                        source_record_id={"id": "e1"}, source_revision={"v": 1})],
        node_groups=[(EXTERNAL, [_node("doc:ext", "doc", "wombat outside", summary="Outside",
                                       dsid="dsid_EXT")])],
    )
    _trust(dsn, deployment, MAIN)
    return deployment


class _Tools:
    def __init__(self, dsn: str, deployment: str) -> None:
        from okg.substrate.mcp import server as srv

        self.mcp = srv.build_server(dsn=dsn, deployment_name=deployment)

    def close(self) -> None:
        self.mcp._okg_pool.close()
        if self.mcp._okg_write_pool:
            self.mcp._okg_write_pool.close()

    def call(self, tool: str, result_format: str = "json", **arguments):
        from mcp.shared.memory import create_connected_server_and_client_session

        out: list = []

        async def drive() -> None:
            async with create_connected_server_and_client_session(self.mcp) as client:
                out.append(await client.call_tool(
                    tool, arguments, meta={"okg/result_format": result_format}))

        anyio.run(drive)
        text = out[0].content[0].text
        return json.loads(text) if result_format == "json" else text


@pytest.fixture
def cited(clean_runtime, okg_mcp_can_log_in, tmp_path):
    tools = _Tools(clean_runtime, _corpus(tmp_path, clean_runtime, cite=True))
    try:
        yield tools
    finally:
        tools.close()


def _rows(page: dict) -> dict:
    assert "results" in page, json.dumps(page)[:800]
    return {row["node_id"]: row for row in page["results"]}


@pytest.mark.postgres
@pytest.mark.parametrize("arguments", [
    {"method": "grep"},                                   # the names list
    {"method": "grep", "view": "cards"},
    {"method": "grep", "view": "cards", "select": ["node_id", "handle"]},
    {"method": "grep", "within": "record"},
    {"method": "lexical"},
    {"method": "lexical", "view": "list"},
])
def test_every_search_row_names_its_citation_id(cited, arguments):
    rows = _rows(cited.call("search", query="wombat", **arguments))
    assert rows["doc:1"]["citation_id"] == {"dsid": "dsid_0001"}, rows["doc:1"]
    assert rows["doc:2"]["citation_id"] == {"dsid": "dsid_0002"}
    assert "citation_id" not in rows["doc:3"]
    # Untrusted producer: fenced like its other text.
    assert rows["doc:ext"]["citation_id"]["dsid"].startswith("⟦UNTRUSTED_DATA")
    text = cited.call("search", "text", query="wombat", **arguments)
    first_lines = [line for line in text.split("\n") if line.startswith("[")]
    assert any(" · dsid dsid_0001" in line and "doc:1 " in line for line in first_lines), text


@pytest.mark.postgres
def test_a_lookup_with_no_query_names_it_too(cited):
    # Round 3 qst_0378: a where-only lookup row carried no dsid.
    rows = _rows(cited.call("search", where={"type": "doc"}, limit=10))
    assert rows["doc:1"]["citation_id"] == {"dsid": "dsid_0001"}
    assert "citation_id" not in rows["doc:3"]
    assert rows["doc:ext"]["citation_id"]["dsid"].startswith("⟦UNTRUSTED_DATA")
    text = cited.call("search", "text", where={"type": "doc"}, limit=10)
    assert any("doc:1 " in line and " · dsid dsid_0001" in line
               for line in text.split("\n") if line.startswith("[")), text


@pytest.mark.postgres
def test_node_and_text_reads_name_it_on_the_first_line(cited):
    node = cited.call("inspect", target={"kind": "node", "node_id": "doc:1"}, select=["node_id"])
    assert node["citation_id"] == {"dsid": "dsid_0001"}
    first = cited.call("inspect", "text", target={"kind": "node", "node_id": "doc:1"}).split("\n")[0]
    assert "doc:1 (doc) · dsid dsid_0001" in first, first
    text = cited.call("inspect", target={"kind": "text", "node_id": "doc:1"})
    assert text["citation_id"] == {"dsid": "dsid_0001"}
    first = cited.call("inspect", "text", target={"kind": "text", "node_id": "doc:1"}).split("\n")[0]
    assert "text of doc:1 (doc) · dsid dsid_0001" in first, first
    none = cited.call("inspect", target={"kind": "node", "node_id": "doc:3"})
    assert "citation_id" not in none


@pytest.mark.postgres
def test_a_deployment_that_names_none_gets_no_field(clean_runtime, okg_mcp_can_log_in, tmp_path):
    tools = _Tools(clean_runtime, _corpus(tmp_path, clean_runtime, cite=False))
    try:
        served = [
            tools.call("search", query="wombat", method="grep"),
            tools.call("search", query="wombat"),
            tools.call("inspect", target={"kind": "node", "node_id": "doc:1"}),
            tools.call("inspect", target={"kind": "text", "node_id": "doc:1"}),
        ]
        text = tools.call("search", "text", query="wombat", method="grep")
    finally:
        tools.close()
    assert "citation_id" not in json.dumps(served)
    assert " · dsid " not in text


@pytest.mark.postgres
def test_a_redacted_records_id_is_the_mask_token(clean_runtime, tmp_path, monkeypatch):
    from okg.substrate.mcp import open_session, search
    from okg.substrate.mcp.citation_id import CITATION_ATTR
    from okg.substrate.nomos.read_masking import MASK_TOKEN
    from tests.substrate.mcp.test_search_list import _activate

    deployment = _corpus(tmp_path, clean_runtime, cite=True)
    _activate(clean_runtime, deployment, [
        {"rule_id": "allow_main", "effect": "allow", "actions": ["graph.read.mcp"],
         "subjects": [READER], "match": {"source_id": MAIN}, "priority": 100},
        {"rule_id": "redact_external", "effect": "redact", "actions": ["graph.read.mcp"],
         "subjects": [READER], "match": {"source_id": EXTERNAL}, "priority": 100,
         "obligations": [{"type": "redaction"}, {"type": "audit"}]},
    ])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"dsid\": \"free_text\", \"summary\": \"free_text\","
            " \"text\": \"non_pii\"}}'::jsonb WHERE name = 'doc'"
        )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    token = CITATION_ATTR.set("dsid")
    try:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name=deployment, nomos_subject=READER)
            page = search(conn, session, "wombat", method="grep", limit=20)
    finally:
        CITATION_ATTR.reset(token)
    rows = _rows(page)
    assert rows["doc:ext"]["citation_id"] == {"dsid": MASK_TOKEN}
    assert "dsid_EXT" not in json.dumps(page)
    assert rows["doc:1"]["citation_id"] == {"dsid": "dsid_0001"}
