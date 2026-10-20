"""A path `where` reaches a passage through its record outside the profile.

Issue #3466 (PACT path-through-record-3466). In the C4 comp-ops audit a
keyword search with a path prefix returned nothing under the deployment's
content profile, which leaves out `document`: the matching chunks' document
lies outside the profile, so the `where` could not read the document's path
(its catalog declaration was not loaded) and the fold dropped every chunk.
Each requirement with its failure case:

* under a profile without the record type, a chunk whose record passes the
  path `where` stays its own row, and one whose record fails it does not;
* a record the profile excludes by path, a record the reader may not see,
  and a readable record whose path is masked still drop their passages;
* `rank_by="self"` judges each row by itself, as before;
* without a profile the record itself answers, as before.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.search_profiles import ResolvedScope, excluded_by_path_globs
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes
from tests.substrate.mcp.test_search_records import _activate, _ids

pytestmark = pytest.mark.postgres

QUERY = "reaperCount"
PREFIX = {"op": "prefix", "field": "path", "value": "apps/prod/"}
VISIBLE = "connector:visible"
SECRET = "connector:secret"
MASKED = "connector:masked"
READER = "subject:path-reader"

SUBTYPES = [
    SubtypeSpec("doc", metadata={
        "searchable_text_fields": ["text"], "path_from": "path",
        "passage_rollup": {"edge_type": "contains", "child_subtype": "chunk"},
    }),
    SubtypeSpec("chunk", metadata={"searchable_text_fields": ["text"]}),
    SubtypeSpec("note", metadata={"searchable_text_fields": ["text"], "path_from": "path"}),
]
NARROWINGS = [("doc", "contains", "chunk")]
#: The deployment's content profile: passages and notes, no documents.
CONTENT = ResolvedScope(profile="content", subtypes=("chunk", "note"))


def _node(node_id: str, subtype: str, text: str, **attrs) -> NodeFact:
    return NodeFact(
        node_id=node_id, subtype=subtype, attrs={"text": text, **attrs},
        source_record_id={"id": node_id}, source_revision={"v": 1},
    )


def _edge(src: str, dst: str) -> EdgeFact:
    return EdgeFact(
        src=src, dst=dst, edge_type="contains",
        source_record_id={"id": f"{src}|contains|{dst}"}, source_revision={"v": 1},
    )


@pytest.fixture
def corpus(clean_runtime, tmp_path):
    visible = [
        _node("doc:in", "doc", "A production file.", path="apps/prod/a.yaml"),
        _node("chunk:in:0", "chunk", f"{QUERY}: 4 in production."),
        _node("doc:out", "doc", "A base file.", path="apps/base/b.yaml"),
        _node("chunk:out:0", "chunk", f"{QUERY}: 2 in base."),
        _node("note:in", "note", f"{QUERY} noted.", path="apps/prod/n.md"),
        _node("chunk:hid:0", "chunk", f"{QUERY}: 9 under a hidden file."),
        _node("chunk:mask:0", "chunk", f"{QUERY}: 7 under a masked path."),
    ]
    secret = [_node("doc:hid", "doc", "A hidden production file.", path="apps/prod/h.yaml")]
    masked = [_node("doc:mask", "doc", "A masked production file.", path="apps/prod/m.yaml")]
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment, subtypes=SUBTYPES, narrowings=NARROWINGS, nodes=[],
        edges=[_edge("doc:in", "chunk:in:0"), _edge("doc:out", "chunk:out:0"),
               _edge("doc:hid", "chunk:hid:0"), _edge("doc:mask", "chunk:mask:0")],
        node_groups=[(VISIBLE, visible), (SECRET, secret), (MASKED, masked)],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"path\": \"free_text\"}}'::jsonb"
            " WHERE name = 'doc'"
        )
    _activate(clean_runtime, deployment, [
        {"rule_id": f"allow_{i}", "effect": "allow", "actions": ["graph.read.mcp"],
         "subjects": [READER], "match": {"source_id": source}, "priority": 100}
        for i, source in enumerate((VISIBLE, "system:analyst_search_fixture"))
    ] + [{
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }])
    return clean_runtime, deployment


def _search(corpus, *, scope=CONTENT, where=PREFIX, **kwargs) -> dict:
    dsn, deployment = corpus
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment, nomos_subject=READER)
        return search(conn, session, QUERY, method="lexical", limit=50,
                      scope=scope, where=where, **kwargs)


def test_a_chunk_matches_through_its_record_outside_the_profile(corpus, monkeypatch):
    """Failure case on dev: only `note:in` came back; `chunk:in:0` was lost."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    page = _search(corpus)
    assert sorted(_ids(page)) == ["chunk:hid:0", "chunk:in:0", "chunk:mask:0", "note:in"]
    assert page["where_applied"] == "path prefix apps/prod/"
    # A prefix no record carries admits nothing.
    assert _ids(_search(corpus, where={"op": "prefix", "field": "path",
                                       "value": "nothere/"})) == []
    # Control: without the `where` every matching chunk answers.
    assert {"chunk:in:0", "chunk:out:0"} <= set(_ids(_search(corpus, where=None)))


def test_a_record_excluded_by_path_still_drops_its_passages(corpus, monkeypatch):
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    excluded = ("apps/prod/a.yaml",)
    assert excluded_by_path_globs({"path": "apps/prod/a.yaml"}, excluded)
    scope = ResolvedScope(profile="content", subtypes=("chunk", "note"),
                          excluded_path_globs=excluded)
    assert "chunk:in:0" not in _ids(_search(corpus, scope=scope))


def test_a_hidden_record_does_not_lend_its_path(corpus, monkeypatch):
    """`doc:hid` is hidden under enforce: its readable chunk must not match
    through a path the reader may not read."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    ids = _ids(_search(corpus))
    assert "chunk:hid:0" not in ids
    assert "chunk:in:0" in ids


def test_a_masked_record_path_does_not_admit_its_passage(corpus, monkeypatch):
    """`doc:mask` is readable but its path is masked under enforce: the path
    reads as unknown, so its chunk is not admitted through it."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    ids = _ids(_search(corpus))
    assert "chunk:mask:0" not in ids
    assert "chunk:in:0" in ids
    # Control: the masked document itself is readable without the `where`.
    assert "chunk:mask:0" in _ids(_search(corpus, where=None))


def test_self_judges_each_row_by_itself(corpus, monkeypatch):
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    assert _ids(_search(corpus, rank_by="self")) == ["note:in"]


def test_without_a_profile_the_record_answers(corpus, monkeypatch):
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    page = _search(corpus, scope=None)
    by_id = {row["node_id"]: row for row in page["results"]}
    assert {"doc:in", "doc:hid", "doc:mask", "note:in"} == set(by_id)
    assert by_id["doc:in"]["rollup_best_chunk_id"] == "chunk:in:0"
