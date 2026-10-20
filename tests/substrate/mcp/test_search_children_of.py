"""`scope={"children_of": id, "edge_type": optional}` on lexical search.

Issue #3363 (PACT search-children-of-3363). The scope covers the records a
parent reaches by one outgoing edge type (unset: the parent's declared
passage relation), never the parent itself, under the record scope's access
rules. Each requirement with its failure case:

* the default relation returns the parent's matching passages, each ranked
  by its own text, and not the parent nor another record's passages;
* an explicit `edge_type` returns those children;
* a parent with no passage relation and no `edge_type`, any rank mode but
  `self` (a fold could land outside the scope), and a list, windows or
  retained read are refused by name;
* under Nomos enforce a hidden parent answers exactly as a missing one even
  when it has a readable child, and a hidden child is neither returned nor
  betrayed by the page's signals; `audit` and `off` hide nothing.

The corpus: `folder:f` holds `ticket:in` (by `holds`); `ticket:out` is
outside it; each ticket contains comments (its declared passages). The
folder, both tickets and every comment carry the query words, so a scope
that leaked would show.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.search_where import (
    ChildrenScope,
    SearchScopeUnsupported,
    normalize_children_scope,
    normalize_record_scope,
)
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes
from tests.substrate.mcp.test_search_records import _activate, _ids

pytestmark = pytest.mark.postgres

QUERY = "zirconium cladding"
VISIBLE = "connector:visible"
SECRET = "connector:secret"
READER = "subject:children-reader"

SUBTYPES = [
    SubtypeSpec("folder", metadata={"searchable_text_fields": ["text"]}),
    SubtypeSpec("ticket", metadata={
        "searchable_text_fields": ["text"],
        "passage_rollup": {"edge_type": "contains", "child_subtype": "comment"},
    }),
    SubtypeSpec("comment", metadata={"searchable_text_fields": ["text"]}),
]
NARROWINGS = [("folder", "holds", "ticket"), ("ticket", "contains", "comment")]


def _node(node_id: str, subtype: str, text: str) -> NodeFact:
    return NodeFact(
        node_id=node_id, subtype=subtype, attrs={"text": text},
        source_record_id={"id": node_id}, source_revision={"v": 1},
    )


def _edge(src: str, dst: str, edge_type: str = "contains") -> EdgeFact:
    return EdgeFact(
        src=src, dst=dst, edge_type=edge_type,
        source_record_id={"id": f"{src}|{edge_type}|{dst}"}, source_revision={"v": 1},
    )


def _deployment(tmp_path, dsn: str) -> str:
    visible = [
        _node("folder:f", "folder", f"Folder about {QUERY}."),
        _node("ticket:in", "ticket", f"Ticket in the folder on {QUERY}."),
        _node("comment:in:0", "comment", f"{QUERY} measured in the folder."),
        _node("ticket:out", "ticket", f"Ticket outside on {QUERY}."),
        _node("comment:out:0", "comment", f"{QUERY} measured outside."),
        _node("ticket:quiet", "ticket", "A quiet ticket in the folder."),
        _node("comment:quiet:0", "comment", f"{QUERY} under the quiet ticket."),
        # Readable, under a hidden parent: scoping to that parent must not
        # serve it.
        _node("comment:underhidden", "comment", f"{QUERY} under the hidden ticket."),
    ]
    secret = [
        _node("ticket:hidden", "ticket", f"Hidden ticket on {QUERY}."),
        _node("comment:hidden:0", "comment", f"{QUERY} xenolith under the hidden ticket."),
        _node("comment:in:secret", "comment", f"{QUERY} xenolith beside the visible one."),
    ]
    edges = [
        _edge("folder:f", "ticket:in", "holds"),
        _edge("folder:f", "ticket:quiet", "holds"),
        _edge("folder:f", "ticket:hidden", "holds"),
        _edge("ticket:in", "comment:in:0"),
        _edge("ticket:in", "comment:in:secret"),
        _edge("ticket:out", "comment:out:0"),
        _edge("ticket:quiet", "comment:quiet:0"),
        _edge("ticket:hidden", "comment:hidden:0"),
        _edge("ticket:hidden", "comment:underhidden"),
    ]
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    publish_nodes(
        dsn, deployment, subtypes=SUBTYPES, narrowings=NARROWINGS,
        nodes=[], edges=edges,
        node_groups=[(VISIBLE, visible), (SECRET, secret)],
    )
    _activate(dsn, deployment, [{
        "rule_id": "allow_visible", "effect": "allow",
        "actions": ["graph.read.mcp"], "subjects": [READER],
        "match": {"source_id": VISIBLE}, "priority": 100,
    }, {
        "rule_id": "allow_main", "effect": "allow",
        "actions": ["graph.read.mcp"], "subjects": [READER],
        "match": {"source_id": "system:analyst_search_fixture"}, "priority": 100,
    }])
    return deployment


@pytest.fixture
def corpus(clean_runtime, tmp_path):
    return clean_runtime, _deployment(tmp_path, clean_runtime)


def _search(corpus, scope, query: str = QUERY, **kwargs) -> dict:
    dsn, deployment = corpus
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment, nomos_subject=READER)
        return search(conn, session, query, method="lexical", limit=50,
                      record_scope=scope, **kwargs)


_SIGNAL_KEYS = (
    "total_count", "zero_hit_reason", "zero_hit_scanned", "subtype_indexed",
    "rank_capped", "total_count_basis", "truncated", "next_offset", "results",
)


def _signals(page: dict) -> dict:
    return {key: page.get(key) for key in _SIGNAL_KEYS}


# ---------- argument shape (no database) ----------

def test_children_of_parses_and_record_forms_are_unchanged():
    assert normalize_children_scope({"children_of": "a"}) == ChildrenScope("a")
    assert normalize_children_scope(
        {"children_of": "a", "edge_type": "holds"}) == ChildrenScope("a", "holds")
    assert normalize_record_scope({"children_of": "a"}) is None
    assert normalize_children_scope({"record": "a"}) is None
    assert normalize_record_scope({"record": "a"}) == ["a"]
    assert ChildrenScope("a", "holds").applied() == {
        "children_of": "a", "edge_type": "holds"}


@pytest.mark.parametrize("scope", [
    {"children_of": ""},
    {"children_of": "a", "record": "b"},
    {"children_of": "a", "edge_type": ""},
    {"children_of": ["a"]},
])
def test_a_malformed_children_of_is_refused(scope):
    with pytest.raises(SearchScopeUnsupported):
        normalize_children_scope(scope)
    with pytest.raises(SearchScopeUnsupported):
        normalize_record_scope(scope)


# ---------- served behaviour ----------

@pytest.mark.parametrize("mode", ["off", "audit"])
def test_the_default_relation_returns_the_parents_own_passages(corpus, monkeypatch, mode):
    """Off and audit hide nothing: both of `ticket:in`'s comments return,
    ranked by their own text; the ticket and `ticket:out`'s comment do not."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    page = _search(corpus, {"children_of": "ticket:in"})
    assert page["rank_by"] == "self"
    assert sorted(_ids(page)) == ["comment:in:0", "comment:in:secret"]
    assert page["scope_applied"] == {"children_of": "ticket:in"}
    # Failure case: unscoped, the parent and other records answer too.
    unscoped = _ids(_search(corpus, None, rank_by="self"))
    assert {"ticket:in", "comment:out:0"} <= set(unscoped)


def test_an_edge_type_returns_those_children_and_never_the_parent(corpus, monkeypatch):
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    scope = {"children_of": "folder:f", "edge_type": "holds"}
    page = _search(corpus, scope)
    # The quiet ticket's own text misses; the folder matches but is the parent.
    assert sorted(_ids(page)) == ["ticket:hidden", "ticket:in"]
    assert page["rank_by"] == "self"
    assert page["scope_applied"] == scope


def test_a_relation_the_parent_does_not_use_scopes_to_nothing(corpus, monkeypatch):
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    page = _search(corpus, {"children_of": "ticket:in", "edge_type": "holds"})
    assert page["results"] == []


@pytest.mark.parametrize("scope,kwargs", [
    # The folder declares no passage relation: an edge type is needed.
    ({"children_of": "folder:f"}, {}),
    # A fold could land outside the scope: on the excluded parent, or on a
    # hidden child through its readable passage.
    ({"children_of": "ticket:in"}, {"rank_by": "records"}),
    ({"children_of": "folder:f", "edge_type": "holds"}, {"rank_by": "records"}),
    ({"children_of": "ticket:in"}, {"rank_by": "passages",
                                    "filter": {"subtype": "ticket"}}),
    # Views and references that hold records.
    ({"children_of": "ticket:in"}, {"delivery_view": "list"}),
    ({"children_of": "ticket:in"}, {"reference_members": True}),
])
def test_unserved_children_of_calls_are_refused_by_name(corpus, monkeypatch, scope, kwargs):
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    with pytest.raises(SearchScopeUnsupported):
        _search(corpus, scope, **kwargs)


def test_children_of_is_lexical_cards_only(corpus):
    dsn, deployment = corpus
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(SearchScopeUnsupported):
            search(conn, session, QUERY, method="alias",
                   record_scope={"children_of": "ticket:in"})


# ---------- Nomos enforce ----------

@pytest.mark.parametrize("edge", [{}, {"edge_type": "contains"}, {"edge_type": "holds"}])
def test_a_hidden_parent_scopes_exactly_as_a_missing_one(corpus, monkeypatch, edge):
    """`ticket:hidden` is hidden but holds the readable `comment:underhidden`:
    the page reads as a scope on an id that names nothing, and neither the
    readable child nor a missing-relation refusal gives the parent away."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    hidden = _search(corpus, {"children_of": "ticket:hidden", **edge})
    missing = _search(corpus, {"children_of": "ticket:nothing", **edge})
    assert hidden["results"] == []
    assert _signals(hidden) == _signals(missing)
    # Control: the readable child is served to the same reader unscoped.
    assert "comment:underhidden" in _ids(_search(corpus, None, rank_by="self"))
    assert _ids(_search(corpus, {"children_of": "ticket:in"}))


def test_hidden_children_are_not_returned_nor_counted(corpus, monkeypatch):
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _search(corpus, {"children_of": "ticket:in"}, completeness="exact")
    assert _ids(page) == ["comment:in:0"]
    assert page["total_count"] == 1
    folder = _search(corpus, {"children_of": "folder:f", "edge_type": "holds"})
    assert _ids(folder) == ["ticket:in"]
    assert "xenolith" not in str(folder) and "xenolith" not in str(page)

    # A word only the hidden children hold answers as a word nobody holds.
    def signals(query: str, scope: dict, **kwargs) -> dict:
        found = _search(corpus, scope, query, **kwargs)
        assert found["results"] == []
        return _signals(found)

    for scope in (
        {"children_of": "ticket:in"},
        {"children_of": "folder:f", "edge_type": "holds"},
    ):
        assert signals("xenolith", scope) == signals("qwertyuiop", scope)
        assert signals("xenolith", scope, completeness="exact") == signals(
            "qwertyuiop", scope, completeness="exact")


# ---------- through the served tool ----------

def test_the_served_tool_defaults_children_of_to_self(corpus, okg_mcp_can_log_in, monkeypatch):
    """The tool resolves `rank_by`'s default before `search()` runs; without
    the children rule there it made `records` (found by the live call on the
    C4 copy)."""
    from okg.substrate.mcp import server as srv

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    dsn, deployment = corpus
    mcp = srv.build_server(dsn=dsn, deployment_name=deployment)
    tool = mcp._tool_manager._tools["search"].fn
    try:
        page = tool(query=QUERY, scope={"children_of": "ticket:in"}, limit=10)
        assert page["rank_by"] == "self"
        assert sorted(_ids(page)) == ["comment:in:0", "comment:in:secret"]
        assert page["scope_applied"] == {"children_of": "ticket:in"}
        # A list and a retained read hold records: refused by name.
        for kwargs in ({"view": "list"}, {"retain": True}, {"view": "windows"},
                       {"rank_by": "records"}):
            with pytest.raises(SearchScopeUnsupported):
                tool(query=QUERY, scope={"children_of": "ticket:in"}, **kwargs)
    finally:
        mcp._okg_pool.close()
