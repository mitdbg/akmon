"""Several parent subtypes declare the same passage child (issue #2753).

example-kb chunks hang off `documentation_page`, not okg's own `document`. Once a
second parent subtype declares `okg_passage_rollup` naming the same child
subtype and edge type, every chunk used to come back `ambiguous_declaration`
(both declared) or `declared_but_unlinked` (only `document` declared), although
a live declared edge reached each one.

The rule under test: a child's parent is chosen by the subtype of the node its
edge ACTUALLY reaches, among the declared `(parent_subtype, edge_type)` pairs.
Nothing is inferred from paths, titles or names; an edge from an undeclared
subtype is still no parent (fail closed); a chunk with parents of two declared
types is `multiple_parents`, never a choice.

The same rule applies to `max_per_parent` diversity, which groups rows by the
parent it resolves, and to `rank_by="passages"`, which ranks one declared
parent subtype through its own declaration.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.passage_rollup import (
    PARENT_STATE_BOUNDED_UNKNOWN,
    PARENT_STATE_CANDIDATE,
    PARENT_STATE_MULTIPLE_PARENTS,
    PARENT_STATE_RESOLVED,
    PARENT_STATE_UNLINKED,
    resolve_passage_parents,
)
from okg.substrate.mcp.search_profiles import resolve_scope
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes


pytestmark = pytest.mark.postgres


TERM = "hafnium"
_ROLLUP = {"edge_type": "contains", "child_subtype": "chunk"}

DOC = SubtypeSpec(
    "doc", metadata={"searchable_text_fields": ["text"], "passage_rollup": _ROLLUP},
)
PAGE = SubtypeSpec(
    "page", metadata={"searchable_text_fields": ["text"], "passage_rollup": _ROLLUP},
)
#: `page` WITHOUT the declaration: the single-declaration catalog of today's
#: example-kb, where only okg's own `doc` declares the relation.
PAGE_UNDECLARED = SubtypeSpec("page", metadata={"searchable_text_fields": ["text"]})
#: Never declares a relation; a `wiki --contains--> chunk` edge is not a
#: passage relation and must never become a parent.
WIKI = SubtypeSpec("wiki", metadata={"searchable_text_fields": ["text"]})
CHUNK = SubtypeSpec("chunk", metadata={"searchable_text_fields": ["text"]})
NARROWINGS = [
    ("doc", "contains", "chunk"),
    ("page", "contains", "chunk"),
    ("wiki", "contains", "chunk"),
]


def _node(node_id: str, subtype: str, **attrs) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype=subtype,
        attrs=dict(attrs),
        source_record_id={"id": node_id},
        source_revision={"v": 1},
    )


def _edge(src: str, dst: str) -> EdgeFact:
    return EdgeFact(
        src=src,
        dst=dst,
        edge_type="contains",
        source_record_id={"id": f"{src}|contains|{dst}"},
        source_revision={"v": 1},
    )


def _corpus() -> tuple[list[NodeFact], list[EdgeFact]]:
    nodes = [
        _node("doc:alpha", "doc", text="Alpha report.",
              citation_label="Alpha Report"),
        _node("chunk:alpha:0", "chunk", text=f"Alpha {TERM} passage."),
        _node("page:beta", "page", text="Beta page.",
              citation_label="Beta Page", title="Beta operations page"),
        _node("chunk:beta:0", "chunk", text=f"Beta {TERM} {TERM} {TERM} one."),
        _node("chunk:beta:1", "chunk", text=f"Beta {TERM} {TERM} two."),
        _node("doc:gamma", "doc", text="Gamma report."),
        _node("page:gamma", "page", text="Gamma page."),
        _node("chunk:shared", "chunk", text=f"Shared {TERM} passage."),
        _node("wiki:delta", "wiki", text="Delta wiki."),
        _node("chunk:delta:0", "chunk", text=f"Delta {TERM} {TERM} {TERM} one."),
        _node("chunk:delta:1", "chunk", text=f"Delta {TERM} {TERM} two."),
        _node("chunk:orphan", "chunk", text=f"Orphan {TERM} passage."),
    ]
    edges = [
        _edge("doc:alpha", "chunk:alpha:0"),
        _edge("page:beta", "chunk:beta:0"),
        _edge("page:beta", "chunk:beta:1"),
        _edge("doc:gamma", "chunk:shared"),
        _edge("page:gamma", "chunk:shared"),
        _edge("wiki:delta", "chunk:delta:0"),
        _edge("wiki:delta", "chunk:delta:1"),
    ]
    return nodes, edges


def _publish(tmp_path, dsn: str, *, page: SubtypeSpec) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    nodes, edges = _corpus()
    publish_nodes(
        dsn,
        deployment,
        subtypes=[DOC, page, WIKI, CHUNK],
        narrowings=NARROWINGS,
        nodes=nodes,
        edges=edges,
    )
    return deployment


def _search(dsn: str, deployment: str, **kwargs) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        # Row-level parent assertions: `self`, unless a test names a mode.
        kwargs.setdefault("rank_by", "self")
        return search(conn, session, TERM, method="lexical", limit=50, **kwargs)


def _parents(page: dict) -> dict:
    return {row["node_id"]: row["parent"] for row in page["results"]}


# ---------- parent resolution on the search card ----------

def test_each_chunk_resolves_to_the_declared_parent_its_edge_reaches(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = _publish(tmp_path, clean_runtime, page=PAGE)
    parents = _parents(_search(clean_runtime, deployment))

    beta = parents["chunk:beta:0"]
    assert beta["state"] == PARENT_STATE_RESOLVED, beta
    assert beta["node_id"] == "page:beta"
    assert beta["subtype"] == "page"
    assert beta["edge_type"] == "contains"
    # The citation is the parent's own producer attr, fenced by its trust.
    assert "Beta Page" in beta["citation"]["label"]

    alpha = parents["chunk:alpha:0"]
    assert alpha["state"] == PARENT_STATE_RESOLVED, alpha
    assert (alpha["node_id"], alpha["subtype"]) == ("doc:alpha", "doc")


def test_parents_of_two_declared_types_are_candidates_never_a_choice(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = _publish(tmp_path, clean_runtime, page=PAGE)
    shared = _parents(_search(clean_runtime, deployment))["chunk:shared"]

    assert shared["state"] == PARENT_STATE_MULTIPLE_PARENTS, shared
    assert shared["node_id"] is None
    assert shared["citation"] is None
    # The outer block names no single subtype: the candidates differ.
    assert shared["subtype"] is None
    assert shared["edge_type"] == "contains"
    assert [(p["node_id"], p["subtype"], p["state"]) for p in shared["parents"]] == [
        ("doc:gamma", "doc", PARENT_STATE_CANDIDATE),
        ("page:gamma", "page", PARENT_STATE_CANDIDATE),
    ]


def test_an_edge_from_an_undeclared_subtype_is_no_parent_with_two_declarations(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = _publish(tmp_path, clean_runtime, page=PAGE)
    parents = _parents(_search(clean_runtime, deployment))

    for node_id in ("chunk:delta:0", "chunk:orphan"):
        block = parents[node_id]
        assert block["state"] == PARENT_STATE_UNLINKED, (node_id, block)
        assert block["node_id"] is None
        assert block["citation"] is None
        # Two relations were declared; the block names both instead of
        # pretending one of them was the relation looked for.
        assert block["subtype"] is None
        assert block["edge_type"] == "contains"
        assert [d["parent_subtype"] for d in block["declarations"]] == [
            "doc", "page",
        ]


def test_the_envelope_lists_every_declaration_for_a_shared_child(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = _publish(tmp_path, clean_runtime, page=PAGE)
    envelope = _search(clean_runtime, deployment)["parent_relation"]

    assert envelope["child_subtypes"]["chunk"] == {
        "declarations": [
            {"parent_subtype": "doc", "edge_type": "contains"},
            {"parent_subtype": "page", "edge_type": "contains"},
        ],
    }
    # alpha, beta:0, beta:1 resolve; shared, delta x2 and orphan do not.
    assert envelope["rows_with_declared_relation"] == 7
    assert envelope["rows_resolved"] == 3


def test_with_one_declaration_an_undeclared_parent_subtype_stays_unlinked(
    clean_runtime: str, tmp_path,
) -> None:
    """Today's example-kb catalog: only `doc` declares. Fail closed, unchanged."""
    deployment = _publish(tmp_path, clean_runtime, page=PAGE_UNDECLARED)
    parents = _parents(_search(clean_runtime, deployment))

    for node_id in ("chunk:beta:0", "chunk:delta:0"):
        block = parents[node_id]
        assert block["state"] == PARENT_STATE_UNLINKED, (node_id, block)
        assert block["node_id"] is None
        assert block["subtype"] == "doc"
        assert "declarations" not in block
    # The shared chunk has exactly one DECLARED parent reachable.
    shared = parents["chunk:shared"]
    assert shared["state"] == PARENT_STATE_RESOLVED, shared
    assert shared["node_id"] == "doc:gamma"


def test_a_saturated_scan_with_two_declarations_reports_unknown(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.bm25_node_search import (
        branch_point,
        resolve_node_text_bm25_index,
    )
    from okg.substrate.mcp.search import _citation_from_attrs

    deployment = _publish(tmp_path, clean_runtime, page=PAGE)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        segment, _index = resolve_node_text_bm25_index(conn)
        blocks, envelope = resolve_passage_parents(
            conn,
            session=session,
            page=[
                {"node_id": "chunk:alpha:0", "subtype": "chunk"},
                {"node_id": "chunk:beta:0", "subtype": "chunk"},
            ],
            node_segment=segment,
            branch_point_value=branch_point(conn, session),
            citation_from_attrs=_citation_from_attrs,
            withhold_passage_counts=False,
            edge_scan_cap=1,
        )

    assert envelope["edge_scan_cap_reached"] is True
    # Neither child is decided: the one edge returned may be only part of its
    # child's edges, so no negative and no parent is claimed.
    assert {block["state"] for block in blocks.values()} == {
        PARENT_STATE_BOUNDED_UNKNOWN,
    }, blocks
    assert all(block["node_id"] is None for block in blocks.values())


def test_inspect_resolves_the_same_parent_under_two_declarations(
    clean_runtime: str, tmp_path,
) -> None:
    """inspect(node) reads the same declarations and the same rule as search."""
    from okg.substrate.mcp.tools import inspect_node

    deployment = _publish(tmp_path, clean_runtime, page=PAGE)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        beta = inspect_node(conn, session, "chunk:beta:0")
        shared = inspect_node(conn, session, "chunk:shared")
        delta = inspect_node(conn, session, "chunk:delta:0")

    assert beta["parent"]["state"] == PARENT_STATE_RESOLVED
    assert (beta["parent"]["node_id"], beta["parent"]["subtype"]) == (
        "page:beta", "page",
    )
    assert shared["parent"]["state"] == PARENT_STATE_MULTIPLE_PARENTS
    assert [p["node_id"] for p in shared["parent"]["parents"]] == [
        "doc:gamma", "page:gamma",
    ]
    assert delta["parent"]["state"] == PARENT_STATE_UNLINKED
    assert delta["parent"]["node_id"] is None


# ---------- rank_by="passages" over a second declared parent subtype ----------

def test_rank_by_passages_ranks_each_declared_parent_subtype_separately(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = _publish(tmp_path, clean_runtime, page=PAGE)
    pages = _search(
        clean_runtime, deployment, filter={"subtype": "page"}, rank_by="passages",
    )
    docs = _search(
        clean_runtime, deployment, filter={"subtype": "doc"}, rank_by="passages",
    )

    assert sorted(row["node_id"] for row in pages["results"]) == [
        "page:beta", "page:gamma",
    ]
    assert {row["subtype"] for row in pages["results"]} == {"page"}
    assert sorted(row["node_id"] for row in docs["results"]) == [
        "doc:alpha", "doc:gamma",
    ]


# ---------- max_per_parent diversity ----------

def _diverse(dsn: str, deployment: str) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = resolve_scope(
            conn,
            catalog_version_id=session.catalog_version_id,
            config=None,
            requested_profile=None,
            requested_max_per_parent=1,
            deployment_name=session.deployment_name,
        )
        return search(
            conn, session, TERM, method="lexical", rank_by="self", limit=50, scope=scope,
        )


def test_diversity_groups_chunks_under_the_second_declared_parent(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = _publish(tmp_path, clean_runtime, page=PAGE)
    page = _diverse(clean_runtime, deployment)
    kept = [row["node_id"] for row in page["results"]]

    # page:beta keeps its best chunk only.
    assert ("chunk:beta:0" in kept) != ("chunk:beta:1" in kept), kept
    # The wiki-linked chunks have no DECLARED parent, so neither is suppressed.
    assert "chunk:delta:0" in kept and "chunk:delta:1" in kept, kept
    assert page["suppressed_sibling_count"] == 1


def test_diversity_never_groups_by_an_undeclared_parent_subtype(
    clean_runtime: str, tmp_path,
) -> None:
    """One declaration (`doc`): page and wiki edges create no parent at all."""
    deployment = _publish(tmp_path, clean_runtime, page=PAGE_UNDECLARED)
    page = _diverse(clean_runtime, deployment)
    kept = [row["node_id"] for row in page["results"]]

    for node_id in ("chunk:beta:0", "chunk:beta:1", "chunk:delta:0", "chunk:delta:1"):
        assert node_id in kept, (node_id, kept)
    assert page["suppressed_sibling_count"] == 0
    # alpha and shared resolve to doc parents; nothing else does.
    assert page["diversity_parents_resolved"] == 2


# ---------- diversity reads the parent node itself ----------
#
# Diversity groups a row under a parent only when that parent NODE is live and
# readable at the pin. A live, readable edge is not enough: a parent the caller
# may not read, or one retired at the pin, must never decide which visible row
# is suppressed.

HIDDEN_PRODUCER = "connector:hidden_pages"
_FIXTURE_PRODUCER = "system:analyst_search_fixture"
_READER = "subject:diversity-reader"
_OPERATOR = "subject:diversity-operator"


def _publish_with_hidden_producer(tmp_path, dsn: str) -> str:
    """The same corpus, with `page:beta` published by its own producer."""
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    nodes, edges = _corpus()
    publish_nodes(
        dsn,
        deployment,
        subtypes=[DOC, PAGE, WIKI, CHUNK],
        narrowings=NARROWINGS,
        nodes=[fact for fact in nodes if fact.node_id != "page:beta"],
        edges=edges,
        node_groups=[
            (HIDDEN_PRODUCER, [f for f in nodes if f.node_id == "page:beta"]),
        ],
    )
    return deployment


def _activate_deny_hidden_producer(dsn: str, deployment: str) -> None:
    from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph

    bundle = compile_policy_graph(
        {
            "policy_bundle_id": "nomos:test:diversity-hidden-parent",
            "policy_version": "v1",
            "default_effect": "deny",
            "rules": [
                {
                    "rule_id": "deny_hidden_pages_for_reader",
                    "effect": "deny",
                    "actions": ["graph.read.mcp"],
                    "subjects": [_READER],
                    "match": {"source_id": HIDDEN_PRODUCER},
                    "priority": 300,
                },
                *[
                    {
                        "rule_id": f"allow_fixture_source_{index}",
                        "effect": "allow",
                        "actions": ["graph.read.mcp"],
                        "subjects": [_READER, _OPERATOR],
                        "match": {"source_id": source},
                        "priority": 100,
                    }
                    for index, source in enumerate(
                        (HIDDEN_PRODUCER, _FIXTURE_PRODUCER)
                    )
                ],
            ],
        }
    )
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=f"deployment:{deployment}",
            activated_by="test:operator",
        )
        conn.commit()


def _diverse_as(dsn: str, deployment: str, subject: str) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(
            conn, deployment_name=deployment, nomos_subject=subject,
        )
        scope = resolve_scope(
            conn,
            catalog_version_id=session.catalog_version_id,
            config=None,
            requested_profile=None,
            requested_max_per_parent=1,
            deployment_name=session.deployment_name,
        )
        return search(
            conn, session, TERM, method="lexical", rank_by="self", limit=50, scope=scope,
        )


def test_diversity_never_groups_under_a_parent_the_caller_cannot_read(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = _publish_with_hidden_producer(tmp_path, clean_runtime)
    _activate_deny_hidden_producer(clean_runtime, deployment)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    operator = _diverse_as(clean_runtime, deployment, _OPERATOR)
    reader = _diverse_as(clean_runtime, deployment, _READER)
    operator_kept = [row["node_id"] for row in operator["results"]]
    reader_kept = [row["node_id"] for row in reader["results"]]

    # The control: the operator reads page:beta, so its two chunks are
    # siblings and one is suppressed.
    assert ("chunk:beta:0" in operator_kept) != ("chunk:beta:1" in operator_kept)
    assert operator["suppressed_sibling_count"] == 1
    # The reader may not read page:beta: both chunks stay, nothing is
    # suppressed, and the hidden parent is not counted.
    assert "chunk:beta:0" in reader_kept and "chunk:beta:1" in reader_kept
    assert reader["suppressed_sibling_count"] == 0
    assert "page:beta" not in str(reader)
    assert reader["diversity_parents_resolved"] == (
        operator["diversity_parents_resolved"] - 1
    )


def _live_counts(conn, session) -> tuple[int, int]:
    """(live edges from page:beta, live page:beta nodes) at the pin."""
    from okg.substrate.bm25_node_search import (
        branch_point,
        resolve_node_text_bm25_index,
    )
    from okg.substrate.mcp.passage_rollup import resolve_edge_segment

    segment, _index = resolve_node_text_bm25_index(conn)
    bp = branch_point(conn, session)
    live = "deleted = FALSE AND live_lo <= %(bp)s AND %(bp)s < live_hi"
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {resolve_edge_segment(conn)} "
            f"WHERE src = 'page:beta' AND {live}",
            {"bp": bp},
        )
        edges = int(cur.fetchone()[0])
        cur.execute(
            f"SELECT count(*) FROM {segment} "
            f"WHERE node_id = 'page:beta' AND {live}",
            {"bp": bp},
        )
        nodes = int(cur.fetchone()[0])
    return edges, nodes


def test_diversity_never_groups_under_a_parent_retired_at_the_pin(
    clean_runtime: str, tmp_path,
) -> None:
    """The parent node is retired while its edges stay live."""
    from okg.substrate.graph_store import DEFAULT_BRANCH, ChronosGraphStore
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    deployment = _publish(tmp_path, clean_runtime, page=PAGE)
    before = _diverse(clean_runtime, deployment)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        # The public wrapper always retires incident edges with a node; the
        # writer underneath can keep them, which is the state under test.
        super(ChronosGraphStore, store).apply_facts(
            [
                NodeFact(
                    node_id="page:beta",
                    subtype="page",
                    attrs={},
                    source_record_id={"id": "page:beta"},
                    source_revision={"v": 2},
                    op="R",
                )
            ],
            branch=DEFAULT_BRANCH,
            source=_FIXTURE_PRODUCER,
            strict_endpoints=False,
            delete_incident_edges=False,
        )
    Publisher(
        clean_runtime,
        deployment,
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        # The precondition this test exists for: edges live, parent not.
        assert _live_counts(conn, session) == (2, 0)

    after = _diverse(clean_runtime, deployment)
    kept = [row["node_id"] for row in after["results"]]
    assert before["suppressed_sibling_count"] == 1
    assert "chunk:beta:0" in kept and "chunk:beta:1" in kept, kept
    assert after["suppressed_sibling_count"] == 0
