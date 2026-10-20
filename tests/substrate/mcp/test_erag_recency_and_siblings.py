"""Recency annotations and sibling surfacing, loudly absent where the graph
has nothing.

PACT ``recover-passage-rollup-reads``. Both affordances must distinguish
"nothing to return" from "no such structure here", because an empty result
cannot be told apart from either.

Two facts from the corpus this was written against shape the tests. 42.0 % of
`fireflies` records (4,271 of 10,173) are dated AFTER today and the corpus runs
to 2029-12-11, so a wall-clock plausibility test would call almost half a
source implausible and a clamp would delete real data — hence the future-dated
assertions below. And a source-formality AUTHORITY prior is measurably
BACKWARDS here (`qst_0428`: the confluence "finalized playbook" holds the
superseded numbers, the drive "working draft" holds the gold), so no such prior
is applied and the envelope says so as a value rather than by omission.
"""

from __future__ import annotations

import psycopg
import pytest

from okg.substrate.bm25_node_search import (
    branch_point,
    resolve_node_text_bm25_index,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.passage_rollup import resolve_edge_segment
from okg.substrate.mcp.read_affordances import (
    RECENCY_ATTRIBUTE,
    RECENCY_DERIVATION,
    sibling_disclosure,
    sibling_relations,
)
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.erag_rollup_fixture import (
    NARROWINGS,
    SUBTYPES,
    TERM,
    build_facts,
)
from tests.substrate.mcp.search_fixture import (
    make_search_deployment,
    publish_nodes,
)


pytestmark = pytest.mark.postgres


#: A record dated years past "today". On the real corpus 42 % of one source
#: looks like this; it must be annotated, never clamped and never dropped.
FUTURE_DATE = "2029-12-11T00:00:00Z"
PAST_DATE = "2024-01-05T00:00:00Z"
MID_DATE = "2026-06-01T00:00:00Z"

DATED = SubtypeSpec(
    "dated_doc",
    metadata={
        "searchable_text_fields": ["text"],
    },
)
OTHER = SubtypeSpec(
    "other_doc",
    metadata={"searchable_text_fields": ["text"]},
)
#: The self-relations a versioned-pair / near-dup enricher will declare. The
#: fixture declares them WITHOUT emitting edges first, so the
#: "declared but nothing produced them yet" state is exercised.
DATED_NARROWINGS = [
    ("dated_doc", "near_duplicate_of", "dated_doc"),
    ("dated_doc", "supersedes", "dated_doc"),
    # The same edge archetype is also valid for a different child subtype.
    # Sibling reads must spend neither their page budget nor their diagnostic
    # count budget on this distinct narrowing triple.
    ("dated_doc", "near_duplicate_of", "other_doc"),
]


def _publish_undated(dsn: str, tmp_path) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    facts = build_facts()
    publish_nodes(
        dsn,
        deployment,
        subtypes=SUBTYPES,
        narrowings=NARROWINGS,
        nodes=[f for f in facts if hasattr(f, "node_id")],
        edges=[f for f in facts if not hasattr(f, "node_id")],
    )
    return deployment


def _dated_node(node_id: str, authored_at: str | None) -> NodeFact:
    # The note's own id ends its text: a records-ranked lexical search folds
    # rows with identical text into one (#3465), and each note must stay a row.
    marker = node_id.rsplit(":", 1)[-1]
    attrs = {"text": f"A dated note about {TERM} cladding, filed as {marker}."}
    if authored_at is not None:
        attrs[RECENCY_ATTRIBUTE] = authored_at
    return NodeFact(
        node_id=node_id,
        subtype=DATED.name,
        attrs=attrs,
        source_record_id={"id": node_id},
        source_revision={"v": 1},
    )


def _publish_dated(dsn: str, tmp_path, *, edges: bool) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    nodes = [
        _dated_node("dated:past", PAST_DATE),
        _dated_node("dated:mid", MID_DATE),
        _dated_node("dated:future", FUTURE_DATE),
        _dated_node("dated:undated", None),
        NodeFact(
            node_id="aaa:other",
            subtype=OTHER.name,
            attrs={"text": "A different record sharing one edge archetype."},
            source_record_id={"id": "aaa:other"},
            source_revision={"v": 1},
        ),
    ]
    edge_facts = []
    if edges:
        edge_facts = [
            EdgeFact(
                src="dated:past",
                dst="dated:mid",
                edge_type="supersedes",
                source_record_id={"id": "e1"},
                source_revision={"v": 1},
            ),
            EdgeFact(
                src="dated:mid",
                dst="dated:future",
                edge_type="near_duplicate_of",
                source_record_id={"id": "e2"},
                source_revision={"v": 1},
            ),
            EdgeFact(
                src="dated:past",
                dst="dated:future",
                edge_type="near_duplicate_of",
                source_record_id={"id": "e3"},
                source_revision={"v": 1},
            ),
            EdgeFact(
                src="dated:mid",
                dst="aaa:other",
                edge_type="near_duplicate_of",
                source_record_id={"id": "e4"},
                source_revision={"v": 1},
            ),
        ]
    publish_nodes(
        dsn,
        deployment,
        subtypes=[DATED, OTHER],
        narrowings=DATED_NARROWINGS,
        nodes=nodes,
        edges=edge_facts,
    )
    return deployment


# ---------- recency ----------


def test_recency_is_loudly_absent_when_the_graph_carries_nothing(
    clean_runtime: str,
    tmp_path,
) -> None:
    """No `authored_at` anywhere ⇒ the page SAYS so, naming the attribute.

    This is the honest behaviour on the current graph: no node carries the
    attribute, so nothing is annotated. An omitted key would read as "recency
    was considered and did not apply", which is the silent state forbidden.
    """
    deployment = _publish_undated(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        payload = search(
            conn,
            session,
            TERM,
            method="lexical",
            filter={"subtype": "doc"},
            rank_by="passages",
            limit=10,
        )

    block = payload["recency"]
    assert block["attribute"] == RECENCY_ATTRIBUTE
    assert block["derivation"] == RECENCY_DERIVATION
    assert block["annotated"] is False
    assert block["rows_on_page"] > 0
    assert block["rows_annotated"] == 0
    assert RECENCY_ATTRIBUTE in block["absent_reason"]
    # The two forbidden mechanisms are reported as values, so a reader can
    # CHECK they were not used rather than trusting an absence.
    assert block["wall_clock_compared"] is False
    assert block["authority_prior_applied"] is False
    assert block["basis"] == "corpus_relative"
    # No row carries a fabricated annotation.
    assert all("recency" not in row for row in payload["results"]), payload


def test_recency_lights_up_with_no_code_change_when_the_attribute_lands(
    clean_runtime: str,
    tmp_path,
) -> None:
    """The same code path annotates once the graph carries the attribute.

    And it annotates ONLY the rows that carry it: an undated row beside dated
    ones gets no annotation rather than an imputed one.
    """
    deployment = _publish_dated(clean_runtime, tmp_path, edges=False)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        payload = search(
            conn,
            session,
            TERM,
            method="lexical",
            filter={"subtype": DATED.name},
            limit=10,
        )

    block = payload["recency"]
    assert block["annotated"] is True
    assert block["rows_annotated"] == 3
    assert block["rows_on_page"] == 4
    assert block["corpus_span_lo"] == PAST_DATE
    assert block["corpus_span_hi"] == FUTURE_DATE
    assert block["corpus_span_scanned"] == "all"

    by_id = {row["node_id"]: row for row in payload["results"]}
    assert by_id["dated:undated"].get("recency") is None
    for node_id in ("dated:past", "dated:mid", "dated:future"):
        annotation = by_id[node_id]["recency"]
        assert annotation["attribute"] == RECENCY_ATTRIBUTE
        assert annotation["derivation"] == RECENCY_DERIVATION
        assert annotation["outside_corpus_span"] is False


def test_a_future_dated_record_is_annotated_not_clamped(
    clean_runtime: str,
    tmp_path,
) -> None:
    """Corpus-relative, never wall-clock.

    The corpus's own maximum IS the future-dated record, so it sits at the top
    of the span rather than being flagged implausible. A wall-clock band would
    reject it; a clamp would silently rewrite it.
    """
    deployment = _publish_dated(clean_runtime, tmp_path, edges=False)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        payload = search(
            conn,
            session,
            TERM,
            method="lexical",
            filter={"subtype": DATED.name},
            limit=10,
        )

    by_id = {row["node_id"]: row for row in payload["results"]}
    future = by_id["dated:future"]["recency"]
    # Value preserved byte-for-byte: no clamp, no normalization to "today".
    assert future["value"] == FUTURE_DATE
    assert future["corpus_span_position"] == pytest.approx(1.0)
    assert future["corpus_span_position_basis"] == "measured"
    past = by_id["dated:past"]["recency"]
    mid = by_id["dated:mid"]["recency"]
    assert past["corpus_span_position"] == pytest.approx(0.0)
    # Strictly between the endpoints, from the measured span — not a band.
    assert 0.0 < mid["corpus_span_position"] < 1.0
    assert payload["recency"]["wall_clock_compared"] is False
    # And no annotation carries a source-formality signal of any kind.
    assert set(future) == {
        "attribute",
        "derivation",
        "value",
        "corpus_span_position",
        "corpus_span_position_basis",
        "outside_corpus_span",
    }


def test_recency_span_filters_rows_before_aggregation(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreadable date cannot move or disclose the corpus endpoints."""
    deployment = _publish_dated(clean_runtime, tmp_path, edges=False)
    from okg.substrate.mcp import read_affordances as affordances

    real_filter = affordances.filter_node_rows_for_session

    def deny_future(conn, session, rows):
        return [
            row
            for row in real_filter(conn, session, rows)
            if row["node_id"] != "dated:future"
        ]

    monkeypatch.setattr(
        affordances,
        "filter_node_rows_for_session",
        deny_future,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        payload = search(
            conn,
            session,
            TERM,
            method="lexical",
            filter={"subtype": DATED.name},
            limit=10,
        )

    assert payload["recency"]["corpus_span_hi"] == MID_DATE
    assert payload["recency"]["corpus_span_rows"] == 2
    assert payload["recency"]["corpus_span_hi"] != FUTURE_DATE


def test_recency_span_compares_instants_and_discloses_incomparable_forms() -> None:
    """Offset spelling never determines time order; naive/aware never mix."""
    from okg.substrate.mcp import read_affordances as affordances

    earlier_but_lexically_later = "2024-01-01T00:30:00+01:00"
    later_but_lexically_earlier = "2023-12-31T23:45:00+00:00"
    assert affordances._timestamp_span(
        [earlier_but_lexically_later, later_but_lexically_earlier]
    ) == (
        earlier_but_lexically_later,
        later_but_lexically_earlier,
        "measured_instants",
    )
    assert affordances._timestamp_span(
        ["2024-01-01T00:00:00", "2024-01-02T00:00:00Z"]
    ) == (None, None, "mixed_timezone_awareness")
    assert affordances._span_position(
        "2024-01-01T12:00:00Z",
        "2024-01-01T00:00:00",
        "2024-01-02T00:00:00Z",
    ) == (None, "mixed_timezone_awareness")


# ---------- siblings ----------


def test_no_declared_relations_is_distinct_from_no_neighbours(
    clean_runtime: str,
    tmp_path,
) -> None:
    """A subtype with no self-relation reports the ONTOLOGY gap, not silence.

    On the current erag-seg catalog this is `document`'s real state: reference,
    near-duplicate and versioned-pair edges are absent by declaration, so no
    ingest and no query can produce one until the ontology declares them.
    """
    deployment = _publish_undated(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        assert (
            sibling_relations(
                conn,
                catalog_version_id=session.catalog_version_id,
                subtype="doc",
            )
            == []
        )
        block = sibling_disclosure(
            conn,
            session=session,
            node_id="doc:sharp",
            subtype="doc",
            catalog_version_id=session.catalog_version_id,
            node_segment=resolve_node_text_bm25_index(conn)[0],
            edge_segment=resolve_edge_segment(conn),
            branch_point_value=branch_point(conn, session),
        )

    assert block["state"] == "no_declared_relations"
    assert block["neighbours"] == []
    assert "ONTOLOGY" in block["reason"]
    # Containment is excluded by the rule itself, not by a special case: the
    # doc HAS `contains` children and they are not siblings.
    assert block["declared_relations"] == []


def test_declared_but_empty_names_the_structure_that_produced_nothing(
    clean_runtime: str,
    tmp_path,
) -> None:
    """Declared relations with no edges yet report a THIRD, separable state.

    `graph_edge_counts` is what separates "this node has no sibling" from
    "nothing in the graph has emitted that edge" — the first is a fact about
    the node, the second is a gap in the pipeline, and only the second is a
    defect.
    """
    deployment = _publish_dated(clean_runtime, tmp_path, edges=False)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        block = sibling_disclosure(
            conn,
            session=session,
            node_id="dated:mid",
            subtype=DATED.name,
            catalog_version_id=session.catalog_version_id,
            node_segment=resolve_node_text_bm25_index(conn)[0],
            edge_segment=resolve_edge_segment(conn),
            branch_point_value=branch_point(conn, session),
        )

    assert block["state"] == "declared_but_empty"
    assert sorted(rel["edge_type"] for rel in block["declared_relations"]) == [
        "near_duplicate_of",
        "supersedes",
    ]
    assert block["graph_edge_counts"] == {
        "near_duplicate_of": 0,
        "supersedes": 0,
    }
    assert block["graph_edge_counts_complete"] == {
        "near_duplicate_of": True,
        "supersedes": True,
    }
    assert block["neighbours"] == []


def test_siblings_are_returned_once_the_edges_exist(
    clean_runtime: str,
    tmp_path,
) -> None:
    """The same code path surfaces neighbours when the structure lands.

    Both directions are reported, because a versioned pair is a relation
    between two documents and the caller anchored on either one wants the
    other.
    """
    deployment = _publish_dated(clean_runtime, tmp_path, edges=True)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        block = sibling_disclosure(
            conn,
            session=session,
            node_id="dated:mid",
            subtype=DATED.name,
            catalog_version_id=session.catalog_version_id,
            node_segment=resolve_node_text_bm25_index(conn)[0],
            edge_segment=resolve_edge_segment(conn),
            branch_point_value=branch_point(conn, session),
        )

    assert block["state"] == "neighbours"
    found = {
        (n["edge_type"], n["neighbour_id"], n["direction"]) for n in block["neighbours"]
    }
    assert ("supersedes", "dated:past", "in") in found, found
    assert ("near_duplicate_of", "dated:future", "out") in found, found
    assert block["graph_edge_counts"] == {
        # Two dated_doc -> dated_doc edges; the third edge with this name has
        # child subtype other_doc and is outside the declared sibling triple.
        "near_duplicate_of": 2,
        "supersedes": 1,
    }
    assert block["graph_edge_counts_complete"] == {
        "near_duplicate_of": True,
        "supersedes": True,
    }
    assert block["reason"] is None


def test_shared_edge_type_does_not_spend_the_sibling_page_budget(
    clean_runtime: str,
    tmp_path,
) -> None:
    """A different narrowing with the same edge name cannot hide a sibling."""
    deployment = _publish_dated(clean_runtime, tmp_path, edges=True)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        block = sibling_disclosure(
            conn,
            session=session,
            node_id="dated:mid",
            subtype=DATED.name,
            catalog_version_id=session.catalog_version_id,
            node_segment=resolve_node_text_bm25_index(conn)[0],
            edge_segment=resolve_edge_segment(conn),
            branch_point_value=branch_point(conn, session),
            limit=1,
        )

    assert [row["neighbour_id"] for row in block["neighbours"]] == [
        "dated:future"
    ]
    assert "aaa:other" not in {
        row["neighbour_id"] for row in block["neighbours"]
    }


def test_graph_production_counts_are_bounded_per_declared_triple(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A saturated diagnostic is a lower bound, never an unbounded count."""
    deployment = _publish_dated(clean_runtime, tmp_path, edges=True)
    from okg.substrate.mcp import read_affordances as affordances

    monkeypatch.setattr(affordances, "_SIBLING_GRAPH_COUNT_CAP", 1)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        block = sibling_disclosure(
            conn,
            session=session,
            node_id="dated:mid",
            subtype=DATED.name,
            catalog_version_id=session.catalog_version_id,
            node_segment=resolve_node_text_bm25_index(conn)[0],
            edge_segment=resolve_edge_segment(conn),
            branch_point_value=branch_point(conn, session),
        )

    assert block["graph_edge_count_cap"] == 1
    assert block["graph_edge_counts"] == {
        "near_duplicate_of": 1,
        "supersedes": 1,
    }
    assert block["graph_edge_counts_complete"] == {
        "near_duplicate_of": False,
        "supersedes": True,
    }


def test_unreadable_sibling_nodes_are_removed_before_disclosure(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A declared edge is not a side door around the node read filter."""
    deployment = _publish_dated(clean_runtime, tmp_path, edges=True)
    from okg.substrate.mcp import read_affordances as affordances

    real_filter = affordances.filter_node_rows_for_session

    def deny_future(conn, session, rows):
        return [
            row
            for row in real_filter(conn, session, rows)
            if row["node_id"] != "dated:future"
        ]

    monkeypatch.setattr(
        affordances,
        "filter_node_rows_for_session",
        deny_future,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        block = sibling_disclosure(
            conn,
            session=session,
            node_id="dated:mid",
            subtype=DATED.name,
            catalog_version_id=session.catalog_version_id,
            node_segment=resolve_node_text_bm25_index(conn)[0],
            edge_segment=resolve_edge_segment(conn),
            branch_point_value=branch_point(conn, session),
        )

    assert [row["neighbour_id"] for row in block["neighbours"]] == ["dated:past"]


def test_sibling_page_does_not_claim_false_empty_after_a_filtered_prefix(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bounded hidden first edge reports uncertainty, not exact emptiness."""
    deployment = _publish_dated(clean_runtime, tmp_path, edges=True)
    from okg.substrate.mcp import read_affordances as affordances

    real_filter = affordances.filter_node_rows_for_session

    def deny_future(conn, session, rows):
        return [
            row
            for row in real_filter(conn, session, rows)
            if row["node_id"] != "dated:future"
        ]

    monkeypatch.setattr(
        affordances,
        "filter_node_rows_for_session",
        deny_future,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        block = sibling_disclosure(
            conn,
            session=session,
            node_id="dated:mid",
            subtype=DATED.name,
            catalog_version_id=session.catalog_version_id,
            node_segment=resolve_node_text_bm25_index(conn)[0],
            edge_segment=resolve_edge_segment(conn),
            branch_point_value=branch_point(conn, session),
            limit=1,
        )

    assert block["state"] == "bounded_unknown"
    assert block["neighbours"] == []
    assert block["selection_complete"] is False
    assert block["truncated"] is True


def test_policy_hidden_siblings_do_not_become_a_graph_empty_oracle(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No readable candidate under active authority yields only uncertainty."""
    deployment = _publish_dated(clean_runtime, tmp_path, edges=True)
    from okg.substrate.mcp import read_affordances as affordances

    monkeypatch.setattr(
        affordances,
        "deployment_requires_nomos_runtime",
        lambda _deployment: True,
    )
    monkeypatch.setattr(
        affordances,
        "filter_node_rows_for_session",
        lambda _conn, _session, _rows: [],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        block = sibling_disclosure(
            conn,
            session=session,
            node_id="dated:mid",
            subtype=DATED.name,
            catalog_version_id=session.catalog_version_id,
            node_segment=resolve_node_text_bm25_index(conn)[0],
            edge_segment=resolve_edge_segment(conn),
            branch_point_value=branch_point(conn, session),
        )

    assert block["state"] == "authority_bounded_unknown"
    assert block["neighbours"] == []
    assert block["selection_complete"] is False
    assert block["graph_edge_counts"] is None
    assert block["graph_edge_counts_complete"] is None
    assert "hidden structure" in block["reason"]


def test_expand_surfaces_the_sibling_block_through_the_real_tool(
    clean_runtime: str,
    tmp_path,
) -> None:
    """Routed through the `expand` MCP tool, not the helper underneath it."""
    deployment = _publish_dated(clean_runtime, tmp_path, edges=True)
    from okg.substrate.mcp.server import build_server

    mcp = build_server(
        dsn=clean_runtime,
        deployment_name=deployment,
        auto_advance="always",
    )
    try:
        expand = mcp._tool_manager._tools["expand"].fn
        without = expand(input={"node_id": "dated:mid"}, limit=5)
        with_siblings = expand(
            input={"node_id": "dated:mid"},
            limit=5,
            include_siblings=True,
        )
        filtered_with_siblings = expand(
            input={"node_id": "dated:mid"},
            order_by="node_id",
            limit=5,
            include_siblings=True,
        )
    finally:
        mcp._okg_pool.close()

    # Opt-in: the block is absent unless asked for, so no existing caller's
    # response shape changes.
    assert "siblings" not in without, without
    assert with_siblings["siblings"]["state"] == "neighbours"
    assert with_siblings["siblings"]["subtype"] == DATED.name
    assert filtered_with_siblings["siblings"]["state"] == "neighbours"


def test_recency_page_measurement_does_not_read_a_bm25_index_it_lacks(
    clean_runtime: str,
    tmp_path,
) -> None:
    """The corpus span is bounded and disclosed, never an unbounded aggregate.

    Guards the cost property: the span is measured off the node segment with a
    LIMIT, and the block always says whether it saw everything.
    """
    deployment = _publish_dated(clean_runtime, tmp_path, edges=False)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        # The segment the span reads is the same one the ranker reads.
        segment, _ = resolve_node_text_bm25_index(conn)
        assert segment
        payload = search(
            conn,
            session,
            TERM,
            method="lexical",
            filter={"subtype": DATED.name},
            limit=10,
        )

    block = payload["recency"]
    assert block["corpus_span_scanned"] in ("all", "sample")
    assert block["corpus_span_rows"] == 3
    assert block["corpus_span_subtypes"] == [DATED.name]
