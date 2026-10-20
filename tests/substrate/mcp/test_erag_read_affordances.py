"""The passage→document rollup: aggregation, pagination, envelope honesty.

PACT ``recover-passage-rollup-reads``. The defect: a `document` node's
`attrs.text` is a
rendering of its own path, so `search(method="lexical",
subtype_filter="document")` is a FILENAME search. Measured on the arm-B run it
opened 20 of 25 questions that way; the gold document for `qst_0118` ranks 49th
on its filename and 1st on its body.

The fixture reproduces the shape at fixture scale — a locator parent, content
chunks, four candidate narrowings between them, and a decoy `figure` child
reachable by the SAME `contains` edge — so every claim here is tested against
the ambiguity that makes the declaration necessary rather than against a graph
where any guess would have worked.
"""

from __future__ import annotations

from dataclasses import replace

import psycopg
import pytest

from okg.substrate.bm25_node_search import (
    branch_point,
    resolve_node_text_bm25_index,
)
from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.passage_rollup import (
    PASSAGE_ROLLUP_AGGREGATION,
    PASSAGE_ROLLUP_CHUNK_CAP,
    PassageRollup,
    PassageRollupOffsetUnordered,
    passage_rollup_candidates,
    resolve_edge_segment,
)
from okg.substrate.transcript_trust import (
    UNTRUSTED_CONTENT_ORIGIN_ATTR,
    WEB_TOOL_RESULT_ORIGIN,
)
from tests.substrate.mcp.erag_rollup_fixture import (
    CHUNK,
    EXPECTED_CHUNK_COUNTS,
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


#: A word that appears ONLY in chunk bodies, never in a document path. A
#: self-ranked document search for it can never match; the rollup can.
BODY_ONLY_TERM = "cladding"

ROLLUP = PassageRollup(
    parent_subtype="doc",
    edge_type="contains",
    child_subtype="chunk",
)


#: One word per chunk, none of them a query term. Several fixture chunks carry
#: the same body, and a records-ranked lexical search folds rows with identical
#: text into one (#3465); a distinct trailing word keeps every chunk its own
#: row. Every chunk gains exactly one token, so equal-weight chunks still tie.
_CHUNK_MARKERS = (
    "alpha",
    "bravo",
    "charlie",
    "delta",
    "echo",
    "foxtrot",
    "golf",
    "hotel",
    "india",
    "juliett",
    "kilo",
    "lima",
)


def _with_unique_chunk_texts(facts: list) -> list:
    chunks = [f for f in facts if getattr(f, "subtype", None) == CHUNK.name]
    assert len(chunks) <= len(_CHUNK_MARKERS), len(chunks)
    markers = dict(zip((f.node_id for f in chunks), _CHUNK_MARKERS))
    return [
        replace(f, attrs={**f.attrs, "text": f"{f.attrs['text']} {markers[f.node_id]}"})
        if getattr(f, "node_id", None) in markers
        else f
        for f in facts
    ]


def _publish(dsn: str, tmp_path) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    facts = _with_unique_chunk_texts(build_facts())
    publish_nodes(
        dsn,
        deployment,
        subtypes=SUBTYPES,
        narrowings=NARROWINGS,
        nodes=[f for f in facts if hasattr(f, "node_id")],
        edges=[f for f in facts if not hasattr(f, "node_id")],
    )
    return deployment


def _chunk_scores(conn, session, query: str) -> dict[str, float]:
    page = search(
        conn,
        session,
        query,
        method="lexical",
        filter={"subtype": "chunk"},
        limit=200,
        select=["node_id", "score"],
    )
    return {row["node_id"]: float(row["score"]) for row in page["results"]}


def _rollup_scores(conn, session, query: str, aggregation: str):
    segment, index_name = resolve_node_text_bm25_index(conn)
    order_sql = (
        f"(attrs->>'text') <@> to_bm25query("
        f"%(q)s, '{index_name.replace(chr(39), chr(39) * 2)}')"
    )
    rows, scanned, capped = passage_rollup_candidates(
        conn,
        session=session,
        rollup=ROLLUP,
        node_segment=segment,
        edge_segment=resolve_edge_segment(conn),
        order_sql=order_sql,
        params={"q": query, "bp": branch_point(conn, session)},
        aggregation=aggregation,
    )
    return {row["node_id"]: row for row in rows}, scanned, capped


def test_the_rollup_finds_documents_a_filename_search_cannot(
    clean_runtime: str,
    tmp_path,
) -> None:
    """The headline. Body-only words are unreachable by self-ranking.

    Paired with the control that makes it non-vacuous: the same query DOES
    match chunks, so a zero self-ranked page is the locator defect and not an
    absent term.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        self_ranked = search(
            conn,
            session,
            BODY_ONLY_TERM,
            method="lexical", completeness="exact",
            filter={"subtype": "doc"},
            rank_by="self",
            limit=10,
        )
        rolled_up = search(
            conn,
            session,
            BODY_ONLY_TERM,
            method="lexical", completeness="exact",
            filter={"subtype": "doc"},
            rank_by="passages",
            limit=10,
        )
        chunks = search(
            conn,
            session,
            BODY_ONLY_TERM,
            method="lexical", completeness="exact",
            filter={"subtype": "chunk"},
            limit=10,
        )

    # Control: the word IS in the corpus, on the chunks.
    assert chunks["total_count"] > 0, chunks
    # Self-ranked, the document subtype cannot reach body-only text because
    # its attrs.text contains only the locator.
    assert self_ranked["total_count"] == 0, self_ranked
    # Rolled up, every document with a matching passage is reachable.
    assert rolled_up["total_count"] == len(EXPECTED_CHUNK_COUNTS), rolled_up
    assert {row["subtype"] for row in rolled_up["results"]} == {"doc"}


def test_max_aggregation_is_exactly_the_best_chunks_score(
    clean_runtime: str,
    tmp_path,
) -> None:
    """`max` = the parent's best chunk's score, computed, not approximated."""
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        chunk_scores = _chunk_scores(conn, session, TERM)
        rolled, _, _ = _rollup_scores(conn, session, TERM, "max")

    assert rolled, "the rollup produced no parents"
    for doc_id, row in rolled.items():
        own = [
            score
            for chunk_id, score in chunk_scores.items()
            if chunk_id.startswith(f"{doc_id}:chunk:")
        ]
        assert own, doc_id
        # Scores are negative and more negative is better, so "max relevance"
        # is the MINIMUM score.
        assert float(row["score"]) == pytest.approx(min(own)), doc_id


def test_sum_top3_aggregation_sums_exactly_the_three_best(
    clean_runtime: str,
    tmp_path,
) -> None:
    """`sum_top3` = the sum of the parent's three best chunk scores.

    Proven on a parent with three chunks and on one with two, so the k-bound
    and the fewer-than-k case are both covered.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        chunk_scores = _chunk_scores(conn, session, TERM)
        rolled, _, _ = _rollup_scores(conn, session, TERM, "sum_top3")

    for doc_id, row in rolled.items():
        own = sorted(
            score
            for chunk_id, score in chunk_scores.items()
            if chunk_id.startswith(f"{doc_id}:chunk:")
        )
        assert float(row["score"]) == pytest.approx(sum(own[:3])), doc_id
    # The two shapes the rule has to handle both exist in the fixture.
    assert EXPECTED_CHUNK_COUNTS["doc:broad"] == 3
    assert EXPECTED_CHUNK_COUNTS["doc:thin"] == 1


def test_the_declared_child_subtype_keeps_figures_out_of_relevance(
    clean_runtime: str,
    tmp_path,
) -> None:
    """A second child on the SAME edge type must not contribute.

    `doc:thin` owns one weak chunk AND a figure whose caption repeats the term
    thirty times. If the rollup followed `contains` alone — the guess the
    declaration exists to forbid — that caption would dominate the document's
    score. Its score must equal its chunk's, exactly.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        chunk_scores = _chunk_scores(conn, session, TERM)
        rolled, _, _ = _rollup_scores(conn, session, TERM, "max")
        figures = search(
            conn,
            session,
            TERM,
            method="lexical", completeness="exact",
            filter={"subtype": "figure"},
            limit=10,
            select=["node_id", "score"],
        )

    # Control: the figure is a real, and much stronger, BM25 match.
    assert figures["total_count"] == 1, figures
    figure_score = float(figures["results"][0]["score"])
    assert figure_score < min(chunk_scores.values()), figure_score

    thin = rolled["doc:thin"]
    assert float(thin["score"]) == pytest.approx(chunk_scores["doc:thin:chunk:0"])
    assert int(thin["rollup_chunk_count"]) == 1


def test_envelope_discloses_the_mechanism(
    clean_runtime: str,
    tmp_path,
) -> None:
    """A rolled-up page says so, names the relation, and counts its chunks."""
    deployment = _publish(clean_runtime, tmp_path)
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
        default = search(
            conn,
            session,
            TERM,
            method="lexical",
            filter={"subtype": "doc"},
            limit=10,
        )

    assert payload["rank_by"] == "passages"
    assert payload["rank_by_relation"] == {
        "parent_subtype": "doc",
        "edge_type": "contains",
        "child_subtype": "chunk",
        "source": "catalog:okg_passage_rollup",
    }
    assert payload["rank_by_aggregation"] == PASSAGE_ROLLUP_AGGREGATION
    assert payload["rollup_chunk_cap"] == PASSAGE_ROLLUP_CHUNK_CAP
    assert payload["rollup_chunks_scanned"] > 0
    assert payload["rollup_chunk_cap_reached"] is False
    assert payload["total_count_unit"] == "doc"
    assert payload["rank_capped_unit"] == "chunks"
    # Per-document contributing-chunk counts, complete over the page.
    assert payload["rollup_contributing_chunks"] == EXPECTED_CHUNK_COUNTS
    # The lexical default is `records` (PACT search-result-shape), and says so;
    # it does not carry the rollup's own disclosure keys.
    assert default["rank_by"] == "records"
    for key in (
        "rank_by_relation",
        "rank_by_aggregation",
        "rollup_chunks_scanned",
        "rollup_contributing_chunks",
    ):
        assert key not in default, key


def test_rank_by_is_reported_on_every_method(
    clean_runtime: str,
    tmp_path,
) -> None:
    """Unconditional disclosure, like `search_profile`.

    An absent key on one method and a present key on another would make the
    caller infer the default — the inference the disclosure removes.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        for method, mode in (("lexical", "records"), ("alias", "self")):
            payload = search(conn, session, TERM, method=method, limit=5)
            assert payload["rank_by"] == mode, method


def test_pagination_counts_documents_and_budgets_chunks(
    clean_runtime: str,
    tmp_path,
) -> None:
    """`total_count` is documents; the cap is chunks; the two are separate."""
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        first = search(
            conn,
            session,
            TERM,
            method="lexical", completeness="exact",
            filter={"subtype": "doc"},
            rank_by="passages",
            limit=2,
            offset=0,
        )
        second = search(
            conn,
            session,
            TERM,
            method="lexical", completeness="exact",
            filter={"subtype": "doc"},
            rank_by="passages",
            limit=2,
            offset=2,
        )
        _, scanned, capped = _rollup_scores(conn, session, TERM, "max")

    total = len(EXPECTED_CHUNK_COUNTS)
    assert first["total_count"] == total
    assert len(first["results"]) == 2
    assert first["truncated"] is True
    assert first["next_offset"] == 2
    assert len(second["results"]) == 2
    assert second["truncated"] is False
    assert second["next_offset"] is None
    # The two pages are disjoint, ordered, and cover the whole ranked set.
    ids = [row["node_id"] for row in first["results"] + second["results"]]
    assert len(set(ids)) == total
    # The chunk budget is a DIFFERENT number from the document count: more
    # chunks were scanned than documents were returned.
    assert scanned > total
    assert capped is False
    # `rank_capped` is reported against the chunk budget, so an unsaturated
    # scan says so even though every document fits on two pages.
    assert first["rank_capped"] is False


def test_an_offset_past_the_ordered_documents_is_refused(
    clean_runtime: str,
    tmp_path,
) -> None:
    """Refused, not answered with an empty page.

    Beyond the chunk budget document ranks are not merely missing, they are
    UNORDERED — a document whose chunks all fell outside the budget has no
    score at all — so "the next page" does not exist to be returned.
    """
    deployment = _publish(clean_runtime, tmp_path)
    total = len(EXPECTED_CHUNK_COUNTS)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        # The last valid offset still answers.
        edge = search(
            conn,
            session,
            TERM,
            method="lexical",
            filter={"subtype": "doc"},
            rank_by="passages",
            limit=2,
            offset=total - 1,
        )
        assert len(edge["results"]) == 1

        for offset in (total, total + 5):
            with pytest.raises(PassageRollupOffsetUnordered) as exc:
                search(
                    conn,
                    session,
                    TERM,
                    method="lexical",
                    filter={"subtype": "doc"},
                    rank_by="passages",
                    limit=2,
                    offset=offset,
                )
            assert exc.value.code == "passage_rollup_offset_unordered"
            assert "UNORDERED" in str(exc.value)

        # A self-ranked search at the same deep offset still returns the empty
        # page it always did: the refusal is specific to the rollup's
        # semantics, not a new global pagination rule.
        deep = search(
            conn,
            session,
            TERM,
            method="lexical",
            filter={"subtype": "doc"},
            limit=2,
            offset=99,
        )
        assert deep["results"] == []


def test_the_chunk_budget_is_reported_when_it_saturates(
    clean_runtime: str,
    tmp_path,
) -> None:
    """A saturated scan says so, and returns only what it ordered.

    Exercised with an explicit small cap rather than a 2,000-chunk fixture: the
    property under test is that saturation is REPORTED, and the cap value is
    a parameter of the same code path the default uses.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        segment, index_name = resolve_node_text_bm25_index(conn)
        order_sql = f"(attrs->>'text') <@> to_bm25query(%(q)s, '{index_name}')"
        rows, scanned, capped = passage_rollup_candidates(
            conn,
            session=session,
            rollup=ROLLUP,
            node_segment=segment,
            edge_segment=resolve_edge_segment(conn),
            order_sql=order_sql,
            params={"q": TERM, "bp": branch_point(conn, session)},
            chunk_cap=2,
        )

    assert scanned == 2
    assert capped is True
    # Two chunks can reach at most two parents, and the parents they reach are
    # the ones owning the two best chunks — not an arbitrary prefix.
    assert 1 <= len(rows) <= 2
    assert sum(int(row["rollup_chunk_count"]) for row in rows) == 2


# ---- issue #2670: a passage-ranked row previews its winning passage ----

#: No profile rows are seeded, so trust comes from the producer id alone: a
#: system prefix reads as trusted, any other producer fails safe to untrusted.
TRUSTED = "_extractor:passage_fixture"
UNTRUSTED = "external:passage_fixture"


def _publish_mixed_trust(dsn: str, tmp_path) -> str:
    """Three docs, one per trust pairing of parent and winning chunk."""
    from tests.substrate.mcp.erag_rollup_fixture import _edge, _fact

    pairs = {
        # doc id: (doc producer, chunk producer)
        "doc:both": (TRUSTED, TRUSTED),
        "doc:chunkout": (TRUSTED, UNTRUSTED),
        "doc:parentout": (UNTRUSTED, TRUSTED),
        # Trusted producers, but the chunk's own attrs mark it as a web tool
        # result: its read trust is untrusted whatever the producer says.
        "doc:transcript": (TRUSTED, TRUSTED),
    }
    groups: dict[str, list] = {TRUSTED: [], UNTRUSTED: []}
    edges = []
    filler = " ".join(f"filler{i}" for i in range(120))
    for doc_id, (doc_source, chunk_source) in pairs.items():
        name = doc_id.split(":")[1]
        groups[doc_source].append(
            _fact(doc_id, "doc", {"path": f"corpus/{name}-report.txt"})
        )
        chunk_id = f"{doc_id}:chunk:0"
        attrs = {"text": f"The {name} assay records cladding tolerances. {filler}"}
        if name == "transcript":
            attrs[UNTRUSTED_CONTENT_ORIGIN_ATTR] = WEB_TOOL_RESULT_ORIGIN
        groups[chunk_source].append(_fact(chunk_id, "chunk", attrs))
        edges.append(_edge(doc_id, chunk_id, "contains"))
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    publish_nodes(
        dsn,
        deployment,
        subtypes=SUBTYPES,
        narrowings=NARROWINGS,
        nodes=[],
        edges=edges,
        node_groups=list(groups.items()),
    )
    return deployment


def _fence_parts(excerpt: str) -> tuple[str, str, str]:
    lines = excerpt.split("\n")
    return lines[0], "\n".join(lines[1:-1]), lines[-1]


def test_a_passage_ranked_row_previews_the_winning_passage(
    clean_runtime: str,
    tmp_path,
) -> None:
    """The excerpt is the chunk's text, fenced by the weaker of the two rows.

    Before #2670 the excerpt was the parent's own locator text (its path), so
    the card named the winning chunk but did not show it.
    """
    from okg.substrate.mcp.provenance import FENCE_CLOSE, FENCE_OPEN, is_fenced

    deployment = _publish_mixed_trust(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        page = search(
            conn,
            session,
            BODY_ONLY_TERM,
            method="lexical",
            filter={"subtype": "doc"},
            rank_by="passages",
            limit=10,
            max_chars=120,
        )
        lean = search(
            conn,
            session,
            BODY_ONLY_TERM,
            method="lexical",
            filter={"subtype": "doc"},
            rank_by="passages",
            limit=10,
            select=["node_id", "rollup_best_chunk_id"],
        )

    rows = {row["node_id"]: row for row in page["results"]}
    assert set(rows) == {
        "doc:both", "doc:chunkout", "doc:parentout", "doc:transcript",
    }
    for doc_id, row in rows.items():
        assert row["rollup_best_chunk_id"] == f"{doc_id}:chunk:0"
        assert "report.txt" not in row["excerpt"], row["excerpt"]

    # Both trusted: plain chunk text, bounded by max_chars.
    both = rows["doc:both"]["excerpt"]
    assert not is_fenced(both)
    assert both.startswith("The both assay records cladding tolerances.")
    assert len(both) == 120

    # Trusted parent, untrusted chunk: fenced and labelled with the CHUNK's
    # producer, exactly as a direct hit on the chunk would be.
    chunkout = rows["doc:chunkout"]
    assert chunkout["provenance"]["trust"] == "trusted"
    assert is_fenced(chunkout["excerpt"], source=UNTRUSTED), chunkout["excerpt"]
    opener, body, closer = _fence_parts(chunkout["excerpt"])
    assert opener == FENCE_OPEN.format(source=UNTRUSTED)
    assert closer == FENCE_CLOSE
    assert body.startswith("The chunkout assay records cladding")
    assert len(body) == 120

    # Untrusted parent, trusted chunk: still fenced, never weaker than the
    # parent row, and the marker names the passage's producer.
    parentout = rows["doc:parentout"]
    assert parentout["provenance"]["trust"] == "untrusted"
    assert is_fenced(parentout["excerpt"], source=TRUSTED), parentout["excerpt"]
    _, body, _ = _fence_parts(parentout["excerpt"])
    assert body.startswith("The parentout assay records cladding")
    assert len(body) == 120

    # Both producers trusted, but the passage's attrs mark web-tool content:
    # the passage's own read trust decides, so the preview is fenced.
    transcript = rows["doc:transcript"]
    assert transcript["provenance"]["trust"] == "trusted"
    assert is_fenced(transcript["excerpt"], source=TRUSTED), transcript["excerpt"]

    # A projection without the excerpt stays without it.
    for row in lean["results"]:
        assert "excerpt" not in row, row
        assert row["rollup_best_chunk_id"] == f"{row['node_id']}:chunk:0"
