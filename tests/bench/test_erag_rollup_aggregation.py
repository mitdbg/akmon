"""PACT ``recover-passage-rollup-reads`` aggregation evidence.

The aggregation was decided by measurement, and the constant must not drift
away from the table that decided it.

The deciding experiment is the development set's own first lexical queries,
at the arm's own requested limits, replayed at the INDEX level under both
candidate rules against the pinned erag-seg generation. No model spend: no
reader run and no judge run.

    document-level (today)   26/47
    rollup, max              33/47
    rollup, sum_top3         30/47

`max` wins by three. PR #1333 preserved the full replay; this recovery test
pins the RESULT so a later edit of the constant fails here rather than
silently re-ranking every deployment's documents.

CI replays the selected aggregation against a hermetic passage fixture. The
302,892-chunk erag-seg replay remains an explicit operator command because CI
does not carry that corpus; it is never represented as a passing or skipped
blocking receipt.
"""

from __future__ import annotations

import os

from okg.substrate.mcp.passage_rollup import (
    PASSAGE_ROLLUP_AGGREGATION,
    PASSAGE_ROLLUP_AGGREGATIONS,
    PASSAGE_ROLLUP_CHUNK_CAP,
)


#: The measured table, recorded. Gold documents landing in the first page of
#: the arm's own first query, over the development set's 47 gold documents.
MEASURED_GOLD_IN_FIRST_PAGE = {
    "document_level": 26,
    "max": 33,
    "sum_top3": 30,
}
GOLD_TOTAL = 47
#: The PACT's declared acceptance floor.
FLOOR = 33

#: Per-question deltas under the winning rule, enumerated rather than netted.
IMPROVED = (
    "qst_0118",
    "qst_0198",
    "qst_0280",
    "qst_0341",
    "qst_0348",
    "qst_0354",
    "qst_0358",
    "qst_0428",
)
REGRESSED = ("qst_0402", "qst_0420")

ERAG_DSN = os.environ.get("OKG_ERAG_SEG_DSN")
ERAG_DEPLOYMENT = os.environ.get("OKG_ERAG_SEG_DEPLOYMENT")


def test_the_applied_aggregation_is_the_one_the_table_chose() -> None:
    """The constant equals the measured winner, and wins by a real margin.

    Both halves matter. The first stops the constant drifting away from its
    evidence; the second stops a future edit claiming the table decided
    something it did not -- if the two rules ever tie, the choice needs a
    restated argument, not a silent default.
    """
    assert PASSAGE_ROLLUP_AGGREGATION in PASSAGE_ROLLUP_AGGREGATIONS
    ranked = sorted(
        (score, name)
        for name, score in MEASURED_GOLD_IN_FIRST_PAGE.items()
        if name in PASSAGE_ROLLUP_AGGREGATIONS
    )
    best_score, best_name = ranked[-1]
    runner_up_score = ranked[-2][0]
    assert PASSAGE_ROLLUP_AGGREGATION == best_name
    assert best_score > runner_up_score, (
        "the aggregations tie on the measured table, so the choice is no "
        "longer decided by measurement and must be restated"
    )


def test_the_rollup_clears_the_declared_floor_over_the_baseline() -> None:
    """26/47 -> 33/47, against a declared floor of 33/47."""
    assert MEASURED_GOLD_IN_FIRST_PAGE["document_level"] == 26
    assert MEASURED_GOLD_IN_FIRST_PAGE[PASSAGE_ROLLUP_AGGREGATION] >= FLOOR
    assert MEASURED_GOLD_IN_FIRST_PAGE[PASSAGE_ROLLUP_AGGREGATION] <= GOLD_TOTAL


def test_the_regressions_are_enumerated_not_netted() -> None:
    """The PACT requires the slight regressions be named, not hidden.

    Measured, there are TWO (`qst_0402` 2 -> 1, `qst_0420` 2 -> 1), where the
    PACT rationale predicted three. Recorded as measured; the net gain is the
    difference between the improvements and these, not a headline on its own.
    """
    assert set(IMPROVED).isdisjoint(REGRESSED)
    assert len(IMPROVED) == 8
    assert len(REGRESSED) == 2
    # The two cases the PACT names as decisive are among the improvements.
    assert "qst_0118" in IMPROVED
    assert "qst_0358" in IMPROVED


def test_the_chunk_budget_is_its_own_constant() -> None:
    """The budget counts CHUNKS, so it is not the document rank cap.

    Measured on erag-seg, a 2,000-chunk budget yields 976-1,642 distinct
    documents -- comfortably more than any page -- while the 200-row document
    cap would bound the document count below a deep page.
    """
    from okg.substrate.mcp.search import _LEXICAL_RANK_CAP

    assert PASSAGE_ROLLUP_CHUNK_CAP == 2000
    assert PASSAGE_ROLLUP_CHUNK_CAP != _LEXICAL_RANK_CAP


def replay_the_two_decisive_cases_against_live_erag_seg() -> None:
    """Operator-only re-derivation of the two decisive erag-seg cases.

    Run with both ``OKG_ERAG_SEG_DSN`` and ``OKG_ERAG_SEG_DEPLOYMENT`` set by
    importing this helper from ``tests.bench.test_erag_rollup_aggregation`` in
    an operator-owned Python process.

    `qst_0118`'s gold ranks 49th on its filename and 1st on its body;
    `qst_0358`'s gold is unreachable at document level at any depth an operator
    would page to. Both are the whole argument for the rollup, so both are
    re-measured rather than quoted.
    """
    if ERAG_DSN is None or ERAG_DEPLOYMENT is None:
        raise RuntimeError(
            "OKG_ERAG_SEG_DSN and OKG_ERAG_SEG_DEPLOYMENT are required for "
            "the live erag-seg aggregation proof"
        )
    import psycopg

    from okg.substrate.mcp.passage_rollup import (
        PassageRollup,
        passage_rollup_candidates,
    )
    from okg.substrate.mcp.session import open_session

    cases = [
        (
            "qst_0118",
            "9a0982d4",
            "inference routes production workload latency SLO clustering "
            "profiling cost sensitivity",
        ),
        ("qst_0358", "858fd59a", "completeness ratio hourly validation metering"),
    ]
    node_seg = "public._chronos_b_interval_okg_graph_nodes_1c8e1fc23f"
    edge_seg = "public._chronos_b_interval_okg_graph_edges_d84ac83c2b"
    index = "idx_okg_chronos_8fc1cb84e69b_node_text_bm25_visible"
    branch_point = 84351199
    rollup = PassageRollup(
        parent_subtype="document",
        edge_type="contains",
        child_subtype="document_chunk",
    )
    order_sql = f"(attrs->>'text') <@> to_bm25query(%(q)s, '{index}')"

    try:
        conn = psycopg.connect(ERAG_DSN, autocommit=True, connect_timeout=3)
    except psycopg.Error as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "erag-seg is not reachable; the aggregation table above is the "
            "recorded measurement and the constant tests still ran"
        ) from exc
    with conn:
        session = open_session(conn, deployment_name=ERAG_DEPLOYMENT)
        with conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {node_seg} LIMIT 1")
            if cur.fetchone() is None:  # pragma: no cover
                raise RuntimeError("erag-seg node segment is empty")
        for label, gold, query in cases:
            rows, scanned, capped = passage_rollup_candidates(
                conn,
                session=session,
                rollup=rollup,
                node_segment=node_seg,
                edge_segment=edge_seg,
                order_sql=order_sql,
                params={"q": query, "bp": branch_point},
                aggregation=PASSAGE_ROLLUP_AGGREGATION,
            )
            rows.sort(key=lambda r: (float(r["score"] or 0.0), str(r["node_id"])))
            ranks = [i for i, row in enumerate(rows, 1) if gold in str(row["node_id"])]
            assert ranks, f"{label}: gold {gold} absent from the rollup"
            assert ranks[0] <= 3, f"{label}: gold at rank {ranks[0]}"
            assert scanned == PASSAGE_ROLLUP_CHUNK_CAP
            assert capped is True
