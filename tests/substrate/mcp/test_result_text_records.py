"""How the search-result-shape envelope fields print (DESIGN.md D6).

Each new field has a normal state that prints nothing and a state that
prints; none may vanish (as `ranked_cap` did: placed, never printed) or fall
under `--- other fields`. Database-free.
"""
from __future__ import annotations

from okg.substrate.mcp.result_text import render

NEW_FIELDS = (
    "ranked_cap", "result_unit", "ordered_exactly_through", "next_offset_reason",
    "excerpt_window_basis", "diversity_applicable", "diversity_reason",
    "rank_by", "max_per_parent_origin",
)


def _page(**extra: object) -> dict:
    return {
        "method": "lexical",
        "rank_by": "records",
        "results": [{"node_id": "doc:1", "subtype": "doc", "score": -3.0}],
        "total_count": 1,
        "offset": 0,
        "next_offset": None,
        "total_count_basis": "exact",
        "result_unit": "record",
        "excerpt_window_basis": "lexized",
        "diversity_applicable": True,
        **extra,
    }


def _status(text: str) -> str:
    return text.splitlines()[0]


def _other_fields(text: str) -> list[str]:
    lines = text.splitlines()
    if "--- other fields" not in lines:
        return []
    return lines[lines.index("--- other fields") + 1:]


def test_the_normal_state_prints_nothing_new() -> None:
    text = render(_page(), tool="search")
    # PACT read-model-s6-page-next: a whole page ends `complete`; #3666
    # says the same claim as "N records, all shown".
    assert _status(text) == "search ok · lexical · 1 record, all shown"
    assert not [line for line in text.splitlines() if line.startswith("! ")]
    assert _other_fields(text) == []


def test_ranked_cap_prints_beside_a_lower_bound_count() -> None:
    text = render(
        _page(total_count=200, total_count_basis="lower_bound",
              ranked_cap={"rows": 200}),
        tool="search",
    )
    assert "1-1 of 200+ (cap 200 rows)" in _status(text)
    fold = render(
        _page(total_count=180, total_count_basis="lower_bound",
              ranked_cap={"rows": 200, "passages": 200}),
        tool="search",
    )
    assert "of 180+ (cap 200 rows, 200 passages)" in _status(fold)


def test_ranked_cap_does_not_print_on_an_exact_count() -> None:
    text = render(_page(ranked_cap={"rows": 200}), tool="search")
    assert "cap" not in _status(text)


def test_a_passage_unit_and_a_non_default_rank_mode_print_on_the_status_line() -> None:
    assert "· passages" in _status(render(_page(result_unit="passage"), tool="search"))
    assert "· rank self" in _status(render(_page(rank_by="self"), tool="search"))
    semantic = render(_page(method="semantic", rank_by="self"), tool="search")
    assert "rank" not in _status(semantic)


def test_the_exact_order_notice_replaces_next() -> None:
    text = render(
        _page(total_count=300, truncated=True, total_count_basis="lower_bound",
              ranked_cap={"rows": 200, "passages": 200},
              ordered_exactly_through=7,
              next_offset_reason="beyond_exact_order"),
        tool="search",
    )
    # PACT read-model-s6-page-next: still no offset to page to; the page
    # ends at the exact-order bound with nothing that continues it.
    assert _status(text).endswith(" · bound reached (exact order) · next: none")
    assert "next: offset" not in _status(text)
    assert (
        "! order exact through record 7; narrow the query to see later records"
        in text.splitlines()
    )


def test_the_excerpt_fallback_and_profile_diversity_print_notices() -> None:
    text = render(
        _page(excerpt_window_basis="whole_word_fallback",
              diversity_applicable=False,
              diversity_reason="records_returns_one_row_per_parent",
              max_per_parent_origin="profile"),
        tool="search",
    )
    lines = text.splitlines()
    assert "! excerpt windows by whole-word match (word bound reached)" in lines
    assert "! profile max_per_parent not applied: one row per record" in lines


def test_no_new_field_falls_under_other_fields() -> None:
    page = _page(
        total_count=300, truncated=True, total_count_basis="lower_bound",
        ranked_cap={"rows": 200, "passages": 200}, ordered_exactly_through=7,
        next_offset_reason="beyond_exact_order", result_unit="passage",
        excerpt_window_basis="whole_word_fallback", diversity_applicable=False,
        diversity_reason="records_returns_one_row_per_parent",
        max_per_parent_origin="profile", rank_by="self",
    )
    other = "\n".join(_other_fields(render(page, tool="search")))
    for field in NEW_FIELDS:
        assert f"{field}:" not in other, field


def test_a_folded_card_names_its_winner_and_count() -> None:
    page = _page(results=[{
        "node_id": "doc:1", "subtype": "doc", "score": -3.0,
        "rollup_best_chunk_id": "chunk:9", "matching_passages": 3,
    }])
    card = render(page, tool="search").splitlines()[1:]
    joined = "\n".join(card)
    # #3666: the winning chunk id and the passage count are in the JSON
    # only; the row shows the matched passage itself (row["passage"]).
    assert "chunk:9" not in joined and "matching passages" not in joined
    assert joined.strip() == "[1] doc:1 (doc) · trust=unlabeled"


# ── #3059's quoting rules on the search-result-shape facts ────────────────


def test_a_rank_mode_with_a_space_or_a_dot_lookalike_is_quoted() -> None:
    """A status-line subject with a space reads as one JSON token (#3059)."""
    status = _status(render(_page(rank_by="self generation x"), tool="search"))
    assert '· rank "self generation x"' in status
    dotted = _status(render(_page(rank_by="a⦁b"), tool="search"))
    assert "rank \"a\\u2981b\"" in dotted or 'rank "a⦁b"' in dotted
    assert "rank a⦁b ·" not in dotted


def test_a_cap_value_or_unit_that_could_break_the_cap_fact_is_quoted() -> None:
    text = render(
        _page(total_count=200, total_count_basis="lower_bound",
              ranked_cap={"rows": "1, 2 (x)", "odd unit": 5}),
        tool="search",
    )
    status = _status(text)
    assert '(cap "1, 2 (x)" rows, 5 "odd unit")' in status, status


def test_the_new_facts_print_and_no_generation_follows() -> None:
    gen = "okg:demo:branch:default:gen:20260101T000000000000Z:0123456789ab"
    text = render(
        _page(result_unit="passage", rank_by="self", pinned_generation_id=gen,
              total_count=200, total_count_basis="lower_bound",
              ranked_cap={"rows": 200}),
        tool="search",
    )
    facts = _status(text).split(" · ")
    # D8 (PACT read-passage-rows-3666): no read reply names its generation.
    assert gen not in text
    assert "passages" in facts and "rank self" in facts


def test_a_non_normal_value_the_notices_do_not_name_falls_under_other_fields() -> None:
    """No placed field was dropped while it was not normal (#3059 B1).

    #3666 (read-passage-rows): a search page has no other-fields block;
    these reason codes and origins are in the JSON form, and each
    non-normal state still prints its notice (what a reader acts on)."""
    text = render(
        _page(next_offset_reason="nr-new", ordered_exactly_through=4,
              diversity_reason="dr-new", diversity_applicable=False,
              max_per_parent_origin="mpo-new", result_unit="unit-new",
              excerpt_window_basis="ewb-new"),
        tool="search",
    )
    assert _other_fields(text) == []
    for notice in ("! order exact through record 4",
                   "! excerpt windows by whole-word match",
                   "! profile max_per_parent not applied"):
        assert any(line.startswith(notice) for line in text.splitlines()), (notice, text)


# ── issue #3465: exact duplicates collapsed ──────────────────────────────


def test_a_card_names_its_identical_copies_among_retrieved_candidates() -> None:
    page = _page(
        duplicates_collapsed=2,
        results=[{
            "node_id": "doc:1", "subtype": "doc", "score": -3.0,
            "copies": ["doc:2", "doc:3"],
        }],
    )
    text = render(page, tool="search")
    card = "\n".join(text.splitlines()[1:])
    assert "2 identical copies among retrieved candidates: doc:2, doc:3" in card
    assert "! 2 exact duplicate rows collapsed (each kept row lists its copies)" in (
        text.splitlines()
    )
    assert _other_fields(text) == []
    exhaustive = render(
        {**page, "coverage": {"retrieval": {"status": "exhaustive"}}}, tool="search",
    )
    assert "2 identical copies: doc:2, doc:3" in exhaustive


def test_one_copy_reads_singular_and_no_collapse_prints_nothing() -> None:
    one = render(_page(duplicates_collapsed=1, results=[{
        "node_id": "doc:1", "subtype": "doc", "score": -3.0, "copies": ["doc:2"],
    }]), tool="search")
    assert "1 identical copy among retrieved candidates: doc:2" in one
    assert "! 1 exact duplicate row collapsed" in one
    for value in (0, None):
        text = render(_page(duplicates_collapsed=value), tool="search")
        assert "duplicate" not in text and "copies" not in text
        assert _other_fields(text) == []


def test_a_list_line_names_its_copies_once() -> None:
    page = _page(view="list", results=[{
        "node_id": "doc:1", "subtype": "doc", "provenance": {"trust": "trusted"},
        "copies": ["doc:2"],
    }])
    text = render(page, tool="search")
    rows = [line for line in text.splitlines() if line.startswith("[1]")]
    assert len(rows) == 1
    assert "1 identical copy among retrieved candidates: doc:2" in rows[0]
    assert "copies=" not in text


def test_a_capped_copy_list_says_how_many_it_lists() -> None:
    listed = [f"doc:{i}" for i in range(2, 12)]
    text = render(_page(results=[{
        "node_id": "doc:1", "subtype": "doc", "score": -3.0,
        "copies": listed, "copies_total": 12,
    }]), tool="search")
    assert (
        "12 identical copies among retrieved candidates (first 10 listed): "
        + ", ".join(listed)
    ) in text
    assert "copies_total" not in text
    assert _other_fields(text) == []
