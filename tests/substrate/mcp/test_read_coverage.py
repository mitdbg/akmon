"""Cardinality, retrieval recall and delivered rows are distinct guarantees."""
from okg.substrate.mcp.envelope import with_read_coverage
from okg.substrate.mcp.result_text import render
import pytest


def page(**changes):
    return {"operator_name": "search", "method": "lexical", "results": [{"node_id": "n:1"}],
            "total_count": 1, "total_count_basis": "exact", "truncated": False,
            "offset": 0, "limit": 50, "rank_by": "records", **changes}


@pytest.mark.parametrize("number", [0, 17])
def test_unknown_basis_supersedes_numeric_count(number):
    out = with_read_coverage(page(total_count=number, total_count_basis="unknown"))
    assert out["total_count"] is None
    assert out["coverage"]["selection"]["size"] is None
    assert out["coverage"]["retrieval"]["status"] == "unknown"


def test_missing_basis_cannot_establish_global_count():
    raw = page(total_count=17)
    del raw["total_count_basis"]
    out = with_read_coverage(raw)
    assert out["total_count"] is None
    assert out["total_count_basis"] == "unknown"
    assert out["coverage"]["retrieval"]["status"] == "unknown"


def test_approximate_terminal_delivery_never_claims_exhaustive_retrieval():
    out = with_read_coverage(page(method="semantic"))
    assert out["coverage"]["retrieval"]["status"] == "approximate"
    assert out["coverage"]["delivery"]["complete"] is True
    assert out["coverage"]["selection"]["basis"] == "retrieved_candidates"
    text = render(out, tool="search")
    # #3666: the count-set name (retrieved_candidates) is an internal no
    # longer printed; the approximate retrieval still qualifies "complete",
    # and the page never says every match was shown.
    assert "retrieval approximate" in text and "all shown" not in text


@pytest.mark.parametrize("cap,reason", [("rank_capped", "candidate_cap"),
                                       ("candidates_capped", "candidate_cap"),
                                       ("rollup_chunk_cap_reached", "work_cap")])
def test_cap_is_independent_of_terminal_page(cap, reason):
    out = with_read_coverage(page(**{cap: True}, total_count_basis="lower_bound"))
    assert out["coverage"]["retrieval"]["status"] == "bounded"
    assert reason in out["coverage"]["retrieval"]["bounds"]
    assert out["coverage"]["delivery"]["complete"] is None
    assert out["total_count"] == 1


def test_page_limit_never_establishes_selection_size_or_membership_limit():
    out = with_read_coverage(page(total_count_basis="lower_bound", limit=1))
    assert out["coverage"]["selection"]["size"] is None
    # No membership limit is declared, so the key is absent, never null.
    assert "limit" not in out["coverage"]["selection"]


def test_full_exact_candidate_selection_can_be_delivered_despite_retrieval_cap():
    out = with_read_coverage(page(rank_capped=True, truncated=True))
    assert out["coverage"]["retrieval"]["status"] == "bounded"
    assert out["coverage"]["delivery"]["complete"] is True
    later = with_read_coverage(page(rank_capped=True, truncated=True, total_count=3, offset=2))
    assert later["coverage"]["delivery"]["complete"] is True


def test_byte_cut_or_remaining_cursor_prevents_selection_delivery_completion():
    for changes in ({"truncation_reason": "response_byte_budget"}, {"next_offset": 1}):
        out = with_read_coverage(page(rank_capped=True, truncated=True, **changes))
        assert out["coverage"]["delivery"]["complete"] is False


def test_unknown_selection_without_cursor_does_not_prove_delivery_complete():
    out = with_read_coverage(page(total_count_basis="unknown"))
    assert out["coverage"]["delivery"]["complete"] is None


def test_final_trim_refreshes_delivery_without_changing_retrieval():
    original = with_read_coverage(page(results=[{"node_id": "n:1"}, {"node_id": "n:2"}], total_count=2))
    out = with_read_coverage({**original, "results": original["results"][:1],
                              "truncated": True, "next_offset": 1,
                              "truncation_reason": "response_byte_budget"})
    assert out["coverage"]["retrieval"] == original["coverage"]["retrieval"]
    assert out["coverage"]["selection"] == original["coverage"]["selection"]
    assert out["coverage"]["delivery"] == {"complete": False, "bounds": ["output_bytes"]}
    assert with_read_coverage(out) == out


def test_aggregate_counts_groups_and_retains_input_set_basis():
    out = with_read_coverage({"operator_name": "aggregate", "groups": [{"count": 8}],
                             "total_count": 1, "total_count_basis": "exact",
                             "selection_basis": "input_set", "truncated": False})
    assert out["coverage"]["selection"]["unit"] == "group"
    assert out["coverage"]["selection"]["basis"] == "input_set"


def test_diversified_survivor_count_is_not_global_matching_count():
    out = with_read_coverage(page(diversity_applied=True))
    assert out["coverage"]["selection"]["basis"] == "retrieved_candidates"
    assert out["coverage"]["retrieval"]["status"] == "unknown"


def test_exact_input_set_cardinality_is_not_source_exhaustion():
    out = with_read_coverage(page(selection_basis="input_set"))
    # An exact size is `total_count` itself and is omitted from coverage.
    assert "size" not in out["coverage"]["selection"] and out["total_count"] == 1
    assert out["coverage"]["retrieval"]["status"] == "unknown"


def test_exact_uncapped_bm25_candidates_do_not_prove_index_enumeration():
    out = with_read_coverage(page(rank_capped=False))
    assert out["total_count"] == 1
    assert out["total_count_basis"] == "exact"
    assert out["coverage"]["selection"]["basis"] == "retrieved_candidates"
    assert out["coverage"]["retrieval"]["status"] == "unknown"


def test_positive_scan_exhaustion_proof_is_independent_of_survivor_count():
    out = with_read_coverage(page(diversity_applied=True, retrieval_exhaustive=True))
    assert out["coverage"]["retrieval"]["status"] == "exhaustive"
    assert out["coverage"]["selection"]["basis"] == "retrieved_candidates"


def test_subtype_count_unit_is_preserved():
    out = with_read_coverage(page(total_count_unit="issue"))
    assert out["total_count_unit"] == "issue"
    assert out["coverage"]["selection"]["unit"] == "record"


def test_identifier_occurrences_are_exhaustive_only_for_supplied_text():
    out = with_read_coverage(page(method="identifier_mentions", selection_basis="input_set",
                                  retrieval_basis="input_text", retrieval_exhaustive=True))
    assert out["coverage"]["selection"]["basis"] == "input_set"
    assert out["coverage"]["selection"]["unit"] == "row"
    assert out["coverage"]["retrieval"] == {
        "status": "exhaustive", "basis": "input_text"}


def test_coverage_is_the_single_source_of_count_provenance():
    """No top-level copy of `coverage.selection`; producer markers stay off
    the wire, and a key dev did not emit is not added."""
    for raw in (page(), page(method="semantic"), page(selection_basis="input_set"),
                page(retrieval_exhaustive=True, retrieval_basis="input_text"),
                page(total_count_basis="unknown", delivery_last_page=True)):
        out = with_read_coverage(raw)
        for key in ("total_count_set", "total_count_unit", "selection_basis",
                    "retrieval_basis", "retrieval_exhaustive", "delivery_last_page"):
            assert key not in out, (key, out)


def test_proven_final_page_with_unknown_count_is_delivered_complete():
    """filter(limit=2, offset=10) at the end of a twelve-row set: the count
    is unknown past the first page, but the scan proved nothing follows."""
    final = page(operator_name="filter", method=None, results=None,
                 items=[{"node_id": "n:11"}, {"node_id": "n:12"}],
                 total_count=None, total_count_basis="unknown", offset=10, limit=2,
                 next_offset=None, delivery_last_page=True)
    del final["results"]
    out = with_read_coverage(final)
    assert out["coverage"]["selection"]["size"] is None
    assert out["coverage"]["delivery"] == {"complete": True}
    # The second pass (after byte fitting) keeps it for the same rows only.
    assert with_read_coverage(out) == out
    trimmed = with_read_coverage({**out, "items": out["items"][:1], "next_offset": 11,
                                  "truncation_reason": "response_byte_budget"})
    assert trimmed["coverage"]["delivery"]["complete"] is False
    # The page ending says it; coverage prints no delivery fact of its own.
    assert "complete" in render(out, tool="filter").splitlines()[0].split(" · ")


def test_trim_without_marker_never_inherits_an_exact_size_completion():
    """First pass: exact total 3, three rows, complete by the exact-size rule.
    A later trim to one row with no cursor and no byte-cut reason must not
    inherit that completion: one of three rows is not a complete delivery."""
    rows = [{"node_id": f"n:{i}"} for i in range(3)]
    whole = with_read_coverage(page(results=rows, total_count=3))
    assert whole["coverage"]["delivery"] == {"complete": True}
    trimmed = with_read_coverage({**whole, "results": rows[:1]})
    assert trimmed["coverage"]["delivery"]["complete"] is not True
    # Unknown count: the inherited marker proof holds for the same rows only.
    final = with_read_coverage(page(operator_name="filter", total_count=None,
                                    total_count_basis="unknown", offset=10,
                                    results=rows, delivery_last_page=True))
    assert final["coverage"]["delivery"] == {"complete": True}
    assert with_read_coverage(final)["coverage"]["delivery"] == {"complete": True}
    fewer = with_read_coverage({**final, "results": rows[:1]})
    assert fewer["coverage"]["delivery"]["complete"] is None
    # A copy that loses the producer proof does not keep the completion.
    copied = {**final, "coverage": {**final["coverage"],
                                    "delivery": dict(final["coverage"]["delivery"])}}
    assert with_read_coverage(copied)["coverage"]["delivery"]["complete"] is None


def test_absent_coverage_fields_are_empty_or_derivable_never_unknown():
    """The omission rule: empty bounds, a null membership limit, delivered
    rows, the continuation flag and an exact size (= total_count) are left
    out; an unknown size and an unknown completion stay explicit nulls."""
    exact = with_read_coverage(page())["coverage"]
    assert exact == {
        "retrieval": {"status": "unknown", "basis": "producer_count"},
        "selection": {"unit": "record", "basis": "retrieved_candidates"},
        "delivery": {"complete": True}}
    unknown = with_read_coverage(page(total_count_basis="unknown"))["coverage"]
    assert unknown["selection"] == {"unit": "record", "basis": "retrieved_candidates",
                                    "size": None}
    assert unknown["delivery"] == {"complete": None}
    floor = with_read_coverage(page(total_count_basis="lower_bound", rank_capped=True,
                                    selection_limit=5))["coverage"]
    assert floor["selection"] == {"unit": "record", "basis": "retrieved_candidates",
                                  "size": None, "limit": 5}
    assert floor["retrieval"]["bounds"] == ["candidate_cap"]


@pytest.mark.parametrize("changes", [
    {},  # `truncated: false` alone is not a last-page proof
    {"delivery_last_page": False},
    {"delivery_last_page": True, "next_offset": 12},
    {"delivery_last_page": True, "pagination_capped": True},
])
def test_last_page_needs_the_producer_proof_and_no_cursor(changes):
    out = with_read_coverage(page(total_count_basis="unknown", offset=10, **changes))
    assert out["coverage"]["delivery"]["complete"] is not True


@pytest.mark.parametrize("changes", [{"operator_name": "query"}, {"error": "failed"},
                                     {"status": "refused"}, {"status": "error"}, {"operator_name": "inspect"}])
def test_non_read_and_refused_payloads_are_not_annotated(changes):
    original = page(**changes)
    assert with_read_coverage(original) == original


def expand_page(**changes):
    return {"operator_name": "expand", "node_id": "n:hub", "direction": "out",
            "neighbors": [{"edge_id": "e:1"}, {"edge_id": "e:2"}],
            "total_count": 5, "total_count_basis": "exact", "retrieval_exhaustive": True,
            "truncated": True, "offset": 0, "limit": 2, "next_offset": 2, **changes}


def test_one_hop_expand_carries_the_same_coverage_fields():
    """#3201: a one-hop expand page is qualified like filter and map."""
    out = with_read_coverage(expand_page())
    assert out["coverage"]["retrieval"]["status"] == "exhaustive"
    assert out["coverage"]["selection"] == {"unit": "row", "basis": "matching_set"}
    assert out["coverage"]["delivery"]["complete"] is False
    last = with_read_coverage(expand_page(offset=4, neighbors=[{"edge_id": "e:5"}],
                                          truncated=False, next_offset=None))
    assert last["coverage"]["delivery"]["complete"] is True
    unknown = with_read_coverage(expand_page(total_count=0, total_count_basis="unknown"))
    assert unknown["total_count"] is None
    assert unknown["coverage"]["selection"]["size"] is None


def test_a_path_expand_without_neighbors_is_left_alone():
    original = {"operator_name": "expand", "paths": [], "truncated": False}
    assert with_read_coverage(original) == original
