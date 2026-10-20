"""Search options mean the same thing for every method that can serve them
(PACT search-options-by-method, issue #3646).

In lane X's structural pilot 19% of okg calls were refused, most for search
options a method could serve but did not, or refused without naming the call
that works. Each case below drives the real server over an MCP client
session: a newly served option is accepted, and a refused one names a call
that, made as written, is accepted.
"""
from __future__ import annotations

import pytest

from tests.substrate.mcp.test_search_list import TERM, Served, _corpus

pytestmark = pytest.mark.postgres

# method=attrs needs a predicate on attributes: twenty documents by title.
DOC_WHERE = {"op": "in", "field": "attrs.summary",
             "values": [f"Document {i:03d}" for i in range(20)]}
# One opening day: about five documents, decided in SQL (an `in` is checked
# after a bounded scan, so its page cannot prove it read every match).
DAY_WHERE = {"op": "eq", "field": "attrs.opened_at", "value": "2026-03-08T09:30:00Z"}


@pytest.fixture
def served(clean_runtime, okg_mcp_can_log_in, tmp_path):
    server = Served(clean_runtime, _corpus(tmp_path, clean_runtime))
    try:
        yield server
    finally:
        server.close()


def _ok(page: dict) -> dict:
    assert page.get("status") != "refused", page.get("error")
    return page


def _refused(page: dict, code: str) -> dict:
    assert page.get("status") == "refused", page
    assert page["error"]["code"] == code, page["error"]
    return page["error"]


# ---------- fields ----------


def test_attrs_serves_the_record_fields_lexical_serves(served):
    page = _ok(served.json(where=DOC_WHERE, limit=5,
                           select=["node_id", "canonical_node_id", "citation", "source_family"]))
    rows = page["results"]
    assert rows and all(row["canonical_node_id"] == row["node_id"] for row in rows)
    assert all("source_family" in row for row in rows)


def test_grep_accepts_location(served):
    _ok(served.json(query=TERM, method="grep", limit=3, select=["node_id", "location"]))


def test_identifier_mentions_names_the_call_that_finds_the_record(served):
    error = _refused(served.json(query="see doc:000", method="identifier_mentions",
                                 select=["node_id", "subtype"]), "select_unknown_field")
    assert error["message"].startswith("search(method=identifier_mentions)")
    assert "spans of your query text" in error["message"]
    # The caller's own query is in the call (#3666): no placeholder.
    assert 'search(method="grep", query="see doc:000")' in error["fix_hint"]
    _ok(served.json(query="doc:000", method="grep", limit=3))


def test_attrs_excerpt_names_attrs_or_a_text_read(served):
    error = _refused(served.json(where=DOC_WHERE, select=["excerpt"]),
                     "select_unknown_field")
    assert "no query to excerpt" in error["fix_hint"]
    # Beside a field the lookup has, `excerpt` is left out and named (#3666).
    page = _ok(served.json(where=DOC_WHERE, limit=2, select=["node_id", "excerpt"]))
    assert page["adjusted"][0]["sent"] == "select `excerpt`"
    assert all("excerpt" not in row for row in page["results"])
    _ok(served.json(where=DOC_WHERE, limit=2, select=["node_id", "attrs"]))


# ---------- options ----------


@pytest.mark.parametrize("arguments", [
    {"query": TERM, "method": "grep"},
    {"where": DOC_WHERE},
])
def test_bounded_completeness_is_accepted_off_lexical(served, arguments):
    page = _ok(served.json(limit=3, completeness="bounded", **arguments))
    assert page.get("completeness") == "bounded"


def test_exact_grep_keeps_a_page_that_read_every_match(served):
    # One record holds this text, so grep reads every visible match.
    page = _ok(served.json(query="body 7.", method="grep", completeness="exact"))
    assert page["completeness"] == "exact"
    assert page["total_count_basis"] == "exact"


def test_exact_attrs_keeps_a_whole_page_and_counts_it(served):
    page = _ok(served.json(where={"op": "eq", "field": "attrs.summary",
                                  "value": "Document 007"}, completeness="exact"))
    assert page["completeness"] == "exact"
    assert page["total_count"] == len(page["results"]) == 1
    assert page["total_count_basis"] == "exact"


def test_exact_attrs_serves_a_listing_that_takes_more_than_one_page(served):
    """#3666 step 2b: 16 of 21 exact refusals in a real run were listings
    that did not fit one page. The page is served, says the listing
    continues, and never says it is every match."""
    first = _ok(served.json(where=DOC_WHERE, limit=5, completeness="exact"))
    assert first["completeness"] == "exact_over_pages"
    assert first["listing"] == {"complete": False, "reason": "more_matches"}
    assert first["next_offset"] == 5 and first["total_count_basis"] != "exact"
    # One line says the rows, that more follow, and the offset to send. An
    # equality the lookup decides in SQL, so the first page knows its rows.
    head = served.text(where=DAY_WHERE, limit=2, completeness="exact").split("\n", 1)[0]
    assert "1-2, more follow" in head and "listing continues" in head, head
    assert head.split(" · generation ")[0].endswith('next: offset=2 completeness="exact"'), head
    # The same rows as the page asked without completeness.
    bounded = _ok(served.json(where=DOC_WHERE, limit=5))
    assert [r["node_id"] for r in first["results"]] == [r["node_id"] for r in bounded["results"]]


def test_grep_cards_accept_matching_passage(served):
    _ok(served.json(query=TERM, method="grep", limit=3, matching="passage"))


def test_grep_cards_with_matching_record_are_read_as_the_windows_call(served):
    # Issue #3666 M4: only the windows read takes matching="record", so the
    # call is served as that read with a note (before: refused, naming it).
    page = _ok(served.json(query=TERM, method="grep", matching="record"))
    assert page["view"] == "windows"
    assert [note["rule"] for note in page["adjusted"]] == ["grep_windows_view"]
    _ok(served.json(query=TERM, method="grep", view="windows", matching="record"))
    _ok(served.json(query=TERM, method="grep", view="windows",
                    scope={"record": "doc:000"}, matching="record"))


def test_grep_scope_is_read_as_the_windows_read_and_it_is_accepted(served):
    # Issue #3666 M4: served as the windows read with a note (was refused).
    page = _ok(served.json(query=TERM, method="grep", scope={"record": "doc:000"}))
    assert page["view"] == "windows"
    assert [note["rule"] for note in page["adjusted"]] == ["grep_windows_view"]
    _ok(served.json(query=TERM, method="grep", view="windows", scope={"record": "doc:000"}))
    _ok(served.json(query=TERM, scope={"record": "doc:000"}))


def test_identifier_mentions_scope_says_drop_it(served):
    error = _refused(served.json(query="doc:000", method="identifier_mentions",
                                 scope={"record": "doc:000"}), "search_scope_unsupported")
    assert error["fix_hint"] == "drop scope"
    _ok(served.json(query="doc:000", method="identifier_mentions"))


def test_max_per_parent_off_lexical_is_refused_not_dropped(served):
    _refused(served.json(query=TERM, method="grep", max_per_parent=1),
             "diversity_not_applicable")
    _ok(served.json(query=TERM, max_per_parent=1, limit=3))


def test_exact_attrs_serves_a_later_page_without_counting_the_set(served):
    """Review r1: rows the read check drops before the offset cannot be
    counted, so a later page is never called exact. Since #3666 step 2b it
    is served as the last page of the listing, with no count."""
    later = _ok(served.json(where=DAY_WHERE, limit=50, offset=1, completeness="exact"))
    assert later["completeness"] == "exact_over_pages"
    assert later["listing"] == {"complete": True, "reason": "last_page"}
    assert later["total_count"] is None
    page = _ok(served.json(where=DAY_WHERE, limit=50, completeness="exact"))
    assert page["completeness"] == "exact" and "listing" not in page
    assert page["total_count"] == len(page["results"]) >= 4
    assert len(later["results"]) == len(page["results"]) - 1


def test_max_per_parent_with_a_where_and_no_query_says_add_query_text(served):
    error = _refused(served.json(where=DOC_WHERE, max_per_parent=1),
                     "diversity_not_applicable")
    assert "add query text" in error["fix_hint"]


def test_exact_is_refused_on_a_windows_read_not_dropped(served):
    arguments = {"query": TERM, "method": "grep", "view": "windows", "limit": 2}
    error = _refused(served.json(completeness="exact", **arguments),
                     "search_completeness_refused")
    assert error["data"]["bound"] == "view='windows'"
    assert "drop completeness" in error["fix_hint"]
    # both calls the hint names are accepted
    _ok(served.json(**arguments))
    _ok(served.json(query="body 7.", method="grep", completeness="exact"))


@pytest.mark.parametrize(("page", "continued", "bound"), [
    # Stopped at a bound with nothing that continues it.
    ({"page_basis": "scan_bound_reached", "total_count_basis": "exact"},
     False, "scan_bound_reached"),
    ({"page_basis": "scan_bound_reached", "continuation_withheld": "listed_cap"},
     False, "continuation withheld (listed_cap)"),
    ({"page_basis": "timed_out"}, True, "timed_out"),
    ({"page_basis": "top_of_all_visible_matches", "total_count_basis": "exact",
      "recheck_dropped": 2}, False, "rows dropped by the read check"),
    ({"page_basis": "top_of_all_visible_matches", "total_count_basis": "lower_bound",
      "count_basis_reasons": ["scan_timeout"]}, False, "count lower_bound (scan_timeout)"),
])
def test_exact_grep_refuses_each_page_that_did_not_read_every_match(page, continued, bound):
    from okg.substrate.mcp.search import SearchCompletenessRefused, require_exact_page

    with pytest.raises(SearchCompletenessRefused) as refused:
        require_exact_page(dict(page), method="grep", continued=continued)
    assert refused.value.data["bound"] == bound
    assert "narrow the search" in refused.value.fix_hint
    assert "completeness" not in page


@pytest.mark.parametrize(("page", "continued", "listing"), [
    ({"page_basis": "scan_bound_reached", "continuation_token": "t"}, False,
     {"complete": False, "reason": "scan_bound"}),
    ({"page_basis": "top_of_all_visible_matches", "continuation_token": "t"}, True,
     {"complete": False, "reason": "more_matches"}),
    ({"page_basis": "top_of_all_visible_matches"}, True,
     {"complete": True, "reason": "last_page"}),
])
def test_exact_grep_serves_a_page_its_continuation_completes(page, continued, listing):
    from okg.substrate.mcp.search import require_exact_page

    page = dict(page)
    require_exact_page(page, method="grep", continued=continued)
    assert page["completeness"] == "exact_over_pages" and page["listing"] == listing


@pytest.mark.parametrize(("has_type", "has_path_prefix", "named", "absent"), [
    (True, False, "path_prefix", "where on type"),
    (False, True, "a where on type", "path_prefix"),
    (True, True, "a rarer term", "where on type"),
])
def test_the_grep_hint_names_only_what_the_call_lacks(has_type, has_path_prefix, named, absent):
    """A real call that already had a where on type was told to add one."""
    from okg.substrate.mcp.search import SearchCompletenessRefused, require_exact_page

    with pytest.raises(SearchCompletenessRefused) as refused:
        require_exact_page({"page_basis": "timed_out"}, method="grep",
                           has_type=has_type, has_path_prefix=has_path_prefix)
    hint = refused.value.fix_hint.split(" so grep reads")[0]
    assert named in hint and absent not in hint, hint


def test_an_exact_grep_page_that_is_not_the_whole_set_says_so_first(served):
    """Lane X2, pilot question pq03: an agent read a grep exact page of 4
    rows out of 35 as the full list. `exact` is about the count; a page that
    holds fewer rows says so on its first line and never says complete."""
    page = _ok(served.json(query=TERM, method="grep", limit=2, completeness="exact"))
    assert page["total_count"] > len(page["results"]) == 2
    head = served.text(query=TERM, method="grep", limit=2, completeness="exact").split("\n", 1)[0]
    assert f"of {page['total_count']}" in head, head
    # #3666: a search page that continues says its rows and its next call
    # ("bound reached (limit 2)", the call's own limit, is no longer said).
    assert f"rows 1-2 of {page['total_count']}" in head and "next:" in head, head
    assert "complete" not in head.replace("completeness", ""), head
