"""Search arguments read as the condition they mean (issue #3666, step 2b).

No database. The rewrite of `source_family` and `profile`, the names a
refusal may state, the calls a refusal may offer, the exact-listing rule
and the first line of a page that has more. The served behaviour is in
test_search_arguments_db.py.
"""
from __future__ import annotations

import collections
import json
from pathlib import Path

import pytest

from okg.substrate.mcp import search_arguments as sa
from okg.substrate.mcp.read_grammar import rewrite_call
from okg.substrate.mcp.response_bounds import (
    _CALL_REWRITE,
    _CallRewrite,
    _proven_servable,
    call_adjusted,
    forget_adjusted,
    note_adjusted,
)
from okg.substrate.mcp.result_text import render
from okg.substrate.mcp.search import SearchCompletenessRefused, require_exact_page
from okg.substrate.mcp.search_profiles import (
    family_fix_hint,
    unknown_profile_refusal,
    unknown_source_refusal,
)
from okg.substrate.mcp.select_vocabulary import call_shape

GEN = "okg:fixture:branch:default:gen:1"
TYPE_T = {"op": "eq", "field": "type", "value": "ticket"}


def _read(**arguments):
    base = dict(
        source_family=None, profile=None, where=None, method="lexical", query="term",
        view=None, declared_families=(), declared_profiles=("content",),
    )
    return sa.read_scope_arguments(**{**base, **arguments})


# ---------- A: a source name sent as source_family ----------


def test_a_source_name_sent_as_source_family_is_a_condition_on_the_source():
    reading = _read(source_family="depot.source-1")
    assert reading.where == {"op": "eq", "field": "source", "value": "depot.source-1"}
    assert reading.source_family is None and reading.source == "depot.source-1"
    assert reading.notes == ({
        "rule": "source_family_as_source",
        "sent": "source_family `depot.source-1`",
        "served": "where source = `depot.source-1`",
    },)


def test_the_callers_where_is_kept_and_the_source_is_anded_to_it():
    reading = _read(source_family=["depot.source-1"], where=TYPE_T, method="grep")
    assert reading.where == {"op": "and", "clauses": [
        TYPE_T, {"op": "eq", "field": "source", "value": "depot.source-1"},
    ]}
    both = _read(source_family="s:1", where={"op": "and", "clauses": [TYPE_T, TYPE_T]})
    assert both.where["clauses"][:2] == [TYPE_T, TYPE_T] and len(both.where["clauses"]) == 3


@pytest.mark.parametrize("arguments", [
    # A declared family is a family.
    {"source_family": "docs", "declared_families": ("docs",)},
    # The deployment's names could not be read: nothing is guessed.
    {"source_family": "depot.source-1", "declared_families": None},
    # Several names, a blank one, one that cannot be printed on a line.
    {"source_family": ["a", "b"]},
    {"source_family": ""},
    {"source_family": "two words"},
    {"source_family": "x\ny"},
    # No query: the lookup pages before it knows a row's source.
    {"source_family": "depot.source-1", "query": None},
    {"source_family": "depot.source-1", "query": "  "},
    # A method that serves no source condition, and the windows read.
    {"source_family": "depot.source-1", "method": "semantic"},
    {"source_family": "depot.source-1", "method": "attrs", "query": None},
    {"source_family": "depot.source-1", "method": "grep", "view": "windows"},
])
def test_a_source_family_with_no_single_reading_is_left_as_sent(arguments):
    reading = _read(**arguments)
    assert reading.notes == () and reading.source is None
    assert reading.source_family == arguments["source_family"] and reading.where is None


# ---------- B: a record type sent as profile ----------


def test_a_record_type_sent_as_profile_is_a_condition_on_the_type():
    reading = _read(profile="ticket", query=None)
    assert reading.where == TYPE_T and reading.profile is None
    assert reading.record_type == "ticket"
    assert reading.notes == ({
        "rule": "profile_as_type", "sent": "profile `ticket`",
        "served": "where type = `ticket`",
    },)


@pytest.mark.parametrize("arguments", [
    {"profile": "content"},                       # a declared profile
    {"profile": "all"},                           # the reserved one
    {"profile": "ticket", "declared_profiles": None},
    {"profile": "ticket", "method": "semantic"},
    {"profile": "ticket", "method": "identifier_mentions"},
    {"profile": "ticket", "method": "grep", "view": "windows"},
    {"profile": "two words"},
])
def test_a_profile_with_no_single_reading_is_left_as_sent(arguments):
    reading = _read(**arguments)
    assert reading.notes == () and reading.profile == arguments["profile"]
    assert reading.where is None


def test_both_arguments_are_read_in_one_call_and_nothing_is_dropped():
    reading = _read(source_family="depot.source-1", profile="ticket", where=TYPE_T)
    assert [clause["field"] for clause in reading.where["clauses"]] == [
        "type", "source", "type"]
    assert [note["rule"] for note in reading.notes] == [
        "source_family_as_source", "profile_as_type"]
    # `all` beside a source name is kept: it still means no profile scope.
    kept = _read(source_family="depot.source-1", profile="all")
    assert kept.profile == "all" and kept.source == "depot.source-1"


# ---------- what the deployment has ----------


def test_a_name_outside_an_incomplete_list_is_unknown_not_absent():
    whole = sa.SourceNames(names=frozenset({"a.b"}), complete=True)
    assert whole.known("a.b") is True and whole.known("a.c") is False
    partial = sa.SourceNames(names=frozenset({"a.b"}), complete=False)
    assert partial.known("a.b") is True and partial.known("a.c") is None
    assert sa.SourceNames().known("a.b") is None


def test_close_names_are_real_names_nearest_first_and_a_handful():
    names = frozenset({f"depot.source-{i}" for i in range(9)} | {"other.history", "x y"})
    close = sa.close_names("depot.source-1x", names)
    assert close[0] == "depot.source-1" and len(close) <= sa.MAX_CLOSE_NAMES
    assert set(close) <= names
    # The part after the last dot finds the same source under another prefix.
    assert sa.close_names("zzz.history", names) == ["other.history"]
    assert sa.close_names("nothing-like-it", frozenset({"abc"})) == []


def test_the_family_hint_never_offers_an_empty_list():
    """The measured hint was "pass one of []" on a deployment with no family."""
    none = family_fix_hint(())
    assert "[]" not in none and "does not declare" in none
    assert 'where={"op": "eq", "field": "source"' in none
    some = family_fix_hint({"docs": 1, "code": 2})
    assert "['code', 'docs']" in some and 'where={"op": "eq", "field": "source"' in some


def test_the_source_refusal_is_one_sentence_with_the_callers_value():
    refusal = unknown_source_refusal("depot.sorce-1", declared=(), close=["depot.source-1"])
    assert refusal.code == "unknown_source_family"
    assert "'depot.sorce-1'" in str(refusal) and str(refusal).count(". ") == 0
    assert "it declares none" in str(refusal) and "[]" not in str(refusal)
    assert refusal.fix_hint.endswith("sources with a close name: depot.source-1")
    assert refusal.data == {"requested": ["depot.sorce-1"], "declared": []}
    declared = unknown_source_refusal("x", declared=("docs",))
    assert "['docs']" in str(declared) and "close name" not in declared.fix_hint


def test_the_profile_refusal_names_the_profiles_and_the_where_on_type():
    refusal = unknown_profile_refusal("tiket", declared=("content",))
    assert refusal.code == "unknown_search_profile"
    assert "'tiket'" in str(refusal) and "['content']" in str(refusal)
    assert "['content'] or 'all'" in refusal.fix_hint
    assert 'where={"op": "eq", "field": "type"' in refusal.fix_hint
    none = unknown_profile_refusal("tiket", declared=())
    assert "[]" not in str(none) + none.fix_hint


# ---------- notes the tool body adds ----------


def test_a_note_the_tool_adds_reaches_the_reply_once_and_can_be_taken_back():
    note = {"rule": "profile_as_type", "sent": "profile `t`", "served": "where type = `t`"}
    assert call_adjusted() == []
    note_adjusted([note])          # outside a call: nothing to note it on
    assert call_adjusted() == []
    token = _CALL_REWRITE.set(_CallRewrite(adjusted=({"rule": "x", "sent": "a", "served": "b"},)))
    try:
        note_adjusted([note])
        note_adjusted([note])      # a byte-fitting rerun of the tool
        assert [record["rule"] for record in call_adjusted()] == ["profile_as_type", "x"]
        forget_adjusted([note])
        assert [record["rule"] for record in call_adjusted()] == ["x"]
    finally:
        _CALL_REWRITE.reset(token)
    line = render({"results": [], "adjusted": [note], "pinned_generation_id": GEN},
                  tool="search").splitlines()[1]
    assert line == "! adjusted: profile `t` read as where type = `t`"


# ---------- C: a type alone, and a list of a lookup ----------


@pytest.mark.parametrize("arguments", [
    {"where": TYPE_T},
    {"where": TYPE_T, "method": "attrs"},
    {"where": TYPE_T, "view": "list"},
    {"where": {"op": "prefix", "field": "path", "value": "docs/"}, "view": "list",
     "method": "lexical"},
])
def test_a_where_with_no_query_has_the_lookups_rows_as_cards_and_as_a_list(arguments):
    assert call_shape("search", arguments) == "attrs"


def test_a_lookup_with_no_condition_is_still_no_shape():
    assert call_shape("search", {"method": "attrs"}) is None
    assert call_shape("search", {}) is None


def test_a_lookup_list_resolves_the_list_defaults_and_takes_the_lookups_select():
    from okg.substrate.mcp.search_views import (
        SearchViewUnsupported,
        attrs_list_projection,
        resolve_search_view,
    )

    def resolve(**extra):
        return resolve_search_view(
            "list", **{"method": "attrs", "rank_by": None,
                       "max_per_parent_argument": False, "limit": None, "select": None,
                       **extra},
        )

    assert resolve() == (100, ["node_id", "subtype", "handle"])
    assert resolve(limit=30, select=["node_id", "attrs"]) == (30, ["node_id", "attrs"])
    with pytest.raises(SearchViewUnsupported):
        resolve(limit=101)
    with pytest.raises(SearchViewUnsupported):
        resolve(method="semantic")
    page = attrs_list_projection({"results": [
        {"node_id": "n:1", "subtype": "t", "evidence_kind": "unknown",
         "evidence_kind_provenance": {"x": 1}, "attrs": {"a": 1}},
    ]}, selected=["node_id", "attrs"])
    assert page["view"] == "list"
    assert page["results"] == [{"node_id": "n:1", "subtype": "t", "attrs": {"a": 1}}]
    text = render({**page, "method": "attrs", "total_count": 1, "total_count_basis": "exact",
                   "pinned_generation_id": GEN}, tool="search").splitlines()
    # #3666: a whole first page reads "N records, all shown" (was "1-1 of 1
    # · complete"); a list row without a head keeps its one-line form.
    assert text[0].startswith("search ok · attrs · list · 1 record, all shown")
    assert text[1].startswith("[1] n:1 (t)") and 'attrs={"a":1}' in text[1]


# ---------- D: an exact listing that takes more than one page ----------


def _attrs_page(**fields):
    return {"method": "attrs", "results": [{"node_id": "n:1", "subtype": "t"}],
            "offset": 0, "limit": 1, "truncated": False, "next_offset": None,
            "total_count": 1, "total_count_basis": "exact", "pinned_generation_id": GEN,
            **fields}


def test_one_whole_page_is_exact_and_counted_as_before():
    page = _attrs_page()
    require_exact_page(page, method="attrs")
    assert page["completeness"] == "exact" and "listing" not in page
    assert page["total_count"] == 1 and page["total_count_basis"] == "exact"


def test_a_page_of_a_longer_listing_never_says_it_is_every_match():
    first = _attrs_page(truncated=True, next_offset=1, total_count_basis="lower_bound")
    require_exact_page(first, method="attrs")
    assert first["completeness"] == "exact_over_pages"
    assert first["listing"] == {"complete": False, "reason": "more_matches"}
    head = render(first, tool="search").splitlines()[0]
    # #3666: "bound reached (limit 1)" (the call's own limit) is no longer
    # said on search; the listing claim and the exact continuation are.
    assert head == (
        "search ok · attrs · rows 1-1, more follow · listing continues (the pages "
        'together are every match) · next: offset=1 '
        'completeness="exact"'  # D8 (read-passage-rows-3666): no generation
    )
    last = _attrs_page(offset=1, total_count=None, total_count_basis="unknown")
    require_exact_page(last, method="attrs")
    assert last["listing"] == {"complete": True, "reason": "last_page"}
    assert last["total_count"] is None
    head = render(last, tool="search").splitlines()[0]
    assert "listing complete (the pages up to this one are every match)" in head
    assert "listing" not in "\n".join(render(last, tool="search").splitlines()[1:])


@pytest.mark.parametrize(("fields", "bound"), [
    # The access filter stopped the scan: no cursor, so paging cannot go on.
    ({"truncated": True, "read_filter_scan_bound": 5000, "pagination_capped": True},
     "read_filter_scan_bound"),
    # Past the offset ceiling.
    ({"truncated": True, "pagination_capped": True, "offset": 10}, "offset_ceiling"),
])
def test_a_lookup_no_paging_can_finish_is_still_refused(fields, bound):
    page = _attrs_page(**fields)
    with pytest.raises(SearchCompletenessRefused) as refused:
        require_exact_page(page, method="attrs")
    assert refused.value.data["bound"] == bound
    assert "narrow the where" in refused.value.fix_hint and "completeness" not in page


def test_every_listing_reason_is_one_of_the_closed_set():
    from okg.substrate.mcp.search import LISTING_REASONS

    assert LISTING_REASONS == ("more_matches", "scan_bound", "last_page")


# ---------- E: the first line of a page that has more ----------


def test_a_floor_that_is_only_this_pages_rows_reads_more_follow_with_the_offset():
    page = {"items": [{"node_id": "n:1", "subtype": "t"}] * 71, "total_count": 71,
            "total_count_basis": "lower_bound", "truncated": True, "limit": 100,
            "offset": 0, "next_offset": 71, "pinned_generation_id": GEN}
    head = render(page, tool="filter").splitlines()[0]
    # Before: "filter ok · 1-71 of 71+ · bound reached (limit 100) · next: offset=71".
    assert head == (
        "filter ok · 1-71, more follow · bound reached (limit 100) · next: offset=71"
    )  # D8 (read-passage-rows-3666): no generation


@pytest.mark.parametrize("page", [
    # A real floor from a rank cap keeps its number and its `+`.
    {"method": "lexical", "results": [{"node_id": "n:1", "subtype": "t"}],
     "total_count": 200, "total_count_basis": "lower_bound", "limit": 1, "offset": 0,
     "next_offset": 1},
    # A floor nothing continues: no offset to send, so no "more follow".
    {"method": "lexical", "results": [{"node_id": "n:1", "subtype": "t"}],
     "total_count": 1, "total_count_basis": "lower_bound", "limit": 1, "offset": 0,
     "next_offset": None},
])
def test_a_count_that_says_more_than_its_rows_keeps_its_plus(page):
    head = render({**page, "pinned_generation_id": GEN}, tool="search").splitlines()[0]
    assert f"of {page['total_count']}+" in head and "more follow" not in head


# ---------- F: the calls a refusal may offer ----------


@pytest.mark.parametrize("arguments", [
    {"query": "term", "scope": {"record": "rec:1"}, "rank_by": "self", "limit": 20},
    {"query": "term", "method": "lexical", "scope": {"records": ["rec:1", "rec:2"]}},
    {"query": "term", "method": "grep", "scope": {"record": "rec:1"}, "view": "windows"},
    {"query": "term", "method": "grep", "matching": "record", "view": "windows", "limit": 5},
    {"query": "term", "method": "grep", "context_chars": 500, "view": "windows"},
])
def test_a_scope_or_windows_call_the_checks_take_is_proven(arguments):
    assert _proven_servable("search", arguments, ceiling=None) is True


@pytest.mark.parametrize("arguments", [
    # The forms the tool refuses: none may be offered.
    {"query": "term", "method": "grep", "scope": {"record": "rec:1"}},
    {"query": "term", "method": "grep", "matching": "record"},
    {"query": "term", "scope": {"record": "rec:1"}, "rank_by": "passages"},
    {"query": "term", "scope": {"children_of": "rec:1"}},
    {"query": "term", "rank_by": "self"},
    {"query": "term", "view": "list"},
    {"query": "term", "context_chars": 100},
    {"query": "term", "method": "semantic", "scope": {"record": "rec:1"}},
])
def test_a_call_the_checks_do_not_cover_is_not_proven(arguments):
    assert _proven_servable("search", arguments, ceiling=None) is False


@pytest.mark.parametrize("arguments", [
    # The windows read refuses these beside its view; the proof asks it.
    {"query": "term", "method": "grep", "view": "windows", "select": ["node_id"]},
    {"query": "path:x", "method": "grep", "view": "windows"},
    {"query": "term", "method": "grep", "view": "windows", "context_chars": 99999},
    {"query": "term", "scope": {"record": ""}},
])
def test_a_windows_call_its_own_check_refuses_is_not_proven(arguments):
    from okg.substrate.errors import SubstrateError

    try:
        assert _proven_servable("search", arguments, ceiling=None) is False
    except SubstrateError:
        # The server's own wrapper reads a refusal here as "not proven".
        pass


def test_grep_with_a_path_prefix_is_proven_and_a_ranked_search_with_one_is_not():
    grep = {"query": "term", "method": "grep", "path_prefix": "docs/", "limit": 5}
    assert _proven_servable("search", grep, ceiling=None) is True
    assert _proven_servable("search", {**grep, "path_prefix": " "}, ceiling=None) is False
    assert _proven_servable(
        "search", {**grep, "view": "windows"}, ceiling=None,
    ) is False
    assert _proven_servable(
        "search", {"query": "term", "scope": {"record": "rec:1"}, "path_prefix": "docs/"},
        ceiling=None,
    ) is False


# ---------- M4: window arguments read as the windows view ----------


@pytest.mark.parametrize(("extra", "named"), [
    ({"context_chars": 300}, "context_chars"),
    ({"matching": "record"}, "matching"),
    ({"scope": {"records": ["rec:1", "rec:2"]}}, "scope"),
    ({"context_chars": 300, "matching": "record"}, "context_chars and matching"),
])
def test_a_grep_call_with_a_windows_argument_and_no_view_is_the_windows_read(extra, named):
    rewritten = rewrite_call("search", {"query": "term", "method": "grep", "limit": 3, **extra})
    assert rewritten.arguments["view"] == "windows"
    assert [note["rule"] for note in rewritten.adjusted] == ["grep_windows_view"]
    assert rewritten.adjusted[0]["sent"] == f"{named} on grep with no view"


@pytest.mark.parametrize("arguments", [
    # The windows read refuses another argument: left for the tool's words.
    {"query": "term", "method": "grep", "context_chars": 300, "where": TYPE_T},
    {"query": "term", "method": "grep", "context_chars": 300, "path_prefix": "docs/"},
    # Not grep, a view already named, or nothing only windows takes.
    {"query": "term", "context_chars": 300},
    {"query": "term", "method": "grep", "context_chars": 300, "view": "cards"},
    {"query": "term", "method": "grep", "matching": "passage"},
    {"query": "term", "method": "grep"},
    # The windows read refuses these: the caller would be refused about a
    # view it never sent (review of 283fa6ea484).
    {"query": "term", "method": "grep", "context_chars": 300, "completeness": "exact"},
    {"query": "term", "method": "grep", "matching": "record", "continuation_token": "t"},
])
def test_a_call_that_is_not_only_the_windows_read_is_left_as_sent(arguments):
    rewritten = rewrite_call("search", arguments)
    assert rewritten.arguments.get("view") == arguments.get("view")
    assert "grep_windows_view" not in [note["rule"] for note in rewritten.adjusted]


# ---------- M4: the lookup's SQL page and the bounded-page rule ----------


def test_a_prefix_or_suffix_on_an_attribute_narrows_the_lookup_page_in_sql():
    from okg.substrate.mcp import search_where as sw

    prefix = {"op": "prefix", "field": "attrs.name", "value": "release-41"}
    suffix = {"op": "suffix", "field": "attrs.name", "value": ".0"}
    where = {"op": "and", "clauses": [TYPE_T, prefix, suffix]}
    # No catalog context is needed: before, the return for a missing context
    # came first and the prefix was checked on the first `limit` rows only.
    clauses = sw.lookup_sql_clauses(where, None, masking=False, explicit_subtypes=True)
    assert prefix in clauses and suffix in clauses
    assert sw.lookup_sql_exact(where, masking=False) is True
    # Under access rules nothing reads a stored value in SQL.
    masked = sw.lookup_sql_clauses(where, None, masking=True, explicit_subtypes=True)
    assert prefix not in masked and suffix not in masked
    assert sw.lookup_sql_exact(where, masking=True) is False


@pytest.mark.parametrize("clause", [
    {"op": "gt", "field": "attrs.summary", "value": "a"},
    {"op": "eq", "field": "source", "value": "main"},
    {"op": "prefix", "field": "title", "value": "Doc"},
    {"op": "not", "clause": {"op": "prefix", "field": "attrs.name", "value": "x"}},
])
def test_a_condition_sql_does_not_decide_makes_the_lookup_count_inexact(clause):
    from okg.substrate.mcp import search_where as sw

    assert sw.lookup_sql_exact({"op": "and", "clauses": [TYPE_T, clause]}, masking=False) is False


def _bounded_page(**extra) -> dict:
    return {
        "operator_name": "search", "method": "attrs", "pinned_generation_id": GEN,
        "results": [], "offset": 0, "limit": 3, "next_offset": 3, "truncated": True,
        "total_count": None, "total_count_basis": "unknown",
        "not_a_total": {"reason": "the page limit (3 stored rows)", "records_read": 3,
                        **extra},
    }


def test_a_bounded_page_says_it_is_not_a_total_and_names_the_call_that_counts():
    call = {"tool": "filter", "mode": "corrected", "arguments": {
        "input": {"kind": "nodes", "subtype": "git_tag"},
        "predicate": {"op": "gt", "field": "attrs.name", "value": "a"}}}
    lines = render(_bounded_page(for_a_total=call), tool="search").splitlines()
    assert "0 on this page of 3 read, not a total" in lines[0], lines[0]
    assert "0 of" not in lines[0] and "count unknown" not in lines[0]
    assert lines[1] == (
        "! not a total: the read stopped at the page limit (3 stored rows); for a total: "
        'filter(input={"kind":"nodes","subtype":"git_tag"}, '
        'predicate={"op":"gt","field":"attrs.name","value":"a"})'
    )
    assert "not_a_total" not in render(_bounded_page(for_a_total=call), tool="search")


def test_a_bounded_page_with_no_call_that_counts_says_to_narrow():
    lines = render(_bounded_page(), tool="search").splitlines()
    assert lines[1] == (
        "! not a total: the read stopped at the page limit (3 stored rows); "
        "narrow the call so it reads every match"
    )


def test_a_windows_page_with_undecided_records_keeps_its_plus():
    page = {"operator_name": "search", "method": "grep", "view": "windows",
            "pinned_generation_id": GEN, "results": [{"node_id": "n:1"}, {"node_id": "n:2"}],
            "offset": 0, "limit": 10, "total_count": 2, "total_count_basis": "lower_bound",
            "undecided_records": ["n:3"], "continuation_token": "tok"}
    head = render(page, tool="search").splitlines()[0]
    assert "1-2 of 2+" in head and "more follow" not in head, head


# ---------- the replay, by the rewrite alone ----------


def _outcome(row: dict, data: dict) -> str:
    code, arguments = row["refusal_code"], row["arguments"]
    call = rewrite_call("search", arguments).arguments
    reading = sa.read_scope_arguments(
        source_family=call.get("source_family"), profile=call.get("profile"),
        where=call.get("where"), method=call.get("method", "lexical"),
        query=call.get("query"), view=call.get("view"),
        declared_families=data["families"], declared_profiles=data["profiles"],
    )
    if code in ("unknown_source_family", "unknown_search_profile"):
        if not reading.notes:
            return "refused_in_words"
        known = (
            (reading.source is None or reading.source in data["sources"])
            and (reading.record_type is None or reading.record_type in data["types"])
        )
        return "served_with_a_note" if known else "refused_in_words"
    if code in ("type_only_lookup", "search_view_unsupported"):
        if call.get("view") == "list" and call.get("query"):
            return "served_by_an_earlier_step"   # a `select` on a list (3a)
        return "served" if call_shape("search", call) == "attrs" else "refused_in_words"
    if code == "search_completeness_refused":
        return "refused_in_words" if call.get("view") == "windows" else "served_page_by_page"
    if code == "path_prefix_not_applicable":
        # The server's rule (issue #3666 M4): the grep form, as a guess, when
        # the call holds no condition grep would drop.
        allowed = {"query", "method", "limit", "path_prefix", "rank_by",
                   "select", "max_chars", "max_bytes"}
        if not set(call) <= allowed:
            return "refused_in_words"
        grep = {key: call[key] for key in ("query", "limit", "path_prefix", "max_bytes")
                if key in call}
        grep["method"] = "grep"
        try:
            offered = not rewrite_call("search", grep).adjusted and _proven_servable(
                "search", grep, ceiling=None,
            )
        except Exception:  # noqa: BLE001 - a refusal of the candidate: no offer.
            offered = False
        return "refused_with_a_call" if offered else "refused_in_words"
    if call.get("view") == "windows":
        return "served_by_an_earlier_step"       # a `select` on windows (3a)
    if code == "search_scope_unsupported" and call.get("rank_by") == "passages":
        corrected = {**call, "rank_by": "self"}
    else:
        corrected = {**call, "view": "windows"}
    try:
        offered = (
            call.get("method") != "identifier_mentions"
            and not rewrite_call("search", corrected).adjusted
            and _proven_servable(
                "search", rewrite_call("search", corrected).arguments, ceiling=None,
            )
        )
    except Exception:  # noqa: BLE001 - a refusal of the candidate: no offer.
        offered = False
    return "refused_with_a_call" if offered else "refused_in_words"


def test_how_many_refused_search_calls_of_the_replay_this_step_serves_an_upper_bound():
    """88 refused search calls of two agent runs (values replaced by neutral
    ones), through the rules of this step alone.

    The counts are an UPPER BOUND on what the server serves: the rules read
    the arguments and the deployment's declared names only, and the tool may
    still refuse a call for another reason (a source the read path cannot
    list, a grep page whose continuation is withheld). One call of each
    class is dispatched in test_search_arguments_db.py.
    """
    data = json.loads(
        (Path(__file__).parent / "data" / "search_argument_refusals_3666.json").read_text()
    )
    counts: collections.Counter = collections.Counter()
    for row in data["rows"]:
        counts[(row["refusal_code"], _outcome(row, data))] += row["calls"]
    assert sum(counts.values()) == 88
    assert dict(counts) == EXPECTED_REPLAY


EXPECTED_REPLAY: dict = {
    # A source name as `source_family`; one call sent a blank name.
    ("unknown_source_family", "served_with_a_note"): 18,
    ("unknown_source_family", "refused_in_words"): 1,
    ("unknown_search_profile", "served_with_a_note"): 10,
    # A where on the type alone, and view="list" over a where with no query.
    ("type_only_lookup", "served"): 7,
    ("search_view_unsupported", "served"): 6,
    ("search_view_unsupported", "served_by_an_earlier_step"): 2,
    # Listings under completeness="exact"; one asked it of a windows read.
    ("search_completeness_refused", "served_page_by_page"): 23,
    ("search_completeness_refused", "refused_in_words"): 1,
    # scope with rank_by="passages": the call with rank_by="self". The grep
    # calls with a scope also carry a select or an offset the windows read
    # does not take as written, so they get words.
    ("search_scope_unsupported", "refused_with_a_call"): 4,
    ("search_scope_unsupported", "refused_in_words"): 5,
    # matching or context_chars without the view, beside a select, a where
    # or a path_prefix the windows read refuses: words that name it.
    ("window_argument_unsupported", "refused_in_words"): 5,
    ("window_argument_unsupported", "served_by_an_earlier_step"): 3,
    # Still refused (ranked terms and exact text are two readings), now with
    # the grep form offered as a guess (issue #3666 M4).
    ("path_prefix_not_applicable", "refused_with_a_call"): 3,
}


# ---------- a grep page's token sent back without `method` (v10 G2) ----------


def _grep_token() -> str:
    from okg.substrate.mcp.continuation import issue_continuation

    return issue_continuation(
        {"kind": "grep_page", "sort": "grep", "projection": "p"}, cursor={"sealed": "x"},
    )


def test_a_grep_page_token_without_method_is_read_as_grep():
    """18 of 18 replayed calls (Format v10 item G2): the token a grep page
    printed came back without `method`; search defaulted to lexical and
    refused it (`continuation_token_not_applicable`). It now reads as grep,
    with an `adjusted` note."""
    method, notes = sa.read_continuation_method("lexical", _grep_token(), method_sent=False)
    assert method == "grep"
    assert [note["rule"] for note in notes] == [sa.RULE_CONTINUATION_TOKEN_METHOD]
    assert notes[0]["served"] == 'method="grep"'
    # An in-process call (sent arguments unknown) reads the same.
    assert sa.read_continuation_method("lexical", _grep_token(), method_sent=None)[0] == "grep"


def test_an_explicit_method_or_a_token_of_another_kind_is_left_as_sent():
    """Failure cases: an explicit other method stays (and is refused by
    name); a token that does not verify, or of another kind, changes nothing."""
    from okg.substrate.mcp.continuation import issue_continuation

    assert sa.read_continuation_method("lexical", _grep_token(), method_sent=True) == (
        "lexical", (),
    )
    assert sa.read_continuation_method("lexical", "a.b", method_sent=False) == ("lexical", ())
    other = issue_continuation({"kind": "inspect_text"}, cursor={})
    assert sa.read_continuation_method("lexical", other, method_sent=False) == ("lexical", ())
    assert sa.read_continuation_method("lexical", None, method_sent=False) == ("lexical", ())
    assert sa.continuation_kind(_grep_token()) == "grep_page"
    assert sa.continuation_kind("a.b") is None


def test_the_types_a_where_admits_are_its_top_level_type_conjuncts():
    """Grep narrows its scan to these types and the passages declared under
    them (Format v10 item G2); a type under `or`/`not` narrows nothing."""
    from okg.substrate.mcp import search_where as sw

    repo = {"op": "eq", "field": "attrs.repo", "value": "r"}
    assert sw.admitted_types({"op": "and", "clauses": [TYPE_T, repo]}) == {"ticket"}
    both = {"op": "and", "clauses": [
        {"op": "in", "field": "type", "values": ["ticket", "doc"]},
        {"op": "in", "field": "type", "values": ["doc", "x"]},
    ]}
    assert sw.admitted_types(both) == {"doc"}
    assert sw.admitted_types(repo) is None
    assert sw.admitted_types(None) is None
    assert sw.admitted_types({"op": "or", "clauses": [TYPE_T, repo]}) is None
    assert sw.admitted_types({"op": "not", "clause": TYPE_T}) is None
