"""Search arguments and listings through the served tool (issue #3666, 2b).

Each test sends a shape real agents were refused for (the shapes of
tests/substrate/mcp/data/search_argument_refusals_3666.json) to a real
server over a publish, and reads the reply as an MCP client receives it. A
served call returns exactly what the corrected call returns, with a note; a
refused call's offered call is dispatched and must be served.
"""
from __future__ import annotations

import json

import pytest

from tests.substrate.mcp.test_read_grammar_db import (
    _DENY_EXTERNAL,
    Tools,
    _dispatched,
    _same,
    _served,
    _under_enforcement,
)
from tests.substrate.mcp.test_search_list import (
    EXTERNAL,
    MAIN,
    READER,
    TERM,
    _activate,
    _corpus,
)

pytestmark = pytest.mark.postgres

TYPE_DOC = {"op": "eq", "field": "type", "value": "doc"}
TYPE_TICKET = {"op": "eq", "field": "type", "value": "ticket"}
SOURCE_MAIN = {"op": "eq", "field": "source", "value": MAIN}
# The corpus stores paths `docs/NNN.md`. Path prefixes here are never the bare
# repository directory name with a slash, which the lane-scope check reads as a
# read of the checkout's docs (tests/ci/test_lane_scope_db_closure.py).


@pytest.fixture
def tools(clean_runtime, okg_mcp_can_log_in, tmp_path):
    server = Tools(clean_runtime, _corpus(tmp_path, clean_runtime))
    try:
        yield server
    finally:
        server.close()


def _reply(tools: Tools, arguments: dict, tool: str = "search") -> dict:
    (result,) = tools.run((tool, arguments, "json"))
    return json.loads(result.content[0].text)


def _refused(tools: Tools, arguments: dict, code: str) -> dict:
    reply = _reply(tools, arguments)
    assert reply.get("status") == "refused", json.dumps(reply)[:600]
    assert reply["error"]["code"] == code, reply["error"]
    assert "results" not in reply
    return reply


def _noted(tools: Tools, sent: dict, corrected: dict, rules: list[str]) -> dict:
    """`sent` is served as `corrected` is, row for row, plus the note."""
    new, old = tools.pair("search", sent, corrected)
    _served(old)
    assert "adjusted" not in old
    assert [record["rule"] for record in _served(new)["adjusted"]] == rules, new["adjusted"]
    assert _same(new, without=("adjusted",)) == _same(old), sent
    return new


def _ids(reply: dict, key: str = "results") -> list[str]:
    return [str(row["node_id"]) for row in _served(reply)[key]]


# ---------- A: a source name sent as source_family ----------


@pytest.mark.parametrize("extra", [
    {},
    {"profile": "all", "select": ["node_id", "handle", "excerpt"]},
    {"method": "grep", "limit": 20},
])
def test_a_source_name_as_source_family_is_served_as_a_where_on_the_source(tools, extra):
    sent = {"query": TERM, "source_family": MAIN, "limit": 5, **extra}
    corrected = {"query": TERM, "where": SOURCE_MAIN, "limit": 5, **extra}
    new = _noted(tools, sent, corrected, ["source_family_as_source"])
    assert new["results"]
    assert new["adjusted"][0]["served"] == f"where source = `{MAIN}`"
    text = tools.text("search", sent)
    assert f"! adjusted: source_family `{MAIN}` read as where source = `{MAIN}`" in text


def test_the_callers_where_is_kept_beside_the_source(tools):
    sent = {"query": TERM, "where": TYPE_TICKET, "source_family": MAIN, "limit": 50}
    corrected = {"query": TERM, "limit": 50,
                 "where": {"op": "and", "clauses": [TYPE_TICKET, SOURCE_MAIN]}}
    new = _noted(tools, sent, corrected, ["source_family_as_source"])
    assert _ids(new) == ["ticket:1"]


def test_a_name_that_is_no_family_and_no_listed_source_is_refused_with_what_exists(tools):
    sent = {"query": TERM, "source_family": MAIN[:-1], "limit": 5}
    reply = _refused(tools, sent, "unknown_source_family")
    error = reply["error"]
    said = error["message"] + " " + error["fix_hint"]
    assert "[]" not in said and "pass one of" not in said
    assert repr(MAIN[:-1]) in error["message"] and "it declares none" in error["message"]
    assert 'where={"op": "eq", "field": "source"' in error["fix_hint"]
    # A real source with a close name, from the list the read path may state.
    assert MAIN in error["fix_hint"]
    assert "adjusted" not in reply
    if "retry_call" in error:
        # The list could not say whether the name is a source: the
        # corrected call is offered as a guess, and it is served.
        assert error["retry_call"]["mode"] == "guess"
        assert error["retry_call"]["arguments"]["where"]["value"] == MAIN[:-1]
        assert _dispatched(tools, reply, sent)["results"] == []


def test_a_blank_family_name_and_a_family_on_a_lookup_keep_their_refusals(tools):
    _refused(tools, {"query": TERM, "source_family": ""}, "unknown_source_family")
    reply = _reply(tools, {"where": TYPE_DOC, "source_family": MAIN})
    assert reply["status"] == "refused" and "adjusted" not in reply


# ---------- B: a record type sent as profile ----------


@pytest.mark.parametrize("extra", [
    {"query": TERM, "limit": 20},
    {"query": TERM, "method": "grep", "limit": 20},
    {"limit": 20},
])
def test_a_record_type_as_profile_is_served_as_a_where_on_the_type(tools, extra):
    new = _noted(tools, {"profile": "ticket", **extra}, {"where": TYPE_TICKET, **extra},
                 ["profile_as_type"])
    assert _ids(new) == ["ticket:1"]
    assert "! adjusted: profile `ticket` read as where type = `ticket`" in tools.text(
        "search", {"profile": "ticket", **extra})


def test_a_profile_that_is_no_profile_and_no_type_is_refused_with_what_exists(tools):
    reply = _refused(tools, {"query": TERM, "profile": "tiket"}, "unknown_search_profile")
    error = reply["error"]
    assert "'tiket'" in error["message"] and "nor a record type" in error["message"]
    assert "'all'" in error["fix_hint"]
    assert 'where={"op": "eq", "field": "type"' in error["fix_hint"]
    assert "--scope" not in error["fix_hint"] and "adjusted" not in reply
    # `all` still means no profile.
    assert _served(_reply(tools, {"query": TERM, "profile": "all", "limit": 3}))["results"]


# ---------- C: a type alone, and a list of a lookup ----------


def _paged(tools: Tools, tool: str, arguments: dict, rows: str) -> list[str]:
    """Every id of a read, following `next_offset` to the end."""
    out: list[str] = []
    offset = 0
    for _ in range(400):
        reply = _served(_reply(tools, {**arguments, "offset": offset}, tool=tool))
        out += _ids(reply, rows)
        if reply.get("next_offset") is None:
            return out
        assert reply["next_offset"] > offset
        offset = reply["next_offset"]
    raise AssertionError("the read does not end")


def _all_of_type(tools: Tools, subtype: str) -> list[str]:
    """The set `filter` lists for a type, under the caller's access view."""
    ids = _paged(tools, "filter", {
        "input": {"kind": "nodes", "subtype": subtype}, "predicate": {}, "limit": 200,
    }, "items")
    assert len(ids) == len(set(ids))
    return sorted(ids)


@pytest.mark.parametrize("arguments", [
    {"where": TYPE_TICKET, "limit": 3, "select": ["node_id", "attrs", "subtype", "handle"]},
    {"method": "attrs", "where": {"op": "eq", "field": "subtype", "value": "ticket"}},
    {"query": "", "method": "attrs", "where": TYPE_TICKET, "profile": "all"},
])
def test_a_where_on_the_type_alone_lists_that_type(tools, arguments):
    reply = _served(_reply(tools, arguments))
    assert reply["method"] == "attrs" and _ids(reply) == ["ticket:1"]
    assert "adjusted" not in reply


def test_a_type_listing_is_the_set_filter_lists(tools):
    docs = _all_of_type(tools, "doc")
    assert len(docs) > 130
    listed = _paged(tools, "search", {"where": TYPE_DOC, "limit": 200,
                                      "select": ["node_id"]}, "results")
    assert sorted(listed) == docs and len(listed) == len(docs)


@pytest.mark.parametrize("arguments", [
    {"where": TYPE_DOC, "limit": 30, "select": ["node_id", "handle"]},
    {"query": "", "method": "lexical", "where": TYPE_DOC, "limit": 100,
     "select": ["node_id", "subtype", "handle"]},
    # A prefix on an attribute is decided in SQL (issue #3666 M4), so the
    # page holds `limit` matches.
    {"method": "attrs", "limit": 5, "select": ["node_id", "handle"],
     "where": {"op": "and", "clauses": [
         TYPE_DOC, {"op": "prefix", "field": "attrs.path", "value": "docs/01"}]}},
    {"where": TYPE_DOC, "limit": 4},
])
def test_a_list_of_a_where_with_no_query_is_the_lookups_page_one_line_per_record(
        tools, arguments):
    listed, cards = tools.pair(
        "search", {**arguments, "view": "list"},
        {**arguments, "select": arguments.get("select") or ["node_id", "subtype", "handle"]},
    )
    assert _served(listed)["view"] == "list" and listed["method"] == "attrs"
    assert _ids(listed) == _ids(cards) and len(listed["results"]) == arguments["limit"]
    assert listed["next_offset"] == cards["next_offset"]
    for row in listed["results"]:
        assert "evidence_kind_provenance" not in row and "excerpt" not in row
    lines = tools.text("search", {**arguments, "view": "list"}).splitlines()
    assert " · attrs · list · " in lines[0]
    rows = [line for line in lines if line.startswith("[")]
    assert len(rows) == arguments["limit"] and rows[0].startswith("[1] doc:")


def test_a_lookup_list_keeps_the_fields_it_was_asked_for_and_the_list_limit(tools):
    listed = _served(_reply(tools, {"where": TYPE_TICKET, "view": "list",
                                    "select": ["node_id", "attrs"]}))
    assert listed["results"][0]["attrs"]["summary"] == "Ticket one"
    assert listed["limit"] == 100
    reply = _refused(tools, {"where": TYPE_DOC, "view": "list", "limit": 101},
                     "search_view_unsupported")
    assert "limit <= 100" in reply["error"]["fix_hint"]


def test_a_lookup_with_no_condition_is_still_refused(tools):
    reply = _refused(tools, {"method": "attrs"}, "invalid_operator_arguments")
    assert "this call has no condition" in reply["error"]["message"]
    assert "where=" in reply["error"]["fix_hint"]


# ---------- D: an exact listing that takes more than one page ----------


@pytest.mark.parametrize("page", [
    {"limit": 25, "select": ["node_id", "attrs"]},
    # A byte budget cuts each page before its limit does.
    {"limit": 100, "max_bytes": 4096, "select": ["node_id", "attrs", "handle"]},
])
def test_an_exact_listing_pages_to_its_end_with_every_record_once(tools, page):
    """The measured call: a listing by type under completeness="exact" that
    one page cannot hold. Every page is served; none says it is every match
    until the last, and the pages together are the set `filter` lists."""
    docs = _all_of_type(tools, "doc")
    seen: list[str] = []
    offset, pages = 0, 0
    while True:
        reply = _served(_reply(tools, {
            "where": TYPE_DOC, "completeness": "exact", "offset": offset, **page,
        }))
        pages += 1
        assert pages < 200, "the listing does not end"
        seen += _ids(reply)
        assert reply["completeness"] == "exact_over_pages", reply.get("completeness")
        if reply["listing"]["complete"]:
            assert reply["listing"]["reason"] == "last_page"
            assert reply["next_offset"] is None
            break
        assert reply["listing"] == {"complete": False, "reason": "more_matches"}
        assert reply["next_offset"] > offset and reply["results"]
        # Issue #3666 M4: with nothing maskable and every condition in SQL,
        # each page also carries the exact total, counted once in SQL.
        assert reply["total_count"] == len(docs), reply.get("total_count")
        assert reply["total_count_basis"] == "exact"
        offset = reply["next_offset"]
    assert pages > 2 and len(seen) == len(set(seen))
    assert sorted(seen) == docs
    if "max_bytes" not in page:
        # The text form of that last page says the listing is complete.
        last = tools.text("search", {"where": TYPE_DOC, "completeness": "exact",
                                     "offset": offset, **page}).splitlines()[0]
        assert "listing complete (the pages up to this one are every match)" in last
        # D8 (read-passage-rows-3666): the ending is the last fact.
        assert last.endswith(" · complete"), last


def test_the_first_page_of_an_exact_listing_says_what_to_send_next_on_one_line(tools):
    sent = {"where": TYPE_DOC, "limit": 7, "completeness": "exact", "select": ["node_id"]}
    head = tools.text("search", sent).splitlines()[0]
    # Before this step the call was refused (search_completeness_refused).
    # Every condition is decided in SQL, so the first page says the total.
    assert f"1-7 of {len(_all_of_type(tools, 'doc'))} " in head, head
    assert "listing continues (the pages together are every match)" in head
    assert head.split(" · generation ")[0].endswith('next: offset=7 completeness="exact"')
    assert " complete " not in head.replace("listing continues", "")


def test_a_listing_one_page_holds_is_exact_and_counted_as_before(tools):
    whole = _served(_reply(tools, {"where": TYPE_TICKET, "completeness": "exact"}))
    assert whole["completeness"] == "exact" and "listing" not in whole
    assert whole["total_count"] == 1 and whole["total_count_basis"] == "exact"


def test_an_exact_grep_with_a_where_on_type_is_served_and_exact_on_windows_refused(tools):
    page = _served(_reply(tools, {"query": TERM, "method": "grep", "where": TYPE_DOC,
                                  "limit": 3, "completeness": "exact"}))
    assert page["completeness"] in ("exact", "exact_over_pages")
    reply = _refused(tools, {"query": TERM, "method": "grep", "view": "windows",
                             "limit": 2, "completeness": "exact"},
                     "search_completeness_refused")
    assert reply["error"]["data"]["bound"] == "view='windows'"


# ---------- E: the first line of a bounded page ----------


def test_a_bounded_page_says_more_follow_and_the_offset_on_its_first_line(tools):
    head = tools.text("search", {"where": TYPE_DOC, "limit": 5}).splitlines()[0]
    # Before: "... · 1-5 of 5+ · retrieval bounded · ... · next: offset=5".
    assert "1-5, more follow" in head and "5+" not in head, head
    assert head.split(" · generation ")[0].endswith("next: offset=5"), head
    head = tools.text("filter", {"input": {"kind": "nodes", "subtype": "doc"},
                                 "predicate": {}, "limit": 4}).splitlines()[0]
    assert "1-4, more follow" in head and head.split(" · generation ")[0].endswith(
        "next: offset=4"), head


# ---------- F: scope, window arguments and path_prefix ----------


def test_a_scope_ranked_by_passages_is_refused_with_the_call_that_ranks_its_passages(tools):
    sent = {"query": TERM, "scope": {"record": "doc:000"}, "rank_by": "passages",
            "limit": 20, "select": ["node_id", "excerpt", "score"]}
    reply = _refused(tools, sent, "search_scope_unsupported")
    retry = reply["error"]["retry_call"]
    assert retry["mode"] == "guess" and retry["arguments"] == {**sent, "rank_by": "self"}
    served = _dispatched(tools, reply, sent)
    assert served["results"] and all(
        row["node_id"] == "doc:000" or row["node_id"].startswith("doc_chunk:000-")
        for row in served["results"]
    )


@pytest.mark.parametrize("argument", [
    {"scope": {"records": ["doc:000", "doc:001"]}, "limit": 5},
    {"matching": "record", "limit": 3},
    {"context_chars": 300, "limit": 3},
])
def test_a_window_argument_without_the_view_is_served_as_the_windows_read(tools, argument):
    """Issue #3666 M4: only the windows read takes these, so the call can
    mean only that read. Before: refused, with that call offered."""
    sent = {"query": TERM, "method": "grep", **argument}
    new = _noted(tools, sent, {**sent, "view": "windows"}, ["grep_windows_view"])
    assert new["view"] == "windows"
    head = tools.text("search", sent).splitlines()
    assert any(line.startswith("! adjusted: ") and 'view="windows"' in line for line in head)


@pytest.mark.parametrize(("extra", "blocker"), [
    ({"where": TYPE_DOC}, "where"),
    ({"path_prefix": "docs/1"}, "path_prefix"),
])
def test_a_window_argument_beside_one_windows_refuse_gets_words_that_name_it(
        tools, extra, blocker):
    """The old hint said to add view="windows" to a call the windows read
    would refuse too."""
    sent = {"query": TERM, "method": "grep", "limit": 3, "context_chars": 300, **extra}
    reply = _refused(tools, sent, "window_argument_unsupported")
    error = reply["error"]
    assert "retry_call" not in error
    assert error["fix_hint"] == (
        'drop context_chars; only view="windows" takes it, and that view does '
        f"not take this call's {blocker}"
    )
    # Both calls the words leave open are served.
    _served(_reply(tools, {k: v for k, v in sent.items() if k != "context_chars"}))


def test_a_path_prefix_on_ranked_search_is_refused_with_the_grep_call_as_a_guess(tools):
    """Issue #3666 M4: still refused (ranked terms and exact text are two
    readings), now with the grep form offered as a guess and dispatched."""
    sent = {"query": TERM, "path_prefix": "docs/1", "limit": 5}
    reply = _refused(tools, sent, "path_prefix_not_applicable")
    error = reply["error"]
    assert '"docs/1"' in error["message"] and "method='grep' only" in error["message"]
    assert "stored path" in error["message"]
    assert error["fix_hint"].startswith("send the same query and path_prefix with method='grep'")
    assert error["retry_call"] == {"tool": "search", "mode": "guess", "arguments": {
        "query": TERM, "limit": 5, "path_prefix": "docs/1", "method": "grep"}}
    served = _dispatched(tools, reply, sent)
    assert served["results"] and all(
        str(row["node_id"]).startswith("doc:") for row in served["results"]
    )


def test_a_path_prefix_beside_a_where_on_ranked_search_gets_no_call(tools):
    """The grep form would drop the where: words only."""
    sent = {"query": TERM, "path_prefix": "docs/1", "where": TYPE_DOC, "limit": 5}
    reply = _refused(tools, sent, "path_prefix_not_applicable")
    assert "retry_call" not in reply["error"]


# ---------- G: bounded pages, exact counts and the case hint (M4) ----------


#: A condition SQL does not decide on the lookup's page (a string range).
SUMMARY_GT = {"op": "gt", "field": "attrs.summary", "value": "Document 125"}


def test_a_prefix_on_an_attribute_reaches_matches_past_the_first_rows(tools):
    """The measured `0 of 0+ · bound reached` (type and a prefix on a name):
    the prefix was checked only on the first `limit` rows of the type."""
    sent = {"where": {"op": "and", "clauses": [
        TYPE_DOC, {"op": "prefix", "field": "attrs.path", "value": "docs/12"}]}, "limit": 3}
    reply = _served(_reply(tools, sent))
    assert _ids(reply) == ["doc:120", "doc:121", "doc:122"]
    assert "not_a_total" not in reply
    head = tools.text("search", sent).splitlines()[0]
    assert "1-3, more follow" in head and "0 of 0+" not in head, head


def test_a_page_stopped_at_its_limit_says_it_is_not_a_total_and_offers_the_count(tools):
    sent = {"where": {"op": "and", "clauses": [TYPE_DOC, SUMMARY_GT]}, "limit": 3}
    reply = _served(_reply(tools, sent))
    assert reply["results"] == [] and reply["next_offset"] == 3
    block = reply["not_a_total"]
    assert block["records_read"] == 3 and "page limit" in block["reason"], block
    assert reply["total_count"] is None and reply["total_count_basis"] == "unknown"
    assert block["for_a_total"] == {"mode": "corrected", "tool": "filter", "arguments": {
        "input": {"kind": "nodes", "subtype": "doc"}, "predicate": SUMMARY_GT}}
    # The offered call is served as written and counts every match.
    (result,) = tools.run(("filter", block["for_a_total"]["arguments"], "json"))
    counted = json.loads(result.content[0].text)
    assert counted.get("status") != "refused" and "adjusted" not in counted, counted
    assert counted["total_count_basis"] == "exact" and counted["total_count"] > 0
    lines = tools.text("search", sent).splitlines()
    assert "0 on this page of 3 read, not a total" in lines[0], lines[0]
    assert "0 of 0+" not in lines[0] and "next: offset=3" in lines[0]
    assert any(
        line.startswith("! not a total: the read stopped at the page limit (3 stored rows); "
                        'for a total: filter(input={"kind":"nodes","subtype":"doc"}')
        for line in lines
    ), lines[:4]


def test_an_exact_lookup_says_its_total_when_sql_decides_every_condition(tools):
    docs = _all_of_type(tools, "doc")
    reply = _served(_reply(tools, {"where": TYPE_DOC, "limit": 10, "completeness": "exact"}))
    assert reply["total_count"] == len(docs) and reply["total_count_basis"] == "exact"
    assert any(note.get("count") == "exact" for note in reply["notes"]), reply["notes"]
    # A condition SQL does not decide: no total, and the page says so.
    bounded = _served(_reply(tools, {
        "where": {"op": "and", "clauses": [TYPE_DOC, SUMMARY_GT]}, "limit": 10,
        "completeness": "exact",
    }))
    assert bounded["completeness"] == "exact_over_pages"
    assert bounded["total_count_basis"] != "exact" and "not_a_total" in bounded


def test_an_exact_lookup_paging_cannot_finish_is_refused_with_the_filter_call(
        tools, monkeypatch):
    from okg.substrate.mcp import server as srv

    # The lookup's paging ceiling, lowered so this small graph reaches it.
    monkeypatch.setattr(srv, "ATTRS_LOOKUP_MAX_OFFSET", 10)
    under = {"op": "prefix", "field": "attrs.path", "value": "docs/1"}
    sent = {"where": {"op": "and", "clauses": [TYPE_DOC, under]}, "offset": 11,
            "completeness": "exact"}
    reply = _refused(tools, sent, "search_completeness_refused")
    assert reply["error"]["data"]["bound"] == "offset_ceiling"
    retry = reply["error"]["retry_call"]
    assert retry["tool"] == "filter" and retry["arguments"] == {
        "input": {"kind": "nodes", "subtype": "doc"}, "predicate": under}
    assert _dispatched(tools, reply, sent)["total_count_basis"] == "exact"
    # A type alone: the `filter` that lists it does not count, so no call.
    alone = _refused(tools, {"where": TYPE_DOC, "offset": 11, "completeness": "exact"},
                     "search_completeness_refused")
    assert "retry_call" not in alone["error"]


def test_an_equality_that_differs_only_in_case_gets_a_did_you_mean(tools):
    """The measured `attrs.repo = "CMSRucio"` where the stored value is
    `cmsrucio`: no match, and the reply names the stored spelling."""
    sent = {"where": {"op": "and", "clauses": [
        TYPE_DOC, {"op": "eq", "field": "attrs.path", "value": "DOCS/001.MD"}]}}
    reply = _served(_reply(tools, sent))
    assert reply["results"] == []
    assert reply["value_hint"] == {
        "field": "attrs.path", "sent": "DOCS/001.MD", "stored": "docs/001.md"}
    lines = tools.text("search", sent).splitlines()
    assert ('! no record has attrs.path = "DOCS/001.MD"; did you mean "docs/001.md"? '
            "(stored with that case)") in lines, lines[:4]
    # A later page says nothing about the value: no hint there.
    later = _served(_reply(tools, {**sent, "offset": 5}))
    assert later["results"] == [] and "value_hint" not in later
    # No hint when no stored value matches in any case.
    none = _served(_reply(tools, {"where": {"op": "and", "clauses": [
        TYPE_DOC, {"op": "eq", "field": "attrs.path", "value": "NOPE.md"}]}}))
    assert none["results"] == [] and "value_hint" not in none


def test_the_case_hint_stays_inside_the_profile_the_call_names(
        clean_runtime, okg_mcp_can_log_in, tmp_path):
    deployment = _corpus(tmp_path, clean_runtime)
    manifest_path = tmp_path / "analyst-search" / "deployment.yaml"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["search"] = {"profiles": {"tickets": {"subtypes": ["ticket"]}}}
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    scoped = Tools(clean_runtime, deployment)
    try:
        sent = {"where": {"op": "eq", "field": "attrs.summary", "value": "DOCUMENT 001"}}
        # Unscoped, the stored "Document 001" of a doc is named.
        unscoped = _served(_reply(scoped, sent))
        assert unscoped["value_hint"]["stored"] == "Document 001"
        # Inside the tickets profile no ticket holds it: no hint.
        inside = _served(_reply(scoped, {**sent, "profile": "tickets"}))
        assert inside["results"] == [] and "value_hint" not in inside
        found = _served(_reply(scoped, {
            "where": {"op": "eq", "field": "attrs.summary", "value": "TICKET ONE"},
            "profile": "tickets"}))
        assert found["value_hint"]["stored"] == "Ticket one"
    finally:
        scoped.close()


def test_a_bounded_page_with_a_date_condition_offers_no_counting_call(tools):
    """`filter` compares a date-typed value as an instant and the lookup as
    text, so the two could count different sets: no call is offered."""
    sent = {"where": {"op": "and", "clauses": [
        TYPE_DOC, {"op": "eq", "field": "attrs.opened_at", "value": "2026-03-28T09:30:00Z"},
        SUMMARY_GT]}, "limit": 2}
    reply = _served(_reply(tools, sent))
    assert reply["results"] == [] and reply["truncated"] is True, reply
    block = reply["not_a_total"]
    assert "for_a_total" not in block
    lines = tools.text("search", sent).splitlines()
    assert any(line.endswith("; narrow the call so it reads every match")
               and line.startswith("! not a total:") for line in lines), lines[:4]


# ---------- access rules ----------


_HIDDEN_CASES = [
    # A record type sent as profile, with and without a caller's where.
    ("search", {"query": "external", "profile": "doc"},
     {"query": "external", "where": TYPE_DOC}, "results"),
    ("search", {"profile": "doc", "where": {"op": "eq", "field": "attrs.path",
                                            "value": "x/y.md"}},
     {"where": {"op": "and", "clauses": [
         {"op": "eq", "field": "attrs.path", "value": "x/y.md"}, TYPE_DOC]}}, "results"),
]


def test_a_hidden_record_is_not_reached_through_a_profile_read_as_a_type(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """Under access enforcement each rewrite returns exactly what the
    corrected call returns a reader who cannot see one source, and returns
    the record once enforcement is off."""
    deployment = _corpus(tmp_path, clean_runtime)
    enforced, opened = _under_enforcement(
        clean_runtime, monkeypatch, deployment,
        [(tool, sent, today) for tool, sent, today, _ in _HIDDEN_CASES],
    )
    for (_tool, _sent, _today, rows), (_new, old) in zip(_HIDDEN_CASES, enforced):
        assert _served(old)[rows] == []
    assert "doc:ext" not in json.dumps(enforced)
    for (_tool, sent, _today, rows), (new, _old) in zip(_HIDDEN_CASES, opened):
        assert [row["node_id"] for row in _served(new)[rows]] == ["doc:ext"], sent


def _denied_reader(clean_runtime, monkeypatch, deployment) -> Tools:
    _activate(clean_runtime, deployment, _DENY_EXTERNAL)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    return Tools(clean_runtime, deployment)


def test_a_type_listing_and_its_list_hide_what_the_reader_may_not_see(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """A listing by type, its list view and its exact paging return, to a
    reader who cannot see one source, the set `filter` returns that reader;
    with enforcement off they return the hidden record too."""
    listing = {"where": TYPE_DOC, "limit": 60, "select": ["node_id"]}
    deployment = _corpus(tmp_path, clean_runtime)
    server = _denied_reader(clean_runtime, monkeypatch, deployment)
    try:
        allowed = _all_of_type(server, "doc")
        assert "doc:ext" not in allowed and len(allowed) > 130
        for extra in ({}, {"view": "list"}, {"completeness": "exact"}):
            assert sorted(_paged(server, "search", {**listing, **extra}, "results")) == allowed
    finally:
        server.close()
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    monkeypatch.delenv("OKG_NOMOS_SUBJECT", raising=False)
    server = Tools(clean_runtime, deployment)
    try:
        opened = _paged(server, "search", listing, "results")
        assert set(opened) - set(allowed) == {"doc:ext"}
    finally:
        server.close()


def test_a_hidden_source_and_an_absent_one_are_refused_alike(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """Under access rules the sources cannot be listed without confirming a
    hidden one. A name the producer registry does not hold is refused with
    the corrected call as a guess, and nothing in the refusal, or in what
    the guess returns, differs between a source that exists and is hidden
    from this reader and a source that does not exist."""
    absent = "connector:absent_docs"
    assert len(absent) != len(EXTERNAL)
    deployment = _corpus(tmp_path, clean_runtime)
    server = _denied_reader(clean_runtime, monkeypatch, deployment)
    try:
        replies = {}
        for name in (EXTERNAL, absent):
            sent = {"query": "external", "source_family": name, "limit": 5}
            reply = _refused(server, sent, "unknown_source_family")
            retry = reply["error"]["retry_call"]
            assert retry["mode"] == "guess"
            assert retry["arguments"]["where"] == {
                "op": "eq", "field": "source", "value": name}
            guess = _dispatched(server, reply, sent)
            assert guess["results"] == []
            text = server.text("search", sent)
            replies[name] = (
                json.dumps(_same(reply)).replace(name, "<name>"),
                json.dumps(_same(guess)).replace(name, "<name>"),
                text.replace(name, "<name>").split(" · generation ")[0],
            )
        assert replies[EXTERNAL] == replies[absent]
        assert "doc:ext" not in json.dumps(replies)
        # A source the registry holds is served as its corrected call is.
        sent = {"query": TERM, "source_family": MAIN, "limit": 5}
        new, old = server.pair("search", sent, {"query": TERM, "where": SOURCE_MAIN,
                                                "limit": 5})
        assert _served(new)["results"]
        assert _same(new, without=("adjusted",)) == _same(old)
    finally:
        server.close()
