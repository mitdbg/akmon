"""Search-row reading keys without a database (PACT read-passage-rows-3666).

A row's passage is the text around its match, never the record's start when
the match is elsewhere; a match inside a dated comment line names the comment's
author and date; code is shown by lines; a short unit is whole; the caller's
`max_chars` replaces the per-type sizes. The head carries per-type fields from
generic attribute names. `select` names `attrs.<key>` as `where` does. The
PostgreSQL tests are in test_lexical_exact_identifier_hits.py.
"""
from __future__ import annotations

import json

from okg.substrate.mcp.excerpt_window import (
    best_window_start,
    centred_cut,
    fallback_matcher,
)
from okg.substrate.mcp.record_text import (
    PASSAGE_SHORT_CHARS,
    _Segment,
    chain_segments,
    fit_record_text,
    text_passage,
    thread_parts,
    thread_record_text,
)
from okg.substrate.mcp.response_bounds import _search_page_at, _shortened_search_page
from okg.substrate.mcp.result_text import render
from okg.substrate.mcp.search import (
    row_head,
    row_read_more,
    split_attr_select,
)

FILLER = "routine maintenance log entry without findings. " * 40
THREAD = (
    "summary: Replica placement\n\n"
    f"description: Please review the disk replicas. {FILLER}\n\n"
    "status: Open\n\n"
    "recent_comments: Ana Diaz 2026-04-17T09:00:00.000+0200: Could someone check the list?\n"
    "Bo Chen 2026-04-21T17:54:32.224+0200: Done: vanadium transfer finished.\n"
    "It needed a second try."
)


def test_a_match_in_a_comment_names_its_author_and_date():
    passage = text_passage(THREAD, fallback_matcher("vanadium"), budget=PASSAGE_SHORT_CHARS,
                           basis="record_text")
    assert passage["where"] == "comment"
    assert (passage["author"], passage["date"]) == ("Bo Chen", "2026-04-21")
    assert passage["text"] == "Done: vanadium transfer finished.\nIt needed a second try."
    assert passage["whole"] is True


def test_the_comment_holding_most_query_words_wins_over_the_window_start():
    """Must-fail on f9634f77389: a lone query word (`T7`) at the end of an
    earlier comment started the best window, whose other words were in the
    next comment, and the row named the earlier comment's author and showed
    its text, which holds almost none of the query."""
    thread = (
        "summary: Site list\n\n"
        "recent_comments: Ana Diaz 2026-04-17T12:12:00.000+0200: "
        "The routing table already sends those jobs to the same country T7. "
        "It works well.\n"
        "Ana Diaz 2026-04-17T16:40:00.000+0200: Can you remove T7_XX_Siteone "
        "from the keep_widget=True list? The site is full.\n"
        "Bo Chen 2026-04-21T09:00:00.000+0200: Done."
    )
    passage = text_passage(thread, fallback_matcher("keep_widget T7_XX_Siteone"),
                           budget=PASSAGE_SHORT_CHARS, basis="record_text")
    assert passage["where"] == "comment"
    assert (passage["author"], passage["date"]) == ("Ana Diaz", "2026-04-17")
    assert "keep_widget=True" in passage["text"] and "Siteone" in passage["text"]
    assert "routing table" not in passage["text"]
    # Failure case: a query only the earlier comment holds still names it.
    early = text_passage(thread, fallback_matcher("routing"), budget=PASSAGE_SHORT_CHARS,
                         basis="record_text")
    assert early["text"].startswith("The routing table")


def test_a_window_starting_in_plain_text_moves_into_the_comment_with_the_words():
    thread = (
        "summary: osmium\n\n"
        "recent_comments: Ana Diaz 2026-04-17T12:12:00.000+0200: "
        "osmium hafnium tantalum all three here."
    )
    passage = text_passage(thread, fallback_matcher("osmium hafnium tantalum"),
                           budget=PASSAGE_SHORT_CHARS, basis="record_text")
    assert (passage["where"], passage["author"]) == ("comment", "Ana Diaz")
    assert passage["text"] == "osmium hafnium tantalum all three here."
    # Failure case: when the text before the comments holds as many words,
    # the earliest place still wins and no comment is named.
    tie = text_passage(thread.replace("summary: osmium", "summary: osmium hafnium tantalum"),
                       fallback_matcher("osmium hafnium tantalum"),
                       budget=PASSAGE_SHORT_CHARS, basis="record_text")
    assert tie["where"] == "record" and tie["author"] is None


def test_chunks_whose_spans_run_on_are_joined_without_a_line_break():
    """Must-fail on f9634f77389: a ticket's window chunks have exact spans
    (the overlap trimmed) but no document length, so the chained text put a
    line feed between them and split a word (`keep_wid` / `get`)."""
    first = _Segment("c0", "the keep_wid", None, None, 0, None, 0, 12, None, False)
    second = _Segment("c1", "get=True list", None, None, 1, None, 12, 25, None, False)
    for segment in (first, second):
        segment.line_break_before = segment is second
    assert chain_segments([first, second]).text == "the keep_widget=True list"
    # Failure case: spans that do not meet keep the line break.
    apart = _Segment("c1", "get=True list", None, None, 1, None, 14, 27, None, False)
    apart.line_break_before = True
    assert chain_segments([first, apart]).text == "the keep_wid\nget=True list"


def test_the_first_comment_after_a_field_label_is_a_comment():
    passage = text_passage(THREAD, fallback_matcher("someone"), budget=PASSAGE_SHORT_CHARS,
                           basis="record_text")
    assert (passage["where"], passage["author"]) == ("comment", "Ana Diaz")
    assert passage["text"] == "Could someone check the list?"


def test_a_match_in_the_description_is_cut_from_the_description():
    passage = text_passage(THREAD, fallback_matcher("disk replicas"), budget=PASSAGE_SHORT_CHARS,
                           basis="record_text")
    assert passage["where"] == "description"
    assert passage["text"].startswith("Please review the disk replicas.")
    assert passage["text"].endswith("...")
    assert len(passage["text"]) <= PASSAGE_SHORT_CHARS + 6


def test_a_match_deep_in_a_record_never_shows_its_start():
    text = FILLER + " the osmium sample failed. " + FILLER
    passage = text_passage(text, fallback_matcher("osmium"), budget=500, basis="own_text")
    assert "osmium" in passage["text"]
    assert passage["text"].startswith("...") and passage["text"].endswith("...")
    assert passage["where"] == "record" and passage["whole"] is False


def test_no_match_is_no_passage_and_a_short_unit_is_whole():
    assert text_passage("nothing here", fallback_matcher("osmium"), budget=500,
                        basis="own_text") is None
    whole = text_passage("A short osmium note.", fallback_matcher("osmium"), budget=500,
                         basis="own_text")
    assert whole["text"] == "A short osmium note." and whole["whole"] is True
    # An unanchored passage (a semantic hit) is the text's start, said whole or not.
    start = text_passage(FILLER, fallback_matcher("osmium"), budget=100, basis="own_text",
                         anchor_required=False)
    assert start["text"].endswith("...") and not start["text"].startswith("...")


def test_max_chars_replaces_the_per_type_size():
    text = FILLER + " the osmium sample failed. " + FILLER
    passage = text_passage(text, fallback_matcher("osmium"), budget=1000, basis="own_text",
                           chars=120)
    assert "osmium" in passage["text"]
    assert len(passage["text"]) <= 126


def test_long_code_is_shown_by_lines_with_its_signature():
    code = "\n".join(f"    value_{i} = compute({i})  # step {i}" for i in range(1, 80))
    code = code.replace("value_50 =", "tantalum_total =")
    passage = text_passage(code, fallback_matcher("tantalum"), budget=1000, basis="own_text",
                           code=True, first_line=10, last_line=88,
                           signature="def f(x):", docstring="Compute f.\nMore.")
    assert passage["where"] == "lines"
    lines = passage["text"].split("\n")
    assert lines[:3] == ["def f(x):", "Compute f.", "..."]
    assert any("tantalum_total" in line for line in lines)
    # Line 50 of the text is source line 59; 15 lines either side.
    assert (passage["start_line"], passage["end_line"]) == (44, 74)
    short = text_passage("def g():\n    return tantalum", fallback_matcher("tantalum"),
                         budget=1000, basis="own_text", code=True, first_line=3, last_line=4)
    assert short["whole"] is True and (short["start_line"], short["end_line"]) == (3, 4)


def test_a_commit_body_shows_its_first_lines_when_the_match_is_in_the_subject():
    passage = text_passage("Body line one.\nBody line two.", fallback_matcher("hafnium"),
                           budget=500, basis="own_text", body=True, anchor_required=False)
    assert passage["where"] == "body"
    assert passage["text"] == "Body line one.\nBody line two."


def test_thread_parts_and_the_record_text_block():
    parts = thread_parts(THREAD)
    assert [c.author for c in parts.comments] == ["Ana Diaz", "Bo Chen"]
    block = thread_record_text(THREAD)
    assert block["fields"] == {"summary": "Replica placement", "status": "Open"}
    assert block["description"].startswith("Please review the disk replicas.")
    assert block["comments"] == [
        {"author": "Ana Diaz", "date": "2026-04-17", "text": "Could someone check the list?"},
        {"author": "Bo Chen", "date": "2026-04-21",
         "text": "Done: vanadium transfer finished.\nIt needed a second try."},
    ]
    assert (block["comments_total"], block["comments_shown"], block["comments_left"]) == (2, 2, 0)
    # Not fielded: no block.
    assert thread_record_text("Plain prose. Bo Chen 2026-04-21T17:54:32Z: hi") is None


def test_a_cut_thread_keeps_the_newest_comments_and_counts_the_rest():
    block = thread_record_text(THREAD)
    cut = fit_record_text(block, lambda candidate: len(candidate["comments"]) <= 1)
    assert [c["author"] for c in cut["comments"]] == ["Bo Chen"]
    assert (cut["comments_shown"], cut["comments_left"]) == (1, 1)
    assert fit_record_text(block, lambda candidate: False) is None


def test_centred_cut_keeps_the_span_and_its_bounds():
    text = "x" * 100 + " target " + "y" * 100
    start, end = centred_cut(text, (101, 107), lo=0, hi=len(text), budget=50)
    assert start <= 101 and end >= 107 and end - start <= 50
    assert centred_cut(text, (101, 107), lo=90, hi=120, budget=50) == (90, 120)


def test_best_window_start_is_unchanged_by_the_span_helper():
    text = "a " * 200 + "osmium " + "b " * 200
    start = best_window_start(text, fallback_matcher("osmium"), 100)
    assert start <= 400 <= start + 100


def test_select_splits_attribute_names_from_row_fields():
    assert split_attr_select(None) == ([], None)
    assert split_attr_select(["node_id", "excerpt"]) == ([], ["node_id", "excerpt"])
    assert split_attr_select(["attrs.status", "attrs.summary"]) == (["status", "summary"], None)
    assert split_attr_select(["node_id", "attrs.status"]) == (["status"], ["node_id"])
    # A bare `attrs.` names no attribute: it stays a row field and is refused there.
    assert split_attr_select(["attrs."]) == ([], ["attrs."])


def test_the_head_carries_generic_per_type_fields():
    ticket = row_head({"node_id": "t:1", "subtype": "issue", "attrs": {
        "status": "Open", "created": "2026-03-20T10:00:00.000+0100",
        "updated": "2026-05-07T08:00:00Z", "summary": "Replica placement",
        "url": "https://tracker.example.org/browse/T-1"}}, {"title_from": "summary"})
    assert ticket["kind"] == "ticket"
    assert ticket["fields"] == [["status", "Open"], ["created", "2026-03-20"],
                                ["updated", "2026-05-07"]]
    assert ticket["title"] == "Replica placement"
    assert ticket["citation_url"] == "https://tracker.example.org/browse/T-1"
    commit = row_head({"node_id": "c:1", "subtype": "git_commit", "attrs": {
        "commit_sha": "0123456789abcdef0123", "repo": "r", "author_name": "Ana Diaz",
        "authored_at": "2026-01-02T03:04:05Z", "message_subject": "Fix loader"}}, {})
    assert commit["kind"] == "commit"
    assert commit["fields"] == [["sha", "0123456"], ["repo", "r"], ["date", "2026-01-02"],
                                ["author", "Ana Diaz"]]
    assert commit["title"] == "Fix loader"
    code = row_head({"node_id": "s:1", "subtype": "code_symbol", "attrs": {
        "qualified_name": "mod.f", "path": "mod.py", "repo": "r", "start_line": 10,
        "end_line": 88}}, {})
    assert (code["kind"], code["fields"], code["title"]) == (
        "code", [["path", "r:mod.py:10-88"]], "mod.f")
    chunk = row_head({"node_id": "chunk:1", "subtype": "document_chunk", "attrs": {
        "doc_path": "guide.md", "heading_path": ["Guide", "Setup"],
        "citation_url": "https://docs.example.org/guide"}}, {})
    assert chunk["fields"] == [["document", "guide.md"], ["section", "§ Guide > Setup"]]
    assert chunk["citation_url"] == "https://docs.example.org/guide"


def test_read_more_names_the_text_read_for_records_with_chunks():
    meta = {"passage_rollup": {"edge_type": "contains", "child_subtype": "document_chunk"}}
    assert row_read_more({"node_id": "t:1", "subtype": "issue"}, meta) == {
        "tool": "inspect", "arguments": {"target": {"kind": "text", "node_id": "t:1"}}}
    assert row_read_more({"node_id": "s:1", "subtype": "code_symbol"}, {}) == {
        "tool": "inspect", "arguments": {"target": {"kind": "node", "node_id": "s:1"}}}
    chunk = {"node_id": "chunk:1", "subtype": "document_chunk",
             "parent": {"node_id": "doc:1"}}
    assert row_read_more(chunk, {})["arguments"]["target"] == {
        "kind": "text", "node_id": "doc:1"}


# A matched comment that is one list: 29 site-like names, about 1,200 chars.
SITE_NAMES = [f"SITE_{chr(ord('A') + i % 5)}_{i + 1}_storage_disk" for i in range(29)]
LIST_COMMENT = (
    "The agreed list is below; no objection was raised.\n"
    + "\n".join(SITE_NAMES)
)


def _list_thread(comment: str) -> str:
    return (
        "summary: Site list\n\n"
        f"description: Choose the sites. {FILLER}\n\n"
        "recent_comments: Ana Diaz 2026-04-17T09:00:00.000+0200: Earlier note.\n"
        f"Bo Chen 2026-08-04T10:00:00.000+0200: {comment}\n"
        "Ana Diaz 2026-09-05T10:00:00.000+0200: Later note."
    )


def test_a_matched_comment_under_the_whole_limit_is_shown_whole():
    """Must-fail on 1ae877ec40e: a matched comment was cut to the
    500-character ticket window, so a 29-name list in one comment showed
    about 20 names and could not be counted. A comment up to 1,500
    characters is now whole, like a short code chunk."""
    assert 600 < len(LIST_COMMENT) < 1_500
    passage = text_passage(_list_thread(LIST_COMMENT), fallback_matcher("agreed objection"),
                           budget=PASSAGE_SHORT_CHARS, basis="record_text")
    assert (passage["where"], passage["author"], passage["date"]) == (
        "comment", "Bo Chen", "2026-08-04")
    assert passage["whole"] is True
    assert passage["text"] == LIST_COMMENT
    assert all(name in passage["text"] for name in SITE_NAMES)
    # Failure case: a longer comment keeps the centred ticket window.
    long_comment = LIST_COMMENT + "\n" + "\n".join(SITE_NAMES * 2)
    assert len(long_comment) > 1_500
    cut = text_passage(_list_thread(long_comment), fallback_matcher("agreed objection"),
                       budget=PASSAGE_SHORT_CHARS, basis="record_text")
    assert cut["where"] == "comment" and cut["whole"] is False
    assert len(cut["text"]) <= PASSAGE_SHORT_CHARS + 6
    # Failure case: the caller's max_chars (D2) still sizes the passage.
    sized = text_passage(_list_thread(LIST_COMMENT), fallback_matcher("agreed objection"),
                         budget=PASSAGE_SHORT_CHARS, basis="record_text", chars=200)
    assert sized["whole"] is False and len(sized["text"]) <= 206


def _fit_row(i: int, passage: dict) -> dict:
    node_id = f"ticket:T-{i}"
    return {
        "node_id": node_id, "subtype": "issue",
        "provenance": {"source": "tickets", "trust": "trusted"},
        "head": {"kind": "ticket", "id": node_id, "fields": [["status", "Open"]],
                 "title": f"Ticket {i}", "citation_url": None},
        "passage": passage,
        "read_more": {"tool": "inspect",
                      "arguments": {"target": {"kind": "text", "node_id": node_id}}},
    }


def _fit_page() -> dict:
    comment = {"text": LIST_COMMENT, "whole": True, "where": "comment",
               "author": "Bo Chen", "date": "2026-08-04", "start_line": None,
               "end_line": None, "chunk_id": "chunk:1"}
    others = [
        _fit_row(i, {"text": "routine maintenance words " * 30, "whole": False,
                     "where": "record", "author": None, "date": None,
                     "start_line": None, "end_line": None, "chunk_id": None})
        for i in range(1, 31)
    ]
    return {"query": "agreed objection", "method": "lexical", "offset": 0,
            "total_count": 31, "results": [*others[:10], _fit_row(0, comment), *others[10:]]}


def _json_size(page: dict) -> int:
    return len(json.dumps(page, separators=(",", ":")))


def test_a_fitted_page_keeps_a_matched_short_comment_whole_while_other_rows_shrink():
    """Must-fail on 1ae877ec40e: D4 cut every passage to one even cap, so
    the matched 29-name comment was cut with 30 other rows' passages
    (in the replayed reply, 17 of 29 names kept) although those passages
    had room to give."""
    page = _fit_page()
    heads = _json_size(_search_page_at(page, 0, False))
    # Room for the comment plus about 150 characters of each other passage.
    budget = heads + len(LIST_COMMENT) + 30 * 170
    assert _json_size(page) > budget
    fitted = _shortened_search_page(page, {}, budget=budget, measure=_json_size)
    assert fitted is not None and _json_size(fitted) <= budget
    assert len(fitted["results"]) == 31
    kept = fitted["results"][10]["passage"]
    assert kept["text"] == LIST_COMMENT and kept["whole"] is True
    others = [row["passage"] for i, row in enumerate(fitted["results"]) if i != 10]
    assert all(p is not None and 0 < len(p["text"]) < 750 for p in others)
    assert fitted["passages_shortened_to"] < len(LIST_COMMENT)
    assert fitted["passages_kept_whole"] == 1
    # As a client reads it: every name of the list is on the page.
    text = render(fitted, tool="search")
    assert all(name in text for name in SITE_NAMES)
    # Failure case: when the comment alone does not fit beside the head
    # lines, every passage is cut evenly as before; every row is kept.
    tight = _shortened_search_page(page, {}, budget=heads + 600, measure=_json_size)
    assert tight is not None and len(tight["results"]) == 31
    assert tight["results"][10]["passage"] is None or (
        tight["results"][10]["passage"]["whole"] is False)
    assert "passages_kept_whole" not in tight
