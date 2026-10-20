"""Grep's match lines (#3666 R5): one line per match, `rg -n` shaped.

Pure Python: the line cutting and the field each line is placed in. The
database part (which matches PostgreSQL reads, the spellings a page reports)
is in `test_grep_where.py`. Synthetic text only.
"""
from __future__ import annotations

import json

from okg.substrate.mcp.grep_search import (
    MATCH_LINE_CHARS,
    PROSE_LINE_CUT_CHARS,
    _match_spans,
    _unit_lines,
)
from okg.substrate.mcp.response_bounds import _match_line_at, _shortened_search_page
from okg.substrate.mcp.result_text import render

TICKET = (
    "summary: Placement review\n"
    "description: First line of the body.\n"
    "A second body line names the widget.\n"
    "recent_comments:\n"
    "Alex Doe 2026-04-21T17:54:32.224+0200: The widget was moved.\n"
    "Sam Roe 2026-04-22T09:00:00Z: widget widget again"
)


def _lines(text: str, word: str, **kwargs) -> list[dict]:
    spans, at = [], text.find(word)
    while at >= 0:
        spans.append((at, at + len(word)))
        at = text.find(word, at + 1)
    options = {"file_like": False, "start_line": None, **kwargs}
    return _unit_lines(text, spans, **options)


def test_a_body_line_takes_the_label_above_it_and_a_comment_its_author():
    got = _lines(TICKET, "widget")
    assert [(e["field"], e["text"], e["author"], e["date"]) for e in got] == [
        # A description paragraph shows whole, from its label's line.
        ("description", "First line of the body.\nA second body line names the widget.",
         None, None),
        ("comment", "The widget was moved.", "Alex Doe", "2026-04-21"),
        # Two matches on one line make one line.
        ("comment", "widget widget again", "Sam Roe", "2026-04-22"),
    ]
    assert all(e["line"] is None for e in got)


def test_a_label_line_drops_its_label_and_a_match_in_a_head_keeps_the_line():
    got = _lines(TICKET, "First")
    assert [(e["field"], e["text"]) for e in got] == [
        ("description", "First line of the body.\nA second body line names the widget."),
    ]
    # Failure case: the match is the author's name, inside the cut head.
    named = _lines(TICKET, "Alex")
    assert named[0]["field"] == "comment"
    assert named[0]["text"].startswith("Alex Doe 2026-04-21T17:54:32.224+0200:")


def test_a_line_with_no_label_or_comment_head_is_plain_text():
    got = _lines("no labels here\nbut a widget below", "widget")
    assert [(e["field"], e["text"], e["author"]) for e in got] == [
        ("text", "but a widget below", None),
    ]
    # Failure case: a capitalised `Note:` is prose, not a field label.
    assert _lines("Note: widget", "widget")[0]["field"] == "text"


def test_a_files_lines_are_numbered_and_never_labelled():
    text = "key: one\nother: widget here\nlast"
    assert [(e["field"], e["text"], e["line"]) for e in _lines(text, "widget", file_like=True)] == [
        ("text", "other: widget here", 2),
    ]
    numbered = _lines(text, "widget", file_like=True, start_line=40)
    assert numbered[0]["line"] == 41
    # Failure case: a record's composed text gets no line number.
    assert _lines(text, "widget")[0]["line"] is None


def test_a_long_line_is_cut_around_its_match():
    text = "x" * 1000 + " needle " + "y" * 1000
    (entry,) = _lines(text, "needle")
    assert "needle" in entry["text"]
    assert len(entry["text"]) <= MATCH_LINE_CHARS + 6
    assert entry["text"].startswith("...") and entry["text"].endswith("...")


def test_matched_texts_are_placed_in_order_from_the_end_of_the_last():
    text = "Keep-Value then keep_value then Keep-Value"
    spans = _match_spans(text, [(0, "Keep-Value"), (0, "keep_value"), (0, "Keep-Value")])
    assert spans == [(0, 10), (16, 26), (32, 42)]
    # Failure case: a text the read did not hold places nothing.
    assert _match_spans(text, [(0, "absent")]) == []


# ── a matched comment or prose paragraph keeps its words (#3666, lane RD) ──
#
# Cut to `MATCH_LINE_CHARS`, a comment's sentence just past the match was
# lost and the line never said so. A comment or paragraph now shows whole up
# to `PROSE_LINE_WHOLE_CHARS`; a longer one is cut around its match, says
# how much it left out, and its row names the call that reads it whole.


def _sentences(count: int, start: int = 0) -> str:
    return " ".join(
        f"Sentence {i} says the panel keeps setting {i} after restart." for i in
        range(start, start + count)
    )


def _thread(comment: str) -> str:
    return (
        "summary: Panel review\n"
        "status: Done\n"
        "comments:\n"
        f"Alex Doe 2026-04-21T17:54:32.224+0200: {comment}\n"
        "Sam Roe 2026-04-22T09:00:00Z: Short reply."
    )


def test_a_700_character_comment_matched_near_its_start_is_served_whole():
    """Must-fail on a58e7786ff: the line was cut to 200 characters around
    the match, and the sentences after the cut were never shown."""
    comment = "The widget was moved. " + _sentences(12)
    assert 650 <= len(comment) <= 760
    (entry,) = _lines(_thread(comment), "widget")
    assert (entry["field"], entry["author"], entry["date"]) == (
        "comment", "Alex Doe", "2026-04-21")
    assert entry["text"] == comment
    assert entry["chars_left"] == 0
    assert "setting 11 after restart." in entry["text"]


def test_a_3000_character_comment_is_cut_around_its_match_and_says_what_is_left():
    comment = _sentences(25) + " The widget was moved here. " + _sentences(25, 25)
    assert len(comment) > 2_900
    (entry,) = _lines(_thread(comment), "widget")
    assert entry["field"] == "comment"
    shown = entry["text"]
    assert "The widget was moved here." in shown
    assert shown.startswith("...") and shown.endswith("...")
    body = shown[3:-3]
    assert len(body) <= PROSE_LINE_CUT_CHARS
    # Cut at word boundaries: the shown text is a run of whole words.
    assert f" {body} " in f" {comment} "
    assert entry["chars_left"] == len(comment) - len(body)
    # Failure case: a caller's `max_chars` sizes the cut instead (D2).
    (sized,) = _lines(_thread(comment), "widget", prose_chars=1_000)
    assert len(sized["text"]) <= 1_006
    assert sized["chars_left"] == len(comment) - len(sized["text"][3:-3])
    # And a `max_chars` larger than the comment shows it whole.
    (whole,) = _lines(_thread(comment), "widget", prose_chars=4_000)
    assert whole["text"] == comment and whole["chars_left"] == 0


def test_two_matches_in_one_comment_make_one_line_and_its_continuation_keeps_the_author():
    text = _thread("The widget was moved.\nA later line of the same widget comment.")
    got = _lines(text, "widget")
    assert [(e["field"], e["author"], e["text"]) for e in got] == [(
        "comment", "Alex Doe",
        "The widget was moved.\nA later line of the same widget comment.",
    )]


def test_a_description_paragraph_ends_at_a_blank_or_label_line():
    text = (
        "summary: Panel review\n"
        "description: First body line.\r\n"
        "A second body line names the widget.\r\n"
        "\r\n"
        "A later paragraph.\n"
        "status: Done"
    )
    (entry,) = _lines(text, "widget")
    assert entry["field"] == "description"
    assert " ".join(entry["text"].split()) == (
        "First body line. A second body line names the widget.")
    assert entry["chars_left"] == 0
    # Failure case: a field that is not prose keeps today's line, uncounted.
    (status,) = _lines(text, "Done")
    assert (status["field"], status["text"]) == ("status", "Done")
    assert "chars_left" not in status


def test_a_files_long_line_keeps_the_200_character_window():
    text = "first\n" + "x" * 600 + " widget " + "y" * 600 + "\nlast"
    for file_like in (True, False):
        (entry,) = _lines(text, "widget", file_like=file_like)
        assert len(entry["text"]) <= MATCH_LINE_CHARS + 6
        assert "chars_left" not in entry
    # A file's text never shows a comment whole, even one shaped like it.
    shaped = "Alex Doe 2026-04-21T17:54:32Z: " + "z " * 400 + "widget " + "z " * 400
    (entry,) = _lines(shaped, "widget", file_like=True)
    assert len(entry["text"]) <= MATCH_LINE_CHARS + 6 and entry["line"] == 1


def _grep_row(key: str, lines: list[dict]) -> dict:
    node_id = f"ticket:{key}"
    return {
        "node_id": node_id, "subtype": "issue",
        "provenance": {"source": "tickets", "trust": "trusted"},
        "head": {"kind": "ticket", "id": node_id, "fields": [["status", "Done"]],
                 "title": f"Title of {key}", "citation_url": None},
        "read_more": {"tool": "inspect",
                      "arguments": {"target": {"kind": "node", "node_id": node_id}}},
        "match_lines": lines,
    }


def _grep_page(*rows: dict) -> dict:
    return {
        "query": '"widget"', "method": "grep", "results": list(rows),
        "total_count": len(rows), "offset": 0,
        "coverage": {"retrieval": {"status": "exhaustive"}, "delivery": {"complete": True}},
    }


def _comment_line(text: str, left: int) -> dict:
    return {"text": text, "field": "comment", "line": None, "author": "Alex Doe",
            "date": "2026-04-21", "chars_left": left}


def _json_size(page: dict) -> int:
    return len(json.dumps(page, separators=(",", ":"), ensure_ascii=False))


def test_a_cut_comment_line_says_what_is_left_and_its_row_names_the_whole_read():
    """Must-fail on a58e7786ff: a cut line printed as if it were whole, and
    the row named no call that reads the rest."""
    page = _grep_page(
        _grep_row("T-1", [_comment_line("...the widget was moved here...", 2_412)]),
        _grep_row("T-2", [_comment_line("The widget is whole.", 0)]),
    )
    lines = render(page, tool="search").splitlines()
    assert (
        "    comment, Alex Doe 2026-04-21: ...the widget was moved here... "
        "[cut, 2,412 chars left]"
    ) in lines
    assert '    read whole: inspect(target={"kind":"node","node_id":"ticket:T-1"})' in lines
    # Failure case: a whole line says nothing more, and its row names no call.
    assert "    comment, Alex Doe 2026-04-21: The widget is whole." in lines
    assert sum(line.startswith("    read whole:") for line in lines) == 1


def test_a_fitted_page_shrinks_long_lines_before_any_row_and_counts_what_it_cut():
    whole = "The widget was moved. " + _sentences(20)
    rows = [_grep_row(f"T-{i}", [_comment_line(whole, 0), _comment_line(whole, 0)])
            for i in range(20)]
    page = _grep_page(*rows)
    budget = 20_000
    assert _json_size(page) > budget
    fitted = _shortened_search_page(page, {}, budget=budget, measure=_json_size)
    assert fitted is not None and _json_size(fitted) <= budget
    # Every row is kept; the lines were shortened instead.
    assert [row["node_id"] for row in fitted["results"]] == [row["node_id"] for row in rows]
    keep = fitted["passages_shortened_to"]
    assert 0 < keep < len(whole)
    for row in fitted["results"]:
        for line in row["match_lines"]:
            assert len(line["text"]) <= keep
            assert line["chars_left"] == len(whole) - len(line["text"])
    text = render(fitted, tool="search")
    assert text.count("read whole: inspect(") == 20
    assert f"[cut, {len(whole) - keep:,} chars left]" in text
    # Failure case: a file's line carries no count, so a cut adds none.
    file_line = {"text": "x" * 300, "field": "text", "line": 4, "author": None, "date": None}
    cut = _match_line_at(file_line, 100)
    assert len(cut["text"]) == 100 and "chars_left" not in cut
    # A line already cut keeps its own count and adds what the fit cut,
    # its closing marker not counted as text.
    already = _comment_line("..." + "y" * 300 + "...", 50)
    assert _match_line_at(already, 100)["chars_left"] == 50 + 303 - 100
