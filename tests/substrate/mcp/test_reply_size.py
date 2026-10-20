"""A reply says its size and the knobs that change it (#3666).

PACT read-reply-size-3666. Lane X2 measured the Sol graph arm on ERAG-500:
tool replies were 90% of its prompt tokens, a grep with `limit` 100 filled
the whole 64 KB budget, `select` did not narrow a grep, and 23.5% of reply
tokens were rows of records the same try had already shown. These tests pin
the five changes: the size facts on the status line (and in JSON), the
modest defaults, `select` narrowing search and grep to head lines, the
one tool-description line, and the session memory of what was shown.

Hermetic: payloads follow the server's payload contract and are rendered
directly; no database, no server.
"""
from __future__ import annotations

import importlib
import json
import re
from typing import Any

import pytest

from okg.substrate.mcp import reply_size, search_views
from okg.substrate.mcp.reply_size import (
    HEAD_ONLY_SELECT,
    REPLY_SIZE_KEY,
    narrow_row,
    select_is_head_only,
    size_text,
)
from okg.substrate.mcp.result_text import (
    ResultContext,
    render,
    result_context,
    status_facts,
)

TRUSTED = {"source": "mail", "trust": "trusted"}


def _grep_row(n: int, lines: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    node_id = f"mail:thread-{n:03d}"
    return {
        "node_id": node_id, "subtype": "email_thread", "provenance": TRUSTED,
        "head": {"kind": "doc", "id": node_id, "fields": [["date", "2026-04-02"]],
                 "title": f"Thread {n} about the rollout plan", "citation_url": None},
        "match_lines": lines if lines is not None else [
            {"text": f"line {n}: we moved the rollout window to the second week, "
                     "after the capacity review signed off on the plan and the "
                     "on-call rota for the first two days was agreed.",
             "field": None, "line": 10 + n, "author": None, "date": None},
            {"text": f"line {n}b: the fallback stays on the old cluster until "
                     "the error rate holds under the agreed threshold.",
             "field": None, "line": 40 + n, "author": None, "date": None},
        ],
        "read_more": {"tool": "inspect",
                      "arguments": {"target": {"kind": "text", "node_id": node_id}}},
    }


def _lex_row(node_id: str, text: str, *, whole: bool = True,
             read_kind: str = "text") -> dict[str, Any]:
    return {
        "node_id": node_id, "subtype": "document", "provenance": TRUSTED,
        "head": {"kind": "doc", "id": node_id, "fields": [], "title": f"Doc {node_id}",
                 "citation_url": None},
        "passage": {"text": text, "whole": whole, "where": "record", "author": None,
                    "date": None, "start_line": None, "end_line": None, "chunk_id": None},
        "read_more": {"tool": "inspect",
                      "arguments": {"target": {"kind": read_kind, "node_id": node_id}}},
    }


def _page(rows: list[dict[str, Any]], *, method: str = "grep", total: int | None = None,
          **extra: Any) -> dict[str, Any]:
    total = len(rows) if total is None else total
    cut = total > len(rows)
    return {
        "query": "rollout", "method": method, "results": rows, "total_count": total,
        "offset": 0, "limit": len(rows),
        "next_offset": len(rows) if cut else None,
        "coverage": {"retrieval": {"status": "exhaustive"}, "delivery": {"complete": not cut}},
        **extra,
    }


def _size_of(text: str) -> int:
    return len(text.encode("utf-8"))


# ── 1. the size line ─────────────────────────────────────────────────────


def _comment_line(n: int, chars: int) -> dict[str, Any]:
    """A grep comment line: prose, sized by `max_chars` (it carries
    `chars_left`, 0 when whole)."""
    return {"text": f"comment {n}: " + "w" * chars, "field": "comment", "line": None,
            "author": "A Person", "date": "2026-04-02", "chars_left": 0}


def test_the_status_line_says_the_served_size_and_the_knobs_for_less() -> None:
    rows = [_grep_row(n, lines=[_comment_line(n, 700)]) for n in range(12)]
    text = render(_page(rows, total=40), tool="search")
    first = text.splitlines()[0]
    facts = status_facts(text)
    # The size is the served text's own, rounded as printed.
    assert size_text(_size_of(text)) in facts, first
    # Tidy values: 12 rows -> limit 5; a ~712-character comment -> 300.
    assert 'fewer: limit=5, max_chars=300, select=["node_id","handle"]' in facts, first
    # Rows past the page are named once, by `next:`; no second offset knob.
    assert "next: offset=12" in facts and not any(f.startswith("more") for f in facts)


def test_grep_code_lines_get_no_max_chars_knob() -> None:
    """Review M1: a grep code or file line is cut to 200 characters whatever
    `max_chars` says, so `max_chars` would not shrink the page: not named."""
    rows = [_grep_row(n) for n in range(12)]
    facts = status_facts(render(_page(rows, total=40), tool="search"))
    fewer = next(f for f in facts if f.startswith("fewer: "))
    assert fewer == 'fewer: limit=5, select=["node_id","handle"]', facts


@pytest.mark.parametrize(("rows", "longest", "limit", "chars"), [
    (20, 206, 10, None),     # X2 arm C: "limit=10, max_chars=103" -> 300 is not below 206
    (100, 3000, 50, 1200),
    (8, 900, 5, 400),
    (4, 2000, None, 800),    # 4 rows: limit 5 would not shrink the page
    (5, 650, None, 300),     # 5 rows: 2.5 rounds to 5, not below 5
])
def test_knobs_are_tidy_values_that_shrink_the_page(
    rows: int, longest: int, limit: int | None, chars: int | None,
) -> None:
    """X2 arm C transcripts: `max_chars=103` and `301` read as noise. A knob
    is a tidy value (limits 5-100, characters 300-4000), and only named
    when it is below what the page holds."""
    page_rows = [_lex_row(f"doc:{n}", "z" * longest) for n in range(rows)]
    facts = reply_size.search_size_facts(_page(page_rows, method="lexical"), page_rows, 30_000)
    assert facts["fewer"].get("limit") == limit, facts
    assert facts["fewer"].get("max_chars") == chars, facts
    for value in facts["fewer"].values():
        assert value in (*reply_size.TIDY_LIMITS, *reply_size.TIDY_CHARS, list(HEAD_ONLY_SELECT))


def test_the_knobs_name_only_published_search_arguments() -> None:
    """Gate 1: every knob a search reply names is a published argument."""
    from okg.substrate.mcp.lean_surface import HIDDEN_ARGUMENTS

    published = {"limit", "max_chars", "select", "offset"}
    assert not published & set(HIDDEN_ARGUMENTS["search"])
    rows = [_lex_row(f"doc:{n}", "x " * 600, whole=False) for n in range(8)]
    facts = reply_size.search_size_facts(_page(rows, method="lexical"), rows, 30_000)
    named = set(facts["fewer"]) | set(facts.get("more", {}))
    assert named <= published, named
    # A 1,200-character cut passage: the next tidy value past 1.25 times it.
    assert facts["more"] == {"max_chars": 1600}


@pytest.mark.parametrize("longest", [3300, 4000, 4090])
def test_more_text_never_repeats_the_callers_max_chars_or_passes_4096(longest: int) -> None:
    """Review L1: a passage cut at `max_chars=4096` is a little short of
    4096; the reply never offers 4096 (or anything at or below what was
    served) back."""
    rows = [_lex_row("doc:1", "q" * longest, whole=False)]
    facts = reply_size.search_size_facts(_page(rows, method="lexical"), rows, 30_000)
    more = facts.get("more", {}).get("max_chars")
    assert more is None or longest < more <= 4096, facts
    assert more is None, facts  # past 4000/1.25 no tidy value fits


def test_a_page_whose_passages_were_shortened_to_fit_says_a_smaller_limit_for_more() -> None:
    rows = [_lex_row(f"doc:{n}", "y " * 300, whole=False) for n in range(10)]
    text = render(_page(rows, method="lexical", passages_shortened_to=600), tool="search")
    facts = status_facts(text)
    assert "more text: limit=5" in facts, facts
    # Review L2: one knob, one meaning: no `limit` under `fewer:` too.
    assert not any(f.startswith("fewer:") and "limit=" in f for f in facts), facts
    assert not any("max_chars=1" in f for f in facts if f.startswith("more"))


def test_no_knob_is_named_under_both_fewer_and_more() -> None:
    """Review L2, for max_chars: a cut passage names `more text: max_chars`,
    so `fewer:` does not name `max_chars` as well."""
    rows = [_lex_row(f"doc:{n}", "v" * 1000, whole=False) for n in range(10)]
    facts = reply_size.search_size_facts(_page(rows, method="lexical"), rows, 30_000)
    assert set(facts["fewer"]).isdisjoint(facts.get("more", {})), facts


def test_a_reply_under_2_kb_says_nothing_of_its_size() -> None:
    """Under 2 KB (about 500 tokens) there is nothing to steer, and the
    facts would cost a tenth of the reply: the line is as before."""
    text = render(_page([_grep_row(1, lines=[
        {"text": "short", "field": None, "line": 3, "author": None, "date": None},
    ])]), tool="search")
    assert _size_of(text) < reply_size.SIZE_LINE_FROM_BYTES
    assert status_facts(text) == ['(exact text) "rollout"', "1 record, all shown"]
    # The facts start at the bound, measured without them.
    big = render(_page([_grep_row(n) for n in range(12)]), tool="search")
    assert size_text(_size_of(big)) in status_facts(big)


@pytest.mark.parametrize("total", [12, 40])
def test_the_ending_stays_the_last_fact(total: int) -> None:
    """The documented grammar: the ending is the last fact, so a program
    that reads it by position still finds it. The size goes just before."""
    text = render(_page([_grep_row(n) for n in range(12)], total=total), tool="search")
    facts = status_facts(text)
    ending = "next: offset=12" if total > 12 else "12 records, all shown"
    assert facts[-1] == ending, facts
    size_at = facts.index(size_text(_size_of(text)))
    assert all(not f.startswith("next:") for f in facts[:size_at]), facts
    node = render(_node(9000), tool="inspect", kind="node")
    assert status_facts(node)[-1] == "complete", status_facts(node)


def test_the_json_form_carries_the_same_facts_with_its_own_size() -> None:
    from okg.substrate.mcp.response_bounds import served_text

    page = _page([_grep_row(n) for n in range(6)], total=9)
    with result_context(ResultContext(format="json", tool="search")):
        served = served_text(page)
    body = json.loads(served)
    assert body[REPLY_SIZE_KEY]["bytes"] == _size_of(served)
    assert body[REPLY_SIZE_KEY]["rows"] == 6
    assert body[REPLY_SIZE_KEY]["fewer"]["select"] == list(HEAD_ONLY_SELECT)
    assert {k: v for k, v in body.items() if k != REPLY_SIZE_KEY} == page


def test_a_refusal_says_no_size() -> None:
    """Failure case: a refused call has no rows and no knobs to name."""
    refused = {"status": "refused", "operator_name": "search",
               "error": {"code": "invalid_argument", "message": "bad " * 900}}
    first = render(refused, tool="search").splitlines()[0]
    assert first.startswith("search refused") and "fewer" not in first
    assert not re.search(r"\d( KB| B)( ·|$)", first), first


def test_the_fit_loop_counts_the_size_line_so_a_reply_never_exceeds_its_budget() -> None:
    """The byte measure renders the size facts, so a page fitted to the
    budget with them is the page served, and still fits."""
    from okg.substrate.mcp.response_bounds import (
        _shortened_search_page,
        public_call_tool_result_bytes,
    )

    rows = [_grep_row(n, lines=[
        {"text": f"{n} " + "w " * 900, "field": None, "line": 5, "author": None,
         "date": None},
    ]) for n in range(100)]
    page = _page(rows)

    def measure(payload: dict[str, Any]) -> int:
        return public_call_tool_result_bytes(payload, tool="search")

    budget = 40_000
    assert measure(page) > budget
    fitted = _shortened_search_page(page, {"limit": 100}, budget=budget, measure=measure)
    assert fitted is not None and measure(fitted) <= budget
    text = render(fitted, tool="search")
    assert size_text(_size_of(text)) in status_facts(text)


# ── 7. inspect says its size and how to get a smaller reply ──────────────


def _node(attr_text: int) -> dict[str, Any]:
    return {
        "status": "ok", "node_id": "doc:big", "subtype": "document", "provenance": TRUSTED,
        "attrs": {"title": "Rollout plan", "status": "final", "body": "z " * attr_text},
    }


def test_inspect_of_a_large_node_names_a_select_and_a_max_bytes() -> None:
    text = render(_node(9000), tool="inspect", kind="node")
    facts = status_facts(text)
    size = _size_of(text)
    assert size_text(size) in facts
    smaller = next(f for f in facts if f.startswith("smaller: "))
    assert smaller == (
        f'smaller: select=["attrs.title","attrs.status"], max_bytes={size // 2 // 1024 * 1024}'
    )


def test_inspect_of_a_small_node_says_no_size() -> None:
    text = render(_node(10), tool="inspect", kind="node")
    facts = status_facts(text)
    assert facts[-1] == "complete", facts
    assert not any("smaller" in f or f.endswith(" B") for f in facts)


def test_inspect_text_names_max_bytes_and_adds_no_default_cap() -> None:
    """No new cap on inspect: the payload is printed as served, whole."""
    body = "line of the document text\n" * 400
    page = _text_page(body)
    text = render(page, tool="inspect", kind="text")
    assert body.strip() in text
    facts = status_facts(text)
    assert any(f.startswith("smaller: max_bytes=") for f in facts), facts


# ── 2. modest defaults ───────────────────────────────────────────────────


def test_search_and_grep_return_eight_rows_when_limit_is_omitted() -> None:
    assert search_views.CARDS_LIMIT == 8
    assert search_views.default_search_limit(None) == 8
    assert search_views.default_search_limit("cards") == 8
    # The maxima stay: cards 1000, list 100.
    assert search_views.CARDS_MAX_LIMIT == 1000 and search_views.LIST_MAX_LIMIT == 100


def test_a_default_passage_holds_at_most_800_characters() -> None:
    from okg.substrate.mcp.excerpt_window import fallback_matcher
    from okg.substrate.mcp.record_text import text_passage

    words = " ".join(f"word{n}" for n in range(600)) + " anchor " + " ".join(
        f"tail{n}" for n in range(600))
    passage = text_passage(words, fallback_matcher("anchor"), budget=1_000, basis="own_text")
    assert passage is not None and len(passage["text"]) <= 800, len(passage["text"])
    assert "anchor" in passage["text"]
    # max_chars still sets any length up to 4096.
    longer = text_passage(words, fallback_matcher("anchor"), budget=1_000,
                          basis="own_text", chars=4096)
    assert longer is not None and len(longer["text"]) > 3000


def test_a_default_code_passage_holds_at_most_800_characters() -> None:
    from okg.substrate.mcp.excerpt_window import fallback_matcher
    from okg.substrate.mcp.record_text import text_passage

    code = "\n".join(f"    value_{n} = compute(value_{n - 1}, {n})" for n in range(1, 120))
    code = code.replace("value_60 =", "anchor_60 =")
    passage = text_passage(code, fallback_matcher("anchor_60"), budget=1_000,
                           basis="own_text", code=True, first_line=1,
                           signature="def build(values):", docstring="Build it.")
    assert passage is not None and len(passage["text"]) <= 800, len(passage["text"])
    assert "anchor_60" in passage["text"]


def test_a_long_comment_is_cut_to_800_characters_by_default() -> None:
    from okg.substrate.mcp.excerpt_window import UNIT_WHOLE_CHARS
    from okg.substrate.mcp.grep_search import PROSE_LINE_WHOLE_CHARS, _prose_text

    assert UNIT_WHOLE_CHARS == PROSE_LINE_WHOLE_CHARS == 800
    text = "start " + "lorem ipsum " * 100 + "needle " + "dolor sit " * 60
    at = text.index("needle")
    shown, left = _prose_text(text, 0, len(text), at, at + 6, chars=None)
    assert len(shown) <= 800 and "needle" in shown and left > 0
    whole, left = _prose_text(text, 0, len(text), at, at + 6, chars=4096)
    assert whole == text.strip() and left == 0


def test_the_old_defaults_are_named_nowhere() -> None:
    """Every place that names the default says the new one."""
    from pathlib import Path

    from okg.substrate.cli._read_help_text import READ_HELP
    from okg.substrate.mcp.lean_surface import HIDDEN_NOTES

    assert "limit=8" in " ".join(READ_HELP["search"]["description"])
    assert "(cards: 8)" in HIDDEN_NOTES["search(view=...)"]
    root = Path(__file__).resolve().parents[3]
    results = (root / "docs/reference/mcp-results.md").read_text(encoding="utf-8")
    assert '`"cards"` is the search card, `limit` default 8' in results
    mcp = (root / "docs/reference/mcp.md").read_text(encoding="utf-8")
    assert '`"cards"` is the card above (default 8)' in mcp


# ── 3. select narrows search and grep ────────────────────────────────────


@pytest.mark.parametrize("select", [
    ["node_id", "handle"], ["node_id"], ["node_id", "subtype", "provenance"],
    ["canonical_node_id", "evidence_kind", "handle"],
])
def test_a_select_of_id_and_head_fields_is_head_lines_only(select: list[str]) -> None:
    assert select_is_head_only(select)


def test_head_names_are_fields_search_serves() -> None:
    """Review L3: every name that narrows is a select field of the search
    shapes that serve it (lexical and grep), so the claim is about selects
    search accepts. `where` names (`title`, `path`, `source`) reach search
    already read as `handle` or `provenance` by the read grammar."""
    from okg.substrate.mcp.select_vocabulary import WHERE_NAMES_IN_ROWS, shape_fields

    served = shape_fields("search")["lexical"] & shape_fields("search")["grep"]
    assert reply_size.HEAD_SELECT_NAMES <= served, reply_size.HEAD_SELECT_NAMES - served
    assert set(WHERE_NAMES_IN_ROWS.values()) <= reply_size.HEAD_SELECT_NAMES
    for name in ("title", "path", "source", "id", "type", "status", "created"):
        assert not select_is_head_only(["node_id", name]), name


@pytest.mark.parametrize("select", [
    None, [], ["node_id", "excerpt"], ["node_id", "match_lines"], ["node_id", "text"],
    ["node_id", "attrs"], ["node_id", "attrs.body"], ["node_id", "attrs.status"],
    ["node_id", "score"],
])
def test_a_select_naming_text_keeps_passages_and_match_lines(select: Any) -> None:
    """The 10-15 guard: select=["node_id","attrs"] keeps the matched lines."""
    assert not select_is_head_only(select)


def _served_search(monkeypatch: pytest.MonkeyPatch, page: dict[str, Any],
                   select: list[str]) -> dict[str, Any]:
    search_mod = importlib.import_module("okg.substrate.mcp.search")

    seen: dict[str, Any] = {}

    def pinned(conn: Any, session: Any, query: str, **kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return page

    monkeypatch.setattr(search_mod, "_search_pinned", pinned)
    return search_mod.search(None, None, "rollout", select=select, method="grep")


def test_search_serves_head_lines_only_for_a_head_only_select(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [_grep_row(n) for n in range(3)] + [_lex_row("doc:1", "a passage")]
    served = _served_search(monkeypatch, _page(rows), ["node_id", "handle"])
    for row in served["results"]:
        assert not {"passage", "match_lines", "excerpt"} & set(row), row
        assert row["head"]["id"] == row["node_id"]
    with result_context(ResultContext(tool="search", select=("node_id", "handle"))):
        text = render(served, tool="search")
    assert "L11:" not in text and "a passage" not in text


@pytest.mark.parametrize("select", [["node_id", "attrs"], ["node_id", "attrs.body"],
                                    ["node_id", "excerpt"]])
def test_search_keeps_match_lines_when_the_select_names_text(
    monkeypatch: pytest.MonkeyPatch, select: list[str],
) -> None:
    rows = [_grep_row(n) for n in range(3)]
    served = _served_search(monkeypatch, _page(rows), select)
    assert all(row["match_lines"] for row in served["results"])
    text = render(served, tool="search")
    assert any(line.startswith("    L10: line 0: we moved") for line in text.splitlines())


def test_narrowing_copies_the_row() -> None:
    row = _grep_row(1)
    narrowed = narrow_row(row)
    assert "match_lines" in row and "match_lines" not in narrowed


def test_search_publishes_select() -> None:
    """Gate 1: the size line names select, so search publishes it."""
    from okg.substrate.cli._read_help_text import READ_HELP

    assert READ_HELP["search"]["arguments"]["select"] == "list of string"
    assert not READ_HELP["search"]["arguments"]["select"].startswith("accepted")


# ── 4. one tool-description line ─────────────────────────────────────────


def test_search_description_says_read_narrow_first() -> None:
    from okg.substrate.cli._read_help_text import READ_HELP

    description = " ".join(READ_HELP["search"]["description"])
    assert (
        'Read narrow first: ids and titles (select=["node_id","handle"] or a '
        "small limit), then read the records that matter."
    ) in description


# ── 5. what this session already showed ─────────────────────────────────


def test_every_passage_shown_is_remembered_not_only_the_last() -> None:
    """(a) A, then B, then A again: the third prints the pointer."""
    state: dict[str, Any] = {}
    a, b = _lex_row("doc:1", "passage A text"), _lex_row("doc:1", "passage B text")
    render(_page([a], method="lexical"), tool="search", session_state=state)
    render(_page([b], method="lexical", query="other"), tool="search", session_state=state)
    third = render(_page([a], method="lexical"), tool="search", session_state=state)
    lines = third.splitlines()
    assert '    shown earlier (search "rollout")' in lines, third
    assert "    passage A text" not in lines


def test_grep_prints_only_lines_not_shown_before() -> None:
    """(b) All lines shown before: the pointer. Some: the new ones and a
    count of the rest."""
    state: dict[str, Any] = {}
    first = [{"text": "alpha", "field": None, "line": 1, "author": None, "date": None},
             {"text": "beta", "field": None, "line": 2, "author": None, "date": None}]
    render(_page([_grep_row(1, lines=first)]), tool="search", session_state=state)
    again = render(_page([_grep_row(1, lines=first[1:])], query="beta"), tool="search",
                   session_state=state).splitlines()
    assert '    shown earlier (search "rollout")' in again
    assert "    L2: beta" not in again
    mixed = render(_page([_grep_row(1, lines=[
        *first, {"text": "gamma", "field": None, "line": 3, "author": None, "date": None},
    ])], query="gamma"), tool="search", session_state=state).splitlines()
    assert "    L3: gamma" in mixed and "    L1: alpha" not in mixed
    assert "    2 lines shown earlier" in mixed


def _text_page(body: str) -> dict[str, Any]:
    total = body.count("\n") or 1
    return {
        "status": "ok", "node_id": "doc:1", "subtype": "document",
        "order_basis": "chunk_ordinal", "coverage": "complete", "complete": True,
        "truncated": False,
        "lines": {"basis": "source", "total": total, "first": 1, "last": total},
        "items": [{"chunk_id": "chunk:1", "char_offset": 0, "lines": [1, total],
                   "text": body, "provenance": {"trust": "trusted"}}],
    }


def test_a_text_read_whole_makes_a_later_row_say_shown_whole() -> None:
    """(c) inspect(text) of the whole record, not only the ticket form."""
    state: dict[str, Any] = {}
    read = render(_text_page("one\ntwo\nthree\n"), tool="inspect", kind="text",
                  session_state=state)
    assert "complete" in read.splitlines()[0]
    later = render(_page([_lex_row("doc:1", "a cut of it", whole=False)], method="lexical"),
                   tool="search", session_state=state).splitlines()
    assert "    shown whole earlier (inspect)" in later
    assert "    a cut of it" not in later


def test_a_node_read_whole_covers_rows_read_as_a_node_only() -> None:
    """(c) inspect(node) printed every attribute: a later row read as that
    node says so. A row read as text is not covered (its text is in chunks
    the node read did not print)."""
    state: dict[str, Any] = {}
    node = {**_node(10), "node_id": "doc:1"}
    render(node, tool="inspect", kind="node", session_state=state)
    as_node = render(_page([_lex_row("doc:1", "attr text", read_kind="node")],
                           method="lexical"), tool="search", session_state=state)
    assert "    shown whole earlier (inspect)" in as_node.splitlines()
    as_text = render(_page([_lex_row("doc:1", "chunk text", read_kind="text")],
                           method="lexical"), tool="search", session_state=state)
    assert "    chunk text" in as_text.splitlines()


def test_a_partial_text_read_is_not_whole() -> None:
    """Failure case: a page of some lines never marks the record whole."""
    state: dict[str, Any] = {}
    page = _text_page("one\ntwo\nthree\n")
    page["lines"] = {"basis": "source", "total": 3, "requested": [[1, 2]],
                     "served": [[1, 2]]}
    render(page, tool="inspect", kind="text", session_state=state)
    later = render(_page([_lex_row("doc:1", "a cut of it", whole=False)], method="lexical"),
                   tool="search", session_state=state).splitlines()
    assert "    a cut of it" in later


def test_filter_rows_get_the_same_pointer() -> None:
    """(d) A filter row this session already printed says so."""
    state: dict[str, Any] = {}
    page = {"items": [{"node_id": "mail:1", "subtype": "email_thread",
                      "attrs": {"title": "Ephemeral admin query whitelist " * 4}}],
            "total_count": 1, "offset": 0,
            "coverage": {"retrieval": {"status": "exhaustive"},
                         "delivery": {"complete": True}}}
    first = render(page, tool="filter", session_state=state)
    assert "Ephemeral admin query whitelist" in first
    second = render(page, tool="filter", session_state=state)
    assert "Ephemeral admin query whitelist" not in second
    assert "  shown earlier (filter)" in second.splitlines()


def _filter_page(status: str, generation: str = "g1") -> dict[str, Any]:
    return {"items": [{"node_id": "doc:1", "subtype": "document",
                       "attrs": {"title": "Rollout plan for the second week",
                                 "status": status, "owner": "the platform team"}}],
            "total_count": 1, "offset": 0, "pinned_generation_id": generation,
            "coverage": {"retrieval": {"status": "exhaustive"},
                         "delivery": {"complete": True}}}


def test_a_whole_node_read_never_stands_for_a_filter_row() -> None:
    """Review M2: after inspect(node) printed the record whole, a filter row
    of it still prints its fields: the row may carry fields, or values, the
    read did not print."""
    state: dict[str, Any] = {}
    node = {**_node(10), "node_id": "doc:1", "pinned_generation_id": "g1"}
    render(node, tool="inspect", kind="node", session_state=state)
    row = render(_filter_page("final"), tool="filter", session_state=state)
    assert "shown" not in row.replace("all shown", ""), row
    assert "the platform team" in row


def test_a_mark_holds_only_within_its_generation() -> None:
    """Review M2: a record printed at one generation and read again at
    another prints again, so a value changed by a publish is never hidden
    behind `shown earlier` / `shown whole earlier`."""
    state: dict[str, Any] = {}
    render(_filter_page("draft", "g1"), tool="filter", session_state=state)
    same = render(_filter_page("draft", "g1"), tool="filter", session_state=state)
    assert "  shown earlier (filter)" in same.splitlines()
    later = render(_filter_page("draft", "g2"), tool="filter", session_state=state)
    assert "shown earlier" not in later and "draft" in later, later
    # A search row under a whole-text mark from another generation.
    text = {**_text_page("one\ntwo\nthree\n"), "pinned_generation_id": "g2"}
    render(text, tool="inspect", kind="text", session_state=state)
    row = _page([_lex_row("doc:1", "a cut of it", whole=False)], method="lexical")
    at_g3 = render({**row, "pinned_generation_id": "g3"}, tool="search", session_state=state)
    assert "    a cut of it" in at_g3.splitlines(), at_g3


def test_the_shell_prints_no_mcp_size_facts() -> None:
    """Review L4: the `okg` shell renders the status line for its own
    output (`--jsonl`, `--ids`, tables); an MCP size and MCP call knobs would
    describe a reply it never prints."""
    from okg.substrate.cli._read_shell import status_lines

    page = _page([_grep_row(n) for n in range(12)], total=40)
    assert size_text(_size_of(render(page, tool="search"))) in status_facts(
        render(page, tool="search"),
    )
    head = status_lines("search", page, {"query": "rollout"})[0]
    assert " KB" not in head and "fewer:" not in head, head
    assert head.endswith("rows 1-12 of 40 · next: offset=12"), head


def test_a_cut_thread_with_no_comment_shown_says_none_of_them() -> None:
    """Review L5: at a tight budget the size facts can leave no room for a
    comment; the cut line then says none of the comments is shown and
    claims no range."""
    from okg.substrate.mcp.result_text import _record_text_cut_line

    line = _record_text_cut_line({
        "chars": 40_000, "served_chars": 1_200,
        "description": {"chars": 1_200, "served_chars": 1_200},
        "comments": {"total": 60, "shown": 0, "first": None,
                     "earlier_left": 51, "later_left": 9},
    })
    assert line.startswith("! record text cut to fit max_bytes: "), line
    assert line.endswith(": the description and none of its 60 comments"), line
    assert "comment 1" not in line and "left out" not in line


def test_the_session_memory_stays_bounded() -> None:
    """Gate 3: at most `_SERVED_CAP` entries over all records."""
    from okg.substrate.mcp import result_text

    state: dict[str, Any] = {}
    cap = result_text._SERVED_CAP
    for n in range(cap + 50):
        result_text._mark_served(state, f"doc:{n % 300}", "search", f"key-{n}")
    served = state["served"]
    held = sum(len(e["keys"]) + len(e["whole"]) for e in served.values())
    assert held <= cap and state[result_text._SERVED_COUNT] == held


def test_without_a_session_nothing_is_remembered() -> None:
    """Failure case: a byte measurement renders without a session; every
    passage prints, so a measured page is never smaller than the served one."""
    for _ in range(2):
        text = render(_page([_lex_row("doc:1", "passage A text")], method="lexical"),
                      tool="search")
        assert "    passage A text" in text.splitlines()


# ── constants restated in the light module ───────────────────────────────


def test_restated_bounds_match_the_server() -> None:
    from okg.substrate.mcp.operator_models import MAX_PREVIEW_CHARS
    from okg.substrate.mcp.response_bounds import MIN_RESPONSE_BYTES

    assert reply_size.MIN_MAX_BYTES == MIN_RESPONSE_BYTES
    assert reply_size.MAX_CHARS_CEILING == MAX_PREVIEW_CHARS
