"""`search(method="grep", view="list")`: every match, compactly (issue #3659).

One row per matching record (id, type, how many of its text units match,
where the first match is), 100 rows a page, no card content. Membership,
order, counts, bounds, continuation and access checks are grep's own.
"""
from __future__ import annotations

import json
import re

import pytest

from okg.substrate.mcp import result_text
from okg.substrate.mcp.search_views import (
    GREP_LIST_FIELDS,
    SearchViewUnsupported,
    grep_list_projection,
    resolve_search_view,
)

# --------------------------------------------------------------------------- #
# (a) pure                                                                     #
# --------------------------------------------------------------------------- #

FENCE_OPEN = "⟦UNTRUSTED_DATA source=connector:x⟧\n"
FENCE_CLOSE = "\n⟦/UNTRUSTED_DATA⟧"


def _resolve(**overrides):
    arguments = {"method": "grep", "rank_by": None, "max_per_parent_argument": False,
                 "limit": None, "select": None, **overrides}
    return resolve_search_view("list", **arguments)


def test_a_grep_list_defaults_to_twenty_rows_and_keeps_grep_rows():
    # 20 (was 100; PACT read-names-list-3666): the list is grep's default
    # delivery now, so its omitted limit is a short page.
    assert _resolve() == (20, None)
    assert _resolve(limit=25) == (25, None)


def test_a_grep_list_refuses_more_than_one_hundred_rows_and_any_select():
    with pytest.raises(SearchViewUnsupported) as excinfo:
        _resolve(limit=101)
    assert "limit <= 100" in excinfo.value.fix_hint
    with pytest.raises(SearchViewUnsupported) as excinfo:
        _resolve(select=["node_id"])
    # The resolver a direct Python caller reaches: its rows are fixed. The
    # served tool cuts them to a select before this runs (#3666 step 3a).
    assert "this read takes no select" in str(excinfo.value)
    assert excinfo.value.fix_hint == (
        "omit select here; the served tool cuts the finished rows to a select of those row keys")
    assert excinfo.value.data["supported_select"] == list(GREP_LIST_FIELDS)


@pytest.mark.parametrize("method", ["semantic", "alias", "identifier_mentions"])
def test_a_list_on_another_method_names_both_methods_that_serve_it(method):
    with pytest.raises(SearchViewUnsupported) as excinfo:
        _resolve(method=method)
    assert "method='lexical' or method='grep'" in excinfo.value.fix_hint
    assert excinfo.value.data["list_served_on"] == (
        "method=lexical rank_by=records; method=grep; a where with no query")


def _page(**extra):
    long_line = "needle " + "y" * 300
    return {
        "method": "grep", "total_count": 3, "total_count_basis": "exact", "next_offset": None,
        "page_basis": "top_of_all_visible_matches", "result_unit": "record",
        "parent_relation": {"scanned": 9}, "max_chars": 500,
        "results": [
            {"node_id": "doc:1", "subtype": "doc", "excerpt": "own text needle",
             "matching_passages": 2, "evidence_kind": "unknown", "method": "grep",
             "provenance": {"trust": "trusted"},
             "match_windows": [
                 {"char_start": 9, "char_end": 15, "line": 3, "excerpt": "own text needle"},
                 {"char_start": 0, "char_end": 6, "line": 1, "excerpt": "needle p", "passage_id": "c:1"},
             ]},
            {"node_id": "doc:2", "subtype": "doc", "excerpt": None, "matching_passages": 3,
             "provenance": {"trust": "untrusted"},
             "match_windows": [
                 {"char_start": 4, "char_end": 10, "line": 7, "passage_id": "c:9",
                  "excerpt": FENCE_OPEN + long_line + FENCE_CLOSE},
             ]},
            {"node_id": "doc:3", "subtype": "doc", "excerpt": "needle", "matching_passages": None,
             "match_windows": [{"char_start": 0, "char_end": 6, "line": 1, "excerpt": "needle"}]},
        ],
        **extra,
    }


def test_projection_keeps_the_page_and_makes_each_row_compact():
    source = _page()
    page = grep_list_projection(source)
    assert page["view"] == "list"
    for key in ("total_count", "total_count_basis", "next_offset", "page_basis", "method"):
        assert page[key] == source[key]
    assert page["parent_relation"] == {} and "max_chars" not in page
    one, two, three = page["results"]
    assert one == {
        "node_id": "doc:1", "subtype": "doc", "matches": 3,
        "first_match": {"chunk": "doc:1", "line": 3, "text": "own text needle"},
        "provenance": {"trust": "trusted"},
    }
    # Matched only through its passages: three units, first in a passage.
    assert two["matches"] == 3 and two["first_match"]["chunk"] == "c:9"
    assert two["first_match"]["line"] == 7
    assert three["matches"] == 1
    # The source page is not changed.
    assert "match_windows" in source["results"][0]


def test_the_line_is_served_as_grep_cut_it_fence_and_all():
    text = grep_list_projection(_page())["results"][1]["first_match"]["text"]
    assert text.startswith(FENCE_OPEN) and text.endswith(FENCE_CLOSE)


def test_the_first_match_is_the_earliest_in_the_own_text_else_in_the_first_passage():
    page = _page()
    page["results"][0]["match_windows"] = [
        {"char_start": 300, "char_end": 306, "line": 40, "excerpt": "later term"},
        {"char_start": 0, "char_end": 6, "line": 1, "excerpt": "passage", "passage_id": "c:1"},
        {"char_start": 20, "char_end": 23, "line": 2, "excerpt": "earlier term"},
    ]
    page["results"][1]["match_windows"] = [
        {"char_start": 90, "char_end": 96, "line": 9, "excerpt": "b", "passage_id": "c:9"},
        {"char_start": 5, "char_end": 9, "line": 2, "excerpt": "a", "passage_id": "c:9"},
        {"char_start": 0, "char_end": 4, "line": 1, "excerpt": "z", "passage_id": "c:2"},
    ]
    one, two, _ = grep_list_projection(page)["results"]
    assert one["first_match"] == {"chunk": "doc:1", "line": 2, "text": "earlier term"}
    assert two["first_match"] == {"chunk": "c:9", "line": 2, "text": "a"}


def test_a_query_with_no_text_term_counts_the_own_unit_and_shows_no_line():
    # A `path:` query leaves no window. Grep says whether the record's own
    # unit matched; the row counts it and has no first_match.
    page = _page()
    page["results"] = [
        {"node_id": "doc:1", "subtype": "doc", "excerpt": None, "matching_passages": 3,
         "match_windows": [], "own_match": True},
        {"node_id": "doc:2", "subtype": "doc", "excerpt": None, "matching_passages": 3,
         "match_windows": [], "own_match": False},
        {"node_id": "doc:3", "subtype": "doc", "excerpt": None, "matching_passages": None,
         "match_windows": [], "own_match": True,
         "parent": {"node_id": "folder:1", "state": "resolved", "summary": "x"}},
    ]
    rows = grep_list_projection(page)["results"]
    assert [row["matches"] for row in rows] == [4, 3, 1]
    assert all("first_match" not in row and "own_match" not in row for row in rows)
    assert rows[2]["parent"] == {"node_id": "folder:1"}
    text = result_text.render(
        {**grep_list_projection(page), "operator_name": "search"}, tool="search")
    assert "[1] doc:1 (doc) · 4 matches" in text and " · first " not in text
    assert "in folder:1" in text


def test_a_page_that_stopped_early_says_its_counts_are_what_was_scanned():
    page = grep_list_projection(_page(matching_passages_basis="scanned"))
    assert page["matches_basis"] == "scanned" and "matching_passages_basis" not in page
    assert "matches_basis" not in grep_list_projection(_page())
    text = result_text.render({**page, "operator_name": "search"}, tool="search")
    assert " · 3+ matches · " in text and " · 1+ matches · " in text


def test_a_passage_page_labels_its_rows():
    page = grep_list_projection(_page(result_unit="passage"))
    assert all(row["unit"] == "passage" for row in page["results"])


def test_text_is_one_line_per_record_with_count_place_and_quoted_line():
    text = result_text.render(
        {**grep_list_projection(_page()), "operator_name": "search"}, tool="search")
    rows = [line for line in text.split("\n") if re.match(r"\[\d+\] ", line)]
    assert len(rows) == 3
    assert rows[0].startswith("[1] doc:1 (doc) · 3 matches · first line 3 · text=\"own text needle\"")
    assert "[2] doc:2 (doc) · 3 matches · first c:9 line 7 · text " in rows[1]
    assert rows[2].startswith("[3] doc:3 (doc) · 1 match · first line 1 · ")
    assert "match_windows" not in text and "char_start" not in text


# --------------------------------------------------------------------------- #
# (b) database: the served tool                                                #
# --------------------------------------------------------------------------- #

psycopg = pytest.importorskip("psycopg")

from tests.substrate.mcp.paging_mismatch import describe  # noqa: E402
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes  # noqa: E402
from tests.substrate.mcp.test_search_list import (  # noqa: E402
    DOC,
    DOC_CHUNK,
    EXTERNAL,
    READER,
    TERM,
    VISIBLE_RECORDS,
    Served,
    _activate,
    _corpus,
    _edge,
    _ids,
    _node,
    _refusal_code,
    _wire_bytes,
)

ROW_KEYS = {"node_id", "subtype", "matches", "first_match", "provenance"}
#: A row may also name its resolved parent and its read-time trust.
OPTIONAL_ROW_KEYS = {"read_provenance", "parent", "handle"}
EXACT = {"completeness": "exact"}


@pytest.fixture
def served(clean_runtime, okg_mcp_can_log_in, tmp_path):
    server = Served(clean_runtime, _corpus(tmp_path, clean_runtime))
    try:
        yield server
    finally:
        server.close()


def _all_rows(served, **arguments) -> tuple[list[dict], list[dict]]:
    """Every page of a grep list, by the served `next_offset` alone."""
    pages, rows, offset = [], [], 0
    while offset is not None:
        page = served.json(query=TERM, method="grep", view="list", offset=offset, **arguments)
        assert page.get("status") != "refused", page.get("error")
        pages.append(page)
        rows += page["results"]
        offset = page["next_offset"]
        assert len(pages) < 100
    return rows, pages


@pytest.mark.postgres
def test_the_list_holds_the_records_of_the_cards_in_their_order_with_the_exact_count(served):
    cards = served.json(query=TERM, method="grep", view="cards", limit=200, max_bytes=262_144,
                        **EXACT)
    assert cards["total_count"] == VISIBLE_RECORDS and cards["total_count_basis"] == "exact"
    rows, pages = _all_rows(served, max_bytes=262_144, limit=100, **EXACT)
    # 100 rows a page (read-names-list-3666: 20 when limit is omitted), then the rest.
    assert [len(page["results"]) for page in pages] == [100, VISIBLE_RECORDS - 100]
    assert pages[0]["limit"] == 100 and pages[0]["next_offset"] == 100
    assert [row["node_id"] for row in rows] == _ids(cards)
    assert len({row["node_id"] for row in rows}) == VISIBLE_RECORDS
    for index, page in enumerate(pages):
        assert page["view"] == "list" and page["completeness"] == "exact"
        assert page["total_count"] == VISIBLE_RECORDS and page["total_count_basis"] == "exact"
        # Each row's count is what the same cards page says of that record
        # (its own text if it matches, plus its matching passages), and the
        # page says so when those counts are of the passages scanned so far.
        same_cards = served.json(query=TERM, method="grep", view="cards", limit=100,
                                 offset=index * 100,
                                 max_bytes=262_144, **EXACT)
        assert _ids(same_cards) == [row["node_id"] for row in page["results"]]
        assert page.get("matches_basis") == same_cards.get("matching_passages_basis")
        for card, row in zip(same_cards["results"], page["results"]):
            own = 1 if card.get("excerpt") is not None else 0
            assert row["matches"] == max((card.get("matching_passages") or 0) + own, 1), row
    for row in rows:
        assert set(row) <= ROW_KEYS | OPTIONAL_ROW_KEYS and ROW_KEYS <= set(row), row
        # #3666: no record here has text `inspect` text reads (its passages
        # are `doc_chunk`), so every line says what it counts: lines of the
        # passage, or of the record's own text.
        in_passage = row["first_match"]["chunk"] != row["node_id"]
        assert set(row["first_match"]) == {"chunk", "line", "text", "line_basis"}
        assert row["first_match"]["line_basis"] == ("passage" if in_passage else "own_text")
        assert row["matches"] >= 1 and row["first_match"]["line"] == 1


@pytest.mark.postgres
def test_a_page_that_read_every_match_counts_each_records_matching_text_units(served):
    # Only the fifteen passages hold "zirconium passage": five documents,
    # three matching passages each, none through the document's own text.
    page = served.json(query=f'"{TERM} passage"', method="grep", view="list", **EXACT)
    assert page["total_count"] == 5 and "matches_basis" not in page
    assert {row["node_id"]: row["matches"] for row in page["results"]} == {
        f"doc:{i:03d}": 3 for i in range(5)
    }
    assert all(row["first_match"]["chunk"].startswith("doc_chunk:") for row in page["results"])
    # One document through its own text and its three passages: four units,
    # the first match in the document itself.
    page = served.json(query="re:/body 3\\.|of document 3\\./", method="grep", view="list", **EXACT)
    assert [(row["node_id"], row["matches"], row["first_match"]["chunk"])
            for row in page["results"]] == [("doc:003", 4, "doc:003")]
    text = served.text(query="re:/body 3\\.|of document 3\\./", method="grep", view="list")
    assert re.search(
        r'\[1\] doc:003 \(doc\) · "Document 003" · "docs/003.md" · .* · 4 matches · '
        r"first own-text line 1 · text", text)
    # Matched only through its passages.
    page = served.json(query="folded passage", method="grep", view="list", **EXACT)
    (row,) = page["results"]
    assert (row["node_id"], row["matches"]) == ("doc:passages-only", 3)
    assert row["first_match"]["chunk"].startswith("doc_chunk:po-")


@pytest.mark.postgres
def test_a_list_page_carries_many_more_records_than_cards_in_the_same_bytes(served):
    budget = 8192
    (listed,) = served.calls(({"query": TERM, "method": "grep", "view": "list",
                               "max_bytes": budget}, "json"))
    (cards,) = served.calls(({"query": TERM, "method": "grep", "view": "cards", "limit": 100,
                              "max_bytes": budget}, "json"))
    assert _wire_bytes(listed) <= budget and _wire_bytes(cards) <= budget
    list_rows = len(json.loads(listed.content[0].text)["results"])
    card_rows = len(json.loads(cards.content[0].text)["results"])
    # read-passage-rows-3666 (D4): a card page over its budget now shortens
    # its passages before it drops a row, so cards hold more rows than they
    # did (11, from about a third of the list's); the list still holds more.
    assert list_rows > card_rows > 0, (list_rows, card_rows)
    # Byte-bounded pages still reach every record once.
    rows, pages = _all_rows(served, max_bytes=budget, **EXACT)
    assert len(rows) == len({row["node_id"] for row in rows}) == VISIBLE_RECORDS, describe(
        pages, expected_count=VISIBLE_RECORDS, note=f"grep list pages under max_bytes={budget}")
    assert len(pages) > 2


@pytest.mark.postgres
def test_text_rows_are_single_safe_lines_that_page_by_the_status_line(served):
    text = served.text(query=TERM, method="grep", view="list", limit=100, max_bytes=262_144,
                       **EXACT)
    status = text.split("\n", 1)[0]
    assert status.startswith("search ok") and " · list" in status
    assert f"{VISIBLE_RECORDS}" in status and "offset=100" in status
    rows = [line for line in text.split("\n") if re.match(r"\[\d+\] ", line)]
    assert len(rows) == 100
    # This page stopped once it was full, so its counts are of what was
    # scanned and say so with a plus sign.
    for line in rows:
        assert re.search(r" · \d+\+ matches · first (\S+ )?line 1 · text", line), line
    # Hostile source text never starts a row or closes a fence early.
    cards = result_text.result_cards(text)
    assert len(cards) == 100 and all(not card.ident.startswith("forged") for card in cards)
    assert "match_windows" not in text and "excerpt" not in text
    passages_only = served.text(query="folded passage", method="grep", view="list")
    assert re.search(r'doc:passages-only \(doc\) · "Passages only" · 3 matches · '
                     r"first doc_chunk:po-\d line 1 · ",
                     passages_only)


@pytest.mark.postgres
def test_a_regular_expression_with_alternatives_lists_every_record_once(served):
    page = served.json(query="re:/folded passage|hostile|undated/", method="grep",
                       view="list", **EXACT)
    assert page["total_count"] == 3 and page["total_count_basis"] == "exact"
    assert {row["node_id"]: row["matches"] for row in page["results"]} == {
        "doc:passages-only": 3, "doc:hostile": 1, "doc:no-date": 1,
    }
    # A phrase that matches nothing is an exact zero, not a refusal.
    empty = served.json(query='"no such phrase anywhere"', method="grep", view="list", **EXACT)
    assert empty["results"] == [] and empty["total_count"] == 0


@pytest.mark.postgres
def test_a_path_only_query_lists_and_counts_without_a_line(served):
    page = served.json(query="path:docs/003.md", method="grep", view="list", **EXACT)
    (row,) = page["results"]
    assert row["node_id"] == "doc:003" and "first_match" not in row and "own_match" not in row
    cards = served.json(query="path:docs/003.md", method="grep", view="cards", **EXACT)
    # The document's own unit plus the passages the cards page counts.
    assert row["matches"] == 1 + (cards["results"][0].get("matching_passages") or 0)
    assert "own_match" not in cards["results"][0]
    text = served.text(query="path:docs/003.md", method="grep", view="list")
    assert re.search(r"\[1\] doc:003 \(doc\) · .* · \d+ match", text) and " · first " not in text


@pytest.mark.postgres
def test_wrong_calls_are_refused_with_the_call_that_works(served):
    # `select` is the one output control on every view (issue #3666, step
    # 3a): a served grep list is cut to the selected keys of its fixed rows,
    # and refused only when it selects none of them.
    page = served.json(query=TERM, method="grep", view="list", select=["node_id", "subtype"])
    assert page.get("status") != "refused" and page["results"], page
    assert all("matches" not in row and row["node_id"] for row in page["results"])
    page = served.json(query=TERM, method="grep", view="list", select=["excerpt"])
    assert _refusal_code(page) == "select_unknown_field"
    assert "first_match, handle, matches, node_id, parent, subtype" in page["error"]["fix_hint"]
    page = served.json(query=TERM, method="grep", view="list", limit=101)
    assert _refusal_code(page) == "search_view_unsupported"
    assert "limit <= 100" in page["error"]["fix_hint"]
    page = served.json(query="zr-alias", method="alias", view="list")
    assert _refusal_code(page) == "search_view_unsupported"
    assert "method='grep'" in page["error"]["fix_hint"]
    # The named fixes work as written.
    assert served.json(query=TERM, method="grep", view="list", limit=100)["results"]
    assert served.json(query=TERM, method="grep", select=["node_id", "subtype"])["results"]


@pytest.mark.postgres
def test_a_hidden_record_is_never_listed_or_counted(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """Through the served tool under access enforcement: a denied source's
    record is absent from the list, in text and JSON, and from its count,
    exactly as it is absent from the cards."""
    deployment = _corpus(tmp_path, clean_runtime)
    _activate(clean_runtime, deployment, [
        {"rule_id": "deny_external", "effect": "deny", "actions": ["graph.read.mcp"],
         "subjects": [READER], "match": {"source_id": EXTERNAL}, "priority": 200},
        {"rule_id": "allow_read", "effect": "allow",
         "actions": ["graph.read.mcp", "mcp.tool.call"], "subjects": [READER], "priority": 100},
    ])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    server = Served(clean_runtime, deployment)
    try:
        rows, pages = _all_rows(server, max_bytes=262_144)
        cards = server.json(query=TERM, method="grep", view="cards", limit=200, max_bytes=262_144)
        texts = [server.text(query=TERM, method="grep", view="list", offset=offset,
                             max_bytes=262_144) for offset in (0, 100)]
        only = server.json(query="external", method="grep", view="list")
    finally:
        server.close()
    ids = [row["node_id"] for row in rows]
    assert "doc:ext" not in ids and len(ids) == VISIBLE_RECORDS - 1
    assert ids == _ids(cards)
    assert pages[0]["total_count"] == cards["total_count"]
    assert pages[0]["total_count"] in (None, VISIBLE_RECORDS - 1)
    served_bytes = json.dumps(pages) + "".join(texts)
    assert "doc:ext" not in served_bytes and "Ignore previous" not in served_bytes
    # Searching for the hidden record's own word lists and counts nothing.
    assert only["results"] == [] and not only.get("total_count")

    # Failure case: with enforcement off the record is listed, so its
    # absence above comes from the policy.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    monkeypatch.delenv("OKG_NOMOS_SUBJECT", raising=False)
    server = Served(clean_runtime, deployment)
    try:
        rows, _ = _all_rows(server, max_bytes=262_144)
    finally:
        server.close()
    assert "doc:ext" in [row["node_id"] for row in rows]


@pytest.mark.postgres
def test_a_listed_passage_never_names_its_hidden_parent(
        clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """A passage the reader may see whose record is hidden is listed as its
    own row, as on grep cards, and the row does not name the hidden record."""
    from tests.substrate.mcp.test_search_list import MAIN, _trust

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment, subtypes=[DOC, DOC_CHUNK],
        narrowings=[("doc", "contains", "doc_chunk")],
        nodes=[
            _node("doc:open", "doc", "pulsar in an open record"),
            _node("doc_chunk:orphan-0", "doc_chunk", "pulsar in a passage of a hidden record"),
        ],
        edges=[_edge("doc:secret", "doc_chunk:orphan-0")],
        node_groups=[(EXTERNAL, [_node("doc:secret", "doc", "the hidden record itself")])],
    )
    _trust(clean_runtime, deployment, MAIN)
    _activate(clean_runtime, deployment, [
        {"rule_id": "deny_external", "effect": "deny", "actions": ["graph.read.mcp"],
         "subjects": [READER], "match": {"source_id": EXTERNAL}, "priority": 200},
        {"rule_id": "allow_read", "effect": "allow",
         "actions": ["graph.read.mcp", "mcp.tool.call"], "subjects": [READER], "priority": 100},
    ])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    server = Served(clean_runtime, deployment)
    try:
        listed = server.json(query="pulsar", method="grep", view="list")
        cards = server.json(query="pulsar", method="grep", view="cards")
        text = server.text(query="pulsar", method="grep", view="list")
    finally:
        server.close()
    assert _ids(listed) == _ids(cards)
    assert "doc:open" in _ids(listed)
    served_bytes = json.dumps(listed) + text
    assert "doc:secret" not in served_bytes and "the hidden record itself" not in served_bytes
    for row in listed["results"]:
        assert row.get("parent", {}).get("node_id") != "doc:secret"


@pytest.mark.postgres
def test_the_first_match_names_its_line_and_cuts_a_long_line(
        clean_runtime, okg_mcp_can_log_in, tmp_path):
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    long_line = "start " + "filler " * 60 + "quasar end of the long line"
    publish_nodes(
        clean_runtime, deployment, subtypes=[DOC, DOC_CHUNK],
        narrowings=[("doc", "contains", "doc_chunk")],
        nodes=[
            _node("doc:lines", "doc", "first line\nsecond line\nthe quasar is on line three\nlast"),
            _node("doc:long", "doc", "intro\n" + long_line),
            _node("doc:parent", "doc", "nothing here"),
            _node("doc_chunk:p-0", "doc_chunk", "a\nb\nc\nd\nquasar in a passage, line five"),
            # Four terms, each on its own line, asked for in reverse order.
            _node("doc:four", "doc", "alphaterm one\nbravoterm two\ncharlieterm three\ndeltaterm four"),
            # A pattern whose `.` crosses a line break.
            _node("doc:span", "doc", "intro\nspanfoo\nspanbar tail"),
        ],
        edges=[_edge("doc:parent", "doc_chunk:p-0")],
    )
    server = Served(clean_runtime, deployment)
    try:
        page = server.json(query="quasar", method="grep", view="list", **EXACT)
        text = server.text(query="quasar", method="grep", view="list")
        four = server.json(query="deltaterm charlieterm bravoterm alphaterm", method="grep",
                           view="list", **EXACT)
        four_cards = server.json(query="deltaterm charlieterm bravoterm alphaterm", method="grep",
                                 view="cards")
        span = server.json(query="re:/spanfoo.spanbar/", method="grep", view="list", **EXACT)
    finally:
        server.close()
    # Four terms, named in reverse order: grep keeps three windows a record,
    # so the earliest line must be among them. Through the real grep read the
    # list row reports line 1, and the cards' windows are the three earliest.
    (row,) = four["results"]
    assert row["node_id"] == "doc:four" and row["first_match"]["line"] == 1, row
    assert "alphaterm" in row["first_match"]["text"]
    assert [w["line"] for w in four_cards["results"][0]["match_windows"]] == [1, 2, 3]
    # A match that runs over a line break is reported at the line it starts on.
    (row,) = span["results"]
    assert row["node_id"] == "doc:span" and row["first_match"]["line"] == 2, row
    assert "spanfoo" in row["first_match"]["text"]
    assert page["total_count"] == 3
    rows = {row["node_id"]: row for row in page["results"]}
    assert rows["doc:lines"]["first_match"]["line"] == 3
    assert "the quasar is on line three" in rows["doc:lines"]["first_match"]["text"]
    assert "first line" not in rows["doc:lines"]["first_match"]["text"]
    long_text = rows["doc:long"]["first_match"]["text"]
    inner = re.sub(r"\n?⟦/UNTRUSTED_DATA⟧", "", re.sub(r"⟦UNTRUSTED_DATA[^⟧]*⟧\n?", "", long_text))
    # The line is cut around the match: the matched word is in what is left.
    assert "quasar" in inner and len(inner) <= 106
    assert rows["doc:long"]["first_match"]["line"] == 2
    # #3666: a `doc_chunk` passage is not text `inspect` reads as part of the
    # record, so the line stays the passage's own and the row says so.
    assert rows["doc:parent"]["first_match"] == {
        "chunk": "doc_chunk:p-0", "line": 5,
        "text": rows["doc:parent"]["first_match"]["text"],
        "line_basis": "passage",
    }
    assert "quasar in a passage" in rows["doc:parent"]["first_match"]["text"]
    assert re.search(r"doc:parent \(doc\) · 1 match · first doc_chunk:p-0 line 5 · ", text)
    assert re.search(r"doc:parent \(doc\) · .* · line counted in the passage", text)
    assert rows["doc:lines"]["first_match"]["line_basis"] == "own_text"
    # read-names-list-3666: "own-text" beside the line (was "line counted in
    # its own text" after it).
    assert re.search(r"doc:lines \(doc\) · 1 match · first own-text line 3 · ", text)
    assert "line counted in its own text" not in text
