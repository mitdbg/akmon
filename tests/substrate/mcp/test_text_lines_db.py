"""Line-range text reads through a real server and publish (issue #3666).

One published file-like document: 120 lines of 50 characters, cut into four
chunks in the middle of lines, each chunk placed by its span, so the read's
coverage is `complete` and its line numbers are the file's. A second
document lacks the second chunk, so its lines are lines of the shown text.
`inspect` text and texts take `lines`; a grep match in a passage reports the
line a range read returns; and under access enforcement a reader's line
numbers count only the text that reader is served.

The last section is text matching as a condition (step M7): ticket keys,
commit messages and release labels matched on filter, map, aggregate and
expand over a second small graph, exactly or refused.
"""
from __future__ import annotations

import json
import pathlib
import re
import uuid
from typing import Any

import jsonschema
import pytest

from okg.substrate.library.sources.base import EdgeFact
from okg.substrate.mcp.result_text import fact_value, status_facts
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp._result_payload import JSON_META, payload, result_text
from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes
from tests.substrate.mcp.test_page_next_db import CHUNK, DOC, _drive

pytestmark = pytest.mark.postgres

MARKER = "zirconiumneedle"
MARKER_LINE = 77
LINE_COUNT = 120


def _line(i: int) -> str:
    words = f"line {i:03d} " + (f"{MARKER} here " if i == MARKER_LINE else "plain filler ")
    return words.ljust(49, "x") + "\n"


SOURCE_LINES = [_line(i) for i in range(1, LINE_COUNT + 1)]
SOURCE = "".join(SOURCE_LINES)
#: Chunk boundaries, each in the middle of a line.
CUTS = [0, 1525, 3010, 4535, len(SOURCE)]
FILE, GAPPED = "doc:file", "doc:gapped"

_FENCED = re.compile(r"⟦UNTRUSTED_DATA[^⟧]*⟧\n?(.*?)\n?⟦/UNTRUSTED_DATA⟧", re.DOTALL)


def unfence(text: str) -> str:
    return re.sub(r"\n?⟦/UNTRUSTED_DATA⟧", "", re.sub(r"⟦UNTRUSTED_DATA[^⟧]*⟧\n?", "", text))


def _bodies(text: str) -> str:
    """The source text of a text-form page: every fenced body, in order."""
    return "".join(_FENCED.findall(text))


def _lines(first: int, last: int) -> str:
    return "".join(SOURCE_LINES[first - 1:last])


#: A record whose own text and whose chunks both hold one line, on line 3
#: of the own text and line 5 of the chunk text, and one with own text only.
OWNED, SOLO = "doc:owned", "doc:solo"
OWN_LINE = "ownneedle sits alone on this line"
OWN_TEXT = f"a title\nan abstract\n{OWN_LINE}\nlast"
OWNED_SOURCE = f"intro one\nintro two\nintro three\nintro four\n{OWN_LINE}\nouter\n"
OWNED_CUTS = [0, 25, len(OWNED_SOURCE)]
SOLO_TEXT = "first\nsecond\nsoloneedle is here"


def _document(
    doc_id: str, *, source: str = SOURCE, cuts: list[int] = CUTS,
    own: str = "a file of numbered lines", skip: frozenset[int] = frozenset(),
) -> tuple[list, list]:
    nodes = [node(doc_id, "document", own, title=doc_id)]
    edges = []
    spans = list(zip(cuts, cuts[1:]))
    for i, (start, end) in enumerate(spans):
        if i in skip:
            continue
        chunk_id = f"{doc_id}:chunk:{i}"
        nodes.append(node(chunk_id, "document_chunk", source[start:end], chunk_index=i))
        edges.append(EdgeFact(
            chunk_id, doc_id, "member_of",
            {"chunk_ordinal": i, "chunk_total": len(spans), "char_offset": start,
             "char_end": end, "doc_char_length": len(source)},
            source_record_id={"id": f"{chunk_id}-m"}, source_revision={"v": 1},
        ))
        edges.append(EdgeFact(
            src=doc_id, dst=chunk_id, edge_type="contains",
            source_record_id={"id": f"{chunk_id}-c"}, source_revision={"v": 1},
        ))
    return nodes, edges


@pytest.fixture
def deployment(clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path) -> str:
    for cut in CUTS[1:-1]:
        assert SOURCE[cut - 1] != "\n", "every chunk boundary must fall inside a line"
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    whole_nodes, whole_edges = _document(FILE)
    gap_nodes, gap_edges = _document(GAPPED, skip=frozenset({1}))
    own_nodes, own_edges = _document(
        OWNED, source=OWNED_SOURCE, cuts=OWNED_CUTS, own=OWN_TEXT,
    )
    publish_nodes(
        clean_runtime, deployment, subtypes=[DOC, CHUNK],
        narrowings=[
            ("document", "contains", "document_chunk"),
            ("document_chunk", "member_of", "document"),
        ],
        nodes=whole_nodes + gap_nodes + own_nodes + [node(SOLO, "document", SOLO_TEXT)],
        edges=whole_edges + gap_edges + own_edges,
    )
    return deployment


async def _json(client: Any, tool: str, arguments: dict, *, budget: int | None = None) -> dict:
    result = await client.call_tool(tool, arguments, meta=JSON_META)
    assert not result.isError, result_text(result)
    if budget is not None:
        assert len(result.model_dump_json(by_alias=True, exclude_none=True).encode()) <= budget
    return payload(result)


async def _text(client: Any, tool: str, arguments: dict, *, budget: int | None = None) -> str:
    result = await client.call_tool(tool, arguments)
    assert not result.isError, result_text(result)
    if budget is not None:
        assert len(result.model_dump_json(by_alias=True, exclude_none=True).encode()) <= budget
    return result_text(result)


def _target(node_id: str, lines: list | None = None) -> dict:
    target: dict[str, Any] = {"kind": "text", "node_id": node_id}
    if lines is not None:
        target["lines"] = lines
    return {"target": target}


def _joined(page: dict) -> str:
    return "".join(unfence(item["text"]) for item in page["items"])


def test_a_range_read_returns_the_lines_a_whole_read_has_at_those_positions(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        whole = await _json(client, "inspect", {**_target(FILE), "max_bytes": 131072})
        assert whole["complete"] and whole["coverage"] == "complete"
        assert _joined(whole) == SOURCE
        # A whole read says the lines it covers and that they are the file's.
        assert whole["lines"] == {"basis": "source", "total": LINE_COUNT, "first": 1,
                                  "last": LINE_COUNT}
        whole_lines = _joined(whole).split("\n")
        for first, last in [(1, 1), (30, 32), (61, 61), (MARKER_LINE, MARKER_LINE), (118, 120)]:
            expected = "".join(line + "\n" for line in whole_lines[first - 1:last])
            assert expected == _lines(first, last)
            page = await _json(client, "inspect", _target(FILE, [[first, last]]))
            assert _joined(page) == expected, (first, last)
            assert page["complete"] is True and "continuation_token" not in page
            assert page["lines"] == {
                "basis": "source", "total": LINE_COUNT,
                "requested": [[first, last]], "served": [[first, last]],
            }
            for item in page["items"]:
                assert first <= item["lines"][0] <= item["lines"][1] <= last, item
            text = await _text(client, "inspect", _target(FILE, [[first, last]]))
            assert _bodies(text) == expected, (first, last)
            head = text.split("\n", 1)[0]
            assert f"source lines {first}-{last} of {LINE_COUNT}" in head, head
            assert "! lines:" not in text
        # Line 31 crosses the first chunk boundary: it comes in two pieces
        # that both say line 31 and join to the line.
        page = await _json(client, "inspect", _target(FILE, [[31, 31]]))
        assert [item["lines"] for item in page["items"]] == [[31, 31], [31, 31]]
        assert [item["chunk_id"] for item in page["items"]] == [
            f"{FILE}:chunk:0", f"{FILE}:chunk:1",
        ]
        # Failure case: without `lines` the same call returns the whole text.
        assert len(_joined(whole)) > len(_joined(page))

    _drive(clean_runtime, deployment, body)


def test_several_ranges_come_back_in_order_and_a_range_past_the_end_says_so(
    clean_runtime: str, deployment: str,
) -> None:
    ranges = [[2, 4], [30, 45], [100, 130]]

    async def body(client: Any) -> None:
        page = await _json(client, "inspect", _target(FILE, ranges))
        assert _joined(page) == _lines(2, 4) + _lines(30, 45) + _lines(100, LINE_COUNT)
        assert page["lines"]["requested"] == ranges
        assert page["lines"]["served"] == [[2, 4], [30, 45], [100, LINE_COUNT]]
        assert page["lines"]["past_end"] is True and page["lines"]["total"] == LINE_COUNT
        firsts = [item["lines"][0] for item in page["items"]]
        assert firsts == sorted(firsts)
        text = await _text(client, "inspect", _target(FILE, ranges))
        assert _bodies(text) == _joined(page)
        head = text.split("\n", 1)[0]
        assert f"source lines 2-4, 30-45, 100-{LINE_COUNT} of {LINE_COUNT}" in head, head
        assert f"! lines: the text ends at line {LINE_COUNT}" in text
        # Wholly past the end: nothing, said plainly, not an error.
        beyond = await _json(client, "inspect", _target(FILE, [[500, 510]]))
        assert beyond["items"] == [] and beyond["complete"] is True
        assert beyond["lines"]["served"] == [] and beyond["lines"]["total"] == LINE_COUNT
        # An open end reads to the last line, and is not past the end.
        tail = await _json(client, "inspect", _target(FILE, [[118, None]]))
        assert _joined(tail) == _lines(118, LINE_COUNT)
        assert tail["lines"]["requested"] == [[118, None]]
        assert tail["lines"]["served"] == [[118, LINE_COUNT]] and "past_end" not in tail["lines"]
        # A record with no text: the line read says no lines exist.
        empty = await _json(client, "inspect", _target(SOLO, [[1, 3]]))
        assert empty["items"] == [] and empty["lines"]["total"] == 0
        assert empty["lines"]["served"] == [] and empty["lines"]["past_end"] is True

    _drive(clean_runtime, deployment, body)


# Overlapping, descending, line-zero and after-an-open-end values are read
# in the range form with a note (Format v10 B2; test_inspect_lines_forms.py).
@pytest.mark.parametrize("lines", [
    [[5, 2]], [[9, 12], [2, 1]], [], "1-5", [[1, 2, 3]],
    [[1, 10_000_001]], [[n, n] for n in range(1, 22)],
], ids=["reversed", "descending-reversed", "empty", "string", "triple",
        "over-the-bound", "twenty-one"])
def test_bad_ranges_and_lines_with_at_are_refused_with_the_accepted_form(
    clean_runtime: str, deployment: str, lines: Any,
) -> None:
    async def body(client: Any) -> None:
        result = await client.call_tool("inspect", _target(FILE, lines), meta=JSON_META)
        assert result.isError
        error = payload(result)["error"]
        assert "[[first, last], ...]" in error["message"], error
        assert "ascending" in error["message"] and "not overlapping" in error["message"]
        both = {"target": {"kind": "text", "node_id": FILE, "lines": [[1, 2]],
                           "at": {"chunk": f"{FILE}:chunk:0", "offset": 3}}}
        result = await client.call_tool("inspect", both, meta=JSON_META)
        assert result.isError
        assert "lines and at cannot be combined" in payload(result)["error"]["message"]
        # Failure case: the same record with a valid range is served.
        assert (await _json(client, "inspect", _target(FILE, [[1, 2]])))["items"]

    _drive(clean_runtime, deployment, body)


@pytest.mark.parametrize("json_format", [True, False], ids=["json", "text"])
def test_a_range_cut_by_max_bytes_is_continued_by_next_and_reassembles_exactly(
    clean_runtime: str, deployment: str, json_format: bool,
) -> None:
    budget = 4096
    first_call = {**_target(FILE, [[5, 60], [70, 112]]), "max_bytes": budget}
    expected = _lines(5, 60) + _lines(70, 112)

    async def body(client: Any) -> None:
        assembled, pages, arguments = "", 0, first_call
        cut: dict | None = None
        for _ in range(40):
            pages += 1
            if json_format:
                page = await _json(client, "inspect", arguments, budget=budget)
                assembled += _joined(page)
                block = page["lines"]
                if cut is not None:
                    # `next` went on from the line the cut page named.
                    assert block["served"][0][0] == cut["next_line"], (cut, block)
                token = page.get("continuation_token")
                cut = block.get("cut")
                assert (cut is None) == (token is None), block
                if cut is not None:
                    assert cut["last_line"] == block["served"][-1][1]
                    assert page["text_left"]["chars"] == len(expected) - len(assembled)
            else:
                text = await _text(client, "inspect", arguments, budget=budget)
                assembled += _bodies(text)
                token = fact_value(status_facts(text), "next:", last=True)
                said = re.search(
                    r"^! lines: served through line (\d+); next continues at line (\d+)$",
                    text, re.M,
                )
                assert (said is None) == (token is None), text.split("\n\n", 1)[0]
            if token is None:
                break
            arguments = {"next": token}
        else:
            raise AssertionError("the line read did not end within 40 pages")
        assert pages > 1, "the ranges must not fit one page for this test to mean anything"
        assert assembled == expected

    _drive(clean_runtime, deployment, body)


def test_texts_takes_ranges_per_item_and_shares_the_page_as_before(
    clean_runtime: str, deployment: str,
) -> None:
    items = [
        {"key": "head", "node_id": FILE, "lines": [[1, 3]]},
        {"key": "two", "node_id": FILE, "lines": [[50, 52], [119, 125]]},
        {"key": "gapped", "node_id": GAPPED, "lines": [[1, 2]]},
        {"key": "chunk", "node_id": f"{FILE}:chunk:2"},
    ]
    expected = {
        "head": _lines(1, 3),
        "two": _lines(50, 52) + _lines(119, LINE_COUNT),
        "gapped": _lines(1, 2),
        "chunk": SOURCE[CUTS[2]:CUTS[3]],
    }

    async def body(client: Any) -> None:
        tools = await client.list_tools()
        schema = next(tool.inputSchema for tool in tools.tools if tool.name == "inspect")
        call = {"target": {"kind": "texts", "items": items}, "max_bytes": 131072}
        # The published schema takes `lines` on a text target and on an
        # item, an open end included, and refuses a value that is no list.
        jsonschema.validate(call, schema)
        jsonschema.validate(_target(FILE, [[1, 3], [9, None]]), schema)
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(_target(FILE, "1-3"), schema)
        page = await _json(client, "inspect", call, budget=131072)
        by_key = {item["key"]: item for item in page["items"]}
        for key, text in expected.items():
            assert by_key[key]["state"] == "delivered", by_key[key]
            assert "".join(unfence(row["text"]) for row in by_key[key]["segments"]) == text, key
        assert by_key["head"]["lines"] == {
            "basis": "source", "total": LINE_COUNT, "requested": [[1, 3]],
        }
        assert by_key["two"]["lines"]["past_end"] is True
        assert by_key["gapped"]["lines"]["basis"] == "shown_text"
        assert by_key["gapped"]["source_coverage"] == "partial"
        assert by_key["head"]["requested_extent"]["kind"] == "lines"
        assert [row["lines"] for row in by_key["two"]["segments"]] == [[50, 52], [119, 120]]
        # An item with no `lines` is read as before and carries no line block.
        assert "lines" not in by_key["chunk"]
        assert all("lines" not in row for row in by_key["chunk"]["segments"])
        text = await _text(client, "inspect", call)
        assert "lines: source lines, 120 in all" in text
        assert "! lines: of the shown text, source has gaps" in text
        assert "! lines: the text ends at line 120" in text

        # A small page shares its bytes among the items; `next` finishes them.
        wide = [
            {"key": "a", "node_id": FILE, "lines": [[1, 55]]},
            {"key": "b", "node_id": FILE, "lines": [[60, 118]]},
        ]
        # At the smallest page (4096) most bytes go to the frame: the stamps
        # repeat the deployment's name, here a long temporary path, and each
        # item carries its ranges in the token. So this reads at 8192, and
        # what it asserts is progress: every page serves more text.
        assembled = {"a": "", "b": ""}
        arguments: dict = {"target": {"kind": "texts", "items": wide}, "max_bytes": 8192}
        pages = 0
        while True:
            pages += 1
            page = await _json(client, "inspect", arguments, budget=8192)
            before = sum(len(text) for text in assembled.values())
            for item in page["items"]:
                assembled[item["key"]] += "".join(unfence(row["text"]) for row in item["segments"])
            served = sum(len(text) for text in assembled.values()) - before
            assert served > 0, f"page {pages} served no text"
            token = page.get("continuation_token")
            if token is None:
                break
            arguments = {"next": token}
        assert pages > 1, "the batch must page for this test to mean anything"
        assert assembled == {"a": _lines(1, 55), "b": _lines(60, 118)}

    _drive(clean_runtime, deployment, body)


def test_a_record_with_a_gap_says_its_lines_are_of_the_shown_text(
    clean_runtime: str, deployment: str,
) -> None:
    shown = SOURCE[CUTS[0]:CUTS[1]] + "\n" + SOURCE[CUTS[2]:]
    shown_lines = shown.split("\n")[:-1]

    async def body(client: Any) -> None:
        whole = await _json(client, "inspect", {**_target(GAPPED), "max_bytes": 131072})
        assert whole["coverage"] == "partial"
        assert whole["lines"]["basis"] == "shown_text"
        assert whole["lines"]["total"] == len(shown_lines) < LINE_COUNT
        page = await _json(client, "inspect", _target(GAPPED, [[30, 33]]))
        assert page["lines"]["basis"] == "shown_text"
        # Lines 30 to 33 of the text this read shows: the end of the first
        # chunk (its last line cut short by the gap), then the chunk after
        # the gap, which starts a line of its own.
        assert [item["lines"] for item in page["items"]] == [[30, 31], [32, 33]]
        assert [unfence(item["text"]) for item in page["items"]] == [
            shown_lines[29] + "\n" + shown_lines[30],
            shown_lines[31] + "\n" + shown_lines[32] + "\n",
        ]
        text = await _text(client, "inspect", _target(GAPPED, [[30, 33]]))
        assert "shown-text lines 30-33 of " in text.split("\n", 1)[0]
        assert "! lines: of the shown text, source has gaps" in text
        # Failure case: the complete record says source lines and no notice.
        full = await _text(client, "inspect", _target(FILE, [[30, 33]]))
        assert "source lines 30-33 of " in full.split("\n", 1)[0]
        assert "! lines:" not in full

    _drive(clean_runtime, deployment, body)


def test_a_grep_match_in_a_passage_reports_the_line_a_range_read_returns(
    clean_runtime: str, deployment: str,
) -> None:
    """The match is in the third passage of each document. The list row's
    line, passed back as `lines`, returns the line that holds the match."""
    in_passage = SOURCE[CUTS[2]:CUTS[3]]
    passage_line = in_passage.count("\n", 0, in_passage.index(MARKER)) + 1

    async def body(client: Any) -> None:
        listed = await _json(client, "search", {
            "query": MARKER, "method": "grep", "view": "list",
        })
        rows = {row["node_id"]: row for row in listed["results"]}
        assert set(rows) == {FILE, GAPPED}, listed["results"]
        cards = await _json(client, "search", {"query": MARKER, "method": "grep"})
        windows = {row["node_id"]: row["match_windows"] for row in cards["results"]}
        for doc_id, row in rows.items():
            first = row["first_match"]
            assert first["chunk"] == f"{doc_id}:chunk:2", first
            assert "line_basis" not in first, first
            line = first["line"]
            page = await _json(client, "inspect", _target(doc_id, [[line, line]]))
            assert MARKER in _joined(page), (doc_id, line, _joined(page))
            assert page["lines"]["served"] == [[line, line]]
            # The cards' window says the same line.
            (window,) = windows[doc_id]
            assert window["passage_id"] == first["chunk"] and window["line"] == line
            assert "line_basis" not in window
        # In the complete document that line is the file's own line number.
        assert rows[FILE]["first_match"]["line"] == MARKER_LINE
        assert _joined(await _json(
            client, "inspect", _target(FILE, [[MARKER_LINE, MARKER_LINE]]),
        )) == SOURCE_LINES[MARKER_LINE - 1]
        # Failure case: the line counted inside the passage is another
        # number, and reading it returns a line without the match.
        assert passage_line not in (MARKER_LINE, rows[GAPPED]["first_match"]["line"])
        wrong = await _json(client, "inspect", _target(FILE, [[passage_line, passage_line]]))
        assert MARKER not in _joined(wrong)
        text = await _text(client, "search", {"query": MARKER, "method": "grep", "view": "list"})
        assert f"first {FILE}:chunk:2 line {MARKER_LINE} · " in text, text
        assert "line counted in the passage" not in text

    _drive(clean_runtime, deployment, body)


def test_a_match_in_a_records_own_text_reports_a_line_that_reads_back_or_says_whose(
    clean_runtime: str, deployment: str,
) -> None:
    """`doc:owned` matches on line 3 of its own text, and its chunks hold
    that same line as line 5. The reported line is 5, the one a range read
    returns. `doc:solo` has own text only, which `inspect` text does not
    read: its line says `own_text`."""

    async def body(client: Any) -> None:
        listed = await _json(client, "search", {
            "query": "ownneedle", "method": "grep", "view": "list",
        })
        (row,) = listed["results"]
        first = row["first_match"]
        assert row["node_id"] == OWNED and first["chunk"] == OWNED, row
        assert OWN_TEXT.split("\n").index(OWN_LINE) + 1 == 3
        assert first["line"] == 5 and "line_basis" not in first, first
        page = await _json(client, "inspect", _target(OWNED, [[first["line"], first["line"]]]))
        assert _joined(page) == OWN_LINE + "\n"
        # Failure case: the own text's line 3 is another line of the record.
        assert _joined(await _json(client, "inspect", _target(OWNED, [[3, 3]]))) == "intro three\n"
        cards = await _json(client, "search", {"query": "ownneedle", "method": "grep"})
        own, passage = cards["results"][0]["match_windows"]
        assert (own["line"], passage["line"]) == (5, 5)
        assert "line_basis" not in own and "line_basis" not in passage
        assert passage["passage_id"] == f"{OWNED}:chunk:1"

        solo = await _json(client, "search", {
            "query": "soloneedle", "method": "grep", "view": "list",
        })
        (row,) = solo["results"]
        assert row["first_match"]["line"] == 3
        assert row["first_match"]["line_basis"] == "own_text", row
        text = await _text(client, "search", {
            "query": "soloneedle", "method": "grep", "view": "list",
        })
        assert "first line 3 · " in text and "line counted in its own text" in text

    _drive(clean_runtime, deployment, body)


# --------------------------------------------------------------------------- #
# access rules
# --------------------------------------------------------------------------- #

OPEN = "open one\nopen two\nopen three\n"
AFTER = "after one\nafter two\nquasarneedle is on this line\nafter four"


def _seed_two(dsn: str) -> tuple[str, str]:
    """Two documents alike but for a hidden middle passage of one line in
    the first and forty in the second; ids of equal length."""
    from tests.substrate.mcp.test_search_location import _record, _seed

    sfx = uuid.uuid4().hex[:8]
    short, long = f"doc:{sfx}-a", f"doc:{sfx}-b"
    _seed(dsn, _record(short, [OPEN, "classified\n", AFTER], secret=frozenset({1}))
          + _record(long, [OPEN, "classified\n" * 40, AFTER], secret=frozenset({1})))
    return short, long


def _reads(doc_id: str) -> list[dict]:
    return [
        {**_target(doc_id), "max_bytes": 131072},
        _target(doc_id, [[1, 200]]),
        _target(doc_id, [[4, 6]]),
        {"target": {"kind": "texts", "items": [
            {"key": "k", "node_id": doc_id, "lines": [[2, 5]]},
        ]}, "max_bytes": 131072},
    ]


def test_a_reader_gets_line_numbers_of_the_text_it_is_served(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Under enforce the middle passage is hidden. Every line number the
    reader gets is counted over the two passages it is served, so the two
    documents answer alike, byte for byte, whether one line or forty are
    hidden."""
    short, long = _seed_two(clean_runtime)
    visible_lines = (OPEN + AFTER).split("\n")

    async def enforced(client: Any) -> None:
        for short_call, long_call in zip(_reads(short), _reads(long)):
            for meta in (None, JSON_META):
                short_result = await client.call_tool("inspect", short_call, meta=meta)
                long_result = await client.call_tool("inspect", long_call, meta=meta)
                assert not short_result.isError, result_text(short_result)
                short_bytes = result_text(short_result).replace(short, long)
                long_bytes = result_text(long_result)
                # #3666: the generation prints on a session's first reply
                # only (call order, not content): left out of both.
                generation = re.compile(r" \u00b7 generation [A-Za-z0-9:._/\-]+")
                short_bytes = generation.sub("", short_bytes)
                long_bytes = generation.sub("", long_bytes)
                # The publish age is wall-clock time between the two calls.
                age = re.compile(r'(publish_age_seconds\\*":)-?[0-9.]+')
                assert age.sub(r"\1<age>", short_bytes) == age.sub(r"\1<age>", long_bytes), (
                    short_call, meta,
                )
                assert "classified" not in long_bytes
        whole = await _json(client, "inspect", {**_target(long), "max_bytes": 131072})
        assert whole["lines"]["total"] == len(visible_lines) == 7
        assert whole["lines"]["basis"] == "shown_text"
        assert _joined(whole) == OPEN + AFTER
        # Lines 4 to 6 are the first three lines of the passage after the
        # hidden one: no line number was spent on hidden text.
        page = await _json(client, "inspect", _target(long, [[4, 6]]))
        assert _joined(page) == "".join(line + "\n" for line in visible_lines[3:6])
        assert [item["chunk_id"] for item in page["items"]] == [f"chunk:{long}:02"]

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    _drive(clean_runtime, "default", enforced)

    # Failure case: with enforcement off the hidden passage is served and
    # numbered, so the two documents differ by its thirty-nine extra lines.
    async def unenforced(client: Any) -> None:
        totals = {}
        for doc_id in (short, long):
            whole = await _json(client, "inspect", {**_target(doc_id), "max_bytes": 131072})
            assert "classified" in _joined(whole)
            totals[doc_id] = whole["lines"]["total"]
        assert totals == {short: 8, long: 47}

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    _drive(clean_runtime, "default", unenforced)


def test_a_match_line_under_access_rules_is_a_line_of_the_served_text(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The match sits after the hidden passage. Under access rules a grep
    page prepares no whole record, so the line is the passage's own and
    says so; it is the same in both documents, and reading that line of the
    passage returns the match."""
    short, long = _seed_two(clean_runtime)

    async def lines_of(client: Any, basis: str | None) -> dict[str, int]:
        listed = await _json(client, "search", {
            "query": "quasarneedle", "method": "grep", "view": "list",
        })
        found = {row["node_id"]: row["first_match"] for row in listed["results"]}
        assert set(found) == {short, long}, listed["results"]
        for doc_id, first in found.items():
            assert first["chunk"] == f"chunk:{doc_id}:02", first
            assert first.get("line_basis") == basis, first
            # The line reads back from what it is a line of.
            read_from = first["chunk"] if basis == "passage" else doc_id
            page = await _json(
                client, "inspect", _target(read_from, [[first["line"], first["line"]]]),
            )
            assert _joined(page) == "quasarneedle is on this line\n", (doc_id, first)
        assert "classified" not in json.dumps(listed)
        return {doc_id: first["line"] for doc_id, first in found.items()}

    seen: dict[str, dict[str, int]] = {}

    async def enforced(client: Any) -> None:
        seen["enforce"] = await lines_of(client, "passage")

    async def unenforced(client: Any) -> None:
        seen["off"] = await lines_of(client, None)

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    _drive(clean_runtime, "default", enforced)
    assert seen["enforce"] == {short: 3, long: 3}
    # Failure case: with the rules off the line is the record's, and the
    # passage that was hidden counts in it.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    _drive(clean_runtime, "default", unenforced)
    assert seen["off"] == {short: 7, long: 46}


# --------------------------------------------------------------------------- #
# text matching as a condition (step M7)                                       #
# --------------------------------------------------------------------------- #

KEY_RE = "re:/CMS[A-Z]+-[0-9]+/"
LABEL_RE = "re:/CMSSW_[0-9]+_[0-9]+_[0-9]+/"
TICKETS = {
    "ticket:T-1": ("Tier-0 replay pinned CMSSW_14_0_7 and CMSSW_14_0_8", "2023-11-02"),
    "ticket:T-2": ("Express fails on CMSSW_15_0_1", "2024-03-01"),
    "ticket:T-3": ("Prompt reco moved to CMSSW_15_0_1\nfrom CMSSW_14_0_7", "2024-05-10"),
    "ticket:T-4": ("No release named here", "2024-06-01"),
}
COMMITS = {
    "commit:c1": "Fix rule evaluation\n\nRefs CMSTRANSF-1177",
    "commit:c2": "Refs CMSDM-42 and CMSTRANSF-1177",
    "commit:c3": "judge the rule\nPlain refactor of the judge",
    "commit:c4": "CMSTRANSF-1180 follow up",
}
COMMIT_DATES = {
    "commit:c1": "2024-06-01", "commit:c2": "2025-02-01",
    "commit:c3": "2025-03-01", "commit:c4": "2025-04-01",
}
#: Each record type declares its creation date, so `date.created` reads it.
DATED = {"searchable_text_fields": ["text"], "created_at_from": "created"}
MEETINGS = {
    "doc:mtg1": ["Agenda\nCMSTRANSF-1177 status", "Also CMSTRANSF-1180 and CMSTRANSF-1177"],
    "doc:mtg2": ["CMSTRANSF-1177 closed"],
    "doc:mtg3": ["Nothing to report", "CMSDM-42 discussed"],
    # Two words in two passages: the record holds both, no passage does.
    "doc:split": ["the reaper restarted", "after the pods drained", "xa x?"],
}


@pytest.fixture
def matches_deployment(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path,
) -> str:
    """Tickets and commits with their text as their own, and meeting pages
    whose text is in declared passages, as on the structural benchmark."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes = [
        node(ticket, "ticket", text, created=created)
        for ticket, (text, created) in TICKETS.items()
    ] + [
        node(commit, "commit", text, created=COMMIT_DATES[commit])
        for commit, text in COMMITS.items()
    ] + [node("board:hub", "board", "the tier-0 board")]
    edges = [
        EdgeFact(src="board:hub", dst=ticket, edge_type="tracks",
                 source_record_id={"id": f"board:hub|tracks|{ticket}"},
                 source_revision={"v": 1})
        for ticket in TICKETS
    ]
    for doc_id, chunks in MEETINGS.items():
        nodes.append(node(doc_id, "document", "meeting page", title=doc_id))
        for i, text in enumerate(chunks):
            chunk_id = f"{doc_id}:chunk:{i}"
            nodes.append(node(chunk_id, "document_chunk", text, chunk_index=i))
            edges.append(EdgeFact(
                src=doc_id, dst=chunk_id, edge_type="contains",
                source_record_id={"id": f"{chunk_id}-c"}, source_revision={"v": 1},
            ))
    publish_nodes(
        clean_runtime, deployment,
        subtypes=[DOC, CHUNK, SubtypeSpec("ticket", metadata=DATED),
                  SubtypeSpec("commit", metadata=DATED), "board"],
        narrowings=[("document", "contains", "document_chunk"),
                    ("board", "tracks", "ticket")],
        nodes=nodes, edges=edges,
    )
    return deployment


async def _refused(client: Any, tool: str, arguments: dict) -> dict:
    result = await client.call_tool(tool, arguments, meta=JSON_META)
    assert result.isError, result_text(result)
    return payload(result)["error"]


def _where(**extra: Any) -> dict:
    return {"text": {"op": "matches", "value": KEY_RE}, **extra}


def test_ticket_keys_across_the_pages_of_a_set_are_counted_per_key(
    clean_runtime: str, matches_deployment: str,
) -> None:
    async def body(client: Any) -> None:
        # Every meeting page whose passages hold a key, a page at a time.
        seen: dict[str, tuple[str, int]] = {}
        offset = 0
        while True:
            page = await _json(client, "filter", {
                "where": _where(type="document"), "limit": 1, "offset": offset,
            })
            if offset == 0:
                assert page["total_count"] == 3 and page["total_count_basis"] == "exact"
            for row in page["items"]:
                seen[row["node_id"]] = (unfence(row["match"]), row["match_count"])
            if page["next_offset"] is None:
                break
            offset = page["next_offset"]
        assert seen == {
            "doc:mtg1": ("CMSTRANSF-1177", 3),
            "doc:mtg2": ("CMSTRANSF-1177", 1),
            "doc:mtg3": ("CMSDM-42", 1),
        }
        # Each key counted over every page: records holding it, and matches.
        counted = await _json(client, "aggregate", {
            "where": _where(type="document"), "by": ["match"], "metrics": ["count"],
            "limit": 2,
        })
        rest = await _json(client, "aggregate", {
            "where": _where(type="document"), "by": ["match"], "metrics": ["count"],
            "limit": 2, "offset": 2,
        })
        groups = [
            (unfence(g["match"]), g["count"], g["occurrences"])
            for g in counted["groups"] + rest["groups"]
        ]
        assert groups == [
            ("CMSTRANSF-1177", 2, 3), ("CMSDM-42", 1, 1), ("CMSTRANSF-1180", 1, 1),
        ]
        assert counted["total_count"] == 3 and counted["total_count_basis"] == "exact"
        assert counted["next_offset"] == 2 and rest["next_offset"] is None
        assert counted["text_match"]["records_counted"] == 3
        text = await _text(client, "aggregate", {
            "where": _where(type="document"), "by": ["match"], "metrics": ["count"],
        })
        assert "! text condition: 3 records' text read; every match in 3 records counted" in text
        # Failure case: `match` without a text condition is refused by name.
        error = await _refused(client, "aggregate", {
            "where": {"type": "document"}, "by": ["match"], "metrics": ["count"],
        })
        assert error["code"] == "invalid_operator_arguments"
        assert error["data"]["reason"] == "match_without_text"

    _drive(clean_runtime, matches_deployment, body)


def test_commits_whose_message_holds_a_key_come_back_with_the_match(
    clean_runtime: str, matches_deployment: str,
) -> None:
    select = ["node_id", "match", "match_count", "matches"]

    async def body(client: Any) -> None:
        page = await _json(client, "filter", {"where": _where(type="commit"), "select": select})
        rows = {row["node_id"]: row for row in page["items"]}
        assert set(rows) == {"commit:c1", "commit:c2", "commit:c4"}
        assert page["total_count"] == 3 and page["total_count_basis"] == "exact"
        assert unfence(rows["commit:c2"]["match"]) == "CMSDM-42"
        assert rows["commit:c2"]["match_count"] == 2
        assert [(m["line"], unfence(m["text"]), m["line_basis"])
                for m in rows["commit:c2"]["matches"]] == [
            (1, "CMSDM-42", "own_text"), (1, "CMSTRANSF-1177", "own_text"),
        ]
        assert [(m["line"], unfence(m["text"])) for m in rows["commit:c1"]["matches"]] == [
            (3, "CMSTRANSF-1177"),
        ]
        assert page["text_match"] == {
            "value": KEY_RE, "matches_cap": 10, "records_scanned": 3, "scan": "index",
        }
        # map reads the same set, and the default row carries the match.
        mapped = await _json(client, "map", {"where": _where(type="commit")})
        assert {r["node_id"]: unfence(r["match"]) for r in mapped["items"]} == {
            "commit:c1": "CMSTRANSF-1177", "commit:c2": "CMSDM-42",
            "commit:c4": "CMSTRANSF-1180",
        }
        text = await _text(client, "filter", {"where": _where(type="commit"), "select": select})
        assert "! text condition: 3 records' text read" in text
        assert "matches lists the first 10 per record, match_count all" in text
        # Failure case: a commit with no key is never on the page, and an
        # excluded word removes the commit that holds it.
        narrowed = await _json(client, "filter", {
            "where": {"type": "commit",
                      "text": {"op": "matches", "value": f"{KEY_RE} -follow"}},
        })
        assert {r["node_id"] for r in narrowed["items"]} == {"commit:c1", "commit:c2"}

    _drive(clean_runtime, matches_deployment, body)


def test_release_labels_in_tickets_since_a_date_are_counted(
    clean_runtime: str, matches_deployment: str,
) -> None:
    def where(**extra: Any) -> dict:
        return {"type": "ticket", "text": {"op": "matches", "value": LABEL_RE}, **extra}

    async def body(client: Any) -> None:
        since = {"attrs.created": {"op": "gt", "value": "2024-01-01"}}
        counted = await _json(client, "aggregate", {
            "where": where(**since), "by": ["match"], "metrics": ["count"],
        })
        assert [(unfence(g["match"]), g["count"]) for g in counted["groups"]] == [
            ("CMSSW_15_0_1", 2), ("CMSSW_14_0_7", 1),
        ]
        assert counted["total_count"] == 2 and counted["total_count_basis"] == "exact"
        # The tickets themselves, through map.
        mapped = await _json(client, "map", {
            "where": where(**since), "select": ["node_id", "match"],
        })
        assert {r["node_id"] for r in mapped["items"]} == {"ticket:T-2", "ticket:T-3"}
        # Grouping the matching tickets by another field counts records.
        by_date = await _json(client, "aggregate", {
            "where": where(), "by": ["attrs.created"], "metrics": ["count"],
        })
        assert sum(g["count"] for g in by_date["groups"]) == 3
        # Failure case: without the date the 2023 ticket's labels count too.
        every = await _json(client, "aggregate", {
            "where": where(), "by": ["match"], "metrics": ["count"],
        })
        assert [(unfence(g["match"]), g["count"]) for g in every["groups"]] == [
            ("CMSSW_14_0_7", 2), ("CMSSW_15_0_1", 2), ("CMSSW_14_0_8", 1),
        ]

    _drive(clean_runtime, matches_deployment, body)


def test_a_regex_with_no_literal_run_is_refused_past_the_scan_limit_with_its_retry(
    clean_runtime: str, matches_deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.mcp import text_match

    monkeypatch.setattr(text_match, "SCAN_LIMIT", 4)
    loose = {"text": {"op": "matches", "value": "re:/[A-Z]+-[0-9]+/"}}

    async def body(client: Any) -> None:
        error = await _refused(client, "filter", {"where": loose})
        assert error["code"] == "text_match_scan_limit"
        assert "no run of 3 letters or digits" in error["message"]
        assert "holds more than 4 records" in error["message"]
        fitting = error["data"]["types_within_limit"]
        assert {"ticket", "commit", "document"} <= set(fitting)
        assert "document_chunk" not in fitting, "five chunks are over a limit of four"
        retry = error["retry_call"]
        assert retry["tool"] == "filter" and retry["mode"] == "guess"
        assert retry["arguments"]["input"]["subtype"] in fitting
        assert retry["arguments"]["predicate"] == {
            "op": "matches", "field": "text", "value": "re:/[A-Z]+-[0-9]+/",
        }
        # The offered call is served as written, exactly.
        served = await _json(client, "filter", retry["arguments"])
        assert served["total_count_basis"] == "exact"
        assert served["text_match"]["scan"] == "set"
        # Narrowed to commits, the whole set is checked: four records read.
        commits = await _json(client, "filter", {"where": {"type": "commit", **loose}})
        assert {r["node_id"] for r in commits["items"]} == {
            "commit:c1", "commit:c2", "commit:c4",
        }
        assert commits["text_match"]["records_scanned"] == 4
        # An indexable value over the limit with nothing to narrow it: the
        # number named, nothing served from part of the set.
        over = await _refused(client, "aggregate", {
            "where": {"text": {"op": "matches", "value": "cms"}},
            "by": ["subtype"], "metrics": ["count"],
        })
        assert over["code"] == "text_match_scan_limit"
        assert "more than 4 records hold the value's indexed term" in over["message"]

    _drive(clean_runtime, matches_deployment, body)

    # Failure case: at the real limit the same small graph is checked whole.
    monkeypatch.setattr(text_match, "SCAN_LIMIT", 10_000)

    async def whole(client: Any) -> None:
        page = await _json(client, "filter", {"where": loose, "limit": 100})
        assert page["total_count_basis"] == "exact"
        assert "commit:c3" not in {r["node_id"] for r in page["items"]}

    _drive(clean_runtime, matches_deployment, whole)


def test_expand_keeps_the_links_whose_far_end_text_matches(
    clean_runtime: str, matches_deployment: str,
) -> None:
    call = {"input": {"node_id": "doc:mtg1"}, "edge_type": "contains", "direction": "out"}

    async def body(client: Any) -> None:
        # A one-hop row is the linked record (#3666 expand records).
        every = await _json(client, "expand", call)
        assert {row["node_id"] for row in every["neighbors"]} == {
            "doc:mtg1:chunk:0", "doc:mtg1:chunk:1",
        }
        kept = await _json(client, "expand", {
            **call, "where": {"text": {"op": "matches", "value": "CMSTRANSF-1180"}},
        })
        assert [row["node_id"] for row in kept["neighbors"]] == ["doc:mtg1:chunk:1"]
        assert kept["total_count"] == 1 and kept["total_count_basis"] == "exact"
        assert kept["text_match"]["records_scanned"] == 2
        # The link-row shape (a select of link fields) keeps the same links.
        links = await _json(client, "expand", {
            **call, "where": {"text": {"op": "matches", "value": "CMSTRANSF-1180"}},
            "select": ["edge_id", "src", "dst", "edge_type", "direction"],
        })
        assert [row["dst"] for row in links["neighbors"]] == ["doc:mtg1:chunk:1"]
        # Failure case: a value no far end holds keeps no record, counted 0.
        none = await _json(client, "expand", {
            **call, "where": {"text": {"op": "matches", "value": "CMSDM-42"}},
        })
        assert none["neighbors"] == []
        assert none["total_count"] == 0

    _drive(clean_runtime, matches_deployment, body)


def test_expand_record_rows_carry_the_match_fields_as_filter_rows_do(
    clean_runtime: str, matches_deployment: str,
) -> None:
    """#3666: a one-hop expand's record rows get `match` and `match_count`
    by default, and with a `select` only the match fields it names, read
    for each page's own records."""
    call = {
        "input": {"node_id": "doc:mtg1"}, "edge_type": "contains", "direction": "out",
        "where": {"text": {"op": "matches", "value": KEY_RE}},
    }

    async def body(client: Any) -> None:
        every = await _json(client, "expand", call)
        rows = {row["node_id"]: row for row in every["neighbors"]}
        assert set(rows) == {"doc:mtg1:chunk:0", "doc:mtg1:chunk:1"}
        assert every["total_count"] == 2
        assert {k: (unfence(r["match"]), r["match_count"]) for k, r in rows.items()} == {
            "doc:mtg1:chunk:0": ("CMSTRANSF-1177", 1),
            "doc:mtg1:chunk:1": ("CMSTRANSF-1180", 2),
        }
        # `matches` only when named.
        assert all("matches" not in row for row in every["neighbors"])

        only = await _json(client, "expand", {**call, "select": ["match"]})
        for row in only["neighbors"]:
            assert "match" in row and "match_count" not in row and "matches" not in row
        assert {r["node_id"]: unfence(r["match"]) for r in only["neighbors"]} == {
            "doc:mtg1:chunk:0": "CMSTRANSF-1177", "doc:mtg1:chunk:1": "CMSTRANSF-1180",
        }

        lines = await _json(client, "expand", {
            **call, "select": ["node_id", "matches", "match_count"],
        })
        listed = {r["node_id"]: r for r in lines["neighbors"]}
        assert [(m["line"], unfence(m["text"]))
                for m in listed["doc:mtg1:chunk:1"]["matches"]] == [
            (1, "CMSTRANSF-1180"), (1, "CMSTRANSF-1177"),
        ]
        assert [(m["line"], unfence(m["text"]))
                for m in listed["doc:mtg1:chunk:0"]["matches"]] == [(2, "CMSTRANSF-1177")]
        assert all("match" not in row for row in lines["neighbors"])

        # Paged one at a time: each page states its own record's match.
        seen = {}
        for offset in (0, 1):
            page = await _json(client, "expand", {**call, "limit": 1, "offset": offset})
            assert page["total_count"] == 2 and len(page["neighbors"]) == 1
            (row,) = page["neighbors"]
            seen[row["node_id"]] = (unfence(row["match"]), row["match_count"])
        assert seen == {k: (unfence(r["match"]), r["match_count"]) for k, r in rows.items()}

        # Failure case: without a text condition the rows carry no match
        # field, and a select of one is refused.
        plain = await _json(client, "expand", {k: v for k, v in call.items() if k != "where"})
        assert all("match" not in row for row in plain["neighbors"])
        refused = await client.call_tool(
            "expand",
            {**{k: v for k, v in call.items() if k != "where"}, "select": ["match"]},
            meta=JSON_META,
        )
        assert refused.isError, result_text(refused)

    _drive(clean_runtime, matches_deployment, body)


def test_expand_text_scan_is_narrowed_by_the_far_type_and_the_hint_names_it(
    clean_runtime: str, matches_deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#3666: the far records a text clause is checked within are those of
    the type asked (target_subtype or a type in where); over the limit,
    the hint names only what the caller can still narrow."""
    from okg.substrate.mcp import text_match

    monkeypatch.setattr(text_match, "SCAN_LIMIT", 1)
    call = {
        "input": {"node_id": "doc:mtg1"}, "edge_type": "contains", "direction": "out",
        "where": {"text": {"op": "matches", "value": KEY_RE}},
    }

    async def body(client: Any) -> None:
        # Two linked chunks, limit 1, no type: refused; edge_type is named,
        # so the hint offers the type only.
        over = await _refused(client, "expand", call)
        assert over["code"] == "text_match_scan_limit"
        assert "target_subtype" in over["fix_hint"] and "edge_type" not in over["fix_hint"]
        # A type no far record has: nothing to check, served empty.
        none = await _json(client, "expand", {**call, "target_subtype": "document"})
        assert none["neighbors"] == [] and none["total_count"] == 0
        assert none["text_match"]["records_scanned"] == 0
        # Failure case: the chunks' own type still leaves two; the hint now
        # sends the caller to filter, naming nothing already named.
        typed = await _refused(client, "expand", {**call, "target_subtype": "document_chunk"})
        assert typed["code"] == "text_match_scan_limit"
        assert "filter" in typed["fix_hint"] and "target_subtype" not in typed["fix_hint"]

        # Link rows read the same type: the hint, then target_subtype, act
        # as on record rows (no hint that loops back to itself).
        links = {**call, "select": ["edge_id", "dst"]}
        over = await _refused(client, "expand", links)
        assert over["code"] == "text_match_scan_limit"
        assert "target_subtype" in over["fix_hint"]
        none = await _json(client, "expand", {**links, "target_subtype": "document"})
        assert none["neighbors"] == []
        assert none["text_match"]["records_scanned"] == 0
        typed = await _refused(client, "expand", {**links, "target_subtype": "document_chunk"})
        assert "target_subtype" not in typed["fix_hint"]

    _drive(clean_runtime, matches_deployment, body)


def test_the_failing_text_shapes_from_the_agent_runs_are_served_with_a_note(
    clean_runtime: str, matches_deployment: str,
) -> None:
    """#3666: shapes real agents sent. The operator written as a key of
    `text`, and a regex without its slashes, each mean one thing: served
    with a note, the same records as the documented form, never a silent
    zero. Another field's operator written as a key is refused with the
    accepted form as a retry_call."""
    exact = {"type": "commit", "text": {"op": "matches", "value": KEY_RE}}
    unslashed = KEY_RE[len("re:/"):-1]

    async def body(client: Any) -> None:
        documented = await _json(client, "filter", {"where": exact})
        want = {r["node_id"] for r in documented["items"]}
        assert want == {"commit:c1", "commit:c2", "commit:c4"}
        for where, rule in (
            ({"type": "commit", "text": {"matches": KEY_RE}}, "text_as_matches"),
            ({"type": "commit", "text": {"op": "matches", "value": f"re:{unslashed}"}},
             "regex_without_slashes"),
            ({"type": "commit", "text": {"matches": f"re:{unslashed}"}},
             "regex_without_slashes"),
        ):
            served = await _json(client, "filter", {"where": where})
            assert {r["node_id"] for r in served["items"]} == want, where
            assert rule in [note["rule"] for note in served["adjusted"]], where
        expanded = await _json(client, "expand", {
            "input": {"node_id": "doc:mtg1"}, "edge_type": "contains", "direction": "out",
            "where": {"text": {"matches": "re:CMSTRANSF-118[0-9]"}},
        })
        assert [r["node_id"] for r in expanded["neighbors"]] == ["doc:mtg1:chunk:1"]
        assert "regex_without_slashes" in [n["rule"] for n in expanded["adjusted"]]
        # Failure case: an operator written as a key on another field is
        # refused, naming the accepted form, with the call to send.
        result = await client.call_tool(
            "filter", {"where": {"type": "commit", "attrs.created": {"prefix": "2025"}}},
            meta=JSON_META,
        )
        assert result.isError, result_text(result)
        error = payload(result)["error"]
        assert '"op": "prefix"' in error["fix_hint"]
        retry = error["retry_call"]
        again = await client.call_tool(retry["tool"], retry["arguments"], meta=JSON_META)
        assert not again.isError, result_text(again)

    _drive(clean_runtime, matches_deployment, body)


def test_an_unslashed_regex_beside_a_phrase_or_a_slashed_term_and_in_a_grep_query(
    clean_runtime: str, matches_deployment: str,
) -> None:
    """#3666: an unslashed regex is found wherever it stands in the value:
    beside a quoted phrase or a slashed regex it is served as the regex,
    with a note in the reply, as in a grep search query. A regex missing
    only its opening slash is refused with the call to send."""
    unslashed = KEY_RE[len("re:/"):-1]

    async def body(client: Any) -> None:
        for sent, documented, want in (
            (f'"Refs" re:{unslashed}', f'"Refs" {KEY_RE}', {"commit:c1", "commit:c2"}),
            ("re:/Refs/ re:CMSDM-[0-9]+", "re:/Refs/ re:/CMSDM-[0-9]+/", {"commit:c2"}),
        ):
            exact = await _json(client, "filter", {
                "where": {"type": "commit", "text": {"op": "matches", "value": documented}},
            })
            assert {r["node_id"] for r in exact["items"]} == want, documented
            served = await _json(client, "filter", {
                "where": {"type": "commit", "text": {"op": "matches", "value": sent}},
            })
            assert {r["node_id"] for r in served["items"]} == want, sent
            assert "regex_without_slashes" in [n["rule"] for n in served["adjusted"]], sent
        # A grep search query: the note reaches the search reply and the rows
        # are the slashed query's.
        query = "re:CMSTRANSF-118[0-9]"
        slashed = await _json(client, "search", {
            "query": "re:/CMSTRANSF-118[0-9]/", "method": "grep", "view": "list",
        })
        searched = await _json(client, "search", {"query": query, "method": "grep", "view": "list"})
        want_rows = {row["node_id"] for row in slashed["results"]}
        assert want_rows and {row["node_id"] for row in searched["results"]} == want_rows
        assert "regex_without_slashes" in [n["rule"] for n in searched["adjusted"]]
        assert "re:/CMSTRANSF-118[0-9]/" in json.dumps(searched["adjusted"])
        # Failure case: a regex missing its opening slash is refused, never
        # read as a literal word. A grep search gets the call to send, served
        # as written; so does a text condition, where access rules are known
        # off, and that call is served.
        result = await client.call_tool(
            "search", {"query": "re:CMSTRANSF-118[0-9]/i", "method": "grep"}, meta=JSON_META,
        )
        assert result.isError, result_text(result)
        error = payload(result)["error"]
        assert "missing its opening slash" in error["message"], error
        retry = error["retry_call"]
        assert retry["arguments"]["query"] == "re:/CMSTRANSF-118[0-9]/"
        again = await client.call_tool(retry["tool"], retry["arguments"], meta=JSON_META)
        assert not again.isError, result_text(again)
        assert payload(again)["results"] and not payload(again).get("adjusted")
        sent = {"type": "commit", "text": {"op": "matches", "value": f"re:{unslashed}/"}}
        error = await _refused(client, "filter", {"where": sent})
        assert "missing its opening slash" in error["message"], error
        # With access rules known off the server's own checks take the
        # slashed text call (loose-match-3666), so it is offered as written.
        retry = error["retry_call"]
        assert retry["arguments"]["where"]["text"] == {"op": "matches", "value": KEY_RE}, error
        fixed = await _json(client, retry["tool"], retry["arguments"])
        assert {r["node_id"] for r in fixed["items"]} == {"commit:c1", "commit:c2", "commit:c4"}

    _drive(clean_runtime, matches_deployment, body)


def test_link_rows_honour_target_subtype_beside_a_text_condition(
    clean_runtime: str, matches_deployment: str,
) -> None:
    """#3666: on link rows target_subtype is part of the far-end condition."""
    call = {
        "input": {"node_id": "doc:mtg1"}, "edge_type": "contains", "direction": "out",
        "where": {"text": {"op": "matches", "value": KEY_RE}},
        "select": ["edge_id", "dst"],
    }

    async def body(client: Any) -> None:
        kept = await _json(client, "expand", {**call, "target_subtype": "document_chunk"})
        assert {row["dst"] for row in kept["neighbors"]} == {
            "doc:mtg1:chunk:0", "doc:mtg1:chunk:1",
        }
        # Failure case: a type no far record has keeps no link.
        none = await _json(client, "expand", {**call, "target_subtype": "ticket"})
        assert none["neighbors"] == []

    _drive(clean_runtime, matches_deployment, body)


def test_link_rows_honour_target_subtype_beside_order_by_alone(
    clean_runtime: str, matches_deployment: str,
) -> None:
    """#3666: an ordered link-row call with target_subtype and no other
    condition keeps only links to that type."""
    call = {
        "input": {"node_id": "doc:mtg1"}, "edge_type": "contains", "direction": "out",
        "select": ["edge_id", "dst"], "order_by": "node_id",
    }

    async def body(client: Any) -> None:
        kept = await _json(client, "expand", {**call, "target_subtype": "document_chunk"})
        assert [row["dst"] for row in kept["neighbors"]] == [
            "doc:mtg1:chunk:0", "doc:mtg1:chunk:1",
        ]
        # Failure case: a type no far record has keeps no link (the
        # condition was dropped when order_by came with no other condition).
        none = await _json(client, "expand", {**call, "target_subtype": "ticket"})
        assert none["neighbors"] == [], none["neighbors"]

    _drive(clean_runtime, matches_deployment, body)


def test_a_text_condition_under_access_rules_is_refused_plainly(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A passage the reader may not read ("classified") is never matched,
    counted or grouped: under access rules every operator refuses the text
    condition before reading any text, and no reply names a record or the
    hidden text. With the rules off the same value matches both records."""
    short, long = _seed_two(clean_runtime)
    hidden = {"text": {"op": "matches", "value": "classified"}}
    calls = [
        ("filter", {"where": {"type": "document", **hidden}}),
        ("map", {"where": {"type": "document", **hidden}, "select": ["node_id", "match"]}),
        ("aggregate", {"where": {"type": "document", **hidden},
                       "by": ["match"], "metrics": ["count"]}),
        ("aggregate", {"where": {"type": "document", **hidden},
                       "by": ["subtype"], "metrics": ["count"]}),
        ("expand", {"input": {"node_id": short}, "edge_type": "contains",
                    "direction": "out", "where": hidden}),
    ]

    async def enforced(client: Any) -> None:
        for tool, arguments in calls:
            result = await client.call_tool(tool, arguments, meta=JSON_META)
            assert result.isError, (tool, result_text(result))
            error = payload(result)["error"]
            assert error["code"] == "unsupported_operator_form", (tool, error)
            assert "not available under access rules on this deployment" in error["message"]
            served = result_text(result)
            sent = json.dumps(arguments)
            # No record id the caller did not send itself is in the reply.
            for leaked in (short, long, f"chunk:{short}", f"chunk:{long}"):
                if leaked not in sent:
                    assert leaked not in served, (tool, leaked)

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    _drive(clean_runtime, "default", enforced)

    # Audit mode refuses the same calls the same plain way (#3666).
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    _drive(clean_runtime, "default", enforced)

    # Failure case: with the rules off the hidden word matches both records.
    async def unenforced(client: Any) -> None:
        page = await _json(client, "filter", {"where": {"type": "document", **hidden}})
        assert {row["node_id"] for row in page["items"]} == {short, long}

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    _drive(clean_runtime, "default", unenforced)


def test_text_and_date_conditions_combine_in_one_where_on_every_operator(
    clean_runtime: str, matches_deployment: str,
) -> None:
    """One `where` holds a text clause and a date clause; each operator
    answers the records that meet both, and drops either one when it goes."""
    since_2025 = {"date.created": {"op": "gt", "value": "2025-01-01"}}
    in_2024 = {"date.created": {"op": "between", "values": ["2024-01-01", "2024-12-31"]}}
    labels = {"text": {"op": "matches", "value": LABEL_RE}}

    async def body(client: Any) -> None:
        # filter: commits citing a key, made in 2025.
        both = await _json(client, "filter", {"where": _where(type="commit", **since_2025)})
        assert {r["node_id"] for r in both["items"]} == {"commit:c2", "commit:c4"}
        assert both["total_count"] == 2 and both["total_count_basis"] == "exact"
        assert all(unfence(r["match"]).startswith("CMS") for r in both["items"])
        # Failure cases: without the date the 2024 commit is in the set;
        # without the text the commit citing no key is.
        keys_only = await _json(client, "filter", {"where": _where(type="commit")})
        assert "commit:c1" in {r["node_id"] for r in keys_only["items"]}
        dates_only = await _json(client, "filter", {"where": {"type": "commit", **since_2025}})
        assert "commit:c3" in {r["node_id"] for r in dates_only["items"]}

        # map: tickets naming a release label, opened in 2024.
        mapped = await _json(client, "map", {
            "where": {"type": "ticket", **labels, **in_2024}, "select": ["node_id", "match"],
        })
        assert {r["node_id"]: unfence(r["match"]) for r in mapped["items"]} == {
            "ticket:T-2": "CMSSW_15_0_1", "ticket:T-3": "CMSSW_15_0_1",
        }

        # aggregate: the labels counted over those tickets, and the same
        # tickets per month.
        counted = await _json(client, "aggregate", {
            "where": {"type": "ticket", **labels, **in_2024},
            "by": ["match"], "metrics": ["count"],
        })
        assert [(unfence(g["match"]), g["count"]) for g in counted["groups"]] == [
            ("CMSSW_15_0_1", 2), ("CMSSW_14_0_7", 1),
        ]
        monthly = await _json(client, "aggregate", {
            "where": {"type": "ticket", **labels, **in_2024},
            "by": ["month(date.created)"], "metrics": ["count"],
        })
        assert sum(g["count"] for g in monthly["groups"]) == 2
        # Failure case: the 2023 ticket counts once the date goes.
        every = await _json(client, "aggregate", {
            "where": {"type": "ticket", **labels}, "by": ["match"], "metrics": ["count"],
        })
        assert ("CMSSW_14_0_8", 1) in [
            (unfence(g["match"]), g["count"]) for g in every["groups"]
        ]

        # expand: the board's tickets that name a label, opened in 2024.
        call = {"input": {"node_id": "board:hub"}, "edge_type": "tracks", "direction": "out"}
        kept = await _json(client, "expand", {**call, "where": {**labels, **in_2024}})
        assert {row["node_id"] for row in kept["neighbors"]} == {"ticket:T-2", "ticket:T-3"}
        assert kept["total_count"] == 2
        undated = await _json(client, "expand", {**call, "where": labels})
        assert {row["node_id"] for row in undated["neighbors"]} == {
            "ticket:T-1", "ticket:T-2", "ticket:T-3",
        }

    _drive(clean_runtime, matches_deployment, body)


def test_terms_and_exclusions_hold_over_the_whole_record_not_one_passage(
    clean_runtime: str, matches_deployment: str,
) -> None:
    def where(value: str) -> dict:
        return {"type": "document", "text": {"op": "matches", "value": value}}

    async def body(client: Any) -> None:
        # Both words occur in the record, each in its own passage.
        both = await _json(client, "filter", {"where": where("reaper pods")})
        assert {r["node_id"] for r in both["items"]} == {"doc:split"}
        assert both["total_count"] == 1 and both["total_count_basis"] == "exact"
        # An exclusion is of the record: the excluded word sits in the other
        # passage, so the record is not kept.
        excluded = await _json(client, "filter", {"where": where("reaper -drained")})
        assert excluded["items"] == [] and excluded["total_count"] == 0
        # Failure case: a word the record does not hold keeps it out, and
        # without the exclusion the record is kept.
        kept = await _json(client, "filter", {"where": where("reaper")})
        assert {r["node_id"] for r in kept["items"]} == {"doc:split"}
        missing = await _json(client, "filter", {"where": where("reaper zebra")})
        assert missing["items"] == []
        # A phrase running from one passage into the next admits the record
        # (its passages are read joined by a line feed), but no one passage
        # holds it, so there is no matched text to count: the reply says so.
        across = await _json(client, "filter", {"where": where('"restarted after"')})
        assert {r["node_id"] for r in across["items"]} == {"doc:split"}
        # So does a regex whose class matches the line feed between them;
        # the index never narrows on such a class, so the record is found.
        spaced = await _json(client, "filter", {
            "where": where("re:/restarted[[:space:]]after/"),
        })
        assert {r["node_id"] for r in spaced["items"]} == {"doc:split"}
        coded = await _json(client, "filter", {
            "where": where(r"re:/restarted\u000aafter/"),
        })
        assert {r["node_id"] for r in coded["items"]} == {"doc:split"}
        counted = await _json(client, "aggregate", {
            "where": where('"restarted after"'), "by": ["match"], "metrics": ["count"],
        })
        assert counted["groups"] == []
        assert counted["text_match"]["records_counted"] == 1
        assert counted["text_match"]["records_without_match"] == 1

    _drive(clean_runtime, matches_deployment, body)


def test_an_end_anchored_match_reports_the_line_postgresql_matched(
    clean_runtime: str, matches_deployment: str,
) -> None:
    async def body(client: Any) -> None:
        page = await _json(client, "filter", {
            "where": {"type": "commit", "text": {"op": "matches", "value": "re:/judge$/"}},
            "select": ["node_id", "match_count", "matches"],
        })
        assert [r["node_id"] for r in page["items"]] == ["commit:c3"]
        row = page["items"][0]
        # `$` is the end of the text: the second "judge", on line 2. The
        # first "judge" (line 1) is not a match.
        assert row["match_count"] == 1
        assert [(m["line"], unfence(m["text"])) for m in row["matches"]] == [(2, "judge")]
        # Failure case: unanchored, both are matches, line 1 first.
        both = await _json(client, "filter", {
            "where": {"type": "commit", "text": {"op": "matches", "value": "re:/judge/"}},
            "select": ["node_id", "matches"],
        })
        assert [m["line"] for m in both["items"][0]["matches"]] == [1, 2]

    _drive(clean_runtime, matches_deployment, body)


def test_anchors_match_at_every_line_of_a_record_of_several_texts(
    clean_runtime: str, matches_deployment: str,
) -> None:
    """Matching is newline-sensitive, as grep reads lines: `^Agenda` holds
    in the meeting page whose first passage starts with "Agenda", though
    the record's whole text starts with its own text."""

    def where(value: str) -> dict:
        return {"type": "document", "text": {"op": "matches", "value": value}}

    async def body(client: Any) -> None:
        page = await _json(client, "filter", {
            "where": where("re:/^Agenda/"), "select": ["node_id", "match", "match_count"],
        })
        assert [(r["node_id"], unfence(r["match"]), r["match_count"])
                for r in page["items"]] == [("doc:mtg1", "Agenda", 1)]
        ends = await _json(client, "filter", {"where": where("re:/status$/")})
        assert [r["node_id"] for r in ends["items"]] == ["doc:mtg1"]
        counted = await _json(client, "aggregate", {
            "where": where("re:/^CMS[A-Z]+-[0-9]+/"), "by": ["match"], "metrics": ["count"],
        })
        # Keys that start a line: mtg1's second line, mtg2's passage, mtg3's
        # second passage; mtg1's other passage has its key mid-line.
        assert [(unfence(g["match"]), g["count"], g["occurrences"])
                for g in counted["groups"]] == [
            ("CMSTRANSF-1177", 2, 2), ("CMSDM-42", 1, 1),
        ]
        assert counted["text_match"]["records_counted"] == 3
        assert counted["text_match"]["records_without_match"] == 0
        # Failure case: a word that is never at a line start is not matched there.
        assert (await _json(client, "filter", {"where": where("re:/^status/")}))["items"] == []

    _drive(clean_runtime, matches_deployment, body)


def test_counting_matches_under_a_date_condition_states_what_the_dates_read(
    clean_runtime: str, matches_deployment: str,
) -> None:
    labels = {"type": "ticket", "text": {"op": "matches", "value": LABEL_RE},
              "date.created": {"op": "between", "values": ["2024-01-01", "2024-12-31"]}}

    async def body(client: Any) -> None:
        listed = await _json(client, "filter", {"where": labels})
        counted = await _json(client, "aggregate", {
            "where": labels, "by": ["match"], "metrics": ["count"],
        })
        notes = {k: v for k, v in listed.items() if k.startswith("date")}
        # The stored dates have no zone: both replies say they were read as UTC.
        assert "dates_read_as_utc" in notes, listed
        assert {k: v for k, v in counted.items() if k.startswith("date")} == notes
        # Failure case: with no date condition there is nothing to state.
        plain = await _json(client, "aggregate", {
            "where": {"type": "ticket", "text": labels["text"]},
            "by": ["match"], "metrics": ["count"],
        })
        assert not any(k.startswith("date") for k in plain)

    _drive(clean_runtime, matches_deployment, body)


def test_a_posix_class_in_brackets_counts_lists_and_groups_alike(
    clean_runtime: str, matches_deployment: str,
) -> None:
    """`x[[:alpha:]()]` matches "xa" and not "x?": the count, the listed
    matches and the groups agree (the `(` stays a literal of the brackets)."""
    where = {"type": "document", "text": {"op": "matches", "value": "re:/x[[:alpha:]()]/"}}

    async def body(client: Any) -> None:
        page = await _json(client, "filter", {
            "where": where, "select": ["node_id", "match_count", "matches"],
        })
        assert [r["node_id"] for r in page["items"]] == ["doc:split"]
        row = page["items"][0]
        assert row["match_count"] == 1
        assert [unfence(m["text"]) for m in row["matches"]] == ["xa"]
        counted = await _json(client, "aggregate", {
            "where": where, "by": ["match"], "metrics": ["count"],
        })
        assert [(unfence(g["match"]), g["count"], g["occurrences"])
                for g in counted["groups"]] == [("xa", 1, 1)]
        assert counted["text_match"]["records_without_match"] == 0
        # Failure case: "x?" is no match, and no group says it is.
        assert "x?" not in json.dumps(counted["groups"])

    _drive(clean_runtime, matches_deployment, body)


# -- overlapping chunks (issue #3802) ------------------------------------------

#: A file a window chunker cut into two chunks that share lines 3-4: the
#: second chunk repeats "keep 90 days ago" from the first one's tail.
OVERLAP, CLAIMED = "doc:overlap", "doc:claimed"
OVERLAP_SOURCE = (
    "alpha one\nkeep 30 days ago\nbeta two\nkeep 90 days ago\n"
    "gamma three\nkeep 7 days ago\ndelta four\n"
)
OVERLAP_SPANS = [
    (0, OVERLAP_SOURCE.index("gamma three")),
    (OVERLAP_SOURCE.index("beta two"), len(OVERLAP_SOURCE)),
]
#: The second chunk's head "beta" is mid-line in the first chunk: `^beta`
#: matches only at the second chunk's start, inside the repeated head.
MIDLINE = "doc:midline"
MIDLINE_TEXTS, MIDLINE_SPANS = ["alpha beta", "beta\ngamma"], [(0, 10), (6, 16)]
#: The head "xyyy" ends the first chunk, where `xy*$` matches it whole; in
#: the second chunk "xyyyz" holds three `y` matches and no `xy*$`.
DOLLAR = "doc:dollar"
DOLLAR_TEXTS, DOLLAR_SPANS = ["q xyyy", "xyyyz\n"], [(0, 6), (2, 8)]
#: The head "aa bb" is one `\w+ \w+` match in the second chunk, but the
#: first chunk's scan matched "xx aa" across it: the chunks split it apart.
WORDS = "doc:words"
WORDS_TEXTS, WORDS_SPANS = ["xx aa bb", "aa bb cc\n"], [(0, 8), (3, 12)]
#: Spans that say the same overlap, but the second chunk's head is other
#: text: nothing proves a repeat, so both chunks are read whole.
CLAIMED_FIRST = "alpha one\nkeep 11 days ago\nbeta two\nkeep 12 days ago\n"
CLAIMED_START = CLAIMED_FIRST.index("beta two")
CLAIMED_SECOND = "BETA TWO\nkeep 12 days ago\ngamma\nkeep 13 days ago\n"
#: The same file cut so the first chunk ends inside "90 days ago": the
#: repeated head ends inside that match, so only the second chunk holds it
#: whole, and it is kept there.
CROSS = "doc:cross"
CROSS_SPANS = [
    (0, OVERLAP_SOURCE.index("90 days ago") + len("90 da")),
    (OVERLAP_SOURCE.index("beta two"), len(OVERLAP_SOURCE)),
]


def _overlapping(
    doc_id: str, texts: list[str], spans: list[tuple[int, int]], length: int,
    *, own: str = "a file cut with overlap",
):
    nodes = [node(doc_id, "document", own, title=doc_id)]
    edges = []
    for i, (text, (start, end)) in enumerate(zip(texts, spans)):
        assert end - start == len(text)
        chunk_id = f"{doc_id}:chunk:{i}"
        nodes.append(node(chunk_id, "document_chunk", text, chunk_index=i))
        edges.append(EdgeFact(
            chunk_id, doc_id, "member_of",
            {"chunk_ordinal": i, "chunk_total": len(texts), "char_offset": start,
             "char_end": end, "doc_char_length": length},
            source_record_id={"id": f"{chunk_id}-m"}, source_revision={"v": 1},
        ))
        edges.append(EdgeFact(
            src=doc_id, dst=chunk_id, edge_type="contains",
            source_record_id={"id": f"{chunk_id}-c"}, source_revision={"v": 1},
        ))
    return nodes, edges


@pytest.fixture
def overlap_deployment(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path,
) -> str:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    overlap = _overlapping(
        OVERLAP, [OVERLAP_SOURCE[a:b] for a, b in OVERLAP_SPANS], OVERLAP_SPANS,
        len(OVERLAP_SOURCE),
    )
    claimed_end = CLAIMED_START + len(CLAIMED_SECOND)
    claimed = _overlapping(
        CLAIMED, [CLAIMED_FIRST, CLAIMED_SECOND],
        [(0, len(CLAIMED_FIRST)), (CLAIMED_START, claimed_end)], claimed_end,
    )
    cross = _overlapping(
        CROSS, [OVERLAP_SOURCE[a:b] for a, b in CROSS_SPANS], CROSS_SPANS,
        len(OVERLAP_SOURCE),
    )
    midline = _overlapping(MIDLINE, MIDLINE_TEXTS, MIDLINE_SPANS, 16)
    dollar = _overlapping(DOLLAR, DOLLAR_TEXTS, DOLLAR_SPANS, 8)
    # Its own text holds no two words in a row, so only the chunks match.
    words = _overlapping(WORDS, WORDS_TEXTS, WORDS_SPANS, 12, own="words")
    built = [overlap, claimed, cross, midline, dollar, words]
    publish_nodes(
        clean_runtime, deployment, subtypes=[DOC, CHUNK],
        narrowings=[
            ("document", "contains", "document_chunk"),
            ("document_chunk", "member_of", "document"),
        ],
        nodes=[n for b in built for n in b[0]], edges=[e for b in built for e in b[1]],
    )
    return deployment


def test_a_line_two_overlapping_chunks_share_is_counted_once_on_every_path(
    clean_runtime: str, overlap_deployment: str,
) -> None:
    """#3802: "keep 90 days ago" is in both chunks of `doc:overlap`. filter's
    match_count and matches, aggregate by match, and the windows view each
    count it once: the file has 3 matching lines, not 4."""
    days = {"type": "document", "text": {"op": "matches", "value": '"days ago"'}}
    numbered = {"type": "document",
                "text": {"op": "matches", "value": "re:/[0-9]+ days ago/"}}
    assert OVERLAP_SPANS[1][0] < OVERLAP_SPANS[0][1], "the chunks must overlap"

    async def body(client: Any) -> None:
        page = await _json(client, "filter", {
            "where": days, "select": ["node_id", "match_count", "matches"],
        })
        rows = {row["node_id"]: row for row in page["items"]}
        counted = await _json(client, "aggregate", {
            "where": numbered, "by": ["match"], "metrics": ["count"],
        })
        groups = {unfence(g["match"]): (g["count"], g["occurrences"])
                  for g in counted["groups"]}
        windows = await _json(client, "search", {
            "query": '"days ago"', "method": "grep", "view": "windows",
            "scope": {"records": [OVERLAP]}, "context_chars": 0, "max_bytes": 131072,
        })
        (record,) = windows["results"]
        # The shared line once on each path: filter, aggregate, windows.
        assert (
            rows[OVERLAP]["match_count"], groups.get("90 days ago"), record["match_count"],
        ) == (3, (2, 2), 3), (rows[OVERLAP], groups, record)
        # The listed lines are the file's lines, each once, and read back.
        lines = [m["line"] for m in rows[OVERLAP]["matches"]]
        assert lines == [2, 4, 6], rows[OVERLAP]["matches"]
        for line in lines:
            read = await _json(client, "inspect", _target(OVERLAP, [[line, line]]))
            assert "days ago" in _joined(read), (line, _joined(read))
        # `doc:overlap` and `doc:cross` hold 30, 90 and 7 once each.
        assert groups == {
            "30 days ago": (2, 2), "90 days ago": (2, 2), "7 days ago": (2, 2),
            "11 days ago": (1, 1), "12 days ago": (1, 2), "13 days ago": (1, 1),
        }, groups
        assert counted["text_match"]["records_counted"] == 3
        assert counted["text_match"]["records_without_match"] == 0
        # The positions are not proven by a producer contract, so the
        # chunks are read as separate texts: the repeat is found from the
        # stored spans and the texts themselves.
        assert "producer_contract" not in record["position_basis"], record
        assert record["match_count"] == 3 and windows["match_count"] == 3, record
        shown = sorted(
            (f["chunk_id"], tuple(f["range"]))
            for w in record["windows"] for f in w["fragments"]
        )
        assert len(shown) == len(set(shown)) == 3, shown
        # Failure case: `doc:claimed` says the same overlap in its spans, but
        # the second chunk's head is other text, so nothing is taken back:
        # its line "keep 12 days ago" is in both chunks and counted twice.
        assert rows[CLAIMED]["match_count"] == 4, rows[CLAIMED]
        claimed = await _json(client, "search", {
            "query": '"days ago"', "method": "grep", "view": "windows",
            "scope": {"records": [CLAIMED]}, "context_chars": 0, "max_bytes": 131072,
        })
        assert claimed["results"][0]["match_count"] == 4, claimed["results"][0]
        # Passage matching keeps each passage's own evidence whole.
        local = await _json(client, "search", {
            "query": '"days ago"', "method": "grep", "view": "windows",
            "scope": {"records": [OVERLAP]}, "context_chars": 0, "max_bytes": 131072,
            "matching": "passage",
        })
        assert local["results"][0]["match_count"] == 4, local["results"][0]

    _drive(clean_runtime, overlap_deployment, body)


def test_a_match_crossing_the_head_end_is_kept_and_an_anchor_at_the_head_start_counts_once(
    clean_runtime: str, overlap_deployment: str,
) -> None:
    """#3802 edges. In `doc:cross` the first chunk ends inside "90 days
    ago", so that match crosses the end of the second chunk's repeated head:
    only the second chunk holds it whole, and it is kept, once. In
    `doc:overlap` the second chunk starts at "beta two", a line start in the
    first chunk too: `^beta` matches at the head's start in both chunks and
    counts once."""
    days = {"type": "document", "text": {"op": "matches", "value": '"days ago"'}}
    beta = {"type": "document", "text": {"op": "matches", "value": "re:/^beta/"}}
    head = CROSS_SPANS[0][1] - CROSS_SPANS[1][0]
    second = OVERLAP_SOURCE[CROSS_SPANS[1][0]:]
    at = second.index("days ago")
    assert at < head < at + len("days ago"), "the match must cross the head's end"

    async def body(client: Any) -> None:
        page = await _json(client, "filter", {
            "where": days, "select": ["node_id", "match_count", "matches"],
        })
        rows = {row["node_id"]: row for row in page["items"]}
        assert rows[CROSS]["match_count"] == 3, rows[CROSS]
        assert [m["line"] for m in rows[CROSS]["matches"]] == [2, 4, 6], rows[CROSS]
        assert rows[CROSS]["matches"][1]["passage_id"] == f"{CROSS}:chunk:1"
        windows = await _json(client, "search", {
            "query": '"days ago"', "method": "grep", "view": "windows",
            "scope": {"records": [CROSS]}, "context_chars": 0, "max_bytes": 131072,
        })
        assert windows["results"][0]["match_count"] == 3, windows["results"][0]
        anchored = await _json(client, "filter", {
            "where": beta, "select": ["node_id", "match_count", "matches"],
        })
        rows = {row["node_id"]: row for row in anchored["items"]}
        assert rows[OVERLAP]["match_count"] == 1, rows[OVERLAP]
        assert [m["line"] for m in rows[OVERLAP]["matches"]] == [3], rows[OVERLAP]
        groups = await _json(client, "aggregate", {
            "where": beta, "by": ["match"], "metrics": ["count"],
        })
        # doc:overlap, doc:cross, doc:claimed's first chunk and doc:midline
        # (see the next test): "beta" once each. Failure case: doc:claimed's
        # "BETA" starts its second chunk, which repeats nothing, so it is
        # counted, as its own group.
        assert {(unfence(g["match"]), g["count"], g["occurrences"])
                for g in groups["groups"]} == {("beta", 4, 4), ("BETA", 1, 1)}, groups

    _drive(clean_runtime, overlap_deployment, body)


def test_a_head_match_the_chunk_before_does_not_hold_is_kept(
    clean_runtime: str, overlap_deployment: str,
) -> None:
    """#3802. A head match is taken back only when the chunk before holds
    the same match at the same place.

    `doc:midline`: chunk A "alpha beta" at [0, 10], chunk B "beta\\ngamma"
    at [6, 16]. `^beta` (line start) matches at B's start, inside the head,
    but A reads that "beta" mid-line and has no match there: B's match is
    kept, as dev counts it, and the record holds a match.

    `doc:overlap` with `^beta`: the text condition reads `^` as a line
    start, A has "beta two" at a line start too, so B's copy is taken back:
    1. The windows view reads `^` as the start of a passage's text: A has
    no `^beta` there, so B's is kept: 1, as on dev. The head's own matches
    plus one bound how many are tried (2 here, though B holds 1): the try
    past the last match is dropped."""
    beta = {"type": "document", "text": {"op": "matches", "value": "re:/^beta/"}}

    async def body(client: Any) -> None:
        page = await _json(client, "filter", {
            "where": beta, "select": ["node_id", "match_count", "matches"],
        })
        rows = {row["node_id"]: row for row in page["items"]}
        assert rows[MIDLINE]["match_count"] == 1, rows[MIDLINE]
        assert [unfence(m["text"]) for m in rows[MIDLINE]["matches"]] == ["beta"]
        assert rows[OVERLAP]["match_count"] == 1, rows[OVERLAP]
        counted = await _json(client, "aggregate", {
            "where": beta, "by": ["match"], "metrics": ["count"],
        })
        groups = {(unfence(g["match"]), g["count"], g["occurrences"])
                  for g in counted["groups"]}
        assert groups == {("beta", 4, 4), ("BETA", 1, 1)}, groups
        # Admitted and holding a match: overlap, cross, claimed, midline.
        assert counted["text_match"]["records_counted"] == 4
        assert counted["text_match"]["records_without_match"] == 0, counted["text_match"]
        for doc_id in (OVERLAP, MIDLINE):
            windows = await _json(client, "search", {
                "query": "re:/^beta/", "method": "grep", "view": "windows",
                "scope": {"records": [doc_id]}, "context_chars": 0, "max_bytes": 131072,
            })
            assert windows["results"][0]["match_count"] == 1, (doc_id, windows["results"])
        # Failure case: passage matching keeps that passage's own match.
        local = await _json(client, "search", {
            "query": "re:/^beta/", "method": "grep", "view": "windows",
            "scope": {"records": [OVERLAP]}, "context_chars": 0, "max_bytes": 131072,
            "matching": "passage",
        })
        assert local["results"][0]["match_count"] == 1, local["results"][0]

    _drive(clean_runtime, overlap_deployment, body)


def test_an_alternation_with_an_end_anchor_is_served_as_dev_serves_it(
    clean_runtime: str, overlap_deployment: str,
) -> None:
    """#3802, disclosed. `doc:dollar`: chunk A "q xyyy" at [0, 6], chunk B
    "xyyyz\\n" at [2, 8], head "xyyy". With `xy*$|y`, A ends in its head
    text, so `xy*$` matches "xyyy" there; B holds "xyyyz", where it does
    not, and three `y`. The file's line "q xyyyz" holds three `y` and no
    `xy*$` (3 matches). A holds none of B's `y` at the same place, so
    nothing is taken back and every path serves what dev serves:

    * filter: match_count 4, listing "xyyy" and the three `y`.
    * aggregate: "xyyy" once, `y` three times.
    * windows (`$` is the end of a passage's text): 4."""
    # The other test files hold "days ago": only this record is asked about.
    where = {"type": "document", "attrs.title": {"op": "eq", "value": DOLLAR},
             "text": {"op": "matches", "value": "re:/xy*$|y/"}}

    async def body(client: Any) -> None:
        page = await _json(client, "filter", {
            "where": where, "select": ["node_id", "match_count", "matches"],
        })
        (row,) = [r for r in page["items"] if r["node_id"] == DOLLAR]
        assert row["match_count"] == 4, row
        assert [unfence(m["text"]) for m in row["matches"]] == ["xyyy", "y", "y", "y"], row
        counted = await _json(client, "aggregate", {
            "where": where, "by": ["match"], "metrics": ["count"],
        })
        groups = {(unfence(g["match"]), g["count"], g["occurrences"])
                  for g in counted["groups"]}
        assert groups == {("xyyy", 1, 1), ("y", 1, 3)}, groups
        windows = await _json(client, "search", {
            "query": "re:/xy*$|y/", "method": "grep", "view": "windows",
            "scope": {"records": [DOLLAR]}, "context_chars": 0, "max_bytes": 131072,
        })
        assert windows["results"][0]["match_count"] == 4, windows["results"][0]

    _drive(clean_runtime, overlap_deployment, body)


def test_a_head_match_the_chunk_before_split_otherwise_is_kept(
    clean_runtime: str, overlap_deployment: str,
) -> None:
    """#3802. `doc:words`: chunk A "xx aa bb" at [0, 8], chunk B "aa bb
    cc\\n" at [3, 12], head "aa bb". With `\\w+ \\w+`, A's scan matches
    "xx aa" and leaves "bb"; B's matches "aa bb" inside the head and leaves
    "cc". A holds no match at "aa bb"'s place, so B's is kept: 2 on every
    path (the file "xx aa bb cc" holds 2, "xx aa" and "bb cc"). A rule that
    took back every match inside the head served 1 here."""
    where = {"type": "document", "attrs.title": {"op": "eq", "value": WORDS},
             "text": {"op": "matches", "value": r"re:/\w+ \w+/"}}

    async def body(client: Any) -> None:
        page = await _json(client, "filter", {
            "where": where, "select": ["node_id", "match_count", "matches"],
        })
        (row,) = [r for r in page["items"] if r["node_id"] == WORDS]
        assert row["match_count"] == 2, row
        assert [unfence(m["text"]) for m in row["matches"]] == ["xx aa", "aa bb"], row
        counted = await _json(client, "aggregate", {
            "where": where, "by": ["match"], "metrics": ["count"],
        })
        groups = {(unfence(g["match"]), g["count"], g["occurrences"])
                  for g in counted["groups"]}
        assert groups == {("xx aa", 1, 1), ("aa bb", 1, 1)}, groups
        windows = await _json(client, "search", {
            "query": r"re:/\w+ \w+/", "method": "grep", "view": "windows",
            "scope": {"records": [WORDS]}, "context_chars": 0, "max_bytes": 131072,
        })
        assert windows["results"][0]["match_count"] == 2, windows["results"][0]

    _drive(clean_runtime, overlap_deployment, body)


# --------------------------------------------------------------------------- #
# counting each distinct match from a grep (Format v10 item C)                 #
# --------------------------------------------------------------------------- #

ABC_RE = "re:/ABC-[0-9]+/"
#: Meeting pages: ABC-12 on three weekly pages, ABC-7 on two, ABC-301 on
#: one; a retro page also holds ABC-12 and is outside the title prefix.
MEETING_PAGES = {
    "mtg:w1": ("Weekly sync 1", "ABC-12 is blocked\nABC-7 waits on review"),
    "mtg:w2": ("Weekly sync 2", "ABC-12 again and ABC-12 once more"),
    "mtg:w3": ("Weekly sync 3", "ABC-7 merged; ABC-301 opened"),
    "mtg:w4": ("Weekly sync 4", "ABC-12 closed"),
    "mtg:w5": ("Weekly sync 5", "nothing tracked this week"),
    "mtg:r1": ("Retro", "ABC-12 came up in the retro"),
}
#: The meeting type reads its title from `name`.
MEETING_META = {"searchable_text_fields": ["text"], "title_from": "name"}


@pytest.fixture
def meetings_deployment(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path,
) -> str:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=[SubtypeSpec("meeting", metadata=MEETING_META)],
        narrowings=[],
        nodes=[node(page, "meeting", text, name=name)
               for page, (name, text) in MEETING_PAGES.items()],
        edges=[],
    )
    return deployment


def test_a_cut_grep_with_a_regex_names_the_aggregate_call_that_counts_each_key(
    clean_runtime: str, meetings_deployment: str,
) -> None:
    grep = {
        "query": ABC_RE, "method": "grep", "limit": 2,
        "where": {"type": "meeting", "title": {"op": "prefix", "value": "Weekly"}},
    }

    async def body(client: Any) -> None:
        page = await _json(client, "search", grep)
        call = page["count_each_match"]
        # Search's `title` is read through the attribute the type declares.
        assert call["tool"] == "aggregate" and call["read_as"] == {"title": "attrs.name"}
        assert call["arguments"] == {
            "where": {"type": "meeting", "attrs.name": {"op": "prefix", "value": "Weekly"},
                      "text": {"op": "matches", "value": ABC_RE}},
            "by": ["match"], "metrics": ["count"],
        }
        assert "dropped" not in call
        # The printed call is served by aggregate as written: one group per
        # key, the weekly pages holding it, and its occurrences.
        counted = await _json(client, "aggregate", call["arguments"])
        assert [(unfence(g["match"]), g["count"], g["occurrences"])
                for g in counted["groups"]] == [
            ("ABC-12", 3, 4), ("ABC-7", 2, 2), ("ABC-301", 1, 1),
        ]
        assert counted["total_count"] == 3 and counted["total_count_basis"] == "exact"
        assert "one_group_plain_words" not in counted["text_match"]
        text = await _text(client, "search", grep)
        printed = [line for line in text.split("\n")
                   if line.startswith("count each distinct match: aggregate(")]
        assert len(printed) == 1, text
        assert '"attrs.name":{"op":"prefix","value":"Weekly"}' in printed[0]
        assert "title read as attrs.name" in printed[0]
        # Failure cases: a page holding every match, and plain words, name no call.
        whole = await _json(client, "search", {**grep, "limit": 50})
        assert "count_each_match" not in whole
        assert "count each distinct match" not in await _text(
            client, "search", {**grep, "limit": 50},
        )
        words = await _json(client, "search", {**grep, "query": "ABC"})
        assert "count_each_match" not in words

    _drive(clean_runtime, meetings_deployment, body)


def test_a_cut_grep_names_no_count_call_where_access_rules_refuse_aggregate(
    clean_runtime: str, meetings_deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review P1-1: the server's own aggregate-offer check says aggregate is
    refused (access rules on, audit or enforce): the cut grep page names no
    call. Only that check is patched; the grep itself is served as before
    (the rules' effect on the rule check itself is covered in
    `test_text_matches.py`)."""
    from okg.substrate.mcp.response_bounds import ByteBoundedFastMCP

    monkeypatch.setattr(ByteBoundedFastMCP, "_okg_aggregate_refused", lambda self: True)
    grep = {
        "query": ABC_RE, "method": "grep", "limit": 2,
        "where": {"type": "meeting", "title": {"op": "prefix", "value": "Weekly"}},
    }

    async def body(client: Any) -> None:
        page = await _json(client, "search", grep)
        assert page["results"], page
        assert "count_each_match" not in page
        assert "count each distinct match" not in await _text(client, "search", grep)

    _drive(clean_runtime, meetings_deployment, body)


def test_a_cut_grep_names_no_count_call_where_aggregate_is_not_served(
    clean_runtime: str, meetings_deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review F1: an allowlist of inspect and search removes aggregate from
    the server, so the cut grep page names no aggregate call. The same grep
    names it when aggregate is served (the test above)."""
    from okg.substrate.mcp.result_text import _cut

    monkeypatch.setenv("OKG_MCP_TOOL_ALLOWLIST", "inspect,search")
    grep = {
        "query": ABC_RE, "method": "grep", "limit": 2,
        "where": {"type": "meeting", "title": {"op": "prefix", "value": "Weekly"}},
    }

    async def body(client: Any) -> None:
        listed = {tool.name for tool in (await client.list_tools()).tools}
        assert "aggregate" not in listed and "search" in listed, listed
        page = await _json(client, "search", grep)
        assert page["results"], page
        assert _cut(page), page  # the page is cut, so only the served check withholds it
        assert "count_each_match" not in page
        assert "count each distinct match" not in await _text(client, "search", grep)

    _drive(clean_runtime, meetings_deployment, body)


def test_counting_plain_words_per_match_says_a_regex_groups_each_match(
    clean_runtime: str, meetings_deployment: str,
) -> None:
    async def body(client: Any) -> None:
        arguments = {
            "where": {"type": "meeting", "text": {"op": "matches", "value": "ABC"}},
            "by": ["match"], "metrics": ["count"],
        }
        counted = await _json(client, "aggregate", arguments)
        assert [unfence(g["match"]) for g in counted["groups"]] == ["ABC"]
        assert counted["text_match"]["one_group_plain_words"] is True
        text = await _text(client, "aggregate", arguments)
        assert "! plain words match only themselves, so one group; a regex" in text
        # Failure case: the regex groups each key and carries no such notice.
        keyed = await _json(client, "aggregate", {
            **arguments, "where": {"type": "meeting",
                                   "text": {"op": "matches", "value": ABC_RE}},
        })
        assert len(keyed["groups"]) == 3
        assert "one_group_plain_words" not in keyed["text_match"]

    _drive(clean_runtime, meetings_deployment, body)


#: The held-out shape (lead replay 2026-10-17): pages whose text is in
#: passages, a grep narrowed by `source` and a title prefix, no type named.
#: Source "docs:notes" holds three "Meeting" pages; the fixture's own source
#: holds four "Retro" pages full of ABC-99, so a call that lost the scope
#: would rank ABC-99 first.
PAGE = SubtypeSpec("page", metadata={
    "searchable_text_fields": ["title", "text"], "title_from": "title",
    "passage_rollup": {"edge_type": "contains", "child_subtype": "page_chunk"},
})
PAGE_CHUNK = SubtypeSpec("page_chunk", metadata={
    "searchable_text_fields": ["text"], "title_from": "heading_path",
})
NOTES_SOURCE = "docs:notes"
NOTES_PAGES = {
    "page:m1": ("Meeting 1", "ABC-12 is blocked; ABC-7 waits"),
    "page:m2": ("Meeting 2", "ABC-12 again"),
    "page:m3": ("Meeting 3", "ABC-7 merged and ABC-12 closed"),
}
OTHER_PAGES = {
    f"page:r{i}": (f"Retro {i}", "ABC-99 ABC-99 came up") for i in range(1, 5)
}


def _pages(pages: dict[str, tuple[str, str]]) -> list:
    facts: list = []
    for page, (title, text) in pages.items():
        chunk = f"{page}:chunk:0"
        facts += [
            node(page, "page", "page notes", title=title),
            node(chunk, "page_chunk", text, chunk_index=0, heading_path=title),
            EdgeFact(src=page, dst=chunk, edge_type="contains",
                     source_record_id={"id": f"{chunk}-c"}, source_revision={"v": 1}),
        ]
    return facts


@pytest.fixture
def scoped_pages_deployment(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path,
) -> str:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment, subtypes=[PAGE, PAGE_CHUNK],
        narrowings=[("page", "contains", "page_chunk")],
        node_groups=[(NOTES_SOURCE, _pages(NOTES_PAGES))],
        nodes=_pages(OTHER_PAGES), edges=[],
    )
    return deployment


def test_a_source_and_title_scoped_grep_names_a_call_that_keeps_the_title_scope(
    clean_runtime: str, scoped_pages_deployment: str,
) -> None:
    """Before: the printed call was `aggregate(where={"text": ...})` over every
    record (source and title both left out), and its top group was ABC-99.
    After: the passage type folds into the page type, the title prefix is
    read through the page's declared title, and the one part aggregate cannot
    take (source) is named with which way the count moves."""
    grep = {
        "query": ABC_RE, "method": "grep", "limit": 1,
        "where": {"source": NOTES_SOURCE, "title": {"op": "prefix", "value": "Meeting"}},
    }

    async def body(client: Any) -> None:
        page = await _json(client, "search", grep)
        call = page["count_each_match"]
        where = call["arguments"]["where"]
        assert where != {"text": {"op": "matches", "value": ABC_RE}}, call
        assert call["read_as"] == {"title": "attrs.title"}
        assert call["dropped"] == [json.dumps({"source": NOTES_SOURCE}, separators=(",", ":"))]
        assert "page_chunk" not in json.dumps(where), "a passage counts through its page"
        counted = await _json(client, "aggregate", call["arguments"])
        assert [(unfence(g["match"]), g["count"], g["occurrences"])
                for g in counted["groups"]] == [("ABC-12", 3, 3), ("ABC-7", 2, 2)]
        text = await _text(client, "search", grep)
        (line,) = [ln for ln in text.split("\n") if ln.startswith("count each distinct match:")]
        assert "title read as attrs.title" in line
        assert "NOT the grep's set, it may count more records: aggregate cannot take" in line
        # Failure case: the scope-free call the page printed before ranks
        # the other source's key first.
        unscoped = await _json(client, "aggregate", {
            "where": {"text": {"op": "matches", "value": ABC_RE}},
            "by": ["match"], "metrics": ["count"],
        })
        assert unfence(unscoped["groups"][0]["match"]) == "ABC-99"
        # A grep narrowed by source alone carries nothing over: no call.
        alone = await _json(client, "search", {**grep, "where": {"source": NOTES_SOURCE}})
        assert "count_each_match" not in alone
        assert "count each distinct match" not in await _text(
            client, "search", {**grep, "where": {"source": NOTES_SOURCE}},
        )

    _drive(clean_runtime, scoped_pages_deployment, body)
