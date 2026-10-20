"""Line numbers of `inspect` text and line-range reads (issue #3666).

Hermetic: the one definition of a line, the `lines` argument's checks,
cutting a prepared record to ranges, and the line span a page reports, with
the graph reads replaced as in test_record_text.py. The served reads over a
real publish are in test_text_lines_db.py.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from okg.substrate.mcp import record_text, result_text
from okg.substrate.mcp.record_text import (
    MAX_LINE_NUMBER,
    MAX_LINE_RANGES,
    OPEN_END,
    MatchPlace,
    span_lines,
    spans_from,
    chunk_offset_line,
    coverage_of,
    fill_separators,
    line_basis,
    line_count,
    line_spans,
    number_lines,
    order_segments,
    own_text_line,
    parse_line_target,
    parse_lines,
    read_record_text,
    record_match_lines,
    trim_overlap,
)
from okg.substrate.mcp.search_views import grep_list_projection
from okg.substrate.mcp.response_bounds import (
    InvalidOperatorArguments,
    public_call_tool_result_bytes,
)
from okg.substrate.mcp.text_batch import read_text_batch
from tests.substrate.mcp.test_record_text import SESSION, _install, _seg, _windows

#: Forty numbered lines, the last one without a line feed.
SOURCE = "".join(f"line {i:02d} of the file\n" for i in range(1, 40)) + "line 40, no feed"


def _lines(text: str) -> list[str]:
    """`text` as lines that keep their line feed, as a range read serves them."""
    parts = text.split("\n")
    return [part + "\n" for part in parts[:-1]] + ([parts[-1]] if parts[-1] else [])


def _prepared(segments: list) -> tuple[list, str, str]:
    ordered, basis = order_segments(segments)
    trim_overlap(ordered)
    fill_separators(ordered)
    number_lines(ordered)
    return ordered, basis, coverage_of(ordered, basis)


def _cut(segments: list, ranges: list[tuple[int, int]]) -> list[tuple[str, tuple[int, int]]]:
    """Each piece of a range cut as its text and the lines it says it holds."""
    return [
        (segments[span.segment].text[span.start:span.end], span_lines(segments, span, span.end))
        for span in line_spans(segments, ranges)
    ]


# --------------------------------------------------------------------------- #
# the one definition of a line
# --------------------------------------------------------------------------- #


def test_lines_of_spans_that_run_on_are_the_source_files_lines() -> None:
    """Chunks cut in the middle of lines still number as the file does."""
    segments, basis, coverage = _prepared(_windows(SOURCE, 137, 0))
    assert coverage == "complete" and len(segments) > 5
    assert not any(s.line_break_before for s in segments)
    assert number_lines(segments) == line_count(segments) == 40
    assert line_basis(segments, coverage, basis) == {"basis": "source"}
    offset = 0
    for segment in segments:
        assert segment.line_start == SOURCE.count("\n", 0, offset) + 1
        offset += len(segment.text)


def test_only_a_line_feed_ends_a_line() -> None:
    """A carriage return, a form feed and a Unicode line separator do not."""
    text = "a\rb\x0cc d\ne\r\nf"
    segments, _basis, _coverage = _prepared(
        [_seg("chunk:0", text, ordinal=0, total=1, offset=0, end=len(text), doc_length=len(text))]
    )
    assert line_count(segments) == 3
    assert [piece for piece, _held in _cut(segments, [(1, 1)])] == ["a\rb\x0cc d\n"]


def test_a_segment_that_does_not_continue_the_one_before_starts_a_new_line() -> None:
    """Paragraph chunks with no positions: no words of two chunks share a
    line, and a chunk that ends with a line feed adds no empty line."""
    segments, basis, coverage = _prepared([
        _seg("chunk:a", "one\ntwo", ordinal=0, total=4),
        _seg("chunk:b", "three", ordinal=1, total=4),
        _seg("chunk:c", "four\n", ordinal=2, total=4),
        _seg("chunk:d", "five", ordinal=3, total=4),
    ])
    assert [s.line_start for s in segments] == [1, 3, 4, 5]
    assert [s.line_break_before for s in segments] == [False, True, True, False]
    assert line_count(segments) == 5
    assert line_basis(segments, coverage, basis) == {"basis": "shown_text"}


def test_a_gap_says_the_lines_are_of_the_shown_text() -> None:
    """The middle chunk is missing: lines are counted over what is shown,
    the chunk after the gap starts a new line, and the reply's words are the
    design's."""
    whole = _windows(SOURCE, 137, 0)
    segments, basis, coverage = _prepared([whole[0], *whole[2:]])
    assert coverage == "partial"
    assert segments[1].line_break_before is True
    assert line_basis(segments, coverage, basis) == {"basis": "shown_text"}
    # Fewer lines than the file: the missing chunk's lines are not counted.
    assert line_count(segments) < 40


def test_a_complete_read_that_carries_a_chunk_the_source_lacks_is_not_source_lines() -> None:
    """A table transcription has no span: coverage stays complete, but the
    lines after it are no longer the file's, and the reply says so."""
    half = len(SOURCE) // 2
    segments, basis, coverage = _prepared([
        _seg("chunk:0", SOURCE[:half], ordinal=0, total=3, offset=0, end=half,
             doc_length=len(SOURCE)),
        _seg("chunk:t", "| a | b |\n| 1 | 2 |", ordinal=1, total=3),
        _seg("chunk:1", SOURCE[half:], ordinal=2, total=3, offset=half, end=len(SOURCE),
             doc_length=len(SOURCE)),
    ])
    assert coverage == "complete"
    assert line_basis(segments, coverage, basis)["basis"] == "shown_text"
    assert line_count(segments) > 40


def test_a_chunk_read_alone_numbers_the_chunks_own_text() -> None:
    segments = [_seg("chunk:0", "a\nb\nc")]
    number_lines(segments)
    assert line_basis(segments, "complete", "single_chunk") == {"basis": "shown_text"}
    # Every value the block can say is in the one vocabulary.
    assert set(record_text.LINE_BASES) == {"source", "shown_text"}
    assert set(record_text.MATCH_LINE_BASES) == {"passage", "own_text"}


# --------------------------------------------------------------------------- #
# the `lines` argument
# --------------------------------------------------------------------------- #


def test_valid_ranges_are_accepted_as_given() -> None:
    assert parse_lines([[1278, 1302], [1555, 1578]]) == ((1278, 1302), (1555, 1578))
    assert parse_lines([[7, 7]]) == ((7, 7),)
    assert len(parse_lines([[i * 2 + 1, i * 2 + 1] for i in range(MAX_LINE_RANGES)])) == 20
    assert parse_lines([[1, MAX_LINE_NUMBER]]) == ((1, MAX_LINE_NUMBER),)


def test_an_open_range_reads_to_the_end_and_is_echoed_as_written(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert parse_lines([[2, 3], [38, None]]) == ((2, 3), (38, OPEN_END))
    _install(monkeypatch, _windows(SOURCE, 137, 0))
    page = read_record_text(None, SESSION, "doc:1", lines=[[38, None]], max_bytes=65536)
    assert "".join(item["text"] for item in page["items"]) == "".join(_lines(SOURCE)[37:])
    # To the end is not past the end; a start after the last line is.
    assert page["lines"] == {
        "basis": "source", "total": 40, "requested": [[38, None]], "served": [[38, 40]],
    }
    head = result_text.render(page, tool="inspect", kind="text").split("\n", 1)[0]
    assert "source lines 38-40 of 40" in head
    beyond = read_record_text(None, SESSION, "doc:1", lines=[[41, None]], max_bytes=65536)
    assert beyond["items"] == [] and beyond["lines"]["past_end"] is True


def test_a_line_number_over_the_bound_is_refused_by_name_whatever_the_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A number of thousands of digits used to leave no room for text and
    came back as a budget refusal; it is the argument that is wrong."""
    _install(monkeypatch, _windows(SOURCE, 137, 0))
    for huge in (MAX_LINE_NUMBER + 1, 10 ** 4000):
        with pytest.raises(InvalidOperatorArguments) as excinfo:
            read_record_text(None, SESSION, "doc:1", lines=[[1, huge]], max_bytes=4096)
        assert f"from 1 to {MAX_LINE_NUMBER}" in str(excinfo.value)


def test_line_numbers_print_without_digit_grouping() -> None:
    """The status line's ranges can be copied back into `lines`."""
    page = {
        "status": "ok", "node_id": "doc:1", "subtype": "document",
        "order_basis": "chunk_ordinal", "coverage": "complete", "complete": False,
        "truncated": True, "continuation_token": "TOK.sig", "generation_id": 7,
        "lines": {"basis": "source", "total": 2961,
                  "requested": [[1278, 1302], [1555, 1578]],
                  "served": [[1278, 1302], [1555, 1560]], "past_end": True,
                  "cut": {"last_line": 1560, "next_line": 1561}},
        "items": [{"chunk_id": "chunk:9", "char_offset": 0, "lines": [1278, 1302],
                   "text": "x", "provenance": {"trust": "trusted"}}],
    }
    text = result_text.render(page, tool="inspect", kind="text")
    assert "source lines 1278-1302, 1555-1560 of 2961" in text.split("\n", 1)[0]
    assert "! lines: served through line 1560; next continues at line 1561" in text
    assert "! lines: the text ends at line 2961" in text
    assert " · lines 1278-1302" in text
    assert "1,278" not in text and "2,961" not in text


@pytest.mark.parametrize("bad", [
    [[1, MAX_LINE_NUMBER + 1]],
    [[1, None], [5, 6]],
    [[None, 5]],
    [],
    [[2, 1]],
    [[1, 5], [5, 9]],
    [[10, 12], [1, 3]],
    [[0, 3]],
    [[1, 2, 3]],
    [[1]],
    "1-5",
    [[True, 2]],
    [[1.0, 2]],
    [["1", "2"]],
    [3, 4],
    [[i * 2 + 1, i * 2 + 1] for i in range(MAX_LINE_RANGES + 1)],
], ids=[
    "over-the-bound", "after-an-open-end", "open-start", "none", "reversed", "overlapping", "descending", "line-zero", "three-numbers",
    "one-number", "a-string", "a-boolean", "a-float", "digit-strings", "flat", "too-many",
])
def test_a_bad_lines_value_is_refused_with_the_accepted_form(bad: Any) -> None:
    with pytest.raises(InvalidOperatorArguments) as excinfo:
        parse_lines(bad)
    message = str(excinfo.value)
    assert "1 to 20 ranges [[first, last], ...]" in message
    assert "first <= last" in message and "ascending" in message and "not overlapping" in message
    assert excinfo.value.data["invalid_fields"] == ["target.lines"]
    assert '"lines":[[1278,1302],[1555,1578]]' in excinfo.value.fix_hint


def test_lines_beside_at_is_refused_and_names_both_fields() -> None:
    at = {"chunk": "chunk:0", "offset": 3}
    with pytest.raises(InvalidOperatorArguments) as excinfo:
        parse_line_target([[1, 2]], at)
    assert "lines and at cannot be combined" in str(excinfo.value)
    assert excinfo.value.data["invalid_fields"] == ["target.lines", "target.at"]
    # Failure case: either alone is accepted.
    assert parse_line_target([[1, 2]], None) == ((1, 2),)
    assert parse_line_target(None, at) is None


def test_a_batch_item_with_bad_lines_refuses_the_call_before_any_read() -> None:
    """The item's own path is named, and no source is prepared (the fake
    connection would fail the first read)."""
    items = [
        {"key": "good", "node_id": "doc:1", "lines": [[1, 2]]},
        {"key": "bad", "node_id": "doc:1", "lines": [[5, 1]]},
    ]
    with pytest.raises(InvalidOperatorArguments) as excinfo:
        read_text_batch(None, SESSION, items, max_bytes=8192, measure=lambda page: 0)
    assert excinfo.value.data["invalid_fields"] == ["target.items[1].lines"]
    both = [{"key": "k", "node_id": "doc:1", "lines": [[1, 2]],
             "at": {"chunk": "chunk:0", "offset": 0}}]
    with pytest.raises(InvalidOperatorArguments) as excinfo:
        read_text_batch(None, SESSION, both, max_bytes=8192, measure=lambda page: 0)
    assert excinfo.value.data["invalid_fields"] == [
        "target.items[0].lines", "target.items[0].at",
    ]


def _batch_source(monkeypatch: pytest.MonkeyPatch, text: str) -> None:
    """Serve `text`, cut into mid-line chunks, to every batch item."""
    from okg.substrate.mcp import text_batch
    from okg.substrate.mcp.record_text import PreparedRecordText, asked_lines

    def prepare(conn: Any, session: Any, node_id: str, *, lines: Any = None, **_kw: Any) -> Any:
        segments, basis, coverage = _prepared(_windows(text, 1333, 0))
        ranges = parse_lines(lines) if lines is not None else None
        payload = {"subtype": "document", "coverage": coverage, "order_basis": basis,
                   "lines": {**line_basis(segments, coverage, basis),
                             "total": line_count(segments),
                             **({"requested": asked_lines(ranges)} if ranges else {})}}
        return PreparedRecordText(node_id, payload, tuple(segments), {}, {}, (0, 0), None, ranges)

    monkeypatch.setattr(text_batch, "prepare_record_text", prepare)
    monkeypatch.setattr(
        text_batch, "continuation_binding",
        lambda conn, session, **kw: {**kw, "generation": session.generation_id},
    )
    monkeypatch.setattr(text_batch, "classify_sources", lambda *a, **k: {"fixture": "trusted"})


def _json_size(value: Any) -> int:
    import json

    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())


def test_every_page_of_a_line_batch_serves_more_text_until_it_ends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two line items sharing the smallest page: every page serves at least
    one more character of some item, no character twice, and the read ends
    with exactly the asked lines."""
    body = "".join(f"line {i:04d} " + "x" * 30 + "\n" for i in range(1, 401))
    _batch_source(monkeypatch, body)
    items = [
        {"key": "a", "node_id": "doc:1", "lines": [[3, 180]]},
        {"key": "b", "node_id": "doc:2", "lines": [[200, 260], [300, None]]},
    ]
    expected = _lines(body)
    assembled = {"a": "", "b": ""}
    token, pages = None, 0
    while True:
        pages += 1
        page = read_text_batch(None, SESSION, items, max_bytes=4096, measure=_json_size,
                               continuation_token=token)
        assert _json_size(page) <= 4096
        served = 0
        for entry in page["items"]:
            for row in entry["segments"]:
                assert row["lines"][0] <= row["lines"][1]
                served += len(row["text"])
                assembled[entry["key"]] += row["text"]
        assert served > 0, f"page {pages} served no text and still continued"
        token = page.get("continuation_token")
        if token is None:
            break
        assert pages < 400
    assert pages > 3, "the items must not fit one page for this test to mean anything"
    assert assembled == {
        "a": "".join(expected[2:180]),
        "b": "".join(expected[199:260]) + "".join(expected[299:]),
    }


def test_a_line_batch_page_with_no_room_for_text_refuses_with_the_size_needed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A page whose frame fits but not one character of any item never
    comes back empty with a token: it refuses and names a size that works."""
    from okg.substrate.mcp import text_batch

    body = "".join(f"line {i:04d} " + "x" * 30 + "\n" for i in range(1, 401))
    _batch_source(monkeypatch, body)
    items = [{"key": "a", "node_id": "doc:1", "lines": [[3, 180]]}]

    def no_room(value: Any) -> int:
        has_text = any(entry["segments"] for entry in value["items"])
        return _json_size(value) + (5000 if has_text else 0)

    with pytest.raises(text_batch.BatchBudgetTooSmall) as caught:
        read_text_batch(None, SESSION, items, max_bytes=4096, measure=no_room)
    needed = caught.value.data["required_budget_bytes"]
    assert needed > 4096
    # The stated size serves text: the probe sized the piece a line item
    # really gets, its `lines` included.
    page = read_text_batch(None, SESSION, items, max_bytes=needed, measure=no_room)
    assert page["items"][0]["segments"], page["items"][0]
    assert page["items"][0]["segments"][0]["lines"][0] == 3


def test_a_textless_line_item_in_a_batch_keeps_its_line_facts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """As the single read says it: no lines exist, the ranges are past the end."""
    _install(monkeypatch, [])
    from okg.substrate.mcp import text_batch

    monkeypatch.setattr(
        text_batch, "continuation_binding",
        lambda conn, session, **kw: {**kw, "generation": session.generation_id},
    )
    monkeypatch.setattr(text_batch, "classify_sources", lambda *a, **k: {})
    page = read_text_batch(
        None, SESSION, [{"key": "k", "node_id": "doc:1", "lines": [[2, 5]]}],
        max_bytes=8192, measure=_json_size,
    )
    (entry,) = page["items"]
    assert entry["state"] == "delivered" and entry["segments"] == []
    assert entry["requested_extent"]["kind"] == "lines"
    assert entry["lines"] == {
        "basis": "shown_text", "total": 0, "requested": [[2, 5]], "served": [],
        "past_end": True,
    }
    assert page["complete"] is True and "continuation_token" not in page


# --------------------------------------------------------------------------- #
# cutting a prepared record to ranges
# --------------------------------------------------------------------------- #


def test_a_range_inside_one_segment_is_exactly_those_lines() -> None:
    segments, _basis, _coverage = _prepared(
        [_seg("chunk:0", SOURCE, ordinal=0, total=1, offset=0, end=len(SOURCE),
              doc_length=len(SOURCE))]
    )
    pieces = _cut(segments, [(3, 5)])
    assert pieces == [("".join(_lines(SOURCE)[2:5]), (3, 5))]


def test_ranges_across_segments_rejoin_to_the_files_lines() -> None:
    """Every range of a file cut into mid-line chunks is the file's lines,
    and each piece says the lines it holds."""
    segments, _basis, _coverage = _prepared(_windows(SOURCE, 137, 0))
    expected = _lines(SOURCE)
    for first, last in [(1, 1), (1, 40), (6, 7), (11, 29), (40, 40), (39, 40)]:
        pieces = _cut(segments, [(first, last)])
        assert "".join(text for text, _held in pieces) == "".join(expected[first - 1:last])
        assert pieces[0][1][0] == first and pieces[-1][1][1] == last
        for _text, (held_first, held_last) in pieces:
            assert first <= held_first <= held_last <= last


def test_ranges_across_a_trimmed_overlap_serve_no_line_twice() -> None:
    """Window chunks repeat 60 characters; the repeat is trimmed before lines
    are counted, so a range over the seam is the file's lines once."""
    segments, _basis, coverage = _prepared(_windows(SOURCE, 200, 60))
    assert coverage == "complete" and any(s.trimmed_chars for s in segments)
    expected = _lines(SOURCE)
    for first, last in [(1, 40), (8, 12), (17, 31)]:
        joined = "".join(text for text, _held in _cut(segments, [(first, last)]))
        assert joined == "".join(expected[first - 1:last])


def test_several_ranges_are_cut_in_order_and_labelled_by_range() -> None:
    segments, _basis, _coverage = _prepared(_windows(SOURCE, 137, 0))
    spans = line_spans(segments, [(2, 3), (20, 22), (38, 40)])
    assert sorted({span.range for span in spans}) == [0, 1, 2]
    assert [span.range for span in spans] == sorted(span.range for span in spans)
    expected = _lines(SOURCE)
    for number, (first, last) in enumerate([(2, 3), (20, 22), (38, 40)]):
        text = "".join(
            segments[s.segment].text[s.start:s.end] for s in spans if s.range == number
        )
        assert text == "".join(expected[first - 1:last])


def test_a_range_past_the_end_gives_what_exists_and_one_wholly_past_gives_nothing() -> None:
    segments, _basis, _coverage = _prepared(_windows(SOURCE, 137, 0))
    pieces = _cut(segments, [(39, 500)])
    assert "".join(text for text, _held in pieces) == "".join(_lines(SOURCE)[38:])
    assert pieces[-1][1][1] == 40
    assert line_spans(segments, [(41, 60)]) == []


def test_a_cursor_resumes_inside_a_range_and_drops_what_was_served() -> None:
    segments, _basis, _coverage = _prepared(_windows(SOURCE, 137, 0))
    spans = line_spans(segments, [(2, 9), (30, 33)])
    assert spans_from(spans, (0, 0)) == spans
    first = spans[0]
    rest = spans_from(spans, (first.segment, first.start + 5))
    assert (rest[0].segment, rest[0].start, rest[0].end) == (
        first.segment, first.start + 5, first.end,
    )
    assert rest[1:] == spans[1:]
    assert spans_from(spans, (first.segment, first.end)) == spans[1:]
    assert spans_from(spans, (len(segments), 0)) == []


def test_a_match_in_a_chunk_maps_to_its_record_line_through_trims() -> None:
    """Every character of every window chunk, the trimmed overlap included,
    is on the line the file has it on; an unknown chunk or another text is
    no line at all."""
    raw = _windows(SOURCE, 200, 60)
    texts = {s.chunk_id: s.text for s in raw}
    offsets = {s.chunk_id: s.char_offset for s in raw}
    segments, _basis, _coverage = _prepared(raw)
    for chunk_id, text in texts.items():
        for offset in range(0, len(text), 7):
            assert chunk_offset_line(segments, chunk_id, text, offset) == (
                SOURCE.count("\n", 0, offsets[chunk_id] + offset) + 1
            )
    some = next(iter(texts))
    assert chunk_offset_line(segments, "chunk:absent", texts[some], 0) is None
    assert chunk_offset_line(segments, some, "another text entirely", 0) is None
    assert chunk_offset_line(segments, some, texts[some], len(texts[some])) is None


def test_match_lines_skip_other_passage_types_and_stop_at_the_work_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A record is prepared once however many matches it holds; a passage of
    another type, a record the reader cannot read and a record past the
    page's work bound get no record line, and none is prepared for them."""
    raw = _windows(SOURCE, 137, 0)
    texts = [s.text for s in raw]
    prepared: list[str] = []

    def fake_segments(conn: Any, session: Any, node_id: str, **_kw: Any) -> Any:
        prepared.append(node_id)
        if node_id in ("doc:hidden", "doc:huge"):
            # Not readable, or over the size cap: no segments.
            return None
        segments, _basis, _coverage = _prepared(_windows(SOURCE, 137, 0))
        return tuple(segments)

    monkeypatch.setattr(record_text, "_match_record_segments", fake_segments)
    monkeypatch.setattr(record_text, "_read_filter_active", lambda session: False)
    monkeypatch.setattr(record_text, "assert_session_matches", lambda conn, session: None)
    places = [
        MatchPlace("doc:hidden", raw[2].chunk_id, "document_chunk", texts[2], 0),
        MatchPlace("doc:huge", raw[2].chunk_id, "document_chunk", texts[2], 0),
        MatchPlace("doc:1", raw[2].chunk_id, "document_chunk", texts[2], 0),
        MatchPlace("doc:1", raw[2].chunk_id, "doc_chunk", texts[2], 0),
        MatchPlace("doc:1", raw[3].chunk_id, "document_chunk", texts[3], 5),
        MatchPlace("doc:2", raw[2].chunk_id, "document_chunk", texts[2], 0),
    ]
    lines = record_match_lines(None, SESSION, places, segment_budget=len(raw))
    assert lines == [
        None, None, SOURCE.count("\n", 0, 2 * 137) + 1, None,
        SOURCE.count("\n", 0, 3 * 137 + 5) + 1, None,
    ]
    # doc:2 came after the bound was spent: it was never read.
    assert prepared == ["doc:hidden", "doc:huge", "doc:1"]
    # Failure case: with room left, the same place in doc:2 gets its line.
    assert record_match_lines(None, SESSION, places[-1:])[0] == lines[2]
    # The byte bound stops further records the same way.
    prepared.clear()
    by_bytes = record_match_lines(
        None, SESSION, [places[2], places[5]], byte_budget=len(SOURCE),
    )
    assert by_bytes == [lines[2], None] and prepared == ["doc:1"]
    # A match in the record's own text: its line has a record line only
    # when the record's text holds that same whole line exactly once.
    own = "a title\n" + _lines(SOURCE)[11] + "repeated\nrepeated\n"
    twice = "repeated\n" * 2
    assert record_match_lines(None, SESSION, [
        MatchPlace("doc:1", None, None, own, own.index("line 12") + 3),
        MatchPlace("doc:1", None, None, own, 2),
    ]) == [12, None]
    segments, _basis, _coverage = _prepared(
        [_seg("chunk:0", twice + "once\n", ordinal=0, total=1)]
    )
    assert own_text_line(segments, own, own.index("repeated")) is None
    assert own_text_line(segments, "x\nonce\n", 3) == 3
    # No record is read once the time bound has passed.
    prepared.clear()
    assert record_match_lines(None, SESSION, places[2:3], stop_at=0.0) == [None]
    assert prepared == []


def test_an_own_text_line_the_own_text_repeats_has_no_record_line() -> None:
    """`x = 1` is on lines 5 and 50 of the own text and once in the chunks.
    A match on own line 50 must not be reported as the chunks' one line: it
    could be either place, so it has no record line."""
    own_lines = [f"own {i}" for i in range(1, 61)]
    own_lines[4] = own_lines[49] = "x = 1"
    own = "\n".join(own_lines)
    chunk_text = "".join(f"chunk {i}\n" for i in range(1, 9)) + "x = 1\nlast\n"
    segments, _basis, _coverage = _prepared(
        [_seg("chunk:0", chunk_text, ordinal=0, total=1, offset=0, end=len(chunk_text),
              doc_length=len(chunk_text))]
    )
    second = own.rindex("x = 1")
    assert own.count("\n", 0, second) + 1 == 50
    assert own_text_line(segments, own, second) is None
    assert own_text_line(segments, own, own.index("x = 1")) is None
    # Failure case: with one occurrence in the own text it is the chunks' line 9.
    own_lines[4] = "own 5"
    once = "\n".join(own_lines)
    assert own_text_line(segments, once, once.index("x = 1")) == 9


def test_a_record_over_the_size_cap_is_not_loaded_and_the_cache_is_never_used(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The size read decides before any text is loaded, and the read that
    follows neither reads nor writes the prepared-text cache."""
    sizes = {"chunks": 3, "bytes": 900}
    loaded: list[str] = []

    class _Graph:
        def query(self, sql: str, params: dict) -> list[dict]:
            assert "octet_length" in sql and params["node_id"].startswith("doc:")
            return [dict(sizes)]

    def read_document(conn: Any, session: Any, node_id: str, **_kw: Any) -> Any:
        loaded.append(node_id)
        segments, _basis, _coverage = _prepared(_windows(SOURCE, 137, 0))
        return SimpleNamespace(is_chunk=False, segments=tuple(segments)), None

    def no_cache(*_args: Any, **_kw: Any) -> Any:
        raise AssertionError("a search read used the prepared-text cache")

    monkeypatch.setattr(record_text, "_checkout", lambda conn, session: _Graph())
    monkeypatch.setattr(record_text, "_read_document", read_document)
    for name in ("_text_cache_key", "_text_cache_get", "_text_cache_put"):
        monkeypatch.setattr(record_text, name, no_cache)
    assert len(record_text._match_record_segments(None, SESSION, "doc:1")) == 6
    assert loaded == ["doc:1"]
    for over in ({"chunks": record_text.MATCH_LINE_RECORD_CHUNKS + 1, "bytes": 10},
                 {"chunks": 2, "bytes": record_text.MATCH_LINE_RECORD_BYTES + 1}):
        sizes.update(over)
        assert record_text._match_record_segments(None, SESSION, "doc:big") is None
    assert loaded == ["doc:1"], "a record over the cap was loaded"


class _TimedConn:
    """A connection as the grep timeout wrapper uses one: its transaction
    status, the `statement_timeout` values it is given, in order, and
    whether it was closed. `stuck` makes every attempt to put the session's
    own value back fail."""

    def __init__(self, status: Any, *, stuck: bool = False) -> None:
        self.info = SimpleNamespace(transaction_status=status)
        self.timeouts: list[str] = []
        self.stuck = stuck
        self.attempts = 0
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def cursor(self, **_kw: Any) -> Any:
        conn = self

        class _Cursor:
            def __enter__(self) -> Any:
                return self

            def __exit__(self, *_exc: Any) -> None:
                return None

            def execute(self, sql: str, params: tuple = ()) -> None:
                import psycopg

                if "set_config" not in sql:
                    return
                if conn.stuck and params[0] == "0":
                    conn.attempts += 1
                    raise psycopg.errors.QueryCanceled("canceling statement")
                conn.timeouts.append(params[0])

            def fetchone(self) -> dict:
                return {"v": "0"}

        return _Cursor()


def test_grep_serves_record_lines_under_its_timeout_and_a_failure_is_never_silent(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    """The ordinary path returns the record lines, with the grep timeout set
    for the reads and the session's own put back. A defect in the read is
    logged with its traceback, not swallowed, and the page still gets an
    answer (no record line, so each window says its basis). A timeout is
    quiet. Inside a caller's transaction, or with under half a second of a
    caller's deadline left, no record is read."""
    import time

    import psycopg

    from okg.substrate.mcp import grep_search

    status = psycopg.pq.TransactionStatus
    places = [MatchPlace("doc:1", "chunk:002", "document_chunk", "text", 0)]
    calls: list[Any] = []

    def served(conn: Any, session: Any, got: Any, *, stop_at: float) -> list:
        calls.append(stop_at)
        return [77]

    monkeypatch.setattr(grep_search, "record_match_lines", served)
    conn = _TimedConn(status.IDLE)
    assert grep_search._record_lines_in_time(conn, SESSION, places) == [77]
    assert conn.timeouts == [f"{grep_search.STATEMENT_TIMEOUT_MS}ms", "0"] and len(calls) == 1
    assert conn.closed is False

    def broken(*_args: Any, **_kw: Any) -> list:
        raise RuntimeError("a defect in the record read")

    monkeypatch.setattr(grep_search, "record_match_lines", broken)
    conn = _TimedConn(status.IDLE)
    with caplog.at_level("ERROR", logger=grep_search.__name__):
        assert grep_search._record_lines_in_time(conn, SESSION, places) == [None]
    assert "record lines" in caplog.text and "a defect in the record read" in caplog.text
    assert conn.timeouts[-1] == "0", "the session's timeout was not put back"

    def timed_out(*_args: Any, **_kw: Any) -> list:
        raise psycopg.errors.QueryCanceled("statement timeout")

    caplog.clear()
    monkeypatch.setattr(grep_search, "record_match_lines", timed_out)
    conn = _TimedConn(status.IDLE)
    with caplog.at_level("ERROR", logger=grep_search.__name__):
        assert grep_search._record_lines_in_time(conn, SESSION, places) == [None]
    assert caplog.text == "" and conn.timeouts[-1] == "0"

    monkeypatch.setattr(grep_search, "record_match_lines", served)
    calls.clear()
    busy = _TimedConn(status.INTRANS)
    assert grep_search._record_lines_in_time(busy, SESSION, places) == [None]
    assert calls == [] and busy.timeouts == []
    short = _TimedConn(status.IDLE)
    with grep_search.deadline(time.monotonic() + 0.2):
        assert grep_search._record_lines_in_time(short, SESSION, places) == [None]
    assert calls == [] and short.timeouts == []


def test_a_timeout_that_cannot_be_lifted_closes_the_connection_and_fails_the_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guard: if the session's own timeout cannot be put back and read
    back after three tries, the connection is closed (its pool drops it on
    return) and the call ends with a server fault, so no request ever runs
    on a session left capped. It is not a refusal the caller could fix."""
    import psycopg

    from okg.substrate.errors import SubstrateError
    from okg.substrate.mcp import grep_search

    places = [MatchPlace("doc:1", "chunk:002", "document_chunk", "text", 0)]
    monkeypatch.setattr(grep_search, "record_match_lines", lambda *a, **k: [77])
    conn = _TimedConn(psycopg.pq.TransactionStatus.IDLE, stuck=True)
    with pytest.raises(grep_search.StatementTimeoutNotRestored) as caught:
        grep_search._record_lines_in_time(conn, SESSION, places)
    assert conn.attempts == 3 and conn.closed is True
    assert conn.timeouts == [f"{grep_search.STATEMENT_TIMEOUT_MS}ms"]
    assert not isinstance(caught.value, SubstrateError)

    # The same guard when the reads themselves failed: still closed, and
    # the guard's error is the one raised.
    def broken(*_args: Any, **_kw: Any) -> list:
        raise RuntimeError("a defect in the record read")

    monkeypatch.setattr(grep_search, "record_match_lines", broken)
    conn = _TimedConn(psycopg.pq.TransactionStatus.IDLE, stuck=True)
    with pytest.raises(grep_search.StatementTimeoutNotRestored):
        grep_search._record_lines_in_time(conn, SESSION, places)
    assert conn.closed is True

    # Failure case: a restore that is confirmed closes nothing and raises nothing.
    monkeypatch.setattr(grep_search, "record_match_lines", lambda *a, **k: [77])
    fine = _TimedConn(psycopg.pq.TransactionStatus.IDLE)
    assert grep_search._record_lines_in_time(fine, SESSION, places) == [77]
    assert fine.closed is False and fine.timeouts[-1] == "0"


def test_a_timeout_keeps_the_lines_already_placed(monkeypatch: pytest.MonkeyPatch) -> None:
    """The second record's read is cancelled by the statement timeout,
    raised wrapped as a read layer would: the first record's line stands,
    the second and third get none, and the third is never read."""
    import psycopg

    raw = _windows(SOURCE, 137, 0)
    read: list[str] = []

    def segments_of(conn: Any, session: Any, node_id: str, **_kw: Any) -> Any:
        read.append(node_id)
        if node_id == "doc:2":
            try:
                raise psycopg.errors.QueryCanceled("canceling statement")
            except psycopg.errors.QueryCanceled as exc:
                raise RuntimeError("the read failed") from exc
        segments, _basis, _coverage = _prepared(_windows(SOURCE, 137, 0))
        return tuple(segments)

    monkeypatch.setattr(record_text, "_match_record_segments", segments_of)
    monkeypatch.setattr(record_text, "_read_filter_active", lambda session: False)
    monkeypatch.setattr(record_text, "assert_session_matches", lambda conn, session: None)
    places = [
        MatchPlace(doc, raw[2].chunk_id, "document_chunk", raw[2].text, 0)
        for doc in ("doc:1", "doc:2", "doc:3")
    ]
    lines = record_match_lines(None, SESSION, places)
    assert lines == [SOURCE.count("\n", 0, 2 * 137) + 1, None, None]
    assert read == ["doc:1", "doc:2"]
    assert record_text.is_statement_timeout(RuntimeError("other")) is False

    # Failure case: an error that is not a timeout is not taken for one.
    def defect(conn: Any, session: Any, node_id: str, **_kw: Any) -> Any:
        raise RuntimeError("a defect")

    monkeypatch.setattr(record_text, "_match_record_segments", defect)
    with pytest.raises(RuntimeError, match="a defect"):
        record_match_lines(None, SESSION, places)


def test_under_access_rules_no_record_is_prepared_for_match_lines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With audit or enforce on, a page of matches reads no whole record
    and so logs no decision its reader did not ask for: every place gets
    no record line."""
    def refuse(*_args: Any, **_kw: Any) -> Any:
        raise AssertionError("a record was prepared under access rules")

    monkeypatch.setattr(record_text, "_match_record_segments", refuse)
    monkeypatch.setattr(record_text, "_read_filter_active", lambda session: True)
    raw = _windows(SOURCE, 137, 0)
    places = [
        MatchPlace("doc:1", raw[2].chunk_id, "document_chunk", raw[2].text, 0),
        MatchPlace("doc:1", None, None, "own text", 0),
    ]
    assert record_match_lines(None, SESSION, places) == [None, None]


def test_a_list_row_says_when_its_line_is_counted_inside_the_passage() -> None:
    def row(node_id: str, **window: Any) -> dict[str, Any]:
        return {"node_id": node_id, "subtype": "doc", "excerpt": None, "matching_passages": 1,
                "provenance": {"trust": "trusted"}, "match_windows": [
                    {"char_start": 0, "char_end": 3, "line": 5, "excerpt": "abc",
                     "passage_id": "c:1", **window},
                ]}

    page = grep_list_projection({
        "method": "grep", "total_count": 2, "total_count_basis": "exact", "next_offset": None,
        "page_basis": "top_of_all_visible_matches", "result_unit": "record",
        "results": [row("doc:1", line_basis="passage"), row("doc:2")],
    })
    placed, unplaced = page["results"][1], page["results"][0]
    assert unplaced["first_match"] == {
        "chunk": "c:1", "line": 5, "text": "abc", "line_basis": "passage",
    }
    assert placed["first_match"] == {"chunk": "c:1", "line": 5, "text": "abc"}
    text = result_text.render({**page, "operator_name": "search"}, tool="search")
    first, second = [line for line in text.split("\n") if line.startswith("[")]
    assert "first c:1 line 5" in first and "line counted in the passage" in first
    assert "first c:1 line 5" in second and "line counted in the passage" not in second


# --------------------------------------------------------------------------- #
# the pager: line reads and the span a page reports
# --------------------------------------------------------------------------- #


def _read_lines(
    ranges: list[list[int]], max_bytes: int,
) -> list[dict[str, Any]]:
    pages, token = [], None
    for _ in range(200):
        page = read_record_text(
            None, SESSION, "doc:1", lines=ranges, continuation_token=token,
            max_bytes=max_bytes,
        )
        pages.append(page)
        assert public_call_tool_result_bytes(page, tool="inspect", kind="text") <= (
            max_bytes - record_text._DEFAULT_RESERVE_BYTES
        )
        if page["complete"]:
            assert "continuation_token" not in page
            return pages
        token = page["continuation_token"]
    raise AssertionError("the line read did not terminate")


def test_a_line_read_returns_exactly_the_asked_lines(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, _windows(SOURCE, 137, 0))
    (page,) = _read_lines([[3, 5], [20, 21]], 65536)
    expected = _lines(SOURCE)
    assert "".join(item["text"] for item in page["items"]) == (
        "".join(expected[2:5]) + "".join(expected[19:21])
    )
    assert page["lines"] == {
        "basis": "source", "total": 40,
        "requested": [[3, 5], [20, 21]], "served": [[3, 5], [20, 21]],
    }
    assert all(item["lines"][0] <= item["lines"][1] for item in page["items"])
    assert page["complete"] is True and page["truncated"] is False
    text = result_text.render(page, tool="inspect", kind="text")
    assert "source lines 3-5, 20-21 of 40" in text.split("\n", 1)[0]
    # The block is said by the status line, not printed again as a field.
    assert "! lines:" not in text and "requested" not in text
    assert " · lines 3-5" in text


def test_a_line_read_cut_by_the_size_limit_says_where_and_next_reassembles_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = "".join(f"line {i:04d} " + "x" * 60 + "\n" for i in range(1, 301))
    _install(monkeypatch, _windows(body, 1500, 200))
    pages = _read_lines([[10, 120], [200, 260]], 4096)
    assert len(pages) > 3, "the fixture must page for this test to mean anything"
    expected = _lines(body)
    assert "".join(item["text"] for page in pages for item in page["items"]) == (
        "".join(expected[9:120]) + "".join(expected[199:260])
    )
    for page, following in zip(pages, pages[1:]):
        cut = page["lines"]["cut"]
        assert cut["last_line"] == page["lines"]["served"][-1][1]
        # `next` continues on the line the cut names.
        assert following["lines"]["served"][0][0] == cut["next_line"]
        assert cut["next_line"] in (cut["last_line"], cut["last_line"] + 1) or (
            cut["last_line"] == 120 and cut["next_line"] == 200
        )
        assert page["text_left"]["chars"] > 0
        text = result_text.render(page, tool="inspect", kind="text")
        assert (
            f"! lines: served through line {cut['last_line']:,}; "
            f"next continues at line {cut['next_line']:,}"
        ) in text
    assert "cut" not in pages[-1]["lines"]
    # What a cut line read says is left is the rest of its lines, not of the
    # record: after the last cut page nothing but the final page's text.
    last_left = pages[-2]["text_left"]["chars"]
    assert last_left == sum(len(item["text"]) for item in pages[-1]["items"])


def test_a_line_read_past_the_end_says_the_last_line(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, _windows(SOURCE, 137, 0))
    (page,) = _read_lines([[39, 90]], 65536)
    assert "".join(item["text"] for item in page["items"]) == "".join(_lines(SOURCE)[38:])
    assert page["lines"]["served"] == [[39, 40]] and page["lines"]["past_end"] is True
    assert page["lines"]["total"] == 40
    assert "! lines: the text ends at line 40" in result_text.render(
        page, tool="inspect", kind="text",
    )
    (beyond,) = _read_lines([[41, 90]], 65536)
    assert beyond["items"] == [] and beyond["complete"] is True
    assert beyond["lines"]["served"] == [] and beyond["lines"]["past_end"] is True
    assert "source lines none of 40" in result_text.render(
        beyond, tool="inspect", kind="text",
    ).split("\n", 1)[0]


def test_a_line_token_continues_those_lines_only(monkeypatch: pytest.MonkeyPatch) -> None:
    from okg.substrate.mcp.continuation import ContinuationTokenInvalid, open_continuation

    body = "".join(f"line {i:04d} " + "x" * 60 + "\n" for i in range(1, 301))
    _install(monkeypatch, _windows(body, 1500, 200))
    first = read_record_text(None, SESSION, "doc:1", lines=[[10, 200]], max_bytes=4096)
    token = first["continuation_token"]
    assert open_continuation(token)["binding"]["projection"]["lines"] == [[10, 200]]
    # Failure cases: the token with other ranges, or with none, is refused.
    with pytest.raises(ContinuationTokenInvalid):
        read_record_text(None, SESSION, "doc:1", lines=[[10, 201]],
                         continuation_token=token, max_bytes=4096)
    with pytest.raises(ContinuationTokenInvalid):
        read_record_text(None, SESSION, "doc:1", continuation_token=token, max_bytes=4096)


def test_a_page_that_is_not_a_line_read_reports_the_lines_it_covers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each page's `first`..`last` fed back as `lines` returns that page's
    text, so an agent goes from a page to a range read."""
    body = "".join(f"line {i:04d} of the ticket body.\n" for i in range(1, 301))
    _install(monkeypatch, _windows(body, 1500, 200))
    pages, token = [], None
    while True:
        page = read_record_text(None, SESSION, "doc:1", continuation_token=token, max_bytes=4096)
        pages.append(page)
        if page["complete"]:
            break
        token = page["continuation_token"]
    assert len(pages) > 3
    assert pages[0]["lines"]["first"] == 1 and pages[-1]["lines"]["last"] == 300
    expected = _lines(body)
    for page in pages:
        block = page["lines"]
        assert set(block) == {"basis", "total", "first", "last"}
        assert block["basis"] == "source" and block["total"] == 300
        assert all("lines" not in item for item in page["items"])
        page_text = "".join(item["text"] for item in page["items"])
        covered = "".join(expected[block["first"] - 1:block["last"]])
        # A page may start or stop inside a line; its span covers every line
        # it touches and no line it does not.
        assert page_text in covered
        assert page_text not in "".join(expected[block["first"]:block["last"]])
        assert page_text not in "".join(expected[block["first"] - 1:block["last"] - 1])
        head = result_text.render(page, tool="inspect", kind="text").split("\n", 1)[0]
        assert f"source lines {block['first']:,}-{block['last']:,} of 300" in head
    for earlier, later in zip(pages, pages[1:]):
        assert later["lines"]["first"] in (
            earlier["lines"]["last"], earlier["lines"]["last"] + 1,
        )


def test_a_line_read_of_a_record_with_no_text_says_no_lines_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, [])
    page = read_record_text(None, SESSION, "doc:1", lines=[[1, 3]], max_bytes=4096)
    assert page["outcome"]["code"] == "no_text_chunks" and page["items"] == []
    assert page["lines"] == {
        "basis": "shown_text", "total": 0,
        "requested": [[1, 3]], "served": [], "past_end": True,
    }
    text = result_text.render(page, tool="inspect", kind="text")
    assert "shown-text lines none of 0" in text.split("\n", 1)[0]
    # Failure case: a read that asks for no lines says nothing about lines.
    assert "lines" not in read_record_text(None, SESSION, "doc:1", max_bytes=4096)


def test_a_gapped_record_says_so_in_text_and_json(monkeypatch: pytest.MonkeyPatch) -> None:
    whole = _windows(SOURCE, 137, 0)
    _install(monkeypatch, [whole[0], *whole[2:]])
    page = read_record_text(None, SESSION, "doc:1", lines=[[1, 4]], max_bytes=65536)
    assert page["coverage"] == "partial"
    assert page["lines"]["basis"] == "shown_text" and "note" not in page["lines"]
    text = result_text.render(page, tool="inspect", kind="text")
    assert "shown-text lines 1-4 of " in text.split("\n", 1)[0]
    assert "! lines: of the shown text, source has gaps" in text
    # Failure case: the whole record says its lines are the source's.
    _install(monkeypatch, _windows(SOURCE, 137, 0))
    full = read_record_text(None, SESSION, "doc:1", lines=[[1, 4]], max_bytes=65536)
    assert full["lines"]["basis"] == "source"
    assert "! lines:" not in result_text.render(full, tool="inspect", kind="text")
