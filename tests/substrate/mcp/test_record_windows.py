"""Finite span proofs for the record-window assembly boundary."""

from itertools import combinations
from types import SimpleNamespace

import pytest

from okg.substrate.mcp.record_windows import (
    MatchEvent,
    ReadableOccurrence,
    WindowContinuityUnknown,
    WindowWorkIncomplete,
    build_chains,
    merge_windows,
    record_matches,
    repeated_head,
    source_coverage,
    supporting_locations,
    window_fragments,
)


def occurrence(text, start=None, *, length=None, chunk="c", **kwargs):
    return ReadableOccurrence(
        "record", chunk, text, "edge:" + chunk,
        char_offset=start,
        char_end=start + len(text) if start is not None else None,
        doc_char_length=length,
        stream_id="canonical:record",
        positions_authoritative=True,
        **kwargs,
    )


def test_verified_overlap_is_trimmed_with_original_coordinates():
    chains = build_chains([
        occurrence("alpha\nbeta", 0, length=15, chunk="one"),
        occurrence("beta gamma", 6, length=16, chunk="two"),
    ])
    # Inconsistent document lengths do not prove adjacency.
    assert len(chains) == 2
    chains = build_chains([
        occurrence("alpha\nbeta", 0, length=16, chunk="one"),
        occurrence("beta gamma", 6, length=16, chunk="two"),
    ])
    assert len(chains) == 1
    assert chains[0].text == "alpha\nbeta gamma"
    assert source_coverage(chains) == "complete"
    pieces = window_fragments(chains[0], 6, 16)
    assert [p["text"] for p in pieces] == ["beta", " gamma"]
    assert [p["range"] for p in pieces] == [[6, 10], [4, 10]]
    assert [p["line_range"] for p in pieces] == [[2, 2], [1, 1]]


def test_only_stored_separator_can_fill_exact_gap():
    left = occurrence("alpha", 0, length=11, chunk="one")
    right = occurrence("beta", 7, length=11, chunk="two")
    assert len(build_chains([left, right])) == 2
    right = occurrence("beta", 7, length=11, chunk="two", separator_before="\n\n")
    chains = build_chains([left, right])
    assert len(chains) == 1
    assert chains[0].text == "alpha\n\nbeta"
    fragments = window_fragments(chains[0], 3, 9)
    assert "".join(f["text"] for f in fragments) == "ha\n\nbe"
    assert fragments[1]["kind"] == "separator"
    assert "range" not in fragments[1]
    assert fragments[1]["edge_id"] == "edge:two"


@pytest.mark.parametrize("right", [
    occurrence("XYZ", 2, length=5),
    occurrence("cde", 2),
    occurrence("cde"),
    occurrence("cde", -1, length=5),
])
def test_unproven_or_conflicting_spans_are_never_concatenated(right):
    chains = build_chains([occurrence("abc", 0, length=5), right])
    assert len(chains) == 2
    assert [chain.text for chain in chains] == ["abc", right.text]
    assert source_coverage(chains) != "complete"


def test_contained_overlap_does_not_repeat_text():
    chains = build_chains([
        occurrence("abcdef", 0, length=8, chunk="one"),
        occurrence("bcd", 1, length=8, chunk="two"),
        occurrence("efgh", 4, length=8, chunk="three"),
    ])
    assert len(chains) == 1
    assert chains[0].text == "abcdefgh"
    assert source_coverage(chains) == "complete"
    locations = supporting_locations(chains[0], 1, 6)
    assert [(location["chunk_id"], location["range"]) for location in locations] == [
        ("one", [1, 6]), ("two", [0, 3]), ("three", [0, 2]),
    ]


def test_equal_length_positions_do_not_join_different_source_streams():
    left = occurrence("alpha", 0, length=9, chunk="one")
    right = ReadableOccurrence(
        "record", "two", "beta", "edge:two", char_offset=5, char_end=9,
        doc_char_length=9, stream_id="unrelated:attachment", positions_authoritative=True,
    )
    chains = build_chains([left, right])
    assert len(chains) == 2
    assert source_coverage(chains) == "unknown"


@pytest.mark.parametrize("stream, authority", [(None, True), ("canonical:record", False)])
def test_position_authority_is_required_even_with_adjacent_spans(stream, authority):
    left = occurrence("alpha", 0, length=9, chunk="one")
    right = ReadableOccurrence(
        "record", "two", "beta", "edge:two", char_offset=5, char_end=9,
        doc_char_length=9, stream_id=stream, positions_authoritative=authority,
    )
    assert len(build_chains([left, right])) == 2


def test_records_cannot_be_joined():
    wrong = ReadableOccurrence("other", "b", "beta", "edge:b")
    with pytest.raises(ValueError, match="membership"):
        build_chains([occurrence("alpha"), wrong])


def term(kind="word", *, negated=False):
    return SimpleNamespace(kind=kind, negated=negated)


def test_record_and_accepts_different_chains_but_negative_in_late_chain_rejects():
    terms = [term(), term(), term(negated=True)]
    assert record_matches(
        terms[:2], [[MatchEvent(0, 0, 5)], [MatchEvent(1, 9, 13)]],
        occurrences_complete=True, continuity_complete=False,
    )
    # An absent negative word could straddle the unproven boundary.
    with pytest.raises(WindowContinuityUnknown):
        record_matches(
            terms, [[MatchEvent(0, 0, 5)], [MatchEvent(1, 9, 13)]],
            occurrences_complete=True, continuity_complete=False,
        )
    assert record_matches(
        terms, [[MatchEvent(0, 0, 5)], [MatchEvent(1, 9, 13)]],
        occurrences_complete=True, continuity_complete=True,
    )
    assert not record_matches(
        terms, [[MatchEvent(0, 0, 5)], [MatchEvent(1, 9, 13), MatchEvent(2, 14, 17)]],
        occurrences_complete=True, continuity_complete=False,
    )


@pytest.mark.parametrize("kind", ["word", "phrase", "near", "regex"])
def test_unknown_cross_boundary_membership_refuses(kind):
    with pytest.raises(WindowContinuityUnknown):
        record_matches(
            [term(kind)], [], occurrences_complete=True, continuity_complete=False,
        )
    with pytest.raises(WindowContinuityUnknown):
        record_matches(
            [term(), term(kind, negated=True)], [[MatchEvent(0, 0, 5)]],
            occurrences_complete=True, continuity_complete=False,
        )
    # A positive presence already proved does not require an absent boundary.
    assert record_matches(
        [term(kind)], [[MatchEvent(0, 0, 5)], []],
        occurrences_complete=True, continuity_complete=False,
    )


def test_incomplete_occurrence_set_never_proves_negative_absence():
    with pytest.raises(WindowWorkIncomplete):
        record_matches(
            [term(), term(negated=True)], [[MatchEvent(0, 0, 1)]],
            occurrences_complete=False, continuity_complete=True,
        )


def test_no_positive_term_refuses():
    with pytest.raises(ValueError, match="positive"):
        record_matches(
            [term(negated=True)], [], occurrences_complete=True, continuity_complete=True,
        )


@pytest.mark.parametrize("bad", [True, False, -1, 4097, 1.5, "20", None])
def test_context_bounds_are_not_coerced(bad):
    with pytest.raises(ValueError, match="context_chars"):
        merge_windows(10, [MatchEvent(0, 2, 3)], context_chars=bad)


def test_adjacent_contexts_merge_without_losing_event_attribution():
    events = [MatchEvent(0, 2, 4), MatchEvent(1, 4, 6), MatchEvent(0, 2, 4)]
    windows = merge_windows(8, events, context_chars=0)
    assert [(w.start, w.end) for w in windows] == [(2, 6)]
    assert windows[0].matches == (events[0], events[1])


def test_finite_interval_merge_preserves_coverage_and_all_logical_events():
    for length in range(1, 7):
        spans = [(a, b) for a in range(length + 1) for b in range(a, length + 1)]
        for pair in combinations(spans, 2):
            events = [MatchEvent(i, a, b) for i, (a, b) in enumerate(pair)]
            for context in range(3):
                windows = merge_windows(length, events, context_chars=context)
                actual = {i for w in windows for i in range(w.start, w.end)}
                expected = {
                    i for e in events
                    for i in range(max(0, e.start - context), min(length, e.end + context))
                }
                assert actual == expected
                assert {e for w in windows for e in w.matches} == set(events)
                assert all(a.end < b.start for a, b in zip(windows, windows[1:]))


def test_every_fragment_page_reconstructs_literal_multibyte_source_once():
    source = "é甲\n🙂beta gamma"
    chain = build_chains([
        occurrence(source[:8], 0, length=len(source), chunk="one"),
        occurrence(source[6:], 6, length=len(source), chunk="two"),
    ])[0]
    fragments = []
    for start in range(0, len(source), 3):
        fragments.extend(window_fragments(chain, start, min(len(source), start + 3)))
    assert "".join(f["text"] for f in fragments) == source
    for fragment in fragments:
        occurrence_text = source[:8] if fragment["chunk_id"] == "one" else source[6:]
        start, end = fragment["range"]
        assert occurrence_text[start:end] == fragment["text"]


def test_repeated_head_is_the_proven_overlap_only():
    """#3802: the head a window chunker repeats counts only when both spans
    are exact, increase and overlap, and the texts agree on it."""
    source = "one\nkeep days ago\ntwo\n"
    first, second = source[:17], source[4:]
    assert repeated_head(first, 0, 17, second, 4, len(source)) == 13
    assert second[:13] == first[4:]
    # Failure cases: each missing proof reads the text whole.
    assert repeated_head(first, 0, 17, second, 17, 17 + len(second)) == 0  # no overlap
    assert repeated_head(first, None, None, second, 4, len(source)) == 0  # no span
    assert repeated_head(first, 0, 16, second, 4, len(source)) == 0  # inexact span
    assert repeated_head(first, 4, 21, second, 0, len(second)) == 0  # decreasing
    other = "X" + second[1:]
    assert repeated_head(first, 0, 17, other, 4, len(source)) == 0  # texts differ
    assert repeated_head(first, True, 17, second, 4, len(source)) == 0  # not an int


def test_zero_width_event_is_retained_at_end_without_fabricated_text():
    event = MatchEvent(0, 3, 3)
    windows = merge_windows(3, [event], context_chars=0)
    assert windows[0].matches == (event,)
    assert (windows[0].start, windows[0].end) == (3, 3)
    chain = build_chains([occurrence("abc")])[0]
    assert window_fragments(chain, 3, 3) == []
