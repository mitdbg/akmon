"""Pure parts of the record-window adapter (PACT read-record-windows).

The producer position contract, the continuity verdict and argument
refusals, without a database. Published behaviour is in
test_record_windows_e2e.py.
"""
from __future__ import annotations

import pytest

from okg.substrate.mcp.grep_query import parse_grep_query
from okg.substrate.mcp.record_window_search import (
    WindowArgumentUnsupported,
    assert_window_query,
    WINDOW_DRIFT_BYTES,
    WINDOW_RESERVE_BYTES,
    continuity_state,
    page_reserve,
    position_authority,
    validate_window_call,
)
from okg.substrate.mcp.record_windows import ReadableOccurrence, build_chains
from okg.substrate.mcp.search import search
from okg.substrate.mcp.search_profiles import ResolvedScope

RECORD = {"node_id": "doc:a", "source": "docs",
          "source_record_id": {"repo": "r", "path": "a.md"}, "source_revision": {"v": 1}}
POSITIONS = {"char_offset": 0, "char_end": 5, "doc_char_length": 9, "chunk_ordinal": 0}


def _edge(**changes):
    edge = {"edge_id": "e1", "source": "docs", "attrs": dict(POSITIONS),
            "source_record_id": {"repo": "r", "path": "a.md", "chunk_id": "chunk:1"},
            "source_revision": {"v": 1}}
    edge.update(changes)
    return edge


def _authority(edge=None, *, raw=None, readable="alpha", stored="alpha",
               producers=frozenset({"docs"}), record=RECORD):
    edge = _edge() if edge is None else edge
    return position_authority(
        record=record, edge=edge, raw_edge_attrs=dict(POSITIONS) if raw is None else raw,
        chunk_id="chunk:1", readable_text=readable, stored_text=stored, producers=producers,
    )


def test_the_producer_contract_proves_a_stream() -> None:
    stream, basis = _authority()
    assert basis == "producer_contract" and stream
    # One stream per producer, source record and revision.
    assert _authority()[0] == stream


@pytest.mark.parametrize("kwargs, basis", [
    ({"producers": frozenset()}, "producer_contract_unknown"),
    ({"edge": _edge(source="other")}, "producer_contract_unknown"),
    ({"record": {**RECORD, "source": "other"}}, "producer_differs_from_record"),
    ({"edge": _edge(source_record_id={"repo": "r", "path": "b.md", "chunk_id": "chunk:1"})},
     "source_record_differs"),
    ({"edge": _edge(source_record_id={"repo": "r", "path": "a.md", "chunk_id": "chunk:2"})},
     "source_record_differs"),
    ({"edge": _edge(source_revision={"v": 2})}, "revision_differs"),
    ({"edge": _edge(attrs={"char_offset": 0, "char_end": 5})}, "positions_not_stored"),
    ({"edge": _edge(attrs={**POSITIONS, "char_offset": True})}, "positions_not_stored"),
    # An accepted override that moves a coordinate never lends it.
    ({"raw": {**POSITIONS, "char_offset": 1}}, "positions_not_stored"),
    # Equal length is not equal text: an override cannot borrow coordinates.
    ({"readable": "bravo"}, "readable_text_differs"),
    # An overridden separator could fake adjacency across a gap.
    ({"edge": _edge(attrs={**POSITIONS, "separator_before": "    "})}, "separator_not_stored"),
])
def test_anything_short_of_the_contract_leaves_positions_unknown(kwargs, basis) -> None:
    assert _authority(**kwargs) == (None, basis)


def test_no_position_edge_is_unknown() -> None:
    assert position_authority(
        record=RECORD, edge=None, raw_edge_attrs=None, chunk_id="chunk:1",
        readable_text="alpha", stored_text="alpha", producers=frozenset({"docs"}),
    ) == (None, "no_position_edge")


def _occurrence(chunk, text, offset=None, *, stream="s", length=20, separator=None):
    return ReadableOccurrence(
        record_id="doc:a", chunk_id=chunk, text=text, edge_id=f"e:{chunk}",
        char_offset=offset, char_end=None if offset is None else offset + len(text),
        doc_char_length=length if offset is not None else None,
        separator_before=separator,
        stream_id=stream if offset is not None else None,
        positions_authoritative=offset is not None,
    )


def test_continuity_verdicts() -> None:
    assert continuity_state([]) == "single"
    assert continuity_state(build_chains([_occurrence("a", "alpha")])) == "single"
    joined = build_chains([_occurrence("a", "alpha ", 0), _occurrence("b", "beta", 6)])
    assert len(joined) == 1 and continuity_state(joined) == "proven"
    gap = build_chains([_occurrence("a", "alpha", 0), _occurrence("b", "beta", 9)])
    assert len(gap) == 2 and continuity_state(gap) == "unknown"
    unknown = build_chains([_occurrence("a", "alpha", 0), _occurrence("b", "beta")])
    assert continuity_state(unknown) == "unknown"
    conflict = build_chains([_occurrence("a", "alpha", 0), _occurrence("b", "XXXX", 3)])
    assert continuity_state(conflict) == "unknown"
    # Two producer streams never adjoin, so each joined stream is proven.
    streams = build_chains([_occurrence("a", "alpha", 0, stream="s1"),
                            _occurrence("b", "beta", 0, stream="s2")])
    assert len(streams) == 2 and continuity_state(streams) == "proven"


def _call(**changes):
    arguments = dict(view="windows", matching=None, context_chars=None, method="grep",
                     record_ids=["doc:a"], query="alpha")
    arguments.update(changes)
    return validate_window_call(**arguments)


def test_defaults_and_legacy() -> None:
    assert _call() == ("record", 200)
    assert _call(matching="passage", context_chars=0) == ("passage", 0)
    assert _call(view=None, record_ids=None) is None


@pytest.mark.parametrize("changes, argument", [
    ({"view": "cards"}, "view"),
    ({"method": "lexical"}, "method"),
    # #3605 (`winscope A`): no record scope reads the grep's own top records,
    # so a missing scope is no longer refused here.
    ({"matching": "chunk"}, "matching"),
    ({"context_chars": -1}, "context_chars"),
    ({"context_chars": 4097}, "context_chars"),
    ({"context_chars": False}, "context_chars"),
    ({"context_chars": 2.0}, "context_chars"),
    ({"offset": 3}, "offset"),
    ({"select": []}, "select"),
    ({"rank_by": "self"}, "rank_by"),
    ({"profile": "all"}, "profile"),
    ({"source_family": "docs"}, "source_family"),
    ({"subtype_filter": "document"}, "subtype_filter"),
    ({"attr_filters": {"a": 1}}, "attr_filters"),
    ({"filter": {"subtype": "document"}}, "filter"),
    ({"max_per_parent": 1}, "max_per_parent"),
    ({"path_prefix": "a"}, "path_prefix"),
    ({"where": {"op": "exists", "field": "path"}}, "where"),
    ({"query": "  "}, "query"),
    ({"view": None, "matching": "record"}, "matching"),
    ({"view": None, "context_chars": 10}, "context_chars"),
])
def test_unsupported_arguments_refuse_by_name(changes, argument) -> None:
    with pytest.raises(WindowArgumentUnsupported) as refused:
        _call(**changes)
    assert refused.value.data["argument"] == argument
    assert refused.value.code == "window_argument_unsupported"


@pytest.mark.parametrize("changes, argument", [
    ({"filter": {"subtype": "document"}}, "subtype_filter"),
    ({"filter": {"attr_filters": {"path": "a.md"}}}, "attr_filters"),
    ({"filter": {"source": "docs"}}, "filter"),
    ({"scope": ResolvedScope(max_per_parent=1)}, "max_per_parent"),
    ({"scope": ResolvedScope(max_per_parent=1, max_per_parent_origin="argument")},
     "max_per_parent"),
    ({"scope": ResolvedScope(source_families=("docs",), source_ids=("docs",))},
     "source_family"),
    ({"scope": ResolvedScope(profile="code", excluded_path_globs=("*.lock",))}, "profile"),
])
def test_the_python_entry_point_refuses_what_windows_would_drop(changes, argument) -> None:
    # Same refusals as the MCP tool, before any connection is used.
    with pytest.raises(WindowArgumentUnsupported) as refused:
        search(None, None, "alpha", method="grep", view="windows",  # type: ignore[arg-type]
               record_scope={"record": "doc:a"}, max_bytes=4096, **changes)
    assert refused.value.data["argument"] == argument


def test_window_queries_need_a_positive_text_term_and_no_path_term() -> None:
    assert_window_query(parse_grep_query('alpha "a phrase" re:/b+/'))
    for query in ("alpha path:docs", "path:docs"):
        with pytest.raises(WindowArgumentUnsupported):
            assert_window_query(parse_grep_query(query))


def test_every_page_keeps_the_full_spare_and_a_retry_keeps_its_cut() -> None:
    """A first page and a retried page both leave `WINDOW_RESERVE_BYTES` over
    the stamps measured in their call; a retry whose stamps grew by up to
    `WINDOW_DRIFT_BYTES` since the token was issued keeps the token's reserve
    (so its cut points), and one that grew more takes a fitting larger one."""
    assert (WINDOW_RESERVE_BYTES, WINDOW_DRIFT_BYTES) == (256, 192)
    budget = 131072
    for issued in range(0, 3000, 7):
        reserve = page_reserve(issued, None, budget)
        assert reserve - issued == WINDOW_RESERVE_BYTES + WINDOW_DRIFT_BYTES
        for grown in range(0, WINDOW_DRIFT_BYTES + 1, 8):
            # Same reserve on the retry: the same cut, with the full spare.
            assert page_reserve(issued + grown, reserve, budget) == reserve
            assert reserve - (issued + grown) >= WINDOW_RESERVE_BYTES
        for later in range(issued, issued + 2000, 13):
            # Whatever the stamps did, a retried page keeps the full spare.
            assert page_reserve(later, reserve, budget) - later >= WINDOW_RESERVE_BYTES
    # Stamps that shrank keep the reserve too; a reserve at or past the
    # budget never carries over.
    assert page_reserve(0, 1024, budget) == 1024
    assert page_reserve(100, 4096, 4096) == 548
    assert page_reserve(-5, None, budget) == 448
