"""A search page keeps every row by leaving out row fields only JSON carries
(PACT read-passage-rows-3666, D4).

Head-only rows that still carried every JSON row field (handle, location,
citation, ranking fields) did not fit a 12,000-byte JSON page of 20 long
records, so the page was read again with fewer rows. Before any title is
cut, those fields are left out; the row keeps its identity floor, head
line, read call, trust and what the caller selected. Hermetic.
"""
from __future__ import annotations

import json
from typing import Any

from okg.substrate.mcp.response_bounds import (
    HEADS_SHORTENED_KEY,
    _search_heads_at,
    _search_page_at,
    _shortened_search_page,
)
from okg.substrate.mcp.result_text import render


def _row(i: int) -> dict[str, Any]:
    node_id = f"doc:{i:03d}"
    return {
        "node_id": node_id, "subtype": "doc", "score": 1.0 - i / 100,
        "evidence_kind": "document",
        "provenance": {"source": "docs", "trust": "trusted"},
        "head": {"kind": "doc", "id": node_id, "fields": [["path", f"long/{i:03d}.md"]],
                 "title": f"Long document {i:03d}", "citation_url": None},
        "passage": {"text": "word " * 200, "whole": False, "where": "record"},
        "read_more": {"tool": "inspect",
                      "arguments": {"target": {"kind": "text", "node_id": node_id}}},
        "attrs": {"summary": f"Long document {i:03d}", "notes": "x" * 800},
        # Fields the head line repeats or that describe the ranking.
        "handle": {"title": f"Long document {i:03d}", "path": f"long/{i:03d}.md"},
        "location": {"path": f"long/{i:03d}.md", "anchor": "top", "kind": "file"},
        "citation": {"source": "docs", "record": node_id, "revision": "r1"},
        "matching_passages": 3, "rollup_best_chunk_id": f"{node_id}:chunk:0",
        "source_record_id": f"rec-{i}", "source_revision": "r1", "match_kind": "bm25",
        "method": "lexical", "canonical_node_id": node_id,
    }


def _page() -> dict[str, Any]:
    return {"query": "q", "method": "lexical", "offset": 0, "total_count": 60,
            "next_offset": 20, "results": [_row(i) for i in range(20)],
            # Page fields that explain the read.
            "recency": {"annotated": False, "absent_reason": "no row carries a date " * 10},
            "pagination_hint": "call search again with offset=20",
            "pinned_generation_id": "gen:1"}


def _size(page: dict[str, Any]) -> int:
    return len(json.dumps(page, separators=(",", ":")))


def _heads(page: dict[str, Any], *, fields: bool) -> dict[str, Any]:
    return _search_heads_at(
        _search_page_at(page, 0, True), links=True, names=True, title_chars=24,
        fields=fields,
    )


def test_every_row_is_kept_by_leaving_out_json_only_row_fields() -> None:
    page = _page()
    attrs = {"select": ["node_id", "attrs"]}
    floor, whole = _size(_heads(page, fields=True)), _size(_heads(page, fields=False))
    assert floor < whole
    # Between the two: the shortest head lines with every row field do not
    # fit; with the floor fields only they do.
    budget = (floor + whole) // 2
    fitted = _shortened_search_page(page, attrs, budget=budget, measure=_size)
    # Must-fail before: no form of 20 rows fit, so the page was None (and
    # the wrapper reread it at fewer rows).
    assert fitted is not None
    assert len(fitted["results"]) == 20
    assert _size(fitted) <= budget
    said = fitted[HEADS_SHORTENED_KEY]
    assert "handle" in said["row_fields"] and "location" in said["row_fields"]
    assert said["page_fields"] == ["recency", "pagination_hint"]
    assert "recency" not in fitted and fitted["pinned_generation_id"] == "gen:1"
    row = fitted["results"][0]
    assert {"node_id", "subtype", "head", "provenance"} <= set(row)
    assert "handle" not in row and "location" not in row
    # One read call for the page, with each row's id under its target kind.
    assert "read_more" not in row and "read_more" in said["row_fields"]
    assert fitted["read_more"] == {
        "tool": "inspect", "kinds": {"text": [f"doc:{i:03d}" for i in range(20)]},
    }
    text = render(fitted, tool="search")
    assert text.splitlines()[-1] == (
        'read a record: inspect(target={"kind":"text","node_id":"<id>"})'
    ), text
    # The caller's whole attrs stays; values cut to nothing are left out.
    assert row["attrs"] == {}
    assert fitted["passages_shortened_to"] == 0


def test_a_page_that_fits_keeps_every_row_field() -> None:
    """Failure case: nothing is left out of a page that fits whole."""
    page = _page()
    # Over the budget only by its passages: they are shortened, the fields stay.
    budget = _size(page) - 500
    fitted = _shortened_search_page(page, None, budget=budget, measure=_size)
    assert fitted is not None and len(fitted["results"]) == 20
    assert HEADS_SHORTENED_KEY not in fitted
    assert all("handle" in row and "location" in row for row in fitted["results"])
    assert "recency" in fitted and "pagination_hint" in fitted
    assert fitted["passages_shortened_to"] > 0


def test_the_status_line_says_matched_passages_kept_whole() -> None:
    """Must-fail on 728ca7c8531: a page whose other passages were left out
    but whose matched comments were kept whole said "head lines only"."""
    def status(**extra: Any) -> str:
        return render({**_page(), **extra}, tool="search").split("\n", 1)[0]

    assert "other passages left out to fit; 2 matched passages kept whole" in status(
        passages_shortened_to=0, passages_kept_whole=2)
    assert "head lines only" not in status(passages_shortened_to=0, passages_kept_whole=2)
    assert "passages shortened to 120 chars to fit; 1 matched passage kept whole" in status(
        passages_shortened_to=120, passages_kept_whole=1)
    # Failure case: nothing kept whole reads as before.
    assert "head lines only to fit" in status(passages_shortened_to=0)
    assert "kept whole" not in status(passages_shortened_to=120)


def test_the_text_says_nothing_for_json_only_fields() -> None:
    page = {**_page(), "passages_shortened_to": 0,
            HEADS_SHORTENED_KEY: {"row_fields": ["handle"]}}
    status = render(page, tool="search").split("\n", 1)[0]
    assert "head lines shortened" not in status, status
    assert "head lines only to fit" in status, status
