"""grep `within`, id-scoped text conditions and the repeat help, without a database.

PACT read-search-caps-3666. The argument check of `within="record"`, the
id scope a text condition's rest names, how a record row prints (the mode
on the first line, the match count on the head line, each line labelled
with its unit), and the help a search page prints when its rows repeat.
The database half is in `test_grep_record_db.py`.
"""
from __future__ import annotations

from typing import Any

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.grep_record import validate_within
from okg.substrate.mcp.result_text import render
from okg.substrate.mcp.text_match import id_scope

TRUSTED = {"trust": "trusted", "source": "system:fixture"}
QUERY = "re:/paged.attn/ workspace"


def _check(**overrides: Any) -> Any:
    arguments: dict[str, Any] = dict(
        method="grep", query=QUERY, view=None, record_scope=False, rank_by=None,
        completeness=None, retain=False, max_per_parent=None, offset=0,
        continuation_token=None,
    )
    arguments.update(overrides)
    within = arguments.pop("within", "record")
    return validate_within(within, **arguments)


def test_within_defaults_to_unit_and_record_is_a_grep_form() -> None:
    assert _check(within=None) is None
    assert _check(within="unit") == "unit"
    assert _check() == "record"
    with pytest.raises(SubstrateError, match="grep form"):
        _check(method="lexical")


@pytest.mark.parametrize(("overrides", "named"), [
    ({"query": "path:src paged"}, "path: term"),
    ({"query": "-paged"}, "exclusions only"),
    ({"view": "windows"}, "view"),
    ({"view": "list"}, "view"),
    ({"record_scope": True}, "scope"),
    ({"rank_by": "self"}, "rank_by"),
    ({"completeness": "exact"}, "completeness"),
    ({"retain": True}, "retain"),
    ({"max_per_parent": 2}, "max_per_parent"),
])
def test_record_form_refuses_what_it_does_not_read(overrides: dict, named: str) -> None:
    with pytest.raises(SubstrateError) as refused:
        _check(**overrides)
    assert named in str(refused.value)
    # Failure case: the unit form takes the same arguments without a word.
    assert _check(within="unit", **overrides) == "unit"


def test_an_unknown_within_and_offset_with_a_token_are_refused() -> None:
    with pytest.raises(SubstrateError, match="'unit' or 'record'"):
        _check(within="passage")
    with pytest.raises(SubstrateError, match="cannot be combined"):
        _check(offset=10, continuation_token="t")


def test_id_scope_reads_node_id_conjuncts_only() -> None:
    ids = {"op": "in", "field": "node_id", "values": ["b", "a"]}
    one = {"op": "eq", "field": "node_id", "value": "a"}
    typed = {"op": "eq", "field": "subtype", "value": "ticket"}
    assert id_scope(ids) == ["a", "b"]
    assert id_scope({"op": "and", "clauses": [ids, typed]}) == ["a", "b"]
    assert id_scope({"op": "and", "clauses": [ids, one]}) == ["a"]
    # Failure cases: no id clause, or ids only under an `or`, scope nothing.
    assert id_scope(typed) is None
    assert id_scope({"op": "or", "clauses": [ids, typed]}) is None
    assert id_scope(None) is None


# ---------- how a record row prints ----------


def _record_row(node_id: str, lines: list[dict[str, Any]], count: int) -> dict[str, Any]:
    return {
        "node_id": node_id, "subtype": "ticket", "provenance": TRUSTED,
        "head": {"kind": "ticket", "id": node_id, "fields": [], "title": f"Ticket {node_id}",
                 "citation_url": None},
        "match_lines": lines, "match_count": count,
    }


def _page(rows: list[dict[str, Any]], **extra: Any) -> dict[str, Any]:
    return {
        "query": QUERY, "method": "grep", "results": rows, "total_count": len(rows),
        "offset": 0, "limit": 8, "next_offset": None, "and_scope": "record",
        "total_count_basis": "exact", **extra,
    }


def _line(text: str, line: int, unit: str, place: int | None = None) -> dict[str, Any]:
    entry: dict[str, Any] = {"text": text, "line": line, "field": None, "author": None,
                             "date": None, "unit": unit}
    if place is not None:
        entry["unit_place"] = place
    return entry


def test_a_record_row_prints_its_mode_count_and_labelled_lines() -> None:
    text = render(_page([_record_row("ticket:split", [
        _line("Workspace overflow report", 1, "text"),
        _line("the paged-attn kernel leaked", 1, "comment", 0),
    ], 2)]), tool="search")
    lines = text.splitlines()
    assert "within record" in lines[0]
    assert "2 matches" in text
    assert "    text L1: Workspace overflow report" in lines
    assert "    comment 0 L1: the paged-attn kernel leaked" in lines
    # Failure case: a unit page says within unit, and its rows no count.
    unit = render(_page([], and_scope="text_unit"), tool="search")
    assert "within unit" in unit.splitlines()[0]
    assert "within record" not in unit


# ---------- the repeat help ----------


def _grep_row(n: int) -> dict[str, Any]:
    node_id = f"ticket:{n:03d}"
    return {
        "node_id": node_id, "subtype": "ticket", "provenance": TRUSTED,
        "head": {"kind": "ticket", "id": node_id, "fields": [], "title": f"Ticket {n}",
                 "citation_url": None},
        "match_lines": [{"text": f"row {n} holds the scratch buffer", "line": 3,
                         "field": None, "author": None, "date": None}],
    }


def _search(rows: list[dict[str, Any]], query: str = "scratch buffer") -> dict[str, Any]:
    return {**_page(rows, and_scope="text_unit"), "query": query}


def test_a_page_of_rows_all_shown_earlier_says_so_and_names_broader_calls() -> None:
    state: dict[str, Any] = {}
    rows = [_grep_row(n) for n in range(4)]
    first = render(_search(rows), tool="search", session_state=state)
    # Failure case: the first page shows new rows, so no help.
    assert "shown earlier" not in first.splitlines()[0]
    assert "rows repeat" not in first
    again = render(_search(rows, query="scratch"), tool="search", session_state=state)
    lines = again.splitlines()
    assert "all 4 rows shown earlier" in lines[0]
    help_line = lines[1]
    assert help_line.startswith("! rows repeat; to find other records: ")
    assert "re:/x|y/" in help_line
    assert 'search(query="scratch", method="grep", within="record")' in help_line
    assert ('search(query="scratch", method="grep", within="record", '
            'select=["node_id","handle"], limit=100)') in help_line
    assert 'where={"type":"<type>"}' in help_line and "path_prefix=" in help_line
    # The counts and the ending stay the status line's last facts.
    assert lines[0].endswith("4 records, all shown"), lines[0]


def test_the_help_fires_from_80_percent_and_three_rows_only() -> None:
    state: dict[str, Any] = {}
    old = [_grep_row(n) for n in range(4)]
    render(_search(old), tool="search", session_state=state)
    # 4 of 5 rows repeat: 80% and at least 3.
    mixed = render(_search([*old, _grep_row(9)]), tool="search", session_state=state)
    assert "4 of 5 rows shown earlier" in mixed.splitlines()[0]
    # Failure cases: 2 of 2 repeat fires (all), 2 of 3 does not (67%).
    state2: dict[str, Any] = {}
    two = [_grep_row(n) for n in range(2)]
    render(_search(two), tool="search", session_state=state2)
    assert "all 2 rows shown earlier" in render(
        _search(two), tool="search", session_state=state2,
    ).splitlines()[0]
    state3: dict[str, Any] = {}
    render(_search(two), tool="search", session_state=state3)
    below = render(_search([*two, _grep_row(7)]), tool="search", session_state=state3)
    assert "shown earlier" not in below.splitlines()[0]
    assert "rows repeat" not in below


def test_a_record_page_does_not_offer_itself() -> None:
    state: dict[str, Any] = {}
    rows = [_record_row(f"ticket:{n}", [_line(f"line {n}", 1, "text")], 1) for n in range(3)]
    render(_page(rows), tool="search", session_state=state)
    again = render(_page(rows), tool="search", session_state=state).splitlines()
    assert "all 3 rows shown earlier" in again[0]
    assert "each term anywhere in a record" not in again[1]
    assert "every match, ids only" in again[1]


class _FakeScope:
    """The part of `_Scope` the index read uses: params and the units SQL."""

    params: dict[str, Any] = {}

    def units_sql(self, *, unit_clause: Any, record_ids: bool) -> str:
        return f"SELECT rid FROM units WHERE {unit_clause('t')}"


def test_a_slow_term_read_stops_at_its_slice_and_the_next_term_is_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On the ERAG graph (542,000 records) the longest literal's index read
    took 3 s and spent the call's budget, so the rare term after it was never
    read and the call refused. Each term's read now stops at INDEX_TERM_MS;
    the rare term still decides the candidates."""
    import time

    from okg.substrate.mcp import grep_record, grep_search
    from okg.substrate.mcp.grep_query import parse_grep_query

    seen: list[tuple[str, float]] = []

    def fake_bounded(conn: Any, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        regexes = " ".join(str(v) for k, v in params.items() if k.startswith("gri"))
        at = grep_search._DEADLINE.get()
        assert at is not None
        seen.append((regexes, at - time.monotonic()))
        if "commonwordlong" in regexes:
            raise grep_search._Cancelled()
        return [{"rid": "erag:b"}, {"rid": "erag:a"}, {"rid": "erag:a"}]

    monkeypatch.setattr(grep_record, "_indexed", lambda *a, **k: True)
    monkeypatch.setattr(grep_record, "_bounded", fake_bounded)
    tried: list[dict[str, Any]] = []
    with grep_search.deadline(time.monotonic() + 30):
        found = grep_record._index_candidates(
            None, _FakeScope(), parse_grep_query("rare commonwordlong"),  # type: ignore[arg-type]
            after="", tried=tried,
        )
    assert found == ["erag:a", "erag:b"]
    assert [entry["read"] for entry in tried] == ["timed_out", "complete"]
    assert "commonwordlong" in seen[0][0] and "rare" in seen[1][0]
    # The outer budget was 30 s; each read ran under the slice.
    assert all(left <= grep_record.INDEX_TERM_MS / 1000 + 0.05 for _r, left in seen)
