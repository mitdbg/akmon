"""The console search counts the rows its pages hold (issue #2789).

No database: the shared search, the exact-id read and the catalog are stubbed,
so these cases hold the paging arithmetic of ``compose_record_search`` alone.
The live read against a published graph is covered in
``tests/substrate/viz/test_operator_record_reads.py``.

The defect: ``total_count`` counts every ranked text match, the exact ones
included, but the exact rows are listed on the first page only and taken out
of the order the later pages walk. A client that counted pages from the total
offered a last page that came back empty and said no record matched.
"""
from __future__ import annotations

import importlib
from types import SimpleNamespace
from typing import Any

import pytest

from okg.substrate.console_views import records

# The module, not the function the `okg.substrate.mcp` package re-exports
# under the same name: the composer imports `search` from the module.
shared_search = importlib.import_module("okg.substrate.mcp.search")


def _row(node_id: str, title: str) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "subtype": "issue",
        "provenance": {"source": "src", "trust": "trusted"},
        "source_record_id": None,
        "source_revision": None,
        "attrs": {"title": title},
        "excerpt": f"{title} body text",
    }


@pytest.fixture()
def ranked(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """21 ranked matches for ``handbook``; the first one's label IS the query."""
    rows = [_row("issue:label-hit", "handbook")] + [
        _row(f"issue:text-{n:02d}", f"notes {n}") for n in range(20)
    ]

    def fake_search(conn: Any, session: Any, query: str, **_: Any) -> dict[str, Any]:
        return {"results": rows, "total_count": len(rows), "rank_capped": False}

    monkeypatch.setattr(shared_search, "search", fake_search)
    monkeypatch.setattr(records, "_source_names", lambda session: ({}, None))
    monkeypatch.setattr(records, "_catalog_types", lambda conn, session: ["issue"])
    monkeypatch.setattr(records, "_exact_record", lambda *args, **kwargs: [])
    return rows


def _search(offset: int) -> dict[str, Any]:
    return records.compose_record_search(
        None, session=SimpleNamespace(), query="handbook", limit=20, offset=offset,
    )


def test_a_label_match_leaves_the_paged_rows_out_of_the_total(
    ranked: list[dict[str, Any]],
) -> None:
    first = _search(0)

    assert first["total_count"] == 21
    # Page 1 holds all 21: the label row, then the 20 text rows.
    assert len(first["results"]) == 21
    assert first["results"][0]["match"] == "label"
    # The pages walk 20 rows, so one page holds them all.
    assert first["paged_count"] == 20
    assert first["next_offset"] is None


def test_the_page_past_the_paged_rows_is_empty(ranked: list[dict[str, Any]]) -> None:
    """The failure case the old count led to: page 2 holds nothing."""
    second = _search(20)

    assert second["results"] == []
    assert second["paged_count"] == 20
    assert second["total_count"] == 21


def test_one_more_paged_row_makes_a_real_second_page(
    ranked: list[dict[str, Any]],
) -> None:
    ranked.append(_row("issue:text-20", "notes 20"))

    first = _search(0)
    second = _search(20)

    assert first["paged_count"] == 21
    assert first["next_offset"] == 20
    assert [row["node_id"] for row in second["results"]] == ["issue:text-20"]


def test_an_empty_query_pages_nothing() -> None:
    payload = records.compose_record_search(
        None, session=SimpleNamespace(), query="   ",
    )

    assert payload["paged_count"] == 0
    assert payload["total_count"] == 0
