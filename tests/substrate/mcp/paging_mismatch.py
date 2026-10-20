"""Failure printout for a paged read compared with one whole read (#3678).

Some database tests page a read (by offset, or by `next_offset`) and compare
the rows with one larger read of the same query. They fail now and then in
CI with the same records in a different order, which suggests records with
tied scores ordered differently by different queries. `describe` is the
message such an assertion prints when it fails: every page and the whole
read with each row's position, id and the sort fields the response shows,
the positions where the order differs, ids served twice or missing, and the
totals each response reported. It only reports; it never changes what the
assertion checks.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

#: Row fields that may decide or explain the order, shown when present.
ROW_SORT_FIELDS = (
    "score",
    "rank",
    "grep_score",
    "matches",
    "matching_passages",
    "copies",
)
#: Page fields that say what the response covered.
PAGE_FIELDS = (
    "offset",
    "limit",
    "next_offset",
    "next_offset_reason",
    "total_count",
    "total_count_basis",
    "ordered_exactly_through",
    "matches_basis",
)
#: Order differences listed in full before the rest is only counted.
MAX_DIFFERENCES = 20


def _rows(response: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return list(response.get("results") or [])


def _fields(row: Mapping[str, Any]) -> str:
    fields = " ".join(
        f"{name}={row[name]!r}" for name in ROW_SORT_FIELDS if name in row
    )
    first = row.get("first_match")
    if isinstance(first, Mapping) and "chunk" in first:
        fields += f" first_chunk={first['chunk']!r}"
    return fields.strip()


def _row_text(row: Mapping[str, Any]) -> str:
    return f"{row.get('node_id')} {_fields(row)}".rstrip()


def _where(index: int | None, read: str) -> str:
    return f"{read} #{index}" if index is not None else f"not in the {read} read"


def _page_text(response: Mapping[str, Any]) -> str:
    return " ".join(
        f"{name}={response[name]!r}" for name in PAGE_FIELDS if name in response
    )


def describe(
    pages: Sequence[Mapping[str, Any]],
    whole: Mapping[str, Any] | None = None,
    *,
    expected_count: int | None = None,
    note: str = "",
) -> str:
    """Explain how the rows of `pages`, in order, differ from `whole`.

    `whole` is the one larger read the pages should match; without it, only
    `expected_count` (the number of distinct records the pages should hold)
    is shown beside the totals.
    """
    paged = [row for page in pages for row in _rows(page)]
    paged_ids = [str(row.get("node_id")) for row in paged]
    lines = ["paged read mismatch (#3678)" + (f": {note}" if note else "")]
    lines.append(
        f"totals: pages={len(pages)} rows={len(paged_ids)} distinct={len(set(paged_ids))}"
        + (f" expected={expected_count}" if expected_count is not None else "")
    )
    whole_rows = _rows(whole) if whole is not None else []
    whole_ids = [str(row.get("node_id")) for row in whole_rows]
    if whole is not None:
        lines.append(
            f"whole read: rows={len(whole_ids)} distinct={len(set(whole_ids))} "
            f"{_page_text(whole)}".rstrip()
        )

    # Ids served at more than one page position.
    where: dict[str, list[str]] = {}
    for number, page in enumerate(pages):
        for index, row in enumerate(_rows(page)):
            where.setdefault(str(row.get("node_id")), []).append(
                f"page {number} #{index}"
            )
    twice = {node_id: spots for node_id, spots in where.items() if len(spots) > 1}
    lines.append(f"served more than once: {len(twice)}")
    lines += [f"  {node_id}: {', '.join(spots)}" for node_id, spots in twice.items()]

    if whole is not None:
        whole_set = set(whole_ids)
        missing = [node_id for node_id in whole_ids if node_id not in where]
        extra = [node_id for node_id in where if node_id not in whole_set]
        lines.append(f"in the whole read, not in any page: {len(missing)} {missing}")
        lines.append(f"in a page, not in the whole read: {len(extra)} {extra}")
        # Positions where the paged order and the whole order differ.
        whole_at = {node_id: index for index, node_id in enumerate(whole_ids)}
        paged_at: dict[str, int] = {}
        for index, node_id in enumerate(paged_ids):
            paged_at.setdefault(node_id, index)
        differ = [
            index
            for index in range(max(len(paged_ids), len(whole_ids)))
            if index >= len(paged_ids)
            or index >= len(whole_ids)
            or paged_ids[index] != whole_ids[index]
        ]
        shown = (
            f" (first {MAX_DIFFERENCES} shown)" if len(differ) > MAX_DIFFERENCES else ""
        )
        lines.append(f"positions that differ: {len(differ)}{shown}")
        for index in differ[:MAX_DIFFERENCES]:
            # A list row may carry no score; show what the whole read says
            # of the same record beside it.
            at = whole_at.get(paged_ids[index]) if index < len(paged) else None
            whole_fields = f" {_fields(whole_rows[at])}" if at is not None else ""
            paged_side = (
                f"{_row_text(paged[index])} ({_where(at, 'whole')}{whole_fields.rstrip()})"
                if index < len(paged)
                else "-"
            )
            whole_side = (
                f"{_row_text(whole_rows[index])} ({_where(paged_at.get(whole_ids[index]), 'paged')})"
                if index < len(whole_rows)
                else "-"
            )
            lines.append(f"  #{index}: paged {paged_side} | whole {whole_side}")

    position = 0
    for number, page in enumerate(pages):
        rows = _rows(page)
        lines.append(f"page {number}: rows={len(rows)} {_page_text(page)}".rstrip())
        for row in rows:
            lines.append(f"  #{position} {_row_text(row)}")
            position += 1
    if whole is not None:
        lines.append("whole read:")
        lines += [
            f"  #{index} {_row_text(row)}" for index, row in enumerate(whole_rows)
        ]
    return "\n".join(lines)
