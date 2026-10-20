"""The effective-attribute digest ignores per-run lineage order (item 92a).

An incremental run and a forced rebuild stored the same effective-attribute
rows, but the rows reach the digest sorted by their full JSON, which still
carries the per-run lineage stamp. The two runs therefore hashed equal rows in
different orders and attr_effective_digest_equal failed.
"""

from __future__ import annotations

from typing import Any

from okg.bench import orchestrator


def _row(title: str, revision: str, generation: str) -> dict[str, Any]:
    return {
        "target_kind": "node",
        "target_id": f"node:{title}",
        "namespace": "default",
        "subtype": "code_symbol",
        # Sorted keys put the lineage stamp before the title, so the stamp
        # decides the order the rows arrive in.
        "attrs": {
            "nomos_source_lineage": {"source_revision": revision},
            "title": title,
        },
        "raw_attrs": {"title": title},
        "overlay_count": 0,
        "raw_text_hash": f"sha256:{title}",
        "generation_id": generation,
        "materialized_at": f"2026-10-01T00:00:0{revision}Z",
    }


def _as_read(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The order ``_attribute_effective_rows`` returns rows in."""
    return sorted(rows, key=orchestrator._stable_json)


def test_equal_rows_digest_equal_whatever_order_the_lineage_gave_them() -> None:
    incremental = _as_read([
        _row("a", "1", "gen:incremental"),
        _row("b", "2", "gen:incremental"),
    ])
    rebuild = _as_read([
        _row("a", "9", "gen:rebuild"),
        _row("b", "3", "gen:rebuild"),
    ])
    # Not vacuous: the two runs read their rows in opposite orders.
    assert [row["target_id"] for row in incremental] == ["node:a", "node:b"]
    assert [row["target_id"] for row in rebuild] == ["node:b", "node:a"]

    assert orchestrator._attribute_effective_digest(
        incremental
    ) == orchestrator._attribute_effective_digest(rebuild)


def test_a_real_difference_still_changes_the_digest() -> None:
    base = _as_read([_row("a", "1", "gen:x"), _row("b", "2", "gen:x")])
    changed = _as_read([_row("a", "1", "gen:x"), _row("c", "2", "gen:x")])
    assert orchestrator._attribute_effective_digest(
        base
    ) != orchestrator._attribute_effective_digest(changed)
