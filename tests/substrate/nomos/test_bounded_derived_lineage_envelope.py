"""The derived lineage envelope is size-bounded, and says so when capped.

PACT ``bound-derived-lineage-envelope``. Inheriting provenance through a
derivation unions per-source record detail across every contributing input,
so a wide derivation grows the envelope without limit — a claim node lifted
from thousands of chunks would carry thousands of record ids in its attrs,
on every such node.

The bound must not be a silent truncation. A capped envelope that still read
as complete would let a record-level policy allow a read on evidence that was
discarded, which is a silent fail-open.
"""
from __future__ import annotations

from okg.substrate.nomos.source_lineage import (
    MAX_LINEAGE_RECORDS_PER_SOURCE,
    RECORD_DETAIL_COMPLETE,
    RECORD_DETAIL_KEY,
    RECORD_DETAIL_TRUNCATED,
    combine_source_lineages,
    normalize_source_lineage,
    source_ids_from_lineage,
)


def _input_lineage(source_id: str, record_id: str) -> dict:
    return normalize_source_lineage(
        source_id=source_id,
        source_record_id={"chunk": record_id},
    )


def test_small_derivation_is_complete_and_unmarked() -> None:
    combined = combine_source_lineages([
        _input_lineage("paper", f"c{i}") for i in range(3)
    ])
    assert combined["status"] == "ok"
    # Absence means complete: an untruncated envelope keeps exactly the
    # bytes it had before the bound existed.
    assert combined.get(RECORD_DETAIL_KEY, RECORD_DETAIL_COMPLETE) == (
        RECORD_DETAIL_COMPLETE
    )
    assert RECORD_DETAIL_KEY not in combined
    source = combined["sources"][0]
    assert len(source["source_record_ids"]) == 3
    assert "record_detail_truncated" not in source


def test_wide_derivation_is_bounded_but_keeps_every_source_id() -> None:
    """source_ids is what trust, policy coverage, and readiness read."""
    inputs = [
        _input_lineage("paper", f"c{i}")
        for i in range(MAX_LINEAGE_RECORDS_PER_SOURCE * 4)
    ]
    inputs += [_input_lineage("slides", "s0"), _input_lineage("notes", "n0")]

    combined = combine_source_lineages(inputs)

    assert combined["status"] == "ok"
    # Every distinct source survives — the bound is on record detail only.
    assert set(source_ids_from_lineage(combined)) == {
        "paper", "slides", "notes",
    }
    by_id = {s["source_id"]: s for s in combined["sources"]}
    assert (
        len(by_id["paper"]["source_record_ids"])
        == MAX_LINEAGE_RECORDS_PER_SOURCE
    )
    # Narrow sources are untouched.
    assert len(by_id["slides"]["source_record_ids"]) == 1
    assert "record_detail_truncated" not in by_id["slides"]


def test_truncation_is_declared_not_silent() -> None:
    combined = combine_source_lineages([
        _input_lineage("paper", f"c{i}")
        for i in range(MAX_LINEAGE_RECORDS_PER_SOURCE + 1)
    ])
    assert combined[RECORD_DETAIL_KEY] == RECORD_DETAIL_TRUNCATED
    by_id = {s["source_id"]: s for s in combined["sources"]}
    assert by_id["paper"]["record_detail_truncated"] is True


def test_bound_is_deterministic_across_input_order() -> None:
    """The kept subset must not depend on merge order.

    Otherwise the same derivation stores different bytes run to run, which
    would churn rows and make the envelope non-reproducible.
    """
    inputs = [
        _input_lineage("paper", f"c{i:03d}")
        for i in range(MAX_LINEAGE_RECORDS_PER_SOURCE * 2)
    ]
    forward = combine_source_lineages(inputs)
    backward = combine_source_lineages(list(reversed(inputs)))
    assert forward["sources"] == backward["sources"]


def test_malformed_input_still_dominates_a_bounded_envelope() -> None:
    """Bounding must not paper over a missing input.

    A wide-but-incomplete derivation is still fail-closed: the meet wins
    over the cap.
    """
    inputs = [
        _input_lineage("paper", f"c{i}")
        for i in range(MAX_LINEAGE_RECORDS_PER_SOURCE * 2)
    ]
    combined = combine_source_lineages([*inputs, None])
    assert combined["status"] == "malformed"
    assert combined[RECORD_DETAIL_KEY] == RECORD_DETAIL_TRUNCATED
