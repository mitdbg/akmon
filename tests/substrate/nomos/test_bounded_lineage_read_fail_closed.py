"""A truncated lineage envelope fails closed for fine-grained reads.

PACT ``bound-derived-lineage-envelope``. Capping per-source record detail
means the contributor list a record-level policy would evaluate is
INCOMPLETE. Deciding on it would allow a read on evidence that was
discarded — a silent fail-open. A truncated envelope must therefore be
treated as unaddressable, exactly as an absent one is.

Unit-level on purpose: this pins the target-construction rule itself, so a
regression is caught without standing up a policy bundle and a graph.
"""
from __future__ import annotations

from okg.substrate.nomos.read_filter import _fine_grained_targets
from okg.substrate.nomos.source_lineage import (
    MAX_LINEAGE_RECORDS_PER_SOURCE,
    RECORD_DETAIL_KEY,
    RECORD_DETAIL_TRUNCATED,
    attrs_with_lineage,
    combine_source_lineages,
    lineage_for_row,
    normalize_source_lineage,
)


# A fine-grained access context: an `as_of` is one of the triggers for
# record-level policy evaluation (see _requires_source_record_context).
_FINE_GRAINED = {"as_of": "2026-01-01T00:00:00Z"}


def _targets(lineage: dict) -> list:
    return _fine_grained_targets(
        {"graph_ref": None, "_nomos_access_context": _FINE_GRAINED},
        row={"node_id": "n1"},
        row_kind="node",
        id_key="node_id",
        lineage=lineage,
        surface="test",
    )


def _wide_lineage(count: int) -> dict:
    return combine_source_lineages([
        normalize_source_lineage(
            source_id="paper",
            source_record_id={"chunk": f"c{i:04d}"},
        )
        for i in range(count)
    ])


def test_complete_envelope_addresses_its_real_contributors() -> None:
    """A within-bound envelope yields one target per contributing record.

    Note it does NOT assert the absence of
    ``is_unaddressable_fine_grained_lineage``: a contributor is already
    marked fail-closed when an ``as_of`` read finds no recorded
    ``source_record_observed_at``, which is pre-existing behaviour and
    unrelated to the bound. What must be absent is the TRUNCATION marker.
    """
    lineage = _wide_lineage(3)
    assert lineage.get(RECORD_DETAIL_KEY) != RECORD_DETAIL_TRUNCATED

    targets = _targets(lineage)
    record_targets = [
        resource for resource, _audit, _fail in targets
        if resource.get("resource_kind") == "source_record"
    ]
    assert len(record_targets) == 3, (
        "a complete envelope addresses each contributing record individually"
    )
    assert not any(
        resource.get("record_detail_truncated") for resource in record_targets
    )


def _round_trip(lineage: dict) -> dict:
    """Store the envelope on a row and read it back the way reads do.

    This is the path that matters: the read side does NOT read the stored
    envelope, it REBUILDS it via ``lineage_for_row`` ->
    ``normalize_source_lineage``. A marker that does not survive this is a
    marker the fail-closed guard never sees.
    """
    attrs = attrs_with_lineage({}, lineage)
    return lineage_for_row(attrs, source_id=None)


def test_truncation_marker_survives_the_read_round_trip() -> None:
    """Regression pin for a dead fail-closed guard.

    The bound stamped the marker and the read path checked for it, but
    normalization rebuilt the envelope from a fixed set of keys and dropped
    it — so the guard never fired and a capped envelope read as complete.
    """
    lineage = _wide_lineage(MAX_LINEAGE_RECORDS_PER_SOURCE * 2)
    assert lineage[RECORD_DETAIL_KEY] == RECORD_DETAIL_TRUNCATED

    reread = _round_trip(lineage)
    assert reread[RECORD_DETAIL_KEY] == RECORD_DETAIL_TRUNCATED, (
        "the truncation marker was lost on the read round-trip; the "
        "fail-closed guard is dead code and a capped envelope reads as "
        "complete"
    )
    assert _targets(reread), "the re-read envelope should still be evaluated"
    assert any(
        resource.get("record_detail_truncated")
        for resource, _audit, _fail in _targets(reread)
    )


def test_truncation_marker_latches_across_a_second_hop() -> None:
    """Truncated absorbs complete.

    A hop-2 node inheriting an already-capped envelope has fewer records
    than the bound, so nothing re-triggers truncation. Without latching it
    would be restamped `complete` and two hops would launder the marker.
    """
    hop1 = _round_trip(_wide_lineage(MAX_LINEAGE_RECORDS_PER_SOURCE * 2))
    hop2 = combine_source_lineages([hop1])
    assert hop2[RECORD_DETAIL_KEY] == RECORD_DETAIL_TRUNCATED


def test_top_level_only_marker_still_latches_through_a_hop() -> None:
    """The latch must not depend on both markers being stamped.

    `combine_source_lineages` discards the per-input envelopes it
    normalizes, keeping only the merged source entries. An input carrying
    the top-level marker but no per-source flags would therefore launder to
    complete in exactly one hop. Nothing writes that shape today; the
    invariant should not depend on nothing ever writing it.
    """
    lineage = normalize_source_lineage(
        source_id="paper",
        source_record_id={"chunk": "c0"},
    )
    lineage[RECORD_DETAIL_KEY] = RECORD_DETAIL_TRUNCATED
    # No per-source `record_detail_truncated` — top level only.
    assert not any(
        source.get("record_detail_truncated")
        for source in lineage["sources"]
    )

    combined = combine_source_lineages([lineage])
    assert combined.get(RECORD_DETAIL_KEY) == RECORD_DETAIL_TRUNCATED


def test_truncated_envelope_is_unaddressable_and_fails_closed() -> None:
    """The capped envelope must not be decided on its surviving subset."""
    lineage = _wide_lineage(MAX_LINEAGE_RECORDS_PER_SOURCE * 2)
    assert lineage[RECORD_DETAIL_KEY] == RECORD_DETAIL_TRUNCATED

    targets = _targets(lineage)
    unaddressable = [
        (resource, audit, fail_closed)
        for resource, audit, fail_closed in targets
        if resource.get("is_unaddressable_fine_grained_lineage")
    ]
    assert unaddressable, (
        "a truncated envelope must emit an unaddressable target; without it "
        "a record-level policy decides on discarded evidence"
    )
    resource, audit, fail_closed = unaddressable[0]
    # Evaluated fail-closed, and the reason is legible in the audit trail
    # rather than looking like an ordinary missing-lineage row.
    assert fail_closed is True
    assert resource["record_detail_truncated"] is True
    assert audit["record_detail_truncated"] is True
